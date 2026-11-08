package bigbuff

// Provenance: 58-arena-muse-spark-1.3-xhigh-prompt-safe.md, section 2
// (ChanPubSub evolved: atomic pong countdown, one parked sender). The response
// supplied a struct and a Send sketch, not a complete implementation. The
// missing membership/acknowledgement pairing is completed here rather than
// declaring the sketch unextractable. The original design identity remains:
// one shared ChanCaster transport, a write-gated participant snapshot, an
// atomic pong count armed BEFORE rendezvous, and a Cond on which only the
// sender parks on the Cond. A per-round closed channel also gates receiver
// re-entry so a fast consumer cannot steal a second copy of the same round;
// the source's nonblocking-Wait / zero-allocation claim is not retained.
//
// Repairs: (1) count actual snapshot participants, not a stale pre-lock count;
// (2) a withdrawal before receipt compensates both ping and pong exactly once;
// (3) iterator/callback ownership transfer prevents an asynchronous steal
// from racing a committed receive; (4) an acknowledgement from an old round
// cannot decrement a new round; (5) the final signal cannot be lost before the
// sender parks. The low-level Add/Wait contract is the original immediate
// receive-then-Wait contract. Do not mix manual Add/Wait with SubscribeContext.

import (
	"context"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

type ChanPubSub58Spark[C ~chan V, V any] struct {
	ping        ChanCaster[C, V]
	sendMu      sync.Mutex
	_           [56]byte
	sendingMu   sync.RWMutex
	subscribers atomic.Int32
	_           [56]byte
	pongN       atomic.Int32
	_           [60]byte
	pongMu      sync.Mutex
	pongCond    sync.Cond
	round       atomic.Uint64
	active      atomic.Bool
	roundDone   chan struct{} // protected by pongMu; closed when pongN reaches zero
	initialized atomic.Bool
}

func NewChanPubSub58Spark[C ~chan V, V any](c C) *ChanPubSub58Spark[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanpubsub58: must use an unbuffered channel")
	}
	x := &ChanPubSub58Spark[C, V]{}
	x.ping.C = c
	x.pongCond.L = &x.pongMu
	x.initialized.Store(true)
	return x
}

func (x *ChanPubSub58Spark[C, V]) check() {
	if !x.initialized.Load() {
		panic("bigbuff: chanpubsub58: use NewChanPubSub58Spark")
	}
}

func (x *ChanPubSub58Spark[C, V]) C() C { return x.ping.C }

// Send returns the number of real receives, excluding withdrawals. It waits
// for a matching Wait for each real receive before returning.
func (x *ChanPubSub58Spark[C, V]) Send(value V) int {
	x.check()
	if x.subscribers.Load() == 0 {
		return 0
	}
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.sendingMu.Lock()
	n := x.subscribers.Load()
	if n == 0 {
		x.sendingMu.Unlock()
		return 0
	}
	x.pongMu.Lock()
	x.round.Add(1)
	x.pongN.Store(n)
	x.roundDone = make(chan struct{})
	x.active.Store(true)
	x.pongMu.Unlock()
	if x.ping.Add(int(n)) != int(n) {
		x.sendingMu.Unlock()
		panic("bigbuff: chanpubsub58: ping snapshot invariant")
	}
	sent := x.ping.Send(value)
	x.sendingMu.Unlock()

	x.pongMu.Lock()
	for x.pongN.Load() != 0 {
		x.pongCond.Wait()
	}
	x.active.Store(false)
	x.pongMu.Unlock()
	return sent
}

// release is called exactly once by the winner of an iterator state CAS, or
// by the immediate manual Wait protocol. The round check rejects late manual
// acknowledgements instead of allowing them to corrupt a subsequent Send.
func (x *ChanPubSub58Spark[C, V]) release(round uint64) <-chan struct{} {
	x.pongMu.Lock()
	if x.round.Load() != round || !x.active.Load() {
		x.pongMu.Unlock()
		return nil
	}
	done := x.roundDone
	n := x.pongN.Add(-1)
	if n < 0 {
		x.pongMu.Unlock()
		panic("bigbuff: chanpubsub58: acknowledgement underflow")
	}
	if n == 0 {
		close(done)
		x.pongCond.Signal()
	}
	x.pongMu.Unlock()
	return done
}

// Wait is the manual, immediate acknowledgement half of a successful receive
// from C. It is not a cancellation API and must not be called speculatively.
func (x *ChanPubSub58Spark[C, V]) Wait() {
	x.check()
	if done := x.release(x.round.Load()); done != nil {
		<-done
	}
}

func (x *ChanPubSub58Spark[C, V]) Subscribe()   { x.Add(1) }
func (x *ChanPubSub58Spark[C, V]) Unsubscribe() { x.Add(-1) }

// Add is the low-level participant-count API. A negative delta during a send
// withdraws an unreceived rendezvous and its pre-armed acknowledgement. As in
// the source protocol, a manual receiver must call Wait immediately after receipt.
func (x *ChanPubSub58Spark[C, V]) Add(delta int) int {
	x.check()
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic("bigbuff: chanpubsub58: delta out of bounds")
	}
	if delta == 0 {
		return int(x.subscribers.Load())
	}
	if delta > 0 {
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		n := x.subscribers.Add(int32(delta))
		if n < int32(delta) {
			panic("bigbuff: chanpubsub58: subscriber overflow")
		}
		return int(n)
	}
	ok := x.sendingMu.TryRLock()
	for !ok && x.ping.Add(0) == 0 {
		runtime.Gosched()
		ok = x.sendingMu.TryRLock()
	}
	n := x.subscribers.Add(int32(delta))
	if n < 0 {
		panic("bigbuff: chanpubsub58: subscriber underflow")
	}
	if !ok {
		x.ping.Add(delta)
		x.release(x.round.Load())
	}
	if ok {
		x.sendingMu.RUnlock()
	}
	return int(n)
}

// SubscribeContext registers synchronously at the next round boundary. The
// iterator has one consumer. Ownership of withdrawal transfers atomically
// from the context callback to the iterator BEFORE it can receive. This is
// essential: an asynchronous steal must never race a receive that the Go
// scheduler has already committed but whose user-space code has not run yet.
// Once the iterator owns withdrawal, cancellation is checked in the same
// select as receipt; a selected value is always acknowledged before cleanup.
func (x *ChanPubSub58Spark[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.check()
	if ctx == nil {
		ctx = context.Background()
	}
	x.sendingMu.RLock()
	acked := x.round.Load()
	x.subscribers.Add(1)
	x.sendingMu.RUnlock()
	var owner atomic.Uint32 // 0 unclaimed, 1 iterator, 2 context callback
	var once sync.Once
	withdraw := func() {
		once.Do(func() {
			ok := x.sendingMu.TryRLock()
			for !ok && x.ping.Add(0) == 0 {
				runtime.Gosched()
				ok = x.sendingMu.TryRLock()
			}
			x.subscribers.Add(-1)
			round := x.round.Load()
			if !ok && x.active.Load() && round > acked {
				x.ping.Add(-1)
				x.release(round)
			}
			if ok {
				x.sendingMu.RUnlock()
			}
		})
	}
	stop := context.AfterFunc(ctx, func() {
		if owner.CompareAndSwap(0, 2) {
			withdraw()
		}
	})
	return func(yield func(V) bool) {
		if !owner.CompareAndSwap(0, 1) {
			return
		}
		stop()
		defer withdraw()
		if yield == nil {
			panic("bigbuff: chanpubsub58: nil iterator yield")
		}
		for {
			select {
			case <-ctx.Done():
				return
			case value, open := <-x.ping.C:
				if !open {
					return
				}
				acked = x.round.Load()
				if done := x.release(acked); done != nil {
					<-done
				}
				if !yield(value) || ctx.Err() != nil {
					return
				}
			}
		}
	}
}
