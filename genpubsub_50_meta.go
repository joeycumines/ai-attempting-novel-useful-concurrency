package bigbuff

// Provenance: 50-meta-ai.md, "GenPubSub — parity-flipped WaitGroup generations".

import (
	"context"
	"errors"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

var ErrGenPubSubUninitialized = errors.New("bigbuff: genpubsub50: use NewGenPubSub50Meta")

// GenPubSub50Meta extracts response 50's parity-flipped sync.WaitGroup
// generations. Source identity: two preallocated WaitGroups indexed by epoch
// parity, with the sender parking in WaitGroup.Wait rather than a Cond loop.
//
// Repairs required for the repository's broadcast contract:
//   - The source's per-subscriber WaitGroup.Add can race Wait when the counter
//     is zero. Membership is counted separately; Send arms the whole snapshot
//     on the parity group before the shared-channel ping.
//   - A Done-only acknowledgement lets a fast receiver steal another copy of
//     the same round. A per-round closed channel gates re-entry (one allocation
//     per Send; the source's zero-allocation claim is not retained).
//   - SubscribeContext owns cancellation through a CAS against the receive
//     loop, so an iterator and its cancellation callback cannot both retire the
//     same subscription or the same acknowledgement.
//   - Send returns delivered acknowledgements, excluding withdrawals.
//
// Trade-off: WaitGroup sender wait plus close-broadcast receiver gate, O(1)
// hub state, one channel allocation per publication, no hub Close.
type GenPubSub50Meta[C ~chan V, V any] struct {
	ping ChanCaster[C, V]

	sendMu      sync.Mutex
	sendingMu   sync.RWMutex
	subscribers atomic.Int32

	genMu     sync.Mutex
	epoch     atomic.Uint64 // parity generation, advanced after Wait
	round     atomic.Uint64 // active publication identity, advanced before ping
	wgs       [2]sync.WaitGroup
	remaining atomic.Int32
	active    atomic.Bool
	roundDone chan struct{} // protected by genMu
}

func NewGenPubSub50Meta[C ~chan V, V any](c C) *GenPubSub50Meta[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: genpubsub50: must use an unbuffered channel")
	}
	x := new(GenPubSub50Meta[C, V])
	x.ping.C = c
	return x
}

func (x *GenPubSub50Meta[C, V]) check() {
	if x == nil || x.ping.C == nil {
		panic(ErrGenPubSubUninitialized)
	}
}

func (x *GenPubSub50Meta[C, V]) C() C { return x.ping.C }

func (x *GenPubSub50Meta[C, V]) Add(delta int) int {
	x.check()
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic("bigbuff: genpubsub50: delta out of bounds")
	}
	if delta == 0 {
		return int(x.subscribers.Load())
	}
	if delta > 0 {
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		n := x.subscribers.Add(int32(delta))
		if n < int32(delta) {
			panic("bigbuff: genpubsub50: subscriber overflow")
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
		panic("bigbuff: genpubsub50: subscriber underflow")
	}
	if !ok {
		x.ping.Add(delta)
		x.release(x.round.Load())
	} else {
		x.sendingMu.RUnlock()
	}
	return int(n)
}

func (x *GenPubSub50Meta[C, V]) Subscribe()   { x.Add(1) }
func (x *GenPubSub50Meta[C, V]) Unsubscribe() { x.Add(-1) }

func (x *GenPubSub50Meta[C, V]) Send(value V) int {
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
	epoch := x.epoch.Load()
	wg := &x.wgs[epoch&1]
	x.genMu.Lock()
	x.round.Add(1)
	wg.Add(int(n))
	x.remaining.Store(n)
	x.roundDone = make(chan struct{})
	x.active.Store(true)
	x.genMu.Unlock()
	if x.ping.Add(int(n)) != int(n) {
		x.sendingMu.Unlock()
		panic("bigbuff: genpubsub50: ping snapshot invariant")
	}
	sent := x.ping.Send(value)
	x.sendingMu.Unlock()
	wg.Wait()

	x.genMu.Lock()
	x.active.Store(false)
	x.epoch.Add(1)
	x.genMu.Unlock()
	if sent < 0 || sent > int(n) {
		panic("bigbuff: genpubsub50: delivery count invariant")
	}
	return sent
}

func (x *GenPubSub50Meta[C, V]) release(round uint64) <-chan struct{} {
	x.genMu.Lock()
	if x.round.Load() != round || !x.active.Load() {
		x.genMu.Unlock()
		return nil
	}
	done := x.roundDone
	x.wgs[(round-1)&1].Done()
	n := x.remaining.Add(-1)
	if n < 0 {
		x.genMu.Unlock()
		panic("bigbuff: genpubsub50: acknowledgement underflow")
	}
	if n == 0 {
		close(done)
	}
	x.genMu.Unlock()
	return done
}

func (x *GenPubSub50Meta[C, V]) Wait() {
	x.check()
	if done := x.release(x.round.Load()); done != nil {
		<-done
	}
}

func (x *GenPubSub50Meta[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.check()
	if ctx == nil {
		ctx = context.Background()
	}
	x.sendingMu.RLock()
	acked := x.round.Load()
	x.subscribers.Add(1)
	x.sendingMu.RUnlock()
	var owner atomic.Uint32 // 0 unclaimed, 1 iterator, 2 callback
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
			panic("bigbuff: genpubsub50: nil iterator yield")
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
