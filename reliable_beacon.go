package bigbuff

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// ReliableBeacon is ChanPubSub reimagined: Beacon for dissemination, atomic ack for Wait.
// Achieves N+1 work (1 close + N acks) vs 2N (N sends + N acks), no hchan contention.
type ReliableBeacon[T any] struct {
	_      noCopy
	beacon Beacon[T]
	// send serialization + subscriber count fence
	sendingMu sync.RWMutex
	sendMu    sync.Mutex
	subs      atomic.Int32
	ackMu     sync.Mutex
	ackCond   *sync.Cond
	pending   atomic.Int32
	armedSeq  atomic.Uint64 // beacon seq of the round `pending` was armed for
	broken    chan struct{}
}

func NewReliableBeacon[T any]() *ReliableBeacon[T] {
	rb := &ReliableBeacon[T]{broken: make(chan struct{})}
	rb.ackCond = sync.NewCond(&rb.ackMu)
	// init beacon sig
	ch := make(chan struct{})
	rb.beacon.sig.Store(&ch)
	rb.beacon.pool.New = func() any { return new(slot[T]) }
	return rb
}

func (x *ReliableBeacon[T]) Add(delta int) int {
	if x.broken == nil {
		panic(`bigbuff: reliable: use factory`)
	}
	select {
	case <-x.broken:
		panic(`bigbuff: chanpubsub: state invariant violation`)
	default:
	}
	if delta == 0 {
		return int(x.subs.Load())
	}
	if delta < 0 {
		// Honour the full magnitude: Add(-n) withdraws n subscriptions,
		// each of which may owe compensation to an armed ack barrier.
		for range -delta {
			x.unsubscribe(nil)
		}
		return int(x.subs.Load())
	}
	x.sendingMu.RLock()
	defer x.sendingMu.RUnlock()
	n := int(x.subs.Add(int32(delta)))
	if n < 0 {
		panic(`negative subs`)
	}
	return n
}
func (x *ReliableBeacon[T]) Subscribe()   { x.Add(1) }
func (x *ReliableBeacon[T]) Unsubscribe() { x.unsubscribe(nil) }

// unsubscribe withdraws a subscription. If a send is in flight and the
// subscriber has not already acked the armed round, it was counted in
// `pending` and will never Wait - so the barrier is compensated atomically
// with the decrement.
func (x *ReliableBeacon[T]) unsubscribe(lastWaited *atomic.Uint64) {
	ok := x.sendingMu.TryRLock()
	for !ok && x.pendingLoad() == 0 {
		runtime.Gosched()
		ok = x.sendingMu.TryRLock()
	}
	x.ackMu.Lock()
	waited := uint64(0)
	if lastWaited != nil {
		waited = lastWaited.Load()
	}
	n := int(x.subs.Add(-1))
	if !ok && x.pending.Load() > 0 && x.armedSeq.Load() != waited {
		if x.pending.Add(-1) == 0 {
			x.ackCond.Broadcast()
		}
	}
	x.ackMu.Unlock()
	if ok {
		x.sendingMu.RUnlock()
	}
	if n < 0 {
		panic(`negative subs`)
	}
}

func (x *ReliableBeacon[T]) Send(v T) int {
	if x.subs.Load() == 0 {
		return 0
	}
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.sendingMu.Lock()
	defer x.sendingMu.Unlock()
	// Snapshot the subscriber count and arm the ack barrier under ackMu, so
	// a concurrent Unsubscribe either lands before the snapshot (excluded
	// from n) or after (compensates pending) - never lost in between.
	x.ackMu.Lock()
	n := int(x.subs.Load())
	if n == 0 {
		x.ackMu.Unlock()
		return 0
	}
	x.pending.Store(int32(n))
	x.armedSeq.Store(x.beacon.seq.Load() + 1)
	x.ackCond.Broadcast() // wake any Wait() parked from a previous round
	x.ackMu.Unlock()
	x.beacon.Send(v) // O(1) wake
	// wait for acks
	x.ackMu.Lock()
	defer x.ackMu.Unlock()
	for x.pending.Load() > 0 {
		x.ackCond.Wait()
		select {
		case <-x.broken:
			panic(`bigbuff: chanpubsub: state invariant violation`)
		default:
		}
	}
	return n
}

func (x *ReliableBeacon[T]) Wait() {
	x.waitTracked(0, nil, false)
}

// waitTracked is Wait plus optional round tracking. When track is true the
// ack only counts against the armed round (seq), and the last-acked round is
// recorded under ackMu so the unsubscribe compensation guard is race-free.
func (x *ReliableBeacon[T]) waitTracked(seq uint64, lastWaited *atomic.Uint64, track bool) {
	x.ackMu.Lock()
	defer x.ackMu.Unlock()
	select {
	case <-x.broken:
		panic(`bigbuff: chanpubsub: state invariant violation`)
	default:
	}
	// wait until a Send is pending
	for x.pending.Load() == 0 {
		x.ackCond.Wait()
		select {
		case <-x.broken:
			panic(`bigbuff: chanpubsub: state invariant violation`)
		default:
		}
	}
	if x.pending.Load() > 0 && (!track || seq == x.armedSeq.Load()) {
		if x.pending.Add(-1) == 0 {
			x.ackCond.Broadcast()
		}
	}
	if track && lastWaited != nil {
		lastWaited.Store(seq)
	}
}

func (x *ReliableBeacon[T]) pendingLoad() int32 {
	return x.pending.Load()
}

func (x *ReliableBeacon[T]) C() <-chan struct{} { return x.beacon.Signal() }
func (x *ReliableBeacon[T]) Load() (T, uint64)  { return x.beacon.Load() }

func (x *ReliableBeacon[T]) SubscribeContext(ctx context.Context) iter.Seq[T] {
	x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	lastWaited := &atomic.Uint64{}
	stop := context.AfterFunc(ctx, func() { x.unsubscribe(lastWaited) })
	return func(yield func(T) bool) {
		if yield == nil {
			if stop() {
				x.unsubscribe(lastWaited)
			}
			panic(`nil yield`)
		}
		if !stop() {
			return
		}
		defer x.unsubscribe(lastWaited)
		next := x.beacon.seq.Load() + 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			s := x.beacon.val.Load()
			if s != nil && s.seq >= next {
				next = s.seq + 1
				x.waitTracked(s.seq, lastWaited, true)
				if !yield(s.val) {
					return
				}
				continue
			}
			sig := x.beacon.Signal()
			if s := x.beacon.val.Load(); s != nil && s.seq >= next {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-sig:
			}
		}
	}
}
