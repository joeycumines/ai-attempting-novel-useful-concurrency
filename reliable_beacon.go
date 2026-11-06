package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// ReliableBeacon is ChanPubSub reimagined: Beacon for dissemination, atomic ack for Wait.
// Achieves N+1 work (1 close + N acks) vs 2N (N sends + N acks), no hchan contention.
type ReliableBeacon[T any] struct {
	_ noCopy
	beacon Beacon[T]
	// send serialization + subscriber count fence
	sendingMu sync.RWMutex
	sendMu    sync.Mutex
	subs      atomic.Int32
	ackMu     sync.Mutex
	ackCond   *sync.Cond
	pending   int32
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
		ok := x.sendingMu.TryRLock()
		for !ok && x.beacon.seq.Load() == 0 { // fast spin if no send
			ok = x.sendingMu.TryRLock()
		}
		n := int(x.subs.Add(int32(delta)))
		if ok {
			x.sendingMu.RUnlock()
		}
		if n < 0 {
			panic(`negative subs`)
		}
		return n
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
func (x *ReliableBeacon[T]) Unsubscribe() { x.Add(-1) }

func (x *ReliableBeacon[T]) Send(v T) int {
	if x.subs.Load() == 0 {
		return 0
	}
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.sendingMu.Lock()
	defer x.sendingMu.Unlock()
	n := int(x.subs.Load())
	if n == 0 {
		return 0
	}
	x.ackMu.Lock()
	x.pending = int32(n)
	x.ackMu.Unlock()
	x.beacon.Send(v) // O(1) wake
	// wait for acks
	x.ackMu.Lock()
	defer x.ackMu.Unlock()
	for x.pending != 0 {
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
	x.ackMu.Lock()
	defer x.ackMu.Unlock()
	select {
	case <-x.broken:
		panic(`bigbuff: chanpubsub: state invariant violation`)
	default:
	}
	// wait until a Send is pending
	for x.pending == 0 {
		x.ackCond.Wait()
		select {
		case <-x.broken:
			panic(`bigbuff: chanpubsub: state invariant violation`)
		default:
		}
	}
	x.pending--
	if x.pending == 0 {
		x.ackCond.Broadcast()
	}
}

func (x *ReliableBeacon[T]) C() <-chan struct{} { return x.beacon.Signal() }
func (x *ReliableBeacon[T]) Load() (T, uint64)  { return x.beacon.Load() }

func (x *ReliableBeacon[T]) SubscribeContext(ctx context.Context) iter.Seq[T] {
	x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, x.Unsubscribe)
	return func(yield func(T) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic(`nil yield`)
		}
		if !stop() {
			return
		}
		defer x.Unsubscribe()
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
				x.Wait()
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
