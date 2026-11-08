package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// Provenance: 35-chatjimmy-prompt-mini.md (ChatJimmy, response #35).
//
// The response's intended design is a shared unbuffered ChanCaster channel
// plus a condition-variable acknowledgement barrier. The delivered file did
// not compile (nonexistent iter import/API, missing context import, wrong
// constructor invocation, and a non-atomic subscriber count), and its Send
// path unlocked the same mutex twice. This port keeps the design but uses one
// sendMu to freeze membership for each complete round and a cond-protected
// acknowledgement count. Consequence: removing a receiver during a blocked
// round is not supported; attachments explicitly avoid that exposure.

type ChanPubSub35ChatJimmy[C ~chan V, V any] struct {
	ping        *ChanCaster[C, V]
	mu          sync.Mutex // membership and Send rounds; never held while a receiver calls Wait
	subscribers atomic.Int32
	pongMu      sync.Mutex
	pong        *sync.Cond
	pending     int
	closed      atomic.Bool
}

type ChanPubSub35Subscription[C ~chan V, V any] struct {
	pub    *ChanPubSub35ChatJimmy[C, V]
	closed atomic.Bool
}

func NewChanPubSub35ChatJimmy[C ~chan V, V any](c C) *ChanPubSub35ChatJimmy[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanpubsub35: requires unbuffered channel")
	}
	x := &ChanPubSub35ChatJimmy[C, V]{ping: NewChanCaster[C, V](c)}
	x.pong = sync.NewCond(&x.pongMu)
	return x
}

// C returns the underlying shared rendezvous channel.
func (x *ChanPubSub35ChatJimmy[C, V]) C() C { return x.ping.C }

// Subscribe adds a participant. A successful receive must be followed by Wait.
func (x *ChanPubSub35ChatJimmy[C, V]) Subscribe() *ChanPubSub35Subscription[C, V] {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed.Load() {
		panic("bigbuff: chanpubsub35: subscribe on closed")
	}
	x.subscribers.Add(1)
	return &ChanPubSub35Subscription[C, V]{pub: x}
}

// Unsubscribe removes one participant between Send rounds. Calling it more than
// once through the same handle is safe; it may block until the current round ends.
func (s *ChanPubSub35Subscription[C, V]) Unsubscribe() {
	if s != nil && s.closed.CompareAndSwap(false, true) {
		s.pub.unsubscribeOne()
	}
}
func (x *ChanPubSub35ChatJimmy[C, V]) unsubscribeOne() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.subscribers.Load() == 0 {
		return
	}
	x.subscribers.Add(-1)
}

// Add adjusts anonymous receiver registrations. Callers must balance deltas.
// Membership is frozen until any current acknowledgement round is complete.
func (x *ChanPubSub35ChatJimmy[C, V]) Add(delta int) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed.Load() {
		panic("bigbuff: chanpubsub35: Add on closed")
	}
	n := int(x.subscribers.Load()) + delta
	if n < 0 || n > 1<<31-1 {
		panic("bigbuff: chanpubsub35: subscriber count out of bounds")
	}
	x.subscribers.Store(int32(n))
	return n
}

// Send delivers one value to each registered participant and does not return
// until each has received and acknowledged it with Wait.
func (x *ChanPubSub35ChatJimmy[C, V]) Send(v V) int {
	x.mu.Lock()
	if x.closed.Load() {
		x.mu.Unlock()
		panic("bigbuff: chanpubsub35: Send on closed")
	}
	n := int(x.subscribers.Load())
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	x.pongMu.Lock()
	x.pending = n
	x.pongMu.Unlock()
	if got := x.ping.Add(n); got != n {
		x.mu.Unlock()
		panic("bigbuff: chanpubsub35: caster registration mismatch")
	}
	if got := x.ping.Send(v); got != n {
		x.mu.Unlock()
		panic("bigbuff: chanpubsub35: rendezvous count mismatch")
	}
	// Keep membership frozen through the acknowledgement barrier: a receiver
	// removed after receive but before Wait is still owed by this round.
	x.pongMu.Lock()
	for x.pending != 0 {
		x.pong.Wait()
	}
	x.pongMu.Unlock()
	x.mu.Unlock()
	return n
}

// Wait acknowledges exactly one value already received from C. Calling Wait
// without a preceding receive or calling it twice for one round violates the API.
func (x *ChanPubSub35ChatJimmy[C, V]) Wait() {
	x.pongMu.Lock()
	defer x.pongMu.Unlock()
	for x.pending == 0 && !x.closed.Load() {
		x.pong.Wait()
	}
	if x.pending > 0 {
		x.pending--
		if x.pending == 0 {
			x.pong.Broadcast()
		}
	}
}

// SubscribeContext eagerly adds a receiver and yields each received value after
// acknowledging it. Cancellation is observed between rounds; it cannot abandon
// a receiver already counted in a Send round.
func (x *ChanPubSub35ChatJimmy[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	sub := x.Subscribe()
	return func(yield func(V) bool) {
		defer sub.Unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case v := <-x.C():
				x.Wait()
				if !yield(v) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close prevents new rounds. It is only safe when no Send round is active;
// callers own and must stop their consumers before closing.
func (x *ChanPubSub35ChatJimmy[C, V]) Close() {
	x.mu.Lock()
	x.closed.Store(true)
	x.mu.Unlock()
	x.pongMu.Lock()
	x.pong.Broadcast()
	x.pongMu.Unlock()
}
