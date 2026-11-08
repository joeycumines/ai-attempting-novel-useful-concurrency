/*
   Copyright 2026 Joseph Cumines
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

// Provenance: 43-grok-chat-prompt-mini.md (Grok, response #43).
// Symbols are response-scoped for the flat package. This implementation holds
// its membership mutex during synchronous sends and uses a global ack count.
// Its harness adapter waits per delivered value; generic concurrent churn is
// exposure-limited because cancelling a receiver before it acknowledges an
// in-flight round can strand the publisher.

import (
	"context"
	"iter"
	"math"
	"sync"
	"sync/atomic"
)

const (
	slotPubSub43StateInvariantViolation = `bigbuff: slotpubsub: state invariant violation`
	defaultPageSize43Grok               = 32
)

// slot43Grok is a permanently-allocated channel holder.
// Once created it is never destroyed; it only moves between the free list
// and the active doubly-linked list.
type slot43Grok[V any] struct {
	c          chan V
	prev, next *slot43Grok[V] // active-list linkage (nil when free)
	page43Grok *page43Grok[V] // owning page43Grok (for diagnostics / future expansion)
}

// page43Grok is a contiguous allocation of slots.  Growth happens by whole pages.
type page43Grok[V any] struct {
	slots [defaultPageSize43Grok]slot43Grok[V]
	next  *page43Grok[V]
}

// SlotSubscription43Grok is the handle returned to a consumer.
// The channel it exposes may be used directly in a select; after every
// successful receive the consumer MUST call Wait (or use Recv / the
// iterator helpers) so that the corresponding Send can complete.
type SlotSubscription43Grok[V any] struct {
	c   <-chan V
	s   *slot43Grok[V]
	pub *SlotPubSub43Grok[V]
}

// C returns the receive-only channel for use in select statements.
func (sub *SlotSubscription43Grok[V]) C() <-chan V { return sub.c }

// Wait acknowledges that the value previously received on C has been observed.
// It must be called exactly once per receive; failure to do so will deadlock
// the corresponding Send.
func (sub *SlotSubscription43Grok[V]) Wait() {
	sub.pub.wait()
}

// Recv is a convenience that performs the receive + Wait handshake.
func (sub *SlotSubscription43Grok[V]) Recv() V {
	v := <-sub.c
	sub.Wait()
	return v
}

// Unsubscribe removes the subscription.  It is safe to call multiple times;
// subsequent calls are no-ops.  After Unsubscribe the channel must not be
// received from again.
func (sub *SlotSubscription43Grok[V]) Unsubscribe() {
	if sub == nil || sub.s == nil {
		return
	}
	sub.pub.unsubscribe(sub.s)
	sub.s = nil // prevent double-unsubscribe
}

// SlotPubSub43Grok is a high-churn, select-friendly, virtually allocation-free
// synchronous broadcaster.
//
// Hot-path allocation is zero once the arena has grown to cover the peak
// concurrent subscriber count.  Fan-out cost is linear in the number of live
// subscribers (identical asymptotics to a callback list) while still exposing
// ordinary Go channels that can be used in select.
type SlotPubSub43Grok[V any] struct {
	mu sync.Mutex // serialises active-list mutations and the fan-out itself

	// freelist (singly linked via slot43Grok.next while free)
	free *slot43Grok[V]

	// active doubly-linked list
	activeHead *slot43Grok[V]
	numActive  int

	// page43Grok chain (for eventual cleanup / inspection; never freed)
	pages *page43Grok[V]

	// fast-path counter (updated under mu, read lock-free)
	subscribers atomic.Int32

	// synchronous barrier
	pongMu sync.Mutex
	pongC  *sync.Cond
	pongN  int

	// permanent failure flag
	broken chan struct{}
}

// NewSlotPubSub creates a broadcaster.  initial is a hint for the number of
// slots to pre-allocate; it may be zero.  Additional pages are allocated
// automatically on demand.
func NewSlotPubSub43Grok[V any](initial int) *SlotPubSub43Grok[V] {
	if initial < 0 {
		initial = 0
	}
	x := &SlotPubSub43Grok[V]{
		broken: make(chan struct{}),
	}
	x.pongC = sync.NewCond(&x.pongMu)

	// Pre-allocate whole pages.
	for initial > 0 {
		n := defaultPageSize43Grok
		if initial < n {
			n = initial
		}
		x.grow(n)
		initial -= n
	}
	return x
}

// grow allocates a new page43Grok and pushes its slots onto the freelist.
// Must be called with mu held (or during construction).
func (x *SlotPubSub43Grok[V]) grow(n int) {
	p := new(page43Grok[V])
	p.next = x.pages
	x.pages = p
	for i := 0; i < n; i++ {
		s := &p.slots[i]
		s.c = make(chan V)
		s.page43Grok = p
		s.next = x.free
		x.free = s
	}
}

// Subscribe registers a new consumer and returns a SlotSubscription43Grok.
// The returned channel is ready for use in select.
func (x *SlotPubSub43Grok[V]) Subscribe() *SlotSubscription43Grok[V] {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	x.mu.Lock()
	defer x.mu.Unlock()

	if x.free == nil {
		x.grow(defaultPageSize43Grok)
	}
	s := x.free
	x.free = s.next
	s.next = nil
	s.prev = nil

	// insert at head of active list
	s.next = x.activeHead
	if x.activeHead != nil {
		x.activeHead.prev = s
	}
	x.activeHead = s
	x.numActive++
	x.subscribers.Add(1)

	return &SlotSubscription43Grok[V]{c: s.c, s: s, pub: x}
}

// unsubscribe is the internal counterpart of SlotSubscription43Grok.Unsubscribe.
func (x *SlotPubSub43Grok[V]) unsubscribe(s *slot43Grok[V]) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	x.mu.Lock()
	defer x.mu.Unlock()

	// Already free?
	if s.prev == nil && s.next == nil && x.activeHead != s {
		return
	}

	// unlink from active list
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		x.activeHead = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	s.prev = nil
	s.next = nil

	// push onto freelist
	s.next = x.free
	x.free = s

	x.numActive--
	if x.numActive < 0 {
		x.markBroken()
		panic(slotPubSub43StateInvariantViolation)
	}
	x.subscribers.Add(-1)
}

// Send delivers value to every subscriber that is registered at the moment
// the fan-out begins.  It blocks until each of those subscribers has both
// received the value and called Wait.  The number of subscribers that were
// actually sent to is returned.
//
// Concurrent Subscribe / Unsubscribe calls that race with Send observe a
// consistent snapshot: a subscriber is either fully included in the fan-out
// or fully excluded; never half-way.
func (x *SlotPubSub43Grok[V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // fast path
	}

	x.mu.Lock()
	n := x.numActive
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	if n > math.MaxInt32 {
		x.mu.Unlock()
		x.markBroken()
		panic(slotPubSub43StateInvariantViolation)
	}

	// Fan-out while holding the lock.  Because the lock is held, no slot43Grok can
	// be reclaimed until every send has completed; this eliminates ABA and
	// stale-value hazards without needing generations or hazard pointers.
	var success bool
	defer func() {
		if !success {
			x.markBroken()
		}
	}()

	for s := x.activeHead; s != nil; s = s.next {
		s.c <- value
	}
	x.mu.Unlock()

	// Synchronous barrier: wait for exactly n acknowledgements.
	x.pongMu.Lock()
	x.pongN = n
	x.pongC.Broadcast()
	for x.pongN != 0 {
		x.pongC.Wait()
		x.checkBroken()
	}
	x.pongMu.Unlock()

	success = true
	return n
}

// wait is the internal implementation of SlotSubscription43Grok.Wait.
func (x *SlotPubSub43Grok[V]) wait() {
	x.checkUsedFactoryFunction()
	x.pongMu.Lock()
	defer x.pongMu.Unlock()
	x.checkBroken()
	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken()
	}
	x.pongN--
	if x.pongN == 0 {
		x.pongC.Broadcast() // wake the Send that is waiting for us
	}
}

// NumSubscribers returns the current number of live subscriptions.
func (x *SlotPubSub43Grok[V]) NumSubscribers() int {
	return int(x.subscribers.Load())
}

// SubscribeContext returns an iterator that yields values until ctx is
// cancelled or the iterator is stopped.  The subscription is automatically
// removed when the iterator exits.
func (x *SlotPubSub43Grok[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				sub.Unsubscribe()
			}
			panic(`bigbuff: slotpubsub: iterator yield func is nil`)
		}
		if !stop() {
			return
		}
		defer sub.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(slotPubSub43StateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(slotPubSub43StateInvariantViolation)
			case v, ok := <-sub.c:
				if !ok {
					return
				}
				sub.Wait()
				if !yield(v) {
					return
				}
			}
		}
	}
}

// ---- internal safety helpers ----

func (x *SlotPubSub43Grok[V]) checkBroken() {
	select {
	case <-x.broken:
		panic(slotPubSub43StateInvariantViolation)
	default:
	}
}

func (x *SlotPubSub43Grok[V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: slotpubsub: must use factory function`)
	}
}

func (x *SlotPubSub43Grok[V]) markBroken() {
	defer func() { recover() }() // we are about to panic anyway
	x.pongMu.Lock()
	defer x.pongMu.Unlock()
	select {
	case <-x.broken:
		// already closed
	default:
		close(x.broken)
	}
	x.pongC.Broadcast()
}
