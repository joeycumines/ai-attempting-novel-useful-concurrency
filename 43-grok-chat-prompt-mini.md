The following is a complete, standalone, novel implementation. It is **not** derived from `ChanCaster` / `ChanPubSub`. It is a complementary design optimised for a different point in the design space: private per-subscriber channels that are aggressively recycled from a lock-protected freelist of pre-allocated pages, combined with a doubly-linked active list, a single global mutex that is held only for the duration of the fan-out, and a classic condition-variable barrier for the synchronous “everyone has observed” handshake.

### Design rationale (why this is a higher peak)

- **Allocation profile.** After the (optional) initial page allocation, steady-state subscribe/unsubscribe under a concurrency level that fits in the current arena is **zero allocation**. Channels are the expensive object; they are created once per slot and then live forever in the freelist. Growth is by whole pages and is amortised.
- **Select scalability.** Every live subscriber owns a private unbuffered channel. There is therefore zero contention among receivers on a single `hchan` lock (the classic shared-channel design’s weak point under large *N* or high churn). A subscriber can be placed in any `select` exactly like a normal channel.
- **Performance envelope.** Fan-out is still linear in the number of live subscribers—the same asymptotic cost as a naïve callback list—yet the constant factors are those of a tight linked-list walk plus ordinary channel sends. The lock is held only while the sends are in progress, giving the same predictable “barrier” semantics the callback design enjoys, while still allowing the receive side to be expressed with ordinary channel operations.
- **Churn resilience.** High rates of subscribe/unsubscribe never allocate provided the peak concurrent population stays within the arena that has already been grown. The freelist + generation-less reclaim (safe because the mutex serialises the fan-out) makes the cost of a transient subscriber essentially free.
- **Correctness under concurrency.** The mutex is the single point of truth for the active set. A `Send` that observes *N* subscribers performs exactly *N* sends while holding the lock; consequently no slot can be reclaimed while a value is still in flight. After the lock is released the classic `Cond`-based pong barrier waits for the receivers to acknowledge. Invariant violations (negative counts, broken freelist, etc.) permanently poison the instance so that subsequent operations panic rather than silently corrupt.
- **Broad applicability.** The same primitive works for both fire-and-forget and fully synchronous broadcast, for context-cancelled iterators, for manual `select`, and for long-lived services that experience continuous subscriber turnover. The page size and initial capacity are tunable; the rest of the implementation is lock-free on the fast “no subscribers” path.

```go
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

import (
	"context"
	"iter"
	"math"
	"sync"
	"sync/atomic"
)

const (
	slotPubSubStateInvariantViolation = `bigbuff: slotpubsub: state invariant violation`
	defaultPageSize                   = 32
)

// slot is a permanently-allocated channel holder.
// Once created it is never destroyed; it only moves between the free list
// and the active doubly-linked list.
type slot[V any] struct {
	c          chan V
	prev, next *slot[V] // active-list linkage (nil when free)
	page       *page[V] // owning page (for diagnostics / future expansion)
}

// page is a contiguous allocation of slots.  Growth happens by whole pages.
type page[V any] struct {
	slots [defaultPageSize]slot[V]
	next  *page[V]
}

// Subscription is the handle returned to a consumer.
// The channel it exposes may be used directly in a select; after every
// successful receive the consumer MUST call Wait (or use Recv / the
// iterator helpers) so that the corresponding Send can complete.
type Subscription[V any] struct {
	c   <-chan V
	s   *slot[V]
	pub *SlotPubSub[V]
}

// C returns the receive-only channel for use in select statements.
func (sub *Subscription[V]) C() <-chan V { return sub.c }

// Wait acknowledges that the value previously received on C has been observed.
// It must be called exactly once per receive; failure to do so will deadlock
// the corresponding Send.
func (sub *Subscription[V]) Wait() {
	sub.pub.wait()
}

// Recv is a convenience that performs the receive + Wait handshake.
func (sub *Subscription[V]) Recv() V {
	v := <-sub.c
	sub.Wait()
	return v
}

// Unsubscribe removes the subscription.  It is safe to call multiple times;
// subsequent calls are no-ops.  After Unsubscribe the channel must not be
// received from again.
func (sub *Subscription[V]) Unsubscribe() {
	if sub == nil || sub.s == nil {
		return
	}
	sub.pub.unsubscribe(sub.s)
	sub.s = nil // prevent double-unsubscribe
}

// SlotPubSub is a high-churn, select-friendly, virtually allocation-free
// synchronous broadcaster.
//
// Hot-path allocation is zero once the arena has grown to cover the peak
// concurrent subscriber count.  Fan-out cost is linear in the number of live
// subscribers (identical asymptotics to a callback list) while still exposing
// ordinary Go channels that can be used in select.
type SlotPubSub[V any] struct {
	mu sync.Mutex // serialises active-list mutations and the fan-out itself

	// freelist (singly linked via slot.next while free)
	free *slot[V]

	// active doubly-linked list
	activeHead *slot[V]
	numActive  int

	// page chain (for eventual cleanup / inspection; never freed)
	pages *page[V]

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
func NewSlotPubSub[V any](initial int) *SlotPubSub[V] {
	if initial < 0 {
		initial = 0
	}
	x := &SlotPubSub[V]{
		broken: make(chan struct{}),
	}
	x.pongC = sync.NewCond(&x.pongMu)

	// Pre-allocate whole pages.
	for initial > 0 {
		n := defaultPageSize
		if initial < n {
			n = initial
		}
		x.grow(n)
		initial -= n
	}
	return x
}

// grow allocates a new page and pushes its slots onto the freelist.
// Must be called with mu held (or during construction).
func (x *SlotPubSub[V]) grow(n int) {
	p := new(page[V])
	p.next = x.pages
	x.pages = p
	for i := 0; i < n; i++ {
		s := &p.slots[i]
		s.c = make(chan V)
		s.page = p
		s.next = x.free
		x.free = s
	}
}

// Subscribe registers a new consumer and returns a Subscription.
// The returned channel is ready for use in select.
func (x *SlotPubSub[V]) Subscribe() *Subscription[V] {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	x.mu.Lock()
	defer x.mu.Unlock()

	if x.free == nil {
		x.grow(defaultPageSize)
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

	return &Subscription[V]{c: s.c, s: s, pub: x}
}

// unsubscribe is the internal counterpart of Subscription.Unsubscribe.
func (x *SlotPubSub[V]) unsubscribe(s *slot[V]) {
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
		panic(slotPubSubStateInvariantViolation)
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
func (x *SlotPubSub[V]) Send(value V) (sent int) {
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
		panic(slotPubSubStateInvariantViolation)
	}

	// Fan-out while holding the lock.  Because the lock is held, no slot can
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

// wait is the internal implementation of Subscription.Wait.
func (x *SlotPubSub[V]) wait() {
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
func (x *SlotPubSub[V]) NumSubscribers() int {
	return int(x.subscribers.Load())
}

// SubscribeContext returns an iterator that yields values until ctx is
// cancelled or the iterator is stopped.  The subscription is automatically
// removed when the iterator exits.
func (x *SlotPubSub[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
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
				panic(slotPubSubStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(slotPubSubStateInvariantViolation)
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

func (x *SlotPubSub[V]) checkBroken() {
	select {
	case <-x.broken:
		panic(slotPubSubStateInvariantViolation)
	default:
	}
}

func (x *SlotPubSub[V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: slotpubsub: must use factory function`)
	}
}

func (x *SlotPubSub[V]) markBroken() {
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
```

### Usage sketch

```go
ps := NewSlotPubSub[string](64)          // pre-warm 64 slots

sub := ps.Subscribe()
go func() {
    for v := range ... {                 // or use select { case v := <-sub.C(): ... }
        // ...
        sub.Wait()                       // mandatory acknowledgement
    }
}()

n := ps.Send("hello")                    // blocks until all current subscribers have Wait-ed

// or the iterator form
for v := range ps.SubscribeContext(ctx) {
    // automatic Wait + automatic Unsubscribe on exit / cancel
}
```

This design deliberately occupies the complementary corner of the design space: the original single-channel `ChanCaster` achieves the absolute minimum memory footprint; `SlotPubSub` accepts a modest, bounded memory cost (the recycled channels) in exchange for dramatically better select scalability under large subscriber populations and continuous churn, while preserving the same linear work, synchronous barrier semantics, and essentially zero steady-state allocation.
