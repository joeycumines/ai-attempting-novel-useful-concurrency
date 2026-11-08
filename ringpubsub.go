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

// Provenance: 18-chatgpt-2.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// ErrRingFull indicates that a RingPubSub cannot admit another value until a
// reader consumes or abandons an outstanding value. No value was published.
var ErrRingFull = errors.New("bigbuff: ringpubsub: full")

// A shared terminal channel makes inactive, including zero-value,
// subscriptions selectable without allocating a channel per subscription.
var ringPubSubInactive = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// RingPubSub is a bounded, lossless broadcast log with selectable subscriptions.
// Each accepted value belongs to every subscription active at publication.
// Readers consume in publication order, independently, with no broadcast-wide
// acknowledgement barrier. A slow reader eventually backpressures publishers.
//
// Unlike ChanPubSub.Send, Send reports admission, NOT completed delivery. Its
// result counts subscriptions at publication, including any that subsequently
// abandon that value. Closing a subscription explicitly abandons its backlog.
//
// The hub has no subscriber registry, per-subscriber channel, callback, or
// background goroutine. Construction allocates a fixed ring and capacity+1
// reusable notification channels. Subscribe returns a small caller-owned value;
// whether that value escapes to the heap depends on its caller. The methods do
// not create per-publication, per-receive, or per-subscription infrastructure.
// Escape analysis, large value temporaries, runtime parking, and caller-owned
// values can still allocate. See the allocation tests and benchmarks.
//
// Publishers and distinct subscriptions may operate concurrently. Each individual
// subscription has one owner; see RingSubscription for its select contract.
// Construct with NewRingPubSub. A hub must not be copied after construction.
type RingPubSub[V any] struct {
	mu          sync.Mutex
	slots       []ringPubSubSlot[V]
	space       chan struct{}
	head        int  // oldest unreclaimed slot; guarded by mu
	used        int  // published but not yet reclaimed slots; guarded by mu
	subscribers int  // guarded by mu
	closed      bool // guarded by mu
	// tail is a physical index, NOT a wrapping message sequence. Release via
	// Store publishes slot initialization to readers that observe this index.
	tail atomic.Uint64
}

type ringPubSubSlot[V any] struct {
	// value is immutable from publication until the last obligation is released.
	value     V
	remaining atomic.Int64
	ready     chan struct{}
}

// RingSubscription is a single-owner cursor in a RingPubSub. Its zero value is
// inactive. An active subscription MUST NOT be copied. Transfer ownership to
// another goroutine by passing its address, or move a newly returned value once
// before use and never use the original again. All methods, and receives from
// Ready, must be serialized for this individual subscription.
//
// Every successful receive from Ready MUST be followed promptly by TryRecv,
// Read, Recv, or Close. A wakeup is a shared notification, not a value or an
// acknowledgement. Keeping it without advancing or closing can delay other
// readers. If another select case wins, no repair is needed: the subscription
// may continue later or Close to abandon its backlog.
//
// Close when finished. An abandoned active cursor can retain values and block
// publishers indefinitely, just like a reader that has stopped consuming.
type RingSubscription[V any] struct {
	noCopy ringPubSubNoCopy
	hub    *RingPubSub[V]
	next   int
}

// A vet marker, deliberately not a self-pointer: self-pointer copy checks would
// force otherwise stack-resident subscriptions to escape.
type ringPubSubNoCopy struct{}

func (*ringPubSubNoCopy) Lock()   {}
func (*ringPubSubNoCopy) Unlock() {}

// NewRingPubSub creates a broadcast log retaining at most capacity values.
// Capacity must be positive and must leave room for capacity+1 physical slots.
// The extra slot is essential: a caught-up cursor must never select a channel
// that still contains a notification for an older generation of that slot.
func NewRingPubSub[V any](capacity int) *RingPubSub[V] {
	if capacity <= 0 || capacity == int(^uint(0)>>1) {
		panic("bigbuff: ringpubsub: invalid capacity")
	}
	x := &RingPubSub[V]{
		slots: make([]ringPubSubSlot[V], capacity+1),
		space: make(chan struct{}, 1),
	}
	for i := range x.slots {
		x.slots[i].ready = make(chan struct{}, 1)
	}
	return x
}

func (x *RingPubSub[V]) checkInitialized() {
	if len(x.slots) == 0 {
		panic("bigbuff: ringpubsub: must use factory function")
	}
}

// Subscribe immediately registers a reader for future publications. It does not
// replay existing values. Subscribing to a closed hub returns an inactive cursor.
// Registration takes constant work and does not wait for readers or publishers
// blocked on capacity. The returned active value must not be copied.
func (x *RingPubSub[V]) Subscribe() RingSubscription[V] {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return RingSubscription[V]{}
	}
	if x.subscribers == int(^uint(0)>>1) {
		panic("bigbuff: ringpubsub: too many subscribers")
	}
	x.subscribers++
	return RingSubscription[V]{hub: x, next: int(x.tail.Load())}
}

// TrySend publishes a value without waiting for capacity. It returns the number
// of subscriptions at publication, ErrRingFull if capacity is exhausted, or
// io.ErrClosedPipe after Close. With no subscribers it returns (0, nil) without
// retaining the value. The mutex may still contend; this is not a wait-free API.
func (x *RingPubSub[V]) TrySend(value V) (int, error) {
	x.checkInitialized()
	x.mu.Lock()
	n, err := x.trySendLocked(value)
	x.mu.Unlock()
	return n, err
}

// Send waits for capacity and publishes value, or returns a context error or
// io.ErrClosedPipe. A nil context means no cancellation. On an error no value
// was published. Cancellation concurrent with successful publication may lose
// the race; a successful result always means publication occurred.
//
// This method does not wait for recipients to consume the value. Publishers are
// ordered by admission under the mutex, not by the start times of their calls.
func (x *RingPubSub[V]) Send(ctx context.Context, value V) (int, error) {
	x.checkInitialized()
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				// We might have consumed a space hint on the previous iteration.
				// Pass it on before cancellation so another writer cannot strand.
				x.mu.Lock()
				x.wakeWriterLocked()
				x.mu.Unlock()
				return 0, err
			}
		}
		x.mu.Lock()
		n, err := x.trySendLocked(value)
		x.mu.Unlock()
		if err != ErrRingFull {
			return n, err
		}
		select {
		case <-done:
			return 0, ctx.Err()
		case <-x.space:
		}
	}
}

// trySendLocked initializes a slot before exposing it through tail. Keep the
// mutex through the initial notification: a fast reader can release the last
// obligation before that notification, but cannot reclaim the slot before it.
func (x *RingPubSub[V]) trySendLocked(value V) (int, error) {
	if x.closed {
		return 0, io.ErrClosedPipe
	}
	n := x.subscribers
	if n == 0 {
		x.wakeWriterLocked()
		return 0, nil
	}
	if x.used == len(x.slots)-1 {
		// Help a final reader that released its obligation but was descheduled
		// before reclaiming. A zero count certifies all old slot accesses are
		// over; publication need not wait for that reader to run again.
		x.reapLocked()
		if x.used == len(x.slots)-1 {
			return 0, ErrRingFull
		}
	}
	i := int(x.tail.Load())
	slot := &x.slots[i]
	slot.value = value
	slot.remaining.Store(int64(n))
	x.used++
	x.tail.Store(uint64(x.advance(i)))
	ringPubSubSignal(slot.ready)
	x.wakeWriterLocked()
	return n, nil
}

// Close stops publication, wakes blocked publishers, and allows every active
// subscription to drain its existing values before observing EOF. It returns
// true exactly once. Only the unused tail notification channel is closed: this
// is a terminal position that every draining cursor will eventually reach.
// Close never races a send on an occupied slot's notification channel.
func (x *RingPubSub[V]) Close() bool {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return false
	}
	x.closed = true
	close(x.slots[int(x.tail.Load())].ready)
	close(x.space)
	return true
}

// Ready returns the notification channel for this cursor's next value. Evaluate
// it again after every successful receive or Read: it changes as the cursor
// advances. Do not retain a channel across a cursor mutation or transfer it to
// another reader. A closed channel indicates EOF; unread values precede EOF.
// An inactive subscription returns a shared, already-closed channel.
//
// A successful receive must promptly be followed by TryRecv, Read, Recv, or
// Close. When the hub is open and the contract is respected, such a receive
// guarantees that this cursor has a value to consume.
func (s *RingSubscription[V]) Ready() <-chan struct{} {
	if s.hub == nil {
		return ringPubSubInactive
	}
	return s.hub.slots[s.next].ready
}

// TryRecv consumes the next value if available. No prior Ready receive is
// necessary. It returns false if the cursor is caught up or inactive; Recv or
// Ready distinguishes a temporarily empty stream from EOF.
//
// The common non-final reader does not acquire the hub mutex. The final release
// reclaims completed slots under that mutex; it may contend with publishers.
func (s *RingSubscription[V]) TryRecv() (value V, ok bool) {
	x := s.hub
	if x == nil || s.next == int(x.tail.Load()) {
		return
	}
	slot := &x.slots[s.next]
	value = slot.value
	// Signal BEFORE releasing our obligation. The obligation pins both the
	// immutable value and its notification channel's generation until we are
	// completely finished with them. In particular, a delayed signal can never
	// leak into a later reuse of this slot.
	ringPubSubSignal(slot.ready)
	remaining := slot.remaining.Add(-1)
	s.next = x.advance(s.next)
	if remaining == 0 {
		x.mu.Lock()
		// Do not reclaim the captured slot here: another reaper may have
		// already reclaimed it while we were acquiring the mutex. Reap only
		// the CURRENT completed prefix, without retiring an old slot twice.
		x.reapLocked()
		x.mu.Unlock()
	}
	return value, true
}

// Read fills as much of dst as is currently available, returning the number of
// values copied. It never waits for more values. The caller owns and may reuse
// dst. An empty dst performs no operation and does not satisfy the obligation
// to advance or close after consuming a Ready notification.
func (s *RingSubscription[V]) Read(dst []V) int {
	for i := range dst {
		value, ok := s.TryRecv()
		if !ok {
			return i
		}
		dst[i] = value
	}
	return len(dst)
}

// Recv consumes a value, waits for one, or returns io.EOF or a context error.
// A nil context means no cancellation. A canceled call leaves the subscription
// active; Close it when no further reads will be made. Cancellation already
// observed at the start of an attempt takes precedence over available data;
// cancellation concurrent with consuming a value may lose the race.
func (s *RingSubscription[V]) Recv(ctx context.Context) (value V, err error) {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				// Ready may have been consumed by this loop OR by the caller.
				// Keep our obligation, but pass on its notification before
				// returning. A pending obligation pins an open data lane;
				// a caught-up cursor names an empty or terminal lane instead.
				if x := s.hub; x != nil && s.next != int(x.tail.Load()) {
					ringPubSubSignal(x.slots[s.next].ready)
				}
				return value, err
			}
		}
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		select {
		case <-done:
			return value, ctx.Err()
		case _, ok := <-s.Ready():
			if !ok {
				return value, io.EOF
			}
		}
	}
}

// Close unregisters this subscription and abandons its unread values. It is
// idempotent, including for a zero-value cursor. It never waits for other readers
// to make progress. Its work is proportional to this cursor's unread backlog,
// bounded by the hub's capacity, plus amortized completed-prefix reclamation.
// It must not run concurrently with another operation on this same cursor.
func (s *RingSubscription[V]) Close() {
	x := s.hub
	if x == nil {
		return
	}
	x.mu.Lock()
	x.subscribers--
	tail := int(x.tail.Load())
	for s.next != tail {
		slot := &x.slots[s.next]
		// Repair a notification even if Close follows a selected wakeup.
		// As in TryRecv, our obligation protects this signal from slot reuse.
		ringPubSubSignal(slot.ready)
		slot.remaining.Add(-1)
		s.next = x.advance(s.next)
	}
	x.reapLocked()
	x.mu.Unlock()
	s.hub = nil
}

func (x *RingPubSub[V]) advance(i int) int {
	i++
	if i == len(x.slots) {
		return 0
	}
	return i
}

// reapLocked only touches the current published prefix, never an old captured
// slot. A zero count proves that all value copies AND all notifications for the
// generation finished. Clear pointer-bearing payloads and stale hints before
// permitting reuse. A delayed final reader may safely reap nothing at all.
func (x *RingPubSub[V]) reapLocked() {
	for x.used != 0 {
		slot := &x.slots[x.head]
		if slot.remaining.Load() != 0 {
			break
		}
		var zero V
		slot.value = zero
		select {
		case <-slot.ready:
		default:
		}
		x.head = x.advance(x.head)
		x.used--
	}
	x.wakeWriterLocked()
}

// Multiple publishers share one coalesced space hint. Every successful writer
// relays it if more capacity exists. Full transitions drain an obsolete hint;
// canceled writers that consumed a hint explicitly repair it in Send.
func (x *RingPubSub[V]) wakeWriterLocked() {
	if x.closed {
		return
	}
	if x.used < len(x.slots)-1 {
		ringPubSubSignal(x.space)
	} else {
		select {
		case <-x.space:
		default:
		}
	}
}

func ringPubSubSignal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}
