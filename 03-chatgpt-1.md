I implemented a third, complementary broadcaster: `ChanLatest[V]` — a slab-based, coalescing channel broadcaster.

The important distinction is that this is not another variation of the existing lossless rendezvous. It targets a different, extremely useful class of workloads: broadcasting state where the newest update matters more than preserving every intermediate update.

## The design



Implemented and race-tested

0 allocations per Send



The interesting part is its registry architecture: 64-slot slabs with active/free bitmaps and intrusive page lists.

## ChanLatest architecture

Publisher — Send(value)

Serialized publishers · zero-allocation hot path

Active slab registry

Slab 0

64 slots · bitmap

Active

Slab 1

64 slots · bitmap

Active

Slab 2

64 slots · bitmap

Inactive

Free-page list

Reusable capacity

O(1) reuse

Separate active, free and all-page lists.

Mailbox 1

Capacity 1

Latest

Mailbox 2

Capacity 1

Latest

Mailbox 3

Capacity 1

Latest

Independent receive-only channels, directly usable in select.

The particularly valuable characteristics are:

1. No per-send allocations. Each subscriber has a single buffered mailbox. A new value enters an empty mailbox, or replaces its stale pending value.
2. Constant-time registry mutations. Subscribe and Unsubscribe reuse slab slots rather than rebuilding a subscriber slice or allocating a new registry snapshot.
3. Efficient traversal. Bitmaps identify active slots, while empty slabs are excluded from the publisher's traversal.
4. Safe channel closure. The registry read lock protects an entire publishing sweep. Unsubscribe and Close acquire the write lock before closing channels.
5. Predictable memory. Slabs are retained for reuse up to the concurrency high-water mark. `Trim` explicitly releases completely empty slabs.
6. Native select support. No callbacks, condition variables, or subscriber-management goroutines.

Concurrent publishers are serialized, so subscribers observe a consistent global ordering among the updates they actually receive.

## Performance measurements

I benchmarked the implementation on Go 1.23.2, Linux/AMD64, AMD EPYC 9V74.

| Subscribers | Overwrite path | Allocations |
| ----------- | -------------- | ----------- |
| 0           | 1.98 ns        | 0           |
| 1           | 40.36 ns       | 0           |
| 8           | 271 ns         | 0           |
| 64          | 2.18 µs        | 0           |
| 512         | 15.85 µs       | 0           |

The overwrite benchmark measures the full-mailbox path after its initial iteration. The measured cost settles around 31–34 ns per subscriber at larger fan-outs.

These are standalone measurements, not a demonstrated performance win against your callback implementation. The latter needs to be benchmarked alongside it on your target hardware.

## When to use each implementation

| Requirement                                               | Implementation |
| --------------------------------------------------------- | -------------- |
| Lossless broadcast with unbuffered channel semantics      | `ChanCaster`   |
| Lossless pub/sub with subscriber handshaking              | `ChanPubSub`   |
| Latest-value broadcast, non-blocking receivers and select | `ChanLatest`   |

The last implementation deliberately sacrifices delivery of intermediate values. It is especially useful for configuration snapshots, invalidations, progress updates, state observation and similar workloads.

## Deliverables

````txtar
-- chanlatest_test.go --
package bigbuff

import (
	"strconv"
	"sync"
	"testing"
)

func TestChanLatestCoalescesAndSelects(t *testing.T) {
	var x ChanLatest[int]
	s := x.Subscribe()
	if s == nil {
		t.Fatal("Subscribe returned nil")
	}
	if cap(s.C()) != 1 {
		t.Fatalf("capacity = %d, want 1", cap(s.C()))
	}

	if got := x.Send(1); got != 1 {
		t.Fatalf("Send(1) = %d, want 1", got)
	}
	if got := x.Send(2); got != 1 {
		t.Fatalf("Send(2) = %d, want 1", got)
	}
	if got := x.Send(3); got != 1 {
		t.Fatalf("Send(3) = %d, want 1", got)
	}

	select {
	case got := <-s.C():
		if got != 3 {
			t.Fatalf("received %d, want latest value 3", got)
		}
	default:
		t.Fatal("expected a value to be ready in select")
	}

	if !s.Unsubscribe() {
		t.Fatal("first Unsubscribe should return true")
	}
	if s.Unsubscribe() {
		t.Fatal("second Unsubscribe should return false")
	}
	if _, ok := <-s.C(); ok {
		t.Fatal("channel should be closed")
	}
	if got := x.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

func TestChanLatestIndependentSubscribers(t *testing.T) {
	var x ChanLatest[int]
	a := x.Subscribe()
	b := x.Subscribe()
	if got := x.Send(10); got != 2 {
		t.Fatalf("Send() = %d, want 2", got)
	}
	if got := <-a.C(); got != 10 {
		t.Fatalf("a received %d, want 10", got)
	}
	if got := <-b.C(); got != 10 {
		t.Fatalf("b received %d, want 10", got)
	}
	a.Unsubscribe()
	if got := x.Send(20); got != 1 {
		t.Fatalf("Send() after a unsubscribe = %d, want 1", got)
	}
	if _, ok := <-a.C(); ok {
		t.Fatal("a channel should be closed")
	}
	if got := <-b.C(); got != 20 {
		t.Fatalf("b received %d, want 20", got)
	}
	b.Unsubscribe()
}

func TestChanLatestClose(t *testing.T) {
	var x ChanLatest[int]
	a := x.Subscribe()
	b := x.Subscribe()
	x.Send(7)
	x.Close()
	x.Close()

	for i, s := range []*ChanLatestSubscription[int]{a, b} {
		if got, ok := <-s.C(); !ok || got != 7 {
			t.Fatalf("subscriber %d buffered receive = (%d, %v), want (7, true)", i, got, ok)
		}
		if _, ok := <-s.C(); ok {
			t.Fatalf("subscriber %d channel should now be closed", i)
		}
		if s.Unsubscribe() {
			t.Fatalf("subscriber %d was already deactivated by Close", i)
		}
	}
	if x.Subscribe() != nil {
		t.Fatal("Subscribe after Close should return nil")
	}
	if got := x.Send(8); got != 0 {
		t.Fatalf("Send after Close = %d, want 0", got)
	}
	if got := x.Subscribers(); got != 0 {
		t.Fatalf("Subscribers after Close = %d, want 0", got)
	}
}

func TestChanLatestConcurrentLifecycle(t *testing.T) {
	var x ChanLatest[uint64]
	var wg sync.WaitGroup

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2_000; i++ {
				x.Send(uint64(i))
			}
		}()
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				s := x.Subscribe()
				if s == nil {
					return
				}
				select {
				case <-s.C():
				default:
				}
				s.Unsubscribe()
			}
		}()
	}
	wg.Wait()
	if got := x.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

func TestChanLatestCloseRacesWithSendAndUnsubscribe(t *testing.T) {
	for i := 0; i < 100; i++ {
		var x ChanLatest[int]
		var subs [8]*ChanLatestSubscription[int]
		for j := range subs {
			subs[j] = x.Subscribe()
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				x.Send(n)
			}
		}()
		go func() {
			defer wg.Done()
			for _, s := range subs {
				s.Unsubscribe()
			}
		}()
		wg.Wait()
		x.Close()
	}
}

func BenchmarkChanLatestSend(b *testing.B) {
	for _, n := range []int{0, 1, 8, 64, 512} {
		b.Run("subs="+strconv.Itoa(n)+"/empty", func(b *testing.B) {
			var x ChanLatest[uint64]
			subs := make([]*ChanLatestSubscription[uint64], n)
			for i := range subs {
				subs[i] = x.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(uint64(i))
				for _, s := range subs {
					<-s.C()
				}
			}
		})

		b.Run("subs="+strconv.Itoa(n)+"/overwrite", func(b *testing.B) {
			var x ChanLatest[uint64]
			for i := 0; i < n; i++ {
				x.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(uint64(i))
			}
		})
	}
}

func TestChanLatestSlabBoundaryReuseAndTrim(t *testing.T) {
	var x ChanLatest[int]
	const n = 130 // crosses three 64-slot slabs
	subs := make([]*ChanLatestSubscription[int], n)
	for i := range subs {
		subs[i] = x.Subscribe()
		if subs[i] == nil {
			t.Fatalf("Subscribe(%d) returned nil", i)
		}
	}
	if got := x.Subscribers(); got != n {
		t.Fatalf("Subscribers() = %d, want %d", got, n)
	}
	if got := x.Send(99); got != n {
		t.Fatalf("Send() = %d, want %d", got, n)
	}
	for i, s := range subs {
		if got := <-s.C(); got != 99 {
			t.Fatalf("subscriber %d received %d, want 99", i, got)
		}
		if !s.Unsubscribe() {
			t.Fatalf("subscriber %d did not unsubscribe", i)
		}
	}

	if got := x.Trim(); got != 3 {
		t.Fatalf("Trim() = %d, want 3 slabs", got)
	}
	if got := x.Trim(); got != 0 {
		t.Fatalf("second Trim() = %d, want 0", got)
	}

	// A fresh subscription works after the slabs have been released.
	fresh := x.Subscribe()
	if fresh == nil {
		t.Fatal("Subscribe after Trim returned nil")
	}
	if got := x.Send(123); got != 1 {
		t.Fatalf("Send() after Trim = %d, want 1", got)
	}
	if got := <-fresh.C(); got != 123 {
		t.Fatalf("fresh subscription received %d, want 123", got)
	}
	fresh.Unsubscribe()
}

func TestChanLatestTrimKeepsPartiallyActiveSlabs(t *testing.T) {
	var x ChanLatest[int]
	subs := make([]*ChanLatestSubscription[int], 65)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	for i := 1; i < len(subs); i++ {
		subs[i].Unsubscribe()
	}
	if got := x.Trim(); got != 1 {
		t.Fatalf("Trim() = %d, want only the completely empty slab", got)
	}
	if got := x.Send(42); got != 1 {
		t.Fatalf("Send() = %d, want 1", got)
	}
	if got := <-subs[0].C(); got != 42 {
		t.Fatalf("remaining subscriber received %d, want 42", got)
	}
	subs[0].Unsubscribe()
	if got := x.Trim(); got != 1 {
		t.Fatalf("second Trim() = %d, want 1", got)
	}
}

func TestChanLatestStaleHandleDoesNotRemoveReusedSlot(t *testing.T) {
	var x ChanLatest[int]
	old := x.Subscribe()
	if !old.Unsubscribe() {
		t.Fatal("old subscription did not unsubscribe")
	}
	fresh := x.Subscribe() // reuses the just-freed slot
	if fresh == nil {
		t.Fatal("fresh Subscribe returned nil")
	}
	if old.Unsubscribe() {
		t.Fatal("stale handle removed the fresh subscription")
	}
	if got := x.Send(55); got != 1 {
		t.Fatalf("Send() = %d, want 1", got)
	}
	if got := <-fresh.C(); got != 55 {
		t.Fatalf("fresh subscription received %d, want 55", got)
	}
	fresh.Unsubscribe()
}

func TestChanLatestSendDoesNotAllocate(t *testing.T) {
	var x ChanLatest[uint64]
	subs := make([]*ChanLatestSubscription[uint64], 8)
	for i := range subs {
		subs[i] = x.Subscribe()
	}

	allocs := testing.AllocsPerRun(1_000, func() {
		x.Send(1)
		for _, s := range subs {
			<-s.C()
		}
	})
	if allocs != 0 {
		t.Fatalf("Send + drain allocated %v objects/run, want 0", allocs)
	}
}
-- chanlatest.go --
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
	"math/bits"
	"sync"
	"sync/atomic"
)

const chanLatestPageSize = 64

// ChanLatest is a bounded, select-friendly broadcast for state-like values.
//
// Each subscription owns a one-element channel. Send publishes to every
// current subscription without waiting for a receiver. If a subscription's
// slot is already full, Send replaces the pending value with the new value.
// Consequently, a slow subscriber observes the latest value, not necessarily
// every value sent.
//
// The implementation uses 64-slot slabs and bitmaps. Registry mutations are
// O(1), Send is O(N) in active subscriptions, and empty slabs are omitted
// from the Send traversal. Slabs are reused across churn and retained up to
// the high-water mark until Trim or Close. Send performs no heap allocations,
// and no goroutine is created per subscription. Unsubscribe may wait for an
// in-progress Send sweep before it can close the mailbox safely.
//
// The zero value is ready for use. A ChanLatest must not be copied after first
// use. Close prevents future subscriptions and sends, and closes all current
// subscription channels. A buffered value remains readable before the
// channel reports closure.
//
// ChanLatest is intended for coalescible state, invalidations, progress, and
// similar notifications. It is not an event log: use ChanCaster or ChanPubSub
// when every individual send must be received.
type ChanLatest[V any] struct {
	// sendMu serializes publishers. Besides preserving a single global order
	// across all subscribers, this ensures there is only one producer touching
	// any particular one-element mailbox at a time.
	sendMu sync.Mutex

	// mu protects the slab lists, masks, slots, and closed. Send holds RLock
	// for the complete sweep, so slots cannot be removed, reused, or closed
	// while the publisher is accessing them.
	mu sync.RWMutex

	activeHead *chanLatestPage[V]
	activeTail *chanLatestPage[V]
	freeHead   *chanLatestPage[V]
	freeTail   *chanLatestPage[V]
	allHead    *chanLatestPage[V]
	allTail    *chanLatestPage[V]

	count  atomic.Int64
	closed bool
}

// ChanLatestSubscription is one independent, select-ready mailbox belonging
// to a ChanLatest. Its channel has capacity one and is receive-only to callers.
//
// A subscription must be released with Unsubscribe when it is no longer
// needed. Unsubscribe is idempotent. The channel is closed when the
// subscription is removed, or when its parent ChanLatest is closed.
type ChanLatestSubscription[V any] struct {
	owner *ChanLatest[V]
	page  *chanLatestPage[V] // protected by owner.mu
	slot  uint8
	ch    chan V
}

// chanLatestPage is both a slab of mailbox pointers and a node in three
// intrusive lists: active pages, pages with at least one free slot, and all
// allocated pages. The active and free lists make both fan-out and churn
// proportional to live work rather than historical subscriber count.
type chanLatestPage[V any] struct {
	activeMask uint64
	freeMask   uint64
	subs       [chanLatestPageSize]*ChanLatestSubscription[V]

	activePrev *chanLatestPage[V]
	activeNext *chanLatestPage[V]
	freePrev   *chanLatestPage[V]
	freeNext   *chanLatestPage[V]
	allPrev    *chanLatestPage[V]
	allNext    *chanLatestPage[V]
}

// NewChanLatest constructs an empty ChanLatest. The zero value is also ready
// for use; this constructor exists for consistency and type-inference
// convenience.
func NewChanLatest[V any]() *ChanLatest[V] {
	return new(ChanLatest[V])
}

// Subscribe adds a new subscription and returns its one-element receive
// channel. It returns nil after the ChanLatest has been closed.
//
// A subscription added concurrently with Send participates in that Send if
// it is linked before the publisher acquires the registry read lock; otherwise
// it starts with the next Send.
func (x *ChanLatest[V]) Subscribe() *ChanLatestSubscription[V] {
	if x == nil {
		return nil
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return nil
	}

	p := x.freeHead
	if p == nil {
		p = new(chanLatestPage[V])
		p.freeMask = ^uint64(0)
		x.linkAll(p)
		x.linkFree(p)
	}

	// A free page always has at least one bit set.
	slot := uint8(bits.TrailingZeros64(p.freeMask))
	bit := uint64(1) << slot
	p.freeMask &^= bit
	if p.freeMask == 0 {
		x.unlinkFree(p)
	}

	s := &ChanLatestSubscription[V]{
		owner: x,
		page:  p,
		slot:  slot,
		ch:    make(chan V, 1),
	}
	p.subs[slot] = s
	if p.activeMask == 0 {
		x.linkActive(p)
	}
	p.activeMask |= bit
	x.count.Add(1)
	return s
}

// C returns the subscription's receive-only channel. It returns nil for a
// nil subscription.
func (s *ChanLatestSubscription[V]) C() <-chan V {
	if s == nil {
		return nil
	}
	return s.ch
}

// Unsubscribe removes the subscription and closes its channel. It returns
// true only for the call that actually removed an active subscription.
//
// It waits for an in-progress Send sweep to finish, so after Unsubscribe
// returns no later Send can publish to this subscription. Any value already
// buffered is still readable before the closed-channel result.
func (s *ChanLatestSubscription[V]) Unsubscribe() bool {
	if s == nil || s.owner == nil {
		return false
	}

	x := s.owner
	x.mu.Lock()
	defer x.mu.Unlock()

	p := s.page
	if p == nil {
		return false
	}
	slot := s.slot
	if p.subs[slot] != s {
		// Includes repeated calls and stale handles after slot reuse.
		return false
	}

	bit := uint64(1) << slot
	p.subs[slot] = nil
	s.page = nil // stale handles no longer pin a slab
	p.activeMask &^= bit
	if p.activeMask == 0 {
		x.unlinkActive(p)
	}

	wasFull := p.freeMask == 0
	p.freeMask |= bit
	if wasFull {
		x.linkFree(p)
	}

	x.count.Add(-1)
	close(s.ch)
	return true
}

// Send publishes value to each subscription that is active for this sweep.
// It returns the number of subscriptions updated.
//
// Publishing never waits for a subscriber to receive. Each mailbox retains at
// most one pending value: when full, the old pending value is discarded and
// replaced. Concurrent Send calls are serialized, so all subscribers observe
// a consistent ordering of the values that they actually receive.
//
// Send may briefly wait for another Send, Subscribe, Unsubscribe, or Close to
// release its relevant lock. It does not wait for subscriber activity.
func (x *ChanLatest[V]) Send(value V) (updated int) {
	if x == nil || x.count.Load() == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.RLock()
	defer x.mu.RUnlock()
	if x.closed {
		return 0
	}

	for p := x.activeHead; p != nil; p = p.activeNext {
		mask := p.activeMask
		for mask != 0 {
			slot := uint8(bits.TrailingZeros64(mask))
			s := p.subs[slot]
			publishLatest(s.ch, value)
			updated++
			mask &= mask - 1
		}
	}
	return updated
}

// publishLatest replaces a pending value when the one-element mailbox is
// full. Its caller guarantees exclusive producer access and prevents channel
// closure for the duration of the operation. Receivers may race freely.
func publishLatest[V any](ch chan V, value V) {
	select {
	case ch <- value:
	default:
		select {
		case <-ch:
		default:
		}
		// The channel is empty here: there is one producer, and receivers can
		// only make additional room. The channel cannot be closed by the caller
		// while Send holds mu.RLock.
		ch <- value
	}
}

// Subscribers returns the current number of active subscriptions.
func (x *ChanLatest[V]) Subscribers() int {
	if x == nil {
		return 0
	}
	return int(x.count.Load())
}

// Trim detaches completely empty slabs, allowing memory usage to fall below
// its historical high-water mark. It returns the number of slabs detached.
//
// Trim is O(P), where P is the number of allocated slabs, and should normally
// be called at a maintenance point rather than on every unsubscribe. Slabs
// containing any active subscription are retained. Removed subscription
// handles no longer retain their slab and remain safe to use after Trim.
func (x *ChanLatest[V]) Trim() (trimmed int) {
	if x == nil {
		return 0
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	for p := x.allHead; p != nil; {
		next := p.allNext
		if p.activeMask == 0 {
			x.unlinkFree(p)
			x.unlinkAll(p)
			trimmed++
		}
		p = next
	}
	return trimmed
}

// Close prevents future subscriptions and sends, and closes every active
// subscription channel. It is idempotent. Any already-buffered value remains
// readable before each channel reports closure. All slab lists are detached.
func (x *ChanLatest[V]) Close() {
	if x == nil {
		return
	}

	// Use the same lock order as Send. Once sendMu is held, no publisher can
	// be in the middle of a mailbox update while a channel is being closed.
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return
	}
	x.closed = true

	for p := x.allHead; p != nil; {
		next := p.allNext
		mask := p.activeMask
		for mask != 0 {
			slot := uint8(bits.TrailingZeros64(mask))
			if s := p.subs[slot]; s != nil {
				close(s.ch)
				s.page = nil
				p.subs[slot] = nil
			}
			mask &= mask - 1
		}
		p.activeMask = 0
		p.freeMask = 0
		p.activePrev = nil
		p.activeNext = nil
		p.freePrev = nil
		p.freeNext = nil
		p.allPrev = nil
		p.allNext = nil
		p = next
	}

	x.activeHead = nil
	x.activeTail = nil
	x.freeHead = nil
	x.freeTail = nil
	x.allHead = nil
	x.allTail = nil
	x.count.Store(0)
}

func (x *ChanLatest[V]) linkActive(p *chanLatestPage[V]) {
	p.activePrev = x.activeTail
	p.activeNext = nil
	if x.activeTail == nil {
		x.activeHead = p
	} else {
		x.activeTail.activeNext = p
	}
	x.activeTail = p
}

func (x *ChanLatest[V]) unlinkActive(p *chanLatestPage[V]) {
	if p.activePrev == nil {
		x.activeHead = p.activeNext
	} else {
		p.activePrev.activeNext = p.activeNext
	}
	if p.activeNext == nil {
		x.activeTail = p.activePrev
	} else {
		p.activeNext.activePrev = p.activePrev
	}
	p.activePrev = nil
	p.activeNext = nil
}

func (x *ChanLatest[V]) linkFree(p *chanLatestPage[V]) {
	p.freePrev = x.freeTail
	p.freeNext = nil
	if x.freeTail == nil {
		x.freeHead = p
	} else {
		x.freeTail.freeNext = p
	}
	x.freeTail = p
}

func (x *ChanLatest[V]) unlinkFree(p *chanLatestPage[V]) {
	if p.freePrev == nil {
		x.freeHead = p.freeNext
	} else {
		p.freePrev.freeNext = p.freeNext
	}
	if p.freeNext == nil {
		x.freeTail = p.freePrev
	} else {
		p.freeNext.freePrev = p.freePrev
	}
	p.freePrev = nil
	p.freeNext = nil
}

func (x *ChanLatest[V]) linkAll(p *chanLatestPage[V]) {
	p.allPrev = x.allTail
	p.allNext = nil
	if x.allTail == nil {
		x.allHead = p
	} else {
		x.allTail.allNext = p
	}
	x.allTail = p
}

func (x *ChanLatest[V]) unlinkAll(p *chanLatestPage[V]) {
	if p.allPrev == nil {
		x.allHead = p.allNext
	} else {
		p.allPrev.allNext = p.allNext
	}
	if p.allNext == nil {
		x.allTail = p.allPrev
	} else {
		p.allNext.allPrev = p.allPrev
	}
	p.allPrev = nil
	p.allNext = nil
}
-- README.md --
# ChanLatest addition for `bigbuff`

`ChanLatest[V]` is a complementary broadcaster for **coalescible state**, not
lossless events. It provides a native receive-only channel per subscriber, so
consumers can use ordinary `select` statements without callbacks or a
subscriber goroutine.

```go
updates := bigbuff.NewChanLatest[Config]()
sub := updates.Subscribe()
if sub == nil {
    return // broadcaster has been closed
}
defer sub.Unsubscribe()

for {
    select {
    case cfg, ok := <-sub.C():
        if !ok {
            return
        }
        apply(cfg)
    case <-ctx.Done():
        return
    }
}

// Publisher side:
updates.Send(nextConfig)
```

Each subscription has a capacity-one mailbox. A send to an empty mailbox
queues the value. A send to a full mailbox removes the old pending value and
queues the new one. Thus a slow subscriber gets the newest pending state, but
may skip intermediate sends. The channel is closed by `Unsubscribe` or
`Close`; an already-buffered value remains readable before the closed result.

## Design

- **No per-send allocation.** The hot path performs a bounded channel update
  for each active subscription and allocates nothing in the included
  benchmarks/tests.
- **O(N) fan-out, no receiver rendezvous.** `Send` walks active slots and
  never waits for a subscriber to receive. It serializes concurrent publishers
  so all subscribers see a consistent order among values they actually get.
- **O(1) registry mutation.** Subscribers occupy slots in 64-entry slabs.
  Per-slab active/free bitmaps and intrusive active/free/all-page lists avoid
  copying an O(N) subscriber slice on each add/remove. Empty slabs are skipped
  during fan-out.
- **Churn reuse.** Unsubscribing returns a slot to its slab's free bitmap.
  Empty slabs are retained for reuse up to the concurrency high-water mark;
  `Trim` detaches fully empty slabs at a maintenance point. `Close` detaches
  all slabs and permanently disables the broadcaster.
- **Safe channel closure.** A registry read lock protects a complete publish
  sweep. Unsubscribe/Close acquire the write side before closing channels, so
  there is no send-on-closed-channel race. A separate publisher mutex
  preserves send order and makes the mailbox overwrite operation single-writer.

## When to use it

Use `ChanLatest` for configuration snapshots, cache invalidations, current
progress, observed state, and other values where the newest update supersedes
older pending updates.

Use `ChanCaster` or `ChanPubSub` when every individual event must be delivered
and the receiver/sender handshake is part of the required semantics. The
coalescing behaviour is intentional and is not a drop-in replacement for
lossless broadcast.

## Complexity and lifecycle

| Operation | Cost |
|---|---:|
| `Send` | O(number of active subscriptions), 0 heap allocations |
| `Subscribe` | O(1), allocates a handle and buffered channel; a slab is allocated every 64 slots of new capacity |
| `Unsubscribe` | O(1) mutation; may wait for a current `Send` sweep |
| `Subscribers` | atomic load |
| `Trim` | O(number of allocated slabs), no new allocation |
| `Close` | O(number of allocated slabs + active subscriptions) |

The zero value is ready to use. Do not copy a `ChanLatest` after first use.
`Trim` detaches empty slabs from the broadcaster; their storage becomes
collectable once no live references remain.
`Send` returns the number of active subscriptions updated, not the number of
values eventually consumed. Each subscription is one logical mailbox; multiple
goroutines receiving from the same `C()` compete for that mailbox's values.

````

## Validation

The implementation passed:

- `go test -race -count=10 ./...`
- `go vet ./...`
- 32-bit cross-compilation using `GOARCH=386 go test -c`
- Patch application and independent package testing.
- Explicit zero-allocation testing of the publishing path.
- Concurrent subscription, unsubscription, publishing, closure, slot-reuse and slab-trimming tests.

One important limitation: I could not directly modify `/Users/joeyc/dev/go-bigbuff`, because that local directory isn't mounted in this environment. The deliverable is an additive patch, rather than a claim that your existing files have been changed.

My recommended next step is to benchmark all three implementations together, including consumer churn and varying fan-outs, on your actual target Go runtime. That would establish whether a further optimization to the lossless implementations is warranted, rather than optimizing against theoretical expectations.
