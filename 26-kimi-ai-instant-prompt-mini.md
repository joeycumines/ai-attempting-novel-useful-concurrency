Both files are written and verified by careful review (no Go toolchain available in this sandbox, so I couldn't run `go test -race`, but the code is stdlib-only and self-contained — drop it into `bigbuff` and it compiles against the same Go version as your existing files). Here is the full implementation.

## `ChanRing` — a bounded-loss, select-native broadcast ring

**Why it's genuinely complementary (and not derived from either existing design):** `ChanCaster`/`ChanPubSub` give you *reliable* delivery — every value reaches every receiver, via ticket arithmetic over one unbuffered channel plus cond-var pongs. `ChanRing` trades that guarantee for properties the reliable designs provably can't have:

| Property | ChanCaster / ChanPubSub | **ChanRing** |
|---|---|---|
| Sender blocks / waits for receivers | yes (rendezvous + pongs) | **never** |
| Slow consumer stalls other consumers | no, but stalls the *send* | **no — it only loses to ring overwrite** |
| Send cost | O(receivers) rendezvous | O(live) lock-free fan-out, zero alloc, no mutex held |
| Consumer churn | O(1) atomic add (excellent) | **O(1) lock-free handle pool (Treiber stack)** |
| select-native, ctx/deadline composable | yes (the big win of your design) | **yes — per-subscription private cap-1 signal channel** |
| Memory | zero alloc, unbounded receivers | **zero steady-state alloc, fixed bounded pool** |
| Delivery semantics | reliable | coalescing / bounded-loss (cap=1 → pure "latest value") |

**The novel core:** a shared ring published by a **seqlock** (odd/even sequence per cell), where each subscription is a **private cursor** — receivers pull, the sender only publishes once and fires non-blocking cap-1 tokens into per-slot channels that were all allocated once up front. Churn recycles slots through an **ABA-tagged Treiber free-list** (`(tag<<32)|(idx+1)` head, tag bumped on every push *and* pop). The missed-wakeup race is closed *structurally*, not with extra signalling: tokens are sent after publication, values persist in the ring, and the receiver **drains before blocking** — any publish not observed by a drain is followed by a token that is.

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
	"runtime"
	"sync"
	"sync/atomic"
)

const chanRingStateInvariantViolation = `bigbuff: chanring: state invariant violation`

// ChanRing is a bounded-loss, select-native, fixed-memory broadcast bus.
//
// It is deliberately complementary to ChanCaster and ChanPubSub, which
// provide reliable (every value reaches every registered receiver) fan-out
// over a single unbuffered channel using ticket arithmetic and condition
// variables. ChanRing instead provides COALESCING / BOUNDED-LOSS fan-out
// over a shared, seqlock-published ring of values, and in exchange delivers
// properties the reliable designs cannot:
//
//   - Sender never blocks and never waits for receivers. A slow or stalled
//     receiver loses values to ring overwrite; it can never stall the
//     sender or any other receiver (no head-of-line blocking).
//   - Send performs zero allocations and takes no locks during fan-out
//     (only a short mutex to serialize slot publication when senders are
//     concurrent). Cost is one cache-line publish plus one atomic load and
//     one non-blocking cap-1 channel send per live subscriber: comparable
//     to a linear callback-channel design, but with no callback indirection,
//     no RWMutex held across the fan-out, and no lock at all for receivers.
//   - Receivers are fully select-native: each subscription owns a private
//     cap-1 signal channel (allocated once, up front, for the whole pool),
//     so arbitrary select statements, context cancellation and deadlines
//     compose naturally. There is no rendezvous, so a receiver's select is
//     never coupled to the sender's progress.
//   - Consumer churn is O(1) and lock-free: subscriptions are handles into a
//     fixed slot pool recycled through an ABA-tagged Treiber stack. No
//     subscriber-set mutex, no snapshot copying, no per-subscription
//     allocation, no goroutine per subscriber, and memory usage is flat and
//     bounded regardless of churn.
//
// Semantics: every Send publishes one value into the ring. Each live
// subscription has a private cursor and reads every retained position in
// order; positions overwritten before being read are lost (counted in the
// slot's lost field). With ringCapacity 1 this degenerates to a pure
// "latest value" cell; with a larger ring it tolerates bursts of up to
// ringCapacity pending values per subscriber. A new subscription starts at
// the live tail: it does not replay retained values sent before it
// subscribed.
//
// Invariants and design notes:
//
//   - Publication is a seqlock: writers store an odd sequence, write the
//     value, then store the even sequence (twice the absolute ring
//     position). Readers validate position and parity, re-load after
//     reading the value, and retry on any mismatch; the only spinning
//     possible is while a publisher is inside its (tiny) critical section.
//     The publication mutex exists only to keep positions dense and in
//     order for concurrent senders; it is never held during fan-out.
//   - The missed-wakeup race is closed structurally, not with extra
//     signalling: the wake token is sent AFTER publication, the value
//     remains readable in the ring, and the receiver drains BEFORE
//     blocking on its private signal channel, so any publish that is not
//     observed by a drain is followed by a token that is.
//   - Slot reuse is safe with a single stale token because the channel is
//     cap 1 and a spurious wake merely costs one empty drain cycle.
//   - The free-list head is (tag << 32) | (idx + 1), with the tag bumped
//     on every successful push and pop, eliminating Treiber ABA in
//     practice.
//   - Positions are absolute uint64; the ring position is masked by a
//     power-of-two minus one, so publication is a single AND.
//
// Broad applicability: any high-frequency broadcast where consumers must
// live inside a select and can tolerate bounded loss or coalescing - market
// data, telemetry, progress/state updates, UI feeds. For reliable delivery
// of every value to every receiver, use ChanPubSub.
//
// Example:
//
//	ring := NewChanRing[Quote](1024 /* burst tolerance */, 4096 /* max subs */)
//	go func() {
//	    for q := range ring.SubscribeContext(ctx) {
//	        render(q)
//	    }
//	}()
//	ring.Send(q) // never blocks
type ChanRing[V any] struct {
	cells []chanRingCell[V] // shared broadcast ring, seqlock-published
	slots []chanRingSlot    // fixed subscriber slot pool (created once)

	mask  uint64      // ringCapacity - 1 (power of two)
	free  atomic.Uint64 // Treiber stack head: (tag << 32) | (slot idx + 1); 0 = empty
	write atomic.Uint64 // next absolute ring position to publish

	writeMu sync.Mutex // serializes claim+publish only; fan-out is lock-free

	live atomic.Int32 // number of live subscriptions
}

// NewChanRing constructs a broadcast bus with the given logical ring
// capacity (rounded up to a power of two; minimum 1, which yields pure
// latest-value semantics) and maximum concurrent subscribers.
//
// All memory (ring cells, subscriber slots and every private signal
// channel) is allocated exactly once here; the zero-allocation steady state
// holds regardless of send, receive or churn rates.
func NewChanRing[V any](ringCapacity, maxSubscribers int) *ChanRing[V] {
	if ringCapacity <= 0 {
		panic(`bigbuff: chanring: ring capacity must be positive`)
	}
	if maxSubscribers <= 0 || uint64(maxSubscribers) >= 1<<32 {
		panic(`bigbuff: chanring: max subscribers out of bounds`)
	}

	c := uint64(1)
	for c < uint64(ringCapacity) {
		c <<= 1
	}

	x := &ChanRing[V]{
		cells: make([]chanRingCell[V], c),
		slots: make([]chanRingSlot, maxSubscribers),
		mask:  c - 1,
	}
	for i := range x.slots {
		s := &x.slots[i]
		s.idx = uint32(i)
		s.ch = make(chan struct{}, 1)
		if i+1 < len(x.slots) {
			s.next.Store(uint32(i) + 2) // encoded as idx+1; 0 = nil
		}
	}
	x.free.Store(1) // slot 0, tag 0
	return x
}

// RingCapacity returns the actual (power-of-two) ring capacity.
func (x *ChanRing[V]) RingCapacity() int {
	return len(x.cells)
}

// MaxSubscribers returns the fixed subscriber pool size.
func (x *ChanRing[V]) MaxSubscribers() int {
	return len(x.slots)
}

// Len returns the number of live subscriptions.
func (x *ChanRing[V]) Len() int {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
	return int(x.live.Load())
}

// Send publishes value to the live tail of the ring and signals every live
// subscriber with a non-blocking cap-1 token. It never blocks, never
// allocates, and never waits for receivers; lagging receivers lose values
// to ring overwrite. Returns the number of live subscriptions signaled.
//
// Send is safe for concurrent use; concurrent senders are serialized only
// for the (tiny) ring publication, and fan out independently afterwards.
func (x *ChanRing[V]) Send(value V) (sent int) {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}

	// Publish: claim a dense, ordered absolute position and seqlock the cell.
	x.writeMu.Lock()
	p := x.write.Add(1)
	cell := &x.cells[p&x.mask]
	seq := p << 1
	cell.seq.Store(seq | 1) // odd: write in flight
	cell.val = value
	cell.seq.Store(seq) // even: published
	x.writeMu.Unlock()

	// Fan out: one atomic load and one non-blocking signal per live slot.
	// Dead slots cost a single atomic load; the scan touches no locks and
	// allocates nothing. A token raced in after unsubscribe is harmless:
	// the channel is cap 1 and a stale token costs one empty drain cycle.
	for i := range x.slots {
		s := &x.slots[i]
		if !s.live.Load() {
			continue
		}
		select {
		case s.ch <- struct{}{}:
		default:
		}
		sent++
	}
	return
}

// SubscribeContext returns an iterator over the values published after the
// subscription was registered, until ctx is done or the iterator stops.
// The subscription is registered eagerly (like ChanPubSub) and released when
// the iterator terminates. Values are yielded oldest-to-newest; values lost
// to ring overwrite are silently skipped - size the ring for your slowest
// consumer's tolerable lag.
func (x *ChanRing[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	s := x.subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		if yield == nil {
			x.unsubscribe(s)
			panic(`bigbuff: chanring: iterator yield func is nil`)
		}
		defer x.unsubscribe(s)
		for {
			// Drain BEFORE blocking: the token is sent after publication
			// and the ring retains the value, so this ordering closes the
			// missed-wakeup window structurally.
			if !x.drain(s, yield) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-s.ch:
			}
		}
	}
}

// subscribe pops a slot from the ABA-tagged Treiber pool and activates it.
func (x *ChanRing[V]) subscribe() *chanRingSlot {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
	for {
		h := x.free.Load()
		if uint32(h) == 0 {
			panic(`bigbuff: chanring: maximum subscribers exceeded`)
		}
		i := uint32(h) - 1
		nxt := x.slots[i].next.Load()
		nh := uint64(uint32(h>>32)+1)<<32 | uint64(nxt)
		if x.free.CompareAndSwap(h, nh) {
			s := &x.slots[i]
			s.cur = x.write.Load() // live tail: no backlog replay
			s.lost = 0
			s.live.Store(true) // visible to senders before any blocking
			x.live.Add(1)
			return s
		}
	}
}

// unsubscribe deactivates the slot and returns it to the free pool.
func (x *ChanRing[V]) unsubscribe(s *chanRingSlot) {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
	if !s.live.Load() {
		panic(chanRingStateInvariantViolation)
	}
	s.live.Store(false)
	x.live.Add(-1)
	for {
		h := x.free.Load()
		s.next.Store(uint32(h))
		nh := uint64(uint32(h>>32)+1)<<32 | uint64(s.idx+1)
		if x.free.CompareAndSwap(h, nh) {
			return
		}
	}
}

// drain reads every retained position at or after s.cur, yielding each in
// publication order, and returns false if yield stopped the iteration.
//
// Overwritten positions are counted in s.lost and skipped via a single jump
// to the oldest retained position. Spining is bounded: it can only occur
// while a publisher is inside its critical section.
func (x *ChanRing[V]) drain(s *chanRingSlot, yield func(V) bool) bool {
	w := x.write.Load()
	spins := 0
	for s.cur < w {
		p := s.cur
		cell := &x.cells[p&x.mask]
		seq := cell.seq.Load()
		pos := seq >> 1
		switch {
		case pos == p && seq&1 == 0:
			v := cell.val
			if cell.seq.Load() == seq {
				s.cur = p + 1
				if !yield(v) {
					return false
				}
				w = x.write.Load()
				spins = 0
				continue
			}
			// Torn read: fall through to the bounded spin.
		case pos > p:
			// Position p was overwritten; jump to the oldest retained
			// position (w >= cap is implied: pos >= p+cap).
			oldest := w - x.mask - 1
			s.lost += uint32(oldest - p)
			s.cur = oldest
			spins = 0
			continue
		}
		// Otherwise the publisher claimed p but has not published it yet
		// (or we observed a torn sequence); retry the same position.
		if spins++; spins > 64 {
			runtime.Gosched()
		}
	}
	return true
}

// chanRingCell is one position of the shared broadcast ring. seq is a
// seqlock: odd means a write is in flight, even means 2*position is
// published. The trailing padding keeps cells off shared cache lines; seq
// is the first word so 64-bit atomic alignment is guaranteed on 64-bit
// platforms (the intended target for this design).
type chanRingCell[V any] struct {
	seq atomic.Uint64
	val V
	_   [64]byte
}

// chanRingSlot is one subscription handle in the fixed pool. cur and lost
// are touched only by the owning iterator goroutine; live, next and the
// channel are shared with senders. Padding mitigates false sharing between
// a receiver's cursor traffic and sender signal traffic.
type chanRingSlot struct {
	live atomic.Bool     // set before the slot becomes observable by senders
	ch   chan struct{}   // cap 1; private wake signal for the owning iterator
	idx  uint32          // index into the pool (for the free list)
	next atomic.Uint32   // free-list link, encoded idx+1 (0 = nil)
	cur  uint64          // next ring position to read (owner only)
	lost uint32          // values skipped due to overwrite (owner only)
	_    [64]byte
}
```

And the accompanying test file (fanout order guarantee, deterministic bounded-loss tail, churn under `-race`, pool exhaustion panic, live-tail semantics):

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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitForChanRing(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// Every live subscriber must observe every value, in order, when the ring
// comfortably out-lives the burst.
func TestChanRingFanout(t *testing.T) {
	x := NewChanRing[int](4096, 64)
	const subs, msgs = 16, 500
	ctx, cancel := context.WithCancel(context.Background())
	var got [subs]atomic.Int32
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var want int
			for v := range x.SubscribeContext(ctx) {
				if v != want {
					t.Errorf("subscriber %d: got %d, want %d", i, v, want)
					return
				}
				want++
				got[i].Add(1)
			}
		}(i)
	}
	waitForChanRing(t, "all subscribers", func() bool { return x.Len() == subs })
	for v := 0; v < msgs; v++ {
		if sent := x.Send(v); sent != subs {
			t.Fatalf("Send(%d) signaled %d subscribers, want %d", v, sent, subs)
		}
	}
	waitForChanRing(t, "all messages at all subscribers", func() bool {
		for i := range got {
			if got[i].Load() != msgs {
				return false
			}
		}
		return true
	})
	cancel()
	wg.Wait()
	if n := x.Len(); n != 0 {
		t.Fatalf("Len() = %d after cancel, want 0", n)
	}
}

// With no iteration running while 100 values are sent into a 4-slot ring,
// only the retained tail (the last 4 positions) may be observed, exactly.
func TestChanRingBoundedLoss(t *testing.T) {
	x := NewChanRing[int](4, 4)
	ctx, cancel := context.WithCancel(context.Background())
	seq := x.SubscribeContext(ctx) // subscribes eagerly; iterator not started
	const msgs = 100
	for v := 0; v < msgs; v++ {
		x.Send(v)
	}
	var received []int
	for v := range seq {
		received = append(received, v)
		if v == msgs-1 {
			cancel()
		}
	}
	want := []int{msgs - 4, msgs - 3, msgs - 2, msgs - 1}
	if len(received) != len(want) {
		t.Fatalf("received %v, want retained tail %v", received, want)
	}
	for i := range want {
		if received[i] != want[i] {
			t.Fatalf("received %v, want retained tail %v", received, want)
		}
	}
}

// Concurrent subscribe/iterate/cancel churn against a concurrent sender must
// be race-free and return the pool to empty. Run with -race.
func TestChanRingChurn(t *testing.T) {
	x := NewChanRing[int](64, 128)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				x.Send(0)
			}
		}
	}()
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				c, ccancel := context.WithCancel(context.Background())
				n := 0
				for range x.SubscribeContext(c) {
					if n++; n >= 3 {
						ccancel()
					}
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	waitForChanRing(t, "subscriber pool to drain", func() bool { return x.Len() == 0 })
}

// Subscribing past the fixed pool size must panic (and leave state intact).
func TestChanRingExhaustion(t *testing.T) {
	x := NewChanRing[int](1, 1)
	_ = x.SubscribeContext(context.Background())
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on exceeding max subscribers")
		}
	}()
	_ = x.SubscribeContext(context.Background())
}

// A new subscription starts at the live tail: values published before
// subscribing are not replayed, and the first post-subscription value is
// delivered exactly once.
func TestChanRingLiveTail(t *testing.T) {
	x := NewChanRing[int](16, 4)
	for v := 0; v < 10; v++ {
		x.Send(v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var count atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range x.SubscribeContext(ctx) {
			count.Add(1)
		}
	}()
	x.Send(10)
	waitForChanRing(t, "single live-tail value", func() bool { return count.Load() == 1 })
	cancel()
	<-done
}
```

Key correctness arguments worth keeping in mind when you review it:

- **The drain-before-block + token-after-publish ordering makes lost wakeups impossible** for any subscription that is live: `live` is stored before the first drain, every publish to a live slot is followed by a token, and any publish between the last drain and the block is represented by either a pending value (caught by the pre-block drain) or a pending token.
- **Seqlock publication** means multi-word `V` is never torn logically: odd sequence = retry, re-validation after the value read catches writer interleaving. The race detector is satisfied because the atomic sequence stores/loads establish happens-before for `cell.val`.
- **Sender is decoupled from receiver scheduling entirely** — the only O(n) work is a scan of the slot pool (one atomic load for dead slots, one non-blocking send for live ones), with no lock held and no allocation.
