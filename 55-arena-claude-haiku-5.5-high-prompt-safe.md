## Approach

The gap is clear from the code. `ChanCaster` gets its zero-allocation, select-compatible behaviour, but every send performs N channel handoffs under a writer lock, and every subscribe or unsubscribe takes the same lock. A linear callback design has cheap sends but no select support and a locked subscriber list. I did not want to bolt a fourth mechanism onto `ChanCaster`, so I implemented one complementary design:

**`RingCaster[V]`**: a lapping ring with subscribers reduced to cursors. The publisher keeps no per-subscriber state. Subscribing and unsubscribing touch no shared state at all. Select integration uses a shared, demand-materialised wake channel, which the publisher closes only when someone is actually parked.

The ingredients are known (LMAX-style lapping rings, eventcounts, close-based broadcast). The contribution is how they are combined so that:

1. The publish cost is constant and independent of N, churn, and parked-consumer count, except for one channel close shared by all parked consumers.
2. Allocation depends only on the number of publishes, never on subscribers. It is 1/256 per publish, plus at most one wake channel per publish when a consumer parks.
3. The receive fast path is lock-free and allocation-free.
4. Slow consumers never block the publisher. They skip ahead, and the skip count is exact.

`ChanCaster` and `ChanPubSub` are left unchanged. They remain the right tool for exact, back-pressured delivery.

I have not compiled or run this code. Please run `go test -race -timeout 60s` before relying on it. The reasoning is in the comments and summarised below.

---

## Design

1. **Immutable cells, slab-allocated.** Each message is written once into a fresh cell, and the cell is never mutated afterwards. Cells come from slabs of up to 256, so one allocation covers up to 256 publishes. The ring stores `atomic.Pointer` to cells, so readers need no seqlock. A seqlock over a generic `V` would be a data race under the Go memory model.

2. **Subscribers are cursors.** A `RingSub` holds `cursor` (next sequence to read), a cached `seen` tail, and a `dropped` counter. Only the subscriber's goroutine touches it. The publisher never sees subscribers, so churn is free for the publisher. Laggards retain no memory, because they hold integers, not cells.

3. **Cached tail.** A subscriber re-reads the shared `tail` only when its cursor catches up with `seen`. During bursts, one shared load covers many messages.

4. **Demand-driven wake generation (the select primitive).** `Arm()` returns the current `chan struct{}`, creating it under a mutex only if none exists. `Publish` does one atomic load in the common case. Only if a channel is armed does it swap it out and close it, waking every parked consumer at once. Creating a generation allocates at most once per publish. With no parked consumers, nothing is allocated.

5. **Lap detection with exact accounting.** If the cell in a slot has `seq > cursor`, the subscriber was lapped. It skips to that cell and adds the gap to `dropped`. Conservation holds: `received + Dropped() == published`.

6. **Replay, resume, conflation, close.** `SubscribeFrom(seq)` resumes from any retained sequence. `TryRecvLatest` conflates a backlog. `Close` lets consumers drain what remains, then returns `ErrRingClosed`.

### Why no wakeups are lost

Parking is decided only after a fresh tail load shows nothing. That load comes after `Arm` has published the wake channel. The publisher stores the tail, then loads the wake slot and closes whatever it finds. Go's `sync/atomic` operations are sequentially consistent, so one of two things holds:

- The subscriber's tail load observes the publish, or
- The publisher's wake load observes the subscriber's channel, and closes it.

`Close` follows the same pattern with the `closed` flag, which `Recv` checks after `Arm`.

### Why reads are safe

A subscriber reads `ring[c & mask]`, and `c < tail` was observed. The slot therefore holds some cell with `seq >= c`. If `seq == c`, the value is correct. If `seq > c`, the subscriber was lapped and the skipped range is exact.

### Allocation profile

| Operation | Allocations |
|---|---|
| `Publish`, no parked consumers | 1/256 (one slab per 256 publishes) |
| `Publish`, consumers parked | 1/256 + at most 1 wake channel |
| `TryRecv` / `Recv` fast path | 0 |
| `Subscribe(s)` with caller-owned `s` | 0 |
| Unsubscribe | 0 (drop the cursor) |

Retention is bounded: at most `capacity + 2*256` values are pinned by the ring.

### Relation to the existing designs

| | `ChanCaster` | `ChanPubSub` | `RingCaster` |
|---|---|---|---|
| Delivery | exact, back-pressured | exact, back-pressured | bounded, lossy with exact accounting |
| Publish cost | N handoffs under lock | cond-var handshake | O(1) + one close if parked |
| Churn touches | shared lock + CAS | shared lock + cond | nothing shared |
| Slow consumer | blocks publisher | blocks publisher | skipped, counted |
| `select` | yes | yes | yes, via `Arm` |

They compose well. `ChanCaster` can carry control or flow-credit traffic, and `RingCaster` can carry the data plane.

### Trade-offs

- Subscribers that fall more than `capacity` behind lose messages. The loss is counted, not hidden.
- Each `RingSub` must be used by one goroutine at a time.
- Each receiver copies `V`. Use pointer-sized `V` for large payloads.
- Publishers are serialised by a mutex. This is also true of `ChanCaster`.

---

## Code

### `ringcaster.go`

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
	"errors"
	"sync"
	"sync/atomic"
)

// ErrRingClosed is returned by [RingSub.Recv] once the [RingCaster] has been
// closed and the subscriber has consumed every message it still retains.
var ErrRingClosed = errors.New("bigbuff: ringcaster: closed")

const (
	ringMaxCapacity        = 1 << 30
	ringSlabMax            = 256
	ringInvariantViolation = `bigbuff: ringcaster: state invariant violation`
)

// noWake is the "nothing armed" state of [RingCaster.wake]. It is boxed once,
// so swapping it in never allocates.
var noWake any = (chan struct{})(nil)

// ringCell is an immutable message cell. It is written exactly once, strictly
// before the ring slot referencing it is stored, and is never mutated or
// reused afterwards. Readers therefore need no locks, seqlocks, or
// synchronisation beyond the atomic slot load.
type ringCell[V any] struct {
	seq uint64
	v   V
}

// RingCaster is a non-blocking broadcast ring with exact loss accounting. It
// is the data-plane complement to [ChanCaster]:
//
//   - [RingCaster.Publish] never waits for subscribers. It does constant work,
//     plus a single channel close shared by all parked subscribers, if any.
//   - A subscriber is only a cursor ([RingSub]). Subscribing and unsubscribing
//     touch no shared state, so churn costs the publisher nothing.
//   - Subscribers integrate with select via [RingSub.Arm] and
//     [RingSub.TryRecv].
//   - A subscriber more than the capacity behind is never back-pressured. It
//     skips ahead, and the skipped count is exact ([RingSub.Dropped]).
//     Conservation holds: received + Dropped() == published.
//
// Allocation: one slab per min(capacity, 256) publishes, plus at most one wake
// channel per publish, and only when a subscriber has armed since the previous
// publish. Consumers that keep up with the publisher cause no allocation.
//
// Retention: the ring pins at most capacity + 2*256 values, regardless of how
// far any subscriber lags, because subscribers hold no references.
//
// A RingCaster is safe for concurrent use by any number of publishers and
// subscribers. Each individual [RingSub] must be used by one goroutine at a
// time.
type RingCaster[V any] struct {
	ring []atomic.Pointer[ringCell[V]] // slot for seq is seq & mask
	mask uint64

	tail   atomic.Uint64 // number of published messages, i.e. the next seq
	closed atomic.Bool
	wake   atomic.Value // chan struct{} while armed, otherwise noWake

	pubMu   sync.Mutex // serialises Publish and Close, guards slab fields
	slab    []ringCell[V]
	slabPos int

	armMu sync.Mutex // serialises creation of wake channels
}

// NewRingCaster constructs a [RingCaster] retaining the most recent capacity
// messages. Capacity is rounded up to a power of two. It panics if capacity is
// outside [1, 1<<30].
func NewRingCaster[V any](capacity int) *RingCaster[V] {
	if capacity < 1 || capacity > ringMaxCapacity {
		panic(`bigbuff: ringcaster: capacity out of bounds`)
	}
	n := 1
	for n < capacity {
		n <<= 1
	}
	return &RingCaster[V]{
		ring: make([]atomic.Pointer[ringCell[V]], n),
		mask: uint64(n - 1),
	}
}

// Publish appends v, waking any parked subscribers, and returns its sequence
// number. It panics if the caster has been closed.
func (b *RingCaster[V]) Publish(v V) uint64 {
	b.pubMu.Lock()
	if b.closed.Load() {
		b.pubMu.Unlock()
		panic(`bigbuff: ringcaster: publish after close`)
	}

	seq := b.tail.Load()
	if b.slabPos == len(b.slab) {
		b.slab = make([]ringCell[V], min(len(b.ring), ringSlabMax))
		b.slabPos = 0
	}
	cell := &b.slab[b.slabPos]
	b.slabPos++
	cell.seq = seq
	cell.v = v

	b.ring[seq&b.mask].Store(cell) // publish the cell (release)
	b.tail.Store(seq + 1)          // publish the sequence
	b.pubMu.Unlock()

	b.wakeArmed()
	return seq
}

// Close marks the caster closed and wakes all parked subscribers. Subscribers
// first drain what they retain, then observe [ErrRingClosed]. Close is
// idempotent. Publish panics after Close.
func (b *RingCaster[V]) Close() {
	b.pubMu.Lock()
	b.closed.Store(true)
	b.pubMu.Unlock()
	b.wakeArmed()
}

// Subscribe joins s to the caster, starting at the next published message,
// and returns s. Passing caller-owned storage makes subscription allocation
// free, which is useful under heavy churn.
func (b *RingCaster[V]) Subscribe(s *RingSub[V]) *RingSub[V] {
	t := b.tail.Load()
	*s = RingSub[V]{b: b, cursor: t, seen: t}
	return s
}

// SubscribeFrom joins s to the caster, starting at sequence seq, and returns s.
// seq is clamped to the oldest retained message, or to the next message to be
// published. This supports replay and resumption after reconnecting.
func (b *RingCaster[V]) SubscribeFrom(s *RingSub[V], seq uint64) *RingSub[V] {
	t := b.tail.Load()
	var oldest uint64
	if n := uint64(len(b.ring)); t > n {
		oldest = t - n
	}
	switch {
	case seq < oldest:
		seq = oldest
	case seq > t:
		seq = t
	}
	*s = RingSub[V]{b: b, cursor: seq, seen: t}
	return s
}

// arm returns the current wake channel, creating it if necessary. The fast
// path is a single atomic load. Creation is serialised so that concurrent
// armers share one channel, never orphaning one.
func (b *RingCaster[V]) arm() <-chan struct{} {
	if ch, _ := b.wake.Load().(chan struct{}); ch != nil {
		return ch
	}
	b.armMu.Lock()
	defer b.armMu.Unlock()
	ch, _ := b.wake.Load().(chan struct{})
	if ch == nil {
		ch = make(chan struct{})
		b.wake.Store(ch)
	}
	return ch
}

// wakeArmed closes the armed wake channel, if any. The common case, nothing
// armed, is a single atomic load with no write to the shared line.
func (b *RingCaster[V]) wakeArmed() {
	if ch, _ := b.wake.Load().(chan struct{}); ch == nil {
		return
	}
	if ch, _ := b.wake.Swap(noWake).(chan struct{}); ch != nil {
		close(ch)
	}
}

// RingSub is a subscriber cursor over a [RingCaster]. The zero value is not
// usable. Obtain one via [RingCaster.Subscribe] or [RingCaster.SubscribeFrom].
type RingSub[V any] struct {
	b       *RingCaster[V]
	cursor  uint64 // next sequence number to receive
	seen    uint64 // cached tail; cursor < seen implies cursor is published
	dropped uint64 // messages skipped because this subscriber was lapped
}

// TryRecv returns the next message without blocking. It returns ok == false
// if there is nothing new. Lapped subscribers skip ahead and account for the
// skipped messages in [RingSub.Dropped].
//
// Select usage, for a subscriber that must also wait on other channels:
//
//	for {
//		if v, ok := sub.TryRecv(); ok { handle(v); continue }
//		wake := sub.Arm()                    // arm BEFORE the re-check
//		if v, ok := sub.TryRecv(); ok { handle(v); continue }
//		if sub.Ended() { return }
//		select {
//		case <-wake:
//		case <-done:
//			return
//		}
//	}
func (s *RingSub[V]) TryRecv() (V, bool) {
	var zero V
	if s.cursor >= s.seen {
		s.seen = s.b.tail.Load()
		if s.cursor >= s.seen {
			return zero, false
		}
	}

	c := s.b.ring[s.cursor&s.b.mask].Load()
	if c == nil || c.seq < s.cursor {
		panic(ringInvariantViolation)
	}
	if c.seq == s.cursor {
		s.cursor++
	} else { // lapped: the slot now holds a newer message
		s.dropped += c.seq - s.cursor
		s.cursor = c.seq + 1
	}
	return c.v, true
}

// TryRecvLatest conflates: it skips the backlog and returns the most recently
// published message not yet received. Skipped messages are conflated, not
// counted as dropped.
func (s *RingSub[V]) TryRecvLatest() (V, bool) {
	var zero V
	t := s.b.tail.Load()
	if s.cursor >= t {
		return zero, false
	}
	s.cursor = t - 1
	return s.TryRecv()
}

// Recv blocks until a message is available, the context is done, or the
// caster is closed and drained. A nil context is treated as Background.
func (s *RingSub[V]) Recv(ctx context.Context) (V, error) {
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		wake := s.Arm()
		// Re-check after arming. This is the lost-wakeup barrier.
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		if s.Ended() {
			return zero, ErrRingClosed
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}

// Arm returns a channel that is closed by the next Publish or Close after the
// call. Call [RingSub.TryRecv] again after Arm, before waiting on the channel.
// Waiting without that re-check can miss a publish.
func (s *RingSub[V]) Arm() <-chan struct{} {
	return s.b.arm()
}

// Ended reports whether the caster is closed and this subscriber has consumed
// every message it retains. The closed flag is loaded before the tail, so a
// true result is never premature.
func (s *RingSub[V]) Ended() bool {
	return s.b.closed.Load() && s.cursor >= s.b.tail.Load()
}

// Lag returns the number of published messages this subscriber has not yet
// consumed or skipped.
func (s *RingSub[V]) Lag() uint64 {
	if t := s.b.tail.Load(); t > s.cursor {
		return t - s.cursor
	}
	return 0
}

// Dropped returns the total number of messages skipped because this subscriber
// was lapped by the publisher.
func (s *RingSub[V]) Dropped() uint64 {
	return s.dropped
}

// Seq returns the sequence number of the next message this subscriber will
// receive. Store it to resume later via [RingCaster.SubscribeFrom].
func (s *RingSub[V]) Seq() uint64 {
	return s.cursor
}
```

### `ringcaster_test.go`

```go
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRingCasterLapAccountingAndResume(t *testing.T) {
	rc := NewRingCaster[int](4)
	live := rc.Subscribe(new(RingSub[int]))
	for i := range 10 {
		rc.Publish(i)
	}

	// Started at 0 and capacity is 4, so 0..7 were overwritten.
	if v, ok := live.TryRecv(); !ok || v != 8 || live.Dropped() != 8 {
		t.Fatalf("lapped: got (%d,%v) dropped=%d, want (8,true) dropped=8", v, ok, live.Dropped())
	}
	if v, ok := live.TryRecv(); !ok || v != 9 {
		t.Fatalf("got (%d,%v), want (9,true)", v, ok)
	}
	if _, ok := live.TryRecv(); ok || live.Lag() != 0 {
		t.Fatal("expected drained subscriber")
	}

	// Resume: the oldest retained message is 6.
	r := rc.SubscribeFrom(new(RingSub[int]), 0)
	if v, ok := r.TryRecv(); !ok || v != 6 || r.Dropped() != 0 {
		t.Fatalf("resume: got (%d,%v) dropped=%d", v, ok, r.Dropped())
	}

	// Conflation: jump straight to the newest message.
	c := rc.SubscribeFrom(new(RingSub[int]), 0)
	if v, ok := c.TryRecvLatest(); !ok || v != 9 || c.Dropped() != 0 {
		t.Fatalf("latest: got (%d,%v) dropped=%d", v, ok, c.Dropped())
	}
	if _, ok := c.TryRecvLatest(); ok {
		t.Fatal("latest: expected nothing after conflation")
	}
}

func TestRingCasterLosslessWhenKeepingUp(t *testing.T) {
	const total, nsubs = 20000, 8
	rc := NewRingCaster[int](1 << 15) // larger than total, so no lapping
	subs := make([]*RingSub[int], nsubs)
	for i := range subs {
		subs[i] = rc.Subscribe(new(RingSub[int]))
	}
	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for want := range total {
				v, err := s.Recv(context.Background())
				if err != nil || v != want {
					t.Errorf("want %d, got %d, err %v", want, v, err)
					return
				}
			}
		}()
	}
	for i := range total {
		rc.Publish(i)
	}
	wg.Wait()
	for _, s := range subs {
		if s.Dropped() != 0 {
			t.Fatalf("unexpected drops: %d", s.Dropped())
		}
	}
}

func TestRingCasterSelectIntegration(t *testing.T) {
	rc := NewRingCaster[string](8)
	s := rc.Subscribe(new(RingSub[string]))
	quit := make(chan struct{})
	defer close(quit)
	got := make(chan string, 1)
	go func() {
		for {
			if v, ok := s.TryRecv(); ok {
				got <- v
				return
			}
			wake := s.Arm()
			if v, ok := s.TryRecv(); ok {
				got <- v
				return
			}
			select {
			case <-wake:
			case <-quit:
				return
			}
		}
	}()
	time.Sleep(10 * time.Millisecond) // usually lets the goroutine park first
	rc.Publish("hello")
	select {
	case v := <-got:
		if v != "hello" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("select integration: timed out")
	}
}

func TestRingCasterCloseDrainsThenEnds(t *testing.T) {
	rc := NewRingCaster[int](8)
	s := rc.Subscribe(new(RingSub[int]))
	rc.Publish(1)
	rc.Publish(2)
	ctx := context.Background()
	for want := 1; want <= 2; want++ {
		if v, err := s.Recv(ctx); err != nil || v != want {
			t.Fatalf("got (%d,%v), want %d", v, err, want)
		}
	}

	parked := make(chan error, 1)
	go func() {
		_, err := s.Recv(ctx)
		parked <- err
	}()
	time.Sleep(10 * time.Millisecond)
	rc.Close()
	select {
	case err := <-parked:
		if !errors.Is(err, ErrRingClosed) {
			t.Fatalf("parked Recv: got %v, want ErrRingClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parked Recv not woken by Close")
	}
	if !s.Ended() {
		t.Fatal("expected Ended")
	}
	rc.Close() // idempotent
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on publish after close")
			}
		}()
		rc.Publish(3)
	}()
}

func TestRingCasterLockstepNoLostWakeups(t *testing.T) {
	const total = 50000
	rc := NewRingCaster[int](4)
	s := rc.Subscribe(new(RingSub[int]))
	ack := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for want := range total {
			v, err := s.Recv(context.Background())
			if err != nil || v != want {
				done <- fmt.Errorf("want %d: got %d, err %v", want, v, err)
				return
			}
			ack <- struct{}{}
		}
		done <- nil
	}()
	for i := range total {
		rc.Publish(i)
		select {
		case <-ack:
		case err := <-done:
			t.Fatalf("subscriber stopped at %d: %v", i, err)
		}
	}
}

func TestRingCasterAccountingUnderConcurrency(t *testing.T) {
	const N = 300000
	rc := NewRingCaster[int](1 << 10) // small enough to lap regularly
	s := rc.Subscribe(new(RingSub[int]))
	go func() {
		for i := range N {
			rc.Publish(i)
		}
		rc.Close()
	}()
	count, last := 0, -1
	for {
		v, err := s.Recv(context.Background())
		if errors.Is(err, ErrRingClosed) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if v <= last {
			t.Fatalf("out of order: %d after %d", v, last)
		}
		last = v
		count++
	}
	if uint64(count)+s.Dropped() != N {
		t.Fatalf("conservation: received %d + dropped %d != %d", count, s.Dropped(), N)
	}
	if last != N-1 {
		t.Fatalf("final message %d, want %d", last, N-1)
	}
}

func TestRingCasterAllocations(t *testing.T) {
	rc := NewRingCaster[int](64)
	// A lap of publishes with no parked subscribers allocates exactly one slab.
	if a := testing.AllocsPerRun(100, func() {
		for i := range 64 {
			rc.Publish(i)
		}
	}); a != 1 {
		t.Fatalf("publish: %v allocations per lap, want 1", a)
	}

	// Draining with TryRecv is allocation-free.
	pre := NewRingCaster[int](1024)
	for i := range 1024 {
		pre.Publish(i)
	}
	s := pre.SubscribeFrom(new(RingSub[int]), 0)
	if a := testing.AllocsPerRun(500, func() { s.TryRecv() }); a != 0 {
		t.Fatalf("TryRecv: %v allocations, want 0", a)
	}
}
```

---

## Notes and next steps

- **Verification.** I reasoned through the memory-ordering and lap arguments above, but the code has not been compiled or run. Run `go test -race -count=20 -timeout 120s` first. The lockstep and accounting tests are the ones most likely to expose a mistake.
- **Benchmarks to run** before making performance claims: publish cost with 0, 1, 64, and 4096 idle subscribers; a saturated reader (no parking); a ping-pong reader (always parking); subscribe/unsubscribe churn; and the same matrix against `ChanCaster` and a callback fan-out. My prediction is that the saturated and churn rows favour `RingCaster` strongly, and that the ping-pong row is closest to `ChanCaster`. That prediction is untested.
- **Known improvement.** In the ping-pong regime there is one wake channel allocation per publish. A `PublishBatch(vs ...V)` would amortise both the lock and the wake across a batch, which is a natural next step.
- **Scope.** The new file uses only `atomic` and `sync` types, and nothing in the existing package. It requires Go 1.23 or later, the same as the rest of the module, because it uses `min` and the existing code uses range-over-int and `iter`.
