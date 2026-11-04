Evolving your alloc-free caster to its ultimate form — first mapping the repo, then designing a novel, scalable successor.

Delivered as a new, complementary implementation rather than a rewrite of the existing two files — `ChanCaster` (zero-alloc, but fungible receivers steal/miss and the sender performs N blocking handoffs) and `ChanPubSub` (exactly-once per generation, but via a global `sync.Cond` barrier that must be `Wait()`ed, with the sender still doing N handoffs and waiting on the slowest subscriber) are both *verified-correct* and worth keeping. The new type below attacks the structural problem they share: **delivery and notification are coupled through the sender's sequential channel sends.**

## The novel idea: decouple delivery from notification

`ChanFanout` splits fanout into two independent mechanisms:

1. **Delivery = pull from a shared ring by per-subscriber cursor.** Values are published once into a fixed power-of-two ring of immutable envelopes (1 atomic slot store). Each subscriber holds only a `uint64` cursor and pulls values in order, independently — the same independent-consumer-group model as SPMC broadcast queues / Disruptor-style rings / Kafka consumer groups, brought into idiomatic Go generics. Consequences: publish is **O(1) regardless of N**, `Send` **never blocks on subscribers** (slow-subscriber isolation is total), churn is **O(1)** (no slice removal, no per-subscriber channel), memory is **bounded O(capacity)**, and if a subscriber lags more than `capacity`, the loss is *explicit* — `TryRecv`/`Recv` return a `skipped` count so the application can repair/resync, instead of silently stealing or blocking everyone.
2. **Notification = close-broadcast of a generation channel.** Each `Send` swaps in a fresh generation channel and closes the previous one. Unlike a send (wakes exactly one receiver), closing wakes *all* waiters — a true broadcast wakeup in O(1), at a cost of exactly one channel allocation per generation, independent of N. Combined with a pre-closed singleton channel returned by `Ready()` when a value is already pending, this gives full `select` integration **without per-subscriber channels**.

The missed-wakeup race is closed by a strict ordering invariant: `Ready()` loads the generation channel *before* checking "pending", and `Send` publishes (slot → head → swap → close last). Either the subscriber's channel gets closed, or its pending-check observes the new head — never neither.

Allocation profile is exact and predictable: **Send = 3 small allocs** (envelope, generation holder, generation channel), **Recv/TryRecv/Ready = 0 allocs**, **Subscribe = 2 allocs**, memory bounded by ring capacity. `capacity == 1` degenerates gracefully to a latest-value cell.

```txtar
-- chanfanout.go --
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
	"math/bits"
	"sync"
	"sync/atomic"
)

// fanoutDefaultCapacity is the ring capacity used when NewChanFanout is
// given a capacity < 1. It should comfortably exceed the maximum lag
// (number of published-but-unread values) you expect of any subscriber;
// subscribers that fall further behind skip the overwritten values, with
// the number skipped reported by FanoutSub.TryRecv and FanoutSub.Recv.
const fanoutDefaultCapacity = 1024

// fanoutClosedChan is a pre-closed channel, returned by FanoutSub.Ready
// when a receive may already succeed (or can never succeed), so that a
// select statement involving Ready never blocks spuriously. It is a
// package-level singleton: it is never sent on, and must never be closed.
var fanoutClosedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// fanoutEnv is a single published value together with its sequence number.
// Instances are immutable after publication: the producer fully constructs
// an env, then publishes it with an atomic store into a ring slot.
// Consumers load the slot pointer atomically, and may then safely read
// seq and value (the atomic store/load pair establishes happens-before).
type fanoutEnv[V any] struct {
	seq   uint64
	value V
}

// fanoutGen is one generation of the wakeup channel. Each Send swaps in a
// fresh fanoutGen and closes the previous generation's channel, which wakes
// every subscriber currently selecting on it. Closing a channel broadcasts
// to all receivers, unlike a send, which wakes exactly one; this is what
// makes notification O(1) in the number of subscribers.
type fanoutGen struct {
	ch chan struct{}
}

// ChanFanout is a scalable, non-blocking fanout: one or more senders
// publish values, and any number of subscribers independently pull every
// published value, in publish order, from a shared fixed-size ring buffer,
// using an independent per-subscriber cursor.
//
// Unlike [ChanCaster] and [ChanPubSub], delivery does not go through the
// sender's sequential channel sends, and unlike a per-subscriber-channel
// ("callback channel") design, subscribers do not each own a channel.
// Delivery (pull from the ring) and notification (close-broadcast of a
// per-generation channel) are decoupled, which yields:
//
//   - Send is O(1) and never blocks on subscribers (it only briefly
//     serializes concurrent senders against each other). There is no
//     head-of-line blocking: a slow or stalled subscriber cannot delay
//     publication, other subscribers, or the next Send.
//   - Each subscriber has independent progress. A fast subscriber cannot
//     steal values from a slow one, and a slow one cannot lose values to
//     a fast one, unless it falls more than the ring capacity behind.
//   - Churn is O(1): subscribing and unsubscribing are single atomic
//     operations, with no locks, no slice shifting, and no channel
//     allocation per subscriber (beyond the subscriber itself and its
//     done channel).
//   - Subscribers integrate with select via [FanoutSub.Ready], without
//     owning a channel and without the O(N) notification cost of
//     per-subscriber channels. See [FanoutSub.Ready] for the pattern.
//
// # Semantics
//
//   - A subscriber starts at the most recently published value: history
//     published before Subscribe is not replayed (standard pub/sub).
//   - Values are delivered in publish order, exactly once, unless the
//     subscriber falls more than the ring capacity behind, in which case
//     the overwritten values are skipped and the number skipped is
//     reported (see [FanoutSub.Recv]). Use [FanoutSub.Lag] to observe
//     lag, [FanoutSub.SkipToLatest] to resynchronize explicitly, and
//     choose the capacity accordingly. For loss-free delivery regardless
//     of lag, use [ChanPubSub] instead; [ChanFanout] trades that for
//     isolation and bounded memory.
//   - Values sent while there are no subscribers are not published
//     (consistent with [ChanPubSub.Send] returning 0 with no subscribers).
//   - Close drains: after Close, subscribers still receive every value
//     published before the close, and only then does Recv report done.
//     Send and Subscribe after Close panic, as does sending on a closed
//     channel. Close is idempotent.
//   - A capacity of 1 degenerates to a latest-value cell: subscribers
//     only ever observe the most recent value.
//
// # Concurrency
//
//   - ChanFanout is safe for concurrent use. Multiple concurrent senders
//     are serialized internally; each Send is one atomic generation.
//   - A FanoutSub must be used by a single goroutine at a time (like a
//     channel or an iterator), except that [FanoutSub.Close] may be
//     called from another goroutine, e.g. to unblock a pending Recv.
//
// # Allocation
//
// Send performs exactly three small allocations (envelope, generation
// holder, generation channel), independent of the number of subscribers.
// Recv, TryRecv and Ready perform zero allocations. Subscribe performs
// two (the subscriber and its done channel). Total memory is bounded by
// O(capacity) ring slots plus at most capacity live envelopes.
//
// Requires Go 1.23 or later (iter.Seq2, typed atomics).
type ChanFanout[V any] struct {
	// slots is the ring buffer of published envelopes. len(slots) is
	// always a power of two, so seq & mask indexes it. Slots are written
	// only by Send (under sendMu) and read by subscribers; each slot is
	// an atomic pointer to an immutable env, so reads are lock-free and
	// race-detector clean (no seqlocks, no torn reads of V).
	slots []atomic.Pointer[fanoutEnv[V]]
	// mask is len(slots)-1, for seq & mask ring indexing.
	mask uint64
	// head is the sequence number of the most recently published value
	// (0 if none). It is stored after the slot store, so any subscriber
	// that observes head >= seq also observes that seq's env.
	head atomic.Uint64
	// gen is the current wakeup generation. Send swaps in a fresh
	// generation and closes the old one, waking all waiters.
	gen atomic.Pointer[fanoutGen]
	// subscribers is the number of live FanoutSub instances. It is only
	// used for Send's return value, its no-subscribers fast path, and
	// Subscribers; it is never on the delivery path.
	subscribers atomic.Int32
	// closed is set by Close. After Close, Send and Subscribe panic, and
	// subscribers drain any remaining values before Recv reports done.
	closed atomic.Bool
	// done is closed by Close, waking any blocked Recv calls.
	done chan struct{}
	// sendMu serializes Send (and Close), making publication a single
	// atomic step even with multiple senders. It is held only briefly:
	// one slot store, one head store, one channel swap and close.
	sendMu sync.Mutex
}

// NewChanFanout constructs a ChanFanout whose ring holds up to capacity
// published values per subscriber lag. The capacity is rounded up to the
// next power of two; a capacity < 1 selects fanoutDefaultCapacity.
//
// The type parameter must be given explicitly, e.g.
//
//	fanout := NewChanFanout[int](1024)
func NewChanFanout[V any](capacity int) *ChanFanout[V] {
	if capacity < 1 {
		capacity = fanoutDefaultCapacity
	}
	size := 1 << bits.Len(uint(capacity-1))
	b := &ChanFanout[V]{
		slots: make([]atomic.Pointer[fanoutEnv[V]], size),
		mask:  uint64(size - 1),
		done:  make(chan struct{}),
	}
	b.gen.Store(&fanoutGen{ch: make(chan struct{})})
	return b
}

// Send publishes value to all subscribers, in publish order, and returns
// the number of subscribers registered at the moment of publication.
// It never blocks on subscribers: a subscriber that has fallen more than
// the ring capacity behind will skip the overwritten values (reported by
// its Recv/TryRecv), without affecting anyone else.
//
// If there are no subscribers, the value is not published and Send
// returns 0, performing no allocations. Send panics if the ChanFanout
// has been closed.
func (b *ChanFanout[V]) Send(value V) int {
	if b.closed.Load() {
		panic(`bigbuff: chanfanout: send on closed ChanFanout`)
	}
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	if b.closed.Load() {
		panic(`bigbuff: chanfanout: send on closed ChanFanout`)
	}
	n := b.subscribers.Load()
	if n == 0 {
		return 0 // no subscribers: do not publish (pub/sub semantics)
	}
	// Publish order matters for the wakeup invariant: slot store, then
	// head store, then generation swap + close. A subscriber that loads
	// the new generation necessarily observes the new head (Ready checks
	// pending after loading the generation); a subscriber that loads the
	// old generation is woken by its close.
	seq := b.head.Load() + 1
	b.slots[seq&b.mask].Store(&fanoutEnv[V]{seq: seq, value: value})
	b.head.Store(seq)
	old := b.gen.Swap(&fanoutGen{ch: make(chan struct{})})
	close(old.ch) // broadcast wakeup to all current waiters
	return int(n)
}

// Subscribe registers a new subscriber, which will receive values
// published after this call, in publish order. It panics if the
// ChanFanout has been closed. The returned FanoutSub must eventually be
// closed (see [FanoutSub.Close]) to release its registration.
//
// Note on linearization: a Send concurrent with Subscribe may be ordered
// either before or after the subscription; the subscriber either receives
// the in-flight value or does not, but never observes a partial publish.
func (b *ChanFanout[V]) Subscribe() *FanoutSub[V] {
	if b.closed.Load() {
		panic(`bigbuff: chanfanout: subscribe on closed ChanFanout`)
	}
	b.subscribers.Add(1)
	return &FanoutSub[V]{
		b:      b,
		done:   make(chan struct{}),
		cursor: b.head.Load(), // start at the latest published value
	}
}

// SubscribeContext subscribes immediately and returns a single-use
// iterator yielding every value published after the call, in order,
// automatically unsubscribing when the context is cancelled, the
// iterator stops early, or the ChanFanout is closed (after draining).
// Behavior is undefined if the iterator is used more than once.
//
// WARNING: the returned iterator MUST be used immediately, OR the
// context MUST be promptly cancelled, to avoid leaking the subscription.
func (b *ChanFanout[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := b.Subscribe() // panics if closed; subscribes synchronously
	if ctx == nil {
		ctx = context.Background()
	}
	// Unsubscribe even if the iterator is never called. Closing the sub
	// also unblocks any Recv in progress (via the sub's done channel).
	stop := context.AfterFunc(ctx, sub.Close)
	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				sub.Close()
			}
			panic(`bigbuff: chanfanout: iterator yield func is nil`)
		}
		if !stop() {
			// Context already cancelled (AfterFunc has run or will run
			// and close sub), or the iterator has already been used.
			return
		}
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			value, _, ok := sub.Recv()
			if !ok {
				return
			}
			if !yield(value) {
				return
			}
		}
	}
}

// Subscribers returns the number of currently registered subscribers.
func (b *ChanFanout[V]) Subscribers() int {
	return int(b.subscribers.Load())
}

// Capacity returns the actual ring capacity (the requested capacity
// rounded up to a power of two). A subscriber that falls more than this
// many values behind will skip overwritten values.
func (b *ChanFanout[V]) Capacity() int {
	return len(b.slots)
}

// Latest returns the sequence number of the most recently published
// value, or 0 if none has been published. Sequence numbers start at 1
// and increase by 1 per published Send; they are useful for observing
// publication progress and for correlating with FanoutSub.Lag.
func (b *ChanFanout[V]) Latest() uint64 {
	return b.head.Load()
}

// Done returns a channel that is closed by Close. It is intended for
// select statements that wait on subscriber readiness, e.g.
//
//	select {
//	case <-sub.Ready():
//		v, _, ok := sub.TryRecv()
//		...
//	case <-fanout.Done():
//		// hub closed
//	case other: // ...
//	}
func (b *ChanFanout[V]) Done() <-chan struct{} {
	return b.done
}

// Closed reports whether Close has been called.
func (b *ChanFanout[V]) Closed() bool {
	return b.closed.Load()
}

// Close shuts down the ChanFanout: it wakes all blocked Recv calls,
// after which subscribers drain any remaining published values before
// Recv reports done. Send and Subscribe panic after Close. Close is
// idempotent. It does not unsubscribe existing subscribers; each
// FanoutSub should still be closed by its owner.
func (b *ChanFanout[V]) Close() {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	if b.closed.CompareAndSwap(false, true) {
		close(b.done)
	}
}

// FanoutSub is a single subscriber's view of a ChanFanout. It owns an
// independent cursor into the shared ring, so its progress is unaffected
// by other subscribers (and theirs by it).
//
// A FanoutSub must be used by a single goroutine at a time, except that
// Close may be called from another goroutine (e.g. from
// context.AfterFunc) to unblock a pending Recv. Once closed, TryRecv and
// Recv report done, and any undelivered values are dropped.
type FanoutSub[V any] struct {
	b      *ChanFanout[V]
	done   chan struct{}
	closed atomic.Bool
	// cursor is the sequence number of the last value received by this
	// subscriber (0 if none). It is only ever accessed by the owning
	// goroutine, so it needs no atomicity of its own.
	cursor uint64
}

// Ready returns a channel that fires when a call to TryRecv may succeed.
// It is designed for use in a select statement:
//
//	for {
//		select {
//		case <-sub.Ready():
//			v, skipped, ok := sub.TryRecv()
//			if !ok {
//				continue // spurious wakeup; re-check Ready
//			}
//			// use v; skipped values were overwritten (lag > capacity)
//		case <-fanout.Done():
//			return // hub closed
//		case other: // ...
//		}
//	}
//
// Ready never blocks spuriously for long: if a value is already pending
// for this subscriber (or the subscriber/hub is closed), it returns a
// pre-closed channel that fires immediately. After Ready fires, always
// check TryRecv's ok result: select chooses pseudo-randomly among ready
// cases, and another... (for this subscriber, no other goroutine may
// consume, but a spurious wakeup is still possible if the value was
// already consumed in a previous iteration).
//
// The missed-wakeup invariant: Ready loads the current generation
// channel before checking whether a value is pending. Send publishes
// (slot, head) before swapping and closing the generation, so either
// this call observes the pending value, or it returns a channel that
// Send is guaranteed to close. It performs no allocation.
func (s *FanoutSub[V]) Ready() <-chan struct{} {
	b := s.b
	g := b.gen.Load() // load generation BEFORE checking pending
	if s.closed.Load() || b.closed.Load() || b.head.Load() > s.cursor {
		return fanoutClosedChan
	}
	return g.ch
}

// TryRecv returns the next published value for this subscriber, without
// blocking. It reports ok == false if no value is currently available
// (or the subscriber has been closed). If the subscriber had fallen more
// than the ring capacity behind, the overwritten values are skipped and
// skipped reports how many; the returned value is the oldest value still
// in the ring. It performs no allocation.
func (s *FanoutSub[V]) TryRecv() (value V, skipped int, ok bool) {
	if s.closed.Load() {
		return value, 0, false
	}
	b := s.b
	next := s.cursor + 1
	if b.head.Load() < next {
		return value, 0, false // caught up
	}
	// head >= next implies seq == next has been published, and its slot
	// store happened-before the head store, so the slot holds that env
	// or a later cycle of it (seq > next, if overwritten). The nil and
	// seq < next cases are therefore unreachable; they are checked
	// defensively rather than trusted blindly.
	e := b.slots[next&b.mask].Load()
	if e == nil || e.seq < next {
		return value, 0, false
	}
	if e.seq != next {
		skipped = int(e.seq - next) // overwritten while lagging
	}
	s.cursor = e.seq
	return e.value, skipped, true
}

// Recv blocks until the next published value is available for this
// subscriber, the subscriber is closed, or the ChanFanout is closed
// (after draining all values published before the close). See TryRecv
// for the meaning of skipped. It performs no allocation.
func (s *FanoutSub[V]) Recv() (value V, skipped int, ok bool) {
	b := s.b
	for {
		if s.closed.Load() {
			return value, 0, false
		}
		if v, sk, ok := s.TryRecv(); ok {
			return v, sk, true
		}
		if b.closed.Load() {
			return value, 0, false
		}
		select {
		case <-s.Ready():
			// Spurious or real wakeup; loop to TryRecv.
		case <-s.done:
			return value, 0, false
		case <-b.done:
			// Hub closed; loop to drain via TryRecv, then exit.
		}
	}
}

// Lag returns the number of published values this subscriber has not yet
// received, at the moment of the call. It is an approximation under
// concurrent sends (the head may advance immediately after it is read),
// but it is exact in the absence of concurrent sends.
func (s *FanoutSub[V]) Lag() int {
	if head := s.b.head.Load(); head > s.cursor {
		return int(head - s.cursor)
	}
	return 0
}

// SkipToLatest drops all currently pending values for this subscriber
// and returns how many were dropped. The next Recv will return the next
// value published after this call. It is equivalent to calling Recv and
// discarding the results, but O(1) and without delivering anything.
func (s *FanoutSub[V]) SkipToLatest() int {
	head := s.b.head.Load()
	if head <= s.cursor {
		return 0
	}
	dropped := int(head - s.cursor)
	s.cursor = head
	return dropped
}

// Seq returns an iterator yielding (value, skipped) pairs, as from
// Recv, until Recv would report done (hub closed and drained, or the
// subscriber closed). Unlike ChanFanout.SubscribeContext, Seq does not
// unsubscribe: the caller retains ownership of the subscriber and must
// still call Close when finished.
func (s *FanoutSub[V]) Seq() iter.Seq2[V, int] {
	return func(yield func(V, int) bool) {
		if yield == nil {
			panic(`bigbuff: chanfanout: iterator yield func is nil`)
		}
		for {
			value, skipped, ok := s.Recv()
			if !ok {
				return
			}
			if !yield(value, skipped) {
				return
			}
		}
	}
}

// Close unsubscribes the subscriber and unblocks any pending Recv by
// this subscriber (including one running in another goroutine). It is
// idempotent. After Close, TryRecv and Recv report done, and undelivered
// values are dropped.
func (s *FanoutSub[V]) Close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.done)
		s.b.subscribers.Add(-1)
	}
}

// Unsubscribe is an alias for Close.
func (s *FanoutSub[V]) Unsubscribe() {
	s.Close()
}

-- chanfanout_test.go --
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
	"testing"
)

func TestChanFanoutBasic(t *testing.T) {
	// Capacity comfortably exceeds the number of sends, so no scheduling
	// delay can cause drops: every subscriber must see every value.
	b := NewChanFanout[int](4096)
	const subs = 8
	const sends = 100

	created := make([]*FanoutSub[int], subs)
	for i := range created {
		created[i] = b.Subscribe()
		defer created[i].Close()
	}

	var wg sync.WaitGroup
	for _, sub := range created {
		wg.Add(1)
		go func(sub *FanoutSub[int]) {
			defer wg.Done()
			for want := 1; want <= sends; want++ {
				value, skipped, ok := sub.Recv()
				if !ok {
					t.Errorf("Recv: ok == false at want %d", want)
					return
				}
				if skipped != 0 {
					t.Errorf("Recv: skipped == %d at want %d, want 0", skipped, want)
					return
				}
				if value != want {
					t.Errorf("Recv: value == %d, want %d", value, want)
					return
				}
			}
		}(sub)
	}

	for i := 1; i <= sends; i++ {
		if n := b.Send(i); n != subs {
			t.Fatalf("Send returned %d, want %d", n, subs)
		}
	}
	wg.Wait()
	if n := b.Subscribers(); n != subs {
		t.Fatalf("Subscribers == %d, want %d", n, subs)
	}
}

func TestChanFanoutSelect(t *testing.T) {
	b := NewChanFanout[string](16)
	sub := b.Subscribe()
	defer sub.Close()
	defer b.Close()

	go func() { b.Send("hello") }()

	select {
	case <-sub.Ready():
		value, skipped, ok := sub.TryRecv()
		if !ok || value != "hello" || skipped != 0 {
			t.Fatalf("TryRecv == (%q, %d, %v), want (%q, 0, true)", value, skipped, ok, "hello")
		}
	case <-b.Done():
		t.Fatal("hub closed unexpectedly")
	}

	// Nothing pending now: Ready must not fire.
	select {
	case <-sub.Ready():
		t.Fatal("Ready fired with no value published")
	default:
	}
}

func TestChanFanoutSlowSubscriberDrops(t *testing.T) {
	b := NewChanFanout[int](8)
	slow := b.Subscribe()
	defer slow.Close()
	fast := b.Subscribe()
	defer fast.Close()

	// Interleave send and fast-read in one goroutine: fast never lags,
	// slow never reads. Fully deterministic, no scheduling involved.
	for i := 1; i <= 100; i++ {
		b.Send(i)
		value, skipped, ok := fast.TryRecv()
		if !ok || value != i || skipped != 0 {
			t.Fatalf("fast TryRecv == (%d, %d, %v), want (%d, 0, true)", value, skipped, ok, i)
		}
	}

	// Ring capacity is 8, so the slot for seq 1 now holds seq 97
	// (the latest seq == 1 mod 8 that is <= 100). Slow must be resynced
	// to 97 with 96 skipped.
	value, skipped, ok := slow.TryRecv()
	if !ok || value != 97 || skipped != 96 {
		t.Fatalf("slow first TryRecv == (%d, %d, %v), want (97, 96, true)", value, skipped, ok)
	}
	for want := 98; want <= 100; want++ {
		value, skipped, ok = slow.TryRecv()
		if !ok || value != want || skipped != 0 {
			t.Fatalf("slow TryRecv == (%d, %d, %v), want (%d, 0, true)", value, skipped, ok, want)
		}
	}
	if _, _, ok := slow.TryRecv(); ok {
		t.Fatal("slow TryRecv after draining: ok == true, want false")
	}
}

func TestChanFanoutLagAndSkip(t *testing.T) {
	b := NewChanFanout[int](64)
	sub := b.Subscribe()
	defer sub.Close()

	for i := 1; i <= 10; i++ {
		b.Send(i)
	}
	if lag := sub.Lag(); lag != 10 {
		t.Fatalf("Lag == %d, want 10", lag)
	}
	if dropped := sub.SkipToLatest(); dropped != 10 {
		t.Fatalf("SkipToLatest == %d, want 10", dropped)
	}
	if lag := sub.Lag(); lag != 0 {
		t.Fatalf("Lag after skip == %d, want 0", lag)
	}
	b.Send(11)
	value, skipped, ok := sub.TryRecv()
	if !ok || value != 11 || skipped != 0 {
		t.Fatalf("TryRecv == (%d, %d, %v), want (11, 0, true)", value, skipped, ok)
	}
}

func TestChanFanoutClose(t *testing.T) {
	b := NewChanFanout[int](16)
	sub := b.Subscribe()
	defer sub.Close()

	for i := 1; i <= 5; i++ {
		b.Send(i)
	}
	b.Close()

	// Close drains: all five values remain deliverable.
	for want := 1; want <= 5; want++ {
		value, _, ok := sub.Recv()
		if !ok || value != want {
			t.Fatalf("drain Recv == (%d, %v), want (%d, true)", value, ok, want)
		}
	}
	if _, _, ok := sub.Recv(); ok {
		t.Fatal("Recv after drain: ok == true, want false")
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Send after Close did not panic")
			}
		}()
		b.Send(6)
	}()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Subscribe after Close did not panic")
			}
		}()
		b.Subscribe()
	}()

	// Idempotency.
	b.Close()
	sub.Close()
	sub.Close()
	sub.Unsubscribe()
}

func TestChanFanoutConcurrentChurn(t *testing.T) {
	b := NewChanFanout[int](8192)
	const sends = 2000

	// Readers subscribe in the main goroutine so they are guaranteed to
	// be registered before the hub is closed.
	const readers = 4
	readerSubs := make([]*FanoutSub[int], readers)
	for i := range readerSubs {
		readerSubs[i] = b.Subscribe()
	}

	var readerWg sync.WaitGroup
	for _, sub := range readerSubs {
		readerWg.Add(1)
		go func(sub *FanoutSub[int]) {
			defer sub.Close()
			defer readerWg.Done()
			last := 0
			for {
				value, _, ok := sub.Recv()
				if !ok {
					return
				}
				if value <= last {
					t.Errorf("values out of order: %d after %d", value, last)
					return
				}
				last = value
			}
		}(sub)
	}

	// Churn: short-lived subscribers hammering Subscribe/TryRecv/Close.
	sending := make(chan struct{})
	var churnWg sync.WaitGroup
	for i := 0; i < 2; i++ {
		churnWg.Add(1)
		go func() {
			defer churnWg.Done()
			for {
				select {
				case <-sending:
					return
				default:
				}
				sub := b.Subscribe()
				sub.TryRecv()
				sub.Close()
			}
		}()
	}

	for i := 1; i <= sends; i++ {
		b.Send(i)
	}
	close(sending)
	churnWg.Wait() // no Subscribe can race with Close below
	b.Close()
	readerWg.Wait()
}

func TestChanFanoutSubscribeContext(t *testing.T) {
	b := NewChanFanout[int](4096)
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	seq := b.SubscribeContext(ctx) // subscribes synchronously, in this goroutine

	var got []int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range seq {
			got = append(got, v)
			if len(got) == 10 {
				cancel()
			}
		}
	}()

	for i := 1; i <= 100; i++ {
		b.Send(i)
	}
	wg.Wait()

	if len(got) != 10 {
		t.Fatalf("received %d values, want 10", len(got))
	}
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("got[%d] == %d, want %d", i, v, i+1)
		}
	}
}

func BenchmarkChanFanoutSend(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	sub := fanout.Subscribe()
	defer sub.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fanout.Send(i)
	}
}

func BenchmarkChanFanoutSendNoSubscribers(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := fanout.Send(i); n != 0 {
			b.Fatalf("Send returned %d, want 0", n)
		}
	}
}

func BenchmarkChanFanoutSendAndTryRecv(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	sub := fanout.Subscribe()
	defer sub.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fanout.Send(i)
		if _, _, ok := sub.TryRecv(); !ok {
			b.Fatal("TryRecv failed")
		}
	}
}

func BenchmarkChanFanoutSubscribeClose(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub := fanout.Subscribe()
		sub.Close()
	}
}
```

## How to validate

Drop the two files into `/Users/joeyc/dev/go-bigbuff` (Go ≥ 1.23 required for `iter.Seq2`), then:

```sh
go vet ./... && go test -race -run ChanFanout -count=1 -timeout 60s ./...
go test -bench ChanFanout -benchmem -benchtime=2000000x ./...
```

Expected benchmark shape: `Send`/`SubscribeClose` are independent of subscriber count (vary the subscriber count in `BenchmarkChanFanoutSend` to confirm), and allocs/op should read 3 for `Send`, 2 for `SubscribeClose`, 0 for the `TryRecv` portion.

## Positioning (which implementation when)

| | `ChanCaster` | `ChanPubSub` | `ChanFanout` (new) |
|---|---|---|---|
| Delivery | N sequential handoffs on your channel | N handoffs + barrier | O(1) ring publish, subscribers pull |
| Slow subscriber | Blocks sender; fast receivers steal, slow miss | Blocks next `Send` until it `Wait()`s | Isolated; skips only beyond `capacity`, with count reported |
| `select` | Native (your channel) | Native + mandatory `Wait()` | Via `Ready()`/`Done()`, no per-sub channel |
| Churn | O(1) count, but unsubscribing mid-`Send` drains a receive | O(1) count, Cond wakeups per generation | O(1), lock-free |
| Memory | O(1) | O(1) | Bounded O(capacity) |
| Alloc/send | 0 | ~0 | 3, independent of N |

Honest tradeoff: `ChanFanout` is deliberately *lossy beyond lag* — that is the price for total slow-subscriber isolation plus bounded memory (exact + isolated + bounded is impossible). If you need loss-free streams, `capacity` should exceed your worst expected lag (observe it with `Lag()` in production); if you need *guaranteed* loss-free delivery, `ChanPubSub` remains the right tool.

Natural future evolutions I deliberately did **not** touch (to avoid destabilising verified code): (1) replacing `ChanPubSub`'s `sync.Cond` pong-barrier with the same generation close-broadcast used here, which would make `Wait()` selectable and remove the thundering herd while preserving its exact semantics; (2) a `Notify()`/selectable variant of `ChanCaster.Send`'s completion.
