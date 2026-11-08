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

// Provenance: 12-openrouter-crouton-alpha-chat.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

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
	var started atomic.Bool
	return func(yield func(V) bool) {
		if yield == nil {
			stop()
			sub.Close()
			panic(`bigbuff: chanfanout: iterator yield func is nil`)
		}
		if started.Swap(true) || ctx.Err() != nil {
			stop()
			sub.Close()
			return
		}
		defer stop()
		defer sub.Close()
		for {
			if ctx.Err() != nil {
				return
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
