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
	"sync"
)

// chanFeedClosedChan is a package-level, always-closed channel. It is returned
// by [ChanFeedSub.Ready] to signal immediate readiness without allocating.
var chanFeedClosedChan = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// ChanFeedMode selects the backpressure policy for a [ChanFeed].
type ChanFeedMode int

const (
	// ChanFeedDropOldest is the default. When the ring buffer is full, the
	// oldest value is overwritten. Subscribers that lag more than capacity
	// values behind the head observe gaps; the number of skipped values is
	// reflected in [ChanFeedSub.Dropped].
	ChanFeedDropOldest ChanFeedMode = iota

	// ChanFeedBlock causes [ChanFeed.Send] to block when the ring buffer is
	// full, until the slowest subscriber has consumed at least one value (or
	// unsubscribed). Guarantees delivery to any subscriber that keeps its lag
	// below capacity.
	ChanFeedBlock
)

// ChanFeed is an asynchronous, bounded, broadcast feed for values, with
// independent per-subscriber consumption, arbitrarily many concurrent
// subscribers, fast subscription churn, and seamless integration with select
// statements.
//
// # Design and Properties
//
// Unlike [ChanCaster] and [ChanPubSub], which use a single unbuffered channel
// as their transport, [ChanFeed] uses a shared fixed-size ring buffer of
// values plus a single broadcast "notify" channel that is closed and replaced
// on each state transition. Subscribers are lightweight cursors into the
// ring, woken by the shared notify channel (so a broadcast wakeup is a single
// channel close, not N sends), and pull values independently.
//
// This yields the following characteristics:
//
//   - O(1) send-side cost independent of subscriber count.
//   - O(1) subscribe and unsubscribe, scaling with consumer churn.
//   - Bounded memory: the ring is fixed at construction.
//   - Select-statement support via [ChanFeedSub.Ready].
//   - Predictable allocation: one small channel per Send that has subscribers,
//     one struct per subscription.
//   - Either synchronous-style backpressure ([ChanFeedBlock]) or best-effort
//     drop-oldest delivery ([ChanFeedDropOldest]).
//
// # Trade-off vs. ChanPubSub
//
// [ChanPubSub] synchronously confirms that every subscriber has *received* a
// value before [ChanPubSub.Send] returns. [ChanFeed] does not: Send returns
// once the value has been written to the ring and the wakeup has been
// broadcast. This makes Send's cost independent of subscriber count and
// removes the "must call Wait immediately after each receive" obligation.
// In exchange, subscribers may fall behind (bounded by capacity) and, in drop
// mode, observe gaps.
//
// # Usage
//
// The simplest way to consume values is via [ChanFeed.SubscribeContext],
// which returns an iter.Seq that manages subscription lifetime:
//
//	for v := range feed.SubscribeContext(ctx) {
//		process(v)
//	}
//
// For select-based consumption, combine [ChanFeedSub.Ready] and
// [ChanFeedSub.Next]:
//
//	sub := feed.Subscribe()
//	defer sub.Unsubscribe()
//	for {
//		select {
//		case <-sub.Ready():
//		case <-ctx.Done():
//			return
//		}
//		for {
//			v, ok := sub.Next()
//			if !ok {
//				if feed.Closed() || sub.Closed() {
//					return
//				}
//				break
//			}
//			process(v)
//		}
//	}
//
// # See Also
//
// [NewChanFeed], [ChanFeed.Send], [ChanFeed.SubscribeContext],
// [ChanFeedSub.Ready], [ChanFeedSub.Next].
type ChanFeed[V any] struct {
	// capacity is the ring size (immutable).
	capacity int
	// mode is the backpressure policy (immutable).
	mode ChanFeedMode

	mu     sync.Mutex
	cond   *sync.Cond // only used when mode == ChanFeedBlock
	ring   []V
	head   uint64 // sequence number of the next write
	notify chan struct{}
	closed bool
	subs   map[*ChanFeedSub[V]]struct{}
}

// ChanFeedSub is a subscriber handle for a [ChanFeed]. Each subscriber holds
// an independent cursor into the feed's ring, enabling independent pacing
// (subject to the feed's backpressure mode).
type ChanFeedSub[V any] struct {
	feed    *ChanFeed[V]
	cursor  uint64
	dropped uint64
	closed  bool
}

// NewChanFeed constructs a [ChanFeed] with the given ring capacity and
// backpressure mode. It panics if capacity <= 0.
//
// Capacity sets the *maximum lag* a subscriber may accumulate before either
// (a) dropping values in [ChanFeedDropOldest] mode, or (b) blocking the sender
// in [ChanFeedBlock] mode.
func NewChanFeed[V any](capacity int, mode ChanFeedMode) *ChanFeed[V] {
	if capacity <= 0 {
		panic(`bigbuff: chanfeed: capacity must be > 0`)
	}
	f := &ChanFeed[V]{
		capacity: capacity,
		mode:     mode,
		ring:     make([]V, capacity),
		notify:   make(chan struct{}),
		subs:     make(map[*ChanFeedSub[V]]struct{}),
	}
	if mode == ChanFeedBlock {
		f.cond = sync.NewCond(&f.mu)
	}
	return f
}

// Send writes value into the feed and wakes all active subscribers, returning
// the number of subscribers present at send time. If the feed is closed, Send
// is a no-op and returns 0.
//
// In [ChanFeedBlock] mode, Send blocks while the ring is full (i.e. while the
// slowest subscriber's cursor lags the head by capacity or more). Blocking is
// released as subscribers consume (via [ChanFeedSub.Next]) or unsubscribe.
//
// In [ChanFeedDropOldest] mode, Send never blocks; the oldest ring slot is
// overwritten if necessary, causing the slowest subscribers to observe drops.
//
// The allocation footprint is at most one small notify channel per call (only
// when subscribers are present); with zero subscribers it is allocation-free.
func (f *ChanFeed[V]) Send(value V) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0
	}
	if f.cond != nil {
		for f.head-f.minCursorLocked() >= uint64(f.capacity) {
			if f.closed {
				return 0
			}
			f.cond.Wait()
		}
		if f.closed {
			return 0
		}
	}
	f.ring[f.head%uint64(f.capacity)] = value
	f.head++
	f.broadcastLocked()
	return len(f.subs)
}

// Subscribe registers a new subscriber and returns its handle. The subscriber
// starts at the current head — it will not observe previously-buffered values.
// If the feed is already closed, the returned subscriber is already closed.
func (f *ChanFeed[V]) Subscribe() *ChanFeedSub[V] {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &ChanFeedSub[V]{feed: f, cursor: f.head, closed: f.closed}
	if !f.closed {
		f.subs[s] = struct{}{}
	}
	return s
}

// SubscribeContext subscribes and returns a single-use iterator over the
// feed's values, unsubscribing automatically when the context is cancelled,
// the iterator stops early, or the feed is closed and drained. Behaviour is
// undefined if the iterator is invoked more than once.
//
// A nil context is treated as [context.Background].
func (f *ChanFeed[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := f.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)
	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				sub.Unsubscribe()
			}
			panic(`bigbuff: chanfeed: iterator yield func is nil`)
		}
		if !stop() {
			// Context already cancelled (or AfterFunc raced); skip iteration.
			return
		}
		defer sub.Unsubscribe()
		for {
			select {
			case <-sub.Ready():
			case <-ctx.Done():
				return
			}
			for {
				v, ok := sub.Next()
				if !ok {
					if f.Closed() || sub.Closed() {
						return
					}
					break
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// Close marks the feed closed. Subsequent [ChanFeed.Send] calls are no-ops.
// All waiting subscribers are woken (via the notify channel) so they may drain
// any pending values and observe closure via [ChanFeed.Closed] and by
// [ChanFeedSub.Next] returning false when drained.
func (f *ChanFeed[V]) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	if len(f.subs) > 0 {
		close(f.notify) // broadcast wakeup; no replacement needed
	}
	if f.cond != nil {
		f.cond.Broadcast()
	}
}

// Closed reports whether the feed has been closed.
func (f *ChanFeed[V]) Closed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// Subscribers returns the number of currently-subscribed subscribers.
func (f *ChanFeed[V]) Subscribers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}

// Len returns the number of values currently stored in the ring (at most
// capacity).
func (f *ChanFeed[V]) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.head > uint64(f.capacity) {
		return f.capacity
	}
	return int(f.head)
}

// minCursorLocked returns the minimum cursor across all subscribers (or head
// if no subscribers). Caller must hold f.mu.
func (f *ChanFeed[V]) minCursorLocked() uint64 {
	if len(f.subs) == 0 {
		return f.head
	}
	min := ^uint64(0)
	for s := range f.subs {
		if s.cursor < min {
			min = s.cursor
		}
	}
	return min
}

// broadcastLocked closes the current notify channel and installs a fresh one,
// waking every waiting subscriber. Caller must hold f.mu.
func (f *ChanFeed[V]) broadcastLocked() {
	if len(f.subs) == 0 {
		return
	}
	close(f.notify)
	f.notify = make(chan struct{})
}

// Ready returns a channel that will be immediately closed if the subscriber
// has pending values, has been unsubscribed, or the feed is closed; otherwise
// it returns the feed's current notify channel, which the next state change
// will close.
//
// The typical pattern is:
//
//	select {
//	case <-sub.Ready():
//	case <-ctx.Done():
//		return
//	}
//
// If a pending value exists at the time of the call, the returned channel is
// already closed so that the select never blocks unnecessarily. If no pending
// value exists, the returned channel will be closed by the next [ChanFeed.Send]
// (or [ChanFeed.Close]), guaranteeing that a select on it cannot miss data.
func (s *ChanFeedSub[V]) Ready() <-chan struct{} {
	f := s.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.closed || f.closed || s.cursor < f.head {
		return chanFeedClosedChan
	}
	return f.notify
}

// Next consumes and returns the subscriber's next available value. It returns
// (zero, false) if no value is currently pending (including when the
// subscriber or feed is closed and pending values have been drained).
//
// Next is non-blocking; combine with [ChanFeedSub.Ready] to wait for data.
// If the subscriber has fallen behind the ring, Next transparently advances
// past dropped values, accumulating their count (see [ChanFeedSub.Dropped]).
func (s *ChanFeedSub[V]) Next() (V, bool) {
	f := s.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.closed {
		var zero V
		return zero, false
	}
	if f.head > uint64(f.capacity) && s.cursor < f.head-uint64(f.capacity) {
		s.dropped += f.head - uint64(f.capacity) - s.cursor
		s.cursor = f.head - uint64(f.capacity)
	}
	if s.cursor < f.head {
		v := f.ring[s.cursor%uint64(f.capacity)]
		s.cursor++
		if f.cond != nil {
			f.cond.Broadcast()
		}
		return v, true
	}
	var zero V
	return zero, false
}

// Unsubscribe removes the subscriber from its feed and marks it closed. It is
// idempotent and safe to call from any goroutine, including concurrently with
// [ChanFeedSub.Next] and [ChanFeedSub.Ready].
func (s *ChanFeedSub[V]) Unsubscribe() {
	f := s.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if _, ok := f.subs[s]; ok {
		delete(f.subs, s)
		if f.cond != nil {
			f.cond.Broadcast()
		}
	}
}

// Closed reports whether the subscriber has been unsubscribed (or was created
// against an already-closed feed).
func (s *ChanFeedSub[V]) Closed() bool {
	f := s.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	return s.closed
}

// Dropped returns the total number of values this subscriber has missed due to
// falling behind the ring (in [ChanFeedDropOldest] mode). In [ChanFeedBlock]
// mode it is always zero.
//
// The value is "as of the last call to [ChanFeedSub.Next]" — drops that have
// occurred but not yet been observed via Next are not yet counted.
func (s *ChanFeedSub[V]) Dropped() uint64 {
	f := s.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	return s.dropped
}
