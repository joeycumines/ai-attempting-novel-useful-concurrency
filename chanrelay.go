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
	"sync/atomic"
)

// relayChunkSize is the number of registry slots per registry chunk. Registry
// chunks are allocated in blocks of this size, so it controls two amortised
// costs: subscription (one chunk allocation per relayChunkSize subscriptions)
// and compaction (one traversal per relayChunkSize detached slots, see below).
// 64 keeps a chunk at a handful of cache lines of pointers, which a linear scan
// walks in a couple of prefetched bursts, while making both amortised costs
// negligible.
const relayChunkSize = 64

// RelayOption configures a [ChanRelay].
type RelayOption interface {
	apply(*relayOptions)
}

type relayOptions struct {
	window int
}

type relayOptionFunc func(*relayOptions)

func (f relayOptionFunc) apply(o *relayOptions) { f(o) }

// WithRelayWindow sets the default window (buffer capacity) of subscriptions
// created by [ChanRelay.Subscribe], and by [ChanRelay.SubscribeContext].
//
// A window of 0 (the default) makes [ChanRelay.Subscribe] return a synchronous
// subscription: [ChanRelay.Send] hands the value directly to the subscriber,
// and does not return until the subscriber has received it. This is the
// interface that [ChanPubSub] offers, with a significant difference: see the
// dispatch modes documented on [ChanRelay.Send].
//
// A window of n > 0 makes the subscription buffered: [ChanRelay.Send] can be up
// to n values ahead of the subscriber without blocking, and [ChanRelay.TrySend]
// (which never blocks) conflates, retaining the most recent values.
//
// A negative window is invalid, and causes a panic.
func WithRelayWindow(window int) RelayOption {
	if window < 0 {
		panic(`bigbuff: chanrelay: negative window`)
	}
	return relayOptionFunc(func(o *relayOptions) { o.window = window })
}

// ChanRelay is a broadcast (publish/subscribe) primitive that delivers each
// published value to every live subscription, without goroutines, without
// callbacks, and without allocating on the publish path.
//
// It is the "dispatch" counterpart of [ChanPubSub]: where [ChanPubSub]
// broadcasts *through one shared channel* - which is what makes it selectable,
// but also forces every subscriber to share a single FIFO, to participate in a
// ping/pong acknowledgement protocol, and to be woken by one condition
// variable broadcast per send - a [ChanRelay] gives *each subscription its own
// transport channel*. A subscription is therefore selectable in the ordinary
// way, with no registration ritual, no shared channel, no stealing of values
// between subscribers, and no mandatory post-receive handshake:
//
//	sub := relay.Subscribe()
//	defer sub.Close()
//	for {
//	    select {
//	    case v := <-sub.C():  // a receive IS the acknowledgement
//	        ...
//	    case <-sub.Done():    // the subscription was closed
//	        return
//	    }
//	}
//
// # The fusion invariant
//
// A receive from [ChanPubSub.C] is only half of a delivery: the subscriber must
// then call [ChanPubSub.Wait], and the sender blocks until it does. Forgetting
// the handshake is a liveness bug, and because the transport is shared, a
// subscriber that receives "too many" times steals values from its peers, so
// [ChanPubSub] must document a seven point contract, and panic when it is
// violated. A [ChanRelay] fuses the value and the acknowledgement into a single
// channel operation, so the handshake cannot be forgotten, subscribers cannot
// interfere with each other, and there is no state machine to corrupt: the same
// primitive is selectable, drainable, and closable from any goroutine at any
// time.
//
// # Dispatch modes
//
// [ChanRelay.Send] is reliable and back-pressured: it returns once every live
// subscription has received the value (for a synchronous subscription) or has
// had room to buffer it, and it blocks - but never deadlocks, and never
// corrupts state - if a subscription is not consuming.
//
// [ChanRelay.TrySend] never blocks: it delivers to every subscription that can
// accept the value immediately, conflates (retains the most recent values) for
// buffered subscriptions, and skips synchronous subscriptions that are not
// currently receiving. It is a sequence of non-blocking channel operations, so
// it is also the lowest latency mode: values are handed directly to parked
// receivers, with no per-subscriber bookkeeping of any kind.
//
// [ChanRelay.SendContext] is [ChanRelay.Send] with an escape hatch: it stops
// dispatching when the context is done.
//
// The three publish methods are serialised (by one mutex, held for the duration
// of a publish, and not by subscribers nor by [RelaySubscription.Close]), which
// is what provides the total order guarantee: every subscription observes the
// same sequence of values, publishes form a strict sequence, and a value is
// never re-ordered with respect to another value, for any subscription. The
// same mutex is what makes [ChanPubSub] serialise sends; unlike ChanPubSub's,
// it is never held by anything on the subscriber side.
//
// # Cost model
//
// A ChanRelay amortises to:
//
//   - zero allocations per send, per receive, and per close;
//   - one transport channel slice (plus two small objects) per subscription,
//     and one registry chunk per relayChunkSize subscriptions;
//   - O(1) [RelaySubscription.Close], which takes no lock shared with
//     publishers, so it is never blocked by an in-flight send - including a
//     send blocked on another subscriber - and never blocks one;
//   - no goroutines, ever;
//   - O(s) registry traversal per send, over contiguous, prefetcher-friendly
//     blocks of pointers, with detached subscriptions compacted away lazily
//     (see below).
//
// Compare [ChanCaster], whose broadcast is a linear sequence of sends into one
// channel, and [ChanPubSub], whose broadcast additionally costs one condition
// variable broadcast (and hence one wakeup of every subscriber) per send: a
// ChanRelay's send is a linear sequence of *non-blocking* channel operations (or
// direct hand-offs to parked receivers), with no shared hot line beyond the send
// serialisation mutex, no wakeup storm, no counter bookkeeping, and no
// acknowledgement state to audit.
//
// # Robustness
//
// ChanPubSub panics on contract violations, because its state machine cannot
// otherwise distinguish "a subscriber is slow" from "a subscriber has
// disappeared". A ChanRelay has no such machine: every potentially blocking
// operation is paired with a select on the subscription's Done channel, so a
// subscription may be closed at any time - from any goroutine, including from
// inside a receive - and publishers observe it, skip it, and continue. A
// subscription that is not consuming can stall a reliable [ChanRelay.Send]
// (that is the point of back-pressure), but closing it releases the publisher,
// and [ChanRelay.TrySend] is never blocked by it at all.
//
// # Registry
//
// Subscriptions live in an append-only chain of fixed-size chunks, published to
// publishers through a single atomic head pointer. Publishing a new
// subscription is an append (no copies, no reallocation); detaching one is an
// atomic store of nil into its registry slot plus a counter decrement, so it is
// wait-free with respect to publishers. Detached slots are "grey": they are
// skipped in O(1) by publishers, and the chain is rebuilt in O(s) by the
// detaching goroutine only once grey slots outnumber live ones, making the
// rebuild O(1) amortised per detach, and keeping it entirely off the publish
// path. This is the property that lets the implementation scale with
// subscription churn - the case that a fixed-arity broadcast, or a callback
// list, handles badly - without costing a subscriber slice copy per publish,
// which is what a snapshot-based implementation pays.
//
// A ChanRelay is safe for concurrent use, but the zero value is not usable; use
// [NewChanRelay].
type ChanRelay[V any] struct {
	// done is closed when the relay is closed.
	done chan struct{}
	// closed is the atomic mirror of done, for the publish fast path.
	closed atomic.Bool
	// sendMu serialises dispatches, which is what provides global ordering, and
	// what makes it safe to store each value into every subscription in turn.
	// It is the only lock held by a publisher.
	sendMu sync.Mutex
	// mu guards the registry (head, tail, grey) and serialises compaction.
	// It is never held by a publisher, so subscribing, detaching, and
	// compacting can never be blocked by, and can never block, a send.
	mu sync.Mutex
	// head and tail are the ends of the registry chunk chain. Guarded by mu.
	// head is also published through headPub, for lock-free traversal.
	head, tail *relayChunk[V]
	// headPub is the entry point used by publishers.
	headPub atomic.Pointer[relayChunk[V]]
	// count is the number of live subscriptions, used for the no-op fast path.
	count atomic.Int32
	// grey is the number of detached registry slots since the last compaction.
	// Guarded by mu.
	grey int
	// window is the default subscription window.
	window int
}

// relayChunk is a fixed-size, append-only block of registry slots. A slot is
// written at most once with a subscription, and may later be cleared (by
// detaching it, or by compaction) - which is why slots are atomic pointers, and
// why publishers can traverse the registry without any lock.
type relayChunk[V any] struct {
	slots [relayChunkSize]atomic.Pointer[RelaySubscription[V]]
	// n is the number of initialised slots. Appending stores the slot, then
	// publishes n, so a publisher that loads n can read those slots without any
	// further synchronisation.
	n atomic.Int32
	// next is the following chunk in the chain, if any.
	next atomic.Pointer[relayChunk[V]]
}

// NewChanRelay constructs a [ChanRelay].
func NewChanRelay[V any](options ...RelayOption) *ChanRelay[V] {
	var o relayOptions
	for _, option := range options {
		option.apply(&o)
	}
	x := &ChanRelay[V]{
		done:   make(chan struct{}),
		head:   new(relayChunk[V]),
		window: o.window,
	}
	x.tail = x.head
	x.headPub.Store(x.head)
	return x
}

// Subscribe registers a new subscription, with the relay's default window.
//
// The subscription will receive every value subsequently published, in publish
// order, and is independent of every other subscription. It is the caller's
// responsibility to [RelaySubscription.Close] the subscription when it is no
// longer required.
//
// If the relay is already closed, the returned subscription is closed.
func (x *ChanRelay[V]) Subscribe() *RelaySubscription[V] {
	return x.SubscribeWindow(x.window)
}

// SubscribeWindow is [ChanRelay.Subscribe] with an explicit window, overriding
// the relay's default. See [WithRelayWindow]. It panics if window is negative.
func (x *ChanRelay[V]) SubscribeWindow(window int) *RelaySubscription[V] {
	if window < 0 {
		panic(`bigbuff: chanrelay: negative window`)
	}
	return x.register(newRelaySubscription(x, make(chan V, window)))
}

// SubscribeChan registers a new subscription that is delivered values through
// a channel owned by the caller, which makes it possible to reuse an existing
// channel - including one shared (with care) with other producers, and one of
// a size chosen by the caller - without allocating a transport per
// subscription.
//
// The caller MUST NOT close the channel while the subscription is live: the
// relay sends to it, and closing it would cause the relay to panic, exactly as
// any other send to a closed channel would. The relay never closes it, so the
// caller remains its owner, and may close it after
// [RelaySubscription.Close] has returned.
//
// The channel's capacity is the subscription's window, and buffering and
// conflation apply to it in the same way as to [ChanRelay.SubscribeWindow].
func (x *ChanRelay[V]) SubscribeChan(channel chan V) *RelaySubscription[V] {
	if channel == nil {
		panic(`bigbuff: chanrelay: nil channel`)
	}
	return x.register(newRelaySubscription(x, channel))
}

// SubscribeContext immediately subscribes to the relay, and returns a
// single-use iterator over the values delivered to that subscription, which is
// closed automatically when the iterator returns (including on context
// cancellation, early termination, or relay closure).
//
// Behaviour is undefined if the iterator is used more than once.
//
// WARNING: The returned iterator MUST be used immediately, OR, the context
// MUST be promptly cancelled, to avoid leaking the subscription.
func (x *ChanRelay[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := x.Subscribe()

	if ctx == nil {
		ctx = context.Background()
	}

	// Covers the case where the iterator is never called, and multiple calls.
	stop := context.AfterFunc(ctx, func() { sub.Close() })

	return func(yield func(V) bool) {
		if yield == nil {
			stop()
			sub.Close()
			panic(`bigbuff: chanrelay: iterator yield func is nil`)
		}

		if !stop() {
			// The context was already cancelled (the AfterFunc closed the
			// subscription, possibly concurrently - Close below makes that
			// deterministic), or the iterator was called more than once.
			sub.Close()
			return
		}

		defer sub.Close()

		// N.B. stopping the AfterFunc above disarms it (no goroutine, and no
		// reference to the subscription, is retained by the context), which is
		// why cancellation is handled here, in the select, rather than by it.
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.done:
				return
			case v, ok := <-sub.c:
				if !ok {
					return
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// newRelaySubscription constructs (but does not register) a subscription.
func newRelaySubscription[V any](r *ChanRelay[V], channel chan V) *RelaySubscription[V] {
	return &RelaySubscription[V]{r: r, c: channel, done: make(chan struct{})}
}

// register appends sub to the registry, or closes it if the relay is closed.
func (x *ChanRelay[V]) register(sub *RelaySubscription[V]) *RelaySubscription[V] {
	x.mu.Lock()

	if x.closed.Load() {
		x.mu.Unlock()
		sub.close(false)
		return sub
	}

	if x.tail.n.Load() >= relayChunkSize {
		chunk := new(relayChunk[V])
		x.tail.next.Store(chunk) // published before the first slot is written
		x.tail = chunk
	}

	n := x.tail.n.Load()
	x.tail.slots[n].Store(sub)
	sub.slot = &x.tail.slots[n]
	x.tail.n.Store(n + 1) // publishes the slot
	x.count.Add(1)

	x.mu.Unlock()

	return sub
}

// Send publishes value to every live subscription, returning the number of
// subscriptions that received it.
//
// This is the reliable, back-pressured dispatch mode:
//
//   - For a synchronous subscription (window 0), the value is received by the
//     subscriber before this call returns. That is the classic synchronous
//     broadcast: value and acknowledgement are one and the same operation. A
//     subscriber's acknowledgement does not wait for it to *process* the value,
//     matching [ChanPubSub], where [ChanPubSub.Wait] is called before the value
//     is yielded.
//   - For a buffered subscription (window n), the value is delivered into the
//     subscriber's buffer, and this call blocks only while that buffer is full
//     (that is, while the subscriber is more than n values behind).
//
// If a subscription is closed concurrently it is skipped: this call does not
// panic, does not wait for it, and does not count it. Subscriptions created
// concurrently with this call may or may not receive the value; subscriptions
// created after it has returned never do.
//
// A synchronous subscription that is not receiving blocks this call. That is
// intentional back-pressure, not a deadlock: it is released as soon as the
// subscription receives, or is closed - and closing it never has to wait for
// this call. Use [ChanRelay.TrySend] or a window to avoid blocking on a slow
// subscriber, and [ChanRelay.SendContext] to bound the wait.
func (x *ChanRelay[V]) Send(value V) int {
	sent, _ := x.dispatch(value, false, nil)
	return sent
}

// TrySend publishes value to every live subscription that can accept it without
// blocking, returning the number of subscriptions that received it.
//
// It never blocks, and it never panics. Its behaviour, per subscription, is:
//
//   - closed: skipped (not counted);
//   - synchronous (window 0) and not receiving: skipped (not counted);
//   - synchronous and receiving: delivered immediately (a direct hand-off);
//   - buffered with room: buffered (not counted as dropped);
//   - buffered and full: the oldest buffered value is discarded, and the newest
//     value is buffered (conflation - "latest value wins").
//
// Every value not delivered to a subscription is counted by
// [RelaySubscription.Dropped].
//
// This is the low latency dispatch mode: it is a sequence of non-blocking
// channel operations, so a slow, stalled, or churning subscriber cannot delay a
// publisher - it is skipped or conflated instead. Note that publishes are
// serialised (see [ChanRelay.Send]), so a TrySend that is concurrent with an
// in-progress publish waits for that publish to complete, which is bounded
// unless that publish is itself blocked on a synchronous subscriber. A relay
// whose subscriptions are all buffered never blocks a publisher at all.
func (x *ChanRelay[V]) TrySend(value V) int {
	sent, _ := x.dispatch(value, true, nil)
	return sent
}

// SendContext is [ChanRelay.Send] with an escape hatch: it stops dispatching as
// soon as ctx is done, returning the number of subscriptions delivered to, and
// ctx's error.
//
// A nil ctx is treated as [context.Background]. The returned error is nil if
// every live subscription was delivered to.
func (x *ChanRelay[V]) SendContext(ctx context.Context, value V) (int, error) {
	if ctx == nil {
		return x.dispatch(value, false, nil)
	}
	return x.dispatch(value, false, ctx)
}

// dispatch implements all three publish modes over one registry traversal.
//
// This is the hot path. It takes exactly one lock (sendMu, uncontended unless
// publishers are concurrent), makes no allocations, and performs one atomic
// load, one closed check, and one channel operation per live subscription.
func (x *ChanRelay[V]) dispatch(value V, try bool, ctx context.Context) (int, error) {
	if x.count.Load() == 0 {
		return 0, nil // no live subscriptions (lock-free fast path)
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	if x.closed.Load() {
		return 0, nil
	}

	var sent int
	for chunk := x.headPub.Load(); chunk != nil; chunk = chunk.next.Load() {
		n := chunk.n.Load()
		for i := int32(0); i < n; i++ {
			sub := chunk.slots[i].Load()
			if sub == nil || sub.closed.Load() {
				continue // detached, or closed
			}
			if ctx != nil {
				select {
				case <-ctx.Done():
					return sent, ctx.Err()
				default:
				}
			}
			var ok bool
			if try {
				ok = sub.trySend(value)
			} else {
				ok = sub.send(value, ctx)
			}
			if ok {
				sent++
			}
		}
	}

	if ctx != nil {
		select {
		case <-ctx.Done():
			return sent, ctx.Err()
		default:
		}
	}

	return sent, nil
}

// Len returns the number of live subscriptions.
func (x *ChanRelay[V]) Len() int {
	if n := int(x.count.Load()); n > 0 {
		return n
	}
	return 0
}

// Done returns a channel that is closed when the relay is closed.
func (x *ChanRelay[V]) Done() <-chan struct{} {
	return x.done
}

// Closed reports whether the relay has been closed.
func (x *ChanRelay[V]) Closed() bool {
	return x.closed.Load()
}

// Close closes the relay, and every subscription registered with it. It is
// idempotent, and safe to call concurrently with any other method.
//
// Closing a relay does not wait for in-flight sends: they observe each
// subscription's closure and skip it as they go. It also does not wait for
// subscribers to consume values already delivered to their buffers.
func (x *ChanRelay[V]) Close() {
	if !x.closed.CompareAndSwap(false, true) {
		return
	}
	close(x.done)

	// Drop the whole registry in O(1), then close the subscriptions so that
	// their consumers observe closure. Registry chunks are immutable once
	// published, so a send in flight finishes against the old chain.
	x.mu.Lock()
	old := x.head
	x.head, x.tail = new(relayChunk[V]), nil
	x.headPub.Store(x.head)
	x.grey = 0
	x.mu.Unlock()

	for chunk := old; chunk != nil; chunk = chunk.next.Load() {
		n := chunk.n.Load()
		for i := int32(0); i < n; i++ {
			if sub := chunk.slots[i].Load(); sub != nil {
				sub.close(false) // registry is already dropped
			}
		}
	}

	x.count.Store(0)
}

// detach removes a subscription from the registry, and is called exactly once
// per subscription, by [RelaySubscription.Close].
//
// Detach is O(1) with respect to publishers: it clears the subscription's
// registry slot (a single atomic store, which publishers skip), and defers any
// real work to a lazy compaction, which is performed here, by the detaching
// goroutine, only once grey slots outnumber live ones - making compaction O(1)
// amortised per detach, and keeping it off the publish path entirely.
func (x *ChanRelay[V]) detach(sub *RelaySubscription[V]) {
	// Clearing the slot is the entire cost that publishers observe.
	if slot := sub.slot; slot != nil {
		slot.Store(nil)
	}

	if n := x.count.Add(-1); n < 0 {
		x.count.Store(0) // relay is closing; nothing to track
	}

	x.mu.Lock()
	x.grey++
	compact := x.grey > int(x.count.Load()) && x.grey >= relayChunkSize
	x.mu.Unlock()

	if compact {
		x.compact()
	}
}

// compact rebuilds the registry chunk chain, dropping detached and closed
// slots.
//
// It is O(live), and is serialised by mu. Publishers are unaffected: they either
// traverse the chain they loaded before the rebuild (which is immutable, and
// still valid), or the one published after it. A publish in flight during a
// rebuild may therefore miss a subscription that is concurrently created - the
// same, documented, behaviour as any concurrent subscribe.
func (x *ChanRelay[V]) compact() {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed.Load() {
		return
	}

	var (
		head, tail *relayChunk[V]
	)

	for chunk := x.head; chunk != nil; chunk = chunk.next.Load() {
		n := chunk.n.Load()
		for i := int32(0); i < n; i++ {
			sub := chunk.slots[i].Load()
			if sub == nil || sub.closed.Load() {
				continue
			}
			if tail == nil || tail.n.Load() >= relayChunkSize {
				next := new(relayChunk[V])
				if tail == nil {
					head = next
				} else {
					tail.next.Store(next)
				}
				tail = next
			}
			j := tail.n.Load()
			tail.slots[j].Store(sub)
			tail.n.Store(j + 1)
		}
	}

	if head == nil {
		head = new(relayChunk[V])
		tail = head
	}

	x.head, x.tail = head, tail
	x.headPub.Store(head)
	x.grey = 0
	// NOTE: x.count is deliberately NOT rewritten here. It is owned solely
	// by register (+1), detach (-1) and Close (0). Rewriting it from a
	// traversal count races with a concurrent detach/register between the
	// traversal and the store, resurrecting dead subscriptions in Len().
}

// RelaySubscription is a single subscription to a [ChanRelay].
//
// Values are received from [RelaySubscription.C], which is a channel owned by
// the subscription: the relay sends to it, and never closes it, so a receive
// cannot observe a closed transport, and cannot steal from a peer. Closure is
// signalled by [RelaySubscription.Done] instead, which is the same split that
// [context.Context] uses for cancellation:
//
//	select {
//	case v := <-sub.C():
//	case <-sub.Done():
//	}
//
// A subscription may be consumed by multiple goroutines (values are then split
// between them), and must be closed by the caller when it is no longer
// required. A subscription that is never closed is retained by its relay: it
// will continue to be visited by publishers (and will stall a blocking
// [ChanRelay.Send] if it is synchronous and not receiving), so a subscription
// should be treated as a resource.
type RelaySubscription[V any] struct {
	r *ChanRelay[V]
	c chan V
	// done is closed exactly once, when the subscription is closed, and is the
	// cancellation signal for every operation that could otherwise block.
	done chan struct{}
	// slot is the registry slot holding this subscription, used for O(1)
	// detach. It is written exactly once, before registration, and never
	// mutated (compaction leaves it stale, which is harmless: it only makes
	// detach a no-op for a subscription compaction has already re-homed).
	slot    *atomic.Pointer[RelaySubscription[V]]
	closed  atomic.Bool
	dropped atomic.Uint64
}

// C returns the subscription's transport channel. It is never closed, so receive
// from it directly, and watch [RelaySubscription.Done] for closure.
func (s *RelaySubscription[V]) C() <-chan V {
	return s.c
}

// Done returns a channel that is closed when the subscription is closed, either
// directly via [RelaySubscription.Close], or because its relay was closed.
func (s *RelaySubscription[V]) Done() <-chan struct{} {
	return s.done
}

// Next receives the next value delivered to the subscription, blocking until one
// is available or the subscription is closed. The returned bool is false if and
// only if the subscription is closed, in which case the value is the zero value.
// Values buffered before a close are not delivered by Next.
//
// Next is safe for concurrent use. When a subscription has a single consumer, a
// ready [RelaySubscription.C] guarantees that a subsequent receive (or Next)
// does not block.
func (s *RelaySubscription[V]) Next() (V, bool) {
	for {
		// Prefer closure, so that Next is deterministic once closed.
		select {
		case <-s.done:
			var zero V
			return zero, false
		default:
		}
		select {
		case v := <-s.c:
			return v, true
		case <-s.done:
			var zero V
			return zero, false
		}
	}
}

// TryNext receives the next value delivered to the subscription, if one is
// available, without blocking. The returned bool is false if no value is
// available, or if the subscription is closed.
func (s *RelaySubscription[V]) TryNext() (V, bool) {
	select {
	case <-s.done:
		var zero V
		return zero, false
	default:
	}
	select {
	case v := <-s.c:
		return v, true
	default:
		var zero V
		return zero, false
	}
}

// Window returns the subscription's buffer capacity (0 for a synchronous
// subscription).
func (s *RelaySubscription[V]) Window() int {
	return cap(s.c)
}

// Pending returns the number of values buffered for the subscription.
func (s *RelaySubscription[V]) Pending() int {
	return len(s.c)
}

// Dropped returns the number of values not delivered to the subscription because
// it could not accept them (see [ChanRelay.TrySend]), including values discarded
// by conflation.
func (s *RelaySubscription[V]) Dropped() uint64 {
	return s.dropped.Load()
}

// Closed reports whether the subscription has been closed.
func (s *RelaySubscription[V]) Closed() bool {
	return s.closed.Load()
}

// Close closes the subscription, returning true if this call is the one that
// closed it.
//
// It is O(1), and takes no lock shared with publishers, so it neither blocks nor
// is blocked by an in-flight publish - even a publish blocked on another
// subscriber. It is safe to call concurrently, and from inside a receive.
//
// Values already buffered for the subscription are abandoned, and become
// eligible for garbage collection with it.
func (s *RelaySubscription[V]) Close() bool {
	return s.close(true)
}

func (s *RelaySubscription[V]) close(detach bool) bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	close(s.done)
	if detach {
		s.r.detach(s)
	}
	return true
}

// send delivers value to the subscription, blocking as required by its window,
// and returns whether it was delivered. Every blocking path is released by
// closure (and by ctx, if provided), so it cannot deadlock, even if the
// subscriber never receives.
func (s *RelaySubscription[V]) send(value V, ctx context.Context) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	// Deliver without blocking if the subscriber is already waiting for a value.
	// This matters beyond saving a syscall-ish amount of work: it means a
	// subscriber that is ready never waits behind one that is not (a synchronous
	// send is O(s) work, but is not serialised behind any subscriber's
	// *processing*, and only serialises behind a subscriber that is not yet
	// receiving).
	select {
	case s.c <- value:
		return true
	default:
	}
	if ctx != nil {
		select {
		case s.c <- value:
			return true
		case <-s.done:
			return false
		case <-ctx.Done():
			return false
		}
	}
	select {
	case s.c <- value:
		return true
	case <-s.done:
		return false
	}
}

// trySend delivers value to the subscription if it can do so without blocking,
// conflating for buffered subscriptions, and returns whether it was delivered.
func (s *RelaySubscription[V]) trySend(value V) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.c <- value:
		return true
	default:
	}
	window := cap(s.c)
	// Full (or synchronous with no parked receiver): discard the oldest value
	// and retry. The attempt bound is only reachable if the transport channel is
	// also written to by someone other than this relay, which [ChanRelay]
	// documents against.
	for attempt := 0; attempt <= window; attempt++ {
		select {
		case s.c <- value:
			return true
		default:
		}
		select {
		case <-s.c:
			s.dropped.Add(1) // conflated
		default:
			// A concurrent receiver made room; retry the push.
		}
		select {
		case <-s.done:
			return false
		default:
		}
	}
	s.dropped.Add(1)
	return false
}
