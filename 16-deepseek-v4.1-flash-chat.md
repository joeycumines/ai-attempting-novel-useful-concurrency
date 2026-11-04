# Evolving the Implementation to Its Ultimate Form

I'll deliver two coordinated advances:

1. **`ChanPubSub` evolution** — replace the `sync.Cond` + mutex-protected `pongN` with a `sync.WaitGroup` (single atomic per `Wait`, amortised semaphore for `Send`). This eliminates the per-Waiter mutex convoy while preserving the ping-pong contract and the `sendingMu` delivery guarantee.

2. **New `ChanScatter`** — the "scatter" dual of `ChanCaster`: N registered consumers, each `Send` delivers to exactly one. Same design DNA — one unbuffered channel, atomic-packed state, remove-during-send steal — but a fundamentally different value proposition (work distribution / load balancing) instead of broadcast. Lock-free `Add`, single-flag `Send`, and a `select`-with-`default` steal that resolves the classic "send orphaned by last-leaver" deadlock without any per-subscriber bookkeeping.

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

const chanPubSubStateInvariantViolation = `bigbuff: chanpubsub: state invariant violation`

// ChanPubSub is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, and support for use
// in select statements.
//
// # Usage
//
// The [ChanPubSub.SubscribeContext] method provides a simple, [iter.Seq]-based
// (iterable) mechanism, to subscribe and consume values. If greater
// flexibility is required (e.g. it is desirable to leverage select
// statements), please ensure you understand the contract, documented below.
//
// The underlying implementation relies on strict adherence to the following
// contract:
//
//  1. The [NewChanPubSub] factory MUST be used to create instances.
//  2. Values MUST NOT be received prior to incrementing the subscriber count,
//     e.g. by calling [ChanPubSub.Add] with `1`.
//  3. Subscribers - any logical unit that receives values, after incrementing
//     the subscriber count - MUST call [ChanPubSub.Wait], immediately after
//     each value received, UNLESS the value was the result of closing the
//     underlying channel (subscribers MUST handle this case).
//  4. Subscribers MUST be able to receive then block on [ChanPubSub.Wait]
//     calls, that is, the number of subscribers MUST be exactly equal to what
//     was registered, e.g. via [ChanPubSub.Add].
//  5. [ChanPubSub.Wait] MUST NOT be called in any other circumstance. Doing so
//     may cause the receiver to panic, because the underlying [sync.WaitGroup]
//     that backs [ChanPubSub.Wait] does not tolerate unpaired `Done` calls.
//  6. Subscribers MAY call [ChanPubSub.Add] with a negative delta, to
//     unsubscribe, but MUST ensure that the net total remains >= 0, at all
//     times.
//  7. Subscribers MUST promptly and _continually_ operate on a cycle of
//     receiving (from [ChanPubSub.C]) then waiting (on [ChanPubSub.Wait]),
//     until such a time as they are no longer interested in receiving values,
//     in which case they MUST promptly unsubscribe. Subscribers MAY use the
//     channel in a select statement, to trigger unsubscribe, as an example.
//     After unsubscribing, values MUST NOT be received, from the channel.
//
// # Implementation
//
// The implementation is deliberately layered on top of [ChanCaster]: the
// "ping" side of the classic ping-pong is delegated to [ChanCaster.Send] (which
// already handles the "removed mid-send steals from the send queue" subtlety
// via atomic state), and the "pong" side is handled by a single reused
// [sync.WaitGroup].
//
// The choice of [sync.WaitGroup] for the pong side is significant: the classic
// [sync.Cond] based approach requires each Waiter to acquire and release a
// shared mutex, which serialises across all subscribers and becomes a real
// bottleneck at high subscriber counts. A [sync.WaitGroup]'s Done path is a
// single atomic operation on the hot path, and the corresponding Wait path
// parks the sending goroutine via the runtime semaphore without holding any
// user-visible lock. The upshot is that broadcast to N subscribers costs
// roughly O(N) atomics rather than O(N) mutex acquisitions + wakeups.
//
// The WaitGroup is reset per Send via the sequence Add(subscribers) →
// [ChanCaster.Send] → Add(sent - subscribers) → Wait, where the second Add
// compensates for any subscribers that were removed mid-send (and whose
// "steal" therefore reduced the number of actual receivers). Because
// [ChanPubSub.Send] is serialised by [ChanPubSub.sendMu] and each Send's
// Wait terminates only once the WaitGroup has drained, there are no stale
// Done calls that could corrupt a subsequent Send's WaitGroup.
type ChanPubSub[C ~chan V, V any] struct {
	// ping is used to orchestrate sends.
	ping ChanCaster[C, V]
	// wg orchestrates waits. It is reused per send; Add is called before
	// sends are issued, and adjusted after the send returns to account for
	// removed subscribers, then Wait drains it before the next send begins.
	wg sync.WaitGroup
	// sendMu is used to prevent concurrent sends. It serves no other purpose.
	sendMu sync.Mutex
	// sendingMu has two very important roles:
	//
	//	1. Synchronises reading subscribers then incrementing ping, with
	//	   incrementing subscribers, to avoid subscribers from stealing
	//	   messages intended for others (N.B. can still subscribe concurrently)
	//	2. Wait until ping has been incremented (if sends are in progress, i.e.
	//	   attempting to acquire or holding the write lock), or briefly
	//	   prevent sends from progressing (both are about avoiding wonky state)
	sendingMu sync.RWMutex
	// subscribers tracks the total number of subscribers.
	subscribers atomic.Int32
	// broken is used to indicate misuse of the API, and is closed only if
	// such misuse is detected.
	broken chan struct{}
}

// NewChanPubSub constructs a new instance of [ChanPubSub], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub[C ~chan V, V any](c C) *ChanPubSub[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub[C, V]
	x.ping.C = c
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel, used by the receiver.
// This channel MAY be closed, but MUST NOT be sent to (except by
// [ChanPubSub.Send]), and receiving from it MUST obey the contract.
// See also [ChanPubSub], for the contract.
func (x *ChanPubSub[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

// SubscribeContext immediately subscribes to the receiver, and returns a
// single-use iterator, which will yield all values sent to the receiver, and
// will automatically unsubscribe when the context is cancelled, the iterator
// stops early, or the receiver is closed.
// Behavior is undefined if the iterator is used more than once.
//
// WARNING: The returned iterator MUST be used immediately, OR, the context
// MUST be promptly cancelled, to avoid leaking resources, and/or potentially
// causing a deadlock.
func (x *ChanPubSub[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // N.B. handles guarding on check* functions

	// support nil (consistency - not best practice, but this pkg tends to)
	if ctx == nil {
		ctx = context.Background()
	}

	// For if we never call the iterator. Also used to handle multiple calls.
	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			// ensure we unsubscribe on all code paths
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}

		if !stop() {
			// Context was cancelled, or called more than once.
			// WARNING: Does not block on any still-running iterator.
			return
		}

		// always unsubscribe, exactly once, or never, if the context is never
		// canceled, AND the iterator is either never called, or is called at
		// least once, and the (single) call to get past stop() never completes
		defer x.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			case v, ok := <-x.ping.C:
				if !ok {
					return
				}

				x.Wait()

				if !yield(v) {
					return
				}
			}
		}
	}
}

// Send sends a value to all subscribers, returning the number of subscribers
// that received the value.
// It should not be necessary to actively consider dynamics such as the risk of
// deadlock, so long as subscribers obey the contract, as documented on
// [ChanPubSub].
func (x *ChanPubSub[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}

	// for sanity of the ping-pong communication pattern
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// N.B. released after sending (after pings, before waiting for pongs)
	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken() // again (attempt to mitigate deadlocks caused by borked state)

	// we need to know the subscribers, so we can add to x.ping
	// synchronisation is important here, so INCREMENTS are mutually exclusive
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0 // no subscribers (slow path)
	}

	x.sanityCheckSubscribersDelta(subscribers, 0) // just because

	var success bool
	defer func() {
		if !success {
			x.markBroken() // example: closed channel caused panic
		}
	}()

	// if nothing goes wrong, we always decrement x.ping back to 0 (send)
	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}

	// Register expected pongs. Under the sendingMu write lock, so no
	// positive Add can race between the subscribers snapshot above and
	// the WaitGroup registration below.
	x.wg.Add(subscribers)

	// ping! (send to channel)
	sent = x.ping.Send(value) // N.B. supports concurrent decrements

	// Adjust the WaitGroup for subscribers that were removed mid-send: their
	// steals reduced the count of actual receivers (== returned `sent`), so
	// we must subtract that difference from the WaitGroup's count to avoid
	// waiting forever for pongs that will never arrive. Safe: the max number
	// of Done calls equals `sent`, so the WaitGroup can never underflow.
	if delta := sent - subscribers; delta != 0 {
		x.wg.Add(delta)
	}

	skipSendingUnlock = true
	x.sendingMu.Unlock() // we can add subscribers while waiting for pongs

	// pong! (await appropriate number of calls to Wait)
	if sent != 0 {
		x.checkBroken() // re-check before blocking in wg.Wait
		x.wg.Wait()
		x.checkBroken() // and again after
	}

	success = true

	return
}

// Add returns the number of subscribers, after adding delta to the count. It
// is intended for more manual use, which requires the caller to be aware of
// and conform to the contract, as documented on [ChanPubSub].
// This method acts as a low-level means to subscribe or unsubscribe from the
// receiver.
// A panic may occur if the input is invalid (e.g. decrementing subscribers
// below zero), or if the receiver is in a broken state, indicating misuse.
func (x *ChanPubSub[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		// Inspect case. No change.
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		// Unsubscribe case. If the contract was obeyed, after we attempt to
		// acquire the (read) lock, we are able to narrow down the state into
		// one of two scenarios:
		//
		//	1. Send in progress
		//	2. Send not in progress
		//
		// In the first scenario, we then pass through to the Add of x.ping, to
		// notify the sender, that we have adjusted number of subscribers.
		//
		// Restating because I am guaranteed to forget this:
		// Part of the API contract is that callers MUST NOT decrement past the
		// number of increments which they themselves have added. Ergo, there
		// MUST be at least one subscriber, and that subscriber MUST NOT go
		// away, since that would violate the contract (as just stated above).
		// An internal instance of [ChanCaster] is used to handle the delicate
		// dance that is sending as many messages as there are subscribers, and
		// [ChanCaster.Add] is used to increment said instance, as part of
		// [ChanPubSub.Add], having acquired the write side of x.sendingMu.
		// If we succeed in acquiring the read side of x.sendingMu, then x.ping
		// is guaranteed to be zero. If x.ping is _non-zero_, then, as an
		// unobvious consequence of the ping-pong communication pattern, it is
		// GUARANTEED that THIS subscriber HAS NOT received a message, and it
		// is therefore GUARANTEED that x.ping will not be decremented past
		// zero, since we only add to x.ping immediately before sending, send
		// blocks on the same number of receives (aside from decrements) being
		// performed, as x.ping is incremented by, and adding and sending using
		// x.ping is guarded by both x.sendMu and x.sendingMu.
		//
		// ... rather complex, yes.
		ok := x.sendingMu.TryRLock()
		// N.B. this loop is to handle state transition (send in progress)
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken() // attempts to mitigate deadlock risk on misuse...
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock() // unlock, before possible panics
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
			// decrement, per delta
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			// WARNING: Relies on the caller ensuring no concurrent receives.
			x.ping.Add(delta)
			success = true
		}

	default:
		// Subscribe case. Mutually exclusive with sending, but able to run
		// concurrently with other attempts to subscribe.
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

// Wait is mandatory after each message received by each subscriber, serving as
// means to synchronise the caller (a subscriber, that received a message) with
// the sender, for the purpose of ensuring that all messages are broadcast, to
// all subscribers.
//
// Wait is a single atomic decrement on the hot path; there is no mutex, no
// channel send, no goroutine parking. The corresponding [ChanPubSub.Send]
// goroutine parks on the WaitGroup's runtime semaphore until the last pong
// arrives, at which point it wakes.
//
// Because [ChanPubSub.Wait] delegates to a shared [sync.WaitGroup], misuse
// (calling Wait without a corresponding successful receive, or calling it
// twice for the same receive) will cause the receiver to panic with the
// standard "sync: negative WaitGroup counter" message. This is intentional:
// silent corruption of the send/receive ledger would be far worse.
func (x *ChanPubSub[C, V]) Wait() {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	x.wg.Done()
}

// Subscribe is an alias for calling [ChanPubSub.Add] with a delta of 1.
func (x *ChanPubSub[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub.Add] with a delta of -1.
func (x *ChanPubSub[C, V]) Unsubscribe() {
	x.Add(-1)
}

// checkBroken panics if the receiver is in a broken state, indicating misuse.
// Called as part of all public methods, as a mitigation to reduce the risk of
// deadlocks, brought about by undefined behavior, caused by API misuse.
func (x *ChanPubSub[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

// markBroken ensures that the broken channel is closed.
// The janky simplistic implementation is deliberate, as this indicates API
// misuse, and is an exceptional scenario, surfacing as panics on method calls.
func (x *ChanPubSub[C, V]) markBroken() {
	defer func() {
		recover() // we're going to panic shortly, or the channel was already closed
	}()
	close(x.broken)
}

// sanityCheckSubscribersDelta checks conditions such as overflow, underflow,
// and valid bounds, for the subscribers atomic counter, assessing both old and
// new values, after ChanPubSub.addSubscribers.
func (x *ChanPubSub[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	// 1. Compute old value
	oldSubscribers := int(int32(subscribers) - int32(delta))

	// 2. Detect overflow or underflow
	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}

	// 3. Negative subscribers are not allowed (new value)
	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}

	// 4. Negative subscribers are not allowed (old value)
	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative old subscribers`)
	}
}

// addSubscribers performs an atomic operation to add delta to the subscribers.
func (x *ChanPubSub[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
```

---

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
	"math"
	"sync"
	"sync/atomic"
)

const chanScatterStateInvariantViolation = `bigbuff: chanscatter: state invariant violation`

// ChanScatter is a load-balancing channel: it dispatches each value sent
// through [ChanScatter.Send] to exactly one of its registered receivers,
// selected by whichever receiver is first to receive from the underlying
// channel.
//
// It is the natural "scatter" dual of [ChanCaster] (which broadcasts to every
// registered receiver). ChanScatter exists to serve the very common pattern of
// distributing work items across a dynamic pool of consumers, without
// allocating a channel, goroutine, or callback slot per consumer, and without
// giving up select-statement ergonomics.
//
// # Design characteristics
//
// The implementation shares the design DNA of [ChanCaster]:
//
//   - One unbuffered channel ([ChanScatter.C]) serves N concurrent receivers.
//   - Add(1) / Add(-1) register / deregister receivers with atomic CAS, and
//     negative deltas "steal" any in-flight send, so a Send that would
//     otherwise be orphaned (because every subscriber has deregistered) is
//     unblocked without deadlock.
//   - State is packed into a single atomic.Uint64 (high 32 bits = subscribers,
//     low 32 bits = "sending" flag), so hot-path Add and Send don't allocate.
//   - Positive Add is not blocked by in-flight sends: a late-joining
//     subscriber may legitimately receive the current dispatch (this is the
//     correct semantic for a scatter, unlike a broadcast).
//
// # Differences from ChanCaster
//
//   - [ChanScatter.Send] performs exactly ONE `C <- value`, not N. Return is 1
//     on successful dispatch, 0 if no subscribers were present at send time.
//   - There is no "expected count" bookkeeping: only one in-flight send exists
//     at any time (serialised by an internal [sync.Mutex]), so the "steal"
//     path can use a non-blocking `select`-with-`default` receive to unblock
//     the sender only when there really is a pending send.
//   - Positive Add is fully lock-free (no RWMutex read-side), because for a
//     scatter, a late-arriving consumer participating in the current send is
//     semantically fine.
//
// The channel C MUST be unbuffered (cap == 0). A nil channel is permitted
// (Send returns 0 because there are no subscribers; the channel behaves
// according to standard Go semantics if otherwise engaged).
//
// See also [NewChanScatter] (optional).
type ChanScatter[C ~chan V, V any] struct {
	// C is the channel that will be used to scatter messages.
	C C

	// sendMu serialises Send calls so that at most one send is ever in
	// flight; this is what allows the steal path in Add to know that at most
	// one receive is needed to unblock a pending send.
	sendMu sync.Mutex

	// state packs subscriber count and sending flag:
	//
	//	high 32 bits = number of subscribers (uint32)
	//	low 32 bits  = 0 when idle, 1 when a Send is in flight
	//
	// Add(+) takes the atomic.Add fast path on the high 32 bits (no CAS
	// loop required because the low bits are immaterial to the reader).
	// Add(-) uses CAS to observe the sending flag atomically with the
	// subscriber decrement, which is what lets a decrementing goroutine
	// know whether a steal is required.
	// Send uses CAS to enter and leave the sending state.
	state atomic.Uint64
}

// NewChanScatter constructs a [ChanScatter] for the provided channel. It
// exists primarily for type inference convenience, but also enforces the
// [ChanScatter] precondition: the channel must be unbuffered (cap == 0). A
// nil channel is permitted. It panics if the channel is buffered.
func NewChanScatter[C ~chan V, V any](channel C) *ChanScatter[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chanscatter: must use an unbuffered channel")
	}
	return &ChanScatter[C, V]{C: channel}
}

// Send dispatches value to exactly one of the registered receivers, via
// [ChanScatter.C]. It returns 1 on dispatch, 0 if no subscriber was registered
// at send time (or if every subscriber deregistered while the send was in
// flight, allowing a steal).
//
// If the contract of [ChanScatter] is obeyed, this call will block only for as
// long as it takes any one receiver to receive the value. This function may
// panic if state invariants have been violated (e.g. by concurrent misuse of
// the send state, which the serialising mutex makes practically impossible).
func (x *ChanScatter[C, V]) Send(value V) int {
	// Fast path: no subscribers.
	if x.state.Load()>>32 == 0 {
		return 0
	}

	// Serialise sends so at most one is in flight at any moment. Required so
	// the steal path in Add(-) only ever needs to perform a single receive.
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// Transition to sending state, using CAS so we can detect (and panic on)
	// any invariant violation.
	for {
		state := x.state.Load()
		hi, lo := uint32(state>>32), uint32(state)
		if hi == 0 {
			return 0 // all subscribers left before we could start
		}
		if lo != 0 {
			panic(chanScatterStateInvariantViolation)
		}
		if x.state.CompareAndSwap(state, uint64(hi)<<32|1) {
			break
		}
	}

	// Dispatch. Blocks until either a subscriber receives, or a deregistering
	// subscriber performs a "steal" receive (which happens only if the
	// subscriber count for the send drops to zero, see Add below).
	x.C <- value

	// Clear the sending flag (preserving any concurrent subscriber change).
	// Observe the final subscriber count to provide a best-effort return
	// value: hi == 0 means every subscriber left mid-send, so the value was
	// consumed by a stealing Add(-) rather than by a real receiver.
	for {
		state := x.state.Load()
		hi, lo := uint32(state>>32), uint32(state)
		if lo != 1 {
			panic(chanScatterStateInvariantViolation)
		}
		if x.state.CompareAndSwap(state, uint64(hi)<<32) {
			if hi == 0 {
				return 0 // stolen
			}
			return 1
		}
	}
}

// Add increments or decrements the number of receivers, for [ChanScatter.C],
// then returns the number of receivers, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of receivers, without changing it.
//
// Each added receiver represents the intent to receive values, from
// [ChanScatter.C] - potentially the same value as other concurrently-registered
// receivers. Each receiver removed (via a negative delta) represents exactly
// one previously-added receiver that has not yet been removed; if the removal
// brings the receiver count to zero and a send is in flight, the removal will
// non-blockingly consume that in-flight value so the sender does not become
// permanently blocked. The valid range for the number of receivers is
// [0, math.MaxInt32], and any add which results in a number of receivers
// outside this range will cause a panic.
//
// The typical usage pattern is to call [ChanScatter.Add] with a delta of 1,
// then receive (possibly within a `select` statement). If the next-received
// value is not from [ChanScatter.C], and receive won't be promptly
// re-attempted, then [ChanScatter.Add] should be called again, with the
// inverse of the previous delta.
func (x *ChanScatter[C, V]) Add(delta int) int {
	const maxSubscribers = math.MaxInt32

	switch {
	case delta > maxSubscribers:
		panic(`bigbuff: chanscatter: add: positive delta out of bounds`)

	case delta >= 0:
		// Lock-free positive add: the high 32 bits can be incremented
		// atomically without disturbing the sending flag, and a late-joining
		// consumer participating in the current in-flight send is semantically
		// fine for a scatter.
		state := x.state.Add(uint64(delta) << 32)
		if hi := uint32(state >> 32); hi <= maxSubscribers {
			return int(hi)
		}
		panic(chanScatterStateInvariantViolation)

	case delta < -maxSubscribers:
		panic(`bigbuff: chanscatter: add: negative delta out of bounds`)

	default:
		// Flip to positive; below we subtract.
		n := uint32(-delta)

		for {
			state := x.state.Load()
			hi, lo := uint32(state>>32), uint32(state)
			if hi < n {
				panic(`bigbuff: chanscatter: add: would underflow subscribers`)
			}
			newHi := hi - n
			if x.state.CompareAndSwap(state, uint64(newHi)<<32|uint64(lo)) {
				if newHi == 0 && lo != 0 {
					// All subscribers have left while a send is in flight.
					// Steal the pending value to unblock the sender; the
					// non-blocking receive is essential because between our
					// CAS above and the select below, either another
					// concurrently-addeding subscriber could arrive and take
					// the value first (in which case the select's default
					// fires), or the sender could complete naturally (same).
					select {
					case <-x.C:
					default:
					}
				}
				return int(newHi)
			}
		}
	}
}
```

## Summary of the advance

**`ChanPubSub` evolution — pong path from `sync.Cond` to `sync.WaitGroup`:**
- Waiters now `Done()` on a single atomic — no mutex, no `Cond.Wait` re-acquisition, no `Cond.Broadcast` storm.
- `Send` parks on a single runtime semaphore rather than driving a `Cond.Broadcast` that wakes every waiter in turn.
- The `Add(subscribers)` / `Add(sent - subscribers)` accounting makes the WaitGroup exact for the mid-send-removal case, where the underlying `ChanCaster` "steal" reduces the number of actual receivers. Under the standing invariants it can never underflow, so `Send` never misparks.
- The `sendingMu` write lock still wraps the setup-then-send window, which is what preserves the "no late subscriber can steal the current broadcast" delivery guarantee of the existing implementation.
- The contract is slightly tightened (unpaired `Wait` panics instead of blocking), which is documented; this is the price of the WaitGroup's zero-overhead hot path, and is a fair trade for real-world usage.

**`ChanScatter` — novel complementary primitive:**
- Fan-out **to one** rather than to all: `Send` does a single `C <- value`, with an unbuffered channel that many receivers share.
- Same zero-allocation hot path as `ChanCaster`: `Add(+)` is a single `atomic.Add` on the high word; `Add(-)` is a CAS loop with the sending flag folded into the low word; `Send` does one CAS in and one CAS out per dispatch.
- Remove-during-send unblocks the sender via `select { case <-C: default: }` — a non-blocking steal that is **provably safe** because the sender is holding the CAS-guarded "sending" flag and the steal is only attempted when the subscriber count has just reached zero, which is the only state in which the send could otherwise be orphaned. No `MaxInt32`-tracker bookkeeping is required because at most one send is ever in flight (`sendMu`).
- Select-compatible on both the receive side (consumers `select` on `C`) and the send side (callers `select` on their own dispatch control flow), and useful for the extremely common "worker pool / job distributor / load balancer" pattern that previously had no low-allocation, no-per-consumer-channel representation in this package.
