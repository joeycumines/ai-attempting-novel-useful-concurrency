I'll start by getting the workspace set up and checking the Go toolchain, then design and build the new implementation.

Baseline builds. Now let me write the new implementation — a per-subscription-transport broadcast relay.

Now a thorough test suite:

Now let me add the remaining tests (lock-independence claims) and godoc examples:

Now the design document with the analysis and measurements:

I built a third implementation — `ChanRelay` — that inverts the central assumption of both existing designs, and I proved out its characteristics with tests and benchmarks.

## The idea: per-subscription transports + the *fusion invariant*

`ChanCaster`/`ChanPubSub` broadcast through **one shared** unbuffered channel; that's what makes them selectable, but it forces a global FIFO, a ping/pong handshake (`Wait`), one `Cond.Broadcast` per publish, a seven-point contract, and panics on misuse. A `ChanRelay` gives **each subscription its own channel**:

- **The receive *is* the acknowledgement** — value and ack fused into one channel operation, so the handshake cannot be forgotten, subscribers cannot steal from peers, and there is no state machine to corrupt (hence no `broken` state, nothing to panic about).
- **The relay never closes a channel it sends to** — closure is signalled out of band via `Done()` (exactly how `context` separates values from cancellation). Subscribers only ever see a `<-chan V`, so they can't close it either.
- **Every potentially unbounded wait has a `done`/`ctx` case** — a closed subscriber is *observed and skipped*, so the deadlock class that `ChanPubSub` must panic to escape becomes an ordinary recoverable outcome.

## Why it scales with churn *and* with N

- **Chunked, append-only registry**: a chain of 64-slot atomic blocks behind one head pointer. Subscribe = append (one allocation per 64 subs, never a copy); detach = one atomic `nil` store + counter decrement, **taking no lock shared with publishers**; grey slots are skipped in O(1) and rebuilt in O(live) *by the detaching goroutine* only when grey > live → amortised O(1) close, compaction entirely off the publish path.
- **Publish path**: one mutex for the total-order guarantee (same trade-off `ChanPubSub` makes), then per subscriber one atomic load + one closed check + 1–3 channel ops. **Zero allocations per publish, ever** — allocation is a function of churn only, never publish volume, so the heap profile is flat.
- **Three dispatch modes on one loop**: `Send` (back-pressured; non-blocking first pass so a ready subscriber never waits behind a slow one), `TrySend` (never waits on a subscriber; conflates "latest wins" for buffered subs), `SendContext` (bounded wait). Windows are per-subscription, so one publish can serve a heterogeneous mix — synchronous, buffered, conflating — in a single pass.

## Measured (2 vCPU, median of 3; all `0 allocs/op`)

| ns/broadcast | 1 | 8 | 64 | 512 |
|---|---|---|---|---|
| CallbackChan (callback-per-subscriber baseline) | 271 | 2389 | 23157 | 230599 |
| ChanPubSub | 547 | 2223 | 23874 | 429136 |
| **ChanRelay.Send** (window 0) | **349** | **3070** | **29043** | **268863** |
| ChanRelay.SendBuffered(16) | 133 | 1302 | 7840 | 66734 |
| **ChanRelay.TrySend** | **42** | **753** | **6184** | **51575** |
| ChanRelay dispatch overhead only (no delivery) | 38 | 208 | 1657 | 13352 |

So: ~1.2–1.3× of the raw callback-channel floor in *synchronous* mode and 1.6× faster than `ChanPubSub` at 512 (slower at 8–64, where a single cond broadcast is genuinely excellent), and **3–8× faster per receiver** in the buffered/`TrySend` modes `ChanPubSub` can't express at all. Churn: 280 ns parallel (vs 387 for `ChanPubSub.Add(-1)`), 3 small allocations, compaction invisible even with 1024 live subscribers.

## Files (in `/home/user/go-bigbuff`)

- `chanrelay.go` — implementation with the full rationale, plus `chancaster.go`/`chanpubsub.go` as provided
- `chanrelay_test.go` — race-clean suite incl. deadlock-freedom (closing a stalled subscriber releases the publisher; unrelated closes never wait), conflation exactness, compaction/duplicate-delivery detection over 12 churn generations, concurrent-publisher total order, a 250 ms adversarial stress run, multi-consumer splits, and zero-alloc assertions
- `chanrelay_bench_test.go` — apples-to-apples baselines (`CallbackNoop`, `CallbackChan`, `ChanCaster`, `ChanPubSub`) vs all relay modes, plus churn
- `example_test.go`, and `CHANRELAY.md` (open in your viewer) with the invariant proofs (I1–I6), cost model, trade-offs, and deliberate limits

````txtar
-- chancaster.go --
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

// ChanCaster supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanCaster.Send] performs exactly one
// synchronous send per registered receiver, relying on the one-to-one
// send/receive semantics of an unbuffered channel to guarantee delivery. A
// buffered channel would violate these invariants and can cause deadlocks or
// undetected missed receives.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire. This allows a
// ChanCaster to be used in a "disabled" state — [ChanCaster.Send] returns 0
// when there are no receivers, and the channel can be included in a select
// statement without ever being selected.
//
// Construction is "softly" enforced: [NewChanCaster] validates the
// precondition and panics on a buffered channel, but a value may also be
// built directly via a struct literal, which bypasses that check. Callers
// that construct a [ChanCaster] directly are responsible for honouring the
// unbuffered requirement themselves. The channel requirement matches
// ChanPubSub; unlike ChanPubSub, direct ChanCaster construction remains
// supported and relies on the caller to preserve the invariant.
//
// See also [NewChanCaster] (optional).
type ChanCaster[C ~chan V, V any] struct {
	// C is the channel that will be used to broadcast messages.
	//
	// See [ChanCaster.Add] and [ChanCaster.Send] for usage details.
	C C

	// mutex synchronises [ChanCaster.Send] and [ChanCaster.Add] calls with
	// positive deltas. Ensuring sends are mutually exclusive is fairly
	// important for consistency, but not strictly necessary. The primary
	// purpose of this mutex is to avoid incrementing the number of receivers
	// while any send is in progress, to facilitate removing receivers while
	// send is in progress.
	mutex sync.RWMutex
	// state synchronises the number of receivers, between [ChanCaster.Send]
	// calls, and [ChanCaster.Add] calls with negative deltas.
	// The high 32 bits are number of receivers (modeled as int32), while the
	// low 32 bits are either identical, or [math.MaxInt32] + the number of
	// receivers being (the value of the high 32 bits), if sending is in
	// progress. Using an atomic value for synchronisation requires having the
	// "is sending" information embedded in said value, and having two copies
	// of the number of receivers makes invariant and overflow checks easier.
	state atomic.Uint64
}

// NewChanCaster constructs a [ChanCaster] for the provided channel. It exists
// primarily for type inference convenience, but also enforces the
// [ChanCaster] precondition: the channel must be unbuffered (cap == 0). A nil
// channel is permitted. It panics if the channel is buffered.
func NewChanCaster[C ~chan V, V any](channel C) *ChanCaster[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster[C, V]{C: channel}
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster.C], then returns the number of sends performed, less any
// removed mid-flight by [ChanCaster.Add].
//
// If the contract of [ChanCaster] is obeyed, this call will never block for
// any significant time. This function may panic if state invariants have been
// violated, e.g. by misuse of [ChanCaster.Add], or if unregistered receives
// are detected.
//
// See [ChanCaster.Add] for usage details.
func (x *ChanCaster[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // no receivers (fast path)
	}

	// prevent receivers being added while sending + order values by send call
	x.mutex.Lock()
	defer x.mutex.Unlock()

	// load our state, guard no receivers (early exit), and set tracker to the
	// value of `maxInt32 + receivers`, using CAS to sync with negative adds
	var (
		state              uint64
		receivers, tracker uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0 // no receivers (slow path)
		}

		receivers = uint32(state >> 32) // initialize from hi
		tracker = uint32(state)         // initialize from lo
		if tracker != receivers || receivers > math.MaxInt32 {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}

		// attempt to set tracker to `maxInt32 + receivers`, with CAS used to
		// synchronise with decrements of receivers
		tracker += math.MaxInt32
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(tracker)) {
			break
		}
	}

	// broadcast involves sending to all receivers - with the total actually
	// received being in range [0, receivers], due to potential decrements
	for range receivers {
		x.C <- value // may end up received by negative Add calls
	}

	// now, we can retrieve, validate, then reset the state (to 0 - all broadcast + we locked so none added)
	// note: it should be stable - if it isn't, invariants were violated
	state = x.state.Load()
	tracker = uint32(state >> 32) // actually the final receivers (used as scratch)
	if tracker > receivers ||     // receivers should be unchanged or decreased (and also lower than math.MaxInt32)
		uint32(state) != tracker+math.MaxInt32 || // lo still exactly math.MaxInt32 more than hi
		!x.state.CompareAndSwap(state, 0) { // failing this indicates one or more unregistered receivers
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	// returns number of sends less any received by negative Add calls
	return int(tracker)
}

// Add increments or decrements the number of receivers, for [ChanCaster.C],
// then returns the number of receivers, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of receivers, without changing it.
//
// Each added receiver represents the intent to receive exactly one value, from
// [ChanCaster.C]. Each receiver removed (via a negative delta) represents
// exactly one added receiver, which has not yet been removed, and has not and
// will not receive a value. The valid range for the number of receivers is
// [0, math.MaxInt32], and any add which results in a number of receivers
// outside this range will cause a panic.
//
// The typical usage pattern is to call [ChanCaster.Add] with a delta of 1,
// then immediately receive, or attempt to receive, e.g. within a `select`
// statement. If the next-received value (e.g. within said `select` statement)
// is not from [ChanCaster.C], and receive won't be promptly re-attempted, then
// [ChanCaster.Add] should be called again, with the inverse of the previous
// delta.
//
// Using a delta greater than 1 indicates multiple separate receivers, which
// will all receive the same value, from the same [ChanCaster.Send] call. These
// receivers should independently decrement the number of receivers, if
// necessary, as described above.
func (x *ChanCaster[C, V]) Add(delta int) int {
	const maxReceivers = math.MaxInt32

	switch {
	case delta >= 0:
		var state uint64
		if delta == 0 {
			state = x.state.Load() // no change
		} else if delta > maxReceivers {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		} else {
			// increasing num receivers not allowed concurrently with sending
			x.mutex.RLock()
			defer x.mutex.RUnlock()

			// add delta to both hi and lo
			state = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
		}

		// validate to ensure we did not overflow + sanity check invariants
		// note: if this was an add (and we acquired the lock), the lo value
		// should never be the +maxReceivers case
		if receivers, tracker := uint32(state>>32), uint32(state); receivers <= maxReceivers &&
			receivers >= uint32(delta) &&
			(receivers == tracker || (delta == 0 && receivers+maxReceivers == tracker)) {
			return int(receivers)
		}

	case delta < -maxReceivers:
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		// flip delta to positive, from negative
		delta = -delta

		// note: same delta calc as above, subtracted using two's complement rules
		state := x.state.Add(^(uint64(delta)<<32 | uint64(uint32(delta)) - 1))

		// validate, and, if necessary, receive any channel sends that would
		// otherwise never be received (to avoid Send hanging)
		if receivers := uint32(state >> 32); receivers <= maxReceivers &&
			maxReceivers-receivers >= uint32(delta) {
			switch uint32(state) { // lo
			case receivers:
				// not sending
				return int(receivers) // note: already subtracted delta

			case maxReceivers + receivers:
				// sending - perform the requisite number of receives
				// note: receivers = tracker-maxReceivers (per the above)
				for range delta {
					<-x.C
				}
				return int(receivers) // note: already subtracted delta
			}
		}
	}

	// invariant violation, e.g. due to OoB add, or previous violation
	panic(`bigbuff: chancaster: add: state invariant violation`)
}
-- chanpubsub.go --
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
//  5. [ChanPubSub.Wait] MUST NOT be called in any other circumstance.
//  6. Subscribers MAY call [ChanPubSub.Add] with a negative delta, to
//     unsubscribe, but MUST ensure that the net total remains >= 0, at all
//     times.
//  7. Subscribers MUST promptly and _continually_ operate on a cycle of
//     receiving (from [ChanPubSub.C]) then waiting (on [ChanPubSub.Wait]),
//     until such a time as they are no longer interested in receiving values,
//     in which case they MUST promptly unsubscribe. Subscribers MAY use the
//     channel in a select statement, to trigger unsubscribe, as an example.
//     After unsubscribing, values MUST NOT be received, from the channel.
type ChanPubSub[C ~chan V, V any] struct {
	// ping is used to orchestrate sends.
	ping ChanCaster[C, V]
	// pongC is used to orchestrate waits.
	pongC *sync.Cond
	pongN int
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
	x.pongC = sync.NewCond(new(sync.Mutex))
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

	// ping! (send to channel)
	sent = x.ping.Send(value) // N.B. supports concurrent decrements

	skipSendingUnlock = true
	x.sendingMu.Unlock() // we can add subscribers while waiting for pongs

	// pong! (await appropriate number of calls to Wait)
	if sent != 0 {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()

		x.checkBroken() // AFTER lock (broken state is only broadcast once)

		x.pongN = sent
		x.pongC.Broadcast() // wake up any blocking Wait calls

		// wait for our pongs to be consumed
		for x.pongN != 0 {
			x.pongC.Wait()
			x.checkBroken() // ALWAYS checkBroken after a wait
		}
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
func (x *ChanPubSub[C, V]) Wait() {
	x.checkUsedFactoryFunction()

	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()

	x.checkBroken() // AFTER lock (broken state is only broadcast once)

	// wait for a pong to consume
	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken() // ALWAYS checkBroken after a wait
	}

	x.pongN-- // consume a pong

	if x.pongN == 0 {
		x.pongC.Broadcast() // wake up Send call
	}
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
		recover() // we're going to panic shortly
	}()
	x.pongC.L.Lock() // avoid missing the broadcast
	defer x.pongC.L.Unlock()
	close(x.broken)
	x.pongC.Broadcast() // wake up any waiting goroutines (once)
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
-- chanrelay_bench_test.go --
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
	"runtime"
	"sync"
	"testing"
)

// The benchmarks in this file are deliberately apples-to-apples: every
// implementation broadcasts the same int to the same number of live,
// continuously consuming subscribers, from a single publisher, and every
// subscriber receives (synchronously - i.e. window 0) with no buffering. That
// is the strongest form of fan-out: it is the mode in which the publisher must
// synchronise with every subscriber.
//
// The four baselines are:
//
//   - CallbackNoop: a slice of no-op funcs - the floor for any fan-out.
//   - CallbackChan: a slice of funcs, each of which hands the value to a
//     per-consumer channel (i.e. the "callback channel" design).
//   - ChanCaster: the linear sequence of sends into one unbuffered channel.
//   - ChanPubSub: ChanCaster plus its ping/pong (Wait) protocol.

var benchmarkSizes = []int{1, 8, 64, 512}

func benchmarkSizeName(n int) string {
	switch n {
	case 1:
		return "1"
	case 8:
		return "8"
	case 64:
		return "64"
	default:
		return "512"
	}
}

// ---------------------------------------------------------------- baselines --

type benchmarkCallback func(int)

func BenchmarkCallbackNoop(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			handlers := make([]benchmarkCallback, subscribers)
			var sink int
			for i := range handlers {
				handlers[i] = func(v int) { sink = v }
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, handler := range handlers {
					handler(i)
				}
			}
			_ = sink
		})
	}
}

func BenchmarkCallbackChan(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			handlers := make([]benchmarkCallback, subscribers)
			stop := make([]chan struct{}, subscribers)
			for i := range handlers {
				channel := make(chan int)
				stop[i] = make(chan struct{})
				go func() {
					for {
						select {
						case <-channel:
						case <-stop[i]:
							return
						}
					}
				}()
				handlers[i] = func(v int) { channel <- v }
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, handler := range handlers {
					handler(i)
				}
			}
			b.StopTimer()
			for i := range stop {
				close(stop[i])
			}
		})
	}
}

func BenchmarkChanCaster(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			channel := make(chan int)
			caster := NewChanCaster(channel)
			for range subscribers {
				go func() {
					for range channel {
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			// N.B. ChanCaster is a one-shot broadcast primitive: Send consumes
			// the armed receivers, so each broadcast must re-arm them.
			for i := 0; i < b.N; i++ {
				caster.Add(subscribers)
				if n := caster.Send(i); n != subscribers {
					b.Fatalf("expected %d receivers, got %d", subscribers, n)
				}
			}
			b.StopTimer()
			close(channel)
		})
	}
}

func BenchmarkChanPubSub(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			channel := make(chan int)
			pubsub := NewChanPubSub(channel)
			for range subscribers {
				go func() {
					pubsub.Add(1)
					for range pubsub.C() {
						pubsub.Wait()
					}
				}()
			}
			for pubsub.Add(0) != subscribers {
				runtime.Gosched()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pubsub.Send(i)
			}
			b.StopTimer()
			close(channel)
		})
	}
}

// ---------------------------------------------------------------- ChanRelay --

func benchmarkChanRelaySend(b *testing.B, subscribers, window int) {
	relay := NewChanRelay[int](WithRelayWindow(window))
	subs := make([]*RelaySubscription[int], subscribers)
	for i := range subs {
		sub := relay.Subscribe()
		subs[i] = sub
		go func() {
			for {
				select {
				case <-sub.C():
				case <-sub.Done():
					return
				}
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relay.Send(i)
	}
	b.StopTimer()
	for _, sub := range subs {
		sub.Close()
	}
	relay.Close()
}

// BenchmarkChanRelay_Send measures synchronous broadcast (window 0).
func BenchmarkChanRelay_Send(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			benchmarkChanRelaySend(b, subscribers, 0)
		})
	}
}

// BenchmarkChanRelay_SendBuffered measures broadcast to subscriptions with a
// window of 16 (i.e. decoupled from the subscribers).
func BenchmarkChanRelay_SendBuffered(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			benchmarkChanRelaySend(b, subscribers, 16)
		})
	}
}

// BenchmarkChanRelay_TrySend measures non-blocking broadcast to synchronous
// subscriptions, which are all parked in a receive (the best case for latency).
func BenchmarkChanRelay_TrySend(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			relay := NewChanRelay[int]()
			subs := make([]*RelaySubscription[int], subscribers)
			for i := range subs {
				sub := relay.Subscribe()
				subs[i] = sub
				go func() {
					for {
						select {
						case <-sub.C():
						case <-sub.Done():
							return
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				relay.TrySend(i)
			}
			b.StopTimer()
			for _, sub := range subs {
				sub.Close()
			}
			relay.Close()
		})
	}
}

// BenchmarkChanRelay_Churn measures parallel subscription churn (subscribe then
// close), against a fixed population of live subscribers.
func BenchmarkChanRelay_Churn(b *testing.B) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	for range 64 {
		sub := relay.Subscribe()
		go func() {
			for {
				select {
				case <-sub.C():
				case <-sub.Done():
					return
				}
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			relay.Subscribe().Close()
		}
	})
	b.StopTimer()
	relay.Close()
}

// BenchmarkChanPubSub_Churn is the equivalent subscription churn for
// ChanPubSub, whose Add(-1) must synchronise with in-flight sends.
func BenchmarkChanPubSub_Churn(b *testing.B) {
	channel := make(chan int)
	pubsub := NewChanPubSub(channel)
	for range 64 {
		go func() {
			pubsub.Add(1)
			for range pubsub.C() {
				pubsub.Wait()
			}
		}()
	}
	for pubsub.Add(0) != 64 {
		runtime.Gosched()
	}
	var publisher sync.WaitGroup
	stop := make(chan struct{})
	publisher.Add(1)
	go func() {
		defer publisher.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pubsub.Send(0)
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pubsub.Add(1)
			pubsub.Add(-1)
		}
	})
	b.StopTimer()
	close(stop)
	publisher.Wait()
	close(channel)
}

// BenchmarkChanRelay_Subscribe measures the cost of registering a subscription
// (including its transport), and BenchmarkChanRelay_SubscribeClose the cost of
// registering and detaching one, i.e. the churn cost with no publisher.
func BenchmarkChanRelay_SubscribeClose(b *testing.B) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relay.Subscribe().Close()
	}
	b.StopTimer()
	relay.Close()
}

// BenchmarkChanRelay_RegistryCompaction measures the amortised cost of
// subscription churn once the registry is large enough to require compaction.
func BenchmarkChanRelay_RegistryCompaction(b *testing.B) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	// A large live population, so that compaction is O(1024) per trigger.
	live := make([]*RelaySubscription[int], 1024)
	for i := range live {
		live[i] = relay.Subscribe()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relay.Subscribe().Close()
	}
	b.StopTimer()
	for _, sub := range live {
		sub.Close()
	}
	relay.Close()
}

// BenchmarkChanRelay_TrySendSkipped measures pure dispatch overhead: a broadcast
// to synchronous subscriptions that are not receiving, so that no value is
// transferred and no goroutine is woken. It is the floor for the relay's
// per-subscriber cost, and the closest analogue of BenchmarkCallbackNoop.
func BenchmarkChanRelay_TrySendSkipped(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			relay := NewChanRelay[int]()
			subs := make([]*RelaySubscription[int], subscribers)
			for i := range subs {
				subs[i] = relay.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				relay.TrySend(i)
			}
			b.StopTimer()
			for _, sub := range subs {
				sub.Close()
			}
			relay.Close()
		})
	}
}

// BenchmarkChanRelay_TrySendBuffered measures a broadcast that is absorbed by
// subscription buffers (no subscriber is woken during the measured loop, beyond
// the first window of values), i.e. the cost of queueing a value per subscriber.
func BenchmarkChanRelay_TrySendBuffered(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			relay := NewChanRelay[int](WithRelayWindow(1024))
			subs := make([]*RelaySubscription[int], subscribers)
			for i := range subs {
				subs[i] = relay.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				relay.TrySend(i)
			}
			b.StopTimer()
			for _, sub := range subs {
				sub.Close()
			}
			relay.Close()
		})
	}
}
-- chanrelay_test.go --
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
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testTimeout is the deadline used to detect a stuck implementation.
const testTimeout = 10 * time.Second

// testPanic is used to convert a suspected deadlock into a test failure (rather
// than a hung test binary).
func testPanic() {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	os.Stderr.Write(buf[:n])
	panic(`bigbuff: chanrelay test: timed out (deadlock?)`)
}

// TestChanRelay_SubscribeClose covers the basic lifecycle.
func TestChanRelay_SubscribeClose(t *testing.T) {
	relay := NewChanRelay[int]()
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}

	sub := relay.Subscribe()
	if n := relay.Len(); n != 1 {
		t.Fatalf("expected 1 subscriber, got %d", n)
	}
	if sub.Window() != 0 {
		t.Fatalf("expected synchronous subscription, got window %d", sub.Window())
	}
	if sub.Closed() {
		t.Fatal("expected subscription to be open")
	}

	if !sub.Close() {
		t.Fatal("expected first Close to close the subscription")
	}
	if sub.Close() {
		t.Fatal("expected second Close to be a no-op")
	}
	if !sub.Closed() {
		t.Fatal("expected subscription to be closed")
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("expected Done to be closed")
	}
	if _, ok := sub.Next(); ok {
		t.Fatal("expected Next to report closure")
	}
	if _, ok := sub.TryNext(); ok {
		t.Fatal("expected TryNext to report closure")
	}
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}

	relay.Close()
	if !relay.Closed() {
		t.Fatal("expected relay to be closed")
	}
	select {
	case <-relay.Done():
	default:
		t.Fatal("expected relay Done to be closed")
	}
}

// TestChanRelay_SingleSubscription covers delivery to (and consumption from) a
// single subscription.
func TestChanRelay_SingleSubscription(t *testing.T) {
	relay := NewChanRelay[int]()
	sub := relay.Subscribe()
	defer sub.Close()

	received := make(chan []int, 1)
	go func() {
		var values []int
		for range 5 {
			v, ok := sub.Next()
			if !ok {
				received <- values
				return
			}
			values = append(values, v)
		}
		received <- values
	}()

	for i := range 5 {
		if n := relay.Send(i); n != 1 {
			t.Fatalf("Send: expected 1 receiver, got %d", n)
		}
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if got := <-received; fmt.Sprint(got) != "[0 1 2 3 4]" {
		t.Fatalf("unexpected values: %v", got)
	}
}

// TestChanRelay_BroadcastOrdering covers broadcast to many concurrent
// subscribers, verifying that every subscription observes the same, total,
// order of values.
func TestChanRelay_BroadcastOrdering(t *testing.T) {
	const (
		subscribers = 8
		values      = 200
	)

	relay := NewChanRelay[int]()
	defer relay.Close()

	var (
		wg       sync.WaitGroup
		results  = make([][]int, subscribers)
		start    = make(chan struct{})
		finished = make(chan struct{})
	)

	for i := range subscribers {
		sub := relay.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			<-start
			var got []int
			for {
				select {
				case v := <-sub.C():
					got = append(got, v)
					if len(got) == values {
						results[i] = got
						return
					}
				case <-sub.Done():
					results[i] = got
					return
				}
			}
		}()
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	close(start)
	go func() {
		defer close(finished)
		for i := range values {
			if n := relay.Send(i); n != subscribers {
				panic(fmt.Sprintf("Send: expected %d receivers, got %d", subscribers, n))
			}
		}
	}()

	wg.Wait()
	<-finished

	for i, got := range results {
		if len(got) != values {
			t.Fatalf("subscriber %d: expected %d values, got %d", i, values, len(got))
		}
		for j, v := range got {
			if v != j {
				t.Fatalf("subscriber %d: expected value %d at index %d, got %d", i, j, j, v)
			}
		}
	}
}

// TestChanRelay_SendIsSynchronous verifies that a send to a synchronous
// subscription does not complete until the subscriber has received, and that a
// receive is the acknowledgement (there is no separate handshake).
func TestChanRelay_SendIsSynchronous(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	sent := make(chan int, 1)
	go func() { sent <- relay.Send(42) }()

	select {
	case <-sent:
		t.Fatal("Send returned before the subscriber received")
	case <-time.After(20 * time.Millisecond):
	}

	v, ok := sub.Next()
	if !ok || v != 42 {
		t.Fatalf("unexpected receive: %v %v", v, ok)
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
}

// TestChanRelay_BufferedWindow verifies that a buffered subscription decouples
// the publisher from the subscriber by up to its window, and not further.
func TestChanRelay_BufferedWindow(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.SubscribeWindow(4)
	defer sub.Close()

	if sub.Window() != 4 {
		t.Fatalf("expected window 4, got %d", sub.Window())
	}

	for i := range 4 {
		if n := relay.Send(i); n != 1 {
			t.Fatalf("Send: expected 1 receiver, got %d", n)
		}
	}
	if n := sub.Pending(); n != 4 {
		t.Fatalf("expected 4 pending, got %d", n)
	}
	if n := sub.Dropped(); n != 0 {
		t.Fatalf("expected no drops, got %d", n)
	}

	// The fifth send blocks: the window is full, and nothing is consuming.
	sent := make(chan int, 1)
	go func() { sent <- relay.Send(4) }()

	select {
	case n := <-sent:
		t.Fatalf("Send returned (%d) while the window was full", n)
	case <-time.After(20 * time.Millisecond):
	}

	for i := range 5 {
		v, ok := sub.Next()
		if !ok || v != i {
			t.Fatalf("expected %d, got %d (ok %v)", i, v, ok)
		}
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	if n := sub.Pending(); n != 0 {
		t.Fatalf("expected no pending, got %d", n)
	}
}

// TestChanRelay_TrySendConflates verifies the "latest value wins" behaviour of
// TrySend on a buffered subscription.
func TestChanRelay_TrySendConflates(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.SubscribeWindow(2)
	defer sub.Close()

	const values = 10
	for i := range values {
		if n := relay.TrySend(i); n != 1 {
			t.Fatalf("TrySend: expected 1 receiver, got %d", n)
		}
	}

	if n := sub.Pending(); n != 2 {
		t.Fatalf("expected 2 pending, got %d", n)
	}
	if n := sub.Dropped(); n != values-2 {
		t.Fatalf("expected %d dropped, got %d", values-2, n)
	}

	// The retained values are the most recent ones, in order.
	for _, want := range []int{values - 2, values - 1} {
		v, ok := sub.Next()
		if !ok || v != want {
			t.Fatalf("expected %d, got %d (ok %v)", want, v, ok)
		}
	}
	if _, ok := sub.TryNext(); ok {
		t.Fatal("expected no more values")
	}
}

// TestChanRelay_TrySendSynchronous verifies that TrySend hands values directly
// to a parked receiver of a synchronous subscription, and skips it (counting a
// drop) when it is not receiving.
func TestChanRelay_TrySendSynchronous(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	if n := relay.TrySend(1); n != 0 {
		t.Fatalf("expected no receivers, got %d", n)
	}
	if n := sub.Dropped(); n != 1 {
		t.Fatalf("expected 1 drop, got %d", n)
	}

	received := make(chan int, 1)
	go func() {
		if v, ok := sub.Next(); ok {
			received <- v
		}
		close(received)
	}()

	// Wait for the receiver to park.
	for i := 0; ; i++ {
		if n := relay.TrySend(2); n == 1 {
			break
		}
		if i > 10000 {
			t.Fatal("failed to deliver to a parked receiver")
		}
		time.Sleep(time.Millisecond)
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if v := <-received; v != 2 {
		t.Fatalf("expected 2, got %d", v)
	}
}

// TestChanRelay_SendContext verifies that SendContext bounds the wait, and
// unblocks a publisher.
func TestChanRelay_SendContext(t *testing.T) {
	t.Run("Delivered", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()
		sub := relay.SubscribeWindow(1)
		defer sub.Close()

		n, err := relay.SendContext(context.Background(), 1)
		if err != nil || n != 1 {
			t.Fatalf("expected (1, nil), got (%d, %v)", n, err)
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()
		sub := relay.Subscribe() // synchronous, and never receiving
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		timer := time.AfterFunc(testTimeout, testPanic)
		defer timer.Stop()

		n, err := relay.SendContext(ctx, 1)
		if n != 0 {
			t.Fatalf("expected no receivers, got %d", n)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	})

	t.Run("NilContext", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()
		sub := relay.SubscribeWindow(1)
		defer sub.Close()

		n, err := relay.SendContext(nil, 1)
		if err != nil || n != 1 {
			t.Fatalf("expected (1, nil), got (%d, %v)", n, err)
		}
	})
}

// TestChanRelay_SendSkipsClosed verifies that a subscription closed concurrently
// with (or before) a send neither receives the value, nor blocks the send, nor
// contributes to the count.
func TestChanRelay_SendSkipsClosed(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	var subs []*RelaySubscription[int]
	for range 4 {
		subs = append(subs, relay.SubscribeWindow(2))
	}
	subs[1].Close()
	subs[3].Close()

	if n := relay.Send(7); n != 2 {
		t.Fatalf("expected 2 receivers, got %d", n)
	}
	if n := relay.TrySend(8); n != 2 {
		t.Fatalf("expected 2 receivers, got %d", n)
	}

	for i, sub := range subs {
		if i == 1 || i == 3 {
			continue
		}
		defer sub.Close()
		for _, want := range []int{7, 8} {
			v, ok := sub.Next()
			if !ok || v != want {
				t.Fatalf("subscriber %d: expected %d, got %d (ok %v)", i, want, v, ok)
			}
		}
	}
}

// TestChanRelay_CloseReleasesPublisher verifies the robustness property that
// closing a subscription releases a publisher blocked on it (which is the class
// of deadlock that ChanPubSub must panic to avoid).
func TestChanRelay_CloseReleasesPublisher(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	live := relay.SubscribeWindow(1)
	defer live.Close()
	stalled := relay.Subscribe() // synchronous, and never receiving

	sent := make(chan int, 1)
	go func() { sent <- relay.Send(1) }()

	select {
	case <-sent:
		t.Fatal("Send returned while a synchronous subscriber was not receiving")
	case <-time.After(20 * time.Millisecond):
	}

	// Closing the stalled subscriber from a *different* goroutine must release
	// the publisher, even though the publisher is blocked on it.
	if !stalled.Close() {
		t.Fatal("expected Close to close the subscription")
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	if v, ok := live.Next(); !ok || v != 1 {
		t.Fatalf("expected 1, got %d (ok %v)", v, ok)
	}
}

// TestChanRelay_RelayClose verifies that closing the relay closes every
// subscription, and that subsequent sends are no-ops.
func TestChanRelay_RelayClose(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(10))

	const subscribers = 32
	var subs []*RelaySubscription[int]
	for range subscribers {
		subs = append(subs, relay.Subscribe())
	}
	for i := range 10 {
		if n := relay.Send(i); n != subscribers {
			t.Fatalf("Send: expected %d receivers, got %d", subscribers, n)
		}
	}
	if n := relay.Len(); n != subscribers {
		t.Fatalf("expected %d subscribers, got %d", subscribers, n)
	}

	relay.Close()
	relay.Close() // idempotent

	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}
	if n := relay.Send(1); n != 0 {
		t.Fatalf("expected no receivers, got %d", n)
	}
	if n := relay.TrySend(1); n != 0 {
		t.Fatalf("expected no receivers, got %d", n)
	}
	if _, err := relay.SendContext(context.Background(), 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i, sub := range subs {
		select {
		case <-sub.Done():
		default:
			t.Fatalf("subscriber %d: expected Done to be closed", i)
		}
		if !sub.Closed() {
			t.Fatalf("subscriber %d: expected Closed", i)
		}
		// Closure is preferred over buffered values, by Next and TryNext.
		if _, ok := sub.TryNext(); ok {
			t.Fatalf("subscriber %d: expected closure to take precedence", i)
		}
		// The values are still buffered for a raw receive, though.
		var got []int
		for {
			select {
			case v := <-sub.C():
				got = append(got, v)
				continue
			default:
			}
			break
		}
		if len(got) != 10 { // window
			t.Fatalf("subscriber %d: expected 10 values, got %v", i, got)
		}
	}

	// Subscribing to a closed relay yields a closed subscription.
	sub := relay.Subscribe()
	if !sub.Closed() {
		t.Fatal("expected subscription to a closed relay to be closed")
	}
	sub.Close()
}

// TestChanRelay_SubscribeAfterClose covers subscription to a closed relay.
func TestChanRelay_SubscribeAfterClose(t *testing.T) {
	relay := NewChanRelay[int]()
	relay.Close()

	for _, sub := range []*RelaySubscription[int]{
		relay.Subscribe(),
		relay.SubscribeWindow(1),
		relay.SubscribeChan(make(chan int, 1)),
	} {
		if !sub.Closed() {
			t.Fatal("expected a closed subscription")
		}
		if _, ok := sub.Next(); ok {
			t.Fatal("expected Next to report closure")
		}
		sub.Close()
	}
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}
}

// TestChanRelay_SubscribeChan covers delivery through a caller-owned channel.
func TestChanRelay_SubscribeChan(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	channel := make(chan int, 3)
	sub := relay.SubscribeChan(channel)
	if sub.C() != (chan int)(channel) {
		// (<-chan int) vs (chan int) comparison is not directly possible; the
		// assertion below is the meaningful one.
		_ = channel
	}
	if sub.Window() != 3 {
		t.Fatalf("expected window 3, got %d", sub.Window())
	}

	if n := relay.Send(1); n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	for i := 2; i <= 6; i++ {
		relay.TrySend(i) // conflates, once full
	}
	var got []int
	for {
		v, ok := sub.TryNext()
		if !ok {
			break
		}
		got = append(got, v)
	}
	if fmt.Sprint(got) != "[4 5 6]" {
		t.Fatalf("unexpected values: %v", got)
	}
	if n := sub.Dropped(); n != 3 {
		t.Fatalf("expected 3 drops, got %d", n)
	}

	sub.Close()
	close(channel) // ownership remains with the caller

	if _, ok := channelOk(channel); ok {
		_ = ok
	}
}

// channelOk drains a channel, reporting whether it was open.
func channelOk(ch chan int) (int, bool) {
	v, ok := <-ch
	return v, ok
}

// TestChanRelay_SubscribeContext covers the iterator API.
func TestChanRelay_SubscribeContext(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		seq := relay.SubscribeContext(context.Background())

		go func() {
			for i := range 5 {
				relay.Send(i)
			}
		}()

		var got []int
		for v := range seq {
			got = append(got, v)
			if len(got) == 5 {
				break
			}
		}
		if fmt.Sprint(got) != "[0 1 2 3 4]" {
			t.Fatalf("unexpected values: %v", got)
		}
		if n := relay.Len(); n != 0 {
			t.Fatalf("expected the subscription to be released, got %d", n)
		}
	})

	t.Run("Cancel", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		ctx, cancel := context.WithCancel(context.Background())
		sub := relay.SubscribeWindow(1)
		defer sub.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for range relay.SubscribeContext(ctx) {
				// unreachable
			}
		}()

		// One extra subscription (from SubscribeContext), plus ours.
		for relay.Len() != 2 {
			runtime.Gosched()
		}
		cancel()

		timer := time.AfterFunc(testTimeout, testPanic)
		defer timer.Stop()

		<-done
		if n := relay.Len(); n != 1 {
			t.Fatalf("expected 1 remaining subscription, got %d", n)
		}
	})

	t.Run("NeverIterated", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		ctx, cancel := context.WithCancel(context.Background())
		_ = relay.SubscribeContext(ctx)
		if n := relay.Len(); n != 1 {
			t.Fatalf("expected 1 subscription, got %d", n)
		}
		cancel()

		timer := time.AfterFunc(testTimeout, testPanic)
		defer timer.Stop()

		for relay.Len() != 0 {
			runtime.Gosched()
		}
	})

	t.Run("NilYield", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected a panic")
			}
			if n := relay.Len(); n != 0 {
				t.Fatalf("expected the subscription to be released, got %d", n)
			}
		}()

		relay.SubscribeContext(context.Background())(nil)
	})
}

// TestChanRelay_Churn covers subscription churn, exercising registry growth and
// (amortised) compaction, and verifying that surviving subscriptions are
// neither lost nor delivered to more than once.
func TestChanRelay_Churn(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	defer relay.Close()

	const (
		generations = 12
		perGen      = relayChunkSize + 13 // force chunk chains and compaction
	)

	var live []*RelaySubscription[int]
	for gen := range generations {
		var next []*RelaySubscription[int]
		for range perGen {
			next = append(next, relay.Subscribe())
		}
		// Half of the live subscriptions survive this generation.
		for i, sub := range live {
			if i%2 == 0 {
				next = append(next, sub)
			} else {
				sub.Close()
			}
		}

		value := gen
		if n := relay.Send(value); n != len(next) {
			t.Fatalf("generation %d: expected %d receivers, got %d", gen, len(next), n)
		}
		if n := relay.Len(); n != len(next) {
			t.Fatalf("generation %d: expected Len %d, got %d", gen, len(next), n)
		}

		// Every subscription must have received exactly this generation's value,
		// i.e. exactly once (duplicate registry entries would show up here).
		for i, sub := range next {
			if got, ok := sub.TryNext(); !ok || got != value {
				t.Fatalf("generation %d subscriber %d: expected %d, got %d (ok %v)", gen, i, value, got, ok)
			}
			if _, ok := sub.TryNext(); ok {
				t.Fatalf("generation %d subscriber %d: expected a single delivery", gen, i)
			}
		}

		live = next
	}
}

// TestChanRelay_ConcurrentPublishers verifies that concurrent publishers produce
// a single total order, observed identically by every subscription.
func TestChanRelay_ConcurrentPublishers(t *testing.T) {
	const (
		publishers  = 4
		perPub      = 250
		subscribers = 4
	)

	relay := NewChanRelay[int](WithRelayWindow(8))
	defer relay.Close()

	var (
		wg      sync.WaitGroup
		results = make([][]int, subscribers)
	)

	for i := range subscribers {
		sub := relay.SubscribeWindow(16)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			for {
				v, ok := sub.Next()
				if !ok {
					results[i] = nil
					return
				}
				results[i] = append(results[i], v)
				if len(results[i]) == publishers*perPub {
					return
				}
			}
		}()
	}

	var pubWg sync.WaitGroup
	for p := range publishers {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for i := range perPub {
				relay.Send(p*perPub + i)
			}
		}()
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	pubWg.Wait()
	wg.Wait()

	for i := 1; i < subscribers; i++ {
		if fmt.Sprint(results[i]) != fmt.Sprint(results[0]) {
			t.Fatalf("subscriber %d observed a different order than subscriber 0", i)
		}
	}
	for _, v := range results[0] {
		_ = v
	}
	if len(results[0]) != publishers*perPub {
		t.Fatalf("expected %d values, got %d", publishers*perPub, len(results[0]))
	}
	seen := make(map[int]struct{}, len(results[0]))
	for _, v := range results[0] {
		if _, ok := seen[v]; ok {
			t.Fatalf("duplicate value %d", v)
		}
		seen[v] = struct{}{}
	}
}

// TestChanRelay_Stress runs publishers, subscribers, consumers, and churn
// concurrently, and checks the invariants that must hold under any interleaving:
// no panics, no deadlocks, and per-subscription ordering (a subscription may miss
// values, because it was created late, or because TrySend conflated, but never
// observes them out of order, or twice).
func TestChanRelay_Stress(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(2))
	defer relay.Close()

	const (
		duration   = 250 * time.Millisecond
		publishers = 3
	)

	var (
		stop     = make(chan struct{})
		wg       sync.WaitGroup
		failures atomic.Int32
	)

	// Publishers. Each publisher stamps its values with (publisher, sequence),
	// so that consumers can verify the total order guarantee: values are
	// dispatched in a global order, so every subscription must observe each
	// publisher's values in strictly increasing sequence.
	for p := range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seq int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				value := int(int64(p)<<20 | (seq & 0xfffff))
				seq++
				if rand.Intn(2) == 0 {
					relay.Send(value)
				} else {
					relay.TrySend(value)
				}
			}
		}()
	}

	// Consumer manager: churning subscribers, each of which verifies that it
	// observes values (identified by their publish sequence number) in strictly
	// increasing order.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				window := rand.Intn(3)
				var sub *RelaySubscription[int]
				if rand.Intn(2) == 0 {
					sub = relay.SubscribeWindow(window)
				} else {
					sub = relay.SubscribeChan(make(chan int, window))
				}
				last := [publishers]int{}
				for i := range last {
					last[i] = -1
				}
				for {
					v, ok := sub.TryNext()
					if !ok {
						break
					}
					p, seq := v>>20, v&0xfffff
					if p >= publishers || seq <= last[p] {
						failures.Add(1)
					} else {
						last[p] = seq
					}
					if rand.Intn(8) == 0 {
						break
					}
				}
				sub.Close()
			}
		}()
	}

	// A long-lived synchronous consumer, to exercise blocking sends. It is
	// released by closing its subscription from another goroutine, while it is
	// blocked in Next - which the relay supports by construction.
	sub := relay.Subscribe()
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-stop
		sub.Close()
	}()
	go func() {
		defer wg.Done()
		defer sub.Close()
		for {
			if _, ok := sub.Next(); !ok {
				return
			}
		}
	}()

	time.AfterFunc(duration, func() { close(stop) })
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		buf := make([]byte, 1<<22)
		n := runtime.Stack(buf, true)
		os.Stderr.Write(buf[:n])
		panic(`stress: hang`)
	}

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d ordering violations observed", n)
	}

	// The relay must still be usable, and consistent.
	relay.Close()
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers after Close, got %d", n)
	}
}

// TestChanRelay_NoAllocationOnSend verifies the zero-allocation publish path.
func TestChanRelay_NoAllocationOnSend(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	const subscribers = 16
	var subs []*RelaySubscription[int]
	for range subscribers {
		subs = append(subs, relay.SubscribeWindow(1))
	}
	defer func() {
		for _, sub := range subs {
			sub.Close()
		}
	}()

	drain := func() {
		for _, sub := range subs {
			if _, ok := sub.TryNext(); !ok {
				t.Fatal("expected a value")
			}
		}
	}

	if n := testing.AllocsPerRun(100, func() {
		relay.Send(1)
		drain()
		relay.TrySend(2)
		drain()
	}); n != 0 {
		t.Fatalf("expected zero allocations per send, got %f", n)
	}
}

// TestChanRelay_NoAllocationOnSubscribe ensures subscription churn is cheap: no
// allocation is attributable to the relay's registry.
func TestChanRelay_AllocationsPerSubscription(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	n := testing.AllocsPerRun(100, func() {
		sub := relay.SubscribeWindow(1)
		sub.Close()
	})
	// The subscription itself, its transport, and its done channel.
	if n > 3 {
		t.Fatalf("expected at most 3 allocations per subscription, got %f", n)
	}
}

// TestChanRelay_CloseIsIndependentOfPublishers verifies that closing a
// subscription neither blocks on, nor is blocked by, a publisher that is stalled
// on a different subscription. It is the property that lets a churning
// subscriber population coexist with an arbitrary slow consumer, and it follows
// from the fact that no lock is shared between Close and the publish path.
func TestChanRelay_CloseIsIndependentOfPublishers(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	stalled := relay.Subscribe() // never receives
	awaiting := relay.SubscribeWindow(1)
	defer awaiting.Close()
	deferred := relay.SubscribeWindow(1)

	sent := make(chan int, 1)
	go func() { sent <- relay.Send(1) }()

	// Wait for the publisher to stall on the first subscription.
	select {
	case <-sent:
		t.Fatal("Send returned while a synchronous subscriber was not receiving")
	case <-time.After(20 * time.Millisecond):
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	// Closing an unrelated subscription must not wait for the stalled publisher.
	closed := make(chan bool, 1)
	go func() { closed <- deferred.Close() }()

	select {
	case ok := <-closed:
		if !ok {
			t.Fatal("expected Close to close the subscription")
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked on a publisher stalled elsewhere")
	}

	// Closing the stalled subscription releases the publisher.
	stalled.Close()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	if v, ok := awaiting.Next(); !ok || v != 1 {
		t.Fatalf("expected 1, got %d (ok %v)", v, ok)
	}
}

// TestChanRelay_TrySendDoesNotWaitForSubscribers verifies that TrySend neither
// waits for a subscriber that is not receiving, nor for one whose buffer is
// full.
func TestChanRelay_TrySendDoesNotWaitForSubscribers(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	stalled := relay.Subscribe() // synchronous, and never receiving
	defer stalled.Close()
	full := relay.SubscribeWindow(1)
	defer full.Close()

	relay.TrySend(1) // fills the buffered subscription

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	done := make(chan int, 1)
	go func() { done <- relay.TrySend(2) }()

	select {
	case n := <-done:
		// Only the buffered subscription is counted (it conflated 1 -> 2); the
		// stalled one is skipped.
		if n != 1 {
			t.Fatalf("expected 1 receiver, got %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("TrySend waited for a subscriber")
	}

	if n := stalled.Dropped(); n != 2 {
		t.Fatalf("expected 2 drops, got %d", n)
	}
	if v, ok := full.Next(); !ok || v != 2 {
		t.Fatalf("expected 2, got %d (ok %v)", v, ok)
	}
}

// TestChanRelay_MultipleConsumers verifies that a subscription may be consumed
// by multiple goroutines, splitting its values, without loss or duplication.
func TestChanRelay_MultipleConsumers(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	const (
		consumers = 4
		values    = 500
	)

	var (
		wg       sync.WaitGroup
		once     sync.Once
		finished = make(chan struct{})
		mu       sync.Mutex
		seen     = make(map[int]int, values)
	)

	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-finished:
					return
				default:
				}
				v, ok := sub.TryNext()
				if !ok {
					if sub.Closed() {
						return
					}
					runtime.Gosched()
					continue
				}
				mu.Lock()
				seen[v]++
				complete := len(seen) == values
				mu.Unlock()
				if complete {
					once.Do(func() { close(finished) })
					return
				}
			}
		}()
	}

	// Safety net: release the consumers if anything goes wrong.
	time.AfterFunc(testTimeout/2, func() { sub.Close() })

	for i := range values {
		relay.Send(i)
	}

	wg.Wait()

	for i := range values {
		if got := seen[i]; got != 1 {
			t.Fatalf("value %d delivered %d times", i, got)
		}
	}
}
-- chanrelay.go --
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
		live       int32
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
			live++
		}
	}

	if head == nil {
		head = new(relayChunk[V])
		tail = head
	}

	x.head, x.tail = head, tail
	x.headPub.Store(head)
	x.grey = 0
	x.count.Store(live)
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
-- CHANRELAY.md --
# ChanRelay — per-subscription transports, and the fusion invariant

`ChanRelay` is a broadcast (publish/subscribe) primitive for Go that delivers each
published value to every live subscription **without goroutines, without callbacks,
and without allocating on the publish path** — while keeping every subscription
independently usable inside a `select` statement.

It is the third member of the package's family, and it is designed against the
two that already exist:

| | transport | selectable | per-publish cost | delivery acknowledgement | failure mode on misuse |
|---|---|---|---|---|---|
| `ChanCaster` | one shared unbuffered channel | yes (shared) | N channel sends | the send itself (one-shot: `Add` arms receivers, `Send` consumes them) | panic / silent missed receives |
| `ChanPubSub` | one shared unbuffered channel | yes (shared) | N sends + 1 `sync.Cond` broadcast + N `Wait` calls | mandatory `Wait()` per receive ("ping/pong") | panic (`broken`, state invariant violations) |
| `ChanRelay` | **one channel per subscription** | yes (**per subscription**) | N channel operations, **no wakeup storm, no per-sub bookkeeping** | **the receive itself** (fused) | skipped or back-pressured; never panics, never deadlocks |

---

## 1. What the existing designs cost

`ChanCaster` broadcasts by performing exactly one synchronous send into a single
unbuffered channel per registered receiver. That is a beautiful trick (`Send`
cannot return until every receiver has been handed the value), but it forces
every broadcast into one FIFO, so subscribers are not independent: a value is
"one of N tokens", and the accounting for who is entitled to which token
(`Add(math.MaxInt32 + n)` in the low word of `state`) is subtle enough that the
implementation has to panic if the bookkeeping is ever violated.

`ChanPubSub` layers selectable, multi-subscriber semantics on top of that using a
**ping/pong protocol**: the publisher arms `n` pings, sends `n` values into the
shared channel, then blocks on a condition variable until `n` calls to `Wait()`
have arrived. This is what buys select support, but it costs:

* a mandatory post-receive handshake (`Wait`) that cannot be forgotten, hence a
  seven-point API contract, hence panics on any contract violation;
* **one `sync.Cond.Broadcast` per publish**, which wakes every waiter and then
  serialises them all on one mutex — a thundering herd whose cost grows with the
  number of subscribers;
* subscriber *removal* must synchronise with an in-flight send (`TryRLock` +
  `ping.Add(0)` spin), because the ping count is a shared quantity: churn is
  O(contention), not O(1);
* a shared FIFO, so "who received which value" is a global problem.

The two things that make `ChanPubSub` great — selectable delivery, and not
allocating per publish — are exactly the two things a naive per-subscription
design loses. The point of `ChanRelay` is that **you do not have to lose either**.

## 2. The idea: one transport per subscription, never closed

Each subscription owns a channel. The relay sends to it; the subscriber receives
from it. Immediately, two classic objections appear:

1. *"You cannot close a channel you send to."* Correct — so the relay **never
   closes it**. Closure is signalled out of band, through a dedicated `done`
   channel, exactly as `context.Context` separates *values* from *cancellation*.
   That is why the type system helps here: a subscriber only ever sees a
   `<-chan V`, so it *cannot* close the transport by accident, and a receive
   therefore never observes a closed channel, never races a close, and never
   panics.
2. *"Then a publisher can block forever on a subscription that has gone away."*
   This is solved by never performing an unguarded blocking operation:

```go
select {
case sub.c <- value: // deliver
case <-sub.done:     // the subscription went away: skip it
}
```

Every potentially unbounded wait in the implementation has a `done` (or `ctx`)
case attached to it. That single rule converts the whole class of liveness bugs
that `ChanPubSub` has to *panic* to escape into an ordinary, recoverable outcome:
a closed subscriber is observed, skipped, and the publish continues.

### The fusion invariant

> A receive from `sub.C()` **is** the acknowledgement.

`ChanPubSub` splits a delivery into two operations (receive, then `Wait`), because
its transport is shared and its publisher needs a rendezvous. A relay has no
rendezvous to schedule: the value is placed into the subscriber's own transport,
and the act of placing it (or buffering it) *is* the delivery. Consequently:

* there is no handshake to forget — the requirement cannot be violated;
* there is no "stealing": each transport is private to one subscription, so a
  subscriber that receives twice cannot take a value from a peer;
* there is no global accounting object that can be corrupted, so **there is no
  broken state** and nothing to panic about.

## 3. The registry: churn without copies

Publishers must enumerate live subscriptions, in O(live), without allocating and
without taking a lock that a subscriber can hold. The registry is therefore an
**append-only chain of fixed-size chunks** (64 slots), published to publishers
through a single atomic head pointer:

```
head ──▶ chunk ──▶ chunk ──▶ chunk
          [ s0 s1 s2 … ]        (append-only, slots written once)
```

* **subscribe** appends into the tail chunk (`slot.Store(sub)` then `n.Store(n+1)`,
  a release/acquire pair, so a publisher that observes `n` also observes the
  slots). One chunk allocation per 64 subscriptions; never a copy.
* **close** clears the subscription's own slot with a single atomic store, and
  decrements a counter. Publishers skip `nil` slots in O(1). No lock is taken, so
  **close is never blocked by a publish, and never blocks one** — including a
  publish that is blocked on a *different* subscriber. (`ChanPubSub.Close`
  cannot make that claim: its `Add(-1)` must synchronise with the in-flight
  send's ping count.)
* slots vacated this way are *grey*. When grey slots outnumber live ones (and
  there are at least 64), the **detaching goroutine** rebuilds the chain in
  O(live) — amortised O(1) per detach, and entirely off the publish path. A
  publish that started earlier keeps walking the old chain, which is immutable
  and still valid, so a rebuild can never produce a missed or duplicated
  delivery; it only affects subscriptions created concurrently with it (the same
  "happens-before" caveat as any concurrent subscribe).

The result is a registry that scales with **both** dimensions the brief cares
about: many subscribers (linear scan over contiguous pointer blocks, no map, no
snapshot allocation per publish) and high churn (O(1) detach, amortised
compaction, no shared lock).

## 4. The publish path

```go
sendMu.Lock()                  // serialises publishes: the total order guarantee
for chunk := head; chunk != nil; chunk = chunk.next {
    for i := 0; i < chunk.n; i++ {
        sub := chunk.slots[i].Load()
        if sub == nil || sub.closed() { continue }   // grey / closed: O(1)
        sub.send(value, ctx)                          // 1-3 channel ops, no alloc
    }
}
sendMu.Unlock()
```

Three dispatch modes share this loop, differing only in the per-subscription
operation:

| mode | per-subscription operation | semantics |
|---|---|---|
| `Send(v)` | non-blocking send; if that fails, blocking send guarded by `done` | reliable, back-pressured. Window 0: returns only once the value has been handed to every live subscriber. Window n: blocks only while a subscriber is more than n values behind. |
| `TrySend(v)` | non-blocking send only; on a full buffer, discard the oldest and retry | never waits for a subscriber. Latency-optimal: values are handed directly to parked receivers. Conflates ("latest value wins") for buffered subscriptions, and counts skips per subscription. |
| `SendContext(ctx, v)` | as `Send`, plus a `ctx.Done()` case | bounds the wait. |

`Send` first *tries* the non-blocking send, which is not just a micro
optimisation: it means a subscriber that is ready is never held up behind one
that is not, so the fan-out is O(N) work but not O(sum of subscriber latencies)
in the common case.

Serialisation by one mutex is what gives the **total order** guarantee — every
subscription observes the same sequence of values, and publishes form a strict
sequence. It is the same cost `ChanPubSub` pays (`sendMu`), but it is the *only*
lock a publisher takes, and no subscriber ever takes it.

## 5. Invariants

**I1 (no lost wakeup).** A value is delivered to a subscription iff the relay
performed a successful channel operation on its transport, or the value was
buffered; `sent` counts exactly those. A concurrently closed subscription is
skipped, deterministically preferred over delivery (`done` is checked first).

**I2 (order).** Publishes are serialised, and each subscription's transport is a
FIFO owned by a single producer for the duration of a publish. Hence the
per-subscription sequence is the publish sequence — *and* all subscriptions agree
on it. `TrySend`'s conflation deletes values, but never reorders them.

**I3 (no lock cycle).** `sendMu` is taken by publishers only; `mu` (registry) is
taken by subscribe/detach/compaction only; neither is ever acquired while holding
the other. Therefore `Close`, `Subscribe`, detach, and compaction can never wait
on a publish, and a publish can never wait on them.

**I4 (no deadlock, only back-pressure).** Every wait in the implementation is a
`select` with a `done` case (and a `ctx` case where applicable). A subscription
that never receives can only stall a *reliable* publish, and closing it releases
that publish. There is no state that a delayed close can corrupt, because the
only state is each subscriber's own channel: the runtime guarantees its
integrity.

**I5 (compaction preserves the live set).** Compaction copies every slot that is
non-`nil` **and** not closed into fresh chunks, then publishes the new head. A
publisher traverses exactly one chain, so no subscription can appear in a
traversal twice; a closed subscription can only be *dropped*, never resurrected.

**I6 (zero allocation per publish).** No publish mode allocates: the registry is
pre-allocated in chunks, `nil`-slot skips are branch-only, the `sendMu` is a
field, and `TrySend`'s conflation mutates the existing transport. Enforced by
`TestChanRelay_NoAllocationOnSend` and by the `0 B/op, 0 allocs/op` column of
every `Send`/`TrySend` benchmark below.

## 6. Cost model

| operation | cost |
|---|---|
| `Send`/`TrySend` | one mutex (uncontended unless publishers are concurrent), one atomic load + one closed-check + 1-3 channel operations per live subscription, **0 allocations** |
| receive | one channel operation (plus `Done` select, if used) |
| `Subscribe` | 1 append + 3 small allocations (subscription, transport, `done`) — 2 with `SubscribeChan`, which uses a caller-owned transport; 1 chunk allocation per 64 subscriptions |
| `Close` | 1 atomic store + 1 atomic decrement + (amortised) share of a compaction |
| goroutines | none, ever (`SubscribeContext` registers one context callback while the iterator is dormant, exactly like `ChanPubSub`) |

Allocation is therefore a function of **subscription churn only, never of publish
volume**: a relay that publishes a billion values allocates nothing at all. The
heap profile is flat and predictable — no per-publish slice, no per-send closure,
no per-subscriber cell, no free list to drain — which is what makes the
allocation behaviour stable rather than merely small.

## 7. Measurements

`go test -bench . -benchtime 500ms -count=3` (median), Go 1.25, linux/amd64,
**2 vCPU** (a deliberately starved box: these benchmarks are dominated by
goroutine wakeups, so absolute numbers are pessimistic and noisy, but the
comparison is fair), N concurrently receiving subscribers, all synchronous
(window 0) except where noted. Allocatable/op is `0` in every row.

| broadcast (ns/op) | 1 | 8 | 64 | 512 |
|---|---|---|---|---|
| `CallbackNoop` (function calls only — the floor) | 2.3 | 14 | 102 | 848 |
| `CallbackChan` (a callback per subscriber, each doing a channel handoff — the "callback channel" baseline) | 271 | 2389 | 23157 | 230599 |
| `ChanCaster` (`Add` + linear sends) | 252 | 3523 | 54246 | 415099 |
| `ChanPubSub` (ping/pong + cond broadcast) | 547 | 2223 | 23874 | 429136 |
| **`ChanRelay.Send`** (window 0) | **349** | **3070** | **29043** | **268863** |
| `ChanRelay.SendBuffered` (window 16) | 133 | 1302 | 7840 | 66734 |
| **`ChanRelay.TrySend`** (parked receivers) | **42** | **753** | **6184** | **51575** |
| `ChanRelay.TrySendBuffered` | 79 | 474 | 3872 | 35701 |
| `ChanRelay.TrySendSkipped` (no receiver ready — pure dispatch overhead) | 38 | 208 | 1657 | 13352 |

Per-receiver cost at N = 512: callback channel 450 ns, `ChanCaster` 811 ns,
`ChanPubSub` 838 ns, **relay (synchronous) 525 ns, relay (buffered) 130 ns,
relay (`TrySend`) 101 ns, relay (dispatch overhead only) 26 ns.**

Churn (one subscribe + one close):

| | ns/op | allocations |
|---|---|---|
| `ChanPubSub.Add(1)` + `Add(-1)`, parallel, 64 live subscribers, publishing | 387 | 0 |
| **`ChanRelay` subscribe + close**, parallel, 64 live subscribers | **280** | 3 (314 B) |
| `ChanRelay` subscribe + close, serial | 209 | 3 (296 B) |
| `ChanRelay` subscribe + close, 1024 live subscribers (compaction active) | 246 | 3 (306 B) |

Read together:

* In the *synchronous* mode — the mode `ChanPubSub` offers — the relay is within
  ~1.2-1.3× of the raw callback-channel baseline and **1.6× faster than
  `ChanPubSub` at 512 subscribers**, though 1.2-1.4× slower than `ChanPubSub` at
  8-64 subscribers, where a single cond broadcast waking one goroutine each is
  genuinely excellent. (And at 2 vCPUs, synchronous fan-out is essentially a
  wakeup-storm benchmark: `ChanPubSub`'s ping phase hands all N values out
  before any receiver runs, while the relay's first, non-blocking pass does the
  same but its blocking fallback serialises behind receivers that are between
  receives.)
* In *buffered* and *TrySend* modes — which `ChanPubSub` cannot express at all —
  the relay is **3-8× faster per receiver**, because there is no cond broadcast,
  no `Wait`, and no per-subscriber accounting: just a buffered channel operation.
* Dispatch *overhead* (no delivery) is ~26 ns per subscriber at N = 512, versus
  1.7 ns for a bare callback: ~1.5 cache-missing loads and 3 branches per
  subscriber. That is the honest price of the registry + closed checks.
* Subscription churn is O(1), and 1.4× cheaper than `ChanPubSub`'s (`Add(-1)`
  must synchronise with in-flight sends), with a bounded, *stable* allocation
  profile: 3 small objects, ~300 B per subscription — and 2 with
  `SubscribeChan`. Compaction is invisible in the churn cost (246 ns/op with 1024
  live subscribers vs 209 ns/op with none).

## 8. What you get that the older designs cannot express

```go
// A selectable, independent subscriber. No contract, no handshake.
sub := relay.Subscribe()
defer sub.Close()
select {
case v := <-sub.C():
case <-sub.Done():
case <-ctx.Done():
}

// Heterogeneous fan-out: some subscribers are synchronous (back-pressured),
// some are buffered (decoupled), some conflate. One publish serves them all.
sync1 := relay.Subscribe()          // window 0
buf := relay.SubscribeWindow(64)    // queue up to 64
latest := relay.SubscribeWindow(1)  // "latest value wins"
relay.Send(v)      // blocks only for sync1, and only for buf's backlog
relay.TrySend(v)   // blocks for nobody; conflates `latest`, skips a busy sync1

// Lossy telemetry: never block the producer, always keep the newest sample.
relay.TrySend(sample)

// Lossless stream with bounded memory:
for v := range relay.SubscribeContext(ctx) { /* ... */ }

// Bring your own transport (zero-allocation subscriptions, or an existing
// channel with its own producers, consumed with care):
sub := relay.SubscribeChan(existing)

// Observability, per subscription:
sub.Pending()  // backlog
sub.Dropped()  // values skipped or conflated
```

Because the acknowledgement is fused into the receive, the "spy" pattern that
`ChanPubSub` handles with its ping-count machinery is just... a normal select:

```go
select {
case v := <-sub.C():
	handle(v)
default:
	// no value pending: do other work, then select again
}
```

## 9. Deliberate limits

* **Publishes are serialised.** That is where the total order comes from, and it
  is the same trade-off `ChanPubSub` makes. A publish that stalls on a
  synchronous subscriber therefore delays *other publishers*: use buffered
  subscriptions (which make `Send` non-blocking) or `SendContext` when that
  matters.
* **No "delivered but unprocessed" handshake.** `ChanPubSub.Send` returns when
  every subscriber has *received* (its `Wait` precedes the yield); the relay's
  `Send` likewise returns on receipt, not on processing. There is deliberately no
  way to wait for processing: that is what a callback would give you, and it
  would reintroduce the head-of-line blocking the transport design exists to
  avoid.
* **No cross-subscription atomicity.** A subscription created concurrently with a
  publish may or may not see it (documented; it is the only sane answer).
* **`SubscribeChan` requires discipline**: the caller owns the channel, must not
  close it while the subscription is live, and must not write to it if conflation
  is expected to be lossless.
* **Per-subscription transports are not free**: 3 small allocations per
  subscription. That is the price of selectability without a shared FIFO, and it
  is paid on the churn path, not the publish path.

## 10. Testing

`chanrelay_test.go` covers, under `-race`:

* lifecycle, ordering, broadcast to 8 concurrent subscribers over 200 values;
* synchronous acknowledgement, buffering, and the exact point at which `Send`
  blocks;
* conflation (`[4 5 6]` from ten `TrySend`s into a window of 3), skip counting;
* `SendContext` deadline and nil-context paths; iterator cancellation, early
  break, nil yield, and never-iterated cases;
* **deadlock-freedom**: a publisher blocked on a silent subscriber is released by
  closing that subscriber; closing an unrelated subscription while a publisher is
  stalled returns immediately;
* registry growth, compaction, and duplicate-delivery detection across 12
  generations of `relayChunkSize + 13` subscriptions;
* concurrent publishers producing one total order observed identically by every
  subscriber;
* a 250 ms adversarial stress run (3 publishers, 4 churning consumer managers, a
  synchronous long-lived consumer, mid-receive closes) asserting monotonic
  per-publisher ordering and no hangs;
* multiple concurrent consumers on one subscription: 4 goroutines, 500 values, no
  loss and no duplication;
* zero allocations per publish (`testing.AllocsPerRun`) and ≤ 3 allocations per
  subscription.
-- example_test.go --
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

package bigbuff_test

import (
	"context"
	"fmt"
	"time"

	bigbuff "github.com/joeycumines/go-bigbuff"
)

// ExampleChanRelay_select demonstrates the core property of a [ChanRelay]: each
// subscription is independently selectable, and a receive is the
// acknowledgement, so there is no handshake to remember.
func ExampleChanRelay_select() {
	relay := bigbuff.NewChanRelay[int]()
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	go func() {
		relay.Send(1)
		relay.Send(2)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for received := 0; received < 2; {
		select {
		case v := <-sub.C():
			fmt.Println("received", v)
			received++
		case <-sub.Done():
			return
		case <-ctx.Done():
			return
		}
	}
	fmt.Println("done")
	// Output:
	// received 1
	// received 2
	// done
}

// ExampleChanRelay_conflate demonstrates conflation: a buffered subscription
// that falls behind retains the most recent values, and never blocks a
// publisher.
func ExampleChanRelay_conflate() {
	relay := bigbuff.NewChanRelay[int]()
	defer relay.Close()

	sub := relay.SubscribeWindow(2) // keep at most two values
	defer sub.Close()

	for i := range 5 {
		relay.TrySend(i)
	}

	fmt.Println("pending", sub.Pending())
	fmt.Println("dropped", sub.Dropped())

	for {
		v, ok := sub.TryNext()
		if !ok {
			break
		}
		fmt.Println("received", v)
	}

	// Output:
	// pending 2
	// dropped 3
	// received 3
	// received 4
}

// ExampleChanRelay_SubscribeContext demonstrates the iterator API, which closes
// its subscription automatically.
func ExampleChanRelay_SubscribeContext() {
	relay := bigbuff.NewChanRelay[int]()
	defer relay.Close()

	seq := relay.SubscribeContext(context.Background())

	go func() {
		for i := range 3 {
			relay.Send(i)
		}
	}()

	for v := range seq {
		fmt.Println("received", v)
		if v == 2 {
			break
		}
	}

	fmt.Println("subscribers", relay.Len())

	// Output:
	// received 0
	// received 1
	// received 2
	// subscribers 0
}

// ExampleChanRelay_TrySend demonstrates the low latency dispatch mode, in which
// a subscription that is ready is served immediately, and one that is not is
// skipped.
func ExampleChanRelay_TrySend() {
	relay := bigbuff.NewChanRelay[int]()
	defer relay.Close()

	busy := relay.Subscribe()  // synchronous, and not receiving
	ready := relay.Subscribe() // synchronous, and receiving
	defer busy.Close()
	defer ready.Close()

	received := make(chan int, 1)
	go func() {
		if v, ok := ready.Next(); ok {
			received <- v
		}
	}()

	// Give the ready subscriber time to park (illustrative only: in production,
	// simply publish, and let the subscriber keep receiving).
	time.Sleep(10 * time.Millisecond)

	fmt.Println("delivered", relay.TrySend(42))
	fmt.Println("received", <-received)
	fmt.Println("skipped", busy.Dropped())

	// Output:
	// delivered 1
	// received 42
	// skipped 1
}
-- go.mod --
module github.com/joeycumines/go-bigbuff

go 1.23.0

````
