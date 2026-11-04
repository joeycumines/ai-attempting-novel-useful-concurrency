Advance the implementation to its ultimate form by evolving one or both implementations to achieve a much greater height of sophistication AND cleverness, AND/OR implement one additional, complementy implementation, of greater sophistication and extremely strong broad applicability, with similarly nuanced and interesting characteristics as this implementation's extremely stable and predictable (very low) allocation coupled with performance comparable (if lesser) to a linear "callback channel" oriented design + its obvious extremely valuable support for use within a select (which the callback-oriented design achieves, though in a manner that doesnt scale with churn of consumers or large numbers of consumers), as an expert in low-level Go runtime optimisations. N.B. *novelty* of design coupled with exceptionally strong characteristics is the kind of solution I require.

---
## IMPLEMENTATIONS/CONTEXT
---

context root: `/Users/joeyc/dev/go-bigbuff`

`````txtar
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

`````
