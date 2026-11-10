// Provenance: 47-arena-gemini-3.8-flash.md (Gemini, response #47). Namespaced to preserve the existing implementations.
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

const chanPubSub47GeminiStateInvariantViolation = `bigbuff: chanpubsub: state invariant violation`

// ChanPubSub47Gemini is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, and support for use
// in select statements.
//
// # Usage
//
// The [ChanPubSub47Gemini.SubscribeContext] method provides a simple, [iter.Seq]-based
// (iterable) mechanism, to subscribe and consume values. If greater
// flexibility is required (e.g. it is desirable to leverage select
// statements), please ensure you understand the contract, documented below.
//
// The underlying implementation relies on strict adherence to the following
// contract:
//
//  1. The [NewChanPubSub47Gemini] factory MUST be used to create instances.
//  2. Values MUST NOT be received prior to incrementing the subscriber count,
//     e.g. by calling [ChanPubSub47Gemini.Add] with `1`.
//  3. Subscribers - any logical unit that receives values, after incrementing
//     the subscriber count - MUST call [ChanPubSub47Gemini.Wait], immediately after
//     each value received, UNLESS the value was the result of closing the
//     underlying channel (subscribers MUST handle this case).
//  4. Subscribers MUST be able to receive then block on [ChanPubSub47Gemini.Wait]
//     calls, that is, the number of subscribers MUST be exactly equal to what
//     was registered, e.g. via [ChanPubSub47Gemini.Add].
//  5. [ChanPubSub47Gemini.Wait] MUST NOT be called in any other circumstance.
//  6. Subscribers MAY call [ChanPubSub47Gemini.Add] with a negative delta, to
//     unsubscribe, but MUST ensure that the net total remains >= 0, at all
//     times.
//  7. Subscribers MUST promptly and _continually_ operate on a cycle of
//     receiving (from [ChanPubSub47Gemini.C]) then waiting (on [ChanPubSub47Gemini.Wait]),
//     until such a time as they are no longer interested in receiving values,
//     in which case they MUST promptly unsubscribe. Subscribers MAY use the
//     channel in a select statement, to trigger unsubscribe, as an example.
//     After unsubscribing, values MUST NOT be received, from the channel.
type ChanPubSub47Gemini[C ~chan V, V any] struct {
	// ping orchestrates message fan-out over the underlying unbuffered channel.
	ping ChanCaster47Gemini[C, V]

	// sendMu serialises concurrent Send calls.
	sendMu sync.Mutex

	// subMu and subCond coordinate subscriber additions and removals with the
	// ping arming transition in Send without busy-waiting.
	subMu     sync.Mutex
	subCond   *sync.Cond
	sending   bool
	pingArmed bool

	// subscribers tracks the active subscriber count.
	subscribers atomic.Int32

	// Lock-free binary-tree channel barrier for the pong phase:
	//   - pongRem counts remaining participants (Wait calls or mid-Send unsubscribes)
	//   - waiters counts goroutines parked in <-pongGate
	//   - pongWake propagates O(log2 N) parallel wakeups across pongGate
	//   - pongExit counts participants finishing Wait; the last signals pongDone
	pongRem  atomic.Int32
	waiters  atomic.Int32
	pongWake atomic.Int32
	pongExit atomic.Int32
	pongGate chan struct{}
	pongDone chan struct{}

	// broken is closed once if API misuse or an invariant violation is detected.
	brokenOnce sync.Once
	broken     chan struct{}
}

// NewChanPubSub47Gemini constructs a new instance of [ChanPubSub47Gemini], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub47Gemini[C ~chan V, V any](c C) *ChanPubSub47Gemini[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub47Gemini[C, V]
	x.ping.C = c
	x.subCond = sync.NewCond(&x.subMu)
	x.pongGate = make(chan struct{})
	x.pongDone = make(chan struct{})
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel, used by the receiver.
// This channel MAY be closed, but MUST NOT be sent to (except by
// [ChanPubSub47Gemini.Send]), and receiving from it MUST obey the contract.
// See also [ChanPubSub47Gemini], for the contract.
func (x *ChanPubSub47Gemini[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

// Subscribers returns the current number of active subscribers.
func (x *ChanPubSub47Gemini[C, V]) Subscribers() int {
	return x.Add(0)
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
func (x *ChanPubSub47Gemini[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // handles guarding on check* functions

	if ctx == nil {
		ctx = context.Background()
	}

	// Guards cleanup if the iterator is never invoked or ctx cancels beforehand.
	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}

		// Disarm AfterFunc before entering the select loop so AfterFunc and
		// this goroutine can never concurrently race on Unsubscribe vs <-C.
		if !stop() {
			return
		}

		// Add classifies the departure itself (mid-flight withdrawal or
		// plain membership change), under the lock that orders it against
		// Send; see the repair note there.
		defer x.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSub47GeminiStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSub47GeminiStateInvariantViolation)
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
// [ChanPubSub47Gemini].
func (x *ChanPubSub47Gemini[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (lock-free fast path)
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.subMu.Lock()
	x.checkBroken()
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		x.subMu.Unlock()
		return 0 // no subscribers (slow path)
	}
	x.sanityCheckSubscribersDelta(subscribers, 0)
	x.sending = true
	x.pingArmed = false
	x.subMu.Unlock()

	var success bool
	defer func() {
		x.subMu.Lock()
		x.sending = false
		x.pingArmed = false
		x.subCond.Broadcast()
		x.subMu.Unlock()
		if !success {
			x.markBroken() // example: closed channel caused panic
		}
	}()

	// Pre-arm both ping and pong counters before releasing parked unsubscribes
	// or sending on the channel, preventing any thundering herd on Wait.
	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSub47GeminiStateInvariantViolation)
	}
	x.waiters.Store(0)
	x.pongWake.Store(0)
	// Integration repair: the sender is itself a participant in the pong
	// barrier (the +1), and takes its place only after the ping phase has
	// fully ended. Without it, the last subscriber to receive released the
	// barrier before ping.Send had cleared its sending state; a subscriber
	// that then unsubscribed straight after Wait was treated as a mid-flight
	// withdrawal, and parked forever on a channel receive for a send that
	// was never coming.
	x.pongRem.Store(int32(subscribers) + 1)
	x.pongExit.Store(int32(subscribers))

	x.subMu.Lock()
	x.pingArmed = true
	x.subCond.Broadcast()
	x.subMu.Unlock()

	// Ping: broadcast value to all active subscribers.
	sent = x.ping.Send(value)

	// The ping phase is over: every registered subscriber has received or
	// withdrawn, and ping holds no state. Take the sender's place in the
	// barrier, releasing the parked subscribers if it is the last to arrive.
	switch rem := x.pongRem.Add(-1); {
	case rem < 0:
		panic(chanPubSub47GeminiStateInvariantViolation)
	case rem == 0:
		if w := x.waiters.Swap(0); w > 0 {
			x.pongWake.Store(w)
			x.propagatePongWake()
		}
	}

	// Pong: wait until every subscriber has either completed Wait or
	// unsubscribed mid-flight via Add(-delta).
	select {
	case <-x.pongDone:
	case <-x.broken:
		panic(chanPubSub47GeminiStateInvariantViolation)
	}

	success = true
	return sent
}

// Add returns the number of subscribers, after adding delta to the count. It
// is intended for more manual use, which requires the caller to be aware of
// and conform to the contract, as documented on [ChanPubSub47Gemini].
// This method acts as a low-level means to subscribe or unsubscribe from the
// receiver.
// A panic may occur if the input is invalid (e.g. decrementing subscribers
// below zero), or if the receiver is in a broken state, indicating misuse.
func (x *ChanPubSub47Gemini[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		x.subMu.Lock()
		// Park cleanly if Send has begun transitioning but has not yet armed
		// ping and pong counters.
		for x.sending && !x.pingArmed {
			x.checkBroken()
			x.subCond.Wait()
		}
		// Integration repair. The caller is a mid-flight withdrawal only if
		// the armed Send's pong barrier has not been released yet: it then
		// cannot have completed Wait for that Send, so (the contract being
		// obeyed) it has not received, its receive intent is still held by
		// ping, and that Send cannot finish before the withdrawal below. Once
		// the barrier is released (pongRem == 0) every subscriber has taken
		// part, and leaving changes membership only. Deciding here, under
		// subMu, ties the decision to one Send; as delivered it was taken
		// from ping and pongRem after the unlock, where a following Send
		// could already have re-armed both.
		inFlight := x.sending && x.pongRem.Load() > 0
		subscribers = x.addSubscribers(delta)
		x.subMu.Unlock()

		x.sanityCheckSubscribersDelta(subscribers, delta)

		if inFlight {
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			// Withdraw -delta receive intents from ping, draining the sends
			// that would otherwise have no receiver, then account for those
			// participants in the pong barrier.
			if !x.ping.tryWithdraw(delta) {
				panic(chanPubSub47GeminiStateInvariantViolation)
			}
			x.completePongParticipants(-delta)
			success = true
		}

	default:
		x.subMu.Lock()
		for x.sending {
			x.checkBroken()
			x.subCond.Wait()
		}
		subscribers = x.addSubscribers(delta)
		x.subMu.Unlock()
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return subscribers
}

// Wait is mandatory after each message received by each subscriber, serving as
// means to synchronise the caller (a subscriber, that received a message) with
// the sender, for the purpose of ensuring that all messages are broadcast, to
// all subscribers.
func (x *ChanPubSub47Gemini[C, V]) Wait() {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	// Pre-register as a potential waiter before decrementing pongRem so the
	// participant that observes pongRem == 0 always sees an exact waiter count.
	x.waiters.Add(1)
	rem := x.pongRem.Add(-1)
	switch {
	case rem > 0:
		select {
		case <-x.pongGate:
		case <-x.broken:
			panic(chanPubSub47GeminiStateInvariantViolation)
		}
		x.propagatePongWake()

	case rem == 0:
		// Undo our own pre-increment since we are not parked on pongGate.
		if w := x.waiters.Swap(0) - 1; w > 0 {
			x.pongWake.Store(w)
			x.propagatePongWake()
		}

	default:
		x.markBroken()
		panic(chanPubSub47GeminiStateInvariantViolation)
	}

	if x.pongExit.Add(-1) == 0 {
		select {
		case x.pongDone <- struct{}{}:
		case <-x.broken:
			panic(chanPubSub47GeminiStateInvariantViolation)
		}
	}
}

// completePongParticipants accounts for count subscribers that unsubscribed
// mid-Send without calling Wait.
func (x *ChanPubSub47Gemini[C, V]) completePongParticipants(count int) {
	rem := x.pongRem.Add(-int32(count))
	if rem < 0 {
		x.markBroken()
		panic(chanPubSub47GeminiStateInvariantViolation)
	}
	if rem == 0 {
		if w := x.waiters.Swap(0); w > 0 {
			x.pongWake.Store(w)
			x.propagatePongWake()
		}
	}

	exit := x.pongExit.Add(-int32(count))
	if exit < 0 {
		x.markBroken()
		panic(chanPubSub47GeminiStateInvariantViolation)
	}
	if exit == 0 {
		select {
		case x.pongDone <- struct{}{}:
		case <-x.broken:
			panic(chanPubSub47GeminiStateInvariantViolation)
		}
	}
}

// propagatePongWake performs a lock-free binary-tree fan-out wakeup across
// pongGate, waking all parked subscribers in O(log2 N) parallel hops without
// mutex contention or thundering-herd broadcasts.
func (x *ChanPubSub47Gemini[C, V]) propagatePongWake() {
	for range 2 {
		if x.pongWake.Add(-1) >= 0 {
			select {
			case x.pongGate <- struct{}{}:
			case <-x.broken:
				panic(chanPubSub47GeminiStateInvariantViolation)
			}
		} else {
			break
		}
	}
}

// Subscribe is an alias for calling [ChanPubSub47Gemini.Add] with a delta of 1.
func (x *ChanPubSub47Gemini[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub47Gemini.Add] with a delta of -1.
func (x *ChanPubSub47Gemini[C, V]) Unsubscribe() {
	x.Add(-1)
}

func (x *ChanPubSub47Gemini[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSub47GeminiStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub47Gemini[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

func (x *ChanPubSub47Gemini[C, V]) markBroken() {
	x.brokenOnce.Do(func() {
		close(x.broken)
	})
}

func (x *ChanPubSub47Gemini[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))

	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}

	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}

	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative old subscribers`)
	}
}

func (x *ChanPubSub47Gemini[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
