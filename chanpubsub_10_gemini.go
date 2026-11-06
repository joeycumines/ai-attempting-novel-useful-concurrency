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


// ChanPubSub10Gemini is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, and support for use
// in select statements.
//
// # Architecture & Runtime Optimisations
//
// ChanPubSub10Gemini pairs [ChanCaster] (for synchronous unbuffered channel dispatch)
// with a zero-allocation, sense-reversing two-phase runtime semaphore barrier
// (replacing traditional sync.Cond lock convoys):
//
//  1. Entry Gate (gateWG): Subscribers that receive a message from C enter
//     [ChanPubSub10Gemini.Wait] and park on the current round's entry WaitGroup. Once
//     [ChanPubSub10Gemini.Send] completes all channel transfers (and knows the exact
//     count of messages delivered vs. mid-flight unsubscribes), it adjusts the
//     acknowledgement counter and releases the entry gate in one operation.
//  2. Lock-Free Acknowledgement (ackWG): Woken subscribers do not contend on a
//     shared user-space mutex (as sync.Cond requires on wakeup); instead, each
//     performs an atomic decrement via ackWG.Done(), and only the final
//     subscriber wakes the sender.
//  3. Lock-Free Fast-Path Invariant Guards: Broken-state checks on hot paths
//     use an atomic bit flag rather than runtime.selectnbrecv on a channel,
//     reserving the channel strictly for multi-case select statements.
type ChanPubSub10Gemini[C ~chan V, V any] struct {
	// ping is used to orchestrate sends.
	ping ChanCaster[C, V]

	// sendMu serialises concurrent Send calls.
	sendMu sync.Mutex

	// sendingMu synchronises subscriber count changes with active ping setup:
	//   - Write-locked by Send while snapshotting subscribers and arming ping.
	//   - Read-locked by positive Add (Subscribe) so new subscribers cannot
	//     steal sends mid-broadcast.
	//   - Polled via TryRLock by negative Add (Unsubscribe) in coordination
	//     with pingarmed to eliminate busy-wait spin loops.
	sendingMu sync.RWMutex

	// round tracks the current parity (0 or 1) of the sense-reversing barrier.
	// Only mutated by Send while holding sendingMu and before ping.Send, and
	// read by subscribers inside Wait after receiving from ping.C.
	round atomic.Uint32

	// pingArmed is set to 1 while ping has been armed for an active broadcast
	// round, allowing concurrent negative Add calls to deterministically know
	// whether ping.Add(delta) must be drained without spinning.
	pingArmed atomic.Bool

	// gateWG holds two alternating WaitGroups acting as the sense-reversing
	// entry barrier for Wait() calls.
	gateWG [2]sync.WaitGroup

	// ackWG counts down the remaining Wait() calls for the active Send round.
	ackWG sync.WaitGroup

	// subscribers tracks the total number of registered subscribers.
	subscribers atomic.Int32

	// isBroken provides a 0-overhead atomic fast path for checkBroken(),
	// avoiding runtime.selectnbrecv on every method call.
	isBroken atomic.Bool

	// brokenOnce ensures broken is closed and barriers are flushed at most once.
	brokenOnce sync.Once

	// broken is closed when API misuse is detected, unblocking select statements
	// and any parked Wait/Send calls.
	broken chan struct{}
}

// NewChanPubSub10Gemini constructs a new instance of [ChanPubSub10Gemini], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub10Gemini[C ~chan V, V any](c C) *ChanPubSub10Gemini[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	return &ChanPubSub10Gemini[C, V]{
		ping:   ChanCaster[C, V]{C: c},
		broken: make(chan struct{}),
	}
}

// C returns the underlying channel, used by the receiver.
// This channel MAY be closed, but MUST NOT be sent to (except by
// [ChanPubSub10Gemini.Send]), and receiving from it MUST obey the contract.
// See also [ChanPubSub10Gemini], for the contract.
func (x *ChanPubSub10Gemini[C, V]) C() C {
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
func (x *ChanPubSub10Gemini[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // N.B. handles guarding on check* functions

	if ctx == nil {
		ctx = context.Background()
	}

	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}

		if !stop() {
			return
		}

		defer x.Unsubscribe()

		for {
			if x.isBroken.Load() {
				panic(chanPubSubStateInvariantViolation)
			}
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
func (x *ChanPubSub10Gemini[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken()

	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0 // no subscribers (slow path)
	}

	x.sanityCheckSubscribersDelta(subscribers, 0)

	// Select the sense-reversing barrier slot for this broadcast round.
	r := x.round.Load() & 1
	gate := &x.gateWG[r]

	var (
		gateClosed bool
		ackArmed   int
		success    bool
	)
	defer func() {
		if !success {
			// Ensure any parked subscribers in Wait() are released before marking broken.
			if ackArmed > 0 {
				x.ackWG.Add(-ackArmed)
			}
			if gateClosed {
				gate.Done()
			}
			x.pingArmed.Store(false)
			x.markBroken()
		}
	}()

	// Arm the entry gate and pre-charge ackWG with the maximum possible
	// number of subscribers before any subscriber can receive from ping.C.
	gate.Add(1)
	gateClosed = true
	x.ackWG.Add(subscribers)
	ackArmed = subscribers

	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}
	x.pingArmed.Store(true)

	// Release sendingMu only AFTER ping is armed? No: sendingMu must remain
	// locked during ping.Send so positive Add calls cannot interleave, while
	// negative Add calls see pingArmed == true and drain ping.C directly.
	sent = x.ping.Send(value)

	x.pingArmed.Store(false)
	x.round.Add(1) // advance sense for the next round

	skipSendingUnlock = true
	x.sendingMu.Unlock() // positive subscribers can now register while we await pongs

	// Adjust ackWG for any subscribers that unsubscribed mid-flight during ping.Send.
	// Because gate is still closed (gate.Add(1)), no subscriber has called ackWG.Done() yet.
	if diff := sent - subscribers; diff != 0 {
		x.ackWG.Add(diff)
	}
	ackArmed = 0

	// Open the entry gate: all subscribers waiting in Wait() are released at once
	// via runtime semaphores without a mutex lock convoy.
	gateClosed = false
	gate.Done()

	if sent != 0 {
		x.ackWG.Wait()
		x.checkBroken()
	}

	success = true
	return
}

// Add returns the number of subscribers, after adding delta to the count.
func (x *ChanPubSub10Gemini[C, V]) Add(delta int) (subscribers int) {
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
		// Attempt to acquire read lock. If Send holds the write lock, inspect
		// pingArmed. If Send is still in the brief setup window before arming
		// ping, wait only until either sendingMu is available or pingArmed becomes true.
		ok := x.sendingMu.TryRLock()
		for !ok && !x.pingArmed.Load() {
			x.checkBroken()
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock()
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			x.ping.Add(delta)
			success = true
		}

	default:
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
func (x *ChanPubSub10Gemini[C, V]) Wait() {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	// Capture the round sense before waiting on the entry gate.
	// Note: If Send is still inside ping.Send(value), round has not yet been
	// incremented, so r is round&1. If Send already finished ping.Send(value)
	// and incremented round, then gateWG[(round-1)&1] is already Done() (0),
	// AND gateWG[round&1] is ALSO 0 because the next Send cannot start until
	// ackWG.Wait() finishes! Thus waiting on BOTH gateWG[0] and gateWG[1] is
	// completely safe and eliminates any race on reading round!
	x.gateWG[0].Wait()
	x.gateWG[1].Wait()

	x.checkBroken()

	// If Wait() was called illegally when no broadcast is active, ackWG.Done()
	// will panic with negative WaitGroup counter, which we catch and convert
	// into our invariant panic.
	defer func() {
		if r := recover(); r != nil {
			x.markBroken()
			panic(chanPubSubStateInvariantViolation)
		}
	}()
	x.ackWG.Done()
}

// Subscribe is an alias for calling [ChanPubSub10Gemini.Add] with a delta of 1.
func (x *ChanPubSub10Gemini[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub10Gemini.Add] with a delta of -1.
func (x *ChanPubSub10Gemini[C, V]) Unsubscribe() {
	x.Add(-1)
}

func (x *ChanPubSub10Gemini[C, V]) checkBroken() {
	if x.isBroken.Load() {
		panic(chanPubSubStateInvariantViolation)
	}
}

func (x *ChanPubSub10Gemini[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

func (x *ChanPubSub10Gemini[C, V]) markBroken() {
	x.brokenOnce.Do(func() {
		x.isBroken.Store(true)
		close(x.broken)
	})
}

func (x *ChanPubSub10Gemini[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
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

func (x *ChanPubSub10Gemini[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
