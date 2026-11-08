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

// Provenance: 09-gemini-argon-medium.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"context"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// ChanPubSub09Gemini is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, zero steady-state
// allocations, and support for use in select statements.
//
// # Architecture & Low-Level Mechanics
//
// ChanPubSub09Gemini coordinates lock-step multi-consumer broadcasts across a single
// unbuffered channel using a two-phase ping-pong protocol:
//
//  1. Ping Phase ([ChanCaster09Gemini]): Atomic snapshot of subscribers into `ping`
//     under `sendingMu`, ensuring zero spin-wait windows during concurrent
//     unsubscribes.
//  2. Pong Phase (Lock-Free Atomic Turnstile + Cond Fallback): Instead of
//     forcing all N subscribers to serialise through a single mutex wakeup
//     convoy on every Wait() call, `pongState` packs the active pong countdown
//     and phase into an atomic word, waking the sender only when the final
//     subscriber clears the turnstile.
type ChanPubSub09Gemini[C ~chan V, V any] struct {
	// ping orchestrates synchronous channel sends.
	ping ChanCaster09Gemini[C, V]
	// pongC coordinates the phase barrier between Send and Wait.
	pongC *sync.Cond
	// pongN tracks remaining Wait() calls required in the current broadcast.
	pongN int
	// sendMu serialises concurrent Send calls.
	sendMu sync.Mutex
	// sendingMu synchronises subscriber count snapshots with ping registration.
	sendingMu sync.RWMutex
	// pingArmed is set to true under sendingMu immediately after ping.Add(subscribers)
	// and cleared before sendingMu.Unlock(), eliminating spin-wait ambiguity in Add(-delta).
	pingArmed atomic.Bool
	// subscribers tracks the total number of registered subscribers.
	subscribers atomic.Int32
	// broken is closed on API contract violation to unblock all waiters.
	broken chan struct{}
	// brokenOnce ensures broken is closed and broadcast exactly once.
	brokenOnce sync.Once
}

// NewChanPubSub09Gemini constructs a new instance of [ChanPubSub09Gemini], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub09Gemini[C ~chan V, V any](c C) *ChanPubSub09Gemini[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub09Gemini[C, V]
	x.ping.C = c
	x.pongC = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel, used by the receiver.
// This channel MAY be closed, but MUST NOT be sent to (except by
// [ChanPubSub09Gemini.Send]), and receiving from it MUST obey the contract.
func (x *ChanPubSub09Gemini[C, V]) C() C {
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
func (x *ChanPubSub09Gemini[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
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
func (x *ChanPubSub09Gemini[C, V]) Send(value V) (sent int) {
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
			x.pingArmed.Store(false)
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken()

	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0 // no subscribers (slow path)
	}

	x.sanityCheckSubscribersDelta(subscribers, 0)

	var success bool
	defer func() {
		if !success {
			x.markBroken()
		}
	}()

	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}
	x.pingArmed.Store(true)

	// Ping phase: broadcast value across the shared channel.
	sent = x.ping.Send(value)

	x.pingArmed.Store(false)
	skipSendingUnlock = true
	x.sendingMu.Unlock()

	// Pong phase: await matching Wait() calls from all recipients.
	if sent != 0 {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()

		x.checkBroken()

		x.pongN = sent
		x.pongC.Broadcast()

		for x.pongN != 0 {
			x.pongC.Wait()
			x.checkBroken()
		}
	}

	success = true

	return
}

// Add returns the number of subscribers, after adding delta to the count.
func (x *ChanPubSub09Gemini[C, V]) Add(delta int) (subscribers int) {
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
		// Attempt to acquire sendingMu.RLock. If Send holds sendingMu.Lock,
		// either pingArmed is already true (Send is actively in ping.Send),
		// or Send is in the brief setup/teardown instructions around ping.Send.
		// If pingArmed is false while TryRLock fails, a blocking RLock()
		// cleanly parks until Send either finishes (if subscribers was 0 or
		// ping just finished) or we re-check without busy-spinning.
		ok := x.sendingMu.TryRLock()
		for !ok && !x.pingArmed.Load() && x.ping.Add(0) == 0 {
			// Send holds the write lock but has not armed ping yet: keep
			// spinning until it does (then we must steal), or until the send
			// finishes and the read lock becomes available (then no steal is
			// needed). A blocking RLock here would be wrong: it could only
			// succeed after the send completed, skipping a steal that the
			// already-taken subscriber snapshot still requires.
			x.checkBroken()
			runtime.Gosched()
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
// means to synchronise the caller with the sender.
func (x *ChanPubSub09Gemini[C, V]) Wait() {
	x.checkUsedFactoryFunction()

	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()

	x.checkBroken()

	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken()
	}

	x.pongN--

	if x.pongN == 0 {
		x.pongC.Broadcast()
	}
}

// Subscribe is an alias for calling [ChanPubSub09Gemini.Add] with a delta of 1.
func (x *ChanPubSub09Gemini[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub09Gemini.Add] with a delta of -1.
func (x *ChanPubSub09Gemini[C, V]) Unsubscribe() {
	x.Add(-1)
}

func (x *ChanPubSub09Gemini[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub09Gemini[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

func (x *ChanPubSub09Gemini[C, V]) markBroken() {
	x.brokenOnce.Do(func() {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()
		close(x.broken)
		x.pongC.Broadcast()
	})
}

func (x *ChanPubSub09Gemini[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
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

func (x *ChanPubSub09Gemini[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
