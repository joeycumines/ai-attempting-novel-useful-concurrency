// -- chanrotor.go --
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

// Provenance: 17-gemini-argon-medium.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"context"
	"iter"
	"math"
	"sync"
	"sync/atomic"
)

const chanRotorStateInvariantViolation = `bigbuff: chanrotor: state invariant violation`

// ChanRotor is a wait-free, dual-channel rotational pub/sub primitive.
//
// Unlike [ChanPubSub], which multiplexes all broadcasts over a single channel
// and requires every subscriber to call Wait() after each receive (forcing a
// second barrier synchronization across all subscribers before any subscriber
// can process its value), [ChanRotor] alternates consecutive broadcasts across
// two unbuffered channels (ping[0] and ping[1]) indexed by a monotonic 64-bit
// epoch sequence number.
//
// # Key Characteristics & Novelty
//
//  1. Zero Wait() Barrier: Once a subscriber receives a value from C(seq), it
//     simply advances its local sequence counter (seq++) and immediately
//     processes the value in parallel with other subscribers. Because its next
//     receive will target ping[(seq+1)&1], it is mathematically impossible for
//     a fast subscriber to steal a message from the still-in-flight broadcast
//     on ping[seq&1].
//  2. Unbuffered Channel Induction Guarantee: Because ping[seq&1] is unbuffered,
//     Broadcast `seq` cannot complete until all N registered subscribers have
//     either received from ping[seq&1] (and transitioned to seq+1) or
//     unsubscribed. Therefore, when Broadcast `seq+1` starts on ping[(seq+1)&1],
//     zero subscribers remain on ping[seq&1].
//  3. Epoch-Exact Mid-Flight Unsubscription: Because each subscriber tracks its
//     local `seq`, calling Unsubscribe(seq) mid-broadcast deterministically
//     distinguishes between a subscriber that has NOT YET received the active
//     broadcast (`seq == armedSeq`, requiring a channel drain on ping[seq&1])
//     and a subscriber that ALREADY received the active broadcast, incremented
//     `seq`, and broke out of its loop early (`seq == armedSeq + 1`, requiring
//     no channel drain and preserving exact Send delivery counts).
//  4. Zero Steady-State Allocations & Full `select` Compatibility: Regardless
//     of whether there are 10 or 1,000,000 subscribers, only two unbuffered
//     channels exist in the entire system.
type ChanRotor[C ~chan V, V any] struct {
	ping [2]ChanCaster17Pumped[C, V]

	// sendMu serialises Send calls.
	sendMu sync.Mutex

	// sendingMu blocks new subscriptions while a broadcast is actively pumping.
	sendingMu sync.RWMutex

	// stateMu protects atomic transitions of (subscribers, nextSeq, armedSeq, ping.armForSend).
	// Never held during blocking channel sends or receives.
	stateMu sync.Mutex

	// nextSeq is the sequence number to be assigned to the next active Send.
	// Starts at 1 so that 0 can serve as the disarmed sentinel for armedSeq.
	nextSeq uint64

	// armedSeq is the sequence number of the currently in-flight broadcast,
	// or 0 if no broadcast is currently pumping.
	armedSeq uint64

	// subscribers tracks the active subscriber count.
	subscribers atomic.Int32

	// isBroken provides a 1-instruction atomic guard for invariant violations.
	isBroken atomic.Bool

	// broken is closed on invariant violation to wake any blocked select loops.
	broken chan struct{}
}

// ChanRotorSub is a value-type subscriber cursor for [ChanRotor] that can be
// used on the stack with zero heap allocations inside custom `select` loops.
type ChanRotorSub[C ~chan V, V any] struct {
	rotor *ChanRotor[C, V]
	seq   uint64
}

// C returns the unbuffered channel to select on for the subscriber's current
// epoch sequence.
func (s *ChanRotorSub[C, V]) C() C {
	return s.rotor.C(s.seq)
}

// Seq returns the subscriber's current epoch sequence number.
func (s *ChanRotorSub[C, V]) Seq() uint64 {
	return s.seq
}

// Advance MUST be called immediately after receiving a value from [ChanRotorSub.C]
// (unless the channel was closed). It is a single register increment with zero
// locks, zero atomic operations, and zero blocking.
func (s *ChanRotorSub[C, V]) Advance() {
	s.seq++
}

// Unsubscribe removes this subscriber from the [ChanRotor]. It is safe to call
// at any point, including immediately after [ChanRotorSub.Advance] while the
// broadcast is still delivering to slower subscribers.
func (s *ChanRotorSub[C, V]) Unsubscribe() {
	s.rotor.Add(s.seq, -1)
}

// NewChanRotor constructs a new [ChanRotor] using the two provided unbuffered
// channels as the alternating transport pair. Both channels must be unbuffered
// (cap == 0) and non-identical (unless both are nil).
func NewChanRotor[C ~chan V, V any](c0, c1 C) *ChanRotor[C, V] {
	if cap(c0) != 0 || cap(c1) != 0 {
		panic(`bigbuff: chanrotor: must use unbuffered channels`)
	}
	if c0 != nil && c0 == c1 {
		panic(`bigbuff: chanrotor: c0 and c1 must be distinct channels`)
	}
	var x ChanRotor[C, V]
	x.ping[0].C = c0
	x.ping[1].C = c1
	x.nextSeq = 1
	x.broken = make(chan struct{})
	return &x
}

// MakeChanRotor constructs a new [ChanRotor] with freshly allocated unbuffered
// channels of type `chan V`.
func MakeChanRotor[V any]() *ChanRotor[chan V, V] {
	return NewChanRotor(make(chan V), make(chan V))
}

// C returns the underlying channel for the given epoch sequence number.
func (x *ChanRotor[C, V]) C(seq uint64) C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping[seq&1].C
}

// Broken returns a channel that is closed if an API invariant violation occurs.
func (x *ChanRotor[C, V]) Broken() <-chan struct{} {
	x.checkUsedFactoryFunction()
	return x.broken
}

// Subscribe registers a single subscriber and returns a zero-allocation
// [ChanRotorSub] cursor synchronized to the current broadcast epoch.
func (x *ChanRotor[C, V]) Subscribe() ChanRotorSub[C, V] {
	seq, _ := x.Add(0, 1)
	return ChanRotorSub[C, V]{rotor: x, seq: seq}
}

// SubscribeContext immediately subscribes to the [ChanRotor] and returns a
// single-use iterator yielding all broadcast values until the context is
// cancelled, the iterator stops early, or the active transport channel closes.
func (x *ChanRotor[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := x.Subscribe()

	if ctx == nil {
		ctx = context.Background()
	}

	// Track current seq atomically so context.AfterFunc can unsubscribe at the
	// exact epoch if the iterator is never invoked.
	var atomicSeq atomic.Uint64
	atomicSeq.Store(sub.seq)

	stop := context.AfterFunc(ctx, func() {
		x.Add(atomicSeq.Load(), -1)
	})

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				sub.Unsubscribe()
			}
			panic(`bigbuff: chanrotor: iterator yield func is nil`)
		}

		if !stop() {
			return
		}

		defer func() {
			sub.Unsubscribe()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanRotorStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanRotorStateInvariantViolation)
			case v, ok := <-x.ping[sub.seq&1].C:
				if !ok {
					return
				}

				// Advance local epoch parity immediately: zero locks, zero Wait()!
				sub.seq++

				if !yield(v) {
					return
				}
			}
		}
	}
}

// Send broadcasts value to all currently registered subscribers, returning the
// exact number of subscribers that received the value.
func (x *ChanRotor[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // fast path
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.sendingMu.Lock()
	defer x.sendingMu.Unlock()

	x.checkBroken()

	x.stateMu.Lock()
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		x.stateMu.Unlock()
		return 0
	}
	x.sanityCheckSubscribersDelta(subscribers, 0)

	seq := x.nextSeq
	x.nextSeq = seq + 1
	x.armedSeq = seq
	caster := &x.ping[seq&1]

	var success bool
	defer func() {
		if !success {
			x.markBroken()
		}
	}()

	caster.armForSend(uint32(subscribers))
	x.stateMu.Unlock()

	// Broadcast across the active channel for this epoch.
	sent = caster.pumpAndReset(value, uint32(subscribers))

	x.stateMu.Lock()
	x.armedSeq = 0
	x.stateMu.Unlock()

	success = true
	return sent
}

// Add adjusts the subscriber count by delta and returns the starting epoch
// sequence number (for positive/zero deltas) and the resulting subscriber count.
//
// For negative deltas (unsubscribing), `seq` MUST be the subscriber's current
// epoch sequence number:
//   - If `seq` matches the initially subscribed sequence before receiving the
//     in-flight broadcast (`seq == armedSeq`), Add drains `delta` values from
//     the active channel so Send does not hang.
//   - If the subscriber already received the in-flight broadcast and advanced
//     its sequence (`seq == armedSeq + 1`), Add decrements the subscriber count
//     for subsequent broadcasts without draining the active channel.
func (x *ChanRotor[C, V]) Add(seq uint64, delta int) (outSeq uint64, subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanrotor: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		x.stateMu.Lock()
		outSeq = x.nextSeq
		subscribers = int(x.subscribers.Load())
		x.stateMu.Unlock()
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		x.stateMu.Lock()
		armed := x.armedSeq
		next := x.nextSeq
		subscribers = int(x.subscribers.Add(int32(delta)))
		x.stateMu.Unlock()

		x.sanityCheckSubscribersDelta(subscribers, delta)

		// Validate sequence epoch invariants:
		//   1. When idle (armed == 0), a valid subscriber's seq MUST equal nextSeq.
		//   2. When a broadcast is armed (armed != 0, next == armed + 1), a valid
		//      subscriber's seq MUST be either `armed` (has not yet received the
		//      in-flight broadcast) or `armed + 1` (already received it).
		if armed == 0 {
			if seq != next {
				x.markBroken()
				panic(chanRotorStateInvariantViolation)
			}
			outSeq = seq
			return
		}

		switch seq {
		case armed:
			// Subscriber has NOT received the in-flight broadcast on ping[armed&1];
			// drain -delta items via ChanCaster.Add(delta).
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			x.ping[armed&1].Add(delta)
			success = true
			outSeq = seq

		case armed + 1:
			// Subscriber ALREADY received the in-flight broadcast on ping[armed&1]
			// and advanced its local seq; no drain needed on ping[armed&1].
			outSeq = seq

		default:
			x.markBroken()
			panic(chanRotorStateInvariantViolation)
		}

	default:
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()

		x.stateMu.Lock()
		outSeq = x.nextSeq
		subscribers = int(x.subscribers.Add(int32(delta)))
		x.stateMu.Unlock()

		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

func (x *ChanRotor[C, V]) checkBroken() {
	if x.isBroken.Load() {
		panic(chanRotorStateInvariantViolation)
	}
}

func (x *ChanRotor[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanrotor: must use factory function`)
	}
}

func (x *ChanRotor[C, V]) markBroken() {
	defer func() {
		recover()
	}()
	if !x.isBroken.Swap(true) {
		close(x.broken)
	}
}

func (x *ChanRotor[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))

	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanrotor: addition overflowed or underflowed")
	}

	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanrotor: negative subscribers`)
	}

	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanrotor: negative old subscribers`)
	}
}
