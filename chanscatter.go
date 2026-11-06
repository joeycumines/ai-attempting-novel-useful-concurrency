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
	// one receive is needed to unblock a pending send. It also coordinates with
	// positive Add calls so receivers cannot be added while a send is in flight.
	sendMu sync.RWMutex

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
		if delta > 0 {
			x.sendMu.RLock()
			defer x.sendMu.RUnlock()
		}
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
					// Steal the pending value to unblock the sender.
					<-x.C
				}
				return int(newHi)
			}
		}
	}
}
