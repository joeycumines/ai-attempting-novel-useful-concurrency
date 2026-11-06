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

const (
	chanCasterMaxReceivers = math.MaxInt32
	chanCasterSendingBit   = uint64(1) << 31
	chanCasterLoMask       = uint64(0x7fffffff)
)

// ChanCaster09Gemini supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanCaster09Gemini.Send] performs synchronous
// sends per registered receiver, relying on the one-to-one send/receive
// semantics of an unbuffered channel to guarantee delivery. A buffered
// channel would violate these invariants and can cause deadlocks or
// undetected missed receives.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire. This allows a
// ChanCaster09Gemini to be used in a "disabled" state — [ChanCaster09Gemini.Send] returns 0
// when there are no receivers, and the channel can be included in a select
// statement without ever being selected.
//
// Construction is "softly" enforced: [NewChanCaster09Gemini] validates the
// precondition and panics on a buffered channel, but a value may also be
// built directly via a struct literal, which bypasses that check. Callers
// that construct a [ChanCaster09Gemini] directly are responsible for honouring the
// unbuffered requirement themselves.
//
// # Architecture & Low-Level Mechanics
//
// ChanCaster09Gemini encodes its entire synchronisation protocol into a single 64-bit
// atomic word (`state`), paired with a slow-path mutex (`mutex`) that is
// completely bypassed by [ChanCaster09Gemini.Add] whenever a broadcast is not
// actively mid-flight:
//
//   - Bits 32..63 (hi, uint32): Current count of active receivers in range
//     [0, math.MaxInt32].
//   - Bit 31 (`chanCasterSendingBit`): Set to 1 while [ChanCaster09Gemini.Send] is
//     actively broadcasting; 0 when idle.
//   - Bits 0..30 (lo, 31-bit uint): Redundant mirror of the active receiver
//     count (`hi`). Any divergence between `hi` and `lo` immediately exposes
//     concurrent underflow, overflow, or state corruption via a single 64-bit
//     two's complement addition.
//
// When [ChanCaster09Gemini.Add] is called with a positive delta while idle
// (`chanCasterSendingBit == 0`), it completes in a single lock-free CAS on
// `state` without touching `mutex`. If a broadcast is in progress
// (`chanCasterSendingBit == 1`), positive [ChanCaster09Gemini.Add] calls fall back to
// `mutex` to wait for the active broadcast batch to complete.
type ChanCaster09Gemini[C ~chan V, V any] struct {
	// C is the channel that will be used to broadcast messages.
	//
	// See [ChanCaster09Gemini.Add] and [ChanCaster09Gemini.Send] for usage details.
	C C

	// mutex serialises concurrent [ChanCaster09Gemini.Send] calls and acts as a
	// slow-path turnstile for positive [ChanCaster09Gemini.Add] calls that arrive
	// while a send batch is actively mid-flight.
	mutex sync.Mutex

	// state packs [hi: 32-bit receivers | bit 31: sending flag | lo: 31-bit mirror].
	state atomic.Uint64
}

// NewChanCaster09Gemini constructs a [ChanCaster09Gemini] for the provided channel. It exists
// primarily for type inference convenience, but also enforces the
// [ChanCaster09Gemini] precondition: the channel must be unbuffered (cap == 0). A nil
// channel is permitted. It panics if the channel is buffered.
func NewChanCaster09Gemini[C ~chan V, V any](channel C) *ChanCaster09Gemini[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster09Gemini[C, V]{C: channel}
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster09Gemini.C], then returns the number of sends performed, less any
// removed mid-flight by [ChanCaster09Gemini.Add].
//
// If the contract of [ChanCaster09Gemini] is obeyed, this call will never block for
// any significant time. This function may panic if state invariants have been
// violated, e.g. by misuse of [ChanCaster09Gemini.Add], or if unregistered receives
// are detected.
func (x *ChanCaster09Gemini[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // no receivers (wait-free fast path)
	}

	// Serialise concurrent Send calls and block slow-path positive Add calls
	// once chanCasterSendingBit is armed.
	x.mutex.Lock()
	defer x.mutex.Unlock()

	var (
		state     uint64
		receivers uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0 // no receivers (slow path)
		}

		receivers = uint32(state >> 32)
		lo := uint32(state & chanCasterLoMask)
		if (state&chanCasterSendingBit) != 0 || lo != receivers || receivers > chanCasterMaxReceivers {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}

		// Arm chanCasterSendingBit atomically. Once set, lock-free positive Add
		// calls cannot succeed and will park on x.mutex.Lock() until we finish.
		if x.state.CompareAndSwap(state, state|chanCasterSendingBit) {
			break
		}
	}

	// Broadcast to all registered receivers. Negative Add calls occurring
	// mid-flight will atomically decrement state and drain their corresponding
	// send from x.C.
	for range receivers {
		x.C <- value
	}

	// Retrieve, validate, and reset state to 0.
	state = x.state.Load()
	finalReceivers := uint32(state >> 32)
	lo := uint32(state & chanCasterLoMask)
	if finalReceivers > receivers ||
		(state&chanCasterSendingBit) == 0 ||
		lo != finalReceivers ||
		!x.state.CompareAndSwap(state, 0) {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	return int(finalReceivers)
}

// Add increments or decrements the number of receivers, for [ChanCaster09Gemini.C],
// then returns the number of receivers, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of receivers, without changing it.
func (x *ChanCaster09Gemini[C, V]) Add(delta int) int {
	switch {
	case delta == 0:
		state := x.state.Load()
		receivers := uint32(state >> 32)
		lo := uint32(state & chanCasterLoMask)
		if receivers <= chanCasterMaxReceivers && receivers == lo {
			return int(receivers)
		}

	case delta > 0:
		if delta > chanCasterMaxReceivers {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		}
		uDelta := uint32(delta)
		packedDelta := uint64(uDelta)<<32 | uint64(uDelta)

		for {
			state := x.state.Load()
			receivers := uint32(state >> 32)
			lo := uint32(state & chanCasterLoMask)
			if receivers > chanCasterMaxReceivers || receivers != lo {
				panic(`bigbuff: chancaster: add: state invariant violation`)
			}

			// Lock-free fast path: when no Send is active, increment both
			// hi and lo in a single CAS without touching x.mutex.
			if (state & chanCasterSendingBit) == 0 {
				if chanCasterMaxReceivers-receivers < uDelta {
					panic(`bigbuff: chancaster: add: state invariant violation`)
				}
				if x.state.CompareAndSwap(state, state+packedDelta) {
					return int(receivers + uDelta)
				}
				continue
			}

			// Slow path: a Send batch is actively mid-flight. Synchronise on
			// x.mutex to wait for the current Send batch to complete and clear
			// chanCasterSendingBit.
			x.mutex.Lock()
			state = x.state.Load()
			receivers = uint32(state >> 32)
			lo = uint32(state & chanCasterLoMask)
			if (state&chanCasterSendingBit) != 0 ||
				receivers > chanCasterMaxReceivers ||
				receivers != lo ||
				chanCasterMaxReceivers-receivers < uDelta {
				x.mutex.Unlock()
				panic(`bigbuff: chancaster: add: state invariant violation`)
			}
			// While holding x.mutex, no Send can start; atomic Add is safe
			// alongside concurrent lock-free positive or negative Adds.
			state = x.state.Add(packedDelta)
			x.mutex.Unlock()

			receivers = uint32(state >> 32)
			lo = uint32(state & chanCasterLoMask)
			if (state&chanCasterSendingBit) == 0 &&
				receivers <= chanCasterMaxReceivers &&
				receivers >= uDelta &&
				receivers == lo {
				return int(receivers)
			}
			panic(`bigbuff: chancaster: add: state invariant violation`)
		}

	case delta < -chanCasterMaxReceivers:
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		uDelta := uint32(-delta)
		packedSub := ^(uint64(uDelta)<<32 | uint64(uDelta) - 1)

		// Single atomic RMW subtracts uDelta from both hi and lo while
		// preserving bit 31 (chanCasterSendingBit) unless lo underflows!
		// Notice why: if lo < uDelta, subtracting uDelta from bits 0..30
		// borrows from bit 31, flipping chanCasterSendingBit and causing
		// lo != receivers — instantly catching underflow in a single instruction.
		state := x.state.Add(packedSub)
		receivers := uint32(state >> 32)
		lo := uint32(state & chanCasterLoMask)

		if receivers <= chanCasterMaxReceivers &&
			chanCasterMaxReceivers-receivers >= uDelta &&
			receivers == lo {
			if (state & chanCasterSendingBit) != 0 {
				// Send is mid-flight; drain our cancelled share of messages.
				for range uDelta {
					<-x.C
				}
			}
			return int(receivers)
		}
	}

	panic(`bigbuff: chancaster: add: state invariant violation`)
}

