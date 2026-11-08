// -- chancaster.go --
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
	"math"
	"sync"
	"sync/atomic"
)

// ChanCaster17FastPath supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanCaster17FastPath.Send] performs exactly one
// synchronous send per registered receiver, relying on the one-to-one
// send/receive semantics of an unbuffered channel to guarantee delivery. A
// buffered channel would violate these invariants and can cause deadlocks or
// undetected missed receives.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire. This allows a
// ChanCaster17FastPath to be used in a "disabled" state — [ChanCaster17FastPath.Send] returns 0
// when there are no receivers, and the channel can be included in a select
// statement without ever being selected.
type ChanCaster17FastPath[C ~chan V, V any] struct {
	// C is the channel that will be used to broadcast messages.
	C C

	// mutex synchronises [ChanCaster17FastPath.Send] and [ChanCaster17FastPath.Add] calls with
	// positive deltas.
	mutex sync.RWMutex

	// state packs two 32-bit words:
	//   - High 32 bits: current receiver count in [0, math.MaxInt32].
	//   - Low 32 bits:  receiver count mirror when idle, or
	//                   (receivers | (1 << 31)) when Send is active.
	// Note: 1<<31 in uint32 is math.MaxInt32 + 1 (0x80000000), which is a pure
	// bit-flag in the sign bit of the low 32-bit word and never carries into
	// the high 32-bit word under two's complement addition/subtraction as long
	// as receivers remains in [0, math.MaxInt32].
	state atomic.Uint64
}

const (
	chanCasterSendingFlag uint32 = 1 << 31 // 0x80000000 (math.MaxInt32 + 1)
	chanCasterMaxRecv     uint32 = math.MaxInt32
)

// NewChanCaster17FastPath constructs a [ChanCaster17FastPath] for the provided channel.
func NewChanCaster17FastPath[C ~chan V, V any](channel C) *ChanCaster17FastPath[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster17FastPath[C, V]{C: channel}
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster17FastPath.C], then returns the number of sends performed, less any
// removed mid-flight by [ChanCaster17FastPath.Add].
func (x *ChanCaster17FastPath[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // fast path: no receivers
	}

	x.mutex.Lock()
	defer x.mutex.Unlock()

	return x.sendLocked(value, nil)
}

// sendLocked executes the broadcast while holding x.mutex.Lock().
// If onArmed != nil, it is invoked immediately after the state transitions
// to the active-sending state (before channel sends begin), enabling
// composite primitives like ChanPubSub to release their outer registration
// locks or signal readiness without spin-waiting.
func (x *ChanCaster17FastPath[C, V]) sendLocked(value V, onArmed func()) int {
	var (
		state              uint64
		receivers, tracker uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0
		}

		receivers = uint32(state >> 32)
		tracker = uint32(state)
		if tracker != receivers || receivers > chanCasterMaxRecv {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}

		// Set the high bit of the low 32-bit word to mark sending in-flight.
		tracker |= chanCasterSendingFlag
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(tracker)) {
			break
		}
	}

	if onArmed != nil {
		onArmed()
	}

	for range receivers {
		x.C <- value
	}

	state = x.state.Load()
	tracker = uint32(state >> 32)
	if tracker > receivers ||
		uint32(state) != (tracker|chanCasterSendingFlag) ||
		!x.state.CompareAndSwap(state, 0) {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	return int(tracker)
}

// Add increments or decrements the number of receivers for [ChanCaster17FastPath.C],
// then returns the number of receivers after the operation.
func (x *ChanCaster17FastPath[C, V]) Add(delta int) int {
	switch {
	case delta >= 0:
		var state uint64
		if delta == 0 {
			state = x.state.Load()
		} else if uint(delta) > uint(chanCasterMaxRecv) {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		} else {
			x.mutex.RLock()
			state = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
			x.mutex.RUnlock()
		}

		if receivers, tracker := uint32(state>>32), uint32(state); receivers <= chanCasterMaxRecv &&
			receivers >= uint32(delta) &&
			(receivers == tracker || (delta == 0 && (receivers|chanCasterSendingFlag) == tracker)) {
			return int(receivers)
		}

	case delta < -int(chanCasterMaxRecv):
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		uDelta := uint32(-delta)
		// Subtract uDelta from both the high 32-bit word and the low 32-bit word.
		// Because uDelta <= receivers <= 0x7FFFFFFF, subtracting uDelta from the
		// low word (which is either `receivers` or `0x80000000 | receivers`)
		// never borrows from bit 31 or bit 32!
		subMask := (uint64(uDelta) << 32) | uint64(uDelta)
		state := x.state.Add(^(subMask - 1))

		if receivers := uint32(state >> 32); receivers <= chanCasterMaxRecv &&
			chanCasterMaxRecv-receivers >= uDelta {
			switch uint32(state) {
			case receivers:
				return int(receivers)
			case receivers | chanCasterSendingFlag:
				for range uDelta {
					<-x.C
				}
				return int(receivers)
			}
		}
	}

	panic(`bigbuff: chancaster: add: state invariant violation`)
}

// -- chancaster.go --
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

// ChanCaster17Pumped supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanCaster17Pumped.Send] performs exactly one
// synchronous send per registered receiver, relying on the one-to-one
// send/receive semantics of an unbuffered channel to guarantee delivery. A
// buffered channel would violate these invariants and can cause deadlocks or
// undetected missed receives.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire. This allows a
// ChanCaster17Pumped to be used in a "disabled" state — [ChanCaster17Pumped.Send] returns 0
// when there are no receivers, and the channel can be included in a select
// statement without ever being selected.
//
// Construction is "softly" enforced: [NewChanCaster17Pumped] validates the
// precondition and panics on a buffered channel, but a value may also be
// built directly via a struct literal, which bypasses that check. Callers
// that construct a [ChanCaster17Pumped] directly are responsible for honouring the
// unbuffered requirement themselves. The channel requirement matches
// ChanPubSub; unlike ChanPubSub, direct ChanCaster17Pumped construction remains
// supported and relies on the caller to preserve the invariant.
//
// See also [NewChanCaster17Pumped] (optional).
type ChanCaster17Pumped[C ~chan V, V any] struct {
	// C is the channel that will be used to broadcast messages.
	//
	// See [ChanCaster17Pumped.Add] and [ChanCaster17Pumped.Send] for usage details.
	C C

	// mutex synchronises [ChanCaster17Pumped.Send] and [ChanCaster17Pumped.Add] calls with
	// positive deltas. Ensuring sends are mutually exclusive is fairly
	// important for consistency, but not strictly necessary. The primary
	// purpose of this mutex is to avoid incrementing the number of receivers
	// while any send is in progress, to facilitate removing receivers while
	// send is in progress.
	mutex sync.RWMutex

	// state synchronises the number of receivers, between [ChanCaster17Pumped.Send]
	// calls, and [ChanCaster17Pumped.Add] calls with negative deltas.
	// The high 32 bits are number of receivers (modeled as int32 in
	// [0, math.MaxInt32]), while the low 32 bits are either identical when
	// idle, or have bit 31 set (receivers | chanCasterSendingFlag) when a
	// send is in progress.
	state atomic.Uint64
}

// NewChanCaster17Pumped constructs a [ChanCaster17Pumped] for the provided channel. It exists
// primarily for type inference convenience, but also enforces the
// [ChanCaster17Pumped] precondition: the channel must be unbuffered (cap == 0). A nil
// channel is permitted. It panics if the channel is buffered.
func NewChanCaster17Pumped[C ~chan V, V any](channel C) *ChanCaster17Pumped[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster17Pumped[C, V]{C: channel}
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster17Pumped.C], then returns the number of sends performed, less any
// removed mid-flight by [ChanCaster17Pumped.Add].
//
// If the contract of [ChanCaster17Pumped] is obeyed, this call will never block for
// any significant time. This function may panic if state invariants have been
// violated, e.g. by misuse of [ChanCaster17Pumped.Add], or if unregistered receives
// are detected.
//
// See [ChanCaster17Pumped.Add] for usage details.
func (x *ChanCaster17Pumped[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // no receivers (fast path)
	}

	// prevent receivers being added while sending + order values by send call
	x.mutex.Lock()
	defer x.mutex.Unlock()

	var (
		state              uint64
		receivers, tracker uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0 // no receivers (slow path)
		}

		receivers = uint32(state >> 32)
		tracker = uint32(state)
		if tracker != receivers || receivers > chanCasterMaxRecv {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}

		// Set chanCasterSendingFlag in the low 32-bit word via CAS to
		// synchronise with concurrent negative Add calls.
		tracker |= chanCasterSendingFlag
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(tracker)) {
			break
		}
	}

	return x.pumpAndReset(value, receivers)
}

// armForSend transitions an idle ChanCaster17Pumped (state == 0) directly into the
// armed sending state for `receivers` receivers in a single atomic CompareAndSwap.
// Caller must ensure mutual exclusion with concurrent Send or positive Add calls.
func (x *ChanCaster17Pumped[C, V]) armForSend(receivers uint32) {
	if receivers == 0 || receivers > chanCasterMaxRecv {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}
	armed := (uint64(receivers) << 32) | uint64(receivers|chanCasterSendingFlag)
	if !x.state.CompareAndSwap(0, armed) {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}
}

// pumpAndReset performs `receivers` channel sends and resets the armed state to 0,
// returning the net number of receivers that were not removed mid-flight.
func (x *ChanCaster17Pumped[C, V]) pumpAndReset(value V, receivers uint32) int {
	for range receivers {
		x.C <- value // may end up received by negative Add calls
	}

	state := x.state.Load()
	tracker := uint32(state >> 32)
	if tracker > receivers ||
		uint32(state) != (tracker|chanCasterSendingFlag) ||
		!x.state.CompareAndSwap(state, 0) {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	return int(tracker)
}

// Add increments or decrements the number of receivers, for [ChanCaster17Pumped.C],
// then returns the number of receivers, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of receivers, without changing it.
func (x *ChanCaster17Pumped[C, V]) Add(delta int) int {
	switch {
	case delta >= 0:
		var state uint64
		if delta == 0 {
			state = x.state.Load() // no change
		} else if uint(delta) > uint(chanCasterMaxRecv) {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		} else {
			// increasing num receivers not allowed concurrently with sending
			x.mutex.RLock()
			state = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
			x.mutex.RUnlock()
		}

		// validate to ensure we did not overflow + sanity check invariants
		if receivers, tracker := uint32(state>>32), uint32(state); receivers <= chanCasterMaxRecv &&
			receivers >= uint32(delta) &&
			(receivers == tracker || (delta == 0 && (receivers|chanCasterSendingFlag) == tracker)) {
			return int(receivers)
		}

	case delta < -int(chanCasterMaxRecv):
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		uDelta := uint32(-delta)

		// Subtract uDelta from both hi and lo 32-bit words via two's complement.
		// Because uDelta <= receivers <= 0x7FFFFFFF, subtracting uDelta from lo
		// (which is either `receivers` or `receivers | 0x80000000`) never borrows
		// across bit 31 or into the high 32-bit word.
		subMask := (uint64(uDelta) << 32) | uint64(uDelta)
		state := x.state.Add(^(subMask - 1))

		if receivers := uint32(state >> 32); receivers <= chanCasterMaxRecv &&
			chanCasterMaxRecv-receivers >= uDelta {
			switch uint32(state) {
			case receivers:
				// not sending
				return int(receivers)

			case receivers | chanCasterSendingFlag:
				// sending - drain the requisite number of sends so Send doesn't hang
				for range uDelta {
					<-x.C
				}
				return int(receivers)
			}
		}
	}

	panic(`bigbuff: chancaster: add: state invariant violation`)
}
