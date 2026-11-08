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
	"math"
	"sync"
	"sync/atomic"
)

const (
	// casterSendingBit occupies bit 63 of ChanCaster47Gemini.state, indicating that a
	// Send call is actively delivering values on ChanCaster47Gemini.C.
	casterSendingBit uint64 = 1 << 63
	// casterWaitingBit occupies bit 31 of ChanCaster47Gemini.state, indicating that one
	// or more positive Add calls are parked waiting for an active Send to finish.
	casterWaitingBit uint64 = 1 << 31
	// casterReceiverMask masks the 31-bit receiver count in both halves of state.
	casterReceiverMask uint32 = math.MaxInt32
)

// ChanCaster47Gemini supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanCaster47Gemini.Send] performs exactly one
// synchronous send per registered receiver, relying on the one-to-one
// send/receive semantics of an unbuffered channel to guarantee delivery. A
// buffered channel would violate these invariants and can cause deadlocks or
// undetected missed receives.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire. This allows a
// ChanCaster47Gemini to be used in a "disabled" state — [ChanCaster47Gemini.Send] returns 0
// when there are no receivers, and the channel can be included in a select
// statement without ever being selected.
//
// Construction is "softly" enforced: [NewChanCaster47Gemini] validates the
// precondition and panics on a buffered channel, but a value may also be
// built directly via a struct literal, which bypasses that check. Callers
// that construct a [ChanCaster47Gemini] directly are responsible for honouring the
// unbuffered requirement themselves. The channel requirement matches
// ChanPubSub47Gemini; unlike ChanPubSub47Gemini, direct ChanCaster47Gemini construction remains
// supported and relies on the caller to preserve the invariant.
//
// See also [NewChanCaster47Gemini] (optional).
type ChanCaster47Gemini[C ~chan V, V any] struct {
	// C is the channel that will be used to broadcast messages.
	//
	// See [ChanCaster47Gemini.Add] and [ChanCaster47Gemini.Send] for usage details.
	C C

	// state packs the receiver count, a mirrored invariant tracker, and the
	// sending/waiting synchronisation flags into a single 64-bit atomic word:
	//
	//	bit  63    : sending flag (1 while Send is actively broadcasting)
	//	bits 32..62: receivers (uint31, in range [0, math.MaxInt32])
	//	bit  31    : waiting flag (1 while positive Add callers are parked)
	//	bits 0..30 : tracker (uint31 mirror of receivers for invariant checks)
	//
	// Placing state immediately after C keeps it 8-byte aligned on both
	// 32-bit and 64-bit architectures while enabling lock-free CAS updates
	// for Add(+delta) and Add(-delta) whenever no Send is mid-loop.
	state atomic.Uint64

	// sendMu serialises concurrent Send calls without contending with Add.
	sendMu sync.Mutex

	// waitMu and waitCh park positive Add calls only when they collide with an
	// in-flight Send. waitCh is allocated lazily so zero-value / struct-literal
	// ChanCaster47Gemini instances remain fully supported.
	waitMu sync.Mutex
	waitCh chan struct{}
}

// NewChanCaster47Gemini constructs a [ChanCaster47Gemini] for the provided channel. It exists
// primarily for type inference convenience, but also enforces the
// [ChanCaster47Gemini] precondition: the channel must be unbuffered (cap == 0). A nil
// channel is permitted. It panics if the channel is buffered.
func NewChanCaster47Gemini[C ~chan V, V any](channel C) *ChanCaster47Gemini[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster47Gemini[C, V]{C: channel}
}

// Len returns the current number of registered receivers after validating
// internal state invariants. It is equivalent to calling [ChanCaster47Gemini.Add] with
// a delta of 0.
func (x *ChanCaster47Gemini[C, V]) Len() int {
	return x.Add(0)
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster47Gemini.C], then returns the number of sends performed, less any
// removed mid-flight by [ChanCaster47Gemini.Add].
//
// If the contract of [ChanCaster47Gemini] is obeyed, this call will never block for
// any significant time. This function may panic if state invariants have been
// violated, e.g. by misuse of [ChanCaster47Gemini.Add], or if unregistered receives
// are detected.
//
// See [ChanCaster47Gemini.Add] for usage details.
func (x *ChanCaster47Gemini[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // no receivers (lock-free fast path)
	}

	// Order values by Send call and ensure only one broadcast runs at a time.
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// Transition state to sending via CAS, synchronising with lock-free Add calls.
	var receivers uint32
	for {
		state := x.state.Load()
		r := uint32(state>>32) & casterReceiverMask
		t := uint32(state) & casterReceiverMask
		if (state&casterSendingBit) != 0 || r != t {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}
		if r == 0 {
			return 0 // no receivers (slow path)
		}

		next := casterSendingBit | (state & casterWaitingBit) | uint64(r)<<32 | uint64(r)
		if x.state.CompareAndSwap(state, next) {
			receivers = r
			break
		}
	}

	// Broadcast to all registered receivers. Any concurrent negative Add calls
	// decrement state atomically and drain the corresponding sends from x.C.
	for range receivers {
		x.C <- value
	}

	// Validate final state and reset back to 0, waking any parked positive Add
	// callers if casterWaitingBit was set.
	for {
		state := x.state.Load()
		rem := uint32(state>>32) & casterReceiverMask
		t := uint32(state) & casterReceiverMask
		if (state&casterSendingBit) == 0 || rem > receivers || rem != t {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}
		hasWaiters := (state & casterWaitingBit) != 0
		if x.state.CompareAndSwap(state, 0) {
			if hasWaiters {
				x.waitMu.Lock()
				if x.waitCh != nil {
					close(x.waitCh)
					x.waitCh = nil
				}
				x.waitMu.Unlock()
			}
			return int(rem)
		}
	}
}

// Add increments or decrements the number of receivers, for [ChanCaster47Gemini.C],
// then returns the number of receivers, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of receivers, without changing it.
//
// Each added receiver represents the intent to receive exactly one value, from
// [ChanCaster47Gemini.C]. Each receiver removed (via a negative delta) represents
// exactly one added receiver, which has not yet been removed, and has not and
// will not receive a value. The valid range for the number of receivers is
// [0, math.MaxInt32], and any add which results in a number of receivers
// outside this range will cause a panic.
//
// The typical usage pattern is to call [ChanCaster47Gemini.Add] with a delta of 1,
// then immediately receive, or attempt to receive, e.g. within a `select`
// statement. If the next-received value (e.g. within said `select` statement)
// is not from [ChanCaster47Gemini.C], and receive won't be promptly re-attempted, then
// [ChanCaster47Gemini.Add] should be called again, with the inverse of the previous
// delta.
//
// Using a delta greater than 1 indicates multiple separate receivers, which
// will all receive the same value, from the same [ChanCaster47Gemini.Send] call. These
// receivers should independently decrement the number of receivers, if
// necessary, as described above.
func (x *ChanCaster47Gemini[C, V]) Add(delta int) int {
	const maxReceivers = math.MaxInt32

	switch {
	case delta == 0:
		state := x.state.Load()
		r := uint32(state>>32) & casterReceiverMask
		t := uint32(state) & casterReceiverMask
		if r == t {
			return int(r)
		}

	case delta > maxReceivers:
		panic(`bigbuff: chancaster: add: positive delta out of bounds`)

	case delta > 0:
		d := uint32(delta)
		for {
			state := x.state.Load()
			r := uint32(state>>32) & casterReceiverMask
			t := uint32(state) & casterReceiverMask
			if r != t || d > maxReceivers-r {
				break // invariant violation or overflow
			}
			if (state & casterSendingBit) != 0 {
				x.waitForSendComplete()
				continue
			}
			newR := r + d
			next := (state & casterWaitingBit) | uint64(newR)<<32 | uint64(newR)
			if x.state.CompareAndSwap(state, next) {
				return int(newR)
			}
		}

	case delta < -maxReceivers:
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		// Lock-free decrement: works both when idle and concurrently mid-Send.
		d := uint32(-delta)
		for {
			state := x.state.Load()
			r := uint32(state>>32) & casterReceiverMask
			t := uint32(state) & casterReceiverMask
			if r != t || d > r {
				break // invariant violation or underflow
			}
			newR := r - d
			next := (state & (casterSendingBit | casterWaitingBit)) | uint64(newR)<<32 | uint64(newR)
			if x.state.CompareAndSwap(state, next) {
				if (state & casterSendingBit) != 0 {
					// Send is actively broadcasting; drain the d sends that
					// would otherwise have no receiver.
					for range d {
						<-x.C
					}
				}
				return int(newR)
			}
		}
	}

	// Invariant violation, e.g. due to out-of-bounds add or prior misuse.
	panic(`bigbuff: chancaster: add: state invariant violation`)
}

// tryWithdraw applies a negative receiver delta unless the send-side count has
// already reached zero. ChanPubSub uses it to close the race between the final
// channel receive and the sender clearing its one-shot state; false means the
// completed Send already consumed all intents, not that a receive is missing.
func (x *ChanCaster47Gemini[C, V]) tryWithdraw(delta int) bool {
	if delta >= 0 || delta < -math.MaxInt32 {
		panic(`bigbuff: chancaster: withdraw delta out of bounds`)
	}
	d := uint32(-delta)
	for {
		state := x.state.Load()
		r := uint32(state>>32) & casterReceiverMask
		t := uint32(state) & casterReceiverMask
		if r != t {
			panic(`bigbuff: chancaster: withdraw: state invariant violation`)
		}
		if r == 0 {
			return false
		}
		if d > r {
			panic(`bigbuff: chancaster: withdraw: underflow`)
		}
		newR := r - d
		next := (state & (casterSendingBit | casterWaitingBit)) | uint64(newR)<<32 | uint64(newR)
		if x.state.CompareAndSwap(state, next) {
			if (state & casterSendingBit) != 0 {
				for range d {
					<-x.C
				}
			}
			return true
		}
	}
}

// waitForSendComplete parks the caller until the active Send clears
// casterSendingBit, setting casterWaitingBit under waitMu so no wakeup is lost.
func (x *ChanCaster47Gemini[C, V]) waitForSendComplete() {
	x.waitMu.Lock()
	for (x.state.Load() & casterSendingBit) != 0 {
		// Advertise that a waiter is present before reading waitCh.
		for {
			state := x.state.Load()
			if (state&casterSendingBit) == 0 || (state&casterWaitingBit) != 0 {
				break
			}
			if x.state.CompareAndSwap(state, state|casterWaitingBit) {
				break
			}
		}
		if (x.state.Load() & casterSendingBit) == 0 {
			break
		}
		ch := x.waitCh
		if ch == nil {
			ch = make(chan struct{})
			x.waitCh = ch
		}
		x.waitMu.Unlock()
		<-ch
		x.waitMu.Lock()
	}
	x.waitMu.Unlock()
}
