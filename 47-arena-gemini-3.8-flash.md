### Low-Level Go Runtime Evolution & Architectural Overview

To advance `bigbuff` to its highest level of runtime sophistication while preserving 100% backward compatibility and zero-allocation predictability, we evolve both existing primitives (`ChanCaster` and `ChanPubSub`) and introduce a third, complementary primitive (`ChanRing`):

1. **`ChanCaster` (`chancaster.go`) — Lock-Free Atomic State Machine**:
   - **Eliminates `sync.RWMutex` cache-line contention on subscriber churn**: In the original implementation, every `Add(+1)` acquired and released an `RWMutex` read lock (three atomic RMW operations across `readerCount` and `state` per subscribe). The evolved implementation packs `sending` (bit 63), `waiting` (bit 31), `receivers` (bits 32..62), and a mirrored `tracker` checksum (bits 0..30) into a single `atomic.Uint64`.
   - **1-CAS fast path**: Both `Add(+delta)` and `Add(-delta)` execute via a single lock-free `CompareAndSwap` when no broadcast is mid-loop, while `Add(-delta)` remains lock-free even *during* an active `Send`. `Send` only touches `sendMu` and lazily parks colliding `Add(+delta)` callers on a channel notification barrier with zero constructor overhead (preserving struct-literal construction).

2. **`ChanPubSub` (`chanpubsub.go`) — Zero-Allocation Binary-Tree Channel Barrier & Spin-Free Unsubscribe**:
   - **Replaces `sync.Cond` thundering-herd pong barrier with a lock-free binary-tree channel relay**: Previously, `N` subscribers contended on a single `sync.Mutex` inside `sync.Cond.Wait()` and suffered a thundering herd on `Broadcast()`. By pre-arming `pongRem` and `pongExit` before `ping.Send`, subscribers decrement an atomic counter and park on a shared unbuffered `pongGate` channel (zero heap allocations via stack-local `runtime.sudog` pooling). When the final subscriber arrives (`pongRem == 0`), it triggers a parallel binary-tree wakeup (`O(log₂ N)` depth across `P` runqueues via `runtime.goready` `runnext` handoffs) with zero mutexes and zero heap allocations per broadcast.
   - **Eliminates the CPU spin loop in `Add(delta < 0)`**: Replaces the `TryRLock` busy-wait loop during `Send` arming with clean `sync.Cond` parking (`0%` idle CPU burn).
   - **Fixes the `SubscribeContext` cancellation race**: Disarms `context.AfterFunc` via `stop()` once the iterator loop starts so `AfterFunc` and the iterator goroutine can never concurrently race on `Unsubscribe` vs. `<-C`.

3. **`ChanRing[V]` (`chanring.go`) — Novel Pipelined Multi-Consumer Broadcast Ring Over Native Go Channels**:
   - **Complements synchronous rendezvous (`ChanCaster` / `ChanPubSub`)**: `ChanCaster` and `ChanPubSub` are unbuffered (`cap == 0`), meaning `Send` blocks until every consumer finishes each individual message (`1` message in flight). `ChanRing[V]` is a bounded, power-of-2 **pipelined multi-consumer broadcast ring** (`K` messages in flight) that supports native `select` statements without per-consumer channel allocations:
     - **Zero-allocation stack-value readers (`RingReader[V]`)**: `Subscribe()` returns a small value struct (`24 bytes`) with **zero heap allocations** and **zero per-consumer `chan` creations**, sustaining extreme subscribe/unsubscribe churn with zero GC pressure.
     - **Native `select` integration (`Ready() <-chan struct{}`)**: Uses per-slot `notifyNode` atomic pointers (`chan struct{}` in Go's `noscan` allocator span). If messages are already buffered (`seq < writeSeq`), `Ready()` returns a pre-closed singleton channel (`0 B/op`, immediate non-parking `select` readiness). If caught up, `Ready()` returns the tail slot's notification channel, waking all waiting consumers in one `runtime.closechan` call when published.
     - **Single-copy payload storage & lock-free batch draining**: `Send` writes `V` **once** into a pre-allocated ring slot regardless of consumer count. Consumers drain bursts via lock-free atomic loads (`TryRecv()`), amortizing runtime scheduler wakeups across entire bursts, and the final reader clears `slot.value` to prevent GC retention and signals `slot.drained` for lossless backpressure (`Send`) or non-blocking overrun detection (`TrySend`).

---

### `chancaster.go`

```go
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
	// casterSendingBit occupies bit 63 of ChanCaster.state, indicating that a
	// Send call is actively delivering values on ChanCaster.C.
	casterSendingBit uint64 = 1 << 63
	// casterWaitingBit occupies bit 31 of ChanCaster.state, indicating that one
	// or more positive Add calls are parked waiting for an active Send to finish.
	casterWaitingBit uint64 = 1 << 31
	// casterReceiverMask masks the 31-bit receiver count in both halves of state.
	casterReceiverMask uint32 = math.MaxInt32
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
	// ChanCaster instances remain fully supported.
	waitMu sync.Mutex
	waitCh chan struct{}
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

// Len returns the current number of registered receivers after validating
// internal state invariants. It is equivalent to calling [ChanCaster.Add] with
// a delta of 0.
func (x *ChanCaster[C, V]) Len() int {
	return x.Add(0)
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

// waitForSendComplete parks the caller until the active Send clears
// casterSendingBit, setting casterWaitingBit under waitMu so no wakeup is lost.
func (x *ChanCaster[C, V]) waitForSendComplete() {
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
```

---

### `chanpubsub.go`

```go
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
	// ping orchestrates message fan-out over the underlying unbuffered channel.
	ping ChanCaster[C, V]

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
	x.subCond = sync.NewCond(&x.subMu)
	x.pongGate = make(chan struct{})
	x.pongDone = make(chan struct{})
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

// Subscribers returns the current number of active subscribers.
func (x *ChanPubSub[C, V]) Subscribers() int {
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
func (x *ChanPubSub[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
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
		panic(chanPubSubStateInvariantViolation)
	}
	x.waiters.Store(0)
	x.pongWake.Store(0)
	x.pongRem.Store(int32(subscribers))
	x.pongExit.Store(int32(subscribers))

	x.subMu.Lock()
	x.pingArmed = true
	x.subCond.Broadcast()
	x.subMu.Unlock()

	// Ping: broadcast value to all active subscribers.
	sent = x.ping.Send(value)

	// Pong: wait until every subscriber has either completed Wait or
	// unsubscribed mid-flight via Add(-delta).
	select {
	case <-x.pongDone:
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	}

	success = true
	return sent
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
		inFlight := x.sending
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
			// Drain -delta messages from ping.C and account for those
			// unsubscribed participants in the pong barrier.
			x.ping.Add(delta)
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
func (x *ChanPubSub[C, V]) Wait() {
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
			panic(chanPubSubStateInvariantViolation)
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
		panic(chanPubSubStateInvariantViolation)
	}

	if x.pongExit.Add(-1) == 0 {
		select {
		case x.pongDone <- struct{}{}:
		case <-x.broken:
			panic(chanPubSubStateInvariantViolation)
		}
	}
}

// completePongParticipants accounts for count subscribers that unsubscribed
// mid-Send without calling Wait.
func (x *ChanPubSub[C, V]) completePongParticipants(count int) {
	rem := x.pongRem.Add(-int32(count))
	if rem < 0 {
		x.markBroken()
		panic(chanPubSubStateInvariantViolation)
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
		panic(chanPubSubStateInvariantViolation)
	}
	if exit == 0 {
		select {
		case x.pongDone <- struct{}{}:
		case <-x.broken:
			panic(chanPubSubStateInvariantViolation)
		}
	}
}

// propagatePongWake performs a lock-free binary-tree fan-out wakeup across
// pongGate, waking all parked subscribers in O(log2 N) parallel hops without
// mutex contention or thundering-herd broadcasts.
func (x *ChanPubSub[C, V]) propagatePongWake() {
	for range 2 {
		if x.pongWake.Add(-1) >= 0 {
			select {
			case x.pongGate <- struct{}{}:
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			}
		} else {
			break
		}
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

func (x *ChanPubSub[C, V]) markBroken() {
	x.brokenOnce.Do(func() {
		close(x.broken)
	})
}

func (x *ChanPubSub[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
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

func (x *ChanPubSub[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
```

---

### `chanring.go`

```go
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
	"math/bits"
	"sync"
	"sync/atomic"
)

// closedRingNotify is a pre-closed singleton channel returned by
// [RingReader.Ready] whenever one or more unread messages are already buffered
// in the ring or the ring has been closed, allowing select statements to fire
// immediately with zero allocations and zero scheduler parking.
var closedRingNotify = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type ringNotifyNode struct {
	seq uint64
	ch  chan struct{}
}

type ringSlot[V any] struct {
	// seq stores the 1-based sequence number currently occupying this slot.
	// Written with Release semantics after value and notify are prepared;
	// read with Acquire semantics by readers.
	seq atomic.Uint64

	// readers tracks how many active subscribers have not yet consumed or
	// skipped this slot. When it transitions 1 -> 0, value is zeroed for GC
	// and any publisher waiting on drained is woken.
	readers atomic.Int32

	// waitingDrained indicates a publisher is blocked waiting for readers -> 0.
	waitingDrained atomic.Bool

	// notify points to the notification channel for the next sequence that
	// will occupy this slot index.
	notify atomic.Pointer[ringNotifyNode]

	// drained is a 1-buffered channel signaled when readers reaches 0 while
	// waitingDrained is true.
	drained chan struct{}

	// value holds the broadcast payload stored once for all subscribers.
	value V
}

// ChanRing is a bounded, multi-consumer, pipelined broadcast ring that
// integrates natively with Go select statements without allocating per-consumer
// channels.
//
// Unlike [ChanCaster] and [ChanPubSub], which perform unbuffered rendezvous
// broadcasts (1 message in flight, stalling the publisher until every
// subscriber finishes each message), ChanRing allows up to Cap() messages to
// be pipelined in flight concurrently while preserving:
//
//   - Zero heap allocations on [ChanRing.Subscribe], [RingReader.Close],
//     [RingReader.Ready], and [RingReader.TryRecv], enabling extreme consumer
//     churn with zero GC pressure.
//   - Single-copy payload storage: each published value V is written once into
//     a pre-allocated ring slot regardless of the number of consumers, and
//     automatically zeroed when the final consumer advances past the slot.
//   - Native select composability via [RingReader.Ready] (<-chan struct{}),
//     paired with lock-free batch draining via [RingReader.TryRecv] to amortize
//     Go runtime scheduler wakeups across message bursts.
//   - Lossless backpressure ([ChanRing.Send]) or non-blocking overrun
//     detection ([ChanRing.TrySend]).
type ChanRing[V any] struct {
	slots []ringSlot[V]
	mask  uint64

	// writeSeq holds the 1-based sequence number of the most recently published
	// message (0 when nothing has been published yet).
	writeSeq atomic.Uint64

	// subscribers tracks the number of currently active RingReader instances.
	subscribers atomic.Int32

	closed atomic.Bool

	// mu serialises Send, TrySend, Subscribe, RingReader.Close, and Close.
	// Hot-path consumer operations (Ready and TryRecv) are 100% lock-free.
	mu sync.Mutex
}

// NewChanRing constructs a [ChanRing] with capacity rounded up to the next
// power of two (minimum 2). It panics if capacity < 1.
func NewChanRing[V any](capacity int) *ChanRing[V] {
	if capacity < 1 {
		panic("bigbuff: chanring: capacity must be >= 1")
	}
	size := nextPowerOfTwo(capacity)
	if size < 2 {
		size = 2
	}

	r := &ChanRing[V]{
		slots: make([]ringSlot[V], size),
		mask:  uint64(size - 1),
	}
	for i := range size {
		// Slot i first serves 1-based sequence i (with slot 0 serving sequence size).
		firstSeq := uint64(i)
		if firstSeq == 0 {
			firstSeq = uint64(size)
		}
		r.slots[i].drained = make(chan struct{}, 1)
		r.slots[i].notify.Store(&ringNotifyNode{
			seq: firstSeq,
			ch:  make(chan struct{}),
		})
	}
	return r
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// Cap returns the ring buffer capacity (always a power of two >= 2).
func (r *ChanRing[V]) Cap() int {
	return len(r.slots)
}

// Subscribers returns the current number of active subscribers.
func (r *ChanRing[V]) Subscribers() int {
	return int(r.subscribers.Load())
}

// Send broadcasts value to all active subscribers, returning the number of
// subscribers that will receive the value.
//
// If the slowest active subscriber is a full ring ([ChanRing.Cap] messages)
// behind, Send blocks until that subscriber advances or unsubscribes, or until
// the ring is closed (in which case Send returns 0).
//
// If there are no active subscribers, Send returns 0 immediately without
// consuming a ring slot or allocating.
func (r *ChanRing[V]) Send(value V) int {
	sent, _ := r.publish(value, true)
	return sent
}

// TrySend attempts to broadcast value to all active subscribers without
// blocking. It returns (sent, true) if the value was published (including when
// there are 0 subscribers, returning (0, true)), or (0, false) if the ring is
// full or closed.
func (r *ChanRing[V]) TrySend(value V) (sent int, ok bool) {
	return r.publish(value, false)
}

func (r *ChanRing[V]) publish(value V, block bool) (int, bool) {
	if r.closed.Load() {
		return 0, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return 0, false
	}

	subs := r.subscribers.Load()
	if subs == 0 {
		return 0, true // fast path: nobody listening
	}

	nextSeq := r.writeSeq.Load() + 1
	slot := &r.slots[nextSeq&r.mask]

	// Ensure the slot from generation (nextSeq - Cap) has been completely
	// drained by all readers before reusing it.
	if slot.readers.Load() > 0 {
		if !block {
			return 0, false
		}
		// Drain any stale token before arming waitingDrained.
		select {
		case <-slot.drained:
		default:
		}
		slot.waitingDrained.Store(true)
		for slot.readers.Load() > 0 && !r.closed.Load() {
			// Release mu while waiting so slow readers can call Close() to
			// unsubscribe and unblock Send without deadlocking!
			r.mu.Unlock()
			<-slot.drained
			r.mu.Lock()
		}
		slot.waitingDrained.Store(false)
		if r.closed.Load() {
			return 0, false
		}
		// Re-check subscribers in case membership changed while parked.
		subs = r.subscribers.Load()
		if subs == 0 {
			return 0, true
		}
	}

	curNode := slot.notify.Load()
	nextNode := &ringNotifyNode{
		seq: nextSeq + uint64(len(r.slots)),
		ch:  make(chan struct{}),
	}

	// Publish value with Release semantics on slot.seq and writeSeq, then close
	// the previous generation's notification channel to wake all caught-up
	// readers in a single runtime closechan operation.
	slot.value = value
	slot.readers.Store(subs)
	slot.notify.Store(nextNode)
	r.writeSeq.Store(nextSeq)
	slot.seq.Store(nextSeq)
	close(curNode.ch)

	return int(subs), true
}

// Close closes the ring, waking any blocked [ChanRing.Send] calls and all
// readers parked on [RingReader.Ready]. Subsequent sends return 0 / false,
// while readers may continue draining any already-published messages via
// [RingReader.TryRecv].
func (r *ChanRing[V]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.closed.CompareAndSwap(false, true) {
		return
	}

	// Wake any publisher blocked on a full slot and close all pending slot
	// notification channels so parked readers wake immediately.
	for i := range r.slots {
		slot := &r.slots[i]
		if slot.waitingDrained.Load() {
			select {
			case slot.drained <- struct{}{}:
			default:
			}
		}
		if node := slot.notify.Load(); node != nil {
			// Replace with an already-closed node before closing node.ch so
			// repeated inspection never observes an unclosed channel.
			slot.notify.Store(&ringNotifyNode{
				seq: node.seq,
				ch:  closedRingNotify,
			})
			if node.ch != closedRingNotify {
				close(node.ch)
			}
		}
	}
}

// Subscribe creates a new [RingReader] positioned at the current head of the
// ring, receiving all messages published after Subscribe returns.
//
// Subscribe performs zero heap allocations; callers should call
// [RingReader.Close] when finished consuming so unread slots can be reclaimed.
func (r *ChanRing[V]) Subscribe() RingReader[V] {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return RingReader[V]{ring: r, closed: true}
	}

	head := r.writeSeq.Load()
	r.subscribers.Add(1)
	return RingReader[V]{
		ring: r,
		seq:  head,
	}
}

// SubscribeContext subscribes to the ring and returns a single-use [iter.Seq]
// that yields broadcast messages until ctx is cancelled, the iterator stops
// early, or the ring is closed.
func (r *ChanRing[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := r.Subscribe()

	if ctx == nil {
		ctx = context.Background()
	}

	// Ensure subscription is cleaned up even if the iterator is never invoked.
	var closeOnce sync.Once
	cleanup := func() {
		closeOnce.Do(sub.Close)
	}
	stop := context.AfterFunc(ctx, cleanup)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				cleanup()
			}
			panic("bigbuff: chanring: iterator yield func is nil")
		}

		if !stop() {
			return
		}
		defer cleanup()

		for {
			// Drain all buffered messages without touching select or runtime channels.
			for {
				v, ok := sub.TryRecv()
				if !ok {
					break
				}
				if !yield(v) {
					return
				}
			}

			if sub.Closed() {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-sub.Ready():
			}
		}
	}
}

// RingReader is a single-goroutine consumer cursor into a [ChanRing].
//
// RingReader is designed to be held by value (zero heap allocations) and used
// by a single consumer goroutine. Its [RingReader.Ready] method returns a
// standard receive-only channel suitable for use in Go select statements.
type RingReader[V any] struct {
	ring   *ChanRing[V]
	seq    uint64
	closed bool
}

// Ready returns a channel that becomes readable (closed) as soon as at least
// one unread message is available in the ring or the ring/reader is closed.
//
// When messages are already buffered, Ready returns a pre-closed channel with
// zero allocations, causing select statements to proceed immediately without
// parking the goroutine.
func (r *RingReader[V]) Ready() <-chan struct{} {
	if r.closed || r.ring == nil || r.ring.closed.Load() {
		return closedRingNotify
	}

	target := r.seq + 1
	if r.ring.writeSeq.Load() >= target {
		return closedRingNotify
	}

	slot := &r.ring.slots[target&r.ring.mask]
	node := slot.notify.Load()
	if node == nil || node.seq != target || slot.seq.Load() >= target || r.ring.closed.Load() {
		return closedRingNotify
	}
	return node.ch
}

// TryRecv consumes and returns the next unread message if available, without
// blocking. Callers woken by [RingReader.Ready] should drain all available
// messages in a loop until TryRecv returns ok == false.
func (r *RingReader[V]) TryRecv() (value V, ok bool) {
	if r.closed || r.ring == nil {
		return value, false
	}

	target := r.seq + 1
	slot := &r.ring.slots[target&r.ring.mask]
	if slot.seq.Load() < target {
		return value, false
	}

	value = slot.value
	r.seq = target
	releaseRingSlot(slot)
	return value, true
}

// Pending returns the number of published messages currently buffered ahead of
// this reader.
func (r *RingReader[V]) Pending() int {
	if r.closed || r.ring == nil {
		return 0
	}
	head := r.ring.writeSeq.Load()
	if head <= r.seq {
		return 0
	}
	return int(head - r.seq)
}

// Closed reports whether either this reader or its underlying [ChanRing] has
// been closed.
func (r *RingReader[V]) Closed() bool {
	return r.closed || r.ring == nil || r.ring.closed.Load()
}

// Close unsubscribes this reader from the underlying [ChanRing], releasing any
// unread slots so they cannot block future [ChanRing.Send] calls. Close is
// idempotent.
func (r *RingReader[V]) Close() {
	if r.closed || r.ring == nil {
		return
	}

	r.ring.mu.Lock()
	defer r.ring.mu.Unlock()

	if r.closed {
		return
	}
	r.closed = true

	if r.ring.closed.Load() {
		// Release any unread slots even if the ring was closed first.
		head := r.ring.writeSeq.Load()
		for r.seq < head {
			r.seq++
			releaseRingSlot(&r.ring.slots[r.seq&r.ring.mask])
		}
		return
	}

	r.ring.subscribers.Add(-1)
	head := r.ring.writeSeq.Load()
	for r.seq < head {
		r.seq++
		releaseRingSlot(&r.ring.slots[r.seq&r.ring.mask])
	}
}

func releaseRingSlot[V any](slot *ringSlot[V]) {
	if slot.readers.Add(-1) == 0 {
		var zero V
		slot.value = zero
		if slot.waitingDrained.Load() {
			select {
			case slot.drained <- struct{}{}:
			default:
			}
		}
	}
}
```

---

### `chanring_test.go` (Verification & Allocation Benchmarks)

```go
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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChanCaster_ConcurrentChurnAndSelect(t *testing.T) {
	ch := make(chan int)
	caster := NewChanCaster(ch)

	const workers = 64
	const rounds = 200

	var received atomic.Int64
	var cancelled atomic.Int64
	var wg sync.WaitGroup

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := range rounds {
				caster.Add(1)
				if (id+r)%5 == 0 {
					// Simulate select timeout / cancellation mid-broadcast.
					if caster.Add(-1) < 0 {
						t.Errorf("negative receivers")
					}
					cancelled.Add(1)
					continue
				}
				v := <-caster.C
				received.Add(int64(v))
			}
		}(w)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			if caster.Len() != 0 {
				t.Fatalf("expected 0 remaining receivers, got %d", caster.Len())
			}
			return
		default:
			caster.Send(1)
		}
	}
}

func TestChanPubSub_ScalableBroadcastAndContextCancel(t *testing.T) {
	ch := make(chan int)
	ps := NewChanPubSub(ch)

	const subscribers = 128
	const messages = 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ready sync.WaitGroup
	var done sync.WaitGroup
	var total atomic.Int64

	for i := range subscribers {
		ready.Add(1)
		done.Add(1)
		go func(id int) {
			defer done.Done()
			subCtx, subCancel := context.WithCancel(ctx)
			defer subCancel()

			seq := ps.SubscribeContext(subCtx)
			ready.Done()

			count := 0
			for v := range seq {
				total.Add(int64(v))
				count++
				if id%4 == 0 && count == 25 {
					subCancel()
				}
			}
		}(i)
	}

	ready.Wait()

	for m := 1; m <= messages; m++ {
		ps.Send(1)
	}

	cancel()
	done.Wait()

	if ps.Subscribers() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", ps.Subscribers())
	}
	if total.Load() == 0 {
		t.Fatal("expected messages to be received")
	}
}

func TestChanRing_PipelinedSelectAndBackpressure(t *testing.T) {
	ring := NewChanRing[int](16)
	defer ring.Close()

	const subscribers = 32
	const messages = 250

	var wg sync.WaitGroup
	var ready sync.WaitGroup

	for s := range subscribers {
		wg.Add(1)
		ready.Add(1)
		go func(id int) {
			defer wg.Done()
			sub := ring.Subscribe()
			defer sub.Close()
			ready.Done()

			expected := 1
			for expected <= messages {
				select {
				case <-sub.Ready():
					for {
						v, ok := sub.TryRecv()
						if !ok {
							break
						}
						if v != expected {
							t.Errorf("sub %d: got %d, want %d", id, v, expected)
							return
						}
						expected++
					}
				case <-time.After(5 * time.Second):
					t.Errorf("sub %d: timed out at %d", id, expected)
					return
				}
			}
		}(s)
	}

	ready.Wait()

	for m := 1; m <= messages; m++ {
		if sent := ring.Send(m); sent != subscribers {
			t.Fatalf("msg %d: sent=%d, want=%d", m, sent, subscribers)
		}
	}

	wg.Wait()

	if ring.Subscribers() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", ring.Subscribers())
	}
}

func BenchmarkChanPubSub_Broadcast128(b *testing.B) {
	ch := make(chan int)
	ps := NewChanPubSub(ch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const subs = 128
	var ready sync.WaitGroup
	var done sync.WaitGroup
	for range subs {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			seq := ps.SubscribeContext(ctx)
			ready.Done()
			for range seq {
			}
		}()
	}
	ready.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ps.Send(i)
	}
	b.StopTimer()
	cancel()
	done.Wait()
}

func BenchmarkChanRing_Broadcast128(b *testing.B) {
	ring := NewChanRing[int](64)
	const subs = 128
	var ready sync.WaitGroup
	var done sync.WaitGroup
	for range subs {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			sub := ring.Subscribe()
			defer sub.Close()
			ready.Done()
			for !sub.Closed() {
				<-sub.Ready()
				for {
					if _, ok := sub.TryRecv(); !ok {
						break
					}
				}
			}
		}()
	}
	ready.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ring.Send(i)
	}
	b.StopTimer()
	ring.Close()
	done.Wait()
}
```
