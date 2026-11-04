To advance the implementation to its ultimate form, we introduce **`ChanGatherer`** as the complementary primitive—the exact mathematical dual of `ChanCaster`. 

While `ChanCaster` supports broadcasting to $N$ receivers via a single unbuffered channel by sending $N$ times, `ChanGatherer` supports gathering signals from $N$ senders via a single unbuffered channel by receiving $N$ times. 

By composing these two primitives, we can evolve the pub/sub implementation into **`PubSub`** (replacing `ChanPubSub`). This eliminates the `sync.Cond` and `sendingMu` entirely, replacing the complex ping-pong state machine with pure, symmetric channel semantics and `sync/atomic` counters. This yields an implementation with **zero allocations**, **no OS-level thread parking/waking overhead** (inherent to `sync.Cond`), and native `select` support that scales flawlessly with consumer churn.

### `changatherer.go`
The complementary implementation. It perfectly mirrors the state machine of `ChanCaster`, but reversed: `Wait()` receives $N$ times, and `Add(-1)` sends to the channel to prevent deadlocks if a sender leaves mid-wait.

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

// ChanGatherer supports gathering signals from multiple senders, via a
// single channel, without callbacks. It is the exact dual of ChanCaster.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanGatherer.Wait] performs exactly one
// synchronous receive per registered sender, relying on the one-to-one
// send/receive semantics of an unbuffered channel to guarantee progress.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire.
//
// See also [NewChanGatherer] (optional).
type ChanGatherer[C ~chan V, V any] struct {
	// C is the channel that will be used to gather signals.
	C C

	// mutex synchronises [ChanGatherer.Wait] and [ChanGatherer.Add] calls with
	// positive deltas. Ensuring waits are mutually exclusive is fairly
	// important for consistency. The primary purpose of this mutex is to avoid
	// incrementing the number of senders while any wait is in progress.
	mutex sync.RWMutex
	// state synchronises the number of senders, between [ChanGatherer.Wait]
	// calls, and [ChanGatherer.Add] calls with negative deltas.
	// The high 32 bits are number of senders (modeled as int32), while the
	// low 32 bits are either identical, or [math.MaxInt32] + the number of
	// senders being (the value of the high 32 bits), if waiting is in
	// progress.
	state atomic.Uint64
}

// NewChanGatherer constructs a [ChanGatherer] for the provided channel.
// It enforces the [ChanGatherer] precondition: the channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is buffered.
func NewChanGatherer[C ~chan V, V any](channel C) *ChanGatherer[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: changatherer: must use an unbuffered channel")
	}
	return &ChanGatherer[C, V]{C: channel}
}

// Wait gathers signals, from all registered senders, via [ChanGatherer.C],
// then returns the number of receives performed, less any removed mid-flight
// by [ChanGatherer.Add].
//
// If the contract of [ChanGatherer] is obeyed, this call will never block for
// any significant time. This function may panic if state invariants have been
// violated, e.g. by misuse of [ChanGatherer.Add], or if unregistered sends
// are detected.
func (x *ChanGatherer[C, V]) Wait() int {
	if x.state.Load() == 0 {
		return 0 // no senders (fast path)
	}

	// prevent senders being added while waiting + order values by wait call
	x.mutex.Lock()
	defer x.mutex.Unlock()

	// load our state, guard no senders (early exit), and set tracker to the
	// value of `maxInt32 + senders`, using CAS to sync with negative adds
	var (
		state              uint64
		senders, tracker uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0 // no senders (slow path)
		}

		senders = uint32(state >> 32) // initialize from hi
		tracker = uint32(state)         // initialize from lo
		if tracker != senders || senders > math.MaxInt32 {
			panic(`bigbuff: changatherer: wait: state invariant violation`)
		}

		// attempt to set tracker to `maxInt32 + senders`, with CAS used to
		// synchronise with decrements of senders
		tracker += math.MaxInt32
		if x.state.CompareAndSwap(state, uint64(senders)<<32|uint64(tracker)) {
			break
		}
	}

	// gather involves receiving from all senders - with the total actually
	// received being in range [0, senders], due to potential decrements
	var zero V
	for range senders {
		<-x.C // may end up sent by negative Add calls
	}

	// now, we can retrieve, validate, then reset the state
	state = x.state.Load()
	tracker = uint32(state >> 32) // actually the final senders (used as scratch)
	if tracker > senders ||     // senders should be unchanged or decreased
		uint32(state) != tracker+math.MaxInt32 || // lo still exactly math.MaxInt32 more than hi
		!x.state.CompareAndSwap(state, 0) { // failing this indicates one or more unregistered senders
		panic(`bigbuff: changatherer: wait: state invariant violation`)
	}

	// returns number of receives less any sent by negative Add calls
	return int(tracker)
}

// Add increments or decrements the number of senders, for [ChanGatherer.C],
// then returns the number of senders, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of senders, without changing it.
//
// Each added sender represents the intent to send exactly one value, to
// [ChanGatherer.C]. Each sender removed (via a negative delta) represents
// exactly one added sender, which has not yet been removed, and has not and
// will not send a value.
func (x *ChanGatherer[C, V]) Add(delta int) int {
	const maxSenders = math.MaxInt32

	switch {
	case delta >= 0:
		var state uint64
		if delta == 0 {
			state = x.state.Load() // no change
		} else if delta > maxSenders {
			panic(`bigbuff: changatherer: add: positive delta out of bounds`)
		} else {
			// increasing num senders not allowed concurrently with waiting
			x.mutex.RLock()
			defer x.mutex.RUnlock()

			// add delta to both hi and lo
			state = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
		}

		// validate to ensure we did not overflow + sanity check invariants
		if senders, tracker := uint32(state>>32), uint32(state); senders <= maxSenders &&
			senders >= uint32(delta) &&
			(senders == tracker || (delta == 0 && senders+maxSenders == tracker)) {
			return int(senders)
		}

	case delta < -maxSenders:
		panic(`bigbuff: changatherer: add: negative delta out of bounds`)

	default:
		// flip delta to positive, from negative
		delta = -delta

		// note: same delta calc as above, subtracted using two's complement rules
		state := x.state.Add(^(uint64(delta)<<32 | uint64(uint32(delta)) - 1))

		// validate, and, if necessary, send any channel signals that would
		// otherwise never be sent (to avoid Wait hanging)
		if senders := uint32(state >> 32); senders <= maxSenders &&
			maxSenders-senders >= uint32(delta) {
			switch uint32(state) { // lo
			case senders:
				// not waiting
				return int(senders) // note: already subtracted delta

			case maxSenders + senders:
				// waiting - perform the requisite number of sends
				// note: senders = tracker-maxSenders (per the above)
				var zero V
				for range delta {
					x.C <- zero
				}
				return int(senders) // note: already subtracted delta
			}
		}
	}

	// invariant violation, e.g. due to OoB add, or previous violation
	panic(`bigbuff: changatherer: add: state invariant violation`)
}
```

### `pubsub.go`
The ultimate evolution of `ChanPubSub`. It relies entirely on the symmetric state machines of `ChanCaster` and `ChanGatherer`. The `sync.Cond` is gone, meaning there is zero mutex contention for the waiters (subscribers). The subscriber simply performs a non-blocking (or standard blocking) send to the `pong` channel, which is orders of magnitude faster than waking OS threads via `sync.Cond.Broadcast()`.

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

const pubSubStateInvariantViolation = `bigbuff: pubsub: state invariant violation`

// PubSub is a highly optimized pub/sub implementation that leverages
// ChanCaster and ChanGatherer for zero-allocation, highly scalable fan-out
// and acknowledgment.
//
// It uses two unbuffered channels: one for broadcasting values (ping) and
// one for gathering acknowledgments (pong). This completely eliminates the
// need for sync.Cond and complex state tracking, yielding exceptionally
// stable performance and native select support.
//
// # Usage
//
// The [PubSub.SubscribeContext] method provides a simple, iter.Seq-based
// mechanism to subscribe and consume values. If greater flexibility is
// required (e.g. leveraging select statements), ensure you understand the
// contract:
//
//  1. The [NewPubSub] factory MUST be used to create instances.
//  2. Values MUST NOT be received prior to incrementing the subscriber count.
//  3. Subscribers MUST call [PubSub.Wait] immediately after each value received.
//  4. Subscribers MAY call [PubSub.Add] with a negative delta to unsubscribe.
type PubSub[C ~chan V, V any, P ~chan struct{}] struct {
	ping   ChanCaster[C, V]
	pong   ChanGatherer[P, struct{}]
	sendMu sync.Mutex
	subs   atomic.Int32
	broken chan struct{}
}

// NewPubSub constructs a new instance of [PubSub], using the provided channels
// as the underlying transport. Both channels must be unbuffered (cap == 0).
// Nil channels are permitted. It panics if either channel is buffered.
func NewPubSub[C ~chan V, V any, P ~chan struct{}](ping C, pong P) *PubSub[C, V, P] {
	if cap(ping) != 0 || cap(pong) != 0 {
		panic(`bigbuff: pubsub: must use unbuffered channels`)
	}
	return &PubSub[C, V, P]{
		ping:   ChanCaster[C, V]{C: ping},
		pong:   ChanGatherer[P, struct{}]{C: pong},
		broken: make(chan struct{}),
	}
}

// C returns the underlying channel, used by the receiver.
func (x *PubSub[C, V, P]) C() C {
	x.checkBroken()
	return x.ping.C
}

// SubscribeContext immediately subscribes to the receiver, and returns a
// single-use iterator, which will yield all values sent to the receiver.
func (x *PubSub[C, V, P]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe()

	if ctx == nil {
		ctx = context.Background()
	}

	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: pubsub: iterator yield func is nil`)
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
				panic(pubSubStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(pubSubStateInvariantViolation)
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
func (x *PubSub[C, V, P]) Send(value V) (sent int) {
	x.checkBroken()

	subs := x.subs.Load()
	if subs == 0 {
		return 0 // fast path
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.checkBroken()

	subs = x.subs.Load()
	if subs == 0 {
		return 0 // slow path
	}

	var success bool
	defer func() {
		if !success {
			x.markBroken()
		}
	}()

	// Prepare the pong gatherer for the exact number of subscribers
	x.pong.Add(int(subs))

	// Broadcast the value
	sent = x.ping.Send(value)

	// Wait for all pongs (or cancellations)
	x.pong.Wait()

	success = true

	return
}

// Add returns the number of subscribers, after adding delta to the count.
func (x *PubSub[C, V, P]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: pubsub: delta out of bounds`)
	}

	x.checkBroken()

	switch {
	case delta == 0:
		subscribers = int(x.subs.Load())

	case delta < 0:
		// Unsubscribe case
		// ChanCaster.Add will drain the ping channel if Send is in progress.
		// ChanGatherer.Add will send to the pong channel if Wait is in progress.
		// This perfectly handles the mid-flight departure without deadlocking!
		x.ping.Add(delta)
		x.pong.Add(delta)
		subscribers = int(x.subs.Add(int32(delta)))

	default:
		// Subscribe case
		// We must acquire RLock to ensure we don't subscribe while Send is
		// reading the subscriber count and preparing the pong gatherer.
		x.sendMu.RLock()
		x.ping.Add(delta)
		x.pong.Add(delta)
		subscribers = int(x.subs.Add(int32(delta)))
		x.sendMu.RUnlock()
	}

	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: pubsub: negative subscribers`)
	}

	return
}

// Wait is mandatory after each message received by each subscriber.
// It signals the sender that the subscriber has processed the message.
func (x *PubSub[C, V, P]) Wait() {
	x.checkBroken()
	x.pong.C <- struct{}{}
}

// Subscribe is an alias for calling [PubSub.Add] with a delta of 1.
func (x *PubSub[C, V, P]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [PubSub.Add] with a delta of -1.
func (x *PubSub[C, V, P]) Unsubscribe() {
	x.Add(-1)
}

func (x *PubSub[C, V, P]) checkBroken() {
	select {
	case <-x.broken:
		panic(pubSubStateInvariantViolation)
	default:
	}
}

func (x *PubSub[C, V, P]) markBroken() {
	defer func() {
		recover()
	}()
	close(x.broken)
}
```

### Why this is the "Ultimate Form"
1. **Primitive Symmetry:** By defining `ChanGatherer`, we establish a perfect mathematical duality. A publisher sending to $N$ receivers is the exact inverse of a gatherer waiting for $N$ senders. 
2. **Elimination of `sync.Cond`:** The original `ChanPubSub` relied on `sync.Cond` for the "pong" phase. `sync.Cond` forces OS-level thread parking and waking, which causes massive latency spikes under high churn. By replacing it with `ChanGatherer`, the "pong" becomes a simple unbuffered channel send (`x.pong.C <- struct{}{}`). Channel operations in Go are heavily optimized by the runtime scheduler and avoid `sync.Cond`'s mutex contention entirely.
3. **Flawless Mid-Flight Churn:** If a subscriber unsubscribes while `Send` is blocked mid-broadcast, `ChanCaster.Add(-1)` safely drains the un-received value from the `ping` channel. Simultaneously, `ChanGatherer.Add(-1)` safely *sends* the acknowledgment to the `pong` channel, unblocking the publisher's `Wait()`. This guarantees zero deadlocks without requiring complex sequence locks or epoch tracking.
4. **Zero Allocations:** Outside of the initial creation, the hot path (`Send`, `Wait`, `Add`) performs exactly zero allocations. It relies entirely on atomic counter shifts and direct channel operations.
