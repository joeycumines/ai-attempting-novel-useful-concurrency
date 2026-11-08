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
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	chanPubSubPackedPhaseIdle uint32 = iota
	chanPubSubPackedPhaseSending
	chanPubSubPackedPhaseFinalizing
)

const (
	chanPubSubPackedMaxSubscribers = uint32(math.MaxInt32)
	chanPubSubPackedBroken         = "bigbuff: chanpubsubpacked: state invariant violation"
)

// ChanPubSubPacked is an alternative channel-backed pub/sub implementation.
//
// It uses one shared, unbuffered channel and a single atomic word to track
// both the subscriber count and the broadcast phase. Unlike ChanPubSub, it
// does not use a nested ChanCaster or a second atomic subscriber counter.
//
// The channel is directly usable in select statements. Each registered
// subscriber represents one receive from C for each Send. A subscriber that
// withdraws before receiving the current value calls Add(-1); while a send is
// active, that call atomically removes its registration and performs the
// corresponding receive itself. This lets the sender finish without waiting
// for a subscriber that has elected to leave.
//
// # Contract
//
// The following rules are required for correct use:
//
//  1. Instances MUST be created with NewChanPubSubPacked. The channel MUST
//     be unbuffered. A nil channel is permitted and behaves as a disabled
//     channel, just as it does for ordinary Go channel operations.
//  2. A subscriber MUST register before receiving, normally with Add(1) or
//     Subscribe.
//  3. After receiving a value from C, a subscriber MUST call Wait exactly
//     once before receiving another value or unsubscribing. A receive that
//     observes a closed channel is not a value and MUST NOT be followed by
//     Wait.
//  4. A subscriber that does not receive the current value MUST promptly
//     unregister, normally with Add(-1) or Unsubscribe. It MUST NOT unregister
//     after receiving the current value but before calling Wait.
//  5. Wait MUST only be called after a successful receive of a value, and
//     each successful receive MUST be followed by exactly one Wait.
//  6. The net subscriber count MUST never become negative. After
//     unsubscribing, a subscriber MUST NOT receive further values.
//  7. Subscribers MUST continue receiving or promptly unsubscribe. A
//     registered subscriber that neither receives nor unregisters can block
//     Send indefinitely; that is inherent in synchronous delivery over an
//     unbuffered channel.
//
// Concurrent calls to Send are serialized. Positive Add calls are excluded
// from the channel-send portion of a broadcast; negative Add calls may
// participate in that broadcast by consuming the receive corresponding to
// the registration they remove. Registration and removal are O(1), and a
// broadcast performs one unbuffered send per registered subscriber at its
// start.
//
// The zero value is not usable. Do not copy a ChanPubSubPacked after first use.
type ChanPubSubPacked[C ~chan V, V any] struct {
	c C

	// state packs subscriber cardinality in the high 32 bits and phase in
	// the low 32 bits. The only mutable state transitions are CAS operations.
	state atomic.Uint64

	// gate prevents registration changes and send-start snapshots from
	// crossing. Negative Add calls in the sending phase deliberately do not
	// take this lock: they decrement state and become channel receivers.
	gate sync.RWMutex

	// sendMu serializes complete broadcasts, including the acknowledgement
	// barrier. Releasing it before all Wait calls complete would allow a fast
	// next send to be consumed by a subscriber that has not acknowledged the
	// previous one.
	sendMu sync.Mutex

	pongMu   sync.Mutex
	pongCond sync.Cond
	pongN    int

	broken     chan struct{}
	brokenOnce sync.Once
}

// NewChanPubSubPacked constructs a ChanPubSubPacked. The supplied channel
// must be unbuffered (cap == 0); a nil channel is allowed. It panics if the
// channel is buffered.
func NewChanPubSubPacked[C ~chan V, V any](c C) *ChanPubSubPacked[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanpubsubpacked: must use an unbuffered channel")
	}
	x := &ChanPubSubPacked[C, V]{c: c, broken: make(chan struct{})}
	x.pongCond.L = &x.pongMu
	return x
}

// C returns the underlying channel. It may be used in a select statement.
// The channel MUST NOT be sent to except by Send, and receives MUST obey the
// ChanPubSubPacked contract.
func (x *ChanPubSubPacked[C, V]) C() C {
	x.checkUsedFactory()
	x.checkBroken()
	return x.c
}

// SubscribeContext immediately registers one subscriber and returns a
// single-use iterator. It automatically unregisters when ctx is cancelled,
// iteration stops early, or the channel is closed.
//
// The iterator MUST be used promptly, or ctx MUST be cancelled promptly, to
// avoid retaining a subscriber that never receives. As with other iter.Seq
// values, using the iterator more than once is unsupported.
func (x *ChanPubSubPacked[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}

	// Exactly one of the context callback and the iterator owns the
	// unsubscription. In particular, cancellation while the iterator is in
	// Wait must not unregister a receiver that has already taken this send.
	var claimed atomic.Bool
	var stop func() bool
	if ctx.Done() != nil {
		stop = context.AfterFunc(ctx, func() {
			if claimed.CompareAndSwap(false, true) {
				x.Unsubscribe()
			}
		})
	}

	return func(yield func(V) bool) {
		if yield == nil {
			if claimed.CompareAndSwap(false, true) {
				if stop != nil {
					stop()
				}
				x.Unsubscribe()
			}
			panic("bigbuff: chanpubsubpacked: iterator yield func is nil")
		}
		if !claimed.CompareAndSwap(false, true) {
			return
		}
		if stop != nil {
			stop()
		}
		defer x.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubPackedBroken)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubPackedBroken)
			case v, ok := <-x.c:
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

// Send broadcasts value to all subscribers registered at the start of the
// send phase. It returns the number of subscribers that received value; a
// subscriber removed during the send phase is excluded from the result.
//
// Send can block until every remaining subscriber receives the value and
// acknowledges it with Wait. It panics if the underlying channel is closed or
// if an internal invariant is violated.
func (x *ChanPubSubPacked[C, V]) Send(value V) (sent int) {
	x.checkUsedFactory()
	x.checkBroken()
	if uint32(x.state.Load()>>32) == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.gate.Lock()
	gateHeld := true
	defer func() {
		if gateHeld {
			x.gate.Unlock()
		}
	}()

	ok := false
	defer func() {
		if !ok {
			x.markBroken()
		}
	}()

	x.checkBroken()
	state := x.state.Load()
	count, phase := unpackChanPubSubPackedState(state)
	if phase != chanPubSubPackedPhaseIdle {
		panic(chanPubSubPackedBroken)
	}
	if count == 0 {
		ok = true
		return 0
	}

	// gate excludes all positive registrations and idle-phase removals. The
	// state CAS is still used to make the transition explicit and auditable.
	initialCount := count
	if !x.state.CompareAndSwap(state, packChanPubSubPackedState(initialCount, chanPubSubPackedPhaseSending)) {
		panic(chanPubSubPackedBroken)
	}

	// The initial count is the exact number of rendezvous this send must
	// complete. Negative Add calls may reduce the live count while consuming
	// one rendezvous apiece, but they do not change this fixed send budget.
	for range initialCount {
		x.c <- value
	}

	// Close the removal window before releasing gate. A negative Add that
	// observes finalizing waits for gate and then performs an ordinary count
	// decrement; it must not attempt a receive when no send remains.
	for {
		state = x.state.Load()
		count, phase = unpackChanPubSubPackedState(state)
		if phase != chanPubSubPackedPhaseSending || count > initialCount {
			panic(chanPubSubPackedBroken)
		}
		if x.state.CompareAndSwap(state, packChanPubSubPackedState(count, chanPubSubPackedPhaseFinalizing)) {
			break
		}
	}
	sent = int(count)
	if !x.state.CompareAndSwap(
		packChanPubSubPackedState(count, chanPubSubPackedPhaseFinalizing),
		packChanPubSubPackedState(count, chanPubSubPackedPhaseIdle),
	) {
		panic(chanPubSubPackedBroken)
	}

	x.gate.Unlock()
	gateHeld = false

	// Every remaining subscriber received a value and owes exactly one Wait.
	if sent != 0 {
		x.waitForPongs(sent)
	}

	ok = true
	return sent
}

func (x *ChanPubSubPacked[C, V]) waitForPongs(sent int) {
	x.pongMu.Lock()
	defer x.pongMu.Unlock()
	x.pongN = sent
	x.pongCond.Broadcast()
	for x.pongN != 0 {
		x.pongCond.Wait()
		x.checkBroken()
	}
}

// Add adjusts the number of registered subscribers and returns the new
// count. A delta of zero inspects and validates the current count.
//
// Positive deltas are serialized against a broadcast's send phase. Negative
// deltas that win a CAS while the phase is sending also receive delta values
// from C, replacing the receives of the subscribers being removed. The valid
// count range is [0, math.MaxInt32]. Invalid deltas or underflow panic.
func (x *ChanPubSubPacked[C, V]) Add(delta int) int {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic("bigbuff: chanpubsubpacked: delta out of bounds")
	}
	x.checkUsedFactory()
	x.checkBroken()

	if delta == 0 {
		count, _ := unpackChanPubSubPackedState(x.state.Load())
		return int(count)
	}

	if delta > 0 {
		x.gate.RLock()
		defer x.gate.RUnlock()
		x.checkBroken()
		increment := uint32(delta)
		for {
			state := x.state.Load()
			count, phase := unpackChanPubSubPackedState(state)
			if phase != chanPubSubPackedPhaseIdle || count > chanPubSubPackedMaxSubscribers-increment {
				x.failInvariant("bigbuff: chanpubsubpacked: subscriber count overflow or phase violation")
			}
			next := count + increment
			if x.state.CompareAndSwap(state, packChanPubSubPackedState(next, phase)) {
				return int(next)
			}
		}
	}

	decrement := uint32(-delta)
	for {
		x.checkBroken()
		state := x.state.Load()
		count, phase := unpackChanPubSubPackedState(state)
		if count < decrement {
			x.failInvariant("bigbuff: chanpubsubpacked: negative subscribers")
		}

		switch phase {
		case chanPubSubPackedPhaseSending:
			// Claim the removal while the send phase is still open. A
			// successful CAS commits this caller to consuming decrement sends.
			if x.state.CompareAndSwap(
				state,
				packChanPubSubPackedState(count-decrement, chanPubSubPackedPhaseSending),
			) {
				for range decrement {
					<-x.c
				}
				return int(count - decrement)
			}

		case chanPubSubPackedPhaseIdle:
			// Do not block on RLock here. A sender may have acquired the
			// write lock but not yet published phaseSending. Blocking behind
			// that sender would strand a cancelled receiver while Send waits
			// for it. TryRLock makes the race explicit: success linearizes the
			// decrement before the sender's snapshot; failure retries until
			// the sender publishes sending, at which point this call becomes
			// a replacement receiver in the branch above.
			if !x.gate.TryRLock() {
				runtime.Gosched()
				continue
			}
			for {
				state = x.state.Load()
				count, phase = unpackChanPubSubPackedState(state)
				if phase != chanPubSubPackedPhaseIdle {
					x.gate.RUnlock()
					x.failInvariant(chanPubSubPackedBroken)
				}
				if count < decrement {
					x.gate.RUnlock()
					x.failInvariant("bigbuff: chanpubsubpacked: negative subscribers")
				}
				if x.state.CompareAndSwap(state, packChanPubSubPackedState(count-decrement, phase)) {
					x.gate.RUnlock()
					return int(count - decrement)
				}
			}

		case chanPubSubPackedPhaseFinalizing:
			// All current rendezvous have completed. The sender is only
			// closing the removal window, so it is now safe to wait for gate
			// and perform a count-only decrement.
			x.gate.RLock()
			for {
				state = x.state.Load()
				count, phase = unpackChanPubSubPackedState(state)
				if phase != chanPubSubPackedPhaseIdle {
					x.gate.RUnlock()
					x.failInvariant(chanPubSubPackedBroken)
				}
				if count < decrement {
					x.gate.RUnlock()
					x.failInvariant("bigbuff: chanpubsubpacked: negative subscribers")
				}
				if x.state.CompareAndSwap(state, packChanPubSubPackedState(count-decrement, phase)) {
					x.gate.RUnlock()
					return int(count - decrement)
				}
			}

		default:
			x.failInvariant(chanPubSubPackedBroken)
		}
	}
}

// Wait acknowledges one value previously received from C. It is mandatory
// after every successful receive and must not be called otherwise.
func (x *ChanPubSubPacked[C, V]) Wait() {
	x.checkUsedFactory()
	x.pongMu.Lock()
	defer x.pongMu.Unlock()
	x.checkBroken()

	for x.pongN == 0 {
		x.pongCond.Wait()
		x.checkBroken()
	}
	x.pongN--
	if x.pongN == 0 {
		x.pongCond.Broadcast()
	}
}

// Subscribe is an alias for Add(1).
func (x *ChanPubSubPacked[C, V]) Subscribe() { x.Add(1) }

// Unsubscribe is an alias for Add(-1).
func (x *ChanPubSubPacked[C, V]) Unsubscribe() { x.Add(-1) }

func packChanPubSubPackedState(count, phase uint32) uint64 {
	return uint64(count)<<32 | uint64(phase)
}

func unpackChanPubSubPackedState(state uint64) (count, phase uint32) {
	count = uint32(state >> 32)
	phase = uint32(state)
	if count > chanPubSubPackedMaxSubscribers || phase > chanPubSubPackedPhaseFinalizing {
		panic(chanPubSubPackedBroken)
	}
	return count, phase
}

func (x *ChanPubSubPacked[C, V]) checkUsedFactory() {
	if x.broken == nil {
		panic("bigbuff: chanpubsubpacked: must use factory function")
	}
}

func (x *ChanPubSubPacked[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubPackedBroken)
	default:
	}
}

func (x *ChanPubSubPacked[C, V]) markBroken() {
	x.brokenOnce.Do(func() {
		x.pongMu.Lock()
		close(x.broken)
		x.pongCond.Broadcast()
		x.pongMu.Unlock()
	})
}

func (x *ChanPubSubPacked[C, V]) failInvariant(message string) {
	x.markBroken()
	panic(message)
}
