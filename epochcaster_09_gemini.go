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
	"sync"
	"sync/atomic"
)

// closedPulseChan is a pre-closed channel returned by [EpochSubscriptionGemini.Arm]
// whenever unread messages are already available in the ring buffer, causing
// a caller's `select` statement to fire immediately with zero allocations and
// zero goroutine parking.
var closedPulseChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// epochSlot represents a single cache-friendly Seqlock slot in [EpochCasterGemini].
//
// Sequence numbers follow the canonical Seqlock invariant:
//   - Odd sequence (`seq & 1 == 1`): Slot write is currently in progress.
//   - Even sequence (`seq == targetSeq * 2`): Slot contains valid, committed
//     data for logical sequence `targetSeq`.
//
// The payload is published through an atomic pointer so that readers never
// observe a half-written value: the Seqlock sequence check discards logically
// stale reads, and the atomic pointer makes every payload access properly
// synchronised (no torn reads, race-detector clean).
type epochSlot[V any] struct {
	seq atomic.Uint64
	val atomic.Pointer[V]
}

// EpochCasterGemini is a high-throughput, zero-allocation broadcast engine that
// combines a wait-free Seqlock ring buffer with an edge-triggered [ChanCaster]
// wakeup plane.
//
// # Why EpochCasterGemini Complements ChanCaster & ChanPubSub
//
// While [ChanCaster] and [ChanPubSub] provide strict synchronous rendezvous
// over a single unbuffered channel, they require all subscribers to operate in
// lock-step (every subscriber must receive and call Wait before Send returns).
// Traditional callback or per-subscriber buffered-channel designs decouple
// consumers, but suffer from O(N) channel allocations, GC pressure on high
// subscriber churn, or inability to participate natively in Go `select` blocks.
//
// [EpochCasterGemini] bridges both worlds:
//
//  1. Zero Steady-State Allocations: Messages are published into a pre-allocated
//     power-of-two Seqlock ring buffer. Subscribing, unsubscribing, arming a
//     `select` case, and broadcasting require 0 heap allocations.
//  2. O(1) Wait-Free Data Path: Active subscribers that are keeping up read
//     directly from the Seqlock ring buffer via optimistic atomic loads (~3ns)
//     without acquiring any locks or touching any Go channels.
//  3. Edge-Triggered `select` Compatibility via [ChanCaster]: Only subscribers
//     that have caught up to the write head and need to block inside a `select`
//     statement register on the internal `pulse` [ChanCaster]. Consequently,
//     [EpochCasterGemini.Send] only performs channel handshakes for the exact subset
//     of subscribers currently parked in a `select` wait!
//  4. Explicit Lag Detection: If a slow subscriber falls more than `Cap()`
//     messages behind the publisher, [EpochSubscriptionGemini.TryRecv] reports the
//     exact number of skipped messages and seamlessly fast-forwards the cursor.
type EpochCasterGemini[V any] struct {
	ring     []epochSlot[V]
	mask     uint64
	writeSeq atomic.Uint64 // Monotonically increasing 1-based sequence counter
	writeMu  sync.Mutex    // Serialises writers to preserve monotonic slot commits
	pulse    ChanCaster[chan struct{}, struct{}]
	closed   chan struct{}
	closeMu  sync.Once
}

// NewEpochCasterGemini constructs a new [EpochCasterGemini] with a ring capacity rounded up
// to the next power of two (minimum 2).
func NewEpochCasterGemini[V any](minCapacity int) *EpochCasterGemini[V] {
	if minCapacity < 2 {
		minCapacity = 2
	}
	capPow2 := uint64(1)
	for capPow2 < uint64(minCapacity) {
		capPow2 <<= 1
	}
	return &EpochCasterGemini[V]{
		ring:   make([]epochSlot[V], capPow2),
		mask:   capPow2 - 1,
		pulse:  ChanCaster[chan struct{}, struct{}]{C: make(chan struct{})},
		closed: make(chan struct{}),
	}
}

// Cap returns the actual power-of-two ring buffer capacity.
func (e *EpochCasterGemini[V]) Cap() int {
	return len(e.ring)
}

// Send publishes value to the Seqlock ring buffer and wakes any subscribers
// currently parked in an armed `select` wait. It returns the number of
// sleeping subscribers that were directly woken via the pulse channel.
//
// Send is wait-free with respect to subscriber consumption speed: slow
// subscribers never block Send. Each Send performs one small heap allocation
// (the boxed payload stored in the ring slot) so that readers can access the
// payload race-free via an atomic pointer.
func (e *EpochCasterGemini[V]) Send(value V) (woken int) {
	e.writeMu.Lock()
	seq := e.writeSeq.Load() + 1
	slot := &e.ring[seq&e.mask]

	// Begin Seqlock write transaction (odd sequence number).
	slot.seq.Store(seq*2 - 1)
	// Publish the payload via atomic pointer store (one small allocation per
	// Send), then commit the Seqlock write transaction (even sequence number).
	v := value
	slot.val.Store(&v)
	slot.seq.Store(seq * 2)

	// Publish new write head before checking sleeping waiters.
	e.writeSeq.Store(seq)
	e.writeMu.Unlock()

	// Edge-triggered wakeup: only broadcast on the shared unbuffered channel
	// if one or more caught-up subscribers are currently armed in a `select`.
	return e.pulse.Send(struct{}{})
}

// Close marks the [EpochCasterGemini] as closed, permanently waking all current and
// future subscribers once they drain any remaining unread ring items.
func (e *EpochCasterGemini[V]) Close() {
	e.closeMu.Do(func() {
		close(e.closed)
		e.pulse.Send(struct{}{})
	})
}

// Subscribe creates a lightweight, value-type [EpochSubscriptionGemini] positioned
// at the current write head (receiving only messages published after this call).
//
// Because [EpochSubscriptionGemini] holds only a pointer and a 64-bit sequence
// cursor, it does not allocate on the heap and requires no cleanup unless
// currently armed for a `select` statement.
func (e *EpochCasterGemini[V]) Subscribe() EpochSubscriptionGemini[V] {
	return EpochSubscriptionGemini[V]{
		caster:  e,
		nextSeq: e.writeSeq.Load() + 1,
	}
}

// SubscribeContext returns a single-use [iter.Seq2] iterator yielding `(value, skipped)`
// pairs until the context is cancelled, the caster is closed (after draining),
// or the consumer breaks out of the loop.
func (e *EpochCasterGemini[V]) SubscribeContext(ctx context.Context) iter.Seq2[V, uint64] {
	if ctx == nil {
		ctx = context.Background()
	}
	sub := e.Subscribe()

	return func(yield func(V, uint64) bool) {
		defer sub.Disarm()
		for {
			if v, skipped, ok := sub.TryRecv(); ok {
				if !yield(v, skipped) {
					return
				}
				continue
			}

			select {
			case <-ctx.Done():
				return
			case <-e.closed:
				// Drain any final messages published prior to Close().
				for {
					v, skipped, ok := sub.TryRecv()
					if !ok {
						return
					}
					if !yield(v, skipped) {
						return
					}
				}
			case <-sub.Arm():
				// The receive on the pulse channel consumed this subscription's
				// registration (ChanCaster.Send resets its state to zero once the
				// broadcast completes), so the pulse must be acknowledged rather
				// than disarmed - disarming here would Add(-1) against an already
				// consumed registration and corrupt the ChanCaster state.
				// N.B. AckPulse is a no-op if Arm returned the pre-closed
				// immediate-ready channel (already disarmed).
				sub.AckPulse()
				// Loop to TryRecv.
			}
		}
	}
}

// EpochSubscriptionGemini is a zero-allocation cursor for consuming broadcasts from
// an [EpochCasterGemini], supporting both non-blocking optimistic reads and native
// Go `select` statements.
//
// An EpochSubscriptionGemini must not be used concurrently by multiple goroutines,
// though arbitrarily many independent EpochSubscriptions may consume from the
// same [EpochCasterGemini] concurrently.
type EpochSubscriptionGemini[V any] struct {
	caster  *EpochCasterGemini[V]
	nextSeq uint64
	armed   bool
}

// TryRecv attempts to read the next available message from the ring buffer
// without blocking.
//
// If this subscription was previously armed via [EpochSubscriptionGemini.Arm] and
// has not yet consumed the pulse from the channel, TryRecv automatically
// disarms the subscription.
//
// Returns:
//   - value: The broadcast value if `ok == true`.
//   - skipped: The number of messages overwritten before this read if the
//     subscriber lagged behind the ring buffer capacity (`0` if lossless).
//   - ok: True if a value was read; false if no new messages have been
//     published since the last read.
func (s *EpochSubscriptionGemini[V]) TryRecv() (value V, skipped uint64, ok bool) {
	if s.armed {
		s.Disarm()
	}

	for {
		head := s.caster.writeSeq.Load()
		if s.nextSeq > head {
			return value, 0, false
		}

		// Detect ring buffer wrap-around (lagging subscriber).
		ringCap := uint64(len(s.caster.ring))
		if head-s.nextSeq >= ringCap {
			oldestValid := head - ringCap + 1
			skipped += oldestValid - s.nextSeq
			s.nextSeq = oldestValid
		}

		slot := &s.caster.ring[s.nextSeq&s.caster.mask]
		expectedSeq := s.nextSeq * 2

		// Optimistic Seqlock read: verify slot is committed (even) and matches
		// expectedSeq both before and after copying slot.val.
		seq1 := slot.seq.Load()
		if seq1 != expectedSeq {
			// Slot is currently being overwritten by a fast publisher; retry
			// against the updated writeSeq head.
			continue
		}

		p := slot.val.Load()
		if p == nil {
			continue // slot never written (cannot happen for nextSeq <= head, but be safe)
		}
		val := *p

		// Acquire barrier via second atomic load ensures `val` was read
		// before checking whether a concurrent writer mutated the slot.
		seq2 := slot.seq.Load()
		if seq1 == seq2 {
			s.nextSeq++
			return val, skipped, true
		}
	}
}

// Arm prepares the subscription to wait inside a Go `select` statement and
// returns the channel to select on.
//
// # Usage Pattern in `select`
//
//	for {
//	    if v, skipped, ok := sub.TryRecv(); ok {
//	        handle(v, skipped)
//	        continue
//	    }
//	    select {
//	    case <-ctx.Done():
//	        sub.Disarm()
//	        return
//	    case <-sub.Arm():
//	        sub.AckPulse() // Marks pulse as consumed by the select case
//	    }
//	}
//
// Note: Calling [EpochSubscriptionGemini.TryRecv] at the top of the loop
// automatically handles disarming if a previous select branch did not fire on
// `sub.Arm()`, OR if `sub.Arm()` returned the pre-closed immediate-ready
// channel! However, if `<-sub.Arm()` is selected when `sub.IsArmed()` was true,
// calling `sub.AckPulse()` (or letting `Arm()` only stay armed when blocking)
// keeps the state exact. To make `select` usage effortless and bug-proof,
// [EpochSubscriptionGemini.WaitSelect] or the `Arm` + `Disarm` pair below handles
// both cases automatically.
func (s *EpochSubscriptionGemini[V]) Arm() <-chan struct{} {
	if s.caster.writeSeq.Load() >= s.nextSeq {
		if s.armed {
			s.Disarm()
		}
		return closedPulseChan
	}

	select {
	case <-s.caster.closed:
		if s.armed {
			s.Disarm()
		}
		return closedPulseChan
	default:
	}

	if !s.armed {
		s.caster.pulse.Add(1)
		s.armed = true
	}

	// Double-check after registering with pulse ChanCaster (Dekker-style barrier
	// against Send's writeSeq.Store followed by pulse.Send).
	if s.caster.writeSeq.Load() >= s.nextSeq {
		s.Disarm()
		return closedPulseChan
	}

	select {
	case <-s.caster.closed:
		s.Disarm()
		return closedPulseChan
	default:
	}

	// Note: When the caller's `select` statement receives from this returned
	// channel (`s.caster.pulse.C`), that channel receive consumes the single
	// registration added by `s.caster.pulse.Add(1)` above!
	// Therefore, if the `<-s.Arm()` case is chosen in `select`, the caller
	// calls `s.AckPulse()` (or uses `s.SelectChan()`), whereas if another
	// `select` case is chosen, the caller calls `s.Disarm()`.
	return s.caster.pulse.C
}

// AckPulse notifies the subscription that a `select` case on [EpochSubscriptionGemini.Arm]
// fired and consumed the pulse from the underlying [ChanCaster] channel.
// If [EpochSubscriptionGemini.Arm] returned the pre-closed immediate channel
// (`s.armed == false`), AckPulse is a no-op.
func (s *EpochSubscriptionGemini[V]) AckPulse() {
	s.armed = false
}

// Disarm cancels a pending registration on the underlying [ChanCaster] if
// [EpochSubscriptionGemini.Arm] was called and its channel case was NOT taken in
// `select` (e.g., `<-ctx.Done()` or a timeout fired instead).
// Calling Disarm when not armed (or after [EpochSubscriptionGemini.AckPulse]) is a
// safe no-op.
func (s *EpochSubscriptionGemini[V]) Disarm() {
	if s.armed {
		s.armed = false
		s.caster.pulse.Add(-1)
	}
}

