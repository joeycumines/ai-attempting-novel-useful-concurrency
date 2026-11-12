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

// Provenance: 10-gemini-argon-medium.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

// Repair (run 09 pass). The delivered source is a bare seqlock; this port put
// the slot's value behind a sync.RWMutex so that the race detector can follow
// it. The publisher marks the slot odd and then takes that lock, which waits
// for any reader that is inside it. A reader that found the slot odd retried
// at once, without yielding, so with more readers than CPUs the retrying
// readers held every CPU while the one reader the publisher was waiting for
// sat in the run queue, until the scheduler preempted somebody (about 10 ms).
// Measurement run 08 shows it: 97 ns a publish with one subscriber and
// 596,687 ns with 256. TryNext now yields when it finds a write in progress.

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// epochTapeClosedChan is a shared, permanently closed channel returned by
// [EpochSub.Ready] whenever unread messages are already available in the tape.
// Receiving from a closed channel inside a Go `select` statement resolves
// immediately in O(1) without acquiring any runtime hchan lock or allocating.
var epochTapeClosedChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// epochTapeSlot is a cache-line padded seqlock slot in an [EpochTape] ring buffer.
type epochTapeSlot[V any] struct {
	// seq tracks the monotonic state of this slot:
	//   - Odd:  write in progress
	//   - Even: committed sequence number (seq == round * 2)
	seq atomic.Uint64
	mu  sync.RWMutex
	val V
}

// EpochTape is a high-throughput, select-compatible, lock-free-read broadcast
// ring ("tape") designed as the asynchronous/bounded-lag counterpart to
// [ChanCaster] and [ChanPubSub].
//
// # Key Characteristics & Novelty
//
//  1. O(1) Publisher Write (Zero Per-Subscriber Channel Sends):
//     Unlike [ChanCaster], which performs N synchronous channel transfers on a
//     single hchan lock, [EpochTape.Publish] writes the payload V ONCE into a
//     seqlock ring slot and wakes parked subscribers by closing a shared epoch
//     signal channel (leveraging Go runtime's sudog broadcast on channel close).
//  2. Lazy Signal Arming (Zero Allocations on Hot Path):
//     A new signal channel is only allocated and closed if at least one
//     subscriber actually parked on [EpochSub.Ready] because the tape was caught
//     up. When subscribers are actively draining, Publish and Next perform
//     0 allocations.
//  3. Native `select` Integration via Pre-Closed Channel Fast Path:
//     [EpochSub.Ready] returns a `<-chan struct{}` usable in any `select`
//     statement. When unread items exist, Ready() returns a package-global
//     pre-closed channel so `select` fires immediately with zero lock contention.
//  4. Infinite Subscriber Churn Scalability:
//     Subscribing and unsubscribing require zero locks and zero coordination
//     with active publishers—simply snapshotting the atomic write cursor.
type EpochTape[V any] struct {
	ring []epochTapeSlot[V]
	mask uint64

	// writeMu serialises concurrent Publish/Close calls. Subscribers never
	// touch writeMu.
	writeMu sync.Mutex

	// published is the highest committed 1-based message sequence number.
	// Upper bit (1 << 63) is set when the tape is closed.
	published atomic.Uint64

	// sig holds the current open broadcast wake channel.
	sig atomic.Pointer[chan struct{}]

	// sigArmed is set to true whenever at least one subscriber loads `sig` to
	// wait for a future publish. Publish only rotates and closes `sig` when
	// sigArmed is true, eliminating channel allocations when no goroutine is
	// parked waiting for data.
	sigArmed atomic.Bool
}

const epochClosedBit uint64 = 1 << 63

// NewEpochTape constructs a new [EpochTape] with the given ring capacity,
// rounded up to the next power of two (minimum 2).
func NewEpochTape[V any](capacity int) *EpochTape[V] {
	if capacity < 2 {
		capacity = 2
	}
	size := uint64(1)
	for size < uint64(capacity) {
		size <<= 1
	}

	ch := make(chan struct{})
	t := &EpochTape[V]{
		ring: make([]epochTapeSlot[V], size),
		mask: size - 1,
	}
	t.sig.Store(&ch)
	return t
}

// Publish writes value to the tape in O(1) time and wakes all currently parked
// subscribers. It returns the 1-based monotonic sequence number assigned to the
// message, or 0 if the tape has been closed.
func (t *EpochTape[V]) Publish(value V) uint64 {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	cur := t.published.Load()
	if cur&epochClosedBit != 0 {
		return 0
	}

	nextSeq := cur + 1
	slot := &t.ring[nextSeq&t.mask]

	// Seqlock begin: set odd sequence number (nextSeq*2 - 1) to invalidate
	// concurrent optimistic readers inspecting this slot.
	slot.seq.Store(nextSeq*2 - 1)
	slot.mu.Lock()
	slot.val = value
	slot.mu.Unlock()
	// Seqlock commit: set even sequence number (nextSeq*2).
	slot.seq.Store(nextSeq * 2)

	t.published.Store(nextSeq)

	// Only allocate a fresh signal channel and close the current one if at
	// least one subscriber armed the signal channel since the last publish.
	if t.sigArmed.Load() && t.sigArmed.Swap(false) {
		fresh := make(chan struct{})
		old := t.sig.Swap(&fresh)
		close(*old)
	}

	return nextSeq
}

// Close marks the tape as closed and permanently wakes all current and future
// subscribers. Subsequent calls to Publish return 0.
func (t *EpochTape[V]) Close() {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	cur := t.published.Load()
	if cur&epochClosedBit != 0 {
		return
	}
	t.published.Store(cur | epochClosedBit)

	// Replace sig with a permanently closed channel so any future Ready() calls
	// resolve immediately.
	closed := make(chan struct{})
	close(closed)
	old := t.sig.Swap(&closed)
	select {
	case <-*old:
		// already closed
	default:
		close(*old)
	}
}

// EpochSub represents an independent, zero-allocation subscriber cursor over
// an [EpochTape]. An EpochSub is used by a single consumer goroutine and does
// not need to be explicitly unsubscribed upon exit—abandoning it is O(1) and
// leak-free.
type EpochSub[V any] struct {
	tape *EpochTape[V]
	next uint64
}

// Subscribe creates a new value-type [EpochSub] positioned to receive all
// messages published after this call. Because EpochSub is a tiny two-word
// struct, callers may keep it on the stack with zero heap allocations.
func (t *EpochTape[V]) Subscribe() EpochSub[V] {
	cur := t.published.Load() &^ epochClosedBit
	return EpochSub[V]{
		tape: t,
		next: cur + 1,
	}
}

// Ready returns a receive-only channel for use in a `select` statement.
//
//   - If an unread message is already available (or the tape is closed), Ready
//     returns a pre-closed channel that fires immediately with zero allocations.
//   - Otherwise, Ready arms the current epoch signal channel, which will be
//     closed by the next [EpochTape.Publish] or [EpochTape.Close] call.
func (s *EpochSub[V]) Ready() <-chan struct{} {
	pub := s.tape.published.Load()
	if (pub&^epochClosedBit) >= s.next || (pub&epochClosedBit) != 0 {
		return epochTapeClosedChan
	}

	// Load the current signal channel BEFORE arming and re-checking published
	// to prevent missed wakeups.
	chPtr := s.tape.sig.Load()
	s.tape.sigArmed.Store(true)

	pub = s.tape.published.Load()
	if (pub&^epochClosedBit) >= s.next || (pub&epochClosedBit) != 0 {
		return epochTapeClosedChan
	}
	return *chPtr
}

// TryNext attempts to read the next available message without blocking.
//
// Returns:
//   - value: the message read (or zero value if ok == false).
//   - missed: the number of messages overwritten before this subscriber could
//     read them (0 if the subscriber kept up with the ring buffer capacity).
//   - closed: true if the tape is closed and all published messages have been
//     consumed.
//   - ok: true if a valid message was read.
func (s *EpochSub[V]) TryNext() (value V, missed uint64, closed bool, ok bool) {
	var totalMissed uint64
	for {
		pubRaw := s.tape.published.Load()
		pub := pubRaw &^ epochClosedBit
		isClosed := (pubRaw & epochClosedBit) != 0

		if s.next > pub {
			return value, totalMissed, isClosed, false
		}

		capLen := uint64(len(s.tape.ring))
		if pub >= capLen && s.next < pub-capLen+1 {
			oldest := pub - capLen + 1
			totalMissed += oldest - s.next
			s.next = oldest
		}

		slot := &s.tape.ring[s.next&s.tape.mask]
		seq1 := slot.seq.Load()
		expectedSeq := s.next * 2
		if seq1 != expectedSeq {
			// Slot was overwritten concurrently while we were reading; retry and
			// advance s.next to the new oldest available sequence.
			if seq1&1 == 1 {
				// A write is in progress, and the publisher may be waiting
				// for a reader that is not running. Let it run (see the
				// repair note at the top of the file).
				runtime.Gosched()
			}
			pubRaw = s.tape.published.Load()
			pub = pubRaw &^ epochClosedBit
			if pub >= capLen && s.next < pub-capLen+1 {
				oldest := pub - capLen + 1
				totalMissed += oldest - s.next
				s.next = oldest
			}
			continue
		}

		slot.mu.RLock()
		val := slot.val
		slot.mu.RUnlock()
		seq2 := slot.seq.Load()
		if seq1 != seq2 {
			// Seqlock validation failed (publisher overwrote slot mid-read); retry.
			continue
		}

		s.next++
		return val, totalMissed, false, true
	}
}

// SubscribeContext returns an [iter.Seq2] yielding `(value V, missed uint64)`
// for each broadcast message until the context is cancelled or the tape is closed.
func (t *EpochTape[V]) SubscribeContext(ctx context.Context) iter.Seq2[V, uint64] {
	if ctx == nil {
		ctx = context.Background()
	}
	sub := t.Subscribe()
	return func(yield func(V, uint64) bool) {
		for {
			if val, missed, closed, ok := sub.TryNext(); ok {
				if !yield(val, missed) {
					return
				}
				continue
			} else if closed {
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
