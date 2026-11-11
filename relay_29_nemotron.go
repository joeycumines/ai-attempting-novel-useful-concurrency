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

// Provenance: response 29.
//
// Repairs made in the completion phase (regression tests:
// relay_29_nemotron_test.go):
//
//  1. WaitNext waited on the doorbell below, which holds one token: one
//     Publish woke one parked link and the others slept until the next
//     Publish (or forever). Links now park on a channel that Publish closes
//     (the wake field); the doorbell is kept for Notify, where one coalesced
//     token is what the response documents.
//  2. A slot is never overwritten by an older sequence than the one it
//     holds. Two publishers a full lap apart could otherwise leave the older
//     value in the slot.
//
// Costs, recorded by the tests. Publish allocates once per call: the value
// is boxed for the slot's atomic pointer, which the first integration
// introduced so that readers do not race a slot rewrite; the response claims
// an allocation-free Publish. Repair 1 adds one channel allocation per
// WaitNext that actually parks, and nothing to Publish beyond closing that
// channel. Next allocates nothing.

import (
	"context"
	"iter"
	"sync/atomic"
)

// relay29NemotronLockBit is the high bit of a slot's sequence word.  While set, the slot is
// mid-write and readers must treat it as unavailable.
const relay29NemotronLockBit uint64 = 1 << 63

// Relay is a lock-free broadcast relay.
//
// Publish appends a value to a circular buffer and returns a monotonically
// increasing sequence.  Any number of Link subscribers can read those values
// independently, at their own pace, without ever being registered with the
// Relay.
//
// Because there is no subscriber registry, Publish never touches per-subscriber
// state: its cost is one atomic increment, one slot write, and one coalesced
// doorbell signal.  Subscribe/Unsubscribe are therefore free operations with
// no global churn cost.
//
// Slow subscribers are naturally conflated.  If a ring slot is reused before a
// link has consumed the previous value stored there, that link simply observes
// a jump in sequence numbers; Lag reports how far behind it is.
type Relay29Nemotron[V any] struct {
	mask  uint64
	slots []relay29NemotronSlot[V]
	// pubSeq is the most recently published sequence (1-based).  Zero means
	// nothing has been published yet.
	pubSeq atomic.Uint64
	// bell is a capacity-1 doorbell.  Publish performs a non-blocking send,
	// so at most one wakeup is pending at a time and concurrent publishes
	// coalesce into a single notification.
	bell chan struct{}

	// wake is the broadcast channel WaitNext parks on: nil while no link is
	// parked, otherwise a channel that the next Publish closes. See WaitNext.
	wake atomic.Pointer[chan struct{}]
}

type relay29NemotronSlot[V any] struct {
	// seqLock combines the write-in-progress flag (high bit) with the
	// message sequence (remaining bits).  Even values mean the slot is
	// stable and readable.
	seqLock atomic.Uint64
	// val is published through an atomic pointer so that a reader snapshotting
	// the payload cannot race a concurrent publisher rewriting the same slot.
	// The seqlock validation below is unchanged: the pointer load is simply the
	// memory-model-correct way to take the snapshot.
	val atomic.Pointer[V]
}

// NewRelay allocates a Relay whose ring buffer holds at least bufferSize
// slots.  The size is rounded up to a power of two so index calculation uses
// a bitmask instead of division.
func NewRelay29Nemotron[V any](bufferSize int) *Relay29Nemotron[V] {
	if bufferSize < 1 {
		panic("bigbuff: relay: buffer size must be at least 1")
	}
	size := uint64(1)
	for size < uint64(bufferSize) {
		size <<= 1
	}
	return &Relay29Nemotron[V]{
		mask:  size - 1,
		slots: make([]relay29NemotronSlot[V], size),
		bell:  make(chan struct{}, 1),
	}
}

// Publish stores value in the next ring-buffer slot and returns its sequence
// number.  The call is wait-free, allocates nothing, and its cost does not
// depend on the number of subscribers.
//
// A publish acquires the slot's seqlock, writes the payload, then releases the
// sequence.  Any subscriber that observes the new sequence is guaranteed to
// observe the corresponding value (provided V is not mutated after Publish).
//
// If the ring buffer wraps and overwrites a slot that a slow subscriber has
// not yet consumed, that subscriber's value is silently dropped; see Link.Lag.
func (r *Relay29Nemotron[V]) Publish(value V) uint64 {
	seq := r.pubSeq.Add(1) // 1-based
	if seq >= relay29NemotronLockBit {
		panic("bigbuff: relay: sequence overflow")
	}

	r.store(seq, value)
	r.ring()
	r.wakeAll()
	return seq
}

// store writes one reserved sequence into its slot.
func (r *Relay29Nemotron[V]) store(seq uint64, value V) {
	idx := (seq - 1) & r.mask
	slot := &r.slots[idx]

	// Acquire the write lock.  Because the critical section is tiny (two
	// stores), a brief spin is cheaper than a kernel-level park/unpark.
	var old uint64
	for {
		old = slot.seqLock.Load()
		if old&relay29NemotronLockBit == 0 && slot.seqLock.CompareAndSwap(old, old|relay29NemotronLockBit) {
			break
		}
	}

	// Integration repair: a slot never goes back to an older sequence.
	// Sequences are reserved before the slot is locked, so with concurrent
	// publishers a later sequence that maps to the same slot (one or more
	// laps ahead) can be written first. As delivered, the earlier sequence
	// then overwrote it; a link waiting for the later one found the slot
	// "not yet published" and stalled until some further Publish. The older
	// value is one a full lap has already superseded, so it is dropped, as
	// it would have been had the two writes happened in order.
	if old >= seq {
		slot.seqLock.Store(old)
		return
	}

	// Payload first, then release the sequence with the write lock cleared.
	slot.val.Store(&value)
	slot.seqLock.Store(seq)
}

// wakeAll releases every link parked in WaitNext. It costs one atomic load
// when no link is parked.
func (r *Relay29Nemotron[V]) wakeAll() {
	if r.wake.Load() == nil {
		return
	}
	if ch := r.wake.Swap(nil); ch != nil {
		close(*ch)
	}
}

// parked returns the channel the next Publish will close, installing one if
// no link is parked yet.
func (r *Relay29Nemotron[V]) parked() <-chan struct{} {
	for {
		if ch := r.wake.Load(); ch != nil {
			return *ch
		}
		ch := make(chan struct{})
		if r.wake.CompareAndSwap(nil, &ch) {
			return ch
		}
	}
}

// ring performs a non-blocking send on the shared doorbell.  Multiple rapid
// publishes collapse into a single pending wakeup.
func (r *Relay29Nemotron[V]) ring() {
	select {
	case r.bell <- struct{}{}:
	default:
	}
}

// Notify returns a channel that is signalled whenever Publish is called.
// The signal is a single coalesced token: it wakes exactly one receiver, so
// Notify serves one selecting goroutine per Relay. Any number of links may
// block in WaitNext (or Iter) instead, which does not use this channel.
// It is safe to use directly in a select statement:
//
//	select {
//	case <-relay.Notify():
//	    // drain link with Next()
//	case <-ctx.Done():
//	    return
//	}
func (r *Relay29Nemotron[V]) Notify() <-chan struct{} {
	return r.bell
}

// Seq returns the most recently published sequence, or 0 if nothing has been
// published yet.
func (r *Relay29Nemotron[V]) Seq() uint64 {
	return r.pubSeq.Load()
}

// Link is an independent read cursor into a Relay.
//
// Links are not registered with the Relay.  Creating or discarding a Link has
// no effect on any other subscriber and costs exactly one allocation.  A Link
// begins consuming at sequence 1 (Subscribe: whatever the ring still holds of
// the current lap, then everything after), at the next unpublished sequence
// (SubscribeLatest), or at an arbitrary sequence (SubscribeFrom). The
// delivered comment said Subscribe starts at the next unpublished sequence;
// the delivered code is Subscribe == SubscribeFrom(0), which is kept.
type Relay29NemotronLink[V any] struct {
	relay *Relay29Nemotron[V]
	seq   atomic.Uint64 // last consumed sequence (0-based cursor)
}

func (r *Relay29Nemotron[V]) Subscribe() *Relay29NemotronLink[V] {
	return &Relay29NemotronLink[V]{relay: r}
}

// SubscribeLatest returns a Link whose first Next will observe the first
// value published after the call (or the most recent value already in the
// buffer).
func (r *Relay29Nemotron[V]) SubscribeLatest() *Relay29NemotronLink[V] {
	l := &Relay29NemotronLink[V]{relay: r}
	l.seq.Store(r.Seq())
	return l
}

// SubscribeFrom returns a Link that attempts to consume starting at seq.
// If seq is 0, the first Next attempts sequence 1.
func (r *Relay29Nemotron[V]) SubscribeFrom(seq uint64) *Relay29NemotronLink[V] {
	l := &Relay29NemotronLink[V]{relay: r}
	l.seq.Store(seq)
	return l
}

// Next attempts to consume the next available value for this link.
//
// It is non-blocking and allocates nothing.  If the target sequence has not
// been published yet, ok is false.  If the slot has already been recycled
// and holds a newer sequence, that newer value is returned and the cursor
// jumps forward (conflation).
//
// Callers typically loop Next until it returns false, then wait on
// Relay.Notify or a context before trying again.
func (l *Relay29NemotronLink[V]) Next() (V, bool) {
	nextSeq := l.seq.Load() + 1
	idx := (nextSeq - 1) & l.relay.mask
	slot := &l.relay.slots[idx]

	// First observation: the slot must not be mid-write.
	old := slot.seqLock.Load()
	if old&relay29NemotronLockBit != 0 {
		var zero V
		return zero, false
	}

	slotSeq := old
	if slotSeq < nextSeq {
		var zero V
		return zero, false
	}

	// Snapshot the payload, then verify the slot was not modified during
	// the read.  This seqlock validation makes Next race-free without any
	// per-slot mutex or per-publish allocation.
	vp := slot.val.Load()
	if vp == nil || slot.seqLock.Load() != old {
		var zero V
		return zero, false
	}

	l.seq.Store(slotSeq)
	return *vp, true
}

// Seq returns the last sequence consumed by this link.
func (l *Relay29NemotronLink[V]) Seq() uint64 {
	return l.seq.Load()
}

// Lag returns the number of published values this link has not yet consumed.
// A lag greater than or equal to the ring buffer size means values have been
// dropped for this subscriber.
func (l *Relay29NemotronLink[V]) Lag() uint64 {
	return l.relay.pubSeq.Load() - l.seq.Load()
}

// WaitNext blocks until a value is available for this link or ctx is
// cancelled.
//
// Integration repair. As delivered, WaitNext received from the shared
// doorbell (Relay.Notify). The doorbell holds one token, so one Publish woke
// one waiting link; every other link blocked in WaitNext slept through the
// publication, indefinitely if it was the last. WaitNext now parks on a
// channel that Publish closes, which wakes every parked link. The channel is
// created by the first link to park (never by Publish) and replaced only
// after a Publish has closed it, so the relay still keeps no subscriber
// registry and Publish still allocates nothing for the wakeup.
func (l *Relay29NemotronLink[V]) WaitNext(ctx context.Context) (V, bool) {
	for {
		if v, ok := l.Next(); ok {
			return v, true
		}
		wake := l.relay.parked()
		// Re-check now that a Publish is bound to close wake: one that
		// finished before parked() installed the channel left nothing to
		// close, but its value is visible to this Next.
		if v, ok := l.Next(); ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			var zero V
			return zero, false
		case <-wake:
		}
	}
}

// Iter adapts this link into a standard Go 1.22 pull iterator over the
// relay's values.  The iterator blocks until ctx is cancelled, then returns.
//
// Usage:
//
//	for v := range link.Iter(context.Background()) {
//	    // consume v
//	}
func (l *Relay29NemotronLink[V]) Iter(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		for {
			v, ok := l.WaitNext(ctx)
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
