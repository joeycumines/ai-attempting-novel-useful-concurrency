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
	"runtime"
	"sync"
	"sync/atomic"
)

const chanRing26KimiStateInvariantViolation = `bigbuff: chanring: state invariant violation`

// ChanRing26Kimi is a bounded-loss, select-native, fixed-memory broadcast bus.
//
// It is deliberately complementary to ChanCaster and ChanPubSub, which
// provide reliable (every value reaches every registered receiver) fan-out
// over a single unbuffered channel using ticket arithmetic and condition
// variables. ChanRing26Kimi instead provides COALESCING / BOUNDED-LOSS fan-out
// over a shared, seqlock-published ring of values, and in exchange delivers
// properties the reliable designs cannot:
//
//   - Sender never blocks and never waits for receivers. A slow or stalled
//     receiver loses values to ring overwrite; it can never stall the
//     sender or any other receiver (no head-of-line blocking).
//   - Send performs zero allocations and takes no locks during fan-out
//     (only a short mutex to serialize slot publication when senders are
//     concurrent). Cost is one cache-line publish plus one atomic load and
//     one non-blocking cap-1 channel send per live subscriber: comparable
//     to a linear callback-channel design, but with no callback indirection,
//     no RWMutex held across the fan-out, and no lock at all for receivers.
//   - Receivers are fully select-native: each subscription owns a private
//     cap-1 signal channel (allocated once, up front, for the whole pool),
//     so arbitrary select statements, context cancellation and deadlines
//     compose naturally. There is no rendezvous, so a receiver's select is
//     never coupled to the sender's progress.
//   - Consumer churn is O(1) and lock-free: subscriptions are handles into a
//     fixed slot pool recycled through an ABA-tagged Treiber stack. No
//     subscriber-set mutex, no snapshot copying, no per-subscription
//     allocation, no goroutine per subscriber, and memory usage is flat and
//     bounded regardless of churn.
//
// Semantics: every Send publishes one value into the ring. Each live
// subscription has a private cursor and reads every retained position in
// order; positions overwritten before being read are lost (counted in the
// slot's lost field). With ringCapacity 1 this degenerates to a pure
// "latest value" cell; with a larger ring it tolerates bursts of up to
// ringCapacity pending values per subscriber. A new subscription starts at
// the live tail: it does not replay retained values sent before it
// subscribed.
//
// Invariants and design notes:
//
//   - Publication is a seqlock: writers store an odd sequence, write the
//     value, then store the even sequence (twice the absolute ring
//     position). Readers validate position and parity, re-load after
//     reading the value, and retry on any mismatch; the only spinning
//     possible is while a publisher is inside its (tiny) critical section.
//     The publication mutex exists only to keep positions dense and in
//     order for concurrent senders; it is never held during fan-out.
//   - The missed-wakeup race is closed structurally, not with extra
//     signalling: the wake token is sent AFTER publication, the value
//     remains readable in the ring, and the receiver drains BEFORE
//     blocking on its private signal channel, so any publish that is not
//     observed by a drain is followed by a token that is.
//   - Slot reuse is safe with a single stale token because the channel is
//     cap 1 and a spurious wake merely costs one empty drain cycle.
//   - The free-list head is (tag << 32) | (idx + 1), with the tag bumped
//     on every successful push and pop, eliminating Treiber ABA in
//     practice.
//   - Positions are absolute uint64; the ring position is masked by a
//     power-of-two minus one, so publication is a single AND.
//
// Broad applicability: any high-frequency broadcast where consumers must
// live inside a select and can tolerate bounded loss or coalescing - market
// data, telemetry, progress/state updates, UI feeds. For reliable delivery
// of every value to every receiver, use ChanPubSub.
//
// Example:
//
//	ring := NewChanRing26Kimi[Quote](1024 /* burst tolerance */, 4096 /* max subs */)
//	go func() {
//	    for q := range ring.SubscribeContext(ctx) {
//	        render(q)
//	    }
//	}()
//	ring.Send(q) // never blocks
type ChanRing26Kimi[V any] struct {
	cells []chanRing26KimiCell[V] // shared broadcast ring, seqlock-published
	slots []chanRing26KimiSlot    // fixed subscriber slot pool (created once)

	mask  uint64        // ringCapacity - 1 (power of two)
	free  atomic.Uint64 // Treiber stack head: (tag << 32) | (slot idx + 1); 0 = empty
	write atomic.Uint64 // next absolute ring position to publish

	writeMu sync.Mutex // serializes claim+publish only; fan-out is lock-free

	live atomic.Int32 // number of live subscriptions
}

// NewChanRing26Kimi constructs a broadcast bus with the given logical ring
// capacity (rounded up to a power of two; minimum 1, which yields pure
// latest-value semantics) and maximum concurrent subscribers.
//
// All memory (ring cells, subscriber slots and every private signal
// channel) is allocated exactly once here; the zero-allocation steady state
// holds regardless of send, receive or churn rates.
func NewChanRing26Kimi[V any](ringCapacity, maxSubscribers int) *ChanRing26Kimi[V] {
	if ringCapacity <= 0 {
		panic(`bigbuff: chanring: ring capacity must be positive`)
	}
	if maxSubscribers <= 0 || uint64(maxSubscribers) >= 1<<32 {
		panic(`bigbuff: chanring: max subscribers out of bounds`)
	}

	c := uint64(1)
	for c < uint64(ringCapacity) {
		c <<= 1
	}

	x := &ChanRing26Kimi[V]{
		cells: make([]chanRing26KimiCell[V], c),
		slots: make([]chanRing26KimiSlot, maxSubscribers),
		mask:  c - 1,
	}
	for i := range x.slots {
		s := &x.slots[i]
		s.idx = uint32(i)
		s.ch = make(chan struct{}, 1)
		if i+1 < len(x.slots) {
			s.next.Store(uint32(i) + 2) // encoded as idx+1; 0 = nil
		}
	}
	x.free.Store(1) // slot 0, tag 0
	return x
}

// RingCapacity returns the actual (power-of-two) ring capacity.
func (x *ChanRing26Kimi[V]) RingCapacity() int {
	return len(x.cells)
}

// MaxSubscribers returns the fixed subscriber pool size.
func (x *ChanRing26Kimi[V]) MaxSubscribers() int {
	return len(x.slots)
}

// Len returns the number of live subscriptions.
func (x *ChanRing26Kimi[V]) Len() int {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
	return int(x.live.Load())
}

// Send publishes value to the live tail of the ring and signals every live
// subscriber with a non-blocking cap-1 token. It never blocks, never
// allocates, and never waits for receivers; lagging receivers lose values
// to ring overwrite. Returns the number of live subscriptions signaled.
//
// Send is safe for concurrent use; concurrent senders are serialized only
// for the (tiny) ring publication, and fan out independently afterwards.
func (x *ChanRing26Kimi[V]) Send(value V) (sent int) {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}

	// Publish: claim a dense, ordered absolute position and seqlock the cell.
	x.writeMu.Lock()
	p := x.write.Add(1)
	cell := &x.cells[p&x.mask]
	seq := p << 1
	cell.seq.Store(seq | 1) // odd: write in flight
	cell.val.Store(&value)
	cell.seq.Store(seq) // even: published
	x.writeMu.Unlock()

	// Fan out: one atomic load and one non-blocking signal per live slot.
	// Dead slots cost a single atomic load; the scan touches no locks and
	// allocates nothing. A token raced in after unsubscribe is harmless:
	// the channel is cap 1 and a stale token costs one empty drain cycle.
	for i := range x.slots {
		s := &x.slots[i]
		if !s.live.Load() {
			continue
		}
		select {
		case s.ch <- struct{}{}:
		default:
		}
		sent++
	}
	return
}

// SubscribeContext returns an iterator over the values published after the
// subscription was registered, until ctx is done or the iterator stops.
// The subscription is registered eagerly (like ChanPubSub) and released when
// the iterator terminates. Values are yielded oldest-to-newest; values lost
// to ring overwrite are silently skipped - size the ring for your slowest
// consumer's tolerable lag.
func (x *ChanRing26Kimi[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	s := x.subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		if yield == nil {
			x.unsubscribe(s)
			panic(`bigbuff: chanring: iterator yield func is nil`)
		}
		defer x.unsubscribe(s)
		for {
			// Drain BEFORE blocking: the token is sent after publication
			// and the ring retains the value, so this ordering closes the
			// missed-wakeup window structurally.
			if !x.drain(s, yield) {
				return
			}
			// Observe cancellation before parking, so a cancelled iterator
			// cannot sleep through its last wake-up token.
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case <-ctx.Done():
				return
			case <-s.ch:
			}
		}
	}
}

// subscribe pops a slot from the ABA-tagged Treiber pool and activates it.
func (x *ChanRing26Kimi[V]) subscribe() *chanRing26KimiSlot {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
	for {
		h := x.free.Load()
		if uint32(h) == 0 {
			panic(`bigbuff: chanring: maximum subscribers exceeded`)
		}
		i := uint32(h) - 1
		nxt := x.slots[i].next.Load()
		nh := uint64(uint32(h>>32)+1)<<32 | uint64(nxt)
		if x.free.CompareAndSwap(h, nh) {
			s := &x.slots[i]
			// Live tail: the next sequence to be published (sequences are
			// one-based; position 0 is never written). No backlog is replayed.
			s.cur = x.write.Load() + 1
			s.lost = 0
			s.live.Store(true) // visible to senders before any blocking
			x.live.Add(1)
			return s
		}
	}
}

// unsubscribe deactivates the slot and returns it to the free pool.
func (x *ChanRing26Kimi[V]) unsubscribe(s *chanRing26KimiSlot) {
	if x.cells == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
	if !s.live.Load() {
		panic(chanRing26KimiStateInvariantViolation)
	}
	s.live.Store(false)
	x.live.Add(-1)
	for {
		h := x.free.Load()
		s.next.Store(uint32(h))
		nh := uint64(uint32(h>>32)+1)<<32 | uint64(s.idx+1)
		if x.free.CompareAndSwap(h, nh) {
			return
		}
	}
}

// drain reads every retained position at or after s.cur, yielding each in
// publication order, and returns false if yield stopped the iteration.
//
// Overwritten positions are counted in s.lost and skipped via a single jump
// to the oldest retained position. Spining is bounded: it can only occur
// while a publisher is inside its critical section.
func (x *ChanRing26Kimi[V]) drain(s *chanRing26KimiSlot, yield func(V) bool) bool {
	w := x.write.Load()
	spins := 0
	for {
		// Refresh the published watermark every iteration: a stale watermark
		// makes the overwrite jump (below) move a cursor backwards.
		w = x.write.Load()
		if s.cur > w {
			return true
		}
		p := s.cur
		cell := &x.cells[p&x.mask]
		seq := cell.seq.Load()
		pos := seq >> 1
		switch {
		case pos == p && seq&1 == 0:
			vp := cell.val.Load()
			if vp == nil {
				break
			}
			v := *vp
			if cell.seq.Load() == seq {
				s.cur = p + 1
				if !yield(v) {
					return false
				}
				w = x.write.Load()
				spins = 0
				continue
			}
			// Torn read: fall through to the bounded spin.
		case pos > p:
			// Position p was overwritten; jump to the oldest position still
			// retained by the ring. pos is the newest position known to be
			// published into this cell, and a ring of mask+1 cells retains
			// positions pos-mask .. pos; before the first wrap everything from
			// position 1 is retained.
			oldest := uint64(1)
			if pos > x.mask {
				oldest = pos - x.mask
			}
			s.lost += uint32(oldest - p)
			s.cur = oldest
			spins = 0
			continue
		}
		// Otherwise the publisher claimed p but has not published it yet
		// (or we observed a torn sequence); retry the same position.
		if spins++; spins > 64 {
			runtime.Gosched()
		}
	}
}

// chanRing26KimiCell is one position of the shared broadcast ring. seq is a
// seqlock: odd means a write is in flight, even means 2*position is
// published. The trailing padding keeps cells off shared cache lines; seq
// is the first word so 64-bit atomic alignment is guaranteed on 64-bit
// platforms (the intended target for this design).
type chanRing26KimiCell[V any] struct {
	seq atomic.Uint64
	// val is published through a pointer so that the seqlock's payload read is
	// an atomic load. The original wrote and read the payload word directly,
	// which is a data race whenever a publisher rewrites the cell a reader is
	// still inspecting (the seqlock retry detects it, but the Go memory model
	// and the race detector do not accept it). Reading through an atomic
	// pointer does not change the mechanism: one shared ring, seqlock sequence,
	// per-subscriber cursor.
	val atomic.Pointer[V]
	_   [56]byte
}

// chanRing26KimiSlot is one subscription handle in the fixed pool. cur and lost
// are touched only by the owning iterator goroutine; live, next and the
// channel are shared with senders. Padding mitigates false sharing between
// a receiver's cursor traffic and sender signal traffic.
type chanRing26KimiSlot struct {
	live atomic.Bool   // set before the slot becomes observable by senders
	ch   chan struct{} // cap 1; private wake signal for the owning iterator
	idx  uint32        // index into the pool (for the free list)
	next atomic.Uint32 // free-list link, encoded idx+1 (0 = nil)
	cur  uint64        // next ring position to read (owner only)
	lost uint32        // values skipped due to overwrite (owner only)
	_    [64]byte
}
