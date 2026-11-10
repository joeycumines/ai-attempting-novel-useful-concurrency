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
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrRingClosed67Haiku is returned by Ring67Haiku.Publish after Close, and by RingCursor67Haiku.Next once the
// ring is closed and the cursor has drained every ticket that was published before Close.
var ErrRingClosed67Haiku = errors.New("bigbuff: ring: closed")

const (
	ringCacheLine67Haiku    = 64
	ringMaxElemBytes67Haiku = 64 // also sizes the fixed scratch buffer on the -race path
	ringMaxElemAlign67Haiku = 8  // slot words are 8-byte aligned
	ringMaxCapacity67Haiku  = 1 << 30
)

// Ring67Haiku is a bounded, lossy, multi-producer broadcast ring of small pointer-free values.
//
// Scenario. Many producers emit fixed-size telemetry records (counters, gauges, latest
// state). Any number of cursors read the stream at their own pace. Publish is wait-free
// with respect to cursors: a stalled or slow reader never delays a writer or another reader.
// A reader that falls more than Cap tickets behind is not blocked. It is told exactly how
// many tickets it missed, resumes from the oldest ticket still retained, and never sees a
// torn value.
//
// Why not chan, sync.Mutex or sync.Map. A buffered-channel fan-out costs the publisher one
// locked send per subscriber and forces a per-subscriber drop decision with no count. A
// mutex-guarded ring serialises every producer and every consumer on one line. A
// sync.Cond broadcast wakes all waiters and cannot be selected on. atomic.Value or
// atomic.Pointer per publish allocates.
//
// Mechanics.
//   - One fetch-add on head assigns each publish a dense ticket t. The ticket is the only
//     coordination point between producers.
//   - Each slot is a seqlock. seq is 2t+1 while ticket t is being written and 2t+2 once it
//     is committed. A reader copies the payload and keeps the copy only if seq is unchanged
//     on both sides of the copy. A seq greater than the reader's expected value means the
//     slot was overwritten, which is detected with no extra state and reported as an exact
//     skip count.
//   - A writer for ticket t claims its slot by CAS from the committed state of ticket t-Cap.
//     Laps of the same slot are therefore strictly ordered and no lock is taken. A writer
//     waits only while ticket t-Cap has not committed, which requires a stalled publisher to
//     have been overtaken by a full ring of later publishes.
//   - Tickets are mapped to slots so that consecutive tickets fall on different 64-byte cache
//     lines (lane striping). Concurrent producers claiming adjacent tickets therefore do not
//     false-share a line. Striping is a no-op when an element does not fit a line.
//   - Blocking is select-compatible. A cursor increments waiters, obtains the current wake
//     channel, and re-checks its slot before parking. A publisher that commits after the
//     re-check must observe waiters > 0 and close that channel. Publish only touches the
//     mutex when someone is parked. A single close wakes every cursor parked on the cycle.
//     Parking allocates only when a new wake cycle starts.
//
// Constraints. V must be pointer-free, at most 64 bytes, and at most 8-byte aligned. These
// are checked at construction and the constructor panics otherwise. Pointer-freedom is what
// makes the seqlock's discarded torn copies invisible to the garbage collector. Use
// ChanPubSub or ChanCaster for pointerful payloads.
//
// Guarantees.
//   - Publish never waits for cursors.
//   - A cursor delivers tickets in increasing order. For each returned value, the skipped
//     count plus the number of values delivered equals the number of tickets passed over.
//   - Values are never torn. Tests verify this with self-checking payloads.
//   - Publish is allocation-free on the steady-state path.
//
// Limitations.
//   - A cursor waiting on a ticket whose publisher stalls mid-write waits for that publisher.
//   - Close is a quiescent shutdown. See Close.
//   - The non-race build copies payloads with plain loads and stores. The Go memory model
//     does not sanction that for racing accesses. The protocol tolerates it only because of
//     post-copy validation, as described in ring_payload_fast.go. The -race build uses atomic
//     word accesses so the race detector can check the rest of the protocol.
type Ring67Haiku[V any] struct {
	// head is the ticket dispenser that every publisher hits, so it sits on its own line.
	head atomic.Uint64
	_    [ringCacheLine67Haiku - 8]byte

	// closed and waiters are read on every Publish but written only on Close and when a
	// cursor parks. mu and wake are touched only on the blocking path.
	closed  atomic.Bool
	waiters atomic.Int64
	mu      sync.Mutex
	wake    chan struct{}
	_       [ringCacheLine67Haiku]byte

	// Read-only after construction.
	slots    []ringSlot67Haiku[V]
	n        uint64 // len(slots), a power of two
	laneMask uint64 // lanes-1
	slotMask uint64 // slots-per-lane minus 1
	laneBits uint
	slotBits uint
}

// ringSlot67Haiku is one ring entry. seq is the seqlock word: 2t+1 while ticket t is being written
// into the slot, 2t+2 once it is committed, and 0 before the first write. Because seq is
// 8 bytes, val always starts at offset 8 and the slot size is a multiple of 8.
type ringSlot67Haiku[V any] struct {
	seq atomic.Uint64
	val V
}

type ringRead67Haiku uint8

const (
	ringOK67Haiku      ringRead67Haiku = iota // the slot holds the requested ticket and the copy is validated
	ringPending67Haiku                        // the ticket is claimed but not yet committed, or not yet claimed
	ringLapped67Haiku                         // a later ticket has overwritten the slot
)

// NewRing67Haiku returns a Ring67Haiku that retains the most recent capacity tickets, rounded up to a
// power of two. It panics if V is not a supported element type.
func NewRing67Haiku[V any](capacity int) *Ring67Haiku[V] {
	return newRing67Haiku[V](capacity, true)
}

// newRing67Haiku is NewRing67Haiku with lane striping switchable. The unstriped form is the ablation
// baseline used by the benchmarks.
func newRing67Haiku[V any](capacity int, striped bool) *Ring67Haiku[V] {
	checkRingElem67Haiku[V]()
	if capacity < 1 || capacity > ringMaxCapacity67Haiku {
		panic(fmt.Sprintf("bigbuff: ring: capacity %d out of range [1, %d]", capacity, ringMaxCapacity67Haiku))
	}
	n := uint64(1) << bits.Len(uint(capacity-1))

	// Slots per cache-line lane is the largest power of two that fits in a line. It stays at
	// 1 when striping is off or the slot is larger than a line.
	perLine := uint64(1)
	if stride := uint64(unsafe.Sizeof(ringSlot67Haiku[V]{})); striped && stride <= ringCacheLine67Haiku {
		perLine = uint64(1) << (bits.Len64(ringCacheLine67Haiku/stride) - 1)
		if perLine > n {
			perLine = n
		}
	}
	slotBits := uint(bits.TrailingZeros64(perLine))
	laneBits := uint(bits.TrailingZeros64(n)) - slotBits
	return &Ring67Haiku[V]{
		slots:    make([]ringSlot67Haiku[V], n),
		n:        n,
		laneMask: (uint64(1) << laneBits) - 1,
		slotMask: perLine - 1,
		laneBits: laneBits,
		slotBits: slotBits,
	}
}

// Cap reports how many tickets the ring retains.
func (r *Ring67Haiku[V]) Cap() int { return int(r.n) }

// Published reports how many tickets have been claimed. Some may still be in flight.
func (r *Ring67Haiku[V]) Published() uint64 { return r.head.Load() }

// Publish appends v as the next ticket and returns its sequence number. Tickets are dense
// and start at 0. Publish never waits for cursors. It waits only while the previous occupant
// of its slot is still being written, which requires a full ring of later publishes to have
// overtaken a publisher that is stalled mid-write.
func (r *Ring67Haiku[V]) Publish(v V) (uint64, error) {
	if r.closed.Load() {
		return 0, ErrRingClosed67Haiku
	}
	t := r.head.Add(1) - 1
	sl := &r.slots[r.index(t)]
	var prev uint64 // a never-written slot holds 0, which is the committed state of "lap -1"
	if t >= r.n {
		prev = 2*(t-r.n) + 2
	}
	for !sl.seq.CompareAndSwap(prev, 2*t+1) {
		runtime.Gosched()
	}
	ringStore67Haiku(sl, v)
	sl.seq.Store(2*t + 2)
	if r.waiters.Load() != 0 {
		r.wakeAll()
	}
	return t, nil
}

// Latest returns the newest committed value and its ticket without creating a cursor. ok is
// false when no ticket in the retained window is committed yet.
func (r *Ring67Haiku[V]) Latest() (value V, ticket uint64, ok bool) {
	for {
		h := r.head.Load()
		retry := false
		for back := uint64(1); back <= h && back <= r.n && !retry; back++ {
			v, st := r.load(h - back)
			switch st {
			case ringOK67Haiku:
				return v, h - back, true
			case ringLapped67Haiku:
				retry = true // a newer ticket landed during the scan, so rescan from a fresh head
			}
		}
		if !retry {
			return value, 0, false
		}
	}
}

// Close stops Publish, which then returns ErrRingClosed67Haiku, and wakes parked cursors. Cursors
// drain the tickets published before Close and then receive ErrRingClosed67Haiku. Close is
// idempotent. Quiesce publishers before calling it. A Publish that races with Close can
// return nil while cursors have already observed the ring as closed and do not see its
// ticket.
func (r *Ring67Haiku[V]) Close() {
	r.closed.Store(true)
	r.wakeAll()
}

// index maps a ticket to its slot. Tickets that differ in their low bits land on different
// lanes, so back-to-back claims by concurrent publishers do not share a cache line. With
// striping off, slotBits is 0 and this reduces to t mod n. Tickets t and t+n always map to
// the same slot.
func (r *Ring67Haiku[V]) index(t uint64) uint64 {
	return (t&r.laneMask)<<r.slotBits | (t>>r.laneBits)&r.slotMask
}

// load reads ticket t under the seqlock protocol. It returns a copy only when the slot
// carries the same committed tag before and after the copy, so a torn copy is never returned.
func (r *Ring67Haiku[V]) load(t uint64) (V, ringRead67Haiku) {
	sl := &r.slots[r.index(t)]
	want := 2*t + 2
	for {
		got := sl.seq.Load()
		if got != want {
			var zero V
			if got > want {
				return zero, ringLapped67Haiku
			}
			return zero, ringPending67Haiku
		}
		v := ringLoad67Haiku(sl)
		if sl.seq.Load() == want {
			return v, ringOK67Haiku
		}
		// A writer started on this slot during the copy. Reclassify on the next pass.
	}
}

// wakeAll closes the current wake cycle, if anyone is parked on it. The next parker starts a
// new cycle, so one close wakes every cursor parked on the same cycle.
func (r *Ring67Haiku[V]) wakeAll() {
	r.mu.Lock()
	ch := r.wake
	r.wake = nil
	r.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// parkChan returns the wake channel for the current cycle, creating it if needed. Callers
// must have incremented waiters first. Then any publisher that commits after the caller's
// re-check is guaranteed to observe the increment and close the channel returned here.
func (r *Ring67Haiku[V]) parkChan() chan struct{} {
	r.mu.Lock()
	if r.wake == nil {
		r.wake = make(chan struct{})
	}
	ch := r.wake
	r.mu.Unlock()
	return ch
}

// RingCursor67Haiku is a single-consumer position in a Ring67Haiku. It is not safe for concurrent use, so
// give each consuming goroutine its own cursor. Cursors are independent. A slow cursor never
// delays a publisher or another cursor.
type RingCursor67Haiku[V any] struct {
	ring    *Ring67Haiku[V]
	next    uint64 // next ticket to deliver
	pending uint64 // tickets passed over since the last return from Next
}

// Subscribe returns a cursor positioned at the next ticket to be published.
func (r *Ring67Haiku[V]) Subscribe() *RingCursor67Haiku[V] {
	return &RingCursor67Haiku[V]{ring: r, next: r.head.Load()}
}

// SubscribeReplay returns a cursor positioned at the most recent n tickets. n is capped at
// Cap and at the number of tickets published so far.
func (r *Ring67Haiku[V]) SubscribeReplay(n int) *RingCursor67Haiku[V] {
	h := r.head.Load()
	back := uint64(0)
	if n > 0 {
		back = uint64(n)
	}
	if back > r.n {
		back = r.n
	}
	if back > h {
		back = h
	}
	return &RingCursor67Haiku[V]{ring: r, next: h - back}
}

// Next returns the next value in ticket order, blocking until it is published or ctx is
// done. The second return is the number of tickets this cursor passed over, because they
// were overwritten before it read them, since the previous return. The count is exact and is
// reported with the next return of any kind, including errors. Next returns ErrRingClosed67Haiku
// once the ring is closed and the cursor has caught up. A nil ctx is treated as
// context.Background.
func (c *RingCursor67Haiku[V]) Next(ctx context.Context) (V, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := c.ring
	for {
		v, st := r.load(c.next)
		switch st {
		case ringOK67Haiku:
			c.next++
			return v, c.take(), nil
		case ringLapped67Haiku:
			c.fastForward()
			continue
		}

		// Pending. Announce first, then re-check. A publisher that commits after our re-check
		// must observe waiters > 0 and close the channel we are about to select on.
		r.waiters.Add(1)
		wake := r.parkChan()
		v, st = r.load(c.next)
		switch st {
		case ringOK67Haiku:
			r.waiters.Add(-1)
			c.next++
			return v, c.take(), nil
		case ringLapped67Haiku:
			r.waiters.Add(-1)
			c.fastForward()
			continue
		}
		// closed is read before head. Every Publish that returned before Close is therefore
		// visible to the head load below.
		if r.closed.Load() && c.next >= r.head.Load() {
			r.waiters.Add(-1)
			var zero V
			return zero, c.take(), ErrRingClosed67Haiku
		}
		select {
		case <-wake:
			r.waiters.Add(-1)
		case <-ctx.Done():
			r.waiters.Add(-1)
			var zero V
			return zero, c.take(), ctx.Err()
		}
	}
}

// fastForward moves the cursor to the oldest ticket whose slot can still hold data, and
// records the tickets passed over. A lapped read means some ticket at least n ahead of
// c.next has been claimed, so the target is always strictly ahead of the cursor.
func (c *RingCursor67Haiku[V]) fastForward() {
	h := c.ring.head.Load()
	target := uint64(0)
	if h >= c.ring.n {
		target = h - c.ring.n + 1
	}
	if target <= c.next {
		panic(`bigbuff: ring: cursor invariant violation`)
	}
	c.pending += target - c.next
	c.next = target
}

// take returns and clears the skipped-ticket count.
func (c *RingCursor67Haiku[V]) take() uint64 {
	s := c.pending
	c.pending = 0
	return s
}

// checkRingElem67Haiku panics if V cannot be stored in a ring slot.
func checkRingElem67Haiku[V any]() {
	t := reflect.TypeFor[V]()
	if t.Size() > ringMaxElemBytes67Haiku || t.Align() > ringMaxElemAlign67Haiku || !ringPointerFree67Haiku(t) {
		panic(fmt.Sprintf("bigbuff: ring: element type %v must be pointer-free, at most %d bytes, and at most %d-byte aligned",
			t, ringMaxElemBytes67Haiku, ringMaxElemAlign67Haiku))
	}
}

// ringPointerFree67Haiku reports whether values of type t contain no GC-visible pointers.
func ringPointerFree67Haiku(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return t.Len() == 0 || ringPointerFree67Haiku(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			if !ringPointerFree67Haiku(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
