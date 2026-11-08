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
	"iter"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrSenseSubClosed is returned by Subscription.Recv once the subscription has been closed.
var ErrSenseSubClosed = errors.New("bigbuff: sensecaster: subscription closed")

// SenseCaster is a synchronous, select-compatible broadcast. Each value passed to
// Send is delivered exactly once to every Subscription registered when Send begins,
// and Send returns only after each of them has received the value or been closed.
// Backpressure therefore matches an unbuffered channel: a subscriber that stops
// receiving stalls publishers.
//
// The work is arranged differently from a shared-channel caster:
//
//   - The value is stored once. Owed subscriptions copy it out themselves, so a busy
//     subscriber costs the publisher only an atomic acknowledgement. No channel send
//     sits on the data path.
//   - A publish touches only parked subscriptions. A subscription with nothing to read
//     advertises that in a three-level bitmap (segment summary, word, bit). The publisher
//     sweeps the bitmap and posts a non-blocking token to each owner it finds. A publish
//     therefore costs O(parked + subscriptions/4096), not O(subscriptions).
//   - Acknowledgement is sense-reversing. Each publish flips one global bit. A
//     subscription is owed the in-flight value exactly when its own sense bit differs
//     from the global one. Joining and leaving need no per-publish bookkeeping.
//
// Subscribe, Close, TryRecv and Send perform no heap allocation once the caster has
// reached its high-water mark of concurrent subscriptions. The zero value is ready to use.
//
// Every subscription must eventually be drained or closed. A subscription that owes a
// value and is never read blocks Send, as an unread unbuffered channel would.
type SenseCaster[V any] struct {
	sendMu sync.Mutex // one publish in flight at a time
	mu     sync.Mutex // guards topology, lane setup, the sense flip and the sweep

	sense atomic.Uint64 // flipped once per publish; the publication point
	_     [56]byte

	lanesLeft atomic.Int64 // lanes that still owe acknowledgements for the in-flight publish
	_         [56]byte

	active atomic.Int64 // mirror of nActive for the lock-free empty fast path
	_      [56]byte

	lanes [scLanes]scLane

	// Guarded by mu.
	nActive    int
	laneActive [scLanes]int
	laneMask   uint64 // bit l set iff laneActive[l] != 0; lets Send touch only populated lanes
	slots      []*scSlot
	segs       []*scSeg
	free       []int
	done       chan struct{} // capacity 1; signalled when lanesLeft reaches zero
	value      V             // in-flight value: written under mu, read only by owed subscriptions
}

const (
	scWordBits = 64
	scWords    = 64
	scSegSlots = scWordBits * scWords // 4096 subscriptions per parked-set segment
	scLanes    = 32                   // outstanding-acknowledgement lanes

	scSense   uint64 = 1 << 0 // a subscription's sense; equals the caster's once acknowledged
	scReading uint64 = 1 << 1 // a receiver is copying the value out; closers wait for it
	scEpochSh        = 2      // the epoch occupies the bits above the two flags
)

// scLane is an outstanding-acknowledgement counter padded to its own cache line.
type scLane struct {
	owed atomic.Int64
	_    [56]byte
}

// scWord is one word of the parked set, padded so that words never share a cache line.
type scWord struct {
	atomic.Uint64
	_ [56]byte
}

// scSeg is one segment of the parked set: 64 padded words of 64 slot bits, plus a summary
// word whose bit w means "words[w] may be non-zero".
type scSeg struct {
	summary atomic.Uint64
	_       [56]byte
	words   [scWords]scWord
}

// scSlot is the reusable cell behind a subscription. Slots are never freed. They cycle
// through the free list, and the epoch in word tells one tenancy from the next.
type scSlot struct {
	word  atomic.Uint64 // epoch<<scEpochSh | scReading | scSense
	ready chan struct{} // capacity 1; carries wake-up tokens only, never closed
	index int
	lane  int

	park    *atomic.Uint64 // words[w] of this slot's segment
	parkBit uint64
	sum     *atomic.Uint64 // summary word of this slot's segment
	sumBit  uint64
}

// arm advertises that the owner is about to wait. Only arm sets bits; only sweeps clear
// them. A bit already observed set is pending for a sweep that has not yet run, and that
// sweep will wake this slot. The caller re-checks the sense after arming, so a publish
// that raced the arm is never slept through.
func (sl *scSlot) arm() {
	if sl.park.Load()&sl.parkBit == 0 {
		sl.park.Or(sl.parkBit)
	}
	if sl.sum.Load()&sl.sumBit == 0 {
		sl.sum.Or(sl.sumBit)
	}
}

// wake posts a wake-up token without blocking. Tokens coalesce, and a spurious token
// costs only one extra TryRecv.
func (sl *scSlot) wake() {
	select {
	case sl.ready <- struct{}{}:
	default:
	}
}

// NewSenseCaster returns an empty caster. The zero value is also ready to use.
func NewSenseCaster[V any]() *SenseCaster[V] {
	return new(SenseCaster[V])
}

// Subscribe registers a new subscription. It is not owed the value of any publish that
// is already in flight.
func (c *SenseCaster[V]) Subscribe() SenseSubscription[V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done == nil {
		c.done = make(chan struct{}, 1)
	}
	var sl *scSlot
	if n := len(c.free); n > 0 {
		sl = c.slots[c.free[n-1]]
		c.free = c.free[:n-1]
	} else {
		sl = c.grow()
	}
	epoch := sl.word.Load() >> scEpochSh
	// Adopting the current sense is what keeps a joiner from being owed the in-flight value.
	sl.word.Store((epoch << scEpochSh) | (c.sense.Load() & scSense))
	c.laneActive[sl.lane]++
	c.laneMask |= uint64(1) << sl.lane
	c.nActive++
	c.active.Store(int64(c.nActive))
	return SenseSubscription[V]{c: c, s: sl, epoch: epoch}
}

// grow allocates a fresh slot. It runs under mu. The slot table and the parked set grow
// together, so a sweep never meets a bit without a slot to wake.
func (c *SenseCaster[V]) grow() *scSlot {
	i := len(c.slots)
	si, local := i/scSegSlots, i%scSegSlots
	if si == len(c.segs) {
		c.segs = append(c.segs, new(scSeg))
	}
	seg := c.segs[si]
	// Transposed mapping: neighbouring subscriptions land in different words, so their
	// concurrent arms contend on different cache lines.
	w, b := local%scWords, local/scWords
	sl := &scSlot{
		ready:   make(chan struct{}, 1),
		index:   i,
		lane:    i % scLanes,
		park:    &seg.words[w].Uint64,
		parkBit: uint64(1) << b,
		sum:     &seg.summary,
		sumBit:  uint64(1) << w,
	}
	c.slots = append(c.slots, sl)
	return sl
}

// Subscribers reports the number of open subscriptions.
func (c *SenseCaster[V]) Subscribers() int {
	return int(c.active.Load())
}

// Send publishes v to every subscription registered when Send begins and returns the
// number of subscriptions it was addressed to. Send blocks until each of them has
// received v or been closed. A subscription closed before receiving discards v. Do not
// call Send from a goroutine that holds an undrained subscription.
func (c *SenseCaster[V]) Send(v V) int {
	if c.active.Load() == 0 {
		return 0 // no subscriptions (fast path)
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	n := c.nActive
	if n == 0 {
		c.mu.Unlock()
		return 0
	}
	c.value = v
	// Between publishes every owed counter is zero, so only populated lanes need a store.
	var lanes int64
	for m := c.laneMask; m != 0; m &= m - 1 {
		l := bits.TrailingZeros64(m)
		c.lanes[l].owed.Store(int64(c.laneActive[l]))
		lanes++
	}
	c.lanesLeft.Store(lanes)
	c.sense.Store(c.sense.Load() ^ scSense) // publication point: owners observe this flip
	c.sweep()
	c.mu.Unlock()

	for c.lanesLeft.Load() != 0 {
		<-c.done
	}
	var zero V
	c.value = zero // drop the reference; nothing reads it once every lane has drained
	return n
}

// sweep wakes every parked owner. Each bit is taken with a swap, so an owner is woken at
// most once per arm. An owner that finds nothing to read arms again.
func (c *SenseCaster[V]) sweep() {
	for si, seg := range c.segs {
		if seg.summary.Load() == 0 {
			continue
		}
		for s := seg.summary.Swap(0); s != 0; s &= s - 1 {
			w := bits.TrailingZeros64(s)
			for m := seg.words[w].Swap(0); m != 0; m &= m - 1 {
				c.slots[si*scSegSlots+bits.TrailingZeros64(m)*scWords+w].wake()
			}
		}
	}
}

// ack records that one owed subscription has acknowledged. The last acknowledgement in
// the last lane releases the publisher.
func (c *SenseCaster[V]) ack(lane int) {
	n := c.lanes[lane].owed.Add(-1)
	if n < 0 {
		panic("bigbuff: sensecaster: acknowledgement underflow")
	}
	if n == 0 && c.lanesLeft.Add(-1) == 0 {
		select {
		case c.done <- struct{}{}:
		default:
		}
	}
}

// Subscription is a receive endpoint on a SenseCaster. It is a small value: copies refer
// to the same subscription. TryRecv, Recv and Close may be called concurrently from any
// goroutine, and each value is delivered to exactly one caller.
type SenseSubscription[V any] struct {
	c     *SenseCaster[V]
	s     *scSlot
	epoch uint64
}

// Closed reports whether the subscription has been closed. The zero value is closed.
func (s SenseSubscription[V]) Closed() bool {
	return s.c == nil || s.s.word.Load()>>scEpochSh != s.epoch
}

// Ready returns a channel that becomes receivable when the subscription may have a value
// to read, or when it has been closed. Ready arms the wake-up itself, so it is safe to
// select on directly. After it fires, call TryRecv: a wake-up consumed without a
// following TryRecv is lost to this subscription. For the zero value it returns nil.
func (s SenseSubscription[V]) Ready() <-chan struct{} {
	if s.c == nil {
		return nil
	}
	s.s.arm()
	if (s.s.word.Load()^s.c.sense.Load())&scSense != 0 {
		s.s.wake() // a publish raced the arm, so its token is ours to post
	}
	return s.s.ready
}

// TryRecv returns the value owed to this subscription without blocking. When there is
// none it arms the wake-up and returns false. It also returns false once the
// subscription is closed; see Closed.
func (s SenseSubscription[V]) TryRecv() (V, bool) {
	var zero V
	c := s.c
	if c == nil {
		return zero, false
	}
	sl := s.s
	for {
		w := sl.word.Load()
		if w>>scEpochSh != s.epoch {
			// Retired. This handle may have just consumed a wake-up meant for the slot's
			// current tenant, which can be waiting on the same channel. Pass it on, so a
			// retired waiter can never swallow a wake-up.
			sl.wake()
			return zero, false
		}
		if w&scReading != 0 {
			runtime.Gosched() // another goroutine is inside its copy-out window
			continue
		}
		if (w^c.sense.Load())&scSense != 0 {
			// Owed. Claim the copy-out window. Closers observe scReading and wait for us.
			if !sl.word.CompareAndSwap(w, w|scReading) {
				continue
			}
			v := c.value
			sl.word.Store(w ^ scSense) // clear the claim and acknowledge in one store
			c.ack(sl.lane)
			return v, true
		}
		sl.arm()
		// Dekker-style re-check. A publish that flipped the sense before our arm became
		// visible may have been swept past us, so look again before sleeping.
		if (sl.word.Load()^c.sense.Load())&scSense == 0 {
			return zero, false
		}
	}
}

// Recv blocks until a value is available, the context ends, or the subscription is
// closed. It returns ErrSenseSubClosed in the last case.
func (s SenseSubscription[V]) Recv(ctx context.Context) (V, error) {
	var zero V
	for {
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		if s.Closed() {
			return zero, ErrSenseSubClosed
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-s.Ready():
		}
	}
}

// All yields values until the context ends or the subscription is closed.
func (s SenseSubscription[V]) All(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		for {
			v, err := s.Recv(ctx)
			if err != nil || !yield(v) {
				return
			}
		}
	}
}

// Close releases the subscription. A value it still owes is discarded on its behalf, so
// a blocked Send is released. Close is idempotent. It may be called from any goroutine,
// including while another goroutine is inside TryRecv or Recv, and it wakes any Recv
// parked on this subscription.
func (s SenseSubscription[V]) Close() {
	c := s.c
	if c == nil {
		return
	}
	sl := s.s
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		w := sl.word.Load()
		if w>>scEpochSh != s.epoch {
			return // already closed
		}
		if w&scReading != 0 {
			runtime.Gosched() // a receiver is copying the value out and will acknowledge on exit
			continue
		}
		if (w^c.sense.Load())&scSense != 0 {
			// Owed. Acknowledge on the owner's behalf. The value is discarded, so nothing
			// else needs protecting beyond the reading check above.
			if sl.word.CompareAndSwap(w, w^scSense) {
				c.ack(sl.lane)
			}
			continue
		}
		// Not owed, and the sense cannot change while we hold mu. Retire this tenancy.
		if sl.word.CompareAndSwap(w, (s.epoch+1)<<scEpochSh) {
			break
		}
	}
	c.nActive--
	c.laneActive[sl.lane]--
	if c.laneActive[sl.lane] == 0 {
		c.laneMask &^= uint64(1) << sl.lane
	}
	c.active.Store(int64(c.nActive))
	c.free = append(c.free, sl.index)
	sl.wake() // unblock a parked Recv; a stale token is harmless
}
