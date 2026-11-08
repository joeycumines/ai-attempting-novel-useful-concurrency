// Provenance: 55-arena-claude-haiku-5.5-high-prompt-safe.md (Claude Haiku, response #55). Namespaced to preserve existing APIs.
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
	"sync"
	"sync/atomic"
)

// ErrRingClosed55Haiku is returned by [RingSub55Haiku.Recv] once the [RingCaster55Haiku] has been
// closed and the subscriber has consumed every message it still retains.
var ErrRingClosed55Haiku = errors.New("bigbuff: ringcaster: closed")

const (
	ring55HaikuMaxCapacity        = 1 << 30
	ring55HaikuSlabMax            = 256
	ring55HaikuInvariantViolation = `bigbuff: ringcaster: state invariant violation`
)

// noWake55Haiku is the "nothing armed" state of [RingCaster55Haiku.wake]. It is boxed once,
// so swapping it in never allocates.
var noWake55Haiku any = (chan struct{})(nil)

var ringClosedWake55Haiku = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// ringCell55Haiku is an immutable message cell. It is written exactly once, strictly
// before the ring slot referencing it is stored, and is never mutated or
// reused afterwards. Readers therefore need no locks, seqlocks, or
// synchronisation beyond the atomic slot load.
type ringCell55Haiku[V any] struct {
	seq uint64
	v   V
}

// RingCaster55Haiku is a non-blocking broadcast ring with exact loss accounting. It
// is the data-plane complement to [ChanCaster]:
//
//   - [RingCaster55Haiku.Publish] never waits for subscribers. It does constant work,
//     plus a single channel close shared by all parked subscribers, if any.
//   - A subscriber is only a cursor ([RingSub55Haiku]). Subscribing and unsubscribing
//     touch no shared state, so churn costs the publisher nothing.
//   - Subscribers integrate with select via [RingSub55Haiku.Arm] and
//     [RingSub55Haiku.TryRecv].
//   - A subscriber more than the capacity behind is never back-pressured. It
//     skips ahead, and the skipped count is exact ([RingSub55Haiku.Dropped]).
//     With TryRecv and a cursor starting at sequence zero, received + Dropped()
//     equals publications; SubscribeFrom's initial retention gap is counted too.
//     TryRecvLatest deliberately excludes conflated values from Dropped().
//
// Allocation: one slab per min(capacity, 256) publishes, plus at most one wake
// channel per publish, and only when a subscriber has armed since the previous
// publish. Consumers that keep up with the publisher cause no allocation.
//
// Retention: the ring pins at most capacity + 2*256 values, regardless of how
// far any subscriber lags, because subscribers hold no references.
//
// A RingCaster55Haiku is safe for concurrent use by any number of publishers and
// subscribers. Each individual [RingSub55Haiku] must be used by one goroutine at a
// time.
type RingCaster55Haiku[V any] struct {
	ring []atomic.Pointer[ringCell55Haiku[V]] // slot for seq is seq & mask
	mask uint64

	tail   atomic.Uint64 // number of published messages, i.e. the next seq
	closed atomic.Bool
	wake   atomic.Value // chan struct{} while armed, otherwise noWake55Haiku

	pubMu   sync.Mutex // serialises Publish and Close, guards slab fields
	slab    []ringCell55Haiku[V]
	slabPos int

	armMu sync.Mutex // serialises creation of wake channels
}

func (b *RingCaster55Haiku[V]) checkInitialized() {
	if b == nil || len(b.ring) == 0 {
		panic(`bigbuff: ringcaster: must use constructor`)
	}
}

func (s *RingSub55Haiku[V]) checkInitialized() {
	if s == nil || s.b == nil {
		panic(`bigbuff: ringcaster: subscription is not initialized`)
	}
	s.b.checkInitialized()
}

// NewRingCaster55Haiku constructs a [RingCaster55Haiku] retaining the most recent capacity
// messages. Capacity is rounded up to a power of two. It panics if capacity is
// outside [1, 1<<30].
func NewRingCaster55Haiku[V any](capacity int) *RingCaster55Haiku[V] {
	if capacity < 1 || capacity > ring55HaikuMaxCapacity {
		panic(`bigbuff: ringcaster: capacity out of bounds`)
	}
	n := 1
	for n < capacity {
		n <<= 1
	}
	return &RingCaster55Haiku[V]{
		ring: make([]atomic.Pointer[ringCell55Haiku[V]], n),
		mask: uint64(n - 1),
	}
}

// Publish appends v, waking any parked subscribers, and returns its sequence
// number. It panics if the caster has been closed.
func (b *RingCaster55Haiku[V]) Publish(v V) uint64 {
	b.checkInitialized()
	b.pubMu.Lock()
	if b.closed.Load() {
		b.pubMu.Unlock()
		panic(`bigbuff: ringcaster: publish after close`)
	}

	seq := b.tail.Load()
	if seq == ^uint64(0) {
		b.pubMu.Unlock()
		panic(`bigbuff: ringcaster: sequence exhausted`)
	}
	if b.slabPos == len(b.slab) {
		b.slab = make([]ringCell55Haiku[V], min(len(b.ring), ring55HaikuSlabMax))
		b.slabPos = 0
	}
	cell := &b.slab[b.slabPos]
	b.slabPos++
	cell.seq = seq
	cell.v = v

	b.ring[seq&b.mask].Store(cell) // publish the cell (release)
	b.tail.Store(seq + 1)          // publish the sequence
	b.pubMu.Unlock()

	b.wakeArmed()
	return seq
}

// Close marks the caster closed and wakes all parked subscribers. Subscribers
// first drain what they retain, then observe [ErrRingClosed55Haiku]. Close is
// idempotent. Publish panics after Close.
func (b *RingCaster55Haiku[V]) Close() {
	b.checkInitialized()
	b.pubMu.Lock()
	b.closed.Store(true)
	b.pubMu.Unlock()
	b.wakeArmed()
}

// Subscribe joins s to the caster, starting at the next published message,
// and returns s. Passing caller-owned storage makes subscription allocation
// free, which is useful under heavy churn.
func (b *RingCaster55Haiku[V]) Subscribe(s *RingSub55Haiku[V]) *RingSub55Haiku[V] {
	b.checkInitialized()
	if s == nil {
		panic(`bigbuff: ringcaster: nil subscription storage`)
	}
	t := b.tail.Load()
	*s = RingSub55Haiku[V]{b: b, cursor: t, seen: t}
	return s
}

// SubscribeFrom joins s to the caster, starting at sequence seq, and returns s.
// seq is clamped to the oldest retained message, or to the next message to be
// published. This supports replay and resumption after reconnecting.
func (b *RingCaster55Haiku[V]) SubscribeFrom(s *RingSub55Haiku[V], seq uint64) *RingSub55Haiku[V] {
	b.checkInitialized()
	if s == nil {
		panic(`bigbuff: ringcaster: nil subscription storage`)
	}
	t := b.tail.Load()
	var oldest uint64
	if n := uint64(len(b.ring)); t > n {
		oldest = t - n
	}
	var dropped uint64
	switch {
	case seq < oldest:
		dropped = oldest - seq
		seq = oldest
	case seq > t:
		seq = t
	}
	*s = RingSub55Haiku[V]{b: b, cursor: seq, seen: t, dropped: dropped}
	return s
}

// arm returns the current wake channel, creating it if necessary. The fast
// path is a single atomic load. Creation is serialised so that concurrent
// armers share one channel, never orphaning one.
func (b *RingCaster55Haiku[V]) arm() <-chan struct{} {
	b.checkInitialized()
	if b.closed.Load() {
		return ringClosedWake55Haiku
	}
	if ch, _ := b.wake.Load().(chan struct{}); ch != nil {
		return ch
	}
	b.armMu.Lock()
	defer b.armMu.Unlock()
	ch, _ := b.wake.Load().(chan struct{})
	if ch == nil {
		ch = make(chan struct{})
		b.wake.Store(ch)
	}
	return ch
}

// wakeArmed closes the armed wake channel, if any. The common case, nothing
// armed, is a single atomic load with no write to the shared line.
func (b *RingCaster55Haiku[V]) wakeArmed() {
	if ch, _ := b.wake.Load().(chan struct{}); ch == nil {
		return
	}
	if ch, _ := b.wake.Swap(noWake55Haiku).(chan struct{}); ch != nil {
		close(ch)
	}
}

// RingSub55Haiku is a subscriber cursor over a [RingCaster55Haiku]. The zero value is not
// usable. Obtain one via [RingCaster55Haiku.Subscribe] or [RingCaster55Haiku.SubscribeFrom].
type RingSub55Haiku[V any] struct {
	b       *RingCaster55Haiku[V]
	cursor  uint64 // next sequence number to receive
	seen    uint64 // cached tail; cursor < seen implies cursor is published
	dropped uint64 // messages skipped by resume clamping or because this cursor was lapped
}

// TryRecv returns the next message without blocking. It returns ok == false
// if there is nothing new. Lapped subscribers skip ahead and account for the
// skipped messages in [RingSub55Haiku.Dropped].
//
// Select usage, for a subscriber that must also wait on other channels:
//
//	for {
//		if v, ok := sub.TryRecv(); ok { handle(v); continue }
//		wake := sub.Arm()                    // arm BEFORE the re-check
//		if v, ok := sub.TryRecv(); ok { handle(v); continue }
//		if sub.Ended() { return }
//		select {
//		case <-wake:
//		case <-done:
//			return
//		}
//	}
func (s *RingSub55Haiku[V]) TryRecv() (V, bool) {
	s.checkInitialized()
	var zero V
	if s.cursor >= s.seen {
		s.seen = s.b.tail.Load()
		if s.cursor >= s.seen {
			return zero, false
		}
	}

	for {
		// Start at the oldest value still retained, not at the slot for an
		// arbitrarily stale cursor. This avoids skipping retained cells when
		// the ring has wrapped several times before this subscriber resumes.
		if retained := uint64(len(s.b.ring)); s.seen > retained {
			oldest := s.seen - retained
			if s.cursor < oldest {
				s.dropped += oldest - s.cursor
				s.cursor = oldest
			}
		}

		c := s.b.ring[s.cursor&s.b.mask].Load()
		if c == nil || c.seq < s.cursor {
			panic(ring55HaikuInvariantViolation)
		}
		if c.seq == s.cursor {
			s.cursor++
			return c.v, true
		}

		// A publisher stores the immutable cell before advancing tail. If
		// this slot contains an as-yet unpublished cell, report no value yet;
		// the subsequent tail store and wake will make it visible.
		tail := s.b.tail.Load()
		if c.seq >= tail {
			if tail > s.seen {
				s.seen = tail
			}
			return zero, false
		}
		if tail > s.seen {
			s.seen = tail
		}
		oldest := uint64(0)
		if n := uint64(len(s.b.ring)); s.seen > n {
			oldest = s.seen - n
		}
		if s.cursor < oldest {
			s.dropped += oldest - s.cursor
			s.cursor = oldest
			continue
		}
		// The cursor is within the retained interval, so its slot cannot
		// legitimately contain a newer committed cell.
		panic(ring55HaikuInvariantViolation)
	}
}

// TryRecvLatest conflates: it skips the backlog and returns the most recently
// published message not yet received. Skipped messages are conflated, not
// counted as dropped.
func (s *RingSub55Haiku[V]) TryRecvLatest() (V, bool) {
	s.checkInitialized()
	var zero V
	t := s.b.tail.Load()
	if s.cursor >= t {
		return zero, false
	}
	s.cursor = t - 1
	return s.TryRecv()
}

// Recv blocks until a message is available, the context is done, or the
// caster is closed and drained. A nil context is treated as Background.
func (s *RingSub55Haiku[V]) Recv(ctx context.Context) (V, error) {
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		wake := s.Arm()
		// Re-check after arming. This is the lost-wakeup barrier.
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		if s.Ended() {
			return zero, ErrRingClosed55Haiku
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}

// Arm returns a channel that is closed by the next Publish or Close after the
// call. Call [RingSub55Haiku.TryRecv] again after Arm, before waiting on the channel.
// Waiting without that re-check can miss a publish.
func (s *RingSub55Haiku[V]) Arm() <-chan struct{} {
	s.checkInitialized()
	return s.b.arm()
}

// Ended reports whether the caster is closed and this subscriber has consumed
// every message it retains. The closed flag is loaded before the tail, so a
// true result is never premature.
func (s *RingSub55Haiku[V]) Ended() bool {
	s.checkInitialized()
	if !s.b.closed.Load() || s.cursor < s.b.tail.Load() {
		return false
	}
	// Close may have raced with a late Arm after its own wake pass. Flush any
	// such generation so a selector cannot retain an unclosed wake channel.
	s.b.wakeArmed()
	return true
}

// Lag returns the number of published messages this subscriber has not yet
// consumed or skipped.
func (s *RingSub55Haiku[V]) Lag() uint64 {
	s.checkInitialized()
	if t := s.b.tail.Load(); t > s.cursor {
		return t - s.cursor
	}
	return 0
}

// Dropped returns the total number of messages skipped because the requested
// resume point was older than retained history or the publisher later lapped
// the cursor. Intentional skipping by TryRecvLatest is not counted.
func (s *RingSub55Haiku[V]) Dropped() uint64 {
	s.checkInitialized()
	return s.dropped
}

// Seq returns the sequence number of the next message this subscriber will
// receive. Store it to resume later via [RingCaster55Haiku.SubscribeFrom].
func (s *RingSub55Haiku[V]) Seq() uint64 {
	s.checkInitialized()
	return s.cursor
}
