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

// Provenance: 03-chatgpt-1.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

const chanLatestPageSize = 64

// ChanLatest is a bounded, select-friendly broadcast for state-like values.
//
// Each subscription owns a one-element channel. Send publishes to every
// current subscription without waiting for a receiver. If a subscription's
// slot is already full, Send replaces the pending value with the new value.
// Consequently, a slow subscriber observes the latest value, not necessarily
// every value sent.
//
// The implementation uses 64-slot slabs and bitmaps. Registry mutations are
// O(1), Send is O(N) in active subscriptions, and empty slabs are omitted
// from the Send traversal. Slabs are reused across churn and retained up to
// the high-water mark until Trim or Close. Send performs no heap allocations,
// and no goroutine is created per subscription. Unsubscribe may wait for an
// in-progress Send sweep before it can close the mailbox safely.
//
// The zero value is ready for use. A ChanLatest must not be copied after first
// use. Close prevents future subscriptions and sends, and closes all current
// subscription channels. A buffered value remains readable before the
// channel reports closure.
//
// ChanLatest is intended for coalescible state, invalidations, progress, and
// similar notifications. It is not an event log: use ChanCaster or ChanPubSub
// when every individual send must be received.
type ChanLatest[V any] struct {
	// sendMu serializes publishers. Besides preserving a single global order
	// across all subscribers, this ensures there is only one producer touching
	// any particular one-element mailbox at a time.
	sendMu sync.Mutex

	// mu protects the slab lists, masks, slots, and closed. Send holds RLock
	// for the complete sweep, so slots cannot be removed, reused, or closed
	// while the publisher is accessing them.
	mu sync.RWMutex

	activeHead *chanLatestPage[V]
	activeTail *chanLatestPage[V]
	freeHead   *chanLatestPage[V]
	freeTail   *chanLatestPage[V]
	allHead    *chanLatestPage[V]
	allTail    *chanLatestPage[V]

	count  atomic.Int64
	closed bool
}

// ChanLatestSubscription is one independent, select-ready mailbox belonging
// to a ChanLatest. Its channel has capacity one and is receive-only to callers.
//
// A subscription must be released with Unsubscribe when it is no longer
// needed. Unsubscribe is idempotent. The channel is closed when the
// subscription is removed, or when its parent ChanLatest is closed.
type ChanLatestSubscription[V any] struct {
	owner *ChanLatest[V]
	page  *chanLatestPage[V] // protected by owner.mu
	slot  uint8
	ch    chan V
}

// chanLatestPage is both a slab of mailbox pointers and a node in three
// intrusive lists: active pages, pages with at least one free slot, and all
// allocated pages. The active and free lists make both fan-out and churn
// proportional to live work rather than historical subscriber count.
type chanLatestPage[V any] struct {
	activeMask uint64
	freeMask   uint64
	subs       [chanLatestPageSize]*ChanLatestSubscription[V]

	activePrev *chanLatestPage[V]
	activeNext *chanLatestPage[V]
	freePrev   *chanLatestPage[V]
	freeNext   *chanLatestPage[V]
	allPrev    *chanLatestPage[V]
	allNext    *chanLatestPage[V]
}

// NewChanLatest constructs an empty ChanLatest. The zero value is also ready
// for use; this constructor exists for consistency and type-inference
// convenience.
func NewChanLatest[V any]() *ChanLatest[V] {
	return new(ChanLatest[V])
}

// Subscribe adds a new subscription and returns its one-element receive
// channel. It returns nil after the ChanLatest has been closed.
//
// A subscription added concurrently with Send participates in that Send if
// it is linked before the publisher acquires the registry read lock; otherwise
// it starts with the next Send.
func (x *ChanLatest[V]) Subscribe() *ChanLatestSubscription[V] {
	if x == nil {
		return nil
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return nil
	}

	p := x.freeHead
	if p == nil {
		p = new(chanLatestPage[V])
		p.freeMask = ^uint64(0)
		x.linkAll(p)
		x.linkFree(p)
	}

	// A free page always has at least one bit set.
	slot := uint8(bits.TrailingZeros64(p.freeMask))
	bit := uint64(1) << slot
	p.freeMask &^= bit
	if p.freeMask == 0 {
		x.unlinkFree(p)
	}

	s := &ChanLatestSubscription[V]{
		owner: x,
		page:  p,
		slot:  slot,
		ch:    make(chan V, 1),
	}
	p.subs[slot] = s
	if p.activeMask == 0 {
		x.linkActive(p)
	}
	p.activeMask |= bit
	x.count.Add(1)
	return s
}

// C returns the subscription's receive-only channel. It returns nil for a
// nil subscription.
func (s *ChanLatestSubscription[V]) C() <-chan V {
	if s == nil {
		return nil
	}
	return s.ch
}

// Unsubscribe removes the subscription and closes its channel. It returns
// true only for the call that actually removed an active subscription.
//
// It waits for an in-progress Send sweep to finish, so after Unsubscribe
// returns no later Send can publish to this subscription. Any value already
// buffered is still readable before the closed-channel result.
func (s *ChanLatestSubscription[V]) Unsubscribe() bool {
	if s == nil || s.owner == nil {
		return false
	}

	x := s.owner
	x.mu.Lock()
	defer x.mu.Unlock()

	p := s.page
	if p == nil {
		return false
	}
	slot := s.slot
	if p.subs[slot] != s {
		// Includes repeated calls and stale handles after slot reuse.
		return false
	}

	bit := uint64(1) << slot
	p.subs[slot] = nil
	s.page = nil // stale handles no longer pin a slab
	p.activeMask &^= bit
	if p.activeMask == 0 {
		x.unlinkActive(p)
	}

	wasFull := p.freeMask == 0
	p.freeMask |= bit
	if wasFull {
		x.linkFree(p)
	}

	x.count.Add(-1)
	close(s.ch)
	return true
}

// Send publishes value to each subscription that is active for this sweep.
// It returns the number of subscriptions updated.
//
// Publishing never waits for a subscriber to receive. Each mailbox retains at
// most one pending value: when full, the old pending value is discarded and
// replaced. Concurrent Send calls are serialized, so all subscribers observe
// a consistent ordering of the values that they actually receive.
//
// Send may briefly wait for another Send, Subscribe, Unsubscribe, or Close to
// release its relevant lock. It does not wait for subscriber activity.
func (x *ChanLatest[V]) Send(value V) (updated int) {
	if x == nil || x.count.Load() == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.RLock()
	defer x.mu.RUnlock()
	if x.closed {
		return 0
	}

	for p := x.activeHead; p != nil; p = p.activeNext {
		mask := p.activeMask
		for mask != 0 {
			slot := uint8(bits.TrailingZeros64(mask))
			s := p.subs[slot]
			publishLatest(s.ch, value)
			updated++
			mask &= mask - 1
		}
	}
	return updated
}

// publishLatest replaces a pending value when the one-element mailbox is
// full. Its caller guarantees exclusive producer access and prevents channel
// closure for the duration of the operation. Receivers may race freely.
func publishLatest[V any](ch chan V, value V) {
	select {
	case ch <- value:
	default:
		select {
		case <-ch:
		default:
		}
		// The channel is empty here: there is one producer, and receivers can
		// only make additional room. The channel cannot be closed by the caller
		// while Send holds mu.RLock.
		ch <- value
	}
}

// Subscribers returns the current number of active subscriptions.
func (x *ChanLatest[V]) Subscribers() int {
	if x == nil {
		return 0
	}
	return int(x.count.Load())
}

// Trim detaches completely empty slabs, allowing memory usage to fall below
// its historical high-water mark. It returns the number of slabs detached.
//
// Trim is O(P), where P is the number of allocated slabs, and should normally
// be called at a maintenance point rather than on every unsubscribe. Slabs
// containing any active subscription are retained. Removed subscription
// handles no longer retain their slab and remain safe to use after Trim.
func (x *ChanLatest[V]) Trim() (trimmed int) {
	if x == nil {
		return 0
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	for p := x.allHead; p != nil; {
		next := p.allNext
		if p.activeMask == 0 {
			x.unlinkFree(p)
			x.unlinkAll(p)
			trimmed++
		}
		p = next
	}
	return trimmed
}

// Close prevents future subscriptions and sends, and closes every active
// subscription channel. It is idempotent. Any already-buffered value remains
// readable before each channel reports closure. All slab lists are detached.
func (x *ChanLatest[V]) Close() {
	if x == nil {
		return
	}

	// Use the same lock order as Send. Once sendMu is held, no publisher can
	// be in the middle of a mailbox update while a channel is being closed.
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return
	}
	x.closed = true

	for p := x.allHead; p != nil; {
		next := p.allNext
		mask := p.activeMask
		for mask != 0 {
			slot := uint8(bits.TrailingZeros64(mask))
			if s := p.subs[slot]; s != nil {
				close(s.ch)
				s.page = nil
				p.subs[slot] = nil
			}
			mask &= mask - 1
		}
		p.activeMask = 0
		p.freeMask = 0
		p.activePrev = nil
		p.activeNext = nil
		p.freePrev = nil
		p.freeNext = nil
		p.allPrev = nil
		p.allNext = nil
		p = next
	}

	x.activeHead = nil
	x.activeTail = nil
	x.freeHead = nil
	x.freeTail = nil
	x.allHead = nil
	x.allTail = nil
	x.count.Store(0)
}

func (x *ChanLatest[V]) linkActive(p *chanLatestPage[V]) {
	p.activePrev = x.activeTail
	p.activeNext = nil
	if x.activeTail == nil {
		x.activeHead = p
	} else {
		x.activeTail.activeNext = p
	}
	x.activeTail = p
}

func (x *ChanLatest[V]) unlinkActive(p *chanLatestPage[V]) {
	if p.activePrev == nil {
		x.activeHead = p.activeNext
	} else {
		p.activePrev.activeNext = p.activeNext
	}
	if p.activeNext == nil {
		x.activeTail = p.activePrev
	} else {
		p.activeNext.activePrev = p.activePrev
	}
	p.activePrev = nil
	p.activeNext = nil
}

func (x *ChanLatest[V]) linkFree(p *chanLatestPage[V]) {
	p.freePrev = x.freeTail
	p.freeNext = nil
	if x.freeTail == nil {
		x.freeHead = p
	} else {
		x.freeTail.freeNext = p
	}
	x.freeTail = p
}

func (x *ChanLatest[V]) unlinkFree(p *chanLatestPage[V]) {
	if p.freePrev == nil {
		x.freeHead = p.freeNext
	} else {
		p.freePrev.freeNext = p.freeNext
	}
	if p.freeNext == nil {
		x.freeTail = p.freePrev
	} else {
		p.freeNext.freePrev = p.freePrev
	}
	p.freePrev = nil
	p.freeNext = nil
}

func (x *ChanLatest[V]) linkAll(p *chanLatestPage[V]) {
	p.allPrev = x.allTail
	p.allNext = nil
	if x.allTail == nil {
		x.allHead = p
	} else {
		x.allTail.allNext = p
	}
	x.allTail = p
}

func (x *ChanLatest[V]) unlinkAll(p *chanLatestPage[V]) {
	if p.allPrev == nil {
		x.allHead = p.allNext
	} else {
		p.allPrev.allNext = p.allNext
	}
	if p.allNext == nil {
		x.allTail = p.allPrev
	} else {
		p.allNext.allPrev = p.allPrev
	}
	p.allPrev = nil
	p.allNext = nil
}
