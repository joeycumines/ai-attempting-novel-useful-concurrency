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

const (
	ringFanoutClosedBit uint32 = 1 << 31
	ringFanoutRefsMask  uint32 = ringFanoutClosedBit - 1

	ringReaderClosedBit uint64 = 1 << 63
	ringReaderRefsMask  uint64 = ringReaderClosedBit - 1
)

var (
	ErrRingFanoutCapacity = errors.New("bigbuff: ringfanout: capacity must be positive")
	ErrRingFanoutClosed   = errors.New("bigbuff: ringfanout: closed")
)

// RingFanout is a bounded overwrite-oldest broadcast log. A publication is
// stored once in a shared ring; each subscriber has a one-slot Ready channel
// carrying coalesced notifications, not payloads. A slow reader can lose
// overwritten entries; TryNext and Next report the number missed. Publish
// never waits for readers and has zero steady-state heap allocations.
//
// This is complementary to Fanout: use Fanout for exact direct channel
// delivery, and RingFanout when shared storage, bounded memory, and nonblocking
// publishers matter more than guaranteed retention for arbitrarily slow
// subscribers. The zero value is not usable; use NewRingFanout. Do not copy
// after first use.
type RingFanout[T any] struct {
	mu        sync.Mutex // protects closed and intrusive list edits
	publishMu sync.Mutex // serializes slot writes, sequence assignment, signals

	head  atomic.Pointer[ringFanoutNode[T]]
	count atomic.Int64

	published atomic.Uint64
	slots     []ringFanoutSlot[T]

	// readerState gates slot reads during Close. Its high bit closes admission;
	// low bits count readers that already hold a slot-access lease.
	readerState atomic.Uint64
	readerDone  chan struct{}

	closed    bool // protected by mu
	closeOnce sync.Once
}

type ringFanoutSlot[T any] struct {
	mu    sync.RWMutex
	seq   uint64
	value T
}

type ringFanoutNode[T any] struct {
	hub   *RingFanout[T]
	ready chan struct{} // capacity one; notifications coalesce

	next atomic.Pointer[ringFanoutNode[T]]
	prev *ringFanoutNode[T] // protected by hub.mu

	// Closed bit gates new leases; low bits count concurrent notification
	// sends and cursor reads. A count is needed because these can overlap.
	state atomic.Uint32

	doneMu   sync.Mutex
	done     chan struct{}
	finished bool // protected by doneMu
	linked   bool // protected by hub.mu
}

// RingFanoutSubscription is a cursor into a RingFanout. Ready is an edge-notification
// channel, not a value channel. After it fires, drain TryNext until it returns
// ok=false. Next combines cursor reads with a context-aware wait.
//
// TryNext and Next serialize access to the cursor and may be called by multiple
// goroutines. Concurrent Next waiters are supported: a waiter re-signals when
// unread values remain. Do not mix Next waiters with independent consumers of
// Ready; choose one receive protocol per subscription.
type RingFanoutSubscription[T any] struct {
	node atomic.Pointer[ringFanoutNode[T]]
	hub  *RingFanout[T]

	ready <-chan struct{}

	readMu sync.Mutex
	cursor uint64

	closeOnce sync.Once
}

// NewRingFanout allocates a ring with exactly capacity slots. Capacity must be
// positive. The slot array never grows.
func NewRingFanout[T any](capacity int) (*RingFanout[T], error) {
	if capacity < 1 {
		return nil, ErrRingFanoutCapacity
	}
	return &RingFanout[T]{slots: make([]ringFanoutSlot[T], capacity), readerDone: make(chan struct{})}, nil
}

func (f *RingFanout[T]) Capacity() int        { return len(f.slots) }
func (f *RingFanout[T]) Published() uint64    { return f.published.Load() }
func (f *RingFanout[T]) SubscriberCount() int { return int(f.count.Load()) }

// Subscribe starts at the current tail and does not replay existing entries.
func (f *RingFanout[T]) Subscribe() (*RingFanoutSubscription[T], error) {
	return f.subscribe(false)
}

// SubscribeReplay starts at the oldest entry currently retained, up to
// Capacity entries.
func (f *RingFanout[T]) SubscribeReplay() (*RingFanoutSubscription[T], error) {
	return f.subscribe(true)
}

func (f *RingFanout[T]) subscribe(replay bool) (*RingFanoutSubscription[T], error) {
	if len(f.slots) == 0 {
		return nil, ErrRingFanoutCapacity
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrRingFanoutClosed
	}

	published := f.published.Load()
	cursor := published
	if replay {
		cursor = ringFanoutOldest(published, uint64(len(f.slots))) - 1
	}
	n := &ringFanoutNode[T]{hub: f, ready: make(chan struct{}, 1), linked: true}
	s := &RingFanoutSubscription[T]{hub: f, ready: n.ready, cursor: cursor}
	s.node.Store(n)

	oldHead := f.head.Load()
	n.next.Store(oldHead)
	if oldHead != nil {
		oldHead.prev = n
	}
	f.head.Store(n)
	f.count.Add(1)
	if cursor < published {
		n.ready <- struct{}{}
	}
	return s, nil
}

// Ready returns a coalesced receive-only signal channel. It is safe to use in
// select. The channel is closed when the subscription or hub closes.
func (s *RingFanoutSubscription[T]) Ready() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ready
}

// TryNext returns the next retained value, the number of entries skipped since
// the previous read, and whether a value was available. A closed subscription
// returns ok=false.
func (s *RingFanoutSubscription[T]) TryNext() (value T, missed uint64, ok bool) {
	if s == nil {
		return value, 0, false
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.hub == nil {
		return value, 0, false
	}
	n := s.node.Load()
	if n == nil || !n.acquire() {
		return value, 0, false
	}
	defer n.release()

	f := s.hub
	if !f.acquireReader() {
		return value, 0, false
	}
	defer f.releaseReader()
	capacity := uint64(len(f.slots))
	published := f.published.Load()
	target := s.cursor + 1
	if target == 0 || target > published {
		return value, 0, false
	}
	oldest := ringFanoutOldest(published, capacity)
	if target < oldest {
		missed = oldest - target
		target = oldest
	}

	for {
		slot := &f.slots[(target-1)%capacity]
		slot.mu.RLock()
		slotSeq := slot.seq
		if slotSeq == target {
			value = slot.value
			slot.mu.RUnlock()
			s.cursor = target
			return value, missed, true
		}
		slot.mu.RUnlock()

		// A greater slot sequence means an overwrite raced our published
		// snapshot. Recompute the retention window from the newer sequence.
		if slotSeq < target {
			panic("bigbuff: ringfanout: slot sequence regressed")
		}
		latest := f.published.Load()
		if latest < slotSeq {
			latest = slotSeq // includes a slot write before published.Store
		}
		newOldest := ringFanoutOldest(latest, capacity)
		if target < newOldest {
			missed += newOldest - target
			target = newOldest
		}
	}
}

// signalPending hands another coalesced wake-up to a concurrent Next waiter if
// there is still unread data after this caller consumed one item.
func (s *RingFanoutSubscription[T]) signalPending() {
	s.readMu.Lock()
	if s.hub == nil {
		s.readMu.Unlock()
		return
	}
	pending := s.cursor < s.hub.published.Load()
	n := s.node.Load()
	s.readMu.Unlock()
	if pending && n != nil {
		n.signal()
	}
}

// Next waits for a retained value or context cancellation. It returns ok=false
// if the subscription closes. missed reports overwritten entries. Concurrent
// Next waiters are supported; do not also consume Ready directly while using
// Next on the same subscription. A nil context panics.
func (s *RingFanoutSubscription[T]) Next(ctx context.Context) (value T, missed uint64, ok bool, err error) {
	if ctx == nil {
		panic("bigbuff: ringfanout: nil context")
	}
	if s == nil {
		return value, 0, false, nil
	}
	for {
		if value, missed, ok = s.TryNext(); ok {
			s.signalPending()
			return value, missed, true, nil
		}
		n := s.node.Load()
		if n == nil || n.state.Load()&ringFanoutClosedBit != 0 {
			return value, 0, false, nil
		}
		select {
		case _, open := <-s.ready:
			if !open {
				return value, 0, false, nil
			}
		case <-ctx.Done():
			return value, 0, false, ctx.Err()
		}
	}
}

// Close removes this subscription, closes Ready after active readers and
// notifiers release their leases, and drops its reference to the hub. It is
// idempotent; all concurrent callers wait for completion.
func (s *RingFanoutSubscription[T]) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		n := s.node.Swap(nil)
		if n == nil {
			return
		}
		n.deactivate()
		n.hub.unlink(n)
		n.waitFinished()
		s.readMu.Lock()
		s.hub = nil
		s.readMu.Unlock()
	})
}

// Publish stores value once, assigns a monotonically increasing sequence, and
// non-blockingly signals each linked subscription. A full Ready channel
// already represents unread work, so notifications coalesce. It never waits
// for a reader and performs no per-publication allocation.
func (f *RingFanout[T]) Publish(value T) (uint64, error) {
	if len(f.slots) == 0 {
		return 0, ErrRingFanoutCapacity
	}
	f.publishMu.Lock()
	defer f.publishMu.Unlock()

	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return f.published.Load(), ErrRingFanoutClosed
	}
	seq := f.published.Load() + 1
	if seq == 0 {
		panic("bigbuff: ringfanout: sequence overflow")
	}
	slot := &f.slots[(seq-1)%uint64(len(f.slots))]
	slot.mu.Lock()
	slot.value = value
	slot.seq = seq
	slot.mu.Unlock()
	f.published.Store(seq)

	f.mu.Lock()
	head := f.head.Load()
	f.mu.Unlock()
	for n := head; n != nil; {
		next := n.next.Load()
		n.signal()
		n = next
	}
	return seq, nil
}

// Close rejects new publications/subscriptions, lets the admitted publication
// finish, closes all Ready channels, and unlinks the entire membership chain.
func (f *RingFanout[T]) Close() {
	f.closeOnce.Do(func() {
		if f.readerDone == nil { // zero value: no storage or subscriptions exist
			f.mu.Lock()
			f.closed = true
			f.mu.Unlock()
			return
		}
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()

		f.publishMu.Lock()

		// No new publications can pass the closed check. Close reader admission
		// and wait for any cursor operation already copying a slot before
		// clearing payload references from the fixed ring.
		f.closeReaders()
		var zero T
		for i := range f.slots {
			slot := &f.slots[i]
			slot.mu.Lock()
			slot.value = zero
			slot.seq = 0
			slot.mu.Unlock()
		}

		f.mu.Lock()
		head := f.head.Load()
		f.head.Store(nil)
		f.count.Store(0)
		for n := head; n != nil; n = n.next.Load() {
			n.prev = nil
			n.linked = false
			n.deactivate()
		}
		f.mu.Unlock()

		for n := head; n != nil; {
			n.waitFinished()
			next := n.next.Load()
			n.next.Store(nil)
			n = next
		}
		f.publishMu.Unlock()
	})
}

func (f *RingFanout[T]) unlink(n *ringFanoutNode[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !n.linked {
		return
	}
	prev, next := n.prev, n.next.Load()
	if prev == nil {
		f.head.Store(next)
	} else {
		prev.next.Store(next)
	}
	if next != nil {
		next.prev = prev
	}
	n.prev = nil
	n.linked = false
	f.count.Add(-1)
}

func (f *RingFanout[T]) acquireReader() bool {
	for {
		state := f.readerState.Load()
		if state&ringReaderClosedBit != 0 {
			return false
		}
		if state&ringReaderRefsMask == ringReaderRefsMask {
			panic("bigbuff: ringfanout: reader lease counter overflow")
		}
		if f.readerState.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (f *RingFanout[T]) releaseReader() {
	if state := f.readerState.Add(^uint64(0)); state == ringReaderClosedBit {
		close(f.readerDone)
	}
}

func (f *RingFanout[T]) closeReaders() {
	for {
		state := f.readerState.Load()
		if state&ringReaderClosedBit != 0 {
			break
		}
		if f.readerState.CompareAndSwap(state, state|ringReaderClosedBit) {
			if state&ringReaderRefsMask == 0 {
				close(f.readerDone)
			}
			break
		}
	}
	<-f.readerDone
}

func (n *ringFanoutNode[T]) acquire() bool {
	for {
		state := n.state.Load()
		if state&ringFanoutClosedBit != 0 {
			return false
		}
		if state&ringFanoutRefsMask == ringFanoutRefsMask {
			panic("bigbuff: ringfanout: lease counter overflow")
		}
		if n.state.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (n *ringFanoutNode[T]) release() {
	if state := n.state.Add(^uint32(0)); state == ringFanoutClosedBit {
		n.finish()
	}
}

func (n *ringFanoutNode[T]) deactivate() {
	for {
		state := n.state.Load()
		if state&ringFanoutClosedBit != 0 {
			return
		}
		if n.state.CompareAndSwap(state, state|ringFanoutClosedBit) {
			if state&ringFanoutRefsMask == 0 {
				n.finish()
			}
			return
		}
	}
}

func (n *ringFanoutNode[T]) signal() {
	if !n.acquire() {
		return
	}
	if len(n.ready) == 0 {
		select {
		case n.ready <- struct{}{}:
		default:
			// Another notification/receiver won the single token slot.
		}
	}
	n.release()
}

func (n *ringFanoutNode[T]) finish() {
	close(n.ready)
	n.doneMu.Lock()
	n.finished = true
	if n.done != nil {
		close(n.done)
	}
	n.doneMu.Unlock()
}

func (n *ringFanoutNode[T]) waitFinished() {
	n.doneMu.Lock()
	if n.finished {
		n.doneMu.Unlock()
		return
	}
	done := n.done
	if done == nil {
		done = make(chan struct{})
		n.done = done
	}
	n.doneMu.Unlock()
	<-done
}

func ringFanoutOldest(published, capacity uint64) uint64 {
	if published < capacity {
		return 1
	}
	return published - capacity + 1
}
