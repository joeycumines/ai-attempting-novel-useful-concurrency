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

// Provenance: 13-openrouter-crouton-alpha-chat.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrEpochCasterClosed is returned by receive operations after the caster
// has been closed and all published values have been drained.
var ErrEpochCasterClosed = errors.New("bigbuff: epochcaster: closed")

const (
	// epochBlockSize is the maximum number of subscriptions in one block.
	epochBlockSize = 64
	// epochRelayThreshold is the block population at which a relay
	// goroutine is started. Relays stop only when the block empties
	// (hysteresis), avoiding start/stop flapping under churn.
	epochRelayThreshold = 8
	epochMinRingSize    = 16
	epochMaxRingSize    = 4096
	// epochDefaultRingTargetBytes bounds the default ring payload memory.
	epochDefaultRingTargetBytes = 64 << 10
)

// EpochCaster13Crouton broadcasts each value to all subscribers registered at the
// time of publication, without callbacks and without a global barrier.
//
// It is select-friendly: a subscriber exposes a per-subscriber wake
// channel suitable for use in a select statement (see EpochSub13Crouton.Wake).
// Unlike a shared channel, a wake token is a hint, not a value: one token
// may stand for several epochs, so a subscriber MUST drain all available
// values after each wake (see EpochSub13Crouton.TryRecv).
//
// The payload is written once into a ring of epoch slots and read by each
// subscriber; the publisher never copies the value per subscriber and
// never waits for subscribers except when the ring wraps. Delivery to a
// subscriber is guaranteed for every epoch published while it is
// registered, provided it keeps receiving and eventually unsubscribes.
type EpochCaster13Crouton[V any] struct {
	slots     []epochSlot13Crouton[V]
	mask      uint64
	published atomic.Uint64
	closed    atomic.Bool
	// subscribers is mutated only under joinMu (write lock).
	subscribers atomic.Int64

	// sendMu serialises publishers.
	sendMu sync.Mutex
	// joinMu makes "snapshot subscriber count + publish epoch" atomic
	// with respect to Subscribe/Unsubscribe. It is held only briefly:
	// never during slot waits, payload writes, or wakeups.
	joinMu sync.RWMutex

	blocksMu   sync.Mutex
	blocksHead atomic.Pointer[epochBlock13Crouton[V]]
}

// epochSlot13Crouton is one ring slot. remaining counts subscribers that have not
// yet consumed the epoch currently occupying the slot; the publisher may
// reuse the slot only once it reaches zero.
type epochSlot13Crouton[V any] struct {
	val       V
	remaining atomic.Int64
}

// epochBlock13Crouton packs up to epochBlockSize subscriptions. Membership slots
// are fixed atomic pointers, so joining/leaving needs no allocation and
// waking needs no lock.
type epochBlock13Crouton[V any] struct {
	wake  chan struct{} // cap 1; token received by the relay goroutine
	mu    sync.Mutex    // guards count and relayDone
	slots [epochBlockSize]atomic.Pointer[EpochSub13Crouton[V]]
	count int // guarded by mu
	relay atomic.Bool
	// relayDone is closed to stop the relay; guarded by mu.
	relayDone chan struct{}
	// next is immutable once the block is linked into the caster list.
	next atomic.Pointer[epochBlock13Crouton[V]]
}

// EpochSub13Crouton is a single subscription. It must be owned by one goroutine at
// a time, and must not be reused after Unsubscribe.
type EpochSub13Crouton[V any] struct {
	caster *EpochCaster13Crouton[V]
	block  *epochBlock13Crouton[V]
	slot   int
	wake   chan struct{} // cap 1
	// next is the next epoch to deliver. Written under joinMu at
	// Subscribe time, then owned by the subscriber goroutine.
	next uint64
	// left is set by Unsubscribe (owner goroutine only).
	left bool
}

func epochNextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func epochPrevPow2(n int) int {
	p := 1
	for p*2 <= n {
		p <<= 1
	}
	return p
}

// epochDefaultRingSize sizes the ring from sizeof(V) so that huge values
// do not imply a huge ring, while small values get generous pipelining.
func epochDefaultRingSize[V any]() int {
	var zero V
	sz := int(unsafe.Sizeof(zero))
	if sz < 8 {
		sz = 8
	}
	n := epochDefaultRingTargetBytes / (sz + 16) // +16: slot overhead
	if n < epochMinRingSize {
		n = epochMinRingSize
	}
	if n > epochMaxRingSize {
		n = epochMaxRingSize
	}
	return epochPrevPow2(n)
}

// NewEpochCaster13Crouton constructs an EpochCaster13Crouton with a ring size chosen from
// sizeof(V) (see epochDefaultRingSize).
func NewEpochCaster13Crouton[V any]() *EpochCaster13Crouton[V] {
	return NewEpochCasterWithRingSize13Crouton[V](0)
}

// NewEpochCasterWithRingSize13Crouton constructs an EpochCaster13Crouton with the given
// ring size (rounded up to a power of two; <= 0 selects the default).
// The ring size bounds how many epochs a subscriber may lag before the
// publisher applies backpressure, and costs ringSize * sizeof(V) bytes.
func NewEpochCasterWithRingSize13Crouton[V any](ringSize int) *EpochCaster13Crouton[V] {
	if ringSize <= 0 {
		ringSize = epochDefaultRingSize[V]()
	} else {
		ringSize = epochNextPow2(ringSize)
		if ringSize > 1<<20 {
			panic("bigbuff: epochcaster: ring size too large")
		}
	}
	return &EpochCaster13Crouton[V]{
		slots: make([]epochSlot13Crouton[V], ringSize),
		mask:  uint64(ringSize - 1),
	}
}

// Send publishes value to all currently registered subscribers and
// returns its epoch (starting at 1), or 0 if there were no subscribers.
// It panics if the caster is closed.
//
// Send does not wait for subscribers to receive, except when the ring is
// about to wrap: then it waits until the epoch occupying the target slot
// has been consumed (or accounted for by unsubscription) by everyone.
func (x *EpochCaster13Crouton[V]) Send(value V) uint64 {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	if x.closed.Load() {
		panic("bigbuff: epochcaster: send on closed caster")
	}
	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}

	epoch := x.published.Load() + 1
	slot := &x.slots[epoch&x.mask]

	// Wait (outside joinMu, so lagging subscribers can unsubscribe and
	// free the slot) until the previous occupant of this slot is done.
	if epoch > uint64(len(x.slots)) {
		for slot.remaining.Load() != 0 {
			runtime.Gosched()
		}
	}

	slot.val = value

	// Snapshot + publish: atomic w.r.t. Subscribe/Unsubscribe, so every
	// counted subscriber will consume this epoch exactly once, and no
	// uncounted subscriber will.
	x.joinMu.RLock()
	n := x.subscribers.Load()
	if n == 0 {
		x.joinMu.RUnlock()
		return 0
	}
	slot.remaining.Store(n)
	x.published.Store(epoch)
	x.joinMu.RUnlock()

	x.wakeAll()
	return epoch
}

// wakeAll hints every block. Small blocks are woken directly; blocks with
// a relay get a single token that the relay fans out in parallel.
// All sends are non-blocking: tokens coalesce in cap-1 channels.
func (x *EpochCaster13Crouton[V]) wakeAll() {
	for b := x.blocksHead.Load(); b != nil; b = b.next.Load() {
		if b.relay.Load() {
			select {
			case b.wake <- struct{}{}:
			default:
			}
			continue
		}
		for i := range b.slots {
			sub := b.slots[i].Load()
			if sub == nil {
				continue
			}
			select {
			case sub.wake <- struct{}{}:
			default:
			}
		}
	}
}

// Close closes the caster. Subscribers drain all published values, then
// observe closure. Close is idempotent.
func (x *EpochCaster13Crouton[V]) Close() {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	if x.closed.Load() {
		return
	}
	x.closed.Store(true)
	x.wakeAll()
}

// Len returns the current number of subscribers.
func (x *EpochCaster13Crouton[V]) Len() int {
	return int(x.subscribers.Load())
}

// Published returns the most recently published epoch (0 if none).
func (x *EpochCaster13Crouton[V]) Published() uint64 {
	return x.published.Load()
}

// RingSize returns the number of epoch slots in the ring.
func (x *EpochCaster13Crouton[V]) RingSize() int {
	return len(x.slots)
}

// Subscribe registers and returns a new subscription. The subscription's
// first delivered epoch is the next epoch published after registration.
// Subscribing to a closed caster returns a subscription that immediately
// reports closure.
func (x *EpochCaster13Crouton[V]) Subscribe() *EpochSub13Crouton[V] {
	sub := &EpochSub13Crouton[V]{caster: x, wake: make(chan struct{}, 1)}

	x.joinMu.Lock()
	defer x.joinMu.Unlock()

	sub.next = x.published.Load() + 1
	if x.closed.Load() {
		sub.left = true // detached; receives report closure
		return sub
	}
	sub.block, sub.slot = x.addToBlockLocked(sub)
	x.subscribers.Add(1)
	return sub
}

// addToBlockLocked places sub in the first block with a free slot,
// creating a block if needed. Callers must hold joinMu; it acquires
// blocksMu then the block mutex (the global lock order).
func (x *EpochCaster13Crouton[V]) addToBlockLocked(sub *EpochSub13Crouton[V]) (*epochBlock13Crouton[V], int) {
	x.blocksMu.Lock()
	defer x.blocksMu.Unlock()

	for b := x.blocksHead.Load(); b != nil; b = b.next.Load() {
		b.mu.Lock()
		if b.count >= epochBlockSize {
			b.mu.Unlock()
			continue
		}
		slot := 0
		for slot < epochBlockSize && b.slots[slot].Load() != nil {
			slot++
		}
		if slot == epochBlockSize {
			b.mu.Unlock()
			continue // defensive; count and slots cannot disagree
		}
		b.slots[slot].Store(sub)
		b.count++
		if b.count == epochRelayThreshold {
			done := make(chan struct{})
			b.relayDone = done
			b.relay.Store(true)
			go b.relayLoop(done)
		}
		b.mu.Unlock()
		return b, slot
	}

	b := &epochBlock13Crouton[V]{wake: make(chan struct{}, 1)}
	b.slots[0].Store(sub)
	b.count = 1
	b.next.Store(x.blocksHead.Load())
	x.blocksHead.Store(b)
	return b, 0
}

// relayLoop fans block wake tokens out to member mailboxes. Duplicate or
// stale tokens are harmless: subscribers re-check the epoch.
func (b *epochBlock13Crouton[V]) relayLoop(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-b.wake:
			for i := range b.slots {
				sub := b.slots[i].Load()
				if sub == nil {
					continue
				}
				select {
				case sub.wake <- struct{}{}:
				default:
				}
			}
		}
	}
}

// Wake returns the subscription's wake channel, for use in a select
// statement. Receiving from it is only a hint: after it fires, the caller
// MUST drain every available value via TryRecv before blocking again,
// because one token may represent several epochs.
func (s *EpochSub13Crouton[V]) Wake() <-chan struct{} {
	return s.wake
}

// NextEpoch returns the epoch that will be delivered next.
func (s *EpochSub13Crouton[V]) NextEpoch() uint64 {
	return s.next
}

// TryRecv returns the next value if one is available, without blocking.
// The results are (value, ok, closed): ok reports a delivered value;
// closed reports that the caster is closed (or the subscription has been
// unsubscribed) and no values remain.
func (s *EpochSub13Crouton[V]) TryRecv() (V, bool, bool) {
	var zero V
	if s.left {
		return zero, false, true
	}
	x := s.caster
	next := s.next
	if next > x.published.Load() {
		if x.closed.Load() {
			return zero, false, true
		}
		return zero, false, false
	}
	slot := &x.slots[next&x.mask]
	v := slot.val // copy out before releasing the slot
	slot.remaining.Add(-1)
	s.next = next + 1
	return v, true, false
}

// Recv blocks until the next value is available or the caster is closed.
// The boolean result is false on closure (after draining).
func (s *EpochSub13Crouton[V]) Recv() (V, bool) {
	for {
		v, ok, closed := s.TryRecv()
		if ok {
			return v, true
		}
		if closed {
			var zero V
			return zero, false
		}
		<-s.wake
	}
}

// RecvContext is Recv with context cancellation. It returns
// ErrEpochCasterClosed after draining a closed caster.
func (s *EpochSub13Crouton[V]) RecvContext(ctx context.Context) (V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		v, ok, closed := s.TryRecv()
		if ok {
			return v, nil
		}
		if closed {
			var zero V
			return zero, ErrEpochCasterClosed
		}
		select {
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		case <-s.wake:
		}
	}
}

// Unsubscribe withdraws the subscription, accounting for every epoch it
// was counted for but had not consumed, so the publisher is never stuck
// waiting for it. Unsubscribe is idempotent. The subscription must not
// be used afterwards.
func (s *EpochSub13Crouton[V]) Unsubscribe() {
	if s.left {
		return
	}
	s.left = true
	x := s.caster
	if s.block == nil {
		return // subscription to a closed caster; never registered
	}

	x.joinMu.Lock()
	// While joinMu is held, no publisher can publish, so the set of owed
	// epochs is stable.
	published := x.published.Load()
	for epoch := s.next; epoch <= published; epoch++ {
		x.slots[epoch&x.mask].remaining.Add(-1)
	}
	s.next = published + 1
	x.subscribers.Add(-1)

	b := s.block
	b.mu.Lock()
	b.slots[s.slot].Store(nil)
	b.count--
	if b.count == 0 {
		b.relay.Store(false)
		if b.relayDone != nil {
			close(b.relayDone)
			b.relayDone = nil
		}
	}
	b.mu.Unlock()
	x.joinMu.Unlock()
}

// SubscribeContext subscribes immediately and returns a single-use
// iterator yielding every value until the context is cancelled, the
// iterator stops early, or the caster is closed; it unsubscribes on exit.
// Behavior is undefined if the iterator is used more than once.
//
// WARNING: the iterator must be used immediately, or the context must be
// promptly cancelled, to avoid leaking the subscription.
func (x *EpochCaster13Crouton[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				sub.Unsubscribe()
			}
			panic("bigbuff: epochcaster: iterator yield func is nil")
		}
		if !stop() {
			return // context cancelled, or iterator used more than once
		}
		defer sub.Unsubscribe()
		for {
			v, err := sub.RecvContext(ctx)
			if err != nil {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
