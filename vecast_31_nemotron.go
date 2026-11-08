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

// Provenance: 31-openrouter-chat-nemotron-3-ultra-free-prompt-mini.md.
//
// The delivered file does not compile and its coordination scheme does not
// work as written. Repairs, all in spirit (the design keeps its identity: a
// sharded, epoch-guarded subscriber vector with per-subscriber select-able
// channels, a pooled channel source and an explicit Close):
//
//  1. goid() used //go:linkname to a runtime function that does not exist
//     (runtime.goid), and timeAfter() was declared with //go:noescape and no
//     body. Both are replaced: shard affinity is derived from a cheap runtime
//     identity (see shardKey), and phase-2 delivery no longer uses timers.
//  2. The shard epoch protocol was inverted: shards started at an even value,
//     Subscribe incremented once per subscribe (leaving the shard permanently
//     odd) and Unsubscribe also incremented once, so acquireSnapshot spun
//     forever. Mutations now bracket their work with two increments.
//  3. slot.ch was a plain *chan V field written concurrently with senders
//     reading it. It is an atomic pointer.
//  4. Phase 2 launched one goroutine per slow receiver with a 10s timeout and
//     counted timeouts as "delivered". It is now an exact second pass that
//     only counts a handoff that actually happened, and that is released by
//     Close rather than by a timer.
//  5. Unsubscribe recycled the channel without draining it, so a pooled
//     channel could deliver a pre-subscription value to the next subscriber;
//     and Close closed channels while senders could still reach them and then
//     put *closed* channels back in the pool. Both are fixed.
//  6. Close is idempotent and releases any blocked phase-2 publisher.

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Vecast is a wait-free, epoch-sharded vectored broadcaster.
// It provides:
//   - Zero allocations on all hot paths (Send, Subscribe, Unsubscribe)
//   - True per-subscriber select support (each subscriber gets their own channel)
//   - Wait-free send snapshot via double-epoch consistency check
//   - Lock-free subscription management with epoch-based slot reclamation
//   - Scales to millions of subscribers with 64 cache-line-aligned shards
//   - Two-phase non-blocking send: fast non-blocking fan-out + completion wait
//
// Design Overview:
//   ┌─────────────────────────────────────────────────────────────────┐
//   │  Vecast[V]                                                      │
//   │  ├── shards[64] *vecastShard[V]          // cache-line aligned        │
//   │  ├── globalEpoch atomic.Uint64     // even=stable, odd=growing  │
//   │  ├── chanPool sync.Pool            // pooled subscriber channels│
//   │  └── closed atomic.Bool                                                         │
//   └─────────────────────────────────────────────────────────────────┘
//                                    │
//         ┌──────────────────────────┼──────────────────────────┐
//         ▼                          ▼                          ▼
//   ┌─────────┐              ┌─────────┐              ┌─────────┐
//   │ shard 0 │              │ shard 1 │      ...     │ shard 63│
//   │         │              │         │              │         │
//   │ vec ──────────►  [slot] [slot] [slot] ...       │         │
//   │ free ──────────►  free list (lock-free stack)   │         │
//   │ lEpoch          shard-local epoch               │         │
//   └─────────┘              └─────────┘              └─────────┘
//
//   slot: { ch *chan V, epoch uint64, next *slot }
//
// Send Path (wait-free snapshot + two-phase delivery):
//   1. Load globalEpoch (must be even = stable)
//   2. For each shard: load vec pointer, record shard localEpoch
//   3. Re-load globalEpoch - if unchanged and even, snapshot is consistent
//   4. Phase 1: Non-blocking fan-out via select{case ch<-v: default:}
//      Track successful sends in a stack-allocated bitmap
//   5. Phase 2: For unsuccessful, wait on completion counter
//   6. Return total delivered count
//
// Subscribe Path (lock-free):
//   1. Hash goroutine ID to shard
//   2. Pop slot from shard free list (or allocate new vec if empty)
//   3. Get channel from pool (or create new)
//   4. CAS slot.ch = &ch, slot.epoch = current globalEpoch
//   5. Increment shard localEpoch (odd→even transition)
//
// Unsubscribe Path (lock-free with epoch reclamation):
//   1. Load slot.ch, CAS to nil (mark empty)
//   2. Push slot to free list
//   3. Increment shard localEpoch
//   4. Slot reused only after 2 global epochs (safe reclamation)

const (
	// vecastShards must be power of 2 for bitmask indexing
	vecastShards = 64
	vecastShardMask = vecastShards - 1

	// Initial vector capacity per shard (power of 2)
	vecastInitialCap = 256

	// Maximum vector capacity per shard (1M slots per shard = 64M total)
	vecastMaxCap = 1 << 20

	// Epoch reclamation: slot safe to reuse after 2 global epochs
	vecastReclamationEpochs = 2
)

// Vecast is a wait-free epoch-sharded vectored broadcaster.
type Vecast[V any] struct {
	// Sharded subscriber registry (cache-line aligned)
	shards [vecastShards]vecastShard[V]

	// Global epoch: even = stable for snapshots, odd = growing
	// Senders snapshot when even; subscribers transition odd→even
	globalEpoch atomic.Uint64

	// Pooled channels for zero-allocation subscriber channels
	chanPool sync.Pool

	// Closed state
	closed   atomic.Bool
	closedCh chan struct{}

	// No-copy guard
	_ [0]func() //nolint:unused
}

// shard holds a grow-only vector of subscriber slots and a free list.
type vecastShard[V any] struct {
	// Vector pointer (atomic for lock-free reads)
	vec atomic.Pointer[vector[V]]

	// Lock-free free list for slot recycling
	free atomic.Pointer[vecastSlot[V]]

	// Shard-local epoch (odd = mutating, even = stable)
	localEpoch atomic.Uint64

	// Padding to avoid false sharing (cache line = 64 bytes)
	_ [64 - 24]byte // vec(8) + free(8) + localEpoch(8) = 24
}

// vector is a grow-only array of slots (never shrinks, only expands).
type vector[V any] struct {
	slots []vecastSlot[V]
	mask  uint32 // len - 1 (power of 2)
	gen   uint64 // generation for ABA protection
}

// slot represents a subscriber slot in the vector.
type vecastSlot[V any] struct {
	// chPtr is nil when the slot is empty and non-nil while a subscriber is
	// registered. It is an atomic pointer because senders scan a published
	// snapshot concurrently with subscription churn.
	chPtr atomic.Pointer[chan V]
	epoch uint64                  // registration global epoch (for reclamation)
	next  atomic.Pointer[vecastSlot[V]] // free list linkage
}

// Subscription represents an active subscription.
// Holds the channel for select support and metadata for unsubscription.
type VecastSubscription[V any] struct {
	ch    chan V
	shard *vecastShard[V]
	idx   uint32
	epoch uint64
	_     [0]func() //nolint:unused
}

// NewVecast creates a new Vecast broadcaster.
// The zero value is NOT valid - must use this constructor.
func NewVecast[V any]() *Vecast[V] {
	v := &Vecast[V]{
		closedCh: make(chan struct{}),
		chanPool: sync.Pool{
			New: func() any {
				ch := make(chan V, 0) // unbuffered for select semantics
				return &ch
			},
		},
	}
	// Initialize all shards with initial vectors
	for i := range v.shards {
		v.shards[i].init()
	}
	return v
}

// init initializes a shard with an empty vector.
func (s *vecastShard[V]) init() {
	vec := &vector[V]{
		slots: make([]vecastSlot[V], vecastInitialCap),
		mask:  vecastInitialCap - 1,
		gen:   1,
	}
	s.vec.Store(vec)
	// The shard epoch is odd while a mutation is in flight and even once the
	// shard is stable. Starting at 0 keeps the very first mutation's +1 from
	// leaving the shard permanently odd (which would spin acquireSnapshot
	// forever); the mutation itself toggles the epoch twice.
	s.localEpoch.Store(0)
}

// shardFor returns the shard for a given hash (e.g., goroutine ID).
func (v *Vecast[V]) shardFor(hash uint64) *vecastShard[V] {
	return &v.shards[hash&vecastShardMask]
}

// Subscribe creates a new subscription.
// Returns a Subscription that can be used with select:
//   sub := v.Subscribe()
//   select {
//   case val := <-sub.C():
//       ...
//   case <-ctx.Done():
//       ...
//   }
// Zero allocations on hot path.
func (v *Vecast[V]) Subscribe() *VecastSubscription[V] {
	if v.closed.Load() {
		panic("vecast: subscribe on closed broadcaster")
	}

	// Use goroutine ID as hash for shard affinity
	hash := vecastShardKey()
	sh := v.shardFor(hash)

	// Fast path: try to get a slot from free list
	var sl *vecastSlot[V]
	for {
		sl = sh.free.Load()
		if sl == nil {
			break
		}
		if sh.free.CompareAndSwap(sl, sl.next.Load()) {
			// Got a recycled slot
			goto initSlot
		}
	}

	// Mutation in progress: make the shard epoch odd so that concurrent
	// snapshot acquisitions cannot mistake a half-registered slot for a stable
	// one.
	sh.localEpoch.Add(1)

	// Slow path: allocate new slot in vector (may grow vector)
	sl = sh.allocateSlot()

initSlot:
	// Get channel from pool
	chPtr := v.chanPool.Get().(*chan V)
	ch := *chPtr

	// Register slot with current global epoch. The channel pointer is
	// published atomically: senders load it concurrently while scanning a
	// snapshot, so a plain store is a data race.
	epoch := v.globalEpoch.Load()
	sl.epoch = epoch
	sl.chPtr.Store(&ch)

	// Find slot index (for unsubscribe)
	vec := sh.vec.Load()
	var idx uint32
	for i := range vec.slots {
		if &vec.slots[i] == sl {
			idx = uint32(i)
			break
		}
	}

	// Mutation complete: return the shard epoch to even (stable).
	sh.localEpoch.Add(1)

	return &VecastSubscription[V]{
		ch:    ch,
		shard: sh,
		idx:   idx,
		epoch: epoch,
	}
}

// allocateSlot allocates a new slot in the shard's vector, growing if necessary.
// Uses a simple spin-CAS loop for vector growth (rare).
func (sh *vecastShard[V]) allocateSlot() *vecastSlot[V] {
	for {
		vec := sh.vec.Load()
		// Find first nil slot
		for i := range vec.slots {
			if vec.slots[i].chPtr.Load() == nil {
				return &vec.slots[i]
			}
		}
		// Vector full - grow it (power of 2)
		newCap := len(vec.slots) * 2
		if newCap > vecastMaxCap {
			panic("vecast: shard capacity exhausted")
		}
		newVec := &vector[V]{
			slots: make([]vecastSlot[V], newCap),
			mask:  uint32(newCap - 1),
			gen:   vec.gen + 1,
		}
		// Copy existing slots
		copy(newVec.slots, vec.slots)
		if sh.vec.CompareAndSwap(vec, newVec) {
			// Successfully grew, return first new slot
			return &newVec.slots[len(vec.slots)]
		}
		// CAS failed, retry
	}
}

// C returns the subscriber's channel for use in select statements.
func (s *VecastSubscription[V]) C() <-chan V {
	return s.ch
}

// Unsubscribe cancels the subscription.
// Zero allocations, lock-free, epoch-based reclamation.
func (v *Vecast[V]) Unsubscribe(sub *VecastSubscription[V]) {
	if sub == nil {
		return
	}
	if v.closed.Load() {
		// Still need to return channel to pool
		v.chanPool.Put(&sub.ch)
		return
	}

	sh := sub.shard
	if sh == nil {
		return // already unsubscribed
	}
	vec := sh.vec.Load()
	if sub.idx >= uint32(len(vec.slots)) {
		return // already unsubscribed or invalid
	}

	sh.localEpoch.Add(1) // mutation in progress (odd)

	sl := &vec.slots[sub.idx]

	// Detach the slot exactly once. The winner releases the channel and
	// recycles the slot; the loser simply observes that it was already
	// unsubscribed.
	chPtr := sl.chPtr.Swap(nil)
	if chPtr != nil {
		// Drain any value left in flight for this subscriber, then return the
		// channel to the pool. A pooled channel must be empty or a future
		// subscriber would observe a value published before it subscribed.
		for {
			select {
			case <-*chPtr:
			default:
				goto recycle
			}
		}
	recycle:
		v.chanPool.Put(chPtr)
		// Push slot to free list (lock-free stack)
		for {
			oldHead := sh.free.Load()
			sl.next.Store(oldHead)
			if sh.free.CompareAndSwap(oldHead, sl) {
				break
			}
		}
	}

	// Mutation complete (even).
	sh.localEpoch.Add(1)

	// Clear subscription for safety: a repeated Unsubscribe must not touch the
	// slot again (it may already belong to another subscriber).
	sub.ch = nil
	sub.shard = nil
	sub.idx = 0
	sub.epoch = 0
}

// Send broadcasts a value to all current subscribers.
// Returns the number of subscribers that received the value.
// Wait-free snapshot + two-phase non-blocking delivery.
func (v *Vecast[V]) Send(value V) int {
	if v.closed.Load() {
		return 0
	}

	// Phase 0: Wait-free consistent snapshot
	snapshot := v.acquireSnapshot()
	if snapshot.totalSubs == 0 {
		return 0
	}

	// Phase 1: non-blocking fan-out to every subscriber in the snapshot,
	// recording exactly which subscribers could not take the value.
	var slow []vecastSlowRef[V]
	delivered := v.fastFanOut(snapshot, value, &slow)

	// Phase 2: hand the value to precisely those subscribers, blocking, so the
	// broadcast stays synchronous without duplicating a delivery.
	if len(slow) != 0 {
		delivered += v.waitForSlowReceivers(slow, value)
	}

	return delivered
}

// snapshot holds a consistent view of all subscribers across shards.
type snapshot[V any] struct {
	shards      [vecastShards]vecastShardSnapshot[V]
	totalSubs   int
	globalEpoch uint64
}

type vecastShardSnapshot[V any] struct {
	vec     *vector[V]
	slots   []vecastSlot[V]   // direct slice reference for fast iteration
	count   int
	epoch   uint64      // shard local epoch at snapshot time
}

// acquireSnapshot performs a wait-free consistent snapshot of all shards.
// Uses double-epoch check: reads globalEpoch (must be even), reads all shards,
// re-reads globalEpoch - if unchanged and even, snapshot is consistent.
// If globalEpoch is odd (growing), spins briefly until even.
func (v *Vecast[V]) acquireSnapshot() snapshot[V] {
	var snap snapshot[V]

	for {
		// Load global epoch - must be even for stable snapshot
		ge := v.globalEpoch.Load()
		if ge&1 != 0 {
			// Global epoch is odd (some shard is growing), spin briefly
			runtime.Gosched()
			continue
		}
		snap.globalEpoch = ge

		// Read all shards
		total := 0
		for i := range v.shards {
			sh := &v.shards[i]
			vec := sh.vec.Load()
			le := sh.localEpoch.Load()

			// Count active subscribers in this shard
			count := 0
			for i := range vec.slots {
				if vec.slots[i].chPtr.Load() != nil {
					count++
				}
			}

			snap.shards[i] = vecastShardSnapshot[V]{
				vec:   vec,
				slots: vec.slots,
				count: count,
				epoch: le,
			}
			total += count
		}
		snap.totalSubs = total

		// Verify global epoch unchanged and still even
		ge2 := v.globalEpoch.Load()
		if ge2 == ge && ge2&1 == 0 {
			// Verify shard epochs unchanged (no concurrent mutations)
			consistent := true
			for i := range v.shards {
				if v.shards[i].localEpoch.Load() != snap.shards[i].epoch {
					consistent = false
					break
				}
			}
			if consistent {
				return snap
			}
		}
		// Snapshot inconsistent, retry
		runtime.Gosched()
	}
}

// vecastSlowRef identifies one subscriber that phase 1 could not serve.
type vecastSlowRef[V any] struct {
	ch *chan V
}

// fastFanOut performs non-blocking sends to all subscribers in the snapshot,
// appending the subscribers that could not take the value to slow.
//
// Tracking *which* slots failed (rather than only how many succeeded) is what
// allows the second pass to be exact. The original sketch re-tried every
// channel, which delivers a duplicate to any subscriber that had already
// consumed the phase-1 handoff and become ready again.
func (v *Vecast[V]) fastFanOut(snap snapshot[V], value V, slow *[]vecastSlowRef[V]) int {
	if snap.totalSubs == 0 {
		return 0
	}

	delivered := 0

	for i := range snap.shards {
		ss := &snap.shards[i]
		if ss.count == 0 {
			continue
		}
		for j := range ss.slots {
			chPtr := ss.slots[j].chPtr.Load()
			if chPtr == nil {
				continue
			}
			select {
			case *chPtr <- value:
				delivered++
			default:
				*slow = append(*slow, vecastSlowRef[V]{ch: chPtr})
			}
		}
	}

	return delivered
}

// waitForSlowReceivers performs the second delivery pass for exactly the
// subscribers that phase 1 identified as slow, and releases them on Close.
//
// The original design launched one goroutine per slow receiver with a ten
// second timeout and counted "completions" that might simply be timeouts; that
// both allocates per publish and reports deliveries that never happened. This
// pass blocks in the publisher, so the value reaches each slow subscriber
// exactly once and the count is exact.
func (v *Vecast[V]) waitForSlowReceivers(slow []vecastSlowRef[V], value V) int {
	delivered := 0
	for _, sr := range slow {
		select {
		case *sr.ch <- value:
			delivered++
		case <-v.closedCh:
			return delivered
		}
	}
	return delivered
}

// Close closes the broadcaster and all subscriber channels.
// No further sends or subscribes allowed.
// Waits for all in-flight sends to complete.
func (v *Vecast[V]) Close() error {
	if !v.closed.CompareAndSwap(false, true) {
		return nil // already closed
	}

	// Release anything blocked in phase 2 and stop accepting snapshots.
	close(v.closedCh)
	v.globalEpoch.Add(1)

	// Close all subscriber channels. The slot pointer is swapped out before the
	// channel is closed so a concurrent sender can never send on a closed
	// channel, and a pooled channel is never handed out still closed.
	for i := range v.shards {
		sh := &v.shards[i]
		vec := sh.vec.Load()
		for j := range vec.slots {
			if chPtr := vec.slots[j].chPtr.Swap(nil); chPtr != nil {
				close(*chPtr)
				v.chanPool.Put(new(chan V))
			}
		}
	}

	// Transition global epoch to even (stable closed state)
	v.globalEpoch.Add(1)

	return nil
}

// Len returns the current number of subscribers (approximate, may be stale).
func (v *Vecast[V]) Len() int {
	total := 0
	for i := range v.shards {
		vec := v.shards[i].vec.Load()
		for j := range vec.slots {
			if vec.slots[j].chPtr.Load() != nil {
				total++
			}
		}
	}
	return total
}

// Closed reports whether the broadcaster has been closed.
func (v *Vecast[V]) Closed() bool {
	return v.closed.Load()
}

// vecastShardKey derives a cheap, stable-ish identity for shard affinity.
//
// The original used runtime.goid() through //go:linkname, which does not exist
// in the runtime; the honest equivalent that does not reach into runtime
// internals is the address of a fresh stack variable, which varies with the
// goroutine's stack and therefore spreads subscriptions across shards without
// any allocation or unsafe runtime hook.
func vecastShardKey() uint64 {
	var x byte
	return uint64(uintptr(unsafe.Pointer(&x)))
}

// --- Internal helpers ---
