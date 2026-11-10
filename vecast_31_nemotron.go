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
//
// Repairs made in the completion phase, after a dedicated review (the first
// integration ran the shared harness only, which subscribes from one
// goroutine and never grows a shard):
//
//  7. Subscribe, Unsubscribe and shard growth are serialised per shard by a
//     mutex. The "lock-free" slot management did not claim a slot before
//     writing to it: two concurrent Subscribe calls took the same slot and
//     the first subscriber silently never received anything (measured: 15 of
//     200 runs with 8 concurrent subscribers). The free list was a Treiber
//     stack without ABA protection whose nodes pointed into a vector that
//     growth then replaced, and growth copied slots while they were being
//     written. Send still takes its snapshot without any lock.
//  8. Every subscription carries a "gone" channel that Unsubscribe closes,
//     and the slow-receiver phase selects on it, so a subscriber that
//     withdraws while a Send is waiting for it releases that Send without
//     being counted. Before, Unsubscribe drained the channel "to empty it
//     for the pool": if a Send was already waiting, the drain took the value
//     and the Send counted a handoff nobody received (measured: Send
//     returned 2 with one receiver); if the Send reached the withdrawn
//     subscriber after the drain, it stayed blocked until Close (the first
//     integration recorded that as an exposure limitation and kept the
//     churn facets away from it).
//  9. Subscriber channels are no longer recycled through a pool. A sender
//     can still hold a withdrawn subscriber's channel, so handing that
//     channel to the next subscriber let one Send reach it twice, or reach
//     it with a value published before it subscribed.
//  10. Send and Close exclude each other (a read/write lock around the whole
//     Send). Close used to close subscriber channels a concurrent Send had
//     already loaded, which is a send on a closed channel. Close now does
//     what its comment always said: releases blocked senders, waits for
//     in-flight sends, then closes the channels.
//  11. Shard affinity mixes the stack address before reducing it. Taken
//     modulo 64 directly, the address has the same low bits in every
//     goroutine (stacks are aligned far more coarsely), so every subscriber
//     landed in one shard.
//
// The feature list and diagram below are the response's. Subscribe
// allocates (the handle and two channels), and a Send allocates whenever a
// subscriber is not ready (the slow list); the tests record both.
// "Lock-free subscription management" and the channel pool no longer exist
// (repairs 7 and 9).

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
	vecastShards    = 64
	vecastShardMask = vecastShards - 1

	// Initial vector capacity per shard (power of 2)
	vecastInitialCap = 256

	// Maximum vector capacity per shard (1M slots per shard = 64M total)
	vecastMaxCap = 1 << 20

	// Epoch reclamation: slot safe to reuse after 2 global epochs
	vecastReclamationEpochs = 2
)

// Vecast is an epoch-sharded vectored broadcaster.
type Vecast[V any] struct {
	// Shard array - each shard manages a vector of subscriber slots
	shards [vecastShards]vecastShard[V]

	// Global epoch: even = stable, odd = Close in progress
	globalEpoch atomic.Uint64

	closed   atomic.Bool
	closedCh chan struct{}

	// sendMu is held shared by every Send for its whole duration, and
	// exclusively by Close while it closes subscriber channels (repair 10).
	sendMu sync.RWMutex
}

// vecastShard manages a growable vector of subscriber slots.
type vecastShard[V any] struct {
	// Vector pointer (atomic: Send reads it without the lock)
	vec atomic.Pointer[vector[V]]

	// Shard-local epoch (odd = mutating, even = stable)
	localEpoch atomic.Uint64

	// mu serialises Subscribe, Unsubscribe, growth and Close's sweep within
	// the shard (repair 7). free and fresh are guarded by it.
	mu    sync.Mutex
	free  *vecastSlot[V] // slots released by Unsubscribe, most recent first
	fresh uint32         // slots [fresh:] of the current vector were never used

	// Padding to avoid false sharing (cache line = 64 bytes)
	_ [64 - 40]byte // vec(8) + localEpoch(8) + mu(8) + free(8) + fresh(4+4) = 40
}

// vector is an immutable-length array of slots; growth replaces it.
type vector[V any] struct {
	slots []vecastSlot[V]
	mask  uint32 // len - 1 (power of 2)
	gen   uint64 // generation, incremented by each growth
}

// vecastRec is one live subscription as a sender sees it.
type vecastRec[V any] struct {
	ch   chan V
	gone chan struct{} // closed by Unsubscribe (repair 8)
}

// vecastSlot is one subscriber position within a shard's vector.
type vecastSlot[V any] struct {
	// rec is nil when the slot is empty and non-nil while a subscriber is
	// registered. It is an atomic pointer because senders scan the vector
	// concurrently with subscription churn.
	rec   atomic.Pointer[vecastRec[V]]
	epoch uint64         // registration global epoch; written under the shard lock
	idx   uint32         // position of this slot in its vector
	next  *vecastSlot[V] // free list linkage, guarded by the shard lock
}

// VecastSubscription is a handle to an active subscription.
type VecastSubscription[V any] struct {
	ch    chan V
	shard *vecastShard[V]
	idx   uint32
	epoch uint64
	rec   *vecastRec[V] // nil once unsubscribed; guarded by the shard lock
}

// NewVecast creates a new Vecast broadcaster.
func NewVecast[V any]() *Vecast[V] {
	v := &Vecast[V]{closedCh: make(chan struct{})}
	for i := range v.shards {
		v.shards[i].init()
	}
	return v
}

func newVecastVector[V any](capacity int, gen uint64) *vector[V] {
	vec := &vector[V]{
		slots: make([]vecastSlot[V], capacity),
		mask:  uint32(capacity - 1),
		gen:   gen,
	}
	for i := range vec.slots {
		vec.slots[i].idx = uint32(i)
	}
	return vec
}

func (s *vecastShard[V]) init() {
	s.vec.Store(newVecastVector[V](vecastInitialCap, 1))
	// The shard epoch is odd while a mutation is in flight and even once the
	// shard is stable. Starting at 0 keeps the very first mutation's +1 from
	// leaving the shard permanently odd (which would spin acquireSnapshot
	// forever); the mutation itself toggles the epoch twice.
	s.localEpoch.Store(0)
}

// shardFor returns the shard for a given hash.
func (v *Vecast[V]) shardFor(hash uint64) *vecastShard[V] {
	return &v.shards[hash&vecastShardMask]
}

// Subscribe adds a new subscriber and returns a subscription handle. The
// subscriber's channel is unbuffered: Send hands each value to it directly.
// It panics if the broadcaster is closed.
func (v *Vecast[V]) Subscribe() *VecastSubscription[V] {
	sh := v.shardFor(vecastShardKey())
	sh.mu.Lock()
	defer sh.mu.Unlock()
	// Checked under the shard lock, which Close's sweep also takes: a
	// subscription is either refused here or swept (its channel closed).
	if v.closed.Load() {
		panic("vecast: subscribe on closed broadcaster")
	}

	rec := &vecastRec[V]{ch: make(chan V), gone: make(chan struct{})}
	epoch := v.globalEpoch.Load()

	sh.localEpoch.Add(1) // mutation in progress (odd)
	sl := sh.claimSlot()
	sl.epoch = epoch
	sl.rec.Store(rec)
	sh.localEpoch.Add(1) // stable (even)

	return &VecastSubscription[V]{
		ch:    rec.ch,
		shard: sh,
		idx:   sl.idx,
		epoch: epoch,
		rec:   rec,
	}
}

// claimSlot returns an empty slot of the current vector, growing the vector
// if it is full. The shard lock must be held.
func (sh *vecastShard[V]) claimSlot() *vecastSlot[V] {
	if sl := sh.free; sl != nil {
		sh.free, sl.next = sl.next, nil
		return sl
	}
	vec := sh.vec.Load()
	if int(sh.fresh) == len(vec.slots) {
		// Full, and nothing on the free list, so no free-list node can be
		// left pointing into the vector being replaced.
		newCap := len(vec.slots) * 2
		if newCap > vecastMaxCap {
			panic("vecast: shard capacity exhausted")
		}
		grown := newVecastVector[V](newCap, vec.gen+1)
		for i := range vec.slots {
			grown.slots[i].epoch = vec.slots[i].epoch
			grown.slots[i].rec.Store(vec.slots[i].rec.Load())
		}
		sh.vec.Store(grown)
		vec = grown
	}
	sl := &vec.slots[sh.fresh]
	sh.fresh++
	return sl
}

// C returns the subscriber's channel for use in select statements. Close
// closes it; Unsubscribe does not.
func (s *VecastSubscription[V]) C() <-chan V {
	return s.ch
}

// Unsubscribe cancels the subscription and releases any Send that is waiting
// to hand a value to it. It is idempotent and safe to call concurrently, and
// after Close.
func (v *Vecast[V]) Unsubscribe(sub *VecastSubscription[V]) {
	if sub == nil || sub.shard == nil {
		return
	}
	sh := sub.shard
	sh.mu.Lock()
	defer sh.mu.Unlock()
	rec := sub.rec
	if rec == nil {
		return // already unsubscribed
	}
	sub.rec = nil
	sl := &sh.vec.Load().slots[sub.idx]
	if sl.rec.Load() != rec {
		return // Close swept the slot first
	}
	sh.localEpoch.Add(1) // mutation in progress (odd)
	sl.rec.Store(nil)
	sl.next, sh.free = sh.free, sl
	sh.localEpoch.Add(1) // stable (even)
	// A sender that loaded rec before the slot was cleared is released here
	// instead of waiting for a receive that will never come (repair 8).
	close(rec.gone)
}

// Send broadcasts value to all subscribers.
// It returns the number of subscribers the value was handed to.
//
// Phase 1 offers the value to every subscriber without blocking. Phase 2
// then waits, in turn, for each subscriber that was not ready, until it
// receives, unsubscribes, or the broadcaster is closed.
func (v *Vecast[V]) Send(value V) int {
	v.sendMu.RLock()
	defer v.sendMu.RUnlock()
	if v.closed.Load() {
		return 0
	}

	snapshot := v.acquireSnapshot()
	if snapshot.totalSubs == 0 {
		return 0
	}

	var slow []*vecastRec[V]
	delivered := v.fastFanOut(&snapshot, value, &slow)
	if len(slow) != 0 {
		delivered += v.waitForSlowReceivers(slow, value)
	}
	return delivered
}

// snapshot is a consistent view of all shards.
type snapshot[V any] struct {
	shards      [vecastShards]vecastShardSnapshot[V]
	totalSubs   int
	globalEpoch uint64
}

type vecastShardSnapshot[V any] struct {
	vec   *vector[V]
	slots []vecastSlot[V] // direct slice reference for fast iteration
	count int
	epoch uint64 // shard local epoch at snapshot time
}

// acquireSnapshot takes a consistent snapshot of all shards without locking:
// it re-reads every shard epoch after the scan and retries if any changed.
func (v *Vecast[V]) acquireSnapshot() snapshot[V] {
	var snap snapshot[V]

	for {
		ge := v.globalEpoch.Load()
		if ge&1 != 0 {
			runtime.Gosched()
			continue
		}
		snap.globalEpoch = ge

		total := 0
		for i := range v.shards {
			sh := &v.shards[i]
			le := sh.localEpoch.Load()
			vec := sh.vec.Load()
			count := 0
			if le&1 == 0 {
				for j := range vec.slots {
					if vec.slots[j].rec.Load() != nil {
						count++
					}
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

		ge2 := v.globalEpoch.Load()
		if ge2 == ge {
			consistent := true
			for i := range v.shards {
				le := snap.shards[i].epoch
				if le&1 != 0 || v.shards[i].localEpoch.Load() != le {
					consistent = false
					break
				}
			}
			if consistent {
				return snap
			}
		}
		runtime.Gosched()
	}
}

// fastFanOut performs the non-blocking first phase. Subscribers that were
// not ready to receive are appended to slow for the second phase.
func (v *Vecast[V]) fastFanOut(snap *snapshot[V], value V, slow *[]*vecastRec[V]) int {
	delivered := 0
	for i := range snap.shards {
		ss := &snap.shards[i]
		if ss.count == 0 {
			continue
		}
		for j := range ss.slots {
			rec := ss.slots[j].rec.Load()
			if rec == nil {
				continue
			}
			select {
			case rec.ch <- value:
				delivered++
			default:
				*slow = append(*slow, rec)
			}
		}
	}
	return delivered
}

// waitForSlowReceivers is the second phase: a blocking handoff to each
// subscriber that was not ready in the first. It counts only handoffs that
// happened. A subscriber that unsubscribes is skipped; Close ends the phase.
func (v *Vecast[V]) waitForSlowReceivers(slow []*vecastRec[V], value V) int {
	delivered := 0
	for _, rec := range slow {
		select {
		case rec.ch <- value:
			delivered++
		case <-rec.gone:
		case <-v.closedCh:
			return delivered
		}
	}
	return delivered
}

// Close closes the broadcaster and all subscriber channels.
// No further sends are delivered and Subscribe panics afterwards.
// It releases senders blocked in the slow-receiver phase, waits for all
// in-flight sends to return, and is idempotent.
func (v *Vecast[V]) Close() error {
	if !v.closed.CompareAndSwap(false, true) {
		return nil // already closed
	}
	close(v.closedCh) // releases senders blocked in phase 2

	// No Send is in flight past this point, and none can start: each checks
	// closed after taking the shared lock. Closing the subscriber channels
	// is therefore safe (repair 10).
	v.sendMu.Lock()
	defer v.sendMu.Unlock()

	v.globalEpoch.Add(1)
	for i := range v.shards {
		sh := &v.shards[i]
		sh.mu.Lock()
		vec := sh.vec.Load()
		for j := range vec.slots {
			if rec := vec.slots[j].rec.Swap(nil); rec != nil {
				close(rec.ch)
			}
		}
		sh.mu.Unlock()
	}
	v.globalEpoch.Add(1)
	return nil
}

// Len returns the approximate number of active subscribers.
func (v *Vecast[V]) Len() int {
	total := 0
	for i := range v.shards {
		vec := v.shards[i].vec.Load()
		for j := range vec.slots {
			if vec.slots[j].rec.Load() != nil {
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

// vecastShardKey derives a cheap identity for shard affinity.
//
// The original used runtime.goid() through //go:linkname, which does not exist
// in the runtime; the honest equivalent that does not reach into runtime
// internals is the address of a fresh stack variable, which differs between
// goroutines. The address is mixed before use (repair 11): its low bits are
// alignment and are the same in every goroutine.
func vecastShardKey() uint64 {
	var x byte
	return (uint64(uintptr(unsafe.Pointer(&x))) * 0x9E3779B97F4A7C15) >> 40
}
