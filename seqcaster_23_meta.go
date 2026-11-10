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

// Provenance: 23-meta-ai.md (response #23, Meta AI).
//
// The original response is a *sketch* in which SeqCaster is only partially
// written down: the ring is described, the lock-free node list is described,
// the sharded fan-out workers are described, but the concrete pieces needed to
// compile and run it are absent or incoherent. Repairs made while integrating
// (all in the spirit of the described design):
//
//  1. The node list is a real lock-free stack (atomic.Pointer head, CAS push,
//     lazy unlink by dropping dead nodes during a scan). Nodes are never freed,
//     which is what the original "lazy reclamation" comment implies.
//  2. Each sharded worker owns its own coalescing wake channel. The original
//     had a single shared workerSig, which would wake exactly one worker and
//     leave the others idle forever — a lost-wakeup defect that voids the
//     fan-out design entirely.
//  3. The subscriber ring read takes ringMu in shared mode. The original says
//     "ringMu-free read ... for strict safety caller uses RingPow2 large",
//     which is an acknowledged data race on V; the design keeps the slot write
//     under ringMu, so a shared reader is the smallest repair that preserves
//     the mechanism (one shared ring, subscribers read by cursor).
//  4. Close waits for the workers, so a closed caster has no goroutine leak.
//  5. The loss accounting uses the true retention bound (values older than
//     seq+2-size are unreadable, because sequence seq+1 is being written into
//     the slot of seq+1-size), rather than the sketch's optimistic bound.

import (
	"context"
	"sync"
	"sync/atomic"
)

// SeqCaster23Meta is a shared-ring broadcast with per-subscriber cursors.
//
// A publication stores the value once in a power-of-two ring, publishes the
// sequence with a single atomic store, then wakes the sharded fan-out workers.
// Each subscriber holds a cursor and reads the shared ring; its only private
// state is a capacity-1 coalescing wake channel, so a busy publisher cannot
// grow per-subscriber memory.
//
// Properties:
//   - Publish is O(shards) with zero per-publication allocations, regardless of
//     the subscriber count.
//   - Subscribe/Unsubscribe are O(1) and never block the publisher.
//   - Subscribers that fall more than (size-1) values behind lose values, but
//     the loss is counted and reported ([SeqSub23Meta.Lost]).
//   - Every subscriber has its own select-able wake channel, so there is no
//     shared-channel stealing and no Add accounting.
type SeqCaster23Meta[V any] struct {
	ring   []V
	mask   uint64
	size   uint64
	seq    atomic.Uint64 // last published sequence; 0 means "nothing published"
	list   atomic.Pointer[seqNode23Meta[V]]
	shards int

	workerSig []chan struct{} // one coalescing wake channel per worker
	closed    atomic.Bool
	closeCh   chan struct{}
	workers   sync.WaitGroup

	ringMu sync.RWMutex // serialises ring writes against cursor reads
}

// seqNode23Meta is one subscriber's control block.
type seqNode23Meta[V any] struct {
	next atomic.Pointer[seqNode23Meta[V]]
	dead atomic.Bool
	sig  chan struct{} // capacity 1, coalescing wake-up only

	// next is also read by workers; head and lost are owner-only. Typed
	// atomics keep these 8-byte aligned on 32-bit architectures.
	head atomic.Uint64
	lost atomic.Uint64
}

// SeqSub23Meta is a subscription handle on a [SeqCaster23Meta]. A single handle
// must be driven by one goroutine at a time.
type SeqSub23Meta[V any] struct {
	n *seqNode23Meta[V]
	x *SeqCaster23Meta[V]
}

// NewSeqCaster23Meta constructs a caster with a ring of 1<<ringPow2 values
// (clamped to [16, 1<<20]) served by the given number of fan-out workers
// (default 4). The workers are started here and stopped by [SeqCaster23Meta.Close].
func NewSeqCaster23Meta[V any](ringPow2 int, shards int) *SeqCaster23Meta[V] {
	if ringPow2 < 4 {
		ringPow2 = 4
	}
	if ringPow2 > 20 {
		ringPow2 = 20
	}
	if shards <= 0 {
		shards = 4
	}
	n := 1 << uint(ringPow2)
	x := &SeqCaster23Meta[V]{
		ring:      make([]V, n),
		mask:      uint64(n - 1),
		size:      uint64(n),
		shards:    shards,
		workerSig: make([]chan struct{}, shards),
		closeCh:   make(chan struct{}),
	}
	for i := range x.workerSig {
		x.workerSig[i] = make(chan struct{}, 1)
		x.workers.Add(1)
		go x.fanout(i)
	}
	return x
}

// Subscribe returns a handle positioned at the next publication. No per-message
// allocation occurs for the lifetime of the subscription.
func (x *SeqCaster23Meta[V]) Subscribe() *SeqSub23Meta[V] {
	n := &seqNode23Meta[V]{sig: make(chan struct{}, 1)}
	n.head.Store(x.seq.Load() + 1)
	for {
		old := x.list.Load()
		n.next.Store(old)
		if x.list.CompareAndSwap(old, n) {
			break
		}
	}
	return &SeqSub23Meta[V]{n: n, x: x}
}

// Signal returns the select-able wake channel. It becomes readable when a
// publication this subscriber has not yet consumed exists, and is coalescing:
// a pending wake-up is never duplicated.
func (s *SeqSub23Meta[V]) Signal() <-chan struct{} { return s.n.sig }

// C is an alias for [SeqSub23Meta.Signal].
func (s *SeqSub23Meta[V]) C() <-chan struct{} { return s.n.sig }

// Unsubscribe detaches the subscription. It is idempotent and never blocks.
func (s *SeqSub23Meta[V]) Unsubscribe() {
	if s.n.dead.CompareAndSwap(false, true) {
		select {
		case s.n.sig <- struct{}{}:
		default:
		}
	}
}

// Lost reports how many values this subscription missed because it fell more
// than a ring behind.
func (s *SeqSub23Meta[V]) Lost() uint64 { return s.n.lost.Load() }

// Send publishes v, returning the sequence it was assigned. It never blocks on
// subscribers and performs no heap allocation.
func (x *SeqCaster23Meta[V]) Send(v V) uint64 {
	if x.closed.Load() {
		panic("bigbuff: seqcaster: send on closed")
	}
	x.ringMu.Lock()
	seq := x.seq.Load() + 1
	x.ring[seq&x.mask] = v
	x.seq.Store(seq)
	x.ringMu.Unlock()
	for _, sig := range x.workerSig {
		select {
		case sig <- struct{}{}:
		default:
		}
	}
	return seq
}

// Seq returns the most recently published sequence (0 when nothing has been
// published).
func (x *SeqCaster23Meta[V]) Seq() uint64 { return x.seq.Load() }

// fanout wakes the subscribers assigned to this worker's shard whenever a
// publication is outstanding.
func (x *SeqCaster23Meta[V]) fanout(id int) {
	defer x.workers.Done()
	var last uint64
	for {
		select {
		case <-x.closeCh:
			return
		case <-x.workerSig[id]:
		}
		for {
			cur := x.seq.Load()
			if cur == last {
				break
			}
			x.deliver(id)
			last = cur
			if x.seq.Load() == cur {
				break
			}
		}
	}
}

// deliver scans the lock-free list, waking every live node owned by shard.
func (x *SeqCaster23Meta[V]) deliver(shard int) {
	idx := 0
	for p := x.list.Load(); p != nil; p = p.next.Load() {
		if idx%x.shards == shard && !p.dead.Load() {
			select {
			case p.sig <- struct{}{}:
			default:
			}
		}
		idx++
	}
}

// Next returns the next value for this subscription. ok is false when the
// subscription has been unsubscribed or ctx is done. lost is the number of
// values skipped by this call.
//
// Usage pattern:
//
//	for {
//		v, ok, lost := sub.Next(ctx)
//		if !ok { return }
//		_ = lost
//		handle(v)
//	}
func (s *SeqSub23Meta[V]) Next(ctx context.Context) (v V, ok bool, lost uint64) {
	n := s.n
	x := s.x
	for {
		if n.dead.Load() {
			return v, false, 0
		}
		cur := x.seq.Load()
		if n.head.Load() <= cur {
			x.ringMu.RLock()
			// Re-read under the read lock: the publisher may have advanced and
			// overwritten the slot we are about to read.
			cur = x.seq.Load()
			head := n.head.Load()
			oldest := uint64(1) // sequence 1 is the oldest that can exist
			if cur+2 > x.size {
				oldest = cur + 2 - x.size
			}
			if head < oldest {
				skipped := oldest - head
				n.lost.Store(n.lost.Load() + skipped)
				lost = skipped
				head = oldest
				n.head.Store(head)
			}
			if head <= cur {
				v = x.ring[head&x.mask]
				n.head.Store(head + 1)
				x.ringMu.RUnlock()
				return v, true, lost
			}
			x.ringMu.RUnlock()
			continue
		}
		select {
		case <-ctx.Done():
			return v, false, lost
		case <-n.sig:
		}
	}
}

// Close stops the fan-out workers and wakes every parked subscriber. It is
// idempotent; publishing afterwards panics.
func (x *SeqCaster23Meta[V]) Close() {
	if !x.closed.CompareAndSwap(false, true) {
		return
	}
	close(x.closeCh)
	for p := x.list.Load(); p != nil; p = p.next.Load() {
		p.dead.Store(true)
		select {
		case p.sig <- struct{}{}:
		default:
		}
	}
	x.workers.Wait()
}

// SubscribeContext yields values delivered after the subscription is created
// until ctx is done, the caster is closed, or yield returns false. Lost values
// are silently skipped (use [SeqSub23Meta.Next] to observe loss counters).
func (x *SeqCaster23Meta[V]) SubscribeContext(ctx context.Context) func(yield func(V) bool) {
	sub := x.Subscribe()
	return func(yield func(V) bool) {
		defer sub.Unsubscribe()
		for {
			v, ok, _ := sub.Next(ctx)
			if !ok || !yield(v) {
				return
			}
		}
	}
}
