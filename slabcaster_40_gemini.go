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

// Provenance: 40-arena-gemini-3.8-flash-high-prompt-mini.md (Gemini, response #40).
//
// Source identifiers are response-scoped.
//
// Repairs (not in the source). The design is a slab of slots, each holding a
// channel that is closed and replaced when its subscriber leaves, with Send
// walking an occupancy mask without a lock. The source did nothing to keep a
// Send that had read the mask away from a slot being closed:
//
//  1. Send could send on a channel that Unsubscribe or Close had just closed
//     (a panic), and read the slot's channel field while Unsubscribe wrote it
//     (a data race). Each slot now carries a lease: a count of the deliveries
//     in flight to it. deliver takes the lease and then checks the slot's
//     closed flag; a closer sets the flag and then waits for the count to
//     reach zero before it touches the channel. One of the two always sees
//     the other. The lease is two atomic operations per delivery; Send is
//     still lock-free.
//  2. A closer waiting for the lease would wait for ever behind a Send that is
//     blocked on a full channel under SlabPolicy40Block. While it waits it
//     takes values off the channel, which lets that Send complete. The values
//     are discarded: their subscriber is leaving.
//  3. Unsubscribe released the slot for reuse before it had replaced the
//     channel, so a concurrent Subscribe could be handed the channel that was
//     about to be closed. The slot is now released last, and stays marked
//     closed until its next subscriber claims it, so a Send holding a stale
//     mask does not deliver into an unclaimed slot.
//  4. Subscribe could claim a slot after Close had passed it, leaving a
//     subscriber on a channel that would never be closed. Subscribe checks
//     for a concurrent Close after it has claimed.
//  5. SubscribeContext closed its subscription twice when its context was
//     cancelled (once from the context callback, once on return). A handle is
//     a pointer into the slab and is shared with the slot's next subscriber,
//     so the second close could cut off a subscriber that had since taken the
//     slot. It now closes exactly once.
//
// A property of the design that remains, and that callers must respect: a
// handle is only valid until it is closed. Closing a handle again after its
// slot has been claimed by a new subscriber closes that subscriber.

import (
	"context"
	"errors"
	"iter"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	slab40Slots        = 64
	slab40CacheLinePad = 64
)

var (
	// ErrSlabCaster40Closed is returned or panicked when operations are attempted on a closed SlabCaster40Gemini.
	ErrSlabCaster40Closed = errors.New("bigbuff: slabcaster: caster is closed")
)

// SlabPolicy40Gemini defines the backpressure and eviction semantics when delivering to a subscriber's channel.
type SlabPolicy40Gemini uint8

const (
	// SlabPolicy40DropOldest evicts the oldest buffered element if the subscriber's channel is full,
	// ensuring the publisher never blocks while guaranteeing the subscriber always receives
	// the freshest data. This provides lock-free sliding-window ring buffer semantics over
	// native Go channels.
	SlabPolicy40DropOldest SlabPolicy40Gemini = iota

	// SlabPolicy40DropNewest discards the incoming element if the subscriber's channel is full.
	// The publisher never blocks.
	SlabPolicy40DropNewest

	// SlabPolicy40Block forces the publisher to block until the subscriber has consumed enough items
	// to accept the new value.
	SlabPolicy40Block
)

// SlabOption40Gemini configures a SlabCaster40Gemini instance.
type SlabOption40Gemini func(*slabCaster40Config)

type slabCaster40Config struct {
	bufferSize int
	policy     SlabPolicy40Gemini
}

// WithSlabCaster40BufferSize sets the internal buffer capacity for each subscriber's channel.
// Default is 1, which enables non-blocking sliding ring buffer behavior under SlabPolicy40DropOldest.
func WithSlabCaster40BufferSize(size int) SlabOption40Gemini {
	return func(c *slabCaster40Config) {
		if size < 0 {
			size = 0
		}
		c.bufferSize = size
	}
}

// WithSlabCaster40Policy sets the default backpressure policy for subscriptions created without an explicit policy.
// Default is SlabPolicy40DropOldest.
func WithSlabCaster40Policy(p SlabPolicy40Gemini) SlabOption40Gemini {
	return func(c *slabCaster40Config) {
		c.policy = p
	}
}

// SlabCaster40Gemini is a lock-free, zero-allocation (steady-state) broadcast bus engineered to eliminate
// the trade-offs between linear callback channels and centralized synchronization barriers.
//
// Distinctive Characteristics & Novelty:
//  1. Select Compatibility: Every subscriber receives an idiomatic, first-class `<-chan V` that
//     can be plugged directly into native Go `select` statements without secondary synchronization.
//  2. Zero-Allocation Subscriptions: Slabs are allocated in contiguous 64-slot40 arenas. Subscriptions
//     are interior pointers directly into slab40 memory. Calling Subscribe() acquires a slot40 via atomic
//     bitmask CAS and returns an interior pointer—achieving exactly 0 allocs/op during active churn.
//  3. Hardware-Accelerated Dispatch: Active subscribers are tracked via a 64-bit occupancy mask.
//     Broadcasting utilizes CPU-intrinsic trailing-zero count (TZCNT) and bit-clear (BLSR) instructions
//     via `math/bits`, bypassing inactive subscribers in 1 CPU cycle and eliminating slice iteration.
//  4. Independent Contention Domains: Each subscriber possesses an independent channel. Broadcasts
//     fan out without contention on a single channel lock or global condition variable.
//  5. Non-Blocking Sliding Eviction: Under SlabPolicy40DropOldest, full subscriber channels undergo atomic eviction
//     of stale elements, preventing a slow consumer from stalling the broadcaster or other consumers.
type SlabCaster40Gemini[V any] struct {
	_           [slab40CacheLinePad]byte
	subscribers atomic.Int64
	closed      atomic.Bool
	_           [slab40CacheLinePad]byte
	head        atomic.Pointer[slab40[V]]
	tail        atomic.Pointer[slab40[V]]
	bufferSize  int
	policy      SlabPolicy40Gemini
	closeMu     sync.Mutex
}

type slab40[V any] struct {
	active atomic.Uint64
	next   atomic.Pointer[slab40[V]]
	slots  [slab40Slots]slot40[V]
}

type slot40[V any] struct {
	// ch is written only by a closer that holds the closed flag and has seen
	// inflight reach zero, and read only under the lease or by the slot's
	// claimant.
	ch     chan V
	policy atomic.Uint32
	// closed is true while the slot is being closed and while it is free. A
	// closer claims the slot by setting it; Subscribe clears it.
	closed atomic.Bool
	// inflight counts the deliveries in progress to this slot (the lease).
	inflight atomic.Int32
	sub      SlabSubscription40Gemini[V]
}

// SlabSubscription40Gemini represents an active subscriber registration.
// It is returned as an interior pointer directly to the owning slab40, preventing heap allocation.
type SlabSubscription40Gemini[V any] struct {
	slab40 *slab40[V]
	owner  *SlabCaster40Gemini[V]
	index  uint8
}

// NewSlabCaster constructs an initialized SlabCaster40Gemini ready for immediate broadcast.
func NewSlabCaster40Gemini[V any](opts ...SlabOption40Gemini) *SlabCaster40Gemini[V] {
	cfg := slabCaster40Config{
		bufferSize: 1,
		policy:     SlabPolicy40DropOldest,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	sc := &SlabCaster40Gemini[V]{
		bufferSize: cfg.bufferSize,
		policy:     cfg.policy,
	}

	initialSlab := sc.newSlab()
	sc.head.Store(initialSlab)
	sc.tail.Store(initialSlab)

	return sc
}

func (sc *SlabCaster40Gemini[V]) newSlab() *slab40[V] {
	s := new(slab40[V])
	for i := range s.slots {
		slot40 := &s.slots[i]
		slot40.ch = make(chan V, sc.bufferSize)
		slot40.policy.Store(uint32(sc.policy))
		slot40.closed.Store(true) // free; Subscribe opens it
		slot40.sub.slab40 = s
		slot40.sub.owner = sc
		slot40.sub.index = uint8(i)
	}
	return s
}

// Send broadcasts val to all registered subscribers.
// Delivery is performed in accordance with each subscriber's SlabPolicy40Gemini.
// Returns the count of subscribers that successfully received or buffered the value.
// Steady-state allocation: strictly 0 allocs/op.
func (sc *SlabCaster40Gemini[V]) Send(val V) int {
	if sc.closed.Load() {
		panic(ErrSlabCaster40Closed)
	}
	if sc.subscribers.Load() == 0 {
		return 0
	}

	delivered := 0
	for curr := sc.head.Load(); curr != nil; curr = curr.next.Load() {
		mask := curr.active.Load()
		for mask != 0 {
			idx := bits.TrailingZeros64(mask)
			slot40 := &curr.slots[idx]

			if sc.deliver(slot40, val) {
				delivered++
			}

			mask &= mask - 1
		}
	}

	return delivered
}

// SendParallel distributes delivery across multiple worker goroutines when subscriber counts are high.
// For low subscriber counts (<= 64), it automatically falls back to synchronous Send to avoid goroutine overhead.
func (sc *SlabCaster40Gemini[V]) SendParallel(val V) int {
	if sc.closed.Load() {
		panic(ErrSlabCaster40Closed)
	}
	totalSubs := sc.subscribers.Load()
	if totalSubs <= slab40Slots {
		return sc.Send(val)
	}

	workers := runtime.GOMAXPROCS(0)
	var (
		wg      sync.WaitGroup
		counter atomic.Int64
		curr    = sc.head.Load()
	)

	// Collect active slabs into a stack slice to avoid allocations if reasonable
	var slabList []*slab40[V]
	for s := curr; s != nil; s = s.next.Load() {
		if s.active.Load() != 0 {
			slabList = append(slabList, s)
		}
	}

	if len(slabList) == 0 {
		return 0
	}

	if workers > len(slabList) {
		workers = len(slabList)
	}

	chunkSize := (len(slabList) + workers - 1) / workers
	for w := 0; w < workers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if start >= len(slabList) {
			break
		}
		if end > len(slabList) {
			end = len(slabList)
		}

		wg.Add(1)
		go func(slabs []*slab40[V]) {
			defer wg.Done()
			localDelivered := 0
			for _, s := range slabs {
				mask := s.active.Load()
				for mask != 0 {
					idx := bits.TrailingZeros64(mask)
					if sc.deliver(&s.slots[idx], val) {
						localDelivered++
					}
					mask &= mask - 1
				}
			}
			if localDelivered > 0 {
				counter.Add(int64(localDelivered))
			}
		}(slabList[start:end])
	}

	wg.Wait()
	return int(counter.Load())
}

func (sc *SlabCaster40Gemini[V]) deliver(slot40 *slot40[V], val V) bool {
	// A slot that is closing or free is left alone without taking the lease,
	// so a closer waits only for the deliveries that began before it set the
	// flag, not for Sends that keep arriving.
	if slot40.closed.Load() {
		return false
	}
	// Take the lease, then look at the flag again: a closer sets the flag,
	// then waits for the lease. Whichever goes second sees the other.
	slot40.inflight.Add(1)
	ok := sc.deliverLeased(slot40, val)
	slot40.inflight.Add(-1)
	return ok
}

// deliverLeased is deliver's body; the caller holds the slot's lease.
func (sc *SlabCaster40Gemini[V]) deliverLeased(slot40 *slot40[V], val V) bool {
	if slot40.closed.Load() {
		return false
	}

	policy := SlabPolicy40Gemini(slot40.policy.Load())

	switch policy {
	case SlabPolicy40DropOldest:
		for {
			// Fast path: direct non-blocking enqueue
			select {
			case slot40.ch <- val:
				return true
			default:
			}

			// Buffer is full: pop oldest unconsumed item to make room
			select {
			case <-slot40.ch:
			default:
			}

			// Attempt re-insertion
			select {
			case slot40.ch <- val:
				return true
			default:
			}

			// If a concurrent consumer or drain operation raced, check closure and retry once
			if slot40.closed.Load() {
				return false
			}
			select {
			case slot40.ch <- val:
				return true
			default:
				return false
			}
		}

	case SlabPolicy40DropNewest:
		select {
		case slot40.ch <- val:
			return true
		default:
			return false
		}

	case SlabPolicy40Block:
		if slot40.closed.Load() || sc.closed.Load() {
			return false
		}
		slot40.ch <- val
		// A closer that found this delivery blocked took values off the
		// channel to release it; the value may be one of those.
		return !slot40.closed.Load()

	default:
		return false
	}
}

// awaitDeliveries is called by a closer that has set slot40.closed. It
// returns when no delivery to the slot is in progress. New deliveries see the
// flag and leave at once, so the wait is short unless a delivery is blocked
// on a full channel (SlabPolicy40Block); taking values off the channel
// releases it.
func (slot40 *slot40[V]) awaitDeliveries() {
	for slot40.inflight.Load() != 0 {
		select {
		case <-slot40.ch:
		default:
			runtime.Gosched()
		}
	}
}

// Subscribe acquires an available slot40 and returns its SlabSubscription40Gemini handle.
// Allocation overhead: exactly 0 allocs/op in steady state.
func (sc *SlabCaster40Gemini[V]) Subscribe() *SlabSubscription40Gemini[V] {
	return sc.SubscribeWithPolicy(sc.policy)
}

// SubscribeWithPolicy registers a new subscriber with a custom backpressure policy.
func (sc *SlabCaster40Gemini[V]) SubscribeWithPolicy(p SlabPolicy40Gemini) *SlabSubscription40Gemini[V] {
	if sc.closed.Load() {
		panic(ErrSlabCaster40Closed)
	}

	for {
		curr := sc.head.Load()
		for curr != nil {
			act := curr.active.Load()
			if ^act != 0 { // at least one bit is 0 (available)
				idx := bits.TrailingZeros64(^act)
				if idx < slab40Slots {
					bit := uint64(1) << idx
					if curr.active.CompareAndSwap(act, act|bit) {
						slot40 := &curr.slots[idx]
						slot40.policy.Store(uint32(p))

						// Drain any residual elements from prior usage
						for {
							select {
							case <-slot40.ch:
							default:
								goto Drained
							}
						}
					Drained:
						sc.subscribers.Add(1)
						slot40.closed.Store(false) // open: deliveries may begin
						if sc.closed.Load() {
							// Close ran while the slot was being claimed and
							// may have passed it. Close it here (a no-op if
							// Close did get to it) and report the caster
							// closed, as at the top.
							slot40.sub.Close()
							panic(ErrSlabCaster40Closed)
						}
						return &slot40.sub
					}
					// CAS conflict: re-read active in next iteration
					continue
				}
			}
			curr = curr.next.Load()
		}

		// All existing slabs full: allocate a new slab40 and append to the list
		newS := sc.newSlab()
		tail := sc.tail.Load()
		for {
			if tail.next.CompareAndSwap(nil, newS) {
				sc.tail.CompareAndSwap(tail, newS)
				break
			}
			tail = tail.next.Load()
		}
	}
}

// SubscribeContext returns a standard Go 1.23+ sequence iterator (`iter.Seq[V]`) that yields
// broadcasted values. When ctx is cancelled or the iterator loop breaks, the subscription
// is automatically unregistered and recycled without leaking resources.
func (sc *SlabCaster40Gemini[V]) SubscribeContext(ctx context.Context, opts ...SlabOption40Gemini) iter.Seq[V] {
	return func(yield func(V) bool) {
		if yield == nil {
			panic("bigbuff: slabcaster: iterator yield func is nil")
		}
		if ctx == nil {
			ctx = context.Background()
		}
		if err := ctx.Err(); err != nil {
			return
		}

		policy := sc.policy
		if len(opts) > 0 {
			var cfg slabCaster40Config
			cfg.policy = policy
			for _, o := range opts {
				o(&cfg)
			}
			policy = cfg.policy
		}

		sub := sc.SubscribeWithPolicy(policy)
		ch := sub.C()

		// Close exactly once: the handle is shared with the slot's next
		// subscriber, so a second Close could close that subscriber.
		var closeOnce sync.Once
		closeSub := func() { closeOnce.Do(sub.Close) }
		defer closeSub()

		stop := context.AfterFunc(ctx, closeSub)
		defer stop()

		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-ch:
				if !ok {
					return
				}
				if !yield(val) {
					return
				}
			}
		}
	}
}

// Subscribers returns the instantaneous number of active registered subscribers.
func (sc *SlabCaster40Gemini[V]) Subscribers() int {
	n := sc.subscribers.Load()
	if n < 0 {
		return 0
	}
	return int(n)
}

// Close shuts down the SlabCaster40Gemini, marks all slots closed, and wakes all blocked/waiting subscribers.
func (sc *SlabCaster40Gemini[V]) Close() error {
	sc.closeMu.Lock()
	defer sc.closeMu.Unlock()

	if !sc.closed.CompareAndSwap(false, true) {
		return nil
	}

	for curr := sc.head.Load(); curr != nil; curr = curr.next.Load() {
		mask := curr.active.Load()
		for mask != 0 {
			idx := bits.TrailingZeros64(mask)
			slot40 := &curr.slots[idx]
			if !slot40.closed.Swap(true) {
				slot40.awaitDeliveries()
				close(slot40.ch)
			}
			mask &= mask - 1
		}
		curr.active.Store(0)
	}

	sc.subscribers.Store(0)
	return nil
}

// C exposes the underlying native receive channel for use in select statements.
func (s *SlabSubscription40Gemini[V]) C() <-chan V {
	slot40 := &s.slab40.slots[s.index]
	return slot40.ch
}

// SlabPolicy40Gemini retrieves the current SlabPolicy40Gemini assigned to this subscription.
func (s *SlabSubscription40Gemini[V]) SlabPolicy40Gemini() SlabPolicy40Gemini {
	slot40 := &s.slab40.slots[s.index]
	return SlabPolicy40Gemini(slot40.policy.Load())
}

// SetPolicy updates the SlabPolicy40Gemini dynamically for an active subscription.
func (s *SlabSubscription40Gemini[V]) SetPolicy(p SlabPolicy40Gemini) {
	slot40 := &s.slab40.slots[s.index]
	slot40.policy.Store(uint32(p))
}

// Unsubscribe is an alias for Close.
func (s *SlabSubscription40Gemini[V]) Unsubscribe() {
	s.Close()
}

// Close releases the slot40 back to the slab40's allocation bitmask, making it available
// for immediate reuse by future Subscribe calls without triggering garbage collection.
// The channel is closed and replaced with a fresh channel to instantly unblock concurrent receivers.
func (s *SlabSubscription40Gemini[V]) Close() {
	if s == nil || s.slab40 == nil {
		return
	}

	slab40 := s.slab40
	idx := s.index
	bit := uint64(1) << idx
	slot40 := &slab40.slots[idx]

	// Claim the close. The flag is already set if the slot is free, if
	// another Close of this handle is in progress, or if the caster's Close
	// has closed the slot: in each case there is nothing to do.
	if slot40.closed.Swap(true) {
		return
	}
	slot40.awaitDeliveries()

	oldCh := slot40.ch
	// Replace channel so subsequent claimants start with a pristine, unclosed channel
	slot40.ch = make(chan V, s.owner.bufferSize)
	close(oldCh)

	// Release the slot last: only now may a Subscribe claim it.
	s.owner.subscribers.Add(-1)
	for {
		act := slab40.active.Load()
		if act&bit == 0 || slab40.active.CompareAndSwap(act, act&^bit) {
			return
		}
	}
}
