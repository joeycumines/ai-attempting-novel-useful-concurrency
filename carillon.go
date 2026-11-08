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
	"sync"
	"sync/atomic"
)

// ErrCarillonUnsubscribed is returned by [CarillonSubscription.Recv] when the
// subscription is no longer live, because it was unsubscribed (or is a zero
// value).
var ErrCarillonUnsubscribed = errors.New("bigbuff: carillon: subscription is not live")

const (
	// carillonLive is set in a slot's word while the slot is bound to a live
	// subscription. The low 63 bits of the word are the owner's cursor.
	carillonLive uint64 = 1 << 63
	// carillonSeqMask extracts the cursor from a slot's word.
	carillonSeqMask = carillonLive - 1
	// carillonChunk is the number of slots per chunk, i.e. the width of the
	// bitmaps. It must match the word size.
	carillonChunk = 64
)

// Carillon broadcasts every value sent to every live [CarillonSubscription], in
// sequence order, through a single ring of values. It is an alternative to
// channel fan-out with three properties that the channel designs in this
// package trade against each other:
//
//   - Allocation is zero per Send, per receive, per Subscribe and per
//     Unsubscribe once the subscriber table has warmed up. The only
//     allocation is a 64-slot chunk, amortised over 64 concurrent
//     subscribers.
//   - Each subscription is select-compatible: [CarillonSubscription.Ready] returns a
//     channel which can be used as a case in a select statement.
//   - The cost of a Send is proportional to the number of subscriptions that
//     are parked (blocked waiting for a value), not the number that exist.
//     Busy subscriptions cost the sender one bit test per 64 of them.
//     Subscribing and unsubscribing take no lock and never wait on the
//     sender, so churn does not contend with broadcasting.
//
// # Semantics
//
// A subscription receives each value published at or after its join point
// (the published sequence observed by [Carillon.Subscribe]) exactly once and in
// order. [Carillon.Send] blocks only while the slowest live subscription is
// `slack` values behind, so `slack` is the pipelining depth: the sender may
// run that far ahead of its slowest consumer. A subscription that stops
// consuming without unsubscribing stalls the sender, just as a stalled
// receiver stalls a channel send.
//
// # Mechanism
//
// Values are stored once, in a power-of-two ring. Each subscription owns a
// slot, which holds a packed word (live bit | cursor) and a 1-buffered
// doorbell channel. Slots are grouped into chunks of 64, each with three
// bitmaps: claimed (slot is bound), parked (owner is blocked, or about to
// block, in [CarillonSubscription.Ready]), and pending (slot joined since the sender
// last absorbed it).
//
// The sender publishes a value, then performs a non-blocking send on the
// doorbell of every parked owner. Owners that are keeping up never park, so
// they are never rung. An owner parks by setting its parked bit and then
// rechecking the published sequence before blocking. Go's atomics are
// sequentially consistent, so either the owner observes the new value, or the
// sender observes the parked bit and rings the doorbell. Doorbells are
// coalescing (capacity 1), so a stale or duplicate ring only causes a spurious
// wake-up, which is harmless.
//
// Flow control uses the ring as a bounded window. The sender may write
// sequence w only once every live cursor exceeds w-size, which is the same
// slot-reuse rule used by LMAX-style disruptor rings. The sender caches a
// lower bound on the cursors and rescans only when that bound is exhausted.
// When blocked, it publishes the cursor value it needs in waitFor, then
// sleeps on a single writer doorbell. A subscription wakes the sender only
// when its cursor reaches exactly that value.
//
// Joining is lock-free. A joiner stores its cursor (the published sequence it
// loaded) together with the live bit, sets its pending bit, and then checks
// the sender's reserved sequence. If the sender has already reserved a
// sequence at or beyond the joiner's cursor plus slack, the slot for that
// cursor may have been overwritten, so the joiner reloads and retries.
// Otherwise every future write that could overwrite the joiner's window is
// preceded, in sequentially consistent order, by a read of the pending
// flag, so the sender absorbs the joiner into its cached bound before it writes
// there. See the comments on [Carillon.Subscribe] and [Carillon.reserve].
//
// # Concurrency
//
// [Carillon.Send] is safe for concurrent use; senders are serialised by an
// internal mutex. Subscribe, Len and the methods of [CarillonSubscription] are safe for
// concurrent use across subscriptions. A single [CarillonSubscription] must be driven by
// one goroutine at a time, and must not be used concurrently with its own
// Unsubscribe.
type Carillon[V any] struct {
	// Sender state, guarded by mu.
	mu        sync.Mutex
	head      uint64 // next sequence to write
	minCached uint64 // lower bound on the cursors of all live subscriptions

	ring []V
	mask uint64
	size uint64 // == len(ring) == slack

	_ [64]byte // keep sender-written fields off the readers' cache lines

	published     atomic.Uint64 // sequences below this may be read
	reserved      atomic.Uint64 // sequence the sender has begun to write (join guard)
	pendingFlag   atomic.Uint32 // non-zero: a subscription joined since the last absorb
	writerWaiting atomic.Uint32 // non-zero: the sender is (or is about to be) blocked
	waitFor       atomic.Uint64 // cursor value that unblocks the sender
	active        atomic.Int64  // number of live subscriptions

	// writerBell wakes a blocked sender. Capacity 1, so signals coalesce.
	writerBell chan struct{}

	growMu sync.Mutex
	chunks atomic.Pointer[[]*chunk] // copy-on-grow directory of chunks

	// joinHook, when non-nil, runs in Subscribe after the joiner has loaded the
	// published sequence and before it publishes its cursor. Tests use it to
	// interleave a sender at the exact point where a join is most fragile. It is
	// always nil in production.
	joinHook func()
}

// slot is one subscription's control block. It is exactly one cache line, so
// an owner updating its cursor does not false-share with its neighbours.
type carillonSlot struct {
	// word is carillonLive | cursor while bound, and 0 otherwise. The owner
	// writes it; the sender reads it.
	word atomic.Uint64
	// bell is the doorbell. It is created with the chunk and never replaced.
	bell chan struct{}

	// Owner-only state. The sender never touches these fields.
	cur   uint64 // the owner's cursor, mirrored to avoid atomic loads
	gen   uint64 // incremented on every Subscribe, to detect stale handles
	armed bool   // the owner's parked bit is currently set
	bound bool   // the slot belongs to a live subscription

	_ [24]byte
}

// chunk is 64 slots plus their summary bitmaps. Each bitmap bit corresponds to
// the slot at the same index.
type chunk struct {
	claimed atomic.Uint64 // slot is bound, or is being bound
	parked  atomic.Uint64 // owner is parked (or arming to park)
	pending atomic.Uint64 // slot joined since the sender last absorbed it

	_     [40]byte
	slots [carillonChunk]carillonSlot
}

func newCarillonChunk() *chunk {
	ch := new(chunk)
	for i := range ch.slots {
		ch.slots[i].bell = make(chan struct{}, 1)
	}
	return ch
}

// NewCarillon constructs a [Carillon] whose ring holds slack values, rounded up
// to a power of two. slack is the number of values the sender may run ahead
// of its slowest subscription. It panics if slack is less than 1.
func NewCarillon[V any](slack int) *Carillon[V] {
	if slack < 1 {
		panic("bigbuff: carillon: slack must be at least 1")
	}
	n := uint64(1)
	for n < uint64(slack) {
		n <<= 1
	}
	c := &Carillon[V]{
		ring:       make([]V, n),
		mask:       n - 1,
		size:       n,
		writerBell: make(chan struct{}, 1),
	}
	empty := make([]*chunk, 0)
	c.chunks.Store(&empty)
	return c
}

// Len returns the number of live subscriptions. It is a snapshot, and may be
// stale by the time the caller acts on it.
func (c *Carillon[V]) Len() int {
	return int(c.active.Load())
}

// Send publishes v to every live subscription, and returns the number of live
// subscriptions at the time of publication. The value is visible to
// subscriptions as soon as Send publishes it, without waiting for them to
// consume it. Send blocks only while the slowest live subscription is slack
// values behind.
func (c *Carillon[V]) Send(v V) int {
	c.mu.Lock()
	w := c.head
	c.reserved.Store(w)
	c.reserve(w)
	c.ring[w&c.mask] = v
	c.head = w + 1
	c.published.Store(w + 1)
	c.mu.Unlock()

	c.ringParked()
	return int(c.active.Load())
}

// Subscribe binds a new subscription to the carillon. The subscription's join
// point is the sequence published when Subscribe loads it. The subscription
// receives every value published at or after that point. Subscribe never
// blocks on the sender, and performs no allocation once the chunk directory
// has grown to cover the number of concurrent subscriptions.
func (c *Carillon[V]) Subscribe() CarillonSubscription[V] {
	ch, i := c.claim()
	bit := uint64(1) << uint(i)
	sl := &ch.slots[i]

	// Publish the cursor, then check that the sender has not already run
	// far enough ahead to have overwritten it. The order matters, and the
	// reasoning is in the package doc. Briefly: the pending bit and flag are
	// set before the reserved check, so any write that could affect this
	// joiner is preceded by a sender-side absorb that sees the joiner.
	var p uint64
	retried := false
	for {
		p = c.published.Load()
		if c.joinHook != nil {
			c.joinHook()
		}
		sl.word.Store(carillonLive | p)
		ch.pending.Or(bit)
		c.pendingFlag.Store(1)
		if c.reserved.Load() < p+c.size {
			break // the sender has not reached the danger zone for p
		}
		// The sender has reserved sequence >= p+size, so the slot for p may
		// already have been reused. Reload and try again.
		retried = true
	}
	if retried {
		// A failed attempt left a cursor that is too low to be safe, and the
		// sender may have observed it while blocked. Only a TryRecv advance
		// signals the sender, and this cursor was not advanced by TryRecv, so
		// signal it here. A join that succeeds first time never needs this: its
		// cursor is at or above any threshold the sender could be waiting on.
		c.wakeWriter()
	}

	sl.cur, sl.armed, sl.bound = p, false, true
	sl.gen++
	c.active.Add(1)
	return CarillonSubscription[V]{c: c, ch: ch, sl: sl, bit: bit, gen: sl.gen}
}

// claim binds a free slot, growing the directory if necessary.
func (c *Carillon[V]) claim() (*chunk, int) {
	for {
		chunks := *c.chunks.Load()
		for _, ch := range chunks {
			for a := ch.claimed.Load(); a != ^uint64(0); a = ch.claimed.Load() {
				i := bits.TrailingZeros64(^a)
				if ch.claimed.CompareAndSwap(a, a|uint64(1)<<uint(i)) {
					return ch, i
				}
			}
		}
		c.grow(len(chunks))
	}
}

// grow appends one chunk, unless another goroutine has already done so since
// the caller observed len seen.
func (c *Carillon[V]) grow(seen int) {
	c.growMu.Lock()
	defer c.growMu.Unlock()
	old := *c.chunks.Load()
	if len(old) != seen {
		return
	}
	next := make([]*chunk, len(old)+1)
	copy(next, old)
	next[len(old)] = newCarillonChunk()
	c.chunks.Store(&next)
}

// reserve blocks until sequence w may be written, which means every live
// cursor is greater than w-size. It must be called with mu held, after
// storing reserved = w.
func (c *Carillon[V]) reserve(w uint64) {
	if c.pendingFlag.Load() != 0 {
		c.absorb()
	}
	for w-c.minCached >= c.size {
		c.minCached = c.scanMin(w)
		if w-c.minCached < c.size {
			return
		}
		// Blocked. Publish the threshold before raising writerWaiting, so that a
		// subscription which crosses the threshold observes both values.
		c.waitFor.Store(w + 1 - c.size)
		c.writerWaiting.Store(1)
		c.minCached = c.scanMin(w)
		if w-c.minCached < c.size {
			c.writerWaiting.Store(0)
			return
		}
		<-c.writerBell
		c.writerWaiting.Store(0)
	}
}

// scanMin returns the minimum cursor over all live slots, or w if there are
// none. Cursors never exceed the published sequence, which is at most w.
func (c *Carillon[V]) scanMin(w uint64) uint64 {
	m := w
	for _, ch := range *c.chunks.Load() {
		for a := ch.claimed.Load(); a != 0; a &= a - 1 {
			x := ch.slots[bits.TrailingZeros64(a)].word.Load()
			if x&carillonLive != 0 {
				if cur := x & carillonSeqMask; cur < m {
					m = cur
				}
			}
		}
	}
	return m
}

// absorb folds subscriptions that joined since the last absorb into the
// cached lower bound. It must be called with mu held. Clearing the flag before
// reading the pending bitmaps is what makes the handshake with Subscribe work.
func (c *Carillon[V]) absorb() {
	c.pendingFlag.Store(0)
	for _, ch := range *c.chunks.Load() {
		if ch.pending.Load() == 0 {
			continue
		}
		for a := ch.pending.Swap(0); a != 0; a &= a - 1 {
			x := ch.slots[bits.TrailingZeros64(a)].word.Load()
			if x&carillonLive != 0 {
				if cur := x & carillonSeqMask; cur < c.minCached {
					c.minCached = cur
				}
			}
		}
	}
}

// ringParked performs a non-blocking send to the doorbell of every parked
// owner. Doorbells are capacity 1, so the send never blocks, and it does not
// allocate.
func (c *Carillon[V]) ringParked() {
	for _, ch := range *c.chunks.Load() {
		for a := ch.parked.Load(); a != 0; a &= a - 1 {
			select {
			case ch.slots[bits.TrailingZeros64(a)].bell <- struct{}{}:
			default:
			}
		}
	}
}

// signalWriter wakes a blocked sender. Signals coalesce.
func (c *Carillon[V]) signalWriter() {
	select {
	case c.writerBell <- struct{}{}:
	default:
	}
}

// wakeWriter signals a blocked sender, if one has announced that it is
// waiting. Subscribers call it at the points where their cursor can change in
// a way the sender needs to see: unsubscribe, and the final store of a join.
func (c *Carillon[V]) wakeWriter() {
	if c.writerWaiting.Load() != 0 {
		c.signalWriter()
	}
}

// CarillonSubscription is a handle to one subscriber's position in a [Carillon]. It
// is a small value, and copies refer to the same subscription. The zero value
// is an unsubscribed handle. Its methods are no-ops, except that Recv returns
// [ErrCarillonUnsubscribed].
type CarillonSubscription[V any] struct {
	c   *Carillon[V]
	ch  *chunk
	sl  *carillonSlot
	bit uint64
	gen uint64
}

// live reports whether the handle still refers to the subscription that
// created it.
func (s CarillonSubscription[V]) live() bool {
	return s.sl != nil && s.sl.bound && s.sl.gen == s.gen
}

// TryRecv returns the next value, if one is available, without blocking. If
// it returns false, call [CarillonSubscription.Ready] before blocking, so that the
// sender can wake this subscription.
func (s CarillonSubscription[V]) TryRecv() (V, bool) {
	var zero V
	if !s.live() {
		return zero, false
	}
	sl, c := s.sl, s.c
	if sl.armed {
		s.ch.parked.And(^s.bit)
		sl.armed = false
	}
	if sl.cur >= c.published.Load() {
		return zero, false
	}
	v := c.ring[sl.cur&c.mask]
	sl.cur++
	sl.word.Store(carillonLive | sl.cur)
	if c.writerWaiting.Load() != 0 && sl.cur == c.waitFor.Load() {
		c.signalWriter()
	}
	return v, true
}

// Ready arms the subscription and returns its doorbell. The doorbell is ready
// when a value might be available. It can fire spuriously, so after it fires,
// call [CarillonSubscription.TryRecv] and loop. Use it in a select, for example:
//
//	for {
//		if v, ok := sub.TryRecv(); ok {
//			use(v)
//			continue
//		}
//		select {
//		case <-sub.Ready():
//		case <-ctx.Done():
//			return ctx.Err()
//		}
//	}
//
// Ready returns nil for an unsubscribed handle, which never becomes ready.
func (s CarillonSubscription[V]) Ready() <-chan struct{} {
	if !s.live() {
		return nil
	}
	sl := s.sl
	if !sl.armed {
		s.ch.parked.Or(s.bit)
		sl.armed = true
	}
	// Recheck after arming. If a value was published before the bit became
	// visible, the sender did not ring us, so ring ourselves now.
	if sl.cur < s.c.published.Load() {
		select {
		case sl.bell <- struct{}{}:
		default:
		}
	}
	return sl.bell
}

// Recv returns the next value, blocking until one is available or ctx is done.
// It returns [ErrCarillonUnsubscribed] if the subscription is not live, and
// ctx.Err() if ctx is done first. A nil ctx never cancels.
func (s CarillonSubscription[V]) Recv(ctx context.Context) (V, error) {
	var zero V
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	for {
		if !s.live() {
			return zero, ErrCarillonUnsubscribed
		}
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		select {
		case <-s.Ready():
		case <-done:
			return zero, ctx.Err()
		}
	}
}

// All returns an iterator over values received by the subscription. It stops
// when ctx is done, or when the subscription is not live.
func (s CarillonSubscription[V]) All(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		for {
			v, err := s.Recv(ctx)
			if err != nil || !yield(v) {
				return
			}
		}
	}
}

// Lag returns the number of values published but not yet received by this
// subscription. It returns 0 for an unsubscribed handle.
func (s CarillonSubscription[V]) Lag() uint64 {
	if !s.live() {
		return 0
	}
	return s.c.published.Load() - s.sl.cur
}

// Unsubscribe releases the subscription's slot for reuse. It is idempotent
// for a given subscription, and a no-op for stale or zero handles. It must not
// run concurrently with the other methods of the same subscription. It may be
// called from any goroutine if the owner is not using the subscription at the
// time, which is the usual case for cancellation.
func (s CarillonSubscription[V]) Unsubscribe() {
	if !s.live() {
		return
	}
	sl := s.sl
	if sl.armed {
		s.ch.parked.And(^s.bit)
		sl.armed = false
	}
	sl.bound = false
	// Clear the word before releasing the claim, so a concurrent scan that
	// still sees the claim also sees the slot as dead.
	sl.word.Store(0)
	s.ch.claimed.And(^s.bit)
	s.c.active.Add(-1)
	select {
	case <-sl.bell: // discard a stale ring, so the next owner starts clean
	default:
	}
	s.c.wakeWriter() // the sender may be waiting on this cursor
}
