// Horizon70Meta: wait-free-publish, lossy 1:N broadcast ring with per-cursor
// positions, coalesced Cond wakeups, and lag/miss observability.
//
// Semantics:
//   - Total order is claim order (one tail XADD per publish); each cursor sees
//     messages in increasing sequence, never duplicated, never reordered.
//   - A cursor that lags more than capacity is jumped forward to the oldest
//     retained sequence; Lag/Missed report the gap. Values are never torn:
//     the per-slot seqlock double-check discards racing reads.
//   - Next() is non-blocking and never returns torn data. WaitNext /
//     WaitNextContext park on a shared sync.Cond with coalesced TryLock
//     wakeups; Close is idempotent and wakes every parked waiter.
//   - Every publish is O(1) in the number of consumers; cursors are
//     single-goroutine and never registered with the ring.
//
// Zero-allocation hot path: Publish, PublishBatch, Next, Latest, SkipToLatest
// and Lag all allocate 0 B/op. Blocking waits use sync.Cond (+ AfterFunc for
// the ctx path) only on the slow path.

package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// horizonSlot70Meta is one broadcast slot.
//
// Slot seq tag encoding: 0 is the committed state of lap -1 at
// construction; 2*(k+1)-1 is "lap k is being written"; 2*(k+1) is "lap k
// is committed". A writer must CAS the tag to the odd writing marker
// before touching val; readers validate against the even committed tag.
// The CAS claim is what keeps the tag monotonically non-decreasing:
// a writer whose lap has already been overtaken elides its write
// instead of regressing a tag that a newer lap already advanced.
//
// Layout is deliberate, not incidental:
//
//   - seq lives alone on its own 64-byte cache line. The publisher's
//     seq CAS/Store is the only write to that line for this slot, and the
//     reader's seq Loads are the only reads. No neighbouring val or
//     seq shares the line, so a publish does not invalidate a reader's
//     line for a different slot.
//   - val lives on the following line(s), followed by 64 bytes of pad.
//     The 64-byte pad after val guarantees the next slot's seq can
//     never share a cache line with this slot's val, even for tiny V
//     (e.g. bool / uint16) where a smaller pad would still alias.
//     For the common case V <= 8 bytes the slot is 8+56+8+64 = 136
//     bytes: seq line, val line, pad. The cost is ~136 KiB for a
//     1024-slot ring, which is L2-resident on modern cores and is
//     paid once at construction, never per message.
//
// GC correctness: val is a real Go field in a real slice, so the GC
// scans it. We deliberately do NOT hide V in a []byte arena via
// unsafe: that would hide pointers (string, slice, *T, interface)
// from the collector and could free a payload while a slow reader
// still holds its slot.
type horizonSlot70Meta[V any] struct {
	seq     atomic.Uint64
	_padSeq [56]byte
	val     V
	_padVal [64]byte
}

// Horizon70Meta is a bounded-spin, lossy, 1:N broadcast ring. Publish is
// O(1) in N; the slot claim is a single CAS in the common case.
//
// Target scenario (the envelope it dominates):
//
//   - M producers, N consumers, N can be 1..1000s.
//   - Producers publish at very high frequency and must never block
//     on a slow consumer. A lagging consumer skips, it never stalls
//     the publisher.
//   - Every consumer wants the same total order (broadcast, not
//     competing consumption). A message is delivered to every cursor,
//     not load-balanced across cursors.
//   - Consumers join at any time, catch up from the oldest retained
//     message, or jump to latest. They poll in the hot path and
//     block only when idle.
//   - Payloads are values (typically small: int64, a small struct,
//     a pointer / string header). Copying V by value is the transfer.
//
// Why chan / Mutex / sync.Map lose here:
//
//   - chan fanout is O(N) per publish: the publisher must Send once
//     per subscriber, taking each chan's lock and waking each
//     waiter. Publish cost grows with N and the publisher's cache
//     line for every chan is ping-ponged.
//   - A Mutex + slice copy per subscriber is O(N) copies per publish
//     plus lock contention on the single mutex.
//   - sync.Map / sync.Cond broadcast still serialise publishers on
//     one lock and copy per subscriber.
//
// Horizon70Meta publish is O(1) in N and O(1) in the number of consumers:
// one LOCK XADD to claim a sequence number, one slot-claim CAS,
// one plain store of V, one seq Store to publish, and (only if someone
// is actually parked) one TryLock + Broadcast. A publisher spins only
// while the previous lap of its slot is mid-write; a writer that finds
// its slot already owned by a newer lap elides the write entirely
// (the ring is lossy). Consumers are completely
// independent: each Cursor70Meta owns its own next sequence number, so
// consumers never contend with each other or with publishers except
// for the single tail Add and the per-slot seq line they are
// reading.
//
// Ordering / loss semantics:
//
//   - Total order is the claim order (tail Add order). A cursor
//     sees messages in increasing seq, never duplicated, never
//     reordered.
//   - If a cursor lags by more than capacity, the messages it missed
//     are gone. Next() silently jumps to the oldest retained seq
//     (tail-capacity). Lag() / Missed() let the caller observe how
//     far behind it is.
//   - If a cursor is caught up but the next slot is claimed and not
//     yet published (publisher between claim and seq Store), Next()
//     returns false. The caller polls again or blocks in WaitNext.
//     It never returns a torn value.
//
// Concurrency contract:
//
//   - Publish / PublishBatch / Latest are safe for concurrent use
//     by any number of goroutines.
//   - A Cursor70Meta is single-goroutine owned, like an iterator. Do not
//     share one Cursor70Meta across goroutines. Create one Cursor70Meta per
//     consumer goroutine; creating a Cursor70Meta is cheap (one struct,
//     no goroutine, no channel, no allocation beyond the struct
//     itself when it escapes).
//   - Horizon70Meta must be created with NewHorizon70Meta. The zero value is
//     not usable. Horizon70Meta must not be copied after first use.
//   - Close must be called after publishers have stopped, as with
//     close(chan). Publish after Close returns (0, false) on a
//     best-effort basis. Close is idempotent and wakes all parked
//     WaitNext callers.
//
// Zero-allocation hot path: Publish, PublishBatch, Next, Latest,
// SkipToLatest, Lag all allocate 0 bytes. Blocking waits use
// sync.Cond and context.AfterFunc only on the slow path.
type Horizon70Meta[V any] struct {
	slots    []horizonSlot70Meta[V]
	mus      []sync.Mutex // race-build only: per-slot mutex for TSan; unused on fast path
	capacity uint64
	mask     uint64

	_padHeader [48]byte // read-only header separated from hot tail

	tail       atomic.Uint64 // next seq to claim; the ONLY hot publisher counter
	_padTail   [56]byte
	waiters    atomic.Int32 // number of goroutines currently parked in Wait
	_padWaiter [60]byte
	closed     atomic.Bool
	_padClosed [56]byte

	mu   sync.Mutex // Cond mutex; only touched when someone parks / wakes
	cond *sync.Cond
}

// NewHorizon70Meta creates a broadcast ring that retains capacity messages.
// Capacity must be a power of two (>0). Power-of-two lets seq->slot
// be a single AND (seq & mask) instead of a division, and keeps the
// slot index branchless on the hot path.
func NewHorizon70Meta[V any](capacity int) *Horizon70Meta[V] {
	if capacity <= 0 {
		panic("bigbuff: horizon: capacity must be > 0")
	}
	if capacity&(capacity-1) != 0 {
		panic("bigbuff: horizon: capacity must be a power of two")
	}
	h := &Horizon70Meta[V]{
		slots:    make([]horizonSlot70Meta[V], capacity),
		mus:      make([]sync.Mutex, capacity),
		capacity: uint64(capacity),
		mask:     uint64(capacity - 1),
	}
	h.cond = sync.NewCond(&h.mu)
	// Slot seq starts at its zero value 0, the committed state of lap -1.
	return h
}

func (h *Horizon70Meta[V]) checkInit() {
	if h == nil || h.cond == nil {
		panic("bigbuff: horizon: must use NewHorizon70Meta")
	}
}

// Cap returns the retention capacity.
func (h *Horizon70Meta[V]) Cap() int {
	h.checkInit()
	return int(h.capacity)
}

// Tail returns the next sequence number to be claimed (i.e. the
// number of messages ever claimed). It is for observability only;
// cursors should use Seq / Lag.
func (h *Horizon70Meta[V]) Tail() uint64 {
	h.checkInit()
	return h.tail.Load()
}

// Closed reports whether Close has been called.
func (h *Horizon70Meta[V]) Closed() bool {
	h.checkInit()
	return h.closed.Load()
}

// Publish publishes one value. It is wait-free in the practical
// sense: one atomic Add (LOCK XADD on amd64, LDXR/STXR loop on
// arm64, always succeeds, no CAS retry storm), one value store,
// one seq Store. No lock, no allocation, no per-subscriber work.
//
// It returns the sequence number assigned to v, and false if the
// Horizon70Meta is closed.
func (h *Horizon70Meta[V]) Publish(v V) (uint64, bool) {
	h.checkInit()
	if h.closed.Load() {
		return 0, false
	}
	// Claim: fetch-add, not CAS. A CAS loop on a single tail under
	// M producers degenerates into retry storms; Add is a single
	// hardware RMW that always makes progress.
	seq := h.tail.Add(1) - 1
	h.storeSlot(seq, v)
	h.notify()
	return seq, true
}

// PublishBatch publishes len(vals) values with a single tail Add
// and a single wakeup, amortising the atomic + broadcast cost over
// the whole batch. Values are published in slice order. If
// len(vals) > capacity, early values in the batch may be overwritten
// by later values in the same batch before a slow reader sees them;
// that is the same lossy rule as single Publish, applied to the
// batch. Returns the first seq in the batch.
func (h *Horizon70Meta[V]) PublishBatch(vals []V) (uint64, bool) {
	h.checkInit()
	if len(vals) == 0 {
		return h.tail.Load(), true
	}
	if h.closed.Load() {
		return 0, false
	}
	first := h.tail.Add(uint64(len(vals))) - uint64(len(vals))
	for i, v := range vals {
		h.storeSlot(first+uint64(i), v)
	}
	h.notify()
	return first, true
}

// notify wakes parked waiters, coalesced.
//
// Fast path (no waiters): one atomic Load, no lock, no futex.
// Slow path (waiters > 0): TryLock the Cond mutex. If TryLock
// fails, another publisher is already holding it to broadcast, or
// a waiter is holding it while re-checking and will see the new
// data itself. In both cases our publish is already visible
// (seq Store happened before notify), so the other broadcast /
// the waiter's own re-check covers us. We must NOT block here:
// blocking on the Cond mutex would serialise all publishers
// behind one slow waiter wakeup and re-introduce the lock
// contention we removed from the publish path.
func (h *Horizon70Meta[V]) notify() {
	if h.waiters.Load() == 0 {
		return
	}
	if h.mu.TryLock() {
		h.cond.Broadcast()
		h.mu.Unlock()
	}
}

// Latest returns the most recent fully-published value without
// allocating a cursor and without touching any consumer state.
// It is a wait-free optimistic read: read tail, read the slot for
// tail-1 with the seqlock double-check, retry only if tail moved
// underneath us. If the latest publisher is mid-publish (claimed
// but not yet stored seq), it returns false rather than blocking.
func (h *Horizon70Meta[V]) Latest() (V, uint64, bool) {
	h.checkInit()
	var zero V
	for attempt := 0; attempt < 8; attempt++ {
		tail := h.tail.Load()
		if tail == 0 {
			return zero, 0, false
		}
		seq := tail - 1
		if v, ok := h.loadSlot(seq); ok {
			return v, seq, true
		}
		// If tail did not move, the latest slot is mid-publish.
		// If tail moved, retry with the newer tail.
		if h.tail.Load() == tail {
			return zero, 0, false
		}
	}
	return zero, 0, false
}

// Subscribe returns a cursor positioned at the current tail: it
// will see only messages published after this call.
func (h *Horizon70Meta[V]) Subscribe() *Cursor70Meta[V] {
	h.checkInit()
	return &Cursor70Meta[V]{h: h, next: h.tail.Load()}
}

// SubscribeFromLatest is Subscribe.
func (h *Horizon70Meta[V]) SubscribeFromLatest() *Cursor70Meta[V] {
	return h.Subscribe()
}

// SubscribeFromOldest returns a cursor positioned at the oldest
// retained message (tail-capacity, or 0 if fewer than capacity
// messages have ever been published).
func (h *Horizon70Meta[V]) SubscribeFromOldest() *Cursor70Meta[V] {
	h.checkInit()
	tail := h.tail.Load()
	var next uint64
	if tail > h.capacity {
		next = tail - h.capacity
	}
	return &Cursor70Meta[V]{h: h, next: next}
}

// SubscribeFrom returns a cursor positioned at seq, clamped to
// [oldest retained, tail]. A seq that is already overwritten is
// clamped forward to the oldest retained message; Next() would
// have skipped there anyway.
func (h *Horizon70Meta[V]) SubscribeFrom(seq uint64) *Cursor70Meta[V] {
	h.checkInit()
	tail := h.tail.Load()
	if seq > tail {
		seq = tail
	}
	if tail-seq > h.capacity {
		if tail > h.capacity {
			seq = tail - h.capacity
		} else {
			seq = 0
		}
	}
	return &Cursor70Meta[V]{h: h, next: seq}
}

// Close marks the Horizon70Meta closed, wakes every parked WaitNext /
// WaitNextContext caller, and is idempotent. Call it after
// publishers have stopped.
func (h *Horizon70Meta[V]) Close() {
	h.checkInit()
	if !h.closed.CompareAndSwap(false, true) {
		return
	}
	h.mu.Lock()
	h.cond.Broadcast()
	h.mu.Unlock()
}

// Cursor70Meta is one consumer's position in a Horizon70Meta. It is
// single-goroutine owned. Creating a cursor is cheap and does
// not register with the Horizon70Meta: the Horizon70Meta never tracks
// cursors, which is exactly why publish stays O(1) in N.
type Cursor70Meta[V any] struct {
	h    *Horizon70Meta[V]
	next uint64 // next seq to read
}

func (c *Cursor70Meta[V]) checkInit() {
	if c == nil || c.h == nil || c.h.cond == nil {
		panic("bigbuff: horizon: cursor: must use Subscribe")
	}
}

// Next is the non-blocking hot path. It returns the next message
// in total order, or false if none is available yet. It never
// blocks, never allocates, and never returns a torn value: the
// slot seqlock double-check discards any read that raced an
// overwrite.
//
// If the cursor has lagged by more than capacity, Next jumps
// forward to the oldest retained message instead of returning
// stale / duplicate data.
func (c *Cursor70Meta[V]) Next() (V, bool) {
	c.checkInit()
	h := c.h
	var zero V
	for {
		tail := h.tail.Load()
		if c.next >= tail {
			return zero, false
		}
		if tail-c.next > h.capacity {
			if tail > h.capacity {
				c.next = tail - h.capacity
			} else {
				c.next = 0
			}
			continue
		}
		if v, ok := h.loadSlot(c.next); ok {
			c.next++
			return v, true
		}
		// Slot not yet published (publisher between claim and
		// seq Store) or overwritten while we were reading it.
		// If tail moved far enough that we are now lagging, loop
		// and let the lag branch skip us forward. Otherwise the
		// publisher is still in flight: return false and let the
		// caller poll or block. Spinning here would burn a core
		// for a publisher that may be descheduled.
		if tail2 := h.tail.Load(); tail2-c.next > h.capacity {
			continue
		}
		return zero, false
	}
}

// WaitNext blocks until the next message is available or the
// Horizon70Meta is closed. It is the slow path: Next() is tried first,
// then the goroutine parks on sync.Cond. The waiter count gates
// publisher notify, so a parked waiter costs publishers nothing
// until it actually parks, and a busy poller costs publishers
// nothing at all.
func (c *Cursor70Meta[V]) WaitNext() (V, bool) {
	c.checkInit()
	h := c.h
	var zero V
	for {
		if v, ok := c.Next(); ok {
			return v, true
		}
		if h.closed.Load() {
			return zero, false
		}
		h.waiters.Add(1)
		h.mu.Lock()
		// Re-check under the Cond lock: this closes the race
		// where a publish + broadcast happened between our
		// Next() above and our park below. If data arrived, we
		// must not park; if Close happened, we must not park.
		if h.closed.Load() || c.next < h.tail.Load() {
			h.mu.Unlock()
			h.waiters.Add(-1)
			continue
		}
		h.cond.Wait()
		h.mu.Unlock()
		h.waiters.Add(-1)
	}
}

// WaitNextContext is WaitNext with context cancellation. It uses
// context.AfterFunc to broadcast on cancellation, so no goroutine
// is spawned per wait and no goroutine leaks if the wait returns
// via data or Close: AfterFunc is stopped on every exit path.
// If data and cancellation race, data wins on the next loop
// iteration because Next() is tried before the ctx check.
func (c *Cursor70Meta[V]) WaitNextContext(ctx context.Context) (V, bool) {
	c.checkInit()
	if ctx == nil {
		ctx = context.Background()
	}
	h := c.h
	var zero V
	for {
		if v, ok := c.Next(); ok {
			return v, true
		}
		if h.closed.Load() {
			return zero, false
		}
		select {
		case <-ctx.Done():
			return zero, false
		default:
		}
		stop := context.AfterFunc(ctx, func() {
			h.mu.Lock()
			h.cond.Broadcast()
			h.mu.Unlock()
		})
		h.waiters.Add(1)
		h.mu.Lock()
		if h.closed.Load() || c.next < h.tail.Load() || ctx.Err() != nil {
			h.mu.Unlock()
			h.waiters.Add(-1)
			stop()
			continue
		}
		h.cond.Wait()
		h.mu.Unlock()
		h.waiters.Add(-1)
		stop()
	}
}

// SkipToLatest jumps the cursor to the current tail, discarding
// any buffered messages it had not yet read.
func (c *Cursor70Meta[V]) SkipToLatest() {
	c.checkInit()
	c.next = c.h.tail.Load()
}

// SkipToOldest jumps the cursor to the oldest retained message.
func (c *Cursor70Meta[V]) SkipToOldest() {
	c.checkInit()
	tail := c.h.tail.Load()
	if tail > c.h.capacity {
		c.next = tail - c.h.capacity
	} else {
		c.next = 0
	}
}

// Seq returns the next sequence number this cursor will read.
func (c *Cursor70Meta[V]) Seq() uint64 {
	c.checkInit()
	return c.next
}

// Lag returns how many messages have been published but not yet
// read by this cursor (0 if caught up).
func (c *Cursor70Meta[V]) Lag() uint64 {
	c.checkInit()
	tail := c.h.tail.Load()
	if tail > c.next {
		return tail - c.next
	}
	return 0
}

// Missed returns how many messages this cursor has permanently
// lost to overwrite (lag beyond capacity). 0 means it can still
// catch up without loss.
func (c *Cursor70Meta[V]) Missed() uint64 {
	c.checkInit()
	lag := c.Lag()
	if lag > c.h.capacity {
		return lag - c.h.capacity
	}
	return 0
}

// All returns a blocking iterator that yields messages until the
// Horizon70Meta is closed or the yield function returns false. It is
// the range-over equivalent of repeatedly calling WaitNext.
// Example:
//
//	for v := range cursor.All() {
//		// handle v
//	}
func (c *Cursor70Meta[V]) All() iter.Seq[V] {
	c.checkInit()
	return func(yield func(V) bool) {
		for {
			v, ok := c.WaitNext()
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

// AllContext is All with context cancellation.
func (c *Cursor70Meta[V]) AllContext(ctx context.Context) iter.Seq[V] {
	c.checkInit()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		for {
			v, ok := c.WaitNextContext(ctx)
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
