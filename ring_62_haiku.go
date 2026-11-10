package bigbuff

import (
	"context"
	"errors"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrClosed62Haiku is returned by Cursor62Haiku.Next once the ring is closed and the cursor has
// consumed or skipped every entry committed before Close.
var ErrClosed62Haiku = errors.New("bigbuff: ring: closed")

const (
	ringPadBytes62Haiku    = 128     // two 64B lines: defeats adjacent-line prefetch pairing; equals an Apple M-series line
	ringSlotWords62Haiku   = 8       // slots are rounded up to whole 64-byte lines
	ringMaxCapacity62Haiku = 1 << 30 // keeps position arithmetic far from overflow
)

type ringPad62Haiku [ringPadBytes62Haiku]byte

// Ring62Haiku is a fixed-capacity, lossy, single-publisher broadcast ring of plain-data values.
//
// Protocol (publisher P at position p, slot s = p & mask, N = capacity):
//
//	P:  q[s] = 2p+1; store payload words; q[s] = 2p+2; head = p+1; if waiters != 0 { wake }
//	C:  at position p with head snapshot h > p:
//	      if h-p > N            -> provably lapped: jump to h-N
//	      q[s] == 2p+2          -> copy payload words, re-load q[s]; unchanged => valid
//	      q[s] >  2p+2          -> lapped: jump to h'-N
//	Parking (Dekker over seq-cst atomics + park mutex):
//	  waiter: lock; waiters++; re-check head; install bell; unlock; select
//	  P:      head store; load waiters; if non-zero, lock; close bell; unlock
//	  Either P observes the waiter (and closes the bell it installed) or the
//	  waiter's head re-check observes the new head. No lost wakeups.
//
// Publish and Close are called by the producer. Cursors are consumer-owned and
// write no shared memory on the consume path.
type Ring62Haiku[V any] struct {
	arena    []uint64 // 64B-aligned slot storage; noscan (V is plain data); never reallocated
	capacity uint64   // power of two
	mask     uint64
	stride   int // words per slot: 1 stamp + payload words, rounded to whole lines
	size     int // unsafe.Sizeof(V)

	_    ringPad62Haiku
	head atomic.Uint64 // committed entries; the only word consumers poll

	_     ringPad62Haiku
	tail  uint64        // next position to write; publisher-private, guarded by guard
	guard atomic.Uint32 // single-publisher exclusion

	_       ringPad62Haiku
	closed  atomic.Bool
	waiters atomic.Int32 // parked consumers; read by every Publish, written only when parking
	mu      sync.Mutex   // slow path only: parking and waking
	bell    chan struct{}
}

// Cursor62Haiku is a consumer's private read position in a Ring62Haiku. A Cursor62Haiku must not be used by
// more than one goroutine at a time. Distinct cursors on one ring are independent.
type Cursor62Haiku[V any] struct {
	r   *Ring62Haiku[V]
	pos uint64 // next position to read; consumer-owned
}

// NewRing62Haiku returns a ring retaining the most recent `capacity` entries (rounded up to a
// power of two). It panics if V is not plain data: payloads move as atomic 64-bit words,
// which is only sound and GC-invisible for pointer-free types.
func NewRing62Haiku[V any](capacity int) *Ring62Haiku[V] {
	if capacity < 1 || capacity > ringMaxCapacity62Haiku {
		panic(`bigbuff: ring: capacity out of range`)
	}
	if !plainData62Haiku(reflect.TypeFor[V]()) {
		panic(`bigbuff: ring: element type must be plain data (no pointers, strings, slices, maps, chans, funcs or interfaces)`)
	}
	n := uint64(1) << bits.Len(uint(capacity-1))
	var zero V
	size := int(unsafe.Sizeof(zero))
	stride := (1 + (size+7)/8 + ringSlotWords62Haiku - 1) / ringSlotWords62Haiku * ringSlotWords62Haiku
	total := int(n) * stride

	raw := make([]uint64, total+ringSlotWords62Haiku)
	off := 0
	if rem := uintptr(unsafe.Pointer(&raw[0])) & 63; rem != 0 {
		off = int((64 - rem) >> 3) // raw is 8-aligned, so this lands on a 64B boundary
	}
	return &Ring62Haiku[V]{
		arena:    raw[off : off+total : off+total],
		capacity: n,
		mask:     n - 1,
		stride:   stride,
		size:     size,
	}
}

// plainData62Haiku reports whether values of t contain no pointers. Conservative: anything
// unrecognized is rejected.
func plainData62Haiku(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return t.Len() == 0 || plainData62Haiku(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			if !plainData62Haiku(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Cap returns the number of entries the ring retains.
func (r *Ring62Haiku[V]) Cap() int { return int(r.capacity) }

// Publish commits v as the next entry, overwriting the oldest retained entry when full.
// It never waits for consumers. It returns false once the ring is closed.
//
// Publish has single-publisher semantics: overlapping calls from different goroutines
// violate the contract and panic. Consumers are unaffected by this check.
func (r *Ring62Haiku[V]) Publish(v V) bool {
	if r.closed.Load() {
		return false
	}
	if !r.guard.CompareAndSwap(0, 1) {
		panic(`bigbuff: ring: concurrent Publish violates the single-publisher contract`)
	}
	pos := r.tail
	slot := r.slot(pos)
	atomic.StoreUint64(&slot[0], 2*pos+1) // in flight: odd stamp makes any reader copy fail validation
	storeWords62Haiku(slot[1:], &v, r.size)
	atomic.StoreUint64(&slot[0], 2*pos+2) // committed: lap-stamped even value
	r.tail = pos + 1
	r.head.Store(pos + 1)
	r.guard.Store(0)
	if r.waiters.Load() != 0 {
		r.wake()
	}
	return true
}

// Close marks the ring closed, unparks every parked consumer, and makes subsequent Publish
// calls return false. Entries committed before Close remain readable. Close is idempotent.
func (r *Ring62Haiku[V]) Close() {
	if !r.closed.Swap(true) {
		r.wake()
	}
}

// Subscribe returns a cursor positioned at the next entry published after this call.
func (r *Ring62Haiku[V]) Subscribe() *Cursor62Haiku[V] {
	return &Cursor62Haiku[V]{r: r, pos: r.head.Load()}
}

// SubscribeReplay returns a cursor positioned at the oldest retained entry.
func (r *Ring62Haiku[V]) SubscribeReplay() *Cursor62Haiku[V] {
	return &Cursor62Haiku[V]{r: r, pos: r.oldest(r.head.Load())}
}

// oldest is the first position guaranteed to be retained for head snapshot h. Positions
// >= h-N may still be overwritten by the in-flight write of h, which the stamp check catches.
func (r *Ring62Haiku[V]) oldest(h uint64) uint64 {
	if h < r.capacity {
		return 0
	}
	return h - r.capacity
}

func (r *Ring62Haiku[V]) slot(pos uint64) []uint64 {
	off := int(pos&r.mask) * r.stride
	return r.arena[off : off+r.stride]
}

// wake closes and clears the doorbell so every parked consumer resumes. Parkers install the
// bell under mu, so this cannot miss a waiter that is about to sleep.
func (r *Ring62Haiku[V]) wake() {
	r.mu.Lock()
	if r.bell != nil {
		close(r.bell)
		r.bell = nil
	}
	r.mu.Unlock()
}

// Lag returns the number of published entries not yet consumed by this cursor. Entries
// that were already overwritten still count until a read reports them as dropped.
func (c *Cursor62Haiku[V]) Lag() uint64 {
	return c.r.head.Load() - c.pos
}

// skipTo advances the cursor to target and returns the number of positions skipped.
func (c *Cursor62Haiku[V]) skipTo(target uint64) uint64 {
	if target <= c.pos {
		target = c.pos + 1 // always make progress
	}
	skipped := target - c.pos
	c.pos = target
	return skipped
}

// step attempts to read the entry at c.pos given head snapshot h (c.pos < h). It returns
// ok=false when the caller must retry with a fresh snapshot: a torn copy, or a lap that
// already moved c.pos forward.
func (c *Cursor62Haiku[V]) step(h uint64, out *V) (bool, uint64) {
	r := c.r
	var dropped uint64
	if h-c.pos > r.capacity { // slot for pos has been reused by pos+N or later: provably lost
		dropped += c.skipTo(r.oldest(h))
	}
	slot := r.slot(c.pos)
	want := 2*c.pos + 2
	switch s := atomic.LoadUint64(&slot[0]); {
	case s == want:
		var tmp V
		loadWords62Haiku(slot[1:], &tmp, r.size)
		if atomic.LoadUint64(&slot[0]) != want {
			return false, dropped // a writer started reusing the slot during our copy
		}
		*out = tmp
		c.pos++
		return true, dropped
	case s > want: // lapped between our head snapshot and this load
		dropped += c.skipTo(r.oldest(r.head.Load()))
		return false, dropped
	default: // s < want cannot occur while c.pos < h: commit is ordered before the head publish
		runtime.Gosched()
		return false, dropped
	}
}

// TryNext returns the next entry without blocking. dropped counts positions skipped since
// the previous call (non-zero only when this cursor fell more than capacity entries behind).
// ok is false when nothing is pending; dropped may still be non-zero in that case.
func (c *Cursor62Haiku[V]) TryNext() (v V, dropped uint64, ok bool) {
	for {
		h := c.r.head.Load()
		if c.pos >= h {
			return v, dropped, false
		}
		var d uint64
		ok, d = c.step(h, &v)
		dropped += d
		if ok {
			return v, dropped, true
		}
	}
}

// Next blocks until an entry is available, the ring is closed and drained (ErrClosed62Haiku), or
// ctx is done. dropped reports positions skipped since the previous return, including on
// error returns, so the accounting never loses positions.
func (c *Cursor62Haiku[V]) Next(ctx context.Context) (V, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var skipped uint64
	for {
		v, d, ok := c.TryNext()
		skipped += d
		if ok {
			return v, skipped, nil
		}
		if c.r.closed.Load() {
			// Observing closed means every pre-Close publish is visible; re-check once more.
			v, d, ok = c.TryNext()
			skipped += d
			if ok {
				return v, skipped, nil
			}
			var zero V
			return zero, skipped, ErrClosed62Haiku
		}
		if err := c.park(ctx); err != nil {
			var zero V
			return zero, skipped, err
		}
	}
}

// park blocks until the ring may have news for this cursor, the ring closes, or ctx is done.
func (c *Cursor62Haiku[V]) park(ctx context.Context) error {
	r := c.r
	r.mu.Lock()
	r.waiters.Add(1)
	if r.head.Load() > c.pos || r.closed.Load() {
		r.waiters.Add(-1)
		r.mu.Unlock()
		return nil
	}
	if r.bell == nil {
		r.bell = make(chan struct{})
	}
	bell := r.bell
	r.mu.Unlock()

	var err error
	select {
	case <-bell:
	case <-ctx.Done():
		err = ctx.Err()
	}
	r.waiters.Add(-1)
	return err
}

// Drain copies up to len(dst) pending entries into dst without blocking. It returns the
// number copied and the number of positions skipped. The head load is amortized over the batch.
func (c *Cursor62Haiku[V]) Drain(dst []V) (n int, dropped uint64) {
	h := c.r.head.Load()
	var tmp V
	for n < len(dst) {
		if c.pos >= h {
			if h = c.r.head.Load(); c.pos >= h {
				break
			}
		}
		ok, d := c.step(h, &tmp)
		dropped += d
		if ok {
			dst[n] = tmp
			n++
		}
	}
	return n, dropped
}

// Latest skips to the newest committed entry and returns it. The skipped positions are
// reported in dropped. ok is false when nothing has been published since the last read.
func (c *Cursor62Haiku[V]) Latest() (V, uint64, bool) {
	if h := c.r.head.Load(); c.pos < h {
		var skipped uint64
		if newest := h - 1; newest > c.pos {
			skipped = c.skipTo(newest)
		}
		v, d, ok := c.TryNext()
		return v, skipped + d, ok
	}
	var zero V
	return zero, 0, false
}

// storeWords62Haiku copies the bytes of *v into dst as individually atomic 64-bit words. The
// tail word is zero-padded. Every shared access is atomic, so there are no data races.
func storeWords62Haiku[V any](dst []uint64, v *V, size int) {
	src := unsafe.Slice((*byte)(unsafe.Pointer(v)), size)
	for i := 0; i < size; i += 8 {
		var w uint64
		copy((*[8]byte)(unsafe.Pointer(&w))[:], src[i:])
		atomic.StoreUint64(&dst[i>>3], w)
	}
}

// loadWords62Haiku is the inverse of storeWords62Haiku. It writes into a caller-private value only.
func loadWords62Haiku[V any](src []uint64, out *V, size int) {
	dst := unsafe.Slice((*byte)(unsafe.Pointer(out)), size)
	for i := 0; i < size; i += 8 {
		w := atomic.LoadUint64(&src[i>>3])
		copy(dst[i:], (*[8]byte)(unsafe.Pointer(&w))[:])
	}
}
