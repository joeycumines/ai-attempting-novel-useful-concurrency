// Horizon66Gemini: lock-free, cache-swizzled seqlock MPMC broadcast ring with
// flat-combined batch ingress, positional sequence reads, and epoch-coalesced
// wakeups.
//
// Semantics:
//   - Every Publish/PublishBatch claims a 1-based sequence via a single atomic
//     XADD; readers read any sequence positionally with O(1) TryRead.
//   - Seqlock slots double-checked against the committed stamp; torn reads of
//     pointer-free payloads are impossible, pointer-bearing payloads copy
//     under a per-slot RWMutex.
//   - Overrun is detectable: reads of sequences overwritten by a newer ring
//     lap report StatusOverrun66Gemini; stateful readers (ReadNext/Subscribe)
//     recover per the caller-chosen CatchUpPolicy66Gemini (skip to oldest,
//     skip to latest, or fail).
//   - WaitFor blocks with a spin/yield/Dekker park protocol; ctx.Done
//     cancellation and Close wake parked consumers. Close seals the ring,
//     waits for pre-close claimants to commit, then releases all waiters.
//
// Sequence numbers are 1-based; 0 means no items published.

package bigbuff

import (
	"context"
	"iter"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	minHorizonCap66Gemini = 8
	maxHorizonCap66Gemini = 1 << 30
	closedBit66Gemini     = uint64(1) << 63
	seqMask66Gemini       = closedBit66Gemini - 1
	spinBudget66Gemini    = 24
)

// ReadStatus66Gemini describes the outcome of a non-blocking or blocking Horizon66Gemini read.
type ReadStatus66Gemini uint8

const (
	// StatusOK66Gemini indicates the exact requested sequence was read without tearing.
	StatusOK66Gemini ReadStatus66Gemini = iota
	// StatusNotReady66Gemini indicates the requested sequence has not been committed yet.
	StatusNotReady66Gemini
	// StatusOverrun66Gemini indicates the requested sequence was overwritten by a newer ring lap.
	StatusOverrun66Gemini
	// StatusClosed66Gemini indicates the Horizon66Gemini is closed and the requested sequence exceeds the final item.
	StatusClosed66Gemini
	// StatusCanceled66Gemini indicates the caller's context.Context was canceled while waiting.
	StatusCanceled66Gemini
)

// CatchUpPolicy66Gemini controls how stateful readers (ReadNext / Subscribe) recover when lapped by producers.
type CatchUpPolicy66Gemini uint8

const (
	// CatchUpSkipToOldest66Gemini jumps to the oldest surviving sequence in the bounded ring window.
	CatchUpSkipToOldest66Gemini CatchUpPolicy66Gemini = iota
	// CatchUpSkipToLatest66Gemini jumps directly to the newest committed sequence at the ring tip.
	CatchUpSkipToLatest66Gemini
	// CatchUpFailOnOverrun66Gemini returns StatusOverrun66Gemini immediately (or terminates Subscribe iteration).
	CatchUpFailOnOverrun66Gemini
)

// HorizonStats66Gemini captures a point-in-time diagnostic snapshot of a Horizon66Gemini instance.
type HorizonStats66Gemini struct {
	Capacity      int
	PublishedSeq  uint64
	OldestSeq     uint64
	ParkedWaiters uint64
	Closed        bool
}

// Horizon66Gemini is a lock-free, cache-swizzled Seqlock MPMC broadcast and bounded-history
// telemetry ring with flat-combined batch ingress and coalesced epoch wakeups.
type Horizon66Gemini[V any] struct {
	// Cache line sector 0: immutable configuration after NewHorizon66Gemini (MESI Shared across all cores).
	slots        []horizonSlot66Gemini[V]
	cap          uint64
	mask         uint64
	swizzleShift uint
	hasPtr       bool
	_pad0        [64]byte

	// Cache line sector 1: producer claim cursor (modified via single atomic XADD / LDADD).
	writeSeq atomic.Uint64
	_pad1    [120]byte

	// Cache line sector 2: parked consumer gauge (MESI Shared when 0; touched only on sleep/wake transitions).
	parkedWaiters atomic.Uint64
	_pad2         [120]byte

	// Cache line sector 3: cold lifecycle & coalesced wakeup state.
	finalSeq  atomic.Uint64
	isClosed  atomic.Bool
	closeDone chan struct{}
	waitMu    sync.Mutex
	waitCond  *sync.Cond
	epochCh   chan struct{}
}

// NewHorizon66Gemini constructs a Horizon66Gemini with capacity rounded up to a power of two (minimum 8).
func NewHorizon66Gemini[V any](capacity int) *Horizon66Gemini[V] {
	if capacity < minHorizonCap66Gemini {
		capacity = minHorizonCap66Gemini
	}
	if capacity > maxHorizonCap66Gemini {
		panic("bigbuff: horizon: capacity exceeds maximum limit")
	}
	pow2 := uint64(1) << bits.Len64(uint64(capacity-1))
	k := uint(bits.TrailingZeros64(pow2))

	h := &Horizon66Gemini[V]{
		slots:        make([]horizonSlot66Gemini[V], pow2),
		cap:          pow2,
		mask:         pow2 - 1,
		swizzleShift: k - 3, // k >= 3 because minHorizonCap66Gemini == 8
		hasPtr:       typeHasPointers66Gemini(reflect.TypeFor[V]()),
		closeDone:    make(chan struct{}),
	}
	h.waitCond = sync.NewCond(&h.waitMu)
	return h
}

func (h *Horizon66Gemini[V]) checkInit() {
	if h == nil || h.slots == nil {
		panic("bigbuff: horizon: must be initialized via NewHorizon66Gemini")
	}
}

// physIndex maps a monotonic sequence number onto a physical slot index using a branchless
// 3-bit circular rotation. Any 8 consecutive sequence numbers are separated by cap/8 slots
// in physical memory, eliminating L1/L2 adjacent-line false sharing across concurrent producers.
//
//go:nosplit
func (h *Horizon66Gemini[V]) physIndex(seq uint64) uint64 {
	idx := seq & h.mask
	return ((idx & 7) << h.swizzleShift) | (idx >> 3)
}

// Capacity returns the bounded ring window size (always a power of two >= 8).
func (h *Horizon66Gemini[V]) Capacity() int {
	h.checkInit()
	return int(h.cap)
}

// PublishedSeq returns the highest sequence number claimed or committed prior to Close.
// Sequence numbers are 1-based; 0 means no items have been published yet.
func (h *Horizon66Gemini[V]) PublishedSeq() uint64 {
	h.checkInit()
	if h.isClosed.Load() {
		return h.finalSeq.Load()
	}
	return h.writeSeq.Load() & seqMask66Gemini
}

// OldestAvailableSeq returns the lowest sequence number currently inside the ring window.
func (h *Horizon66Gemini[V]) OldestAvailableSeq() uint64 {
	pub := h.PublishedSeq()
	if pub <= h.cap {
		return 1
	}
	return pub - h.cap + 1
}

// Stats returns a point-in-time snapshot of the Horizon66Gemini state.
func (h *Horizon66Gemini[V]) Stats() HorizonStats66Gemini {
	h.checkInit()
	return HorizonStats66Gemini{
		Capacity:      int(h.cap),
		PublishedSeq:  h.PublishedSeq(),
		OldestSeq:     h.OldestAvailableSeq(),
		ParkedWaiters: h.parkedWaiters.Load(),
		Closed:        h.isClosed.Load(),
	}
}

// Publish writes a single value into the Horizon66Gemini in wait-free O(1) time (1 atomic XADD on the fast path).
// It returns the 1-based monotonic sequence number assigned to value, or (0, false) if closed.
func (h *Horizon66Gemini[V]) Publish(value V) (seq uint64, ok bool) {
	h.checkInit()
	if h.writeSeq.Load()&closedBit66Gemini != 0 {
		return 0, false
	}
	raw := h.writeSeq.Add(1)
	if raw&closedBit66Gemini != 0 {
		return 0, false
	}
	phys := h.physIndex(raw)
	h.writeSlot(&h.slots[phys], raw, value)

	// Store-Load barrier is enforced by slot commit Swap before loading parkedWaiters.
	if h.parkedWaiters.Load() != 0 {
		h.wakeWaiters()
	}
	return raw, true
}

// PublishBatch claims len(values) contiguous sequence numbers using a single atomic XADD,
// populates all swizzled slots, and emits at most one coalesced wakeup.
func (h *Horizon66Gemini[V]) PublishBatch(values []V) (firstSeq, lastSeq uint64, ok bool) {
	h.checkInit()
	n := uint64(len(values))
	if n == 0 {
		return 0, 0, !h.isClosed.Load()
	}
	if h.writeSeq.Load()&closedBit66Gemini != 0 {
		return 0, 0, false
	}
	raw := h.writeSeq.Add(n)
	if raw&closedBit66Gemini != 0 {
		return 0, 0, false
	}
	firstSeq = raw - n + 1
	lastSeq = raw
	for i := uint64(0); i < n; i++ {
		s := firstSeq + i
		phys := h.physIndex(s)
		h.writeSlot(&h.slots[phys], s, values[i])
	}
	if h.parkedWaiters.Load() != 0 {
		h.wakeWaiters()
	}
	return firstSeq, lastSeq, true
}

// TryRead performs a non-blocking O(1) optimistic Seqlock read of exact sequence seq (1-based).
func (h *Horizon66Gemini[V]) TryRead(seq uint64) (val V, status ReadStatus66Gemini) {
	h.checkInit()
	if seq == 0 {
		seq = 1
	}
	phys := h.physIndex(seq)
	val, status = h.readSlot(&h.slots[phys], seq)
	if status == StatusNotReady66Gemini {
		if h.isClosed.Load() && seq > h.finalSeq.Load() {
			return val, StatusClosed66Gemini
		}
		if cur := h.PublishedSeq(); cur >= seq+h.cap {
			return val, StatusOverrun66Gemini
		}
	}
	return val, status
}

// ReadBatch drains up to len(dst) contiguous committed items starting at startSeq into dst
// with zero heap allocations. If startSeq was overrun, ReadBatch automatically fast-forwards
// to the oldest surviving sequence in the ring window and sets overrun = true.
func (h *Horizon66Gemini[V]) ReadBatch(startSeq uint64, dst []V) (count int, nextSeq uint64, overrun bool) {
	h.checkInit()
	if startSeq == 0 {
		startSeq = 1
	}
	nextSeq = startSeq
	limit := len(dst)
	if limit == 0 {
		return 0, nextSeq, false
	}

	for count < limit {
		val, status := h.TryRead(nextSeq)
		switch status {
		case StatusOK66Gemini:
			dst[count] = val
			count++
			nextSeq++
		case StatusOverrun66Gemini:
			overrun = true
			if count > 0 {
				// Return the contiguous prefix already collected before jumping.
				return count, nextSeq, true
			}
			oldest := h.OldestAvailableSeq()
			if oldest > nextSeq {
				nextSeq = oldest
			} else {
				nextSeq++
			}
		case StatusNotReady66Gemini, StatusClosed66Gemini:
			return count, nextSeq, overrun
		}
	}
	return count, nextSeq, overrun
}

// Latest reads the most recently committed value in the Horizon66Gemini in wait-free O(1) time.
// If no items have been published yet, Latest returns (zero, 0, false).
func (h *Horizon66Gemini[V]) Latest() (val V, seq uint64, ok bool) {
	h.checkInit()
	for attempt := 0; attempt < 4; attempt++ {
		pub := h.PublishedSeq()
		if pub == 0 {
			return val, 0, false
		}
		minSeq := uint64(1)
		if pub > h.cap {
			minSeq = pub - h.cap + 1
		}
		// Scan backward from tip so an in-flight writer at pub never stalls reading pub-1.
		for s := pub; s >= minSeq; s-- {
			if v, status := h.TryRead(s); status == StatusOK66Gemini {
				return v, s, true
			}
			if pub-s >= 16 || s == 1 {
				break
			}
		}
		runtime.Gosched()
	}
	return val, 0, false
}

// WaitFor blocks until sequence seq is committed, overrun, closed, or ctx is canceled.
// When ctx.Done() is nil (e.g., context.Background()) or data arrives within the adaptive spin
// window, WaitFor performs zero heap allocations (0 B/op, 0 allocs/op).
func (h *Horizon66Gemini[V]) WaitFor(ctx context.Context, seq uint64) (val V, status ReadStatus66Gemini) {
	h.checkInit()
	if seq == 0 {
		seq = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Phase 1: Immediate optimistic probe + micro-architectural spin poll.
	for i := 0; i < spinBudget66Gemini; i++ {
		val, status = h.TryRead(seq)
		if status != StatusNotReady66Gemini {
			return val, status
		}
		if i >= 4 {
			runtime.Gosched()
		}
	}

	if err := ctx.Err(); err != nil {
		return val, StatusCanceled66Gemini
	}

	// Phase 2: Register as a parked waiter (full atomic RMW barrier before re-checking slot).
	h.parkedWaiters.Add(1)
	defer h.parkedWaiters.Add(^uint64(0))

	doneCh := ctx.Done()
	for {
		val, status = h.TryRead(seq)
		if status != StatusNotReady66Gemini {
			return val, status
		}
		if doneCh != nil {
			select {
			case <-doneCh:
				return val, StatusCanceled66Gemini
			default:
			}
		}

		h.waitMu.Lock()
		val, status = h.TryRead(seq)
		if status != StatusNotReady66Gemini {
			h.waitMu.Unlock()
			return val, status
		}
		if doneCh == nil {
			// Zero-allocation fast wait path when context cannot be canceled.
			h.waitCond.Wait()
			h.waitMu.Unlock()
			continue
		}

		// Cancellable context path: share a single epoch broadcast channel across all N waiters.
		ch := h.epochCh
		if ch == nil {
			ch = make(chan struct{})
			h.epochCh = ch
		}
		h.waitMu.Unlock()

		select {
		case <-ch:
		case <-doneCh:
			return val, StatusCanceled66Gemini
		}
	}
}

// ReadNext blocks for the item at *cursor, automatically applying policy if *cursor was overrun,
// and advances *cursor to seq + 1 upon success.
func (h *Horizon66Gemini[V]) ReadNext(ctx context.Context, cursor *uint64, policy CatchUpPolicy66Gemini) (val V, seq uint64, status ReadStatus66Gemini) {
	h.checkInit()
	if cursor == nil {
		panic("bigbuff: horizon: ReadNext cursor must not be nil")
	}
	if *cursor == 0 {
		*cursor = 1
	}

	for {
		target := *cursor
		val, status = h.WaitFor(ctx, target)
		switch status {
		case StatusOK66Gemini:
			*cursor = target + 1
			return val, target, StatusOK66Gemini

		case StatusOverrun66Gemini:
			switch policy {
			case CatchUpFailOnOverrun66Gemini:
				return val, target, StatusOverrun66Gemini
			case CatchUpSkipToOldest66Gemini:
				oldest := h.OldestAvailableSeq()
				if oldest > target {
					*cursor = oldest
				} else {
					*cursor = target + 1
				}
			case CatchUpSkipToLatest66Gemini:
				if v, latestSeq, ok := h.Latest(); ok && latestSeq >= target {
					*cursor = latestSeq + 1
					return v, latestSeq, StatusOK66Gemini
				}
				oldest := h.OldestAvailableSeq()
				if oldest > target {
					*cursor = oldest
				} else {
					*cursor = target + 1
				}
			default:
				panic("bigbuff: horizon: invalid CatchUpPolicy66Gemini")
			}

		default:
			return val, target, status
		}
	}
}

// Subscribe returns a Go 1.23+ range-over-func iterator (iter.Seq2[uint64, V]) starting at startSeq
// and applying policy whenever the consumer lags behind the bounded ring window.
func (h *Horizon66Gemini[V]) Subscribe(ctx context.Context, startSeq uint64, policy CatchUpPolicy66Gemini) iter.Seq2[uint64, V] {
	h.checkInit()
	if startSeq == 0 {
		startSeq = 1
	}
	return func(yield func(uint64, V) bool) {
		if yield == nil {
			panic("bigbuff: horizon: iterator yield func is nil")
		}
		cursor := startSeq
		for {
			val, seq, status := h.ReadNext(ctx, &cursor, policy)
			if status != StatusOK66Gemini {
				return
			}
			if !yield(seq, val) {
				return
			}
		}
	}
}

// Close seals the Horizon66Gemini against further publications, waits for all in-flight pre-close
// producers to commit their slots, and wakes all blocked consumers.
// Close is idempotent; it returns true on the first call that transitions to closed.
func (h *Horizon66Gemini[V]) Close() bool {
	h.checkInit()
	var final uint64
	for {
		cur := h.writeSeq.Load()
		if cur&closedBit66Gemini != 0 {
			<-h.closeDone
			return false
		}
		if h.writeSeq.CompareAndSwap(cur, cur|closedBit66Gemini) {
			final = cur
			break
		}
	}

	// Wait for any in-flight producers that claimed a sequence <= final before the close barrier
	// to finish committing their slot stamps.
	if final > 0 {
		start := uint64(1)
		if final > h.cap {
			start = final - h.cap + 1
		}
		for s := start; s <= final; s++ {
			phys := h.physIndex(s)
			slot := &h.slots[phys]
			for {
				stamp := slot.stamp.Load()
				slotSeq := stamp >> 1
				writing := (stamp & 1) != 0
				if slotSeq > s || (slotSeq == s && !writing) {
					break
				}
				runtime.Gosched()
			}
		}
	}

	h.finalSeq.Store(final)
	h.isClosed.Store(true)
	h.wakeWaiters()
	close(h.closeDone)
	return true
}

func (h *Horizon66Gemini[V]) wakeWaiters() {
	h.waitMu.Lock()
	h.waitCond.Broadcast()
	if h.epochCh != nil {
		close(h.epochCh)
		h.epochCh = nil
	}
	h.waitMu.Unlock()
}

// typeHasPointers66Gemini inspects t once during NewHorizon66Gemini to determine whether values of type V
// contain any Go pointers (which require GC write-barrier coordination during concurrent overwrites).
func typeHasPointers66Gemini(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return false
	case reflect.Array:
		if t.Len() == 0 {
			return false
		}
		return typeHasPointers66Gemini(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeHasPointers66Gemini(t.Field(i).Type) {
				return true
			}
		}
		return false
	default:
		// Pointers, slices, strings, maps, channels, funcs, interfaces, UnsafePointer.
		return true
	}
}
