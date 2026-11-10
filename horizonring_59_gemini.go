// Copyright 2026 Joseph Cumines
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigbuff

import (
	"context"
	"errors"
	"iter"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	// ErrClosed59Gemini is returned by blocking read operations when the HorizonRing59Gemini
	// has been closed and all published items up to the final sequence have been drained.
	ErrClosed59Gemini = errors.New("bigbuff: horizon ring closed")
)

// CursorStartMode59Gemini specifies where a newly created Cursor59Gemini begins reading.
type CursorStartMode59Gemini uint8

const (
	// StartLatest59Gemini positions the cursor at the live tail so it receives only items
	// published after the cursor is created.
	StartLatest59Gemini CursorStartMode59Gemini = iota

	// StartEarliest59Gemini positions the cursor at the oldest valid item currently retained
	// within the ring's horizon window.
	StartEarliest59Gemini
)

// CursorPolicy59Gemini governs how a Cursor59Gemini behaves when a producer has claimed a sequence
// number inside the active horizon window but has not yet finished writing the slot.
type CursorPolicy59Gemini uint8

const (
	// CursorPolicyOrdered59Gemini waits briefly (via hardware spin and scheduler yield) for an
	// in-window producer to finish committing its slot, preserving strict ordering unless
	// the ring wraps around past the cursor.
	CursorPolicyOrdered59Gemini CursorPolicy59Gemini = iota

	// CursorPolicyWaitFreeSkip59Gemini skips over an uncommitted in-window slot if a producer
	// remains stalled after the cursor's bounded spin budget and newer sequences exist.
	CursorPolicyWaitFreeSkip59Gemini
)

// HorizonConfig59Gemini configures optional parameters for a HorizonRing59Gemini.
type HorizonConfig59Gemini struct {
	// SpinLimit is the number of hardware pause iterations a cursor performs
	// when waiting for an in-progress slot write or before parking. Defaults to 64.
	SpinLimit int

	// YieldLimit is the number of runtime.Gosched() calls a blocking reader performs
	// after spinning before parking on the coalesced epoch channel. Defaults to 4.
	YieldLimit int

	// DefaultPolicy sets the default CursorPolicy59Gemini for new cursors.
	DefaultPolicy CursorPolicy59Gemini
}

// HorizonRing59Gemini is a lock-free, overwritable Seqlock-slot MPMC broadcast and replay ring
// with speculative slot-first polling and coalesced epoch-channel wakeup.
//
// Memory layout is partitioned into four 128-byte cache-line-isolated zones to defeat
// both 64-byte false sharing and 128-byte adjacent-line hardware prefetcher thrashing:
//  1. Read-only geometry and configuration invariants.
//  2. Producer sequence claim counter (writeSeq).
//  3. Coalesced wakeup barrier state (hasWaiters, closed, wakeMu, epochCh).
//  4. Contiguous 128-byte-padded Seqlock slot array.
type HorizonRing59Gemini[T any] struct {
	// --- Zone 1: Immutable Geometry & Configuration (Read-Only Hot Cache Line) ---
	cap        uint64
	mask       uint64
	spinLimit  int
	yieldLimit int
	defPolicy  CursorPolicy59Gemini
	ptrFree    bool
	closedCh   chan struct{}
	slots      []ringSlot59Gemini[T]
	_pad0      [64]byte

	// --- Zone 2: Producer Sequence Claim Line (128B Isolated) ---
	writeSeq atomic.Uint64
	_pad1    [120]byte

	// --- Zone 3: Coalesced Epoch Wakeup & Lifecycle Line (128B Isolated) ---
	hasWaiters atomic.Uint32
	closed     atomic.Bool
	wakeMu     sync.Mutex
	epochCh    chan struct{}
	_pad2      [80]byte
}

// NewHorizonRing59Gemini allocates a new HorizonRing59Gemini with capacity rounded up to the next
// power of two (minimum 2).
func NewHorizonRing59Gemini[T any](capacity int, cfg ...HorizonConfig59Gemini) *HorizonRing59Gemini[T] {
	if capacity < 2 {
		capacity = 2
	}
	pow2 := uint64(1) << bits.Len64(uint64(capacity-1))
	if pow2 < 2 {
		pow2 = 2
	}

	var c HorizonConfig59Gemini
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if c.SpinLimit <= 0 {
		c.SpinLimit = 64
	}
	if c.YieldLimit <= 0 {
		c.YieldLimit = 4
	}

	closedCh := make(chan struct{})
	close(closedCh)

	r := &HorizonRing59Gemini[T]{
		cap:        pow2,
		mask:       pow2 - 1,
		spinLimit:  c.SpinLimit,
		yieldLimit: c.YieldLimit,
		defPolicy:  c.DefaultPolicy,
		ptrFree:    typeIsPointerFree59Gemini(reflect.TypeFor[T]()),
		closedCh:   closedCh,
		slots:      make([]ringSlot59Gemini[T], pow2),
	}
	return r
}

// Cap returns the power-of-two slot capacity of the ring.
func (r *HorizonRing59Gemini[T]) Cap() int {
	return int(r.cap)
}

// IsPointerFree reports whether T contains zero Go pointers and therefore uses
// the pure read-only optimistic Seqlock fast path on non-race builds.
func (r *HorizonRing59Gemini[T]) IsPointerFree() bool {
	return r.ptrFree
}

// HeadSeq returns the highest claimed sequence number (0 if no items have been published).
func (r *HorizonRing59Gemini[T]) HeadSeq() uint64 {
	return r.writeSeq.Load()
}

// OldestSeq returns the lowest sequence number currently within the ring's valid
// retention window, or 0 if no items have been published.
func (r *HorizonRing59Gemini[T]) OldestSeq() uint64 {
	head := r.writeSeq.Load()
	if head == 0 {
		return 0
	}
	if head <= r.cap {
		return 1
	}
	return head - r.cap + 1
}

// Publish publishes a single value into the ring in wait-free O(1) time.
// It returns the monotonically increasing sequence number (>= 1) assigned to the item
// and true, or (0, false) if the ring has been closed.
func (r *HorizonRing59Gemini[T]) Publish(val T) (seq uint64, ok bool) {
	if r.closed.Load() {
		return 0, false
	}
	seq = r.writeSeq.Add(1)
	slot := &r.slots[seq&r.mask]
	slotWrite59Gemini(slot, seq, val, r.ptrFree)

	if r.hasWaiters.Load() != 0 {
		r.wakeWaiters()
	}
	return seq, true
}

// PublishBatch publishes a slice of values into the ring using a single atomic
// sequence reservation and a single wakeup check at the end of the batch.
// It returns [firstSeq, lastSeq] and true, or (0, 0, false) if the ring is closed or vals is empty.
func (r *HorizonRing59Gemini[T]) PublishBatch(vals []T) (firstSeq, lastSeq uint64, ok bool) {
	n := uint64(len(vals))
	if n == 0 || r.closed.Load() {
		return 0, 0, false
	}

	lastSeq = r.writeSeq.Add(n)
	firstSeq = lastSeq - n + 1

	// If batch exceeds ring capacity, only the trailing r.cap items remain in the horizon;
	// skip writing slots that this very batch would immediately overwrite.
	startIdx := uint64(0)
	if n > r.cap {
		startIdx = n - r.cap
	}

	mask := r.mask
	ptrFree := r.ptrFree
	slots := r.slots
	for i := startIdx; i < n; i++ {
		seq := firstSeq + i
		slotWrite59Gemini(&slots[seq&mask], seq, vals[i], ptrFree)
	}

	if r.hasWaiters.Load() != 0 {
		r.wakeWaiters()
	}
	return firstSeq, lastSeq, true
}

// Close marks the ring as closed and broadcasts a final wakeup to all blocked readers
// or select waiters. Close is idempotent and returns true on the first call.
func (r *HorizonRing59Gemini[T]) Close() bool {
	if !r.closed.CompareAndSwap(false, true) {
		return false
	}
	r.wakeMu.Lock()
	r.hasWaiters.Store(0)
	if r.epochCh != nil {
		close(r.epochCh)
		r.epochCh = nil
	}
	r.wakeMu.Unlock()
	return true
}

// IsClosed reports whether Close has been called on the ring.
func (r *HorizonRing59Gemini[T]) IsClosed() bool {
	return r.closed.Load()
}

// WaitChan returns a channel that is closed as soon as the ring's published horizon
// advances past afterSeq, or when the ring is closed.
//
// If the horizon has already advanced past afterSeq (or the ring is closed), WaitChan
// returns an already-closed channel and ready=true with zero heap allocations.
// This enables zero-callback integration with standard Go select statements.
func (r *HorizonRing59Gemini[T]) WaitChan(afterSeq uint64) (ch <-chan struct{}, ready bool) {
	if r.writeSeq.Load() > afterSeq || r.closed.Load() {
		return r.closedCh, true
	}

	r.wakeMu.Lock()
	if r.epochCh == nil {
		r.epochCh = make(chan struct{})
	}
	r.hasWaiters.Store(1)

	// Dekker double-check after publishing hasWaiters = 1.
	if r.writeSeq.Load() > afterSeq || r.closed.Load() {
		r.wakeMu.Unlock()
		return r.closedCh, true
	}
	ch = r.epochCh
	r.wakeMu.Unlock()
	return ch, false
}

func (r *HorizonRing59Gemini[T]) wakeWaiters() {
	r.wakeMu.Lock()
	if r.hasWaiters.Load() != 0 {
		r.hasWaiters.Store(0)
		if r.epochCh != nil {
			close(r.epochCh)
			r.epochCh = nil
		}
	}
	r.wakeMu.Unlock()
}

// Cursor59Gemini represents an autonomous, lock-free reader position over a HorizonRing59Gemini.
// Cursors require zero central registration and never block producers or other cursors.
// A single Cursor59Gemini instance is intended for use by one consumer goroutine at a time;
// multiple consumer goroutines should each create their own Cursor59Gemini via NewCursor59Gemini or Clone.
type Cursor59Gemini[T any] struct {
	ring       *HorizonRing59Gemini[T]
	nextSeq    uint64
	spinLimit  int
	yieldLimit int
	policy     CursorPolicy59Gemini
}

// NewCursor59Gemini creates a new autonomous Cursor59Gemini positioned according to mode.
func (r *HorizonRing59Gemini[T]) NewCursor59Gemini(mode CursorStartMode59Gemini) *Cursor59Gemini[T] {
	var start uint64
	head := r.writeSeq.Load()
	switch mode {
	case StartEarliest59Gemini:
		if head <= r.cap {
			start = 1
		} else {
			start = head - r.cap + 1
		}
	default: // StartLatest59Gemini
		start = head + 1
	}
	return &Cursor59Gemini[T]{
		ring:       r,
		nextSeq:    start,
		spinLimit:  r.spinLimit,
		yieldLimit: r.yieldLimit,
		policy:     r.defPolicy,
	}
}

// NewCursorAt creates a Cursor59Gemini positioned to read starting at the specified sequence number (>= 1).
func (r *HorizonRing59Gemini[T]) NewCursorAt(seq uint64) *Cursor59Gemini[T] {
	if seq < 1 {
		seq = 1
	}
	return &Cursor59Gemini[T]{
		ring:       r,
		nextSeq:    seq,
		spinLimit:  r.spinLimit,
		yieldLimit: r.yieldLimit,
		policy:     r.defPolicy,
	}
}

// SetPolicy configures the cursor's behavior when encountering an in-window uncommitted slot.
func (c *Cursor59Gemini[T]) SetPolicy(policy CursorPolicy59Gemini) {
	c.policy = policy
}

// NextSeq returns the next sequence number the cursor expects to read.
func (c *Cursor59Gemini[T]) NextSeq() uint64 {
	return c.nextSeq
}

// Seek repositions the cursor to the specified sequence number (>= 1).
func (c *Cursor59Gemini[T]) Seek(seq uint64) {
	if seq < 1 {
		seq = 1
	}
	c.nextSeq = seq
}

// Clone returns an independent copy of the cursor at its current sequence position.
func (c *Cursor59Gemini[T]) Clone() *Cursor59Gemini[T] {
	cp := *c
	return &cp
}

// TryRead attempts to read the next available item without parking the goroutine.
//
// If the producer lapped this cursor, TryRead automatically snaps forward to the oldest
// valid sequence in the horizon and returns the exact number of overwritten items in dropped.
// Conservation invariant: when ok == true, seq == (previous NextSeq) + dropped.
func (c *Cursor59Gemini[T]) TryRead() (val T, seq uint64, dropped uint64, ok bool) {
	r := c.ring
	mask := r.mask
	ptrFree := r.ptrFree
	capVal := r.cap

	for {
		targetSeq := c.nextSeq
		slot := &r.slots[targetSeq&mask]

		// 1. Speculative Slot-First Poll: inspect the target slot directly without
		// reading the global r.writeSeq cache line.
		v, obsRawSeq, readOK := slotRead59Gemini(slot, targetSeq, ptrFree)
		if readOK {
			c.nextSeq = targetSeq + 1
			return v, targetSeq, dropped, true
		}

		// 2. If the slot already holds a newer sequence from a future wrap-around,
		// snap forward immediately.
		targetEven := targetSeq << 1
		if obsRawSeq > targetEven+1 {
			head := r.writeSeq.Load()
			oldestValid := targetSeq + 1
			if head >= capVal && head-capVal+1 > oldestValid {
				oldestValid = head - capVal + 1
			}
			slotSeq := obsRawSeq >> 1
			if slotSeq >= capVal && slotSeq-capVal+1 > oldestValid {
				oldestValid = slotSeq - capVal + 1
			}
			dropped += oldestValid - targetSeq
			c.nextSeq = oldestValid
			continue
		}

		// 3. Check global writeSeq to distinguish "caught up to live tail" from
		// "producer claimed targetSeq and is currently mid-write".
		head := r.writeSeq.Load()
		if head < targetSeq {
			return val, 0, dropped, false
		}

		if head-targetSeq >= capVal {
			oldestValid := head - capVal + 1
			dropped += oldestValid - targetSeq
			c.nextSeq = oldestValid
			continue
		}

		// 4. targetSeq is within the active window [head - cap + 1, head] and a producer
		// is mid-write. Spin briefly for the store to complete.
		for spin := 0; spin < c.spinLimit; spin++ {
			cpuPause59Gemini()
			v, obsRawSeq, readOK = slotRead59Gemini(slot, targetSeq, ptrFree)
			if readOK {
				c.nextSeq = targetSeq + 1
				return v, targetSeq, dropped, true
			}
			if obsRawSeq > targetEven+1 {
				break
			}
		}

		if obsRawSeq > targetEven+1 || r.writeSeq.Load()-targetSeq >= capVal {
			continue
		}

		if c.policy == CursorPolicyWaitFreeSkip59Gemini {
			if r.writeSeq.Load() > targetSeq {
				dropped++
				c.nextSeq = targetSeq + 1
				continue
			}
			return val, 0, dropped, false
		}

		// In Ordered policy, yield processor so preempted producer can finish its store.
		runtime.Gosched()
	}
}

// ReadBatch drains up to len(dst) contiguous items into dst without heap allocations.
//
// Every item dst[i] in the returned slice [0:n] is guaranteed to have exact sequence
// number firstSeq + uint64(i). If a wrap-around gap occurred prior to firstSeq, dropped
// reports the exact count of overwritten items skipped before firstSeq.
func (c *Cursor59Gemini[T]) ReadBatch(dst []T) (n int, firstSeq uint64, dropped uint64) {
	maxN := len(dst)
	if maxN == 0 {
		return 0, c.nextSeq, 0
	}

	v0, seq0, drop0, ok := c.TryRead()
	if !ok {
		return 0, c.nextSeq, drop0
	}
	dst[0] = v0
	firstSeq = seq0
	dropped = drop0
	n = 1

	r := c.ring
	mask := r.mask
	ptrFree := r.ptrFree
	slots := r.slots

	for n < maxN {
		targetSeq := c.nextSeq
		slot := &slots[targetSeq&mask]

		// Speculative contiguous slot poll: zero global writeSeq reads while draining.
		v, obsRawSeq, readOK := slotRead59Gemini(slot, targetSeq, ptrFree)
		if readOK {
			dst[n] = v
			n++
			c.nextSeq = targetSeq + 1
			continue
		}

		targetEven := targetSeq << 1
		if obsRawSeq > targetEven+1 {
			// A wrap-around overwrite occurred mid-batch; return the contiguous prefix
			// already collected so firstSeq + i remains exact for all dst[0:n].
			break
		}

		head := r.writeSeq.Load()
		if head < targetSeq || head-targetSeq >= r.cap {
			break
		}

		// Producer is mid-write on targetSeq; spin briefly to keep batch contiguous.
		committed := false
		for spin := 0; spin < c.spinLimit; spin++ {
			cpuPause59Gemini()
			v, obsRawSeq, readOK = slotRead59Gemini(slot, targetSeq, ptrFree)
			if readOK {
				dst[n] = v
				n++
				c.nextSeq = targetSeq + 1
				committed = true
				break
			}
			if obsRawSeq > targetEven+1 {
				break
			}
		}
		if !committed {
			break
		}
	}

	return n, firstSeq, dropped
}

// ReadLatest jumps directly to the newest committed item in the ring in O(1) time,
// advancing the cursor to seq + 1 and reporting how many unread items prior to seq
// were skipped (skipped == seq - previousNextSeq).
func (c *Cursor59Gemini[T]) ReadLatest() (val T, seq uint64, skipped uint64, ok bool) {
	r := c.ring
	mask := r.mask
	ptrFree := r.ptrFree
	capVal := r.cap

	for {
		head := r.writeSeq.Load()
		if head < c.nextSeq {
			return val, 0, 0, false
		}

		// Probe head first; spin briefly in case the producer just incremented writeSeq.
		slot := &r.slots[head&mask]
		for spin := 0; spin < 16; spin++ {
			v, _, readOK := slotRead59Gemini(slot, head, ptrFree)
			if readOK {
				skipped = head - c.nextSeq
				c.nextSeq = head + 1
				return v, head, skipped, true
			}
			cpuPause59Gemini()
		}

		// If head is still mid-write by a preempted producer, scan backward within
		// the valid window to find the highest already-committed sequence.
		minSeq := c.nextSeq
		if head >= capVal && head-capVal+1 > minSeq {
			minSeq = head - capVal + 1
		}
		for probe := head; probe >= minSeq; probe-- {
			v, _, readOK := slotRead59Gemini(&r.slots[probe&mask], probe, ptrFree)
			if readOK {
				skipped = probe - c.nextSeq
				c.nextSeq = probe + 1
				return v, probe, skipped, true
			}
			if probe == 1 {
				break
			}
		}

		if r.writeSeq.Load() == head {
			runtime.Gosched()
		}
	}
}

// ReadContext reads the next available item, using 3-phase adaptive waiting
// (hardware spin -> scheduler yield -> coalesced epoch channel select) if caught up.
// Returns ErrClosed59Gemini once the ring is closed and all published items have been drained,
// or ctx.Err() if the context is canceled while waiting.
func (c *Cursor59Gemini[T]) ReadContext(ctx context.Context) (val T, seq uint64, dropped uint64, err error) {
	var totalDropped uint64
	r := c.ring

	for {
		v, s, d, ok := c.TryRead()
		totalDropped += d
		if ok {
			return v, s, totalDropped, nil
		}

		// Phase 1: Bounded hardware spin on the target slot's Seqlock.
		targetSeq := c.nextSeq
		slot := &r.slots[targetSeq&r.mask]
		for spin := 0; spin < c.spinLimit; spin++ {
			cpuPause59Gemini()
			if slot.seq.Load() >= (targetSeq << 1) {
				break
			}
		}
		v, s, d, ok = c.TryRead()
		totalDropped += d
		if ok {
			return v, s, totalDropped, nil
		}

		// Phase 2: Brief scheduler yield to let runnable producers on the same P execute.
		for y := 0; y < c.yieldLimit; y++ {
			runtime.Gosched()
			v, s, d, ok = c.TryRead()
			totalDropped += d
			if ok {
				return v, s, totalDropped, nil
			}
		}

		if r.closed.Load() && r.writeSeq.Load() < c.nextSeq {
			return val, 0, totalDropped, ErrClosed59Gemini
		}
		if err := ctx.Err(); err != nil {
			return val, 0, totalDropped, err
		}

		// Phase 3: Park on the coalesced epoch broadcast channel.
		waitCh, ready := r.WaitChan(c.nextSeq - 1)
		if ready {
			if r.closed.Load() && r.writeSeq.Load() < c.nextSeq {
				return val, 0, totalDropped, ErrClosed59Gemini
			}
			continue
		}

		select {
		case <-waitCh:
		case <-ctx.Done():
			return val, 0, totalDropped, ctx.Err()
		}
	}
}

// Seq returns a Go 1.23+ range-over-func iterator yielding (seq, value) pairs until
// ctx is canceled, the ring is closed and drained, or the caller breaks out of the loop.
func (c *Cursor59Gemini[T]) Seq(ctx context.Context) iter.Seq2[uint64, T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(uint64, T) bool) {
		for {
			val, seq, _, err := c.ReadContext(ctx)
			if err != nil {
				return
			}
			if !yield(seq, val) {
				return
			}
		}
	}
}

func typeIsPointerFree59Gemini(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return typeIsPointerFree59Gemini(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !typeIsPointerFree59Gemini(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
