//go:build race

package bigbuff

import (
	"context"
	"iter"
	"sync/atomic"
)

// TelemetryRing63Qwen implementation for the Race Detector.
// Uses atomic.Pointer to satisfy TSAN's happens-before requirements cleanly.
// Note: Publish allocates one *V per call in this build only; the !race
// build is the zero-allocation hot path. Loss policy (LossySnapToLatest)
// and the single-producer contract (concurrent Publish panics) match the
// !race build.
type TelemetryRing63Qwen[V any] struct {
	head         atomic.Uint64
	producerHead uint64
	guard        atomic.Uint32 // single-producer exclusion (see Publish)
	mask         uint64
	vals         []atomic.Pointer[V]
	notifier     *Notifier63Qwen
}

func NewTelemetryRing63Qwen[V any](capacity int, notify bool) *TelemetryRing63Qwen[V] {
	if capacity <= 0 || (capacity&(capacity-1)) != 0 {
		panic("bigbuff: telemetryring63qwen: capacity must be a positive power of 2")
	}
	r := &TelemetryRing63Qwen[V]{
		mask: uint64(capacity - 1),
		vals: make([]atomic.Pointer[V], capacity),
	}
	if notify {
		r.notifier = NewNotifier63Qwen()
	}
	return r
}

func (r *TelemetryRing63Qwen[V]) Publish(v V) {
	if !r.guard.CompareAndSwap(0, 1) {
		panic("bigbuff: telemetryring63qwen: concurrent Publish violates the single-producer contract")
	}
	defer r.guard.Store(0)
	head := r.producerHead
	idx := head & r.mask

	// In race mode, we allocate to satisfy atomic.Pointer semantics safely.
	newV := new(V)
	*newV = v
	r.vals[idx].Store(newV)

	r.producerHead = head + 1
	r.head.Store(head + 1)

	if r.notifier != nil {
		r.notifier.Notify()
	}
}

func (r *TelemetryRing63Qwen[V]) Latest() (V, bool) {
	head := r.head.Load()
	if head == 0 {
		var zero V
		return zero, false
	}
	idx := (head - 1) & r.mask
	ptr := r.vals[idx].Load()
	if ptr == nil {
		var zero V
		return zero, false
	}
	return *ptr, true
}

func (r *TelemetryRing63Qwen[V]) Next(cursor *uint64) (V, bool) {
	head := r.head.Load()
	if head == 0 {
		var zero V
		return zero, false
	}

	c := *cursor
	if c == 0 {
		// Positions are 1-based; zero is the caller's initial cursor.
		c = 1
	}
	// Caught up means every published sequence (1..head) has been
	// delivered, i.e. c > head; c == head still has entry seq == head
	// outstanding and must be delivered.
	if c > head {
		var zero V
		return zero, false
	}

	// Fell a full capacity behind: snap to the newest position
	// (LossySnapToLatest).
	if head-c > r.mask {
		c = head
		*cursor = c
	}

	idx := (c - 1) & r.mask
	ptr := r.vals[idx].Load()
	if ptr == nil {
		var zero V
		return zero, false
	}
	*cursor = c + 1
	return *ptr, true
}

// Head returns the number of values published so far, which is also the
// 1-based position of the newest one. A cursor is the next position to read,
// so Head()+1 makes Next and Subscribe start after everything already
// published ("tail attach"); a cursor of zero starts at the first retained
// entry. Added in the completion phase: the ring had no way to obtain this
// position without reading through the retained history.
func (r *TelemetryRing63Qwen[V]) Head() uint64 { return r.head.Load() }

func (r *TelemetryRing63Qwen[V]) NotifyChan() <-chan struct{} {
	if r.notifier == nil {
		return nil
	}
	return r.notifier.WaitChan()
}

func (r *TelemetryRing63Qwen[V]) Subscribe(ctx context.Context, startCursor uint64) iter.Seq[V] {
	return func(yield func(V) bool) {
		cursor := startCursor
		for {
			if ctx.Err() != nil {
				return
			}
			v, ok := r.NextWait(ctx, &cursor)
			if !ok || !yield(v) {
				return
			}
		}
	}
}
