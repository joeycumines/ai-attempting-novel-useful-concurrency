//go:build !race

package bigbuff

import (
	"context"
	"iter"
	"runtime"
	"sync/atomic"
)

const cacheLineSize63Qwen = 128

// TelemetryRing63Qwen is a wait-free, zero-allocation, bounded-history
// telemetry ring buffer.
//
// Loss policy: if a reader falls a full capacity behind, its cursor silently
// snaps to the newest position (LossySnapToLatest). There is no loss counter;
// pair it with a consumer that only cares about fresh telemetry. Publish is
// single-producer; overlapping Publish calls panic rather than corrupt the
// ring. Sequence positions are 1-based; a cursor value of zero is the
// caller's initial cursor, meaning "read from the first retained entry".
type TelemetryRing63Qwen[V any] struct {
	head         atomic.Uint64
	_pad0        [cacheLineSize63Qwen - 8]byte // Isolate hot reader path
	producerHead uint64
	guard        atomic.Uint32                 // single-producer exclusion (see Publish)
	_pad1        [cacheLineSize63Qwen - 8]byte // Isolate producer state
	mask         uint64
	_pad2        [cacheLineSize63Qwen - 8]byte
	seqs         []atomic.Uint64 // Seqlock counters
	_pad3        [cacheLineSize63Qwen - 8]byte
	vals         []V // Generic value storage
	_pad4        [cacheLineSize63Qwen - 8]byte
	notifier     *Notifier63Qwen
}

func NewTelemetryRing63Qwen[V any](capacity int, notify bool) *TelemetryRing63Qwen[V] {
	if capacity <= 0 || (capacity&(capacity-1)) != 0 {
		panic("bigbuff: telemetryring63qwen: capacity must be a positive power of 2")
	}
	r := &TelemetryRing63Qwen[V]{
		mask: uint64(capacity - 1),
		seqs: make([]atomic.Uint64, capacity),
		vals: make([]V, capacity),
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

	// Seqlock Acquire: Invalidate slot with sentinel value
	r.seqs[idx].Store(^uint64(0))

	// Write value. SC atomic Stores act as full barriers, preventing reordering.
	r.vals[idx] = v

	// Seqlock Release: Publish sequence number
	r.seqs[idx].Store(head + 1)

	r.producerHead = head + 1
	r.head.Store(head + 1)

	if r.notifier != nil {
		r.notifier.Notify()
	}
}

func (r *TelemetryRing63Qwen[V]) Latest() (V, bool) {
	for spins := uint(0); ; spins++ {
		if spins >= 10_000 {
			var zero V
			return zero, false // writer starved or slot never settled; caller may retry
		}
		head := r.head.Load()
		if head == 0 {
			var zero V
			return zero, false
		}
		idx := (head - 1) & r.mask

		seq := r.seqs[idx].Load()
		if seq == ^uint64(0) || seq == 0 {
			if spins&63 == 63 {
				runtime.Gosched()
			}
			continue
		}

		val := r.vals[idx]
		seq2 := r.seqs[idx].Load()

		if seq == seq2 && seq == head {
			return val, true
		}
		if spins&63 == 63 {
			runtime.Gosched()
		}
	}
}

func (r *TelemetryRing63Qwen[V]) Next(cursor *uint64) (V, bool) {
	for spins := uint(0); ; spins++ {
		if spins >= 10_000 {
			var zero V
			return zero, false // slot never settled; caller may retry
		}
		head := r.head.Load()
		if head == 0 {
			var zero V
			return zero, false
		}

		c := *cursor
		if c == 0 {
			// Positions are 1-based; zero is the caller's initial cursor,
			// meaning "read from the first retained entry". Without this
			// normalisation the (c - 1) slot arithmetic underflows and the
			// retry loop never terminates on a ring with head <= capacity.
			c = 1
		}
		// Caught up means every published sequence (1..head) has been
		// delivered, i.e. c > head. c == head still has entry seq == head
		// outstanding and must be delivered.
		if c > head {
			var zero V
			return zero, false
		}

		// Fell a full capacity behind: snap to the newest position
		// (LossySnapToLatest; the missed entries are gone, silently).
		if head-c > r.mask {
			c = head
			*cursor = c
		}

		idx := (c - 1) & r.mask
		seq := r.seqs[idx].Load()
		if seq == ^uint64(0) || seq == 0 {
			if spins&63 == 63 {
				runtime.Gosched()
			}
			continue
		}

		val := r.vals[idx]
		seq2 := r.seqs[idx].Load()

		if seq == seq2 && seq == c {
			*cursor = c + 1
			return val, true
		}

		// If seq != c, the slot was overwritten by a newer item. Snap to latest.
		if seq != c {
			c = head
			*cursor = c
			continue
		}

		if spins&63 == 63 {
			runtime.Gosched()
		}
	}
}

func (r *TelemetryRing63Qwen[V]) NotifyChan() <-chan struct{} {
	if r.notifier == nil {
		return nil
	}
	return r.notifier.WaitChan()
}

func (r *TelemetryRing63Qwen[V]) Subscribe(ctx context.Context, startCursor uint64) iter.Seq[V] {
	return func(yield func(V) bool) {
		cursor := startCursor
		waitChan := r.NotifyChan()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			drained := false
			for {
				v, ok := r.Next(&cursor)
				if !ok {
					break
				}
				drained = true
				if !yield(v) {
					return
				}
			}

			if drained {
				continue
			}

			if waitChan == nil {
				runtime.Gosched()
			} else {
				select {
				case <-ctx.Done():
					return
				case <-waitChan:
				}
			}
		}
	}
}
