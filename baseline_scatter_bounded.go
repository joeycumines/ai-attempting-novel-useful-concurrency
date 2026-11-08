package bigbuff

// BaselineBoundedScatterQueue is the ordinary answer to a bounded competing-
// consumer queue with explicit overload eviction: a buffered channel plus a
// mutex around the drop-oldest admission sequence. No ring math, atomics on
// payload slots, or novel wakeup protocol. The mutex makes the "make room then
// enqueue" pair atomic with respect to other publishers; consumers still use
// ordinary channel receive/select semantics.

import (
	"context"
	"sync"
	"sync/atomic"
)

type BaselineBoundedScatterQueue[V any] struct {
	C       chan V
	mu      sync.Mutex
	sendMu  sync.Mutex
	n       int
	dropped atomic.Uint64
}

func NewBaselineBoundedScatterQueue[V any](capacity int) *BaselineBoundedScatterQueue[V] {
	if capacity <= 0 {
		panic("bigbuff: baseline bounded scatter capacity must be positive")
	}
	return &BaselineBoundedScatterQueue[V]{C: make(chan V, capacity)}
}

func (x *BaselineBoundedScatterQueue[V]) Add(delta int) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.n += delta
	if x.n < 0 {
		x.n = 0
		panic("bigbuff: baseline bounded scatter receiver underflow")
	}
	return x.n
}

func (x *BaselineBoundedScatterQueue[V]) Send(v V) int {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.mu.Lock()
	n := x.n
	x.mu.Unlock()
	if n == 0 {
		return 0
	}
	select {
	case x.C <- v:
	default:
		select {
		case <-x.C:
			x.dropped.Add(1)
		default:
		}
		// Publishers are serialized; consumers can only create more space.
		x.C <- v
	}
	return 1
}

func (x *BaselineBoundedScatterQueue[V]) Wait(ctx context.Context) (V, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		var zero V
		return zero, false
	}
	select {
	case <-ctx.Done():
		var zero V
		return zero, false
	case v := <-x.C:
		return v, true
	}
}

func (x *BaselineBoundedScatterQueue[V]) TryWait() (V, bool) {
	select {
	case v := <-x.C:
		return v, true
	default:
		var zero V
		return zero, false
	}
}

func (x *BaselineBoundedScatterQueue[V]) Dropped() uint64 { return x.dropped.Load() }
