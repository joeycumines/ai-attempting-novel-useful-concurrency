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

// BaselineFanIn is the staff-engineer-in-a-vacuum answer to the bounded
// fan-in merge use case (many independent producers, one batch-oriented
// consumer, bounded memory, overload drops with push-time accounting): a
// single shared buffered channel fed by every producer, drained with a
// select-default, counted drops, and no lane isolation at all.
//
// It is deliberately ordinary: one lock domain (the channel), no per-lane
// fairness or isolation, no batching cleverness. Its entire value is that it
// is what a competent engineer would write first; the ported EdgeFanIn68Chatgpt
// earns its complexity (per-lane SPSC rings, an MPSC ready-stack, batch
// cursor publishing) against this yardstick.

import (
	"context"
	"sync"
	"sync/atomic"
)

// BaselineFanIn is a bounded, lossy multi-producer single-consumer merge.
// Capacity is the shared buffer's total size. The zero value is not ready
// for use; construct with NewBaselineFanIn.
type BaselineFanIn[V any] struct {
	// mu serialises the push path against Close so a producer can never
	// send on a closed channel. The consumer never takes mu.
	mu     sync.Mutex
	ch     chan V
	drops  atomic.Uint64
	closed bool
	once   sync.Once
}

// NewBaselineFanIn returns an empty merge with a shared buffer of capacity
// entries. A capacity below 1 is clamped to 1.
func NewBaselineFanIn[V any](capacity int) *BaselineFanIn[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &BaselineFanIn[V]{ch: make(chan V, capacity)}
}

// TryPush offers v to the shared buffer. It returns true iff v was accepted;
// a false return means the buffer was full (drop-newest) or the merge is
// closed. A false return from a full buffer is counted in Drops; a refusal
// after Close is not a drop and is not counted. Never blocks.
func (b *BaselineFanIn[V]) TryPush(v V) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	select {
	case b.ch <- v:
		return true
	default:
		b.drops.Add(1)
		return false
	}
}

// TryPushBatch offers the largest prefix of values that currently fits; the
// remainder is dropped by the caller. Accepted pushes are counted per-call;
// dropped pushes are counted in Drops. Returns the number accepted.
func (b *BaselineFanIn[V]) TryPushBatch(values []V) int {
	n := 0
	for _, v := range values {
		if !b.TryPush(v) {
			break
		}
		n++
	}
	return n
}

// Drops returns the total number of values refused because the buffer was
// full. Refused pushes after Close are not counted (they are not drops, they
// are closed-register writes).
func (b *BaselineFanIn[V]) Drops() uint64 { return b.drops.Load() }

// Drain appends up to len(dst) buffered values to dst and returns the number
// written. It never blocks; it stops at len(dst) or when the buffer is
// momentarily empty (or closed and drained), whichever comes first.
func (b *BaselineFanIn[V]) Drain(dst []V) int {
	n := 0
	for n < len(dst) {
		select {
		case v, ok := <-b.ch:
			if !ok {
				return n
			}
			dst[n] = v
			n++
		default:
			return n
		}
	}
	return n
}

// Next blocks until a value is buffered, the merge is closed and drained, or
// ctx is cancelled. Buffered values are returned before closure is
// reported; on cancellation the error is ctx.Err(). Reading from a closed,
// drained merge returns (zero, false, nil).
func (b *BaselineFanIn[V]) Next(ctx context.Context) (V, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case v, ok := <-b.ch:
		if !ok {
			var zero V
			return zero, false, nil
		}
		return v, true, nil
	case <-ctx.Done():
		var zero V
		return zero, false, ctx.Err()
	}
}

// Close stops accepting values and releases pending buffer readers: after
// Close returns, Next first drains the buffer and then reports closure.
// Idempotent; safe for concurrent calls.
func (b *BaselineFanIn[V]) Close() {
	b.once.Do(func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.closed = true
		close(b.ch)
	})
}
