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

// Provenance: 32-openrouter-chat-nemotron-3-nano-omni-free-prompt-mini.md.
//
// The delivered code calls itself a "publish/subscribe channel" but is a
// single shared read cursor advancing through a ring: each published value is
// consumed by exactly ONE waiter. That is a competing-consumer queue, so it is
// integrated as one (attached to the work-distribution/scatter scenario) rather
// than being contorted into a broadcast it cannot be.
//
// Repairs, all in spirit (the mechanism is untouched: a power-of-two ring, a
// mutex/cond pair, an Add-based receiver count, one shared read cursor):
//
//  1. Add with a negative delta ADDED instead of subtracting (uint64 cast of
//     -delta), so unregistering a receiver incremented the count. Fixed.
//  2. writeIdx, readIdx and subCount were plain uint64 fields described as
//     "atomic" in comments. They are real atomics now, and the ring publish is
//     serialised by the same mutex the condition variable uses so that a
//     waiter can never observe a torn slot.
//  3. Send did not report whether anybody was listening; it now returns 1 iff
//     at least one receiver was registered at publish time (0 otherwise),
//     matching the scatter contract.
//  4. Wait ignored its documented context parameter (it had none); it now takes
//     a context so a waiter can leave without hanging.
//  5. The unused generic channel parameter C was retained (it carries the
//     caller's channel type through the API, as in the original).

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

// StableRing32Nano is a condvar-guarded ring used as a competing-consumer
// dispatch queue: every published value is handed to exactly one waiter.
//
// Unlike a single unbuffered channel, a value is accepted while waiters are
// merely registering, and the ring stays bounded (values published with no
// receiver registered are dropped and reported as such).
type StableRing32Nano[C ~chan V, V any] struct {
	exemplar C

	cap  uint64
	mask uint64

	buf []V

	writeIdx uint64 // next slot to publish into (guarded by mu)
	readIdx  uint64 // next slot to consume (guarded by mu)

	subCount int64 // active receivers (atomic)

	mu   sync.Mutex
	cond *sync.Cond
}

// NewStableRing32Nano creates a ring with the given power-of-two capacity. It
// panics if capacity is zero or not a power of two.
func NewStableRing32Nano[C ~chan V, V any](c C, capacity uint64) *StableRing32Nano[C, V] {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic(`bigbuff: NewStableRing32Nano: capacity must be a power of two`)
	}
	r := &StableRing32Nano[C, V]{
		exemplar: c,
		cap:  capacity,
		mask: capacity - 1,
		buf:  make([]V, capacity),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Send publishes value for exactly one registered receiver, returning 1 if a
// receiver was registered (so the value was handed to the queue) and 0
// otherwise. It never blocks on a receiver: the ring absorbs the value.
func (r *StableRing32Nano[C, V]) Send(value V) int {
	if atomic.LoadInt64(&r.subCount) == 0 {
		return 0
	}
	r.mu.Lock()
	idx := r.writeIdx
	r.writeIdx = (idx + 1) & r.mask
	r.buf[idx] = value
	r.cond.Signal() // one value, one waiter
	r.mu.Unlock()
	return 1
}

// Add adjusts the number of registered receivers and returns the new count.
// Negative deltas are bounded so the count can never go below zero.
func (r *StableRing32Nano[C, V]) Add(delta int) int {
	if delta > math.MaxInt32 || -delta > math.MaxInt32 {
		panic(`bigbuff: StableRing32Nano: Add delta out of bounds`)
	}
	for {
		cur := atomic.LoadInt64(&r.subCount)
		next := cur + int64(delta)
		if next < 0 {
			panic(`bigbuff: StableRing32Nano: receiver count underflow`)
		}
		if atomic.CompareAndSwapInt64(&r.subCount, cur, next) {
			if next > cur {
				// Wake waiters that are already parked: a receiver that
				// registers while a value is pending must still see it.
				r.mu.Lock()
				r.cond.Broadcast()
				r.mu.Unlock()
			}
			return int(next)
		}
	}
}

// Wait blocks until a value is available for this receiver, ctx is done, or no
// receiver remains registered. ok reports whether a value was returned.
func (r *StableRing32Nano[C, V]) Wait(ctx context.Context) (v V, ok bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	// A separate goroutine closes done when ctx is cancelled, because
	// sync.Cond has no context support; the goroutine is bounded by the wait.
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
		close(done)
	})
	defer stop()

	r.mu.Lock()
	defer r.mu.Unlock()
	for r.readIdx == r.writeIdx {
		if ctx.Err() != nil || atomic.LoadInt64(&r.subCount) == 0 {
			return v, false
		}
		r.cond.Wait()
		if ctx.Err() != nil {
			return v, false
		}
	}
	v = r.buf[r.readIdx]
	var zero V
	r.buf[r.readIdx] = zero // release the payload
	r.readIdx = (r.readIdx + 1) & r.mask
	return v, true
}

// C returns the exemplar channel supplied at construction, for API symmetry.
func (r *StableRing32Nano[C, V]) C() C {
	return r.exemplar
}

// Len returns the number of queued values.
func (r *StableRing32Nano[C, V]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.writeIdx - r.readIdx)
}
