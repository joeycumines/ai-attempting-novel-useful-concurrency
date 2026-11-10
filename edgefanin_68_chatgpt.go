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
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	edgeCachePad68Chatgpt  = 128
	edgeBatchSize68Chatgpt = 32
)

var (
	// ErrClosed68Chatgpt is returned by NewProducer after the fan-in has started closing.
	ErrClosed68Chatgpt = errors.New("bigbuff: edgefanin: closed")
	// ErrInvalidCapacity68Chatgpt indicates that a producer capacity was not positive or
	// could not be rounded up to a representable power of two.
	ErrInvalidCapacity68Chatgpt = errors.New("bigbuff: edgefanin: invalid capacity")
)

// EdgeFanIn68Chatgpt is a bounded, lossy M:1 fan-in queue for a set of producer
// goroutines. Its intended workload is many independent writers feeding one
// batch-oriented reader.
//
// Each Producer is an SPSC lane: exactly one goroutine may call TryPush or
// TryPushBatch on a given Producer, while exactly one goroutine consumes from
// the EdgeFanIn68Chatgpt. Different Producer values may be used concurrently. The
// consumer may use TryPop, Drain, or Next, but must not use them concurrently
// with one another.
//
// A full lane drops new values; it never overwrites accepted values. Ordering
// is FIFO within each producer lane. There is intentionally no global ordering
// between producers. Capacity is bounded independently per producer.
//
// The zero value is not ready for use; construct with NewEdgeFanIn68Chatgpt.
type EdgeFanIn68Chatgpt[V any] struct {
	ready atomic.Pointer[edgeLane68Chatgpt[V]]
	wake  chan struct{}
	done  chan struct{}

	// mu and lanes are control-plane state only. The hot path never takes mu.
	mu      sync.Mutex
	lanes   []*edgeLane68Chatgpt[V]
	closing atomic.Bool
	closed  atomic.Bool

	// These fields belong exclusively to the single consumer.
	pending *edgeLane68Chatgpt[V]
	current *edgeLane68Chatgpt[V]
	budget  uint32
}

// EdgeProducer68Chatgpt is one SPSC ingress lane belonging to an EdgeFanIn68Chatgpt.
// Do not copy an EdgeProducer68Chatgpt after first use. TryPush and TryPushBatch must
// be called by only one goroutine at a time; concurrent calls panic rather than
// silently corrupt the SPSC ring.
type EdgeProducer68Chatgpt[V any] struct {
	queue *EdgeFanIn68Chatgpt[V]
	lane  *edgeLane68Chatgpt[V]
}

// Producer and consumer cursors occupy distinct padded regions. This reduces
// false sharing between the two sides of a lane. The padding is conservative
// for contemporary x86-64 and arm64 systems, not a cache-line-size guarantee.
type edgeProducerState68Chatgpt struct {
	_         [edgeCachePad68Chatgpt]byte
	active    atomic.Bool
	tail      uint64 // producer goroutine only
	published atomic.Uint64
	_         [104]byte
}

type edgeConsumerState68Chatgpt struct {
	_        [edgeCachePad68Chatgpt]byte
	head     uint64 // consumer goroutine only
	consumed atomic.Uint64
	_        [112]byte
}

type edgeLane68Chatgpt[V any] struct {
	producer edgeProducerState68Chatgpt
	consumer edgeConsumerState68Chatgpt

	slots []V
	mask  uint64

	// queued protects the intrusive ready-stack node. A lane is present in the
	// MPSC ready stack at most once, however many values it contains.
	queued atomic.Bool
	next   atomic.Pointer[edgeLane68Chatgpt[V]]
	closed atomic.Bool
	queue  *EdgeFanIn68Chatgpt[V]
}

// NewEdgeFanIn68Chatgpt constructs an empty fan-in. It starts no goroutines.
func NewEdgeFanIn68Chatgpt[V any]() *EdgeFanIn68Chatgpt[V] {
	return &EdgeFanIn68Chatgpt[V]{
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

// NewProducer adds a bounded SPSC lane. Capacity is rounded up to a power of
// two, so the actual capacity may be greater than requested. A capacity of 1
// is valid. The lane may be added while the queue is being consumed, but not
// after Close has begun.
func (q *EdgeFanIn68Chatgpt[V]) NewProducer(capacity int) (*EdgeProducer68Chatgpt[V], error) {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn68Chatgpt")
	}
	n, ok := edgeRoundCapacity68Chatgpt(capacity)
	if !ok {
		return nil, ErrInvalidCapacity68Chatgpt
	}

	l := &edgeLane68Chatgpt[V]{
		slots: make([]V, n),
		mask:  uint64(n - 1),
		queue: q,
	}

	q.mu.Lock()
	if q.closing.Load() {
		q.mu.Unlock()
		return nil, ErrClosed68Chatgpt
	}
	q.lanes = append(q.lanes, l)
	q.mu.Unlock()

	return &EdgeProducer68Chatgpt[V]{queue: q, lane: l}, nil
}

func edgeRoundCapacity68Chatgpt(n int) (int, bool) {
	if n < 1 {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	v := 1
	for v < n {
		if v > maxInt/2 {
			return 0, false
		}
		v <<= 1
	}
	return v, true
}

// Capacity reports the lane's rounded-up capacity.
func (p *EdgeProducer68Chatgpt[V]) Capacity() int {
	if p == nil || p.lane == nil {
		return 0
	}
	return len(p.lane.slots)
}

// TryPush inserts value if the lane has room and remains open. It is a
// non-blocking drop-newest operation: false means either full or closed.
// For sustained high-rate traffic, TryPushBatch amortizes publication and is
// the preferred producer API.
func (p *EdgeProducer68Chatgpt[V]) TryPush(value V) bool {
	var one [1]V
	one[0] = value
	return p.TryPushBatch(one[:]) == 1
}

// TryPushBatch inserts the largest prefix of values that currently fits and
// returns the number accepted. The remaining suffix is dropped by the caller.
// It publishes the entire accepted prefix with one cursor store, making this
// method particularly effective when producers naturally emit batches.
//
// TryPushBatch performs no allocation. A zero-length input returns zero.
func (p *EdgeProducer68Chatgpt[V]) TryPushBatch(values []V) int {
	if p == nil || p.lane == nil {
		panic("bigbuff: edgefanin: nil producer")
	}
	if len(values) == 0 {
		return 0
	}
	q, l := p.queue, p.lane
	if q.closing.Load() || l.closed.Load() {
		return 0
	}
	if !l.producer.active.CompareAndSwap(false, true) {
		if q.closing.Load() || l.closed.Load() {
			return 0
		}
		panic("bigbuff: edgefanin: concurrent push on one producer")
	}

	// This second check forms the admission boundary. A push that passes it is
	// in-flight, and Close waits for active to clear before returning.
	if q.closing.Load() || l.closed.Load() {
		l.producer.active.Store(false)
		return 0
	}

	tail := l.producer.tail
	consumed := l.consumer.consumed.Load()
	used := tail - consumed
	capacity := uint64(len(l.slots))
	if used > capacity {
		l.producer.active.Store(false)
		panic("bigbuff: edgefanin: producer/consumer cursor invariant violated")
	}
	free := int(capacity - used)
	n := len(values)
	if n > free {
		n = free
	}
	if n == 0 {
		l.producer.active.Store(false)
		return 0
	}

	index := int(tail & l.mask)
	first := len(l.slots) - index
	if first > n {
		first = n
	}
	copy(l.slots[index:index+first], values[:first])
	if first < n {
		copy(l.slots[:n-first], values[first:n])
	}

	tail += uint64(n)
	l.producer.tail = tail
	l.producer.published.Store(tail)

	// Read-only queued loads keep the shared ready bit in Shared state during
	// bursts. Only an idle-to-ready edge performs a contended CAS and stack push.
	if !l.queued.Load() && l.queued.CompareAndSwap(false, true) {
		q.pushReady(l)
		q.signal()
	}

	l.producer.active.Store(false)
	return n
}

// Close prevents future pushes on this lane and waits for an in-flight push.
// Values already accepted remain available to the EdgeFanIn68Chatgpt consumer.
func (p *EdgeProducer68Chatgpt[V]) Close() {
	if p == nil || p.lane == nil {
		return
	}
	p.lane.close()
}

func (l *edgeLane68Chatgpt[V]) close() {
	l.closed.Store(true)
	for spins := 0; l.producer.active.Load(); spins++ {
		if spins&63 == 63 {
			runtime.Gosched()
		}
	}
}

// TryPop returns one value if any producer lane has one ready. It is intended
// for low-latency consumers; Drain is more efficient for batch consumers.
// Ordering is FIFO within each producer lane; inter-lane selection is
// intentionally unspecified.
func (q *EdgeFanIn68Chatgpt[V]) TryPop() (V, bool) {
	var one [1]V
	if q.Drain(one[:]) == 0 {
		var zero V
		return zero, false
	}
	return one[0], true
}

// Drain appends up to len(dst) values to dst and returns the number written.
// It amortizes cursor loads/stores across each lane batch and performs no
// allocation. A zero-length destination is a no-op.
func (q *EdgeFanIn68Chatgpt[V]) Drain(dst []V) int {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn68Chatgpt")
	}
	written := 0
	for written < len(dst) {
		l := q.current
		if l == nil {
			if q.pending == nil {
				q.pending = q.ready.Swap(nil)
			}
			l = q.pending
			if l == nil {
				return written
			}
			// Capture next before re-arm can reuse this intrusive node.
			q.pending = l.next.Load()
			q.current = l
			q.budget = edgeBatchSize68Chatgpt
		}

		published := l.producer.published.Load()
		head := l.consumer.head
		available := published - head
		if available == 0 {
			q.rearm(l)
			q.current = nil
			continue
		}
		if available > uint64(len(l.slots)) {
			panic("bigbuff: edgefanin: published cursor invariant violated")
		}

		n := len(dst) - written
		if n > int(available) {
			n = int(available)
		}
		if n > int(q.budget) {
			n = int(q.budget)
		}

		index := int(head & l.mask)
		first := len(l.slots) - index
		if first > n {
			first = n
		}
		copy(dst[written:written+first], l.slots[index:index+first])
		clear(l.slots[index : index+first])
		if first < n {
			copy(dst[written+first:written+n], l.slots[:n-first])
			clear(l.slots[:n-first])
		}

		head += uint64(n)
		l.consumer.head = head
		l.consumer.consumed.Store(head)
		q.budget -= uint32(n)
		written += n

		// Retain this lane as current when dst filled before the lane batch did.
		// Otherwise rotate it back to the ready stack after a bounded quantum.
		if q.budget == 0 || head == published {
			q.rearm(l)
			q.current = nil
		}
	}
	return written
}

// Next blocks until a value is available, ctx is cancelled, or Close has
// stopped all producers and the queue has been drained. Buffered values are
// returned before a cancellation is reported. A nil context means Background.
// The bool is false only when the queue is fully closed and drained; on context
// cancellation, Next returns ctx.Err().
func (q *EdgeFanIn68Chatgpt[V]) Next(ctx context.Context) (value V, ok bool, err error) {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn68Chatgpt")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if value, ok = q.TryPop(); ok {
			return value, true, nil
		}
		if q.closed.Load() {
			return value, false, nil
		}
		select {
		case <-ctx.Done():
			return value, false, ctx.Err()
		case <-q.wake:
		case <-q.done:
		}
	}
}

// Close rejects new pushes, waits for all admitted pushes to finish, then
// wakes blocked Next calls. It does not discard already accepted values and
// starts no teardown goroutines. Close is idempotent and safe for concurrent
// callers.
func (q *EdgeFanIn68Chatgpt[V]) Close() {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn68Chatgpt")
	}

	q.mu.Lock()
	if q.closing.Load() {
		done := q.done
		q.mu.Unlock()
		<-done
		return
	}
	q.closing.Store(true)
	lanes := q.lanes
	q.mu.Unlock()

	for _, l := range lanes {
		l.close()
	}

	q.closed.Store(true)
	close(q.done)
}

func (q *EdgeFanIn68Chatgpt[V]) pushReady(l *edgeLane68Chatgpt[V]) {
	for {
		head := q.ready.Load()
		l.next.Store(head)
		if q.ready.CompareAndSwap(head, l) {
			return
		}
	}
}

// rearm closes the lost-wakeup window around clearing queued. If a producer
// published while queued was true, the post-clear published check puts the
// lane back. If it publishes later, that producer wins the false->true edge.
func (q *EdgeFanIn68Chatgpt[V]) rearm(l *edgeLane68Chatgpt[V]) {
	l.queued.Store(false)
	if l.consumer.head != l.producer.published.Load() &&
		l.queued.CompareAndSwap(false, true) {
		q.pushReady(l) // the consumer is already active; no wake token is needed
	}
}

func (q *EdgeFanIn68Chatgpt[V]) signal() {
	select {
	case q.wake <- struct{}{}:
	default: // one buffered token coalesces arbitrarily many ready edges
	}
}
