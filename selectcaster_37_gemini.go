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

// Provenance: 37-gemini-3.8-flash-high-prompt-mini.md (Gemini, response #37).
//
// Exported identifiers are response-scoped to avoid collisions with other
// extracted candidate APIs in this flat package. Subscribe is serialized against
// Close: the original CAS-only membership loop could publish a node after Close
// detached the list, leaking an active subscription that no future Close could
// reach. Send remains lock-free; this small mutex is restricted to membership
// creation/close.

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// SelectPolicy37Gemini defines how backpressure is handled when a subscriber's channel buffer is full.
type SelectPolicy37Gemini uint8

const (
	// SelectPolicy37Sliding evicts the oldest buffered element to make room for the newest.
	// This ensures real-time subscribers always receive fresh data without halting
	// the publisher or other subscribers.
	SelectPolicy37Sliding SelectPolicy37Gemini = iota

	// SelectPolicy37Dropping drops the incoming value if the subscriber's buffer is saturated.
	SelectPolicy37Dropping

	// SelectPolicy37Blocking blocks the publisher until space is available or the subscriber
	// is closed/unsubscribed.
	SelectPolicy37Blocking
)

const (
	selectCaster37Closed = -1
	selectCaster37Idle   = 0
)

// selectCaster37Pad guards against false sharing across cache lines on modern multicore CPUs.
type selectCaster37Pad [64]byte

// SelectCaster37Gemini is a lock-free, zero-allocation, high-throughput broadcast engine
// designed for seamless integration with Go select blocks.
type SelectCaster37Gemini[V any] struct {
	// mu orders Subscribe against Close. Send remains lock-free; without this
	// gate, Subscribe could CAS a new node after Close detached the list.
	mu          sync.Mutex
	_           selectCaster37Pad
	head        atomic.Pointer[SelectSubscription37Gemini[V]]
	subscribers atomic.Int64
	closed      atomic.Bool
	closedCh    chan struct{}
	_           selectCaster37Pad
	defaultBuf  int
	policy      SelectPolicy37Gemini
}

// SelectSubscription37Gemini is an intrusive, selectable consumer handle.
type SelectSubscription37Gemini[V any] struct {
	caster  *SelectCaster37Gemini[V]
	ch      chan V
	done    chan struct{}
	policy  SelectPolicy37Gemini
	state   atomic.Int32 // >=0: active sender leases, -1: closed
	closed  atomic.Bool
	dropped atomic.Uint64
	next    atomic.Pointer[SelectSubscription37Gemini[V]]
}

// SelectOption37Gemini configures broadcaster and subscription characteristics.
type SelectOption37Gemini func(*selectCaster37Config)

type selectCaster37Config struct {
	bufferSize int
	policy     SelectPolicy37Gemini
}

// WithSelectCaster37BufferSize sets the internal channel buffer capacity for subscribers.
func WithSelectCaster37BufferSize(size int) SelectOption37Gemini {
	return func(c *selectCaster37Config) {
		if size < 1 {
			size = 1
		}
		c.bufferSize = size
	}
}

// WithSelectCaster37Policy sets the overflow/backpressure policy.
func WithSelectCaster37Policy(p SelectPolicy37Gemini) SelectOption37Gemini {
	return func(c *selectCaster37Config) {
		c.policy = p
	}
}

// NewSelectCaster creates a new high-performance, non-blocking broadcast engine.
func NewSelectCaster37Gemini[V any](opts ...SelectOption37Gemini) *SelectCaster37Gemini[V] {
	cfg := selectCaster37Config{
		bufferSize: 16,
		policy:     SelectPolicy37Sliding,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &SelectCaster37Gemini[V]{
		closedCh:   make(chan struct{}),
		defaultBuf: cfg.bufferSize,
		policy:     cfg.policy,
	}
}

// Subscribe allocates an intrusive subscription node and registers it using an
// atomic, wait-free CAS loop. Zero map lookups, zero slice reallocations.
func (c *SelectCaster37Gemini[V]) Subscribe(opts ...SelectOption37Gemini) *SelectSubscription37Gemini[V] {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		panic("bigbuff: selectcaster: subscribe on closed broadcaster")
	}

	cfg := selectCaster37Config{
		bufferSize: c.defaultBuf,
		policy:     c.policy,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	sub := &SelectSubscription37Gemini[V]{
		caster: c,
		ch:     make(chan V, cfg.bufferSize),
		done:   make(chan struct{}),
		policy: cfg.policy,
	}

	for {
		head := c.head.Load()
		sub.next.Store(head)
		if c.head.CompareAndSwap(head, sub) {
			c.subscribers.Add(1)
			break
		}
	}

	return sub
}

// SubscribeContext creates a subscription that automatically unregisters itself
// when ctx is cancelled or times out.
func (c *SelectCaster37Gemini[V]) SubscribeContext(ctx context.Context, opts ...SelectOption37Gemini) *SelectSubscription37Gemini[V] {
	sub := c.Subscribe(opts...)
	if ctx != nil && ctx.Done() != nil {
		context.AfterFunc(ctx, sub.Close)
	}
	return sub
}

// Send broadcasts val to all active subscribers.
//
// Characteristics:
// - Zero heap allocations.
// - Zero global mutex acquisitions.
// - O(1) fast-path return when subscriber count is zero.
// - Self-pruning: dead subscriptions encountered during traversal are lazily unlinked.
func (c *SelectCaster37Gemini[V]) Send(val V) (delivered int) {
	if c.closed.Load() {
		return 0
	}
	if c.subscribers.Load() == 0 {
		return 0 // Fast-path: no consumers
	}

	var prev *SelectSubscription37Gemini[V]
	curr := c.head.Load()

	for curr != nil {
		next := curr.next.Load()

		// Lazy self-pruning of closed nodes
		if curr.state.Load() < 0 {
			if prev == nil {
				if !c.head.CompareAndSwap(curr, next) {
					// CAS conflict on head; restart scan from updated head
					curr = c.head.Load()
					prev = nil
					continue
				}
			} else {
				prev.next.CompareAndSwap(curr, next)
			}
			curr = next
			continue
		}

		if curr.deliver(val) {
			delivered++
		}

		prev = curr
		curr = next
	}

	return delivered
}

// deliver safely routes val into the subscriber's channel using an Atomic Sender Lease.
func (s *SelectSubscription37Gemini[V]) deliver(val V) bool {
	// Acquire Sender Lease: guarantees the channel will not be closed during send
	for {
		cur := s.state.Load()
		if cur < 0 {
			return false // Subscriber is closed
		}
		if s.state.CompareAndSwap(cur, cur+1) {
			break
		}
	}
	defer s.state.Add(-1)

	switch s.policy {
	case SelectPolicy37Sliding:
		for {
			select {
			case s.ch <- val:
				return true
			default:
				// Saturated channel buffer: evict the oldest unread element
				select {
				case <-s.ch:
					s.dropped.Add(1)
				default:
					// Eviction race won by receiver drain; retry delivery
				}
			}
		}

	case SelectPolicy37Dropping:
		select {
		case s.ch <- val:
			return true
		default:
			s.dropped.Add(1)
			return false
		}

	case SelectPolicy37Blocking:
		select {
		case s.ch <- val:
			return true
		case <-s.done:
			return false
		case <-s.caster.closedCh:
			return false
		}

	default:
		return false
	}
}

// C exposes the receive-only channel for idiomatic, direct use inside Go select statements.
func (s *SelectSubscription37Gemini[V]) C() <-chan V {
	return s.ch
}

// Done returns a channel that is closed when the subscription is terminated.
func (s *SelectSubscription37Gemini[V]) Done() <-chan struct{} {
	return s.done
}

// Dropped returns the count of messages evicted or dropped due to buffer saturation.
func (s *SelectSubscription37Gemini[V]) Dropped() uint64 {
	return s.dropped.Load()
}

// Close idempotently and safely shuts down the subscription.
//
// It guarantees that:
// 1. Any in-flight blocking send aborts immediately.
// 2. No sender will ever trigger a "send on closed channel" panic.
// 3. The underlying channel is drained and closed cleanly.
func (s *SelectSubscription37Gemini[V]) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return // Already closed
	}

	// Step 1: Interrupt any blocked SelectPolicy37Blocking senders
	close(s.done)

	// Step 2: Atomic Lease Gate shutdown.
	// Spin/yield until active sender leases drop to zero, then CAS to selectCaster37Closed (-1).
	for {
		cur := s.state.Load()
		if cur <= 0 {
			if s.state.CompareAndSwap(cur, selectCaster37Closed) {
				break
			}
		} else {
			runtime.Gosched()
		}
	}

	// Step 3: Now that lease gate is locked at -1, no sender can ever enter deliver()
	close(s.ch)

	// Step 4: Decrement hub subscriber count
	s.caster.subscribers.Add(-1)
}

// Values implements a standard Go range-over-func iterator (iter.Seq[V]).
func (s *SelectSubscription37Gemini[V]) Values(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		if ctx == nil {
			ctx = context.Background()
		}
		stop := context.AfterFunc(ctx, s.Close)
		defer stop()
		defer s.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case v, ok := <-s.ch:
				if !ok {
					return
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// Values returns an iterator directly from the broadcaster.
func (c *SelectCaster37Gemini[V]) Values(ctx context.Context, opts ...SelectOption37Gemini) iter.Seq[V] {
	return func(yield func(V) bool) {
		sub := c.SubscribeContext(ctx, opts...)
		defer sub.Close()
		for v := range sub.Values(ctx) {
			if !yield(v) {
				return
			}
		}
	}
}

// Len returns the current count of active registered subscribers.
func (c *SelectCaster37Gemini[V]) Len() int {
	n := c.subscribers.Load()
	if n < 0 {
		return 0
	}
	return int(n)
}

// Close terminates the broadcaster and tears down all registered subscriptions.
func (c *SelectCaster37Gemini[V]) Close() {
	c.mu.Lock()
	if !c.closed.CompareAndSwap(false, true) {
		c.mu.Unlock()
		return
	}
	close(c.closedCh)

	// Detach the subscriber chain while Subscribe is excluded. Otherwise a
	// subscriber that passed its initial closed check could link itself after
	// this swap and remain live forever on a closed caster.
	curr := c.head.Swap(nil)
	c.mu.Unlock()
	for curr != nil {
		next := curr.next.Load()
		curr.Close()
		curr = next
	}
}
