package bigbuff

// Provenance: 14-xing4.0-29b-a4b.md, section 3 "RingFanOut". The response
// deliberately withheld source and specified a central dispatcher, pooled
// subscriber channels, bounded buffering, Add/Remove, selectable C and Close.
// This is a completed interpretation, not a claim of verbatim source extraction.
//
// The prose does not resolve overflow, the identity of its singular C(), or
// how "in-place" delivery can work with independently consumed channels. We
// make those choices explicit: drop-oldest at both stages with separate counters;
// C() is the selectable input transport, NOT a subscription (use Add); ordinary
// channel copies preserve payload ownership. We do not retain the unsupported
// zero-allocation/O(1)-at-any-fan-out claims: channel operations allocate and
// dispatch scans the active subscribers. Membership slices have fixed capacity.
//
// Add/Remove use channel handles, not numeric deltas. Remove serializes with
// dispatch and drains the channel before reuse. Its caller must stop selecting
// on the handle: a stale handle can observe a later tenant. SubscribeContext
// owns that retirement boundary. Close waits for all callers, stops admission,
// abandons undispatched input, and closes pooled channels; buffered mail remains
// drainable. Idle-period sends are not retained for future subscribers.

import (
	"context"
	"errors"
	"iter"
	"sync"
)

var (
	ErrRingFanOut14Closed    = errors.New("bigbuff: ringfanout14: closed")
	ErrRingFanOut14Exhausted = errors.New("bigbuff: ringfanout14: subscriber pool exhausted")
)

type RingFanOut14Xing[C ~chan V, V any] struct {
	mu              sync.Mutex // admission, membership, shutdown and counters
	input           chan V
	active          []C
	free            []C
	pool            []C
	inputDropped    uint64
	dispatchDropped uint64
	done            chan struct{}
	closed          bool
	dispatching     bool
	tornDown        bool
	shutdown        sync.Cond
}

func NewRingFanOut14Xing[C ~chan V, V any](queueCapacity, maxSubscribers, subscriberCapacity int) *RingFanOut14Xing[C, V] {
	return newRingFanOut14Xing[C, V](queueCapacity, maxSubscribers, subscriberCapacity, true)
}

// start=false is the serial model/benchmark configuration. No background
// dispatcher exists in that configuration; the caller explicitly dispatches.
func newRingFanOut14Xing[C ~chan V, V any](queueCapacity, maxSubscribers, subscriberCapacity int, start bool) *RingFanOut14Xing[C, V] {
	if queueCapacity <= 0 || maxSubscribers <= 0 || subscriberCapacity <= 0 {
		panic("bigbuff: ringfanout14: capacities must be positive")
	}
	x := &RingFanOut14Xing[C, V]{
		input:  make(chan V, queueCapacity),
		active: make([]C, 0, maxSubscribers),
		free:   make([]C, 0, maxSubscribers),
		pool:   make([]C, 0, maxSubscribers),
		done:   make(chan struct{}),
	}
	x.shutdown.L = &x.mu
	for range maxSubscribers {
		ch := make(chan V, subscriberCapacity)
		x.pool = append(x.pool, ch)
		x.free = append(x.free, ch)
	}
	if start {
		go x.run()
	}
	return x
}

func (x *RingFanOut14Xing[C, V]) run() {
	for {
		select {
		case <-x.done:
			return
		case v := <-x.input:
			x.dispatch(v)
		}
	}
}

// Send enqueues one value without waiting for subscriber readiness. An error
// means closed; success does not imply delivery to every subscriber. Inspect
// InputDropped and DispatchDropped after dispatch to account for overload.
func (x *RingFanOut14Xing[C, V]) Send(value V) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.enqueueLocked(value)
}

func (x *RingFanOut14Xing[C, V]) enqueueLocked(value V) error {
	if x.closed {
		return ErrRingFanOut14Closed
	}
	if len(x.active) == 0 {
		return nil
	}
	select {
	case x.input <- value:
	default:
		// Admission is serialized with dispatch, so a full queue stays full
		// until this eviction completes. No blocking fallback send is needed.
		<-x.input
		x.inputDropped++
		x.input <- value
	}
	return nil
}

func (x *RingFanOut14Xing[C, V]) dispatch(value V) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return
	}
	x.dispatching = true
	defer func() {
		x.dispatching = false
		if x.closed {
			x.shutdown.Broadcast()
		}
	}()
	for _, ch := range x.active {
		select {
		case ch <- value:
		default:
			<-ch
			x.dispatchDropped++
			ch <- value
		}
	}
}

// C exposes the input transport for select-based integrations. Receiving from
// it competes with the dispatcher and is not a broadcast subscription. Use Add
// or SubscribeContext to receive published values independently.
func (x *RingFanOut14Xing[C, V]) C() C { return x.input }

// Add leases a pooled channel. Pool exhaustion is an error, not an allocation.
func (x *RingFanOut14Xing[C, V]) Add() (C, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	var zero C
	if x.closed {
		return zero, ErrRingFanOut14Closed
	}
	if len(x.free) == 0 {
		return zero, ErrRingFanOut14Exhausted
	}
	ch := x.free[len(x.free)-1]
	x.free = x.free[:len(x.free)-1]
	x.active = append(x.active, ch)
	return ch, nil
}

// Remove drains and returns an active channel to the pool. The caller must
// no longer select on ch after retirement. A stale/already removed handle is
// rejected without draining another tenant's channel.
func (x *RingFanOut14Xing[C, V]) Remove(ch C) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return false
	}
	for i, active := range x.active {
		if active == ch {
			x.active[i] = x.active[len(x.active)-1]
			x.active[len(x.active)-1] = ch
			x.active = x.active[:len(x.active)-1]
			for {
				select {
				case <-ch:
				default:
					x.free = append(x.free, ch)
					return true
				}
			}
		}
	}
	return false
}

// Close is idempotent. Every caller waits for teardown, including a concurrent
// second caller. It must not be called from a subscriber's iterator callback.
func (x *RingFanOut14Xing[C, V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.closed {
		x.closed = true
		close(x.done)
	}
	for !x.tornDown {
		// The first caller to observe an idle dispatcher performs teardown.
		// Cond.Wait releases mu, allowing an in-flight dispatch to complete.
		if !x.dispatching {
			for _, ch := range x.pool {
				close(ch)
			}
			clear(x.active)
			clear(x.free)
			clear(x.pool)
			x.active = x.active[:0]
			x.free = x.free[:0]
			x.pool = x.pool[:0]
			x.tornDown = true
			x.shutdown.Broadcast()
			return
		}
		x.shutdown.Wait()
	}
}

func (x *RingFanOut14Xing[C, V]) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.active)
}
func (x *RingFanOut14Xing[C, V]) InputDropped() uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.inputDropped
}
func (x *RingFanOut14Xing[C, V]) DispatchDropped() uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.dispatchDropped
}
func (x *RingFanOut14Xing[C, V]) IsClosed() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.closed
}

func (x *RingFanOut14Xing[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	ch, err := x.Add()
	var once sync.Once
	remove := func() {
		if err == nil {
			once.Do(func() { x.Remove(ch) })
		}
	}
	stop := context.AfterFunc(ctx, remove)
	return func(yield func(V) bool) {
		defer stop()
		defer remove()
		if err != nil {
			return
		}
		if yield == nil {
			panic("bigbuff: ringfanout14: nil iterator yield")
		}
		if ctx.Err() != nil {
			return
		}
		for {
			// Drain before honoring cancellation so a committed channel receive
			// cannot be discarded by a racing callback retiring the same handle.
			select {
			case v, ok := <-ch:
				if !ok || !yield(v) {
					return
				}
				continue
			default:
			}
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok || !yield(v) {
					return
				}
			}
		}
	}
}
