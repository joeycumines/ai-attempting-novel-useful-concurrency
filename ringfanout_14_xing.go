package bigbuff

// Provenance: 14-xing4.0-29b-a4b.md, section 3 "RingFanOut". The response
// deliberately withheld source and gave only a specification: a central
// dispatcher goroutine, pooled per-subscriber channels, bounded buffering,
// Add/Remove, selectable C, and Close. This extraction completes that spec.
//
// Interpretations required to make the prose sound:
//   - "The dispatcher never blocks waiting for every subscriber" is completed
//     as explicit drop-oldest admission at BOTH the input queue and each
//     subscriber mailbox, with separate counters. A blocking copy would
//     contradict the stated nonblocking property.
//   - Pooled channels are reused only after Remove drains them. The low-level
//     Add/Remove contract requires the caller to stop selecting on a removed
//     channel before Remove returns; a stale handle may observe a later tenant.
//     SubscribeContext provides the safe ownership wrapper.
//   - Close is idempotent, stops admission, lets the dispatcher finish its
//     current nonblocking fan-out, then closes pooled channels. Buffered values
//     remain drainable; queued-but-undispatched input is abandoned.
//
// Trade-off: burst absorption and O(1) publisher enqueue, bounded memory,
// lossy detectable overload, one dispatcher goroutine, fixed subscriber pool.

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
)

var ErrRingFanOut14Closed = errors.New("bigbuff: ringfanout14: closed")

type RingFanOut14Xing[C ~chan V, V any] struct {
	sendMu          sync.Mutex
	input           chan V
	queueCap        int
	inputDropped    atomic.Uint64
	dispatchDropped atomic.Uint64

	mu     sync.Mutex
	active []C
	free   []C
	pool   []C

	done   chan struct{}
	closed atomic.Bool
	wg     sync.WaitGroup
}

func NewRingFanOut14Xing[C ~chan V, V any](queueCapacity, maxSubscribers, subscriberCapacity int) *RingFanOut14Xing[C, V] {
	return newRingFanOut14Xing[C, V](queueCapacity, maxSubscribers, subscriberCapacity, true)
}

func newRingFanOut14Xing[C ~chan V, V any](queueCapacity, maxSubscribers, subscriberCapacity int, start bool) *RingFanOut14Xing[C, V] {
	if queueCapacity <= 0 || maxSubscribers <= 0 || subscriberCapacity <= 0 {
		panic("bigbuff: ringfanout14: capacities must be positive")
	}
	x := &RingFanOut14Xing[C, V]{
		input:    make(chan V, queueCapacity),
		queueCap: queueCapacity,
		done:     make(chan struct{}),
	}
	for i := 0; i < maxSubscribers; i++ {
		ch := make(chan V, subscriberCapacity)
		x.pool = append(x.pool, ch)
		x.free = append(x.free, ch)
	}
	if start {
		x.wg.Add(1)
		go x.run()
	}
	return x
}

func (x *RingFanOut14Xing[C, V]) run() {
	defer x.wg.Done()
	for {
		select {
		case <-x.done:
			return
		case v := <-x.input:
			x.dispatch(v)
		}
	}
}

func (x *RingFanOut14Xing[C, V]) Send(value V) error {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	return x.enqueueLocked(value)
}

func (x *RingFanOut14Xing[C, V]) enqueueLocked(value V) error {
	if x.closed.Load() {
		return ErrRingFanOut14Closed
	}
	x.mu.Lock()
	empty := len(x.active) == 0
	x.mu.Unlock()
	if empty {
		return nil // live-start: idle-period sends are not queued for late subscribers
	}
	select {
	case x.input <- value:
		return nil
	default:
		select {
		case <-x.input:
			x.inputDropped.Add(1)
		default:
		}
		x.input <- value
		return nil
	}
}

func (x *RingFanOut14Xing[C, V]) dispatch(value V) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, ch := range x.active {
		select {
		case ch <- value:
		default:
			select {
			case <-ch:
				x.dispatchDropped.Add(1)
			default:
			}
			ch <- value
		}
	}
}

// Add leases a pooled channel. The caller must Remove it before reuse.
func (x *RingFanOut14Xing[C, V]) Add() (C, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	var zero C
	if x.closed.Load() {
		return zero, ErrRingFanOut14Closed
	}
	if len(x.free) == 0 {
		return zero, errors.New("bigbuff: ringfanout14: subscriber pool exhausted")
	}
	ch := x.free[len(x.free)-1]
	x.free = x.free[:len(x.free)-1]
	x.active = append(x.active, ch)
	return ch, nil
}

// Remove drains and returns a channel to the pool. It reports whether ch was
// active. The caller must no longer select on ch after Remove returns.
func (x *RingFanOut14Xing[C, V]) Remove(ch C) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	for i, active := range x.active {
		if active == ch {
			x.active = append(x.active[:i], x.active[i+1:]...)
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

func (x *RingFanOut14Xing[C, V]) Close() {
	x.sendMu.Lock()
	if x.closed.Swap(true) {
		x.sendMu.Unlock()
		return
	}
	close(x.done)
	x.sendMu.Unlock()
	x.wg.Wait()
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, ch := range x.pool {
		close(ch)
	}
	x.active = nil
	x.free = nil
}

func (x *RingFanOut14Xing[C, V]) Len() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.active)
}
func (x *RingFanOut14Xing[C, V]) InputDropped() uint64    { return x.inputDropped.Load() }
func (x *RingFanOut14Xing[C, V]) DispatchDropped() uint64 { return x.dispatchDropped.Load() }
func (x *RingFanOut14Xing[C, V]) IsClosed() bool          { return x.closed.Load() }

func (x *RingFanOut14Xing[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	ch, err := x.Add()
	return func(yield func(V) bool) {
		if err != nil {
			return
		}
		defer x.Remove(ch)
		if yield == nil {
			panic("bigbuff: ringfanout14: nil iterator yield")
		}
		for {
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
