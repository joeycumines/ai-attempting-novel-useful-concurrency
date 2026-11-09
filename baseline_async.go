package bigbuff

// BaselineAsyncFanout is the ordinary staff-engineer answer to bounded
// asynchronous lossy fan-out: a buffered input channel, one dispatcher and a
// mutex-guarded set of drop-oldest subscriber mailboxes. No channel pool or
// custom ring is used. Unlike composing BaselineFanout directly, admission and
// subscriber retirement share one mutex so removing/closing a channel cannot
// race a dispatcher's send. Send reports queued acceptance, not delivery.

import (
	"context"
	"iter"
	"sync"
)

type BaselineAsyncFanout[V any] struct {
	mu           sync.Mutex
	input        chan V
	subs         map[uint64]chan V
	nextID       uint64
	capacity     int
	inputDropped uint64
	done         chan struct{}
	closed       bool
	stopped      bool
	shutdown     sync.Cond
}

func NewBaselineAsyncFanout[V any](queueCapacity, subscriberCapacity int) *BaselineAsyncFanout[V] {
	if queueCapacity <= 0 || subscriberCapacity <= 0 {
		panic("bigbuff: baseline async fanout capacities must be positive")
	}
	x := &BaselineAsyncFanout[V]{
		input:    make(chan V, queueCapacity),
		subs:     make(map[uint64]chan V),
		capacity: subscriberCapacity,
		done:     make(chan struct{}),
	}
	x.shutdown.L = &x.mu
	go x.run()
	return x
}

func (x *BaselineAsyncFanout[V]) run() {
	defer func() {
		x.mu.Lock()
		x.stopped = true
		x.shutdown.Broadcast()
		x.mu.Unlock()
	}()
	for {
		select {
		case <-x.done:
			return
		case v := <-x.input:
			x.mu.Lock()
			if !x.closed {
				for _, ch := range x.subs {
					select {
					case ch <- v:
					default:
						// Drop oldest, then retry the offer; both operations
						// are non-blocking, so a consumer draining the
						// mailbox between the two attempts can never park
						// the dispatcher (and with it every Send, Subscribe,
						// Unsubscribe) forever.
						select {
						case <-ch:
						default:
						}
						select {
						case ch <- v:
						default:
						}
					}
				}
			}
			x.mu.Unlock()
		}
	}
}

func (x *BaselineAsyncFanout[V]) Send(v V) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed || len(x.subs) == 0 {
		return 0 // idle-period sends are not queued for future subscribers
	}
	select {
	case x.input <- v:
	default:
		// The dispatcher may drain the input between the failed offer and
		// the drop attempt; both are non-blocking, and the new value is
		// dropped (counted) when the input will not accept it either way.
		select {
		case <-x.input:
		default:
		}
		x.inputDropped++
		select {
		case x.input <- v:
		default:
		}
	}
	return len(x.subs)
}

func (x *BaselineAsyncFanout[V]) Subscribe() uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.nextID++
	id := x.nextID
	if !x.closed {
		x.subs[id] = make(chan V, x.capacity)
	}
	return id
}

func (x *BaselineAsyncFanout[V]) Unsubscribe(id uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if ch, ok := x.subs[id]; ok {
		delete(x.subs, id)
		close(ch)
	}
}

func (x *BaselineAsyncFanout[V]) C(id uint64) <-chan V {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.subs[id]
}

func (x *BaselineAsyncFanout[V]) Subscribers() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.subs)
}

func (x *BaselineAsyncFanout[V]) InputDropped() uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.inputDropped
}

func (x *BaselineAsyncFanout[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	id := x.Subscribe()
	ch := x.C(id)
	return func(yield func(V) bool) {
		defer x.Unsubscribe(id)
		if ch == nil {
			return
		}
		if yield == nil {
			panic("bigbuff: baseline async fanout: nil iterator yield")
		}
		if ctx.Err() != nil {
			return
		}
		for {
			select {
			case v, ok := <-ch:
				if !ok || !yield(v) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close waits for dispatcher exit and all earlier Close callers. Subscriber
// channels retain already dispatched values for draining, then report closed.
func (x *BaselineAsyncFanout[V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.closed {
		x.closed = true
		close(x.done)
	}
	for !x.stopped {
		x.shutdown.Wait()
	}
	for id, ch := range x.subs {
		delete(x.subs, id)
		close(ch)
	}
}
