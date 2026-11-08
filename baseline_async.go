package bigbuff

// BaselineAsyncFanout is the ordinary staff-engineer answer to bounded
// asynchronous lossy fan-out: a buffered input channel, one dispatcher
// goroutine, and the standard drop-oldest per-subscriber fan-out. It uses no
// pooled channels or custom ring; composition is the entire baseline.

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

type BaselineAsyncFanout[V any] struct {
	input        chan V
	fan          *BaselineFanout[V]
	done         chan struct{}
	closed       atomic.Bool
	sendMu       sync.Mutex
	wg           sync.WaitGroup
	inputDropped atomic.Uint64
}

func NewBaselineAsyncFanout[V any](queueCapacity, subscriberCapacity int) *BaselineAsyncFanout[V] {
	if queueCapacity <= 0 || subscriberCapacity <= 0 {
		panic("bigbuff: baseline async fanout capacities must be positive")
	}
	x := &BaselineAsyncFanout[V]{
		input: make(chan V, queueCapacity),
		fan:   NewBaselineFanout[V](subscriberCapacity),
		done:  make(chan struct{}),
	}
	x.wg.Add(1)
	go func() {
		defer x.wg.Done()
		for {
			select {
			case <-x.done:
				return
			case v := <-x.input:
				x.fan.Send(v)
			}
		}
	}()
	return x
}

func (x *BaselineAsyncFanout[V]) Send(v V) int {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	if x.closed.Load() || x.fan.Subscribers() == 0 {
		return 0 // live-start: idle-period sends are not queued for late subscribers
	}
	select {
	case x.input <- v:
	default:
		select {
		case <-x.input:
			x.inputDropped.Add(1)
		default:
		}
		x.input <- v
	}
	return x.fan.Subscribers()
}

func (x *BaselineAsyncFanout[V]) Subscribe() uint64     { return x.fan.Subscribe() }
func (x *BaselineAsyncFanout[V]) Unsubscribe(id uint64) { x.fan.Unsubscribe(id) }
func (x *BaselineAsyncFanout[V]) C(id uint64) <-chan V  { return x.fan.C(id) }
func (x *BaselineAsyncFanout[V]) InputDropped() uint64  { return x.inputDropped.Load() }
func (x *BaselineAsyncFanout[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	return x.fan.SubscribeContext(ctx)
}

func (x *BaselineAsyncFanout[V]) Close() {
	x.sendMu.Lock()
	if x.closed.Swap(true) {
		x.sendMu.Unlock()
		return
	}
	close(x.done)
	x.sendMu.Unlock()
	x.wg.Wait()
	x.fan.Close()
}
