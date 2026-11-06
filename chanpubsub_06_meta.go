package bigbuff

import (
	"sync"
	"sync/atomic"
)

// ChanPubSubV2 from 06-meta: fused barrier broadcast.
type ChanPubSubV2[C ~chan V, V any] struct {
	ping   ChanCasterV2[C, V]
	mu     sync.Mutex
	cond   *sync.Cond
	wait   atomic.Int64 // number of pongs expected
	subs   atomic.Int32
	broken chan struct{}
}

func NewChanPubSubV2[C ~chan V, V any](c C) *ChanPubSubV2[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanpubsub_v2: must use an unbuffered channel")
	}
	var x ChanPubSubV2[C, V]
	x.ping.C = c
	x.cond = sync.NewCond(&sync.Mutex{})
	x.broken = make(chan struct{})
	return &x
}

func (x *ChanPubSubV2[C, V]) C() C {
	return x.ping.C
}

func (x *ChanPubSubV2[C, V]) Subscribe() {
	x.subs.Add(1)
}

func (x *ChanPubSubV2[C, V]) Unsubscribe() {
	x.subs.Add(-1)
}

func (x *ChanPubSubV2[C, V]) Send(v V) int {
	if x.subs.Load() == 0 {
		return 0
	}
	x.mu.Lock()
	defer x.mu.Unlock()

	n := int(x.subs.Load())
	if n == 0 {
		return 0
	}
	x.wait.Store(int64(n))
	if x.ping.Add(n) != n {
		panic("broken")
	}
	sent := x.ping.Send(v)

	// barrier: wait for Wait() to decrement wait to 0
	x.cond.L.Lock()
	for x.wait.Load() > 0 {
		x.cond.Wait()
	}
	x.cond.L.Unlock()

	return sent
}

func (x *ChanPubSubV2[C, V]) Wait() {
	if x.wait.Add(-1) == 0 {
		x.cond.L.Lock()
		x.cond.Broadcast()
		x.cond.L.Unlock()
	}
}
