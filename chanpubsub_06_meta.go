package bigbuff

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// ChanPubSubV2 from 06-meta: fused barrier broadcast.
type ChanPubSubV2[C ~chan V, V any] struct {
	ping ChanCasterV2[C, V]
	// mu serialises Send calls.
	mu sync.Mutex
	// sendingMu is held (write) by Send while the ping is armed and
	// broadcasting, so that Unsubscribe can detect an in-flight send and
	// steal its share, and Subscribe cannot interleave with a broadcast.
	sendingMu sync.RWMutex
	cond      *sync.Cond
	wait      atomic.Int64 // number of pongs expected in the active round
	subs      atomic.Int32
	broken    chan struct{}
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
	x.Add(1)
}

func (x *ChanPubSubV2[C, V]) Unsubscribe() {
	x.Add(-1)
}

// Add registers (delta > 0) or withdraws (delta < 0) a subscriber.
// A withdrawal during an in-flight send steals the departing subscriber's
// ping, so the sender never blocks on a receive that will never arrive.
func (x *ChanPubSubV2[C, V]) Add(delta int) int {
	switch {
	case delta > 0:
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		return int(x.subs.Add(int32(delta)))
	case delta < 0:
		ok := x.sendingMu.TryRLock()
		for !ok && x.ping.Add(0) == 0 {
			runtime.Gosched()
			ok = x.sendingMu.TryRLock()
		}
		n := int(x.subs.Add(int32(delta)))
		if ok {
			x.sendingMu.RUnlock()
		} else {
			// send in flight: drain our share of the broadcast
			x.ping.Add(delta)
		}
		return n
	default:
		return int(x.subs.Load())
	}
}

func (x *ChanPubSubV2[C, V]) Send(v V) int {
	if x.subs.Load() == 0 {
		return 0
	}
	x.mu.Lock()
	defer x.mu.Unlock()

	x.sendingMu.Lock()
	n := int(x.subs.Load())
	if n == 0 {
		x.sendingMu.Unlock()
		return 0
	}
	if x.ping.Add(n) != n {
		x.sendingMu.Unlock()
		panic("broken")
	}
	sent := x.ping.Send(v)
	x.sendingMu.Unlock()

	// Arm the pong barrier only after the ping phase has fully completed:
	// receivers call Wait() immediately after receiving, and parking them
	// until the broadcast is done enforces exactly one receive per round.
	x.cond.L.Lock()
	x.wait.Store(int64(sent))
	x.cond.Broadcast()
	for x.wait.Load() > 0 {
		x.cond.Wait()
	}
	x.cond.L.Unlock()

	return sent
}

func (x *ChanPubSubV2[C, V]) Wait() {
	x.cond.L.Lock()
	defer x.cond.L.Unlock()
	// park until a round is active (wait > 0); a Wait without a matching
	// receive is API misuse
	for x.wait.Load() == 0 {
		x.cond.Wait()
	}
	if x.wait.Add(-1) == 0 {
		x.cond.Broadcast()
	}
}
