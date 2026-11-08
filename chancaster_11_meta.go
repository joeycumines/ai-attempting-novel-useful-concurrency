package bigbuff

// Provenance: 11-meta-chat-probably-muse-spark-1.3.md, "Ultimate Evolution A:
// LockFree ChanCaster". The response supplied the state encoding and CAS/spin
// shape but elided the Send/Add bodies. This extraction completes that sketch
// while preserving its identity: one atomic dual-counter word, no RWMutex on
// the Add path, positive Add spins only while a Send is armed, and negative Add
// compensates an armed Send by consuming the withdrawn rendezvous. Send-vs-Send
// serialization is a mutex, as in the reference protocol.

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

type ChanCaster11Meta[C ~chan V, V any] struct {
	C      C
	state  atomic.Uint64 // hi=receivers, lo=receivers or receivers+MaxInt32
	sendMu sync.Mutex
}

func NewChanCaster11Meta[C ~chan V, V any](c C) *ChanCaster11Meta[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chancaster11: must use an unbuffered channel")
	}
	return &ChanCaster11Meta[C, V]{C: c}
}

func (x *ChanCaster11Meta[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0
	}
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	var receivers uint32
	for {
		state := x.state.Load()
		if state == 0 {
			return 0
		}
		receivers = uint32(state >> 32)
		tracker := uint32(state)
		if tracker != receivers || receivers > math.MaxInt32 {
			panic("bigbuff: chancaster11: send state invariant")
		}
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(receivers+math.MaxInt32)) {
			break
		}
	}
	for range receivers {
		x.C <- value
	}
	state := x.state.Load()
	delivered := uint32(state >> 32)
	if delivered > receivers || uint32(state) != delivered+math.MaxInt32 || !x.state.CompareAndSwap(state, 0) {
		panic("bigbuff: chancaster11: post-send state invariant")
	}
	return int(delivered)
}

func (x *ChanCaster11Meta[C, V]) Add(delta int) int {
	if delta > math.MaxInt32 || delta < -math.MaxInt32 {
		panic("bigbuff: chancaster11: delta out of bounds")
	}
	if delta == 0 {
		state := x.state.Load()
		r, t := uint32(state>>32), uint32(state)
		if r <= math.MaxInt32 && (r == t || r+math.MaxInt32 == t) {
			return int(r)
		}
		panic("bigbuff: chancaster11: load state invariant")
	}
	if delta > 0 {
		for {
			state := x.state.Load()
			r, t := uint32(state>>32), uint32(state)
			if r+math.MaxInt32 == t {
				runtime.Gosched()
				continue
			}
			if r != t || r > math.MaxInt32-uint32(delta) {
				panic("bigbuff: chancaster11: add state invariant or overflow")
			}
			next := uint64(r+uint32(delta))<<32 | uint64(r+uint32(delta))
			if x.state.CompareAndSwap(state, next) {
				return int(r + uint32(delta))
			}
		}
	}
	d := uint32(-delta)
	state := x.state.Add(^(uint64(d)<<32 | uint64(d) - 1))
	r := uint32(state >> 32)
	if r > math.MaxInt32 || math.MaxInt32-r < d {
		panic("bigbuff: chancaster11: receiver underflow")
	}
	switch uint32(state) {
	case r:
		return int(r)
	case r + math.MaxInt32:
		for range d {
			<-x.C
		}
		return int(r)
	default:
		panic("bigbuff: chancaster11: withdrawal state invariant")
	}
}
