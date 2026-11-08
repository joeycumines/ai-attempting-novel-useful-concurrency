package bigbuff

// Provenance: 11-meta-chat-probably-muse-spark-1.3.md, "Ultimate Evolution A:
// LockFree ChanCaster". The response supplied a dual-counter encoding and an
// elided CAS/spin sketch. This completion preserves atomic membership updates,
// positive-Add retry while armed, and compensating receives on withdrawal.
// Send-vs-Send serialization remains a mutex, as in the reference protocol.
// Membership updates use the sketch's CAS shape. A temporary signed Add
// reservation is unsafe here: Send can reset the word before its inverse.
//
// Repair beyond the sketch: arming uses a distinct sentinel (math.MaxUint32),
// not receivers+MaxInt32, which aliases a high idle count. CAS validates bounds
// before publishing a count, without exposing a transient over-reservation.
// These are arithmetic/ownership repairs, not a new delivery design.

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

const chanCaster11MaxReceivers = math.MaxInt32

type ChanCaster11Meta[C ~chan V, V any] struct {
	C      C
	state  atomic.Uint64 // hi=receivers; lo=receivers or MaxUint32 while armed
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
	var state uint64
	var receivers uint32
	for {
		state = x.state.Load()
		if state == 0 {
			return 0
		}
		receivers = uint32(state >> 32)
		if uint32(state) != receivers || receivers > chanCaster11MaxReceivers {
			panic("bigbuff: chancaster11: send state invariant")
		}
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|math.MaxUint32) {
			break
		}
	}
	for range receivers {
		x.C <- value
	}
	state = x.state.Load()
	delivered := uint32(state >> 32)
	if delivered > receivers || uint32(state) != math.MaxUint32 || !x.state.CompareAndSwap(state, 0) {
		panic("bigbuff: chancaster11: post-send state invariant")
	}
	return int(delivered)
}

func (x *ChanCaster11Meta[C, V]) Add(delta int) int {
	if delta > chanCaster11MaxReceivers || delta < -chanCaster11MaxReceivers {
		panic("bigbuff: chancaster11: delta out of bounds")
	}
	if delta == 0 {
		state := x.state.Load()
		r, tracker := uint32(state>>32), uint32(state)
		if r <= chanCaster11MaxReceivers && (tracker == r || tracker == math.MaxUint32) {
			return int(r)
		}
		panic("bigbuff: chancaster11: load state invariant")
	}
	if delta < 0 {
		d := uint32(-delta)
		for {
			before := x.state.Load()
			r, tracker := uint32(before>>32), uint32(before)
			if r < d || r > chanCaster11MaxReceivers || (tracker != r && tracker != math.MaxUint32) {
				panic("bigbuff: chancaster11: receiver underflow or state invariant")
			}
			nextTracker := tracker
			if tracker != math.MaxUint32 {
				nextTracker = r - d
			}
			after := uint64(r-d)<<32 | uint64(nextTracker)
			if !x.state.CompareAndSwap(before, after) {
				continue
			}
			if tracker == math.MaxUint32 {
				for range d {
					<-x.C
				}
			}
			return int(r - d)
		}
	}

	// Positive Add is CAS, not a temporary atomic over-reservation: otherwise
	// a concurrent withdrawal can observe (and panic on) a transient overflow.
	for {
		before := x.state.Load()
		r, tracker := uint32(before>>32), uint32(before)
		if r > chanCaster11MaxReceivers || (tracker != r && tracker != math.MaxUint32) {
			panic("bigbuff: chancaster11: add state invariant")
		}
		if tracker == math.MaxUint32 {
			runtime.Gosched()
			continue
		}
		if r > chanCaster11MaxReceivers-uint32(delta) {
			panic("bigbuff: chancaster11: receiver overflow")
		}
		next := uint64(r+uint32(delta))<<32 | uint64(r+uint32(delta))
		if x.state.CompareAndSwap(before, next) {
			return int(r + uint32(delta))
		}
	}
}
