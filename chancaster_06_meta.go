package bigbuff

// Provenance: 06-meta-chat-probably-muse-spark-1.3.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"math"
	"sync"
	"sync/atomic"
)

// ChanCasterV2 from 06-meta: wait-free positive Add path.
type ChanCasterV2[C ~chan V, V any] struct {
	C       C
	sendMu  sync.Mutex    // only Send vs Send
	stateMu sync.RWMutex  // RLock = Add(+), Lock = Send snapshot
	state   atomic.Uint64 // hi=receivers lo=receivers OR receivers+MaxInt32
}

func NewChanCasterV2[C ~chan V, V any](c C) *ChanCasterV2[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chancaster_v2: must use an unbuffered channel")
	}
	return &ChanCasterV2[C, V]{C: c}
}

func (x *ChanCasterV2[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0
	}
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.stateMu.Lock()
	defer x.stateMu.Unlock()

	var (
		state     uint64
		receivers uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0
		}
		receivers = uint32(state >> 32)
		tracker := uint32(state)
		if tracker != receivers || receivers > math.MaxInt32 {
			panic("bigbuff: chancaster_v2: state invariant violation")
		}
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(receivers+math.MaxInt32)) {
			break
		}
	}

	for range receivers {
		x.C <- value
	}

	state = x.state.Load()
	finalRec := uint32(state >> 32)
	if finalRec > receivers || uint32(state) != finalRec+math.MaxInt32 || !x.state.CompareAndSwap(state, 0) {
		panic("bigbuff: chancaster_v2: state invariant violation")
	}
	return int(finalRec)
}

func (x *ChanCasterV2[C, V]) Add(delta int) int {
	if delta > 0 {
		if delta > math.MaxInt32 {
			panic("oob")
		}
		x.stateMu.RLock()
		defer x.stateMu.RUnlock()
		s := x.state.Add(uint64(delta)<<32 | uint64(delta))
		if uint32(s>>32) > math.MaxInt32 {
			panic("overflow")
		}
		if uint32(s) != uint32(s>>32) {
			panic("invariant: add during send fast path - should be prevented by RLock")
		}
		return int(uint32(s >> 32))
	}
	if delta < 0 {
		d := -delta
		s := x.state.Add(^(uint64(d)<<32 | uint64(d) - 1))
		rec := uint32(s >> 32)
		lo := uint32(s)
		switch {
		case lo == rec: // not sending
			return int(rec)
		case lo == rec+math.MaxInt32:
			for range d {
				<-x.C
			}
			return int(rec)
		default:
			panic("invariant")
		}
	}
	s := x.state.Load()
	if uint32(s>>32) != uint32(s) && uint32(s) != uint32(s>>32)+math.MaxInt32 {
		panic("invariant")
	}
	return int(uint32(s >> 32))
}
