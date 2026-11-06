package bigbuff

import (
	"math"
	"sync"
	"sync/atomic"
)

// ChanCaster2 is the ultimate 1:1 rendezvous form.
// Same API as ChanCaster but:
//   - state/mutex on separate cache lines (no false sharing),
//   - single Add(MaxInt32) not CAS loop (increments excluded by RLock),
//   - steal via non-blocking select for fail-fast violation detection.
type ChanCaster2[C ~chan V, V any] struct {
	_ noCopy
	_ [64]byte
	C C
	_ [64]byte
	state atomic.Uint64 // hi=receivers lo=receivers or receivers+Max
	_ [56]byte
	mutex sync.RWMutex
	_ [56]byte
}

func NewChanCaster2[C ~chan V, V any](c C) *ChanCaster2[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chancaster2: unbuffered only`)
	}
	return &ChanCaster2[C, V]{C: c}
}

func (x *ChanCaster2[C, V]) Send(v V) int {
	if x.state.Load() == 0 {
		return 0
	}
	x.mutex.Lock()
	defer x.mutex.Unlock()
	st := x.state.Load()
	receivers := uint32(st >> 32)
	if receivers == 0 {
		return 0
	}
	if uint32(st) != receivers {
		panic(`bigbuff: chancaster2: invariant`)
	}
	// one atomic Add replaces CAS loop - only decrements race, and they Add(-d) on both halves
	x.state.Add(uint64(math.MaxInt32))
	for range receivers {
		x.C <- v
	}
	st = x.state.Load()
	tracker := uint32(st >> 32)
	if tracker > receivers || uint32(st) != tracker+math.MaxInt32 || !x.state.CompareAndSwap(st, 0) {
		panic(`bigbuff: chancaster2: invariant post-send`)
	}
	return int(tracker)
}

func (x *ChanCaster2[C, V]) Add(delta int) int {
	const max = math.MaxInt32
	switch {
	case delta >= 0:
		var st uint64
		if delta == 0 {
			st = x.state.Load()
		} else {
			if delta > max {
				panic(`bigbuff: chancaster2: delta bound`)
			}
			x.mutex.RLock()
			defer x.mutex.RUnlock()
			st = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
		}
		if r, t := uint32(st>>32), uint32(st); r <= max && r >= uint32(delta) && (r == t || (delta == 0 && r+max == t)) {
			return int(r)
		}
	case delta < -max:
		panic(`bigbuff: chancaster2: delta bound`)
	default:
		delta = -delta
		st := x.state.Add(^(uint64(delta)<<32 | uint64(uint32(delta)) - 1))
		if r := uint32(st >> 32); r <= max && max-r >= uint32(delta) {
			switch uint32(st) {
			case r:
				return int(r)
			case max + r:
				for range delta {
					<-x.C
				}
				return int(r)
			}
		}
	}
	panic(`bigbuff: chancaster2: invariant add`)
}


