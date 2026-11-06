package bigbuff

import (
	"math"
	"sync"
	"sync/atomic"
)

const maxReceivers = math.MaxInt32

// ChanCaster14Xing ... (unchanged) comments, see original spec.
type ChanCaster14Xing[C ~chan V, V any] struct {
	C C
	mutex sync.RWMutex
	state atomic.Uint64
}

func NewChanCaster14Xing[C ~chan V, V any](channel C) *ChanCaster14Xing[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster14Xing[C, V]{C: channel}
}

// Send broadcasts a value to every registered receiver.
// ... (signature unchanged)
func (x *ChanCaster14Xing[C, V]) Send(value V) int {
	// Fast path: no receivers at all.
	if x.state.Load() == 0 {
		return 0
	}

	// Coarse grained protection: copy the current count while holding the
	// write lock.  After the copy we never need to touch shared state again.
	x.mutex.Lock()
	receivers := uint32(x.state.Load() >> 32)
	if receivers > maxReceivers {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}
	// Guard the invariant that the low 32 bits never carry a value larger than
	// maxReceivers; they only contain maxReceivers + receivers while a send is
	// in flight.
	lo := uint32(x.state.Load())
	tracker := uint32(lo) // currently the “extra” high band
	if lo != receivers && tracker > maxReceivers {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	// Initialise the tracker to maxReceivers + receivers, atomically syncing
	// with any concurrent decrements.
	tracker += maxReceivers
	success := x.state.CompareAndSwap(uint64(receivers)<<32|uint64(tracker),
		uint64(receivers)<<32|uint64(tracker))
	if !success {
		// Should not happen because the write lock excludes other writers,
		// but defend anyway.
		panic(`bigbuff: chancaster: send: invariant CAS failed`)
	}

	// Broadcast loop – NO LOCK HELD DURING SENDS TO AVOID BLOCKING CONSUMERS.
	var delivered int
	for range receivers {
		x.C <- value
		delivered++
	}

	// Final invariant check: after the loop the number of receivers must still
	// be unchanged (or decreased) and the low 32 bits must equal
	// maxReceivers + finalReceiverCount.
	finalLo := uint32(x.state.Load())
	finalHi := uint32(x.state.Load() >> 32)
	if finalHi != receivers || finalLo != tracker {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	// Clear the tracker band.  Because no receiver was added while we held the
	// lock, we can simply reset to zero safely.
	x.state.Store(0)
	return delivered
}

// Add increments or decrements the receiver count.
// ... (signature unchanged)
func (x *ChanCaster14Xing[C, V]) Add(delta int) int {
	switch {
	case delta >= 0:
		var st uint64
		if delta == 0 {
			st = x.state.Load()
		} else {
			// Read side of the RWMutex: safe because no send can be in progress.
			x.mutex.RLock()
			defer x.mutex.RUnlock()
			st = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
		}
		receivers := uint32(st >> 32)
		if receivers <= maxReceivers &&
			receivers >= uint32(delta) &&
			(receivers == uint32(st) || (delta == 0 && receivers+maxReceivers == uint32(st))) {
			return int(receivers)
		}
		panic(`bigbuff: chancaster: add: state invariant violation`)

	default:
		// Negative delta.
		delta = -delta
		// Two's‑complement subtraction.
		st := x.state.Add(^(uint64(delta)<<32 | uint64(uint32(delta)) - 1))
		receivers := uint32(st >> 32)
		if receivers <= maxReceivers && maxReceivers-receivers >= uint32(delta) {
			// low bits indicate either idle or actively sending.
			if uint32(st) == receivers || uint32(st) == maxReceivers+receivers {
				return int(receivers)
			}
		}
		panic(`bigbuff: chancaster: add: state invariant violation`)
	}
}
