package bigbuff

// Provenance: 14-xing4.0-29b-a4b.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"math"
	"sync"
	"sync/atomic"
)

const maxReceivers = math.MaxInt32

// ChanCaster14Xing ... (unchanged) comments, see original spec.
type ChanCaster14Xing[C ~chan V, V any] struct {
	C     C
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
	// write lock.
	x.mutex.Lock()
	defer x.mutex.Unlock()

	// CAS loop (as in the reference ChanCaster): a concurrent negative Add
	// (which does not take the mutex) may change the state between the load
	// and the tracker update, so retry against the fresh state until the
	// tracker is set - panicking or broadcasting against a stale count on CAS
	// failure would be wrong.
	var (
		st        uint64
		receivers uint32
	)
	for {
		st = x.state.Load()
		receivers = uint32(st >> 32)
		if receivers == 0 {
			return 0
		}
		if receivers > maxReceivers || uint32(st) != receivers {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}
		if x.state.CompareAndSwap(st, st+uint64(maxReceivers)) {
			break
		}
	}

	// Broadcast loop
	for range receivers {
		x.C <- value
	}

	// Final invariant check: after the loop the number of receivers must still
	// be unchanged (or decreased) and the low 32 bits must equal
	// maxReceivers + finalReceiverCount.
	state := x.state.Load()
	finalHi := uint32(state >> 32)
	finalLo := uint32(state)
	if finalHi > receivers || finalLo != finalHi+maxReceivers || !x.state.CompareAndSwap(state, 0) {
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	return int(finalHi)
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
			switch uint32(st) {
			case receivers:
				// not sending
				return int(receivers)
			case maxReceivers + receivers:
				// sending - drain the requisite number of in-flight sends,
				// so the sender never blocks on a receive that will never
				// arrive (same contract as the reference ChanCaster).
				for range delta {
					<-x.C
				}
				return int(receivers)
			}
		}
		panic(`bigbuff: chancaster: add: state invariant violation`)
	}
}
