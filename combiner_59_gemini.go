// Copyright 2026 Joseph Cumines
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigbuff

import (
	"math/bits"
	"runtime"
	"sync/atomic"
)

const (
	laneFree59Gemini uint32 = iota
	laneFilling59Gemini
	laneReady59Gemini
	laneDone59Gemini
	laneClosed59Gemini
)

// combinerLane59Gemini is a 128-byte cache-line-isolated flat-combining slot.
// A producer deposits its value in val, transitions state to laneReady59Gemini, and spins
// locally on state until the combiner leader sweeps the batch and writes seq.
type combinerLane59Gemini[T any] struct {
	state atomic.Uint32
	_     uint32
	seq   uint64
	val   T
	_pad  [104]byte
}

// StripedCombiner59Gemini is a flat-combining multi-producer ingress engine that coalesces
// concurrent single-item Submit calls across striped cache-line-isolated lanes into
// vectorized HorizonRing59Gemini.PublishBatch operations.
//
// Design note (single-leader flat combining): at most one goroutine acts as the
// combiner at a time (leaderLock). Submitted values wait on their lane's state
// word while the leader sweeps all lanes and publishes the batch. This keeps the
// combining critical section free of allocation and per-lane locks, but it means
// combining throughput under extreme contention is bounded by one sweeping
// goroutine; producers that find every probed lane saturated fall back to a
// direct wait-free Publish instead of queueing unboundedly.
type StripedCombiner59Gemini[T any] struct {
	ring       *HorizonRing59Gemini[T]
	laneMask   uint64
	lanes      []combinerLane59Gemini[T]
	seed       atomic.Uint64
	_pad0      [64]byte
	leaderLock atomic.Uint32
	_pad1      [124]byte
	batchVals  []T
	batchIdxs  []int
}

// NewStripedCombiner59Gemini creates a StripedCombiner59Gemini fronting the given HorizonRing59Gemini.
// stripes is rounded up to a power of two (defaults to 64 if <= 0).
func NewStripedCombiner59Gemini[T any](ring *HorizonRing59Gemini[T], stripes int) *StripedCombiner59Gemini[T] {
	if ring == nil {
		panic("bigbuff: striped combiner: nil HorizonRing59Gemini")
	}
	if stripes <= 0 {
		stripes = 64
	}
	if stripes < 4 {
		stripes = 4
	}
	pow2 := uint64(1) << bits.Len64(uint64(stripes-1))

	return &StripedCombiner59Gemini[T]{
		ring:      ring,
		laneMask:  pow2 - 1,
		lanes:     make([]combinerLane59Gemini[T], pow2),
		batchVals: make([]T, pow2),
		batchIdxs: make([]int, pow2),
	}
}

// Submit publishes val to the underlying HorizonRing59Gemini, automatically combining
// concurrent single-item producers into amortized PublishBatch calls under contention.
// It returns the exact sequence number assigned to val and true, or (0, false) if closed.
func (c *StripedCombiner59Gemini[T]) Submit(val T) (seq uint64, ok bool) {
	if c.ring.IsClosed() {
		return 0, false
	}

	// Fast path when no combiner sweep is active and we can grab a lane:
	// hash across stripes using a Weyl/SplitMix64 increment.
	h := c.seed.Add(0x9e3779b97f4a7c15)
	mask := c.laneMask
	var chosen *combinerLane59Gemini[T]

	for probe := uint64(0); probe < 4; probe++ {
		idx := (h + probe*17) & mask
		lane := &c.lanes[idx]
		if lane.state.Load() == laneFree59Gemini && lane.state.CompareAndSwap(laneFree59Gemini, laneFilling59Gemini) {
			chosen = lane
			break
		}
	}

	if chosen == nil {
		// All probed combining lanes are saturated; overflow directly to wait-free Publish.
		return c.ring.Publish(val)
	}

	chosen.val = val
	chosen.state.Store(laneReady59Gemini)

	for {
		st := chosen.state.Load()
		if st == laneDone59Gemini {
			seq = chosen.seq
			chosen.state.Store(laneFree59Gemini)
			return seq, true
		}
		if st == laneClosed59Gemini {
			chosen.state.Store(laneFree59Gemini)
			return 0, false
		}

		// Attempt to become the flat-combining leader.
		if c.leaderLock.Load() == 0 && c.leaderLock.CompareAndSwap(0, 1) {
			c.sweepLanes()
			c.leaderLock.Store(0)
			continue
		}

		runtime.Gosched()
	}
}

func (c *StripedCombiner59Gemini[T]) sweepLanes() {
	count := 0
	lanes := c.lanes
	batchVals := c.batchVals
	batchIdxs := c.batchIdxs

	for i := range lanes {
		if lanes[i].state.Load() == laneReady59Gemini {
			batchVals[count] = lanes[i].val
			batchIdxs[count] = i
			count++
		}
	}

	if count == 0 {
		return
	}

	firstSeq, _, ok := c.ring.PublishBatch(batchVals[:count])
	if !ok {
		for k := 0; k < count; k++ {
			idx := batchIdxs[k]
			lanes[idx].seq = 0
			lanes[idx].state.Store(laneClosed59Gemini)
		}
		return
	}

	for k := 0; k < count; k++ {
		idx := batchIdxs[k]
		lanes[idx].seq = firstSeq + uint64(k)
		lanes[idx].state.Store(laneDone59Gemini)
	}
}
