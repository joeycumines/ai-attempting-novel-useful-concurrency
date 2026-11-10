//go:build !race

package bigbuff

import (
	"runtime"
	"sync/atomic"
)

// latestSnapshot72Gemini implements an optimistic lock-free seqlock for Latest() state observation
// with 0 heap allocations on both write and read paths.
type latestSnapshot72Gemini[V any] struct {
	seq   atomic.Uint64
	val   V
	epoch uint64
}

func (s *latestSnapshot72Gemini[V]) store(v V, epoch uint64) {
	cur := s.seq.Load()
	s.seq.Store(cur + 1) // Odd sequence: write in progress
	s.val = v
	s.epoch = epoch
	s.seq.Store(cur + 2) // Even sequence: consistent snapshot published
}

func (s *latestSnapshot72Gemini[V]) load() (V, uint64, bool) {
	for {
		seq1 := s.seq.Load()
		if seq1 == 0 {
			var zero V
			return zero, 0, false
		}
		if seq1&1 != 0 {
			runtime.Gosched()
			continue
		}
		v := s.val
		ep := s.epoch
		seq2 := s.seq.Load()
		if seq1 == seq2 {
			return v, ep, true
		}
		runtime.Gosched()
	}
}
