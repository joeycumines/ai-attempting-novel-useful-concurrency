//go:build !race

package bigbuff

import (
	"runtime"
	"sync/atomic"
)

// slot is a single-writer, single-reader seqlock-protected value cell.
// Writes are serialized by Fanout74Deepseek.sendMu; reads may race with a write for
// the *next* epoch, which the seqlock seq validation catches.
type slot74Deepseek[V any] struct {
	seq   atomic.Uint64
	epoch uint64
	value V
}

func (s *slot74Deepseek[V]) write(v V, epoch uint64) {
	s.seq.Add(1) // odd => writing
	s.epoch = epoch
	s.value = v
	s.seq.Add(1) // even => stable
}

func (s *slot74Deepseek[V]) read() (V, uint64) {
	for spin := 0; ; spin++ {
		s1 := s.seq.Load()
		if s1&1 == 0 {
			v := s.value
			e := s.epoch
			s2 := s.seq.Load()
			if s1 == s2 {
				return v, e
			}
		}
		if spin > 1000 {
			runtime.Gosched()
			spin = 0
		}
	}
}
