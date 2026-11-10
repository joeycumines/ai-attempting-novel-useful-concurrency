//go:build race

package bigbuff

import "sync/atomic"

type snapshotEntry72Gemini[V any] struct {
	val   V
	epoch uint64
}

// latestSnapshot72Gemini uses an atomic pointer under -race so ThreadSanitizer verifies
// happens-before edges on arbitrary generic V types without false positives.
type latestSnapshot72Gemini[V any] struct {
	ptr atomic.Pointer[snapshotEntry72Gemini[V]]
}

func (s *latestSnapshot72Gemini[V]) store(v V, epoch uint64) {
	s.ptr.Store(&snapshotEntry72Gemini[V]{val: v, epoch: epoch})
}

func (s *latestSnapshot72Gemini[V]) load() (V, uint64, bool) {
	p := s.ptr.Load()
	if p == nil {
		var zero V
		return zero, 0, false
	}
	return p.val, p.epoch, true
}
