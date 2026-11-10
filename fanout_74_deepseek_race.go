//go:build race

package bigbuff

import "sync"

type slot74Deepseek[V any] struct {
	mu    sync.Mutex
	epoch uint64
	value V
}

func (s *slot74Deepseek[V]) write(v V, epoch uint64) {
	s.mu.Lock()
	s.epoch = epoch
	s.value = v
	s.mu.Unlock()
}

func (s *slot74Deepseek[V]) read() (V, uint64) {
	s.mu.Lock()
	v, e := s.value, s.epoch
	s.mu.Unlock()
	return v, e
}
