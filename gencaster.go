package bigbuff

import (
	"sync"
	"sync/atomic"
)

// GenCaster is O(1) alloc per Send, O(1) time, unbounded subscribers, select-able
// and zero-copy for V, from 06-meta.
type GenCaster[V any] struct {
	mu  sync.Mutex
	cur atomic.Pointer[gencasterEpoch[V]]
}

type gencasterEpoch[V any] struct {
	seq  uint64
	val  V
	next atomic.Pointer[gencasterEpoch[V]]
	wake chan struct{} // closed when superseded
}

func NewGenCaster[V any]() *GenCaster[V] {
	g := &GenCaster[V]{}
	e := &gencasterEpoch[V]{wake: make(chan struct{})}
	g.cur.Store(e)
	return g
}

func (g *GenCaster[V]) Send(v V) {
	ne := &gencasterEpoch[V]{val: v, wake: make(chan struct{})}
	g.mu.Lock()
	ce := g.cur.Load()
	ne.seq = ce.seq + 1
	ce.next.Store(ne)
	g.cur.Store(ne)
	g.mu.Unlock()
	close(ce.wake) // wakes ALL subscribers at once
}

type GenCasterSub[V any] struct {
	g   *GenCaster[V]
	cur *gencasterEpoch[V]
}

func (g *GenCaster[V]) Subscribe() GenCasterSub[V] {
	return GenCasterSub[V]{g: g, cur: g.cur.Load()}
}

// C returns a channel that fires when a value is available.
// This makes it usable in select with NO churn cost.
func (s *GenCasterSub[V]) C() <-chan struct{} { return s.cur.wake }

func (s *GenCasterSub[V]) Next() (V, bool) {
	// fast path
	if nxt := s.cur.next.Load(); nxt != nil {
		s.cur = nxt
		return nxt.val, true
	}
	<-s.cur.wake
	nxt := s.cur.next.Load()
	if nxt == nil { // closed
		var zero V
		return zero, false
	}
	s.cur = nxt
	return nxt.val, true
}

func (s *GenCasterSub[V]) TryNext() (v V, ok bool) {
	nxt := s.cur.next.Load()
	if nxt == nil {
		return
	}
	s.cur = nxt
	return nxt.val, true
}
