package bigbuff

// BaselineQueuedFanout is the ordinary staff-engineer answer to nonblocking
// lossless fan-out: one mutex, a private slice queue per subscriber, and a
// capacity-one wake channel. It deliberately does not attempt COW snapshots,
// lock-free delivery, bounded retention, or clever queue reclamation. A slow
// subscriber grows its own queue without blocking Send; that memory cost is
// the scenario's defining trade-off, not a defect to optimize away.

import (
	"context"
	"sync"
)

type BaselineQueuedFanout[V any] struct {
	mu     sync.Mutex
	subs   map[*baselineQueuedSub[V]]struct{}
	closed bool
}

type baselineQueuedSub[V any] struct {
	parent *BaselineQueuedFanout[V]
	queue  []V
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func NewBaselineQueuedFanout[V any]() *BaselineQueuedFanout[V] {
	return &BaselineQueuedFanout[V]{subs: make(map[*baselineQueuedSub[V]]struct{})}
}

func (b *BaselineQueuedFanout[V]) Subscribe() *baselineQueuedSub[V] {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &baselineQueuedSub[V]{parent: b, wake: make(chan struct{}, 1), done: make(chan struct{})}
	if b.closed {
		s.once.Do(func() { close(s.done) })
	} else {
		b.subs[s] = struct{}{}
	}
	return s
}

func (b *BaselineQueuedFanout[V]) Send(v V) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0
	}
	for s := range b.subs {
		s.queue = append(s.queue, v)
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return len(b.subs)
}

func (b *BaselineQueuedFanout[V]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for s := range b.subs {
		s.once.Do(func() { close(s.done) })
	}
	clear(b.subs)
}

func (s *baselineQueuedSub[V]) Unsubscribe() {
	s.parent.mu.Lock()
	delete(s.parent.subs, s)
	s.parent.mu.Unlock()
	s.once.Do(func() { close(s.done) })
}

func (s *baselineQueuedSub[V]) QueueLen() int {
	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()
	return len(s.queue)
}

func (s *baselineQueuedSub[V]) Receive(ctx context.Context) (V, bool) {
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		s.parent.mu.Lock()
		if len(s.queue) != 0 {
			v := s.queue[0]
			var zero V
			s.queue[0] = zero
			s.queue = s.queue[1:]
			s.parent.mu.Unlock()
			return v, true
		}
		s.parent.mu.Unlock()
		select {
		case <-ctx.Done():
			return zero, false
		case <-s.done:
			// Withdrawal stops admission, not drain of already accepted values.
			s.parent.mu.Lock()
			if len(s.queue) != 0 {
				v := s.queue[0]
				var zero V
				s.queue[0] = zero
				s.queue = s.queue[1:]
				s.parent.mu.Unlock()
				return v, true
			}
			s.parent.mu.Unlock()
			return zero, false
		case <-s.wake:
		}
	}
}

func (s *baselineQueuedSub[V]) TryReceive() (V, bool) {
	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()
	if len(s.queue) == 0 {
		var zero V
		return zero, false
	}
	v := s.queue[0]
	var zero V
	s.queue[0] = zero
	s.queue = s.queue[1:]
	return v, true
}
