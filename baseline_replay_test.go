package bigbuff

import (
	"context"
	"iter"
	"sync"
)

// BaselineBoundedReplay is the ordinary mutex-and-per-subscriber-channel
// design for the bounded recent-history catch-up scenario. Membership and the
// history snapshot share one mutex, while sends are serialized and delivered
// through independent bounded channels. It intentionally favors simplicity
// over the response-derived lockstep protocol.
type BaselineBoundedReplay[V any] struct {
	mu      sync.Mutex
	sendMu  sync.Mutex
	history []V
	head    int
	length  int
	subs    map[*baselineBoundedReplaySub[V]]struct{}
	closed  bool
}

type baselineBoundedReplaySub[V any] struct {
	parent   *BaselineBoundedReplay[V]
	replay   []V
	index    int
	ch       chan V
	done     chan struct{}
	doneOnce sync.Once
}

func NewBaselineBoundedReplay[V any](capacity int) *BaselineBoundedReplay[V] {
	if capacity <= 0 {
		panic("bigbuff: baseline replay capacity must be positive")
	}
	return &BaselineBoundedReplay[V]{
		history: make([]V, capacity),
		subs:    make(map[*baselineBoundedReplaySub[V]]struct{}),
	}
}

func (b *BaselineBoundedReplay[V]) Send(value V) int {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0
	}
	b.history[b.head] = value
	b.head = (b.head + 1) % len(b.history)
	if b.length < len(b.history) {
		b.length++
	}
	subs := make([]*baselineBoundedReplaySub[V], 0, len(b.subs))
	for sub := range b.subs {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	for _, sub := range subs {
		select {
		case <-sub.done:
		case sub.ch <- value:
		}
	}
	return len(subs)
}

func (b *BaselineBoundedReplay[V]) Replay() []V {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked()
}

// Subscribe creates a live-only cursor. SubscribeWithReplay atomically captures
// bounded recent history and registers a live cursor for future values.
func (b *BaselineBoundedReplay[V]) Subscribe() *baselineBoundedReplaySub[V] {
	return b.subscribe(false)
}

func (b *BaselineBoundedReplay[V]) SubscribeWithReplay() *baselineBoundedReplaySub[V] {
	return b.subscribe(true)
}

func (b *BaselineBoundedReplay[V]) subscribe(withReplay bool) *baselineBoundedReplaySub[V] {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub := &baselineBoundedReplaySub[V]{
		parent: b,
		ch:     make(chan V, len(b.history)),
		done:   make(chan struct{}),
	}
	if withReplay {
		sub.replay = b.snapshotLocked()
	}
	if b.closed {
		sub.closeDone()
	} else {
		b.subs[sub] = struct{}{}
	}
	return sub
}

func (b *BaselineBoundedReplay[V]) snapshotLocked() []V {
	values := make([]V, b.length)
	start := b.head - b.length
	if start < 0 {
		start += len(b.history)
	}
	for i := range values {
		values[i] = b.history[(start+i)%len(b.history)]
	}
	return values
}

func (b *BaselineBoundedReplay[V]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for sub := range b.subs {
		sub.closeDone()
	}
	clear(b.subs)
	b.mu.Unlock()
}

func (s *baselineBoundedReplaySub[V]) Receive(ctx context.Context) (V, bool) {
	if s.index < len(s.replay) {
		v := s.replay[s.index]
		s.index++
		return v, true
	}
	select {
	case <-ctx.Done():
		var zero V
		return zero, false
	case <-s.done:
		var zero V
		return zero, false
	case v := <-s.ch:
		return v, true
	}
}

func (s *baselineBoundedReplaySub[V]) TryReceive() (V, bool) {
	if s.index < len(s.replay) {
		v := s.replay[s.index]
		s.index++
		return v, true
	}
	select {
	case <-s.done:
		var zero V
		return zero, false
	case v := <-s.ch:
		return v, true
	default:
		var zero V
		return zero, false
	}
}

func (s *baselineBoundedReplaySub[V]) Close() {
	b := s.parent
	b.mu.Lock()
	delete(b.subs, s)
	s.closeDone()
	b.mu.Unlock()
}

func (s *baselineBoundedReplaySub[V]) closeDone() {
	s.doneOnce.Do(func() { close(s.done) })
}

func (b *BaselineBoundedReplay[V]) SubscribeContextWithReplay(ctx context.Context) iter.Seq[V] {
	sub := b.SubscribeWithReplay()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		if yield == nil {
			sub.Close()
			panic("bigbuff: baseline replay iterator yield func is nil")
		}
		defer sub.Close()
		for {
			v, ok := sub.Receive(ctx)
			if !ok || !yield(v) {
				return
			}
		}
	}
}
