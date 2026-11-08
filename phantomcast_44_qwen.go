package bigbuff

import (
	"context"
	"sync"
	"sync/atomic"
)

// Provenance: 44-qwen-chat-qwen-3.7-max-prompt-mini.md (Qwen, response #44).
//
// This keeps the output's Disruptor-style bounded ring, independent cursors,
// coalesced per-subscriber select notifications, and copy-on-write subscriber
// list. The candidate had two correctness failures: its head advanced before
// storing the payload (and its cursor was off by one), and plain []V ring
// cells raced with overwrites. The repaired implementation publishes
// immutable payload pointers behind a per-cell atomic version, and only then
// advances head. Pooling was removed because an old Send snapshot could still
// signal a node/channel after Unsubscribe returned it to a new owner.

type phantom44Cell[V any] struct {
	version atomic.Uint64 // odd while writing, even publication version
	value   atomic.Pointer[V]
}

type phantom44Sub[V any] struct {
	owner  *PhantomCast44Qwen[V]
	notify chan struct{}
	done   chan struct{}
	closed atomic.Bool
	next   atomic.Uint64 // next sequence to consume; starts at published+1
	missed atomic.Uint64
}

// PhantomCast44Qwen is a bounded, lossy broadcast ring. Each independent
// subscription has its own cursor; slow subscribers resume at the oldest
// retained sequence and can query Missed. Signals coalesce and Send never
// waits for receivers.
type PhantomCast44Qwen[V any] struct {
	cells []phantom44Cell[V]
	mask  uint64
	head  atomic.Uint64 // last published sequence

	mu   sync.Mutex // serializes copy-on-write subscriber changes
	subs atomic.Pointer[[]*phantom44Sub[V]]

	writeMu sync.Mutex // serializes ring publication
	closed  atomic.Bool
	done    chan struct{}
}

// PhantomSubscription44Qwen is one independent cursor on a PhantomCast44Qwen.
type PhantomSubscription44Qwen[V any] struct{ sub *phantom44Sub[V] }

// NewPhantomCast44Qwen requires a nonzero power-of-two capacity.
func NewPhantomCast44Qwen[V any](capacity uint64) *PhantomCast44Qwen[V] {
	if capacity == 0 || capacity&(capacity-1) != 0 || capacity > uint64(^uint(0)>>1) {
		panic("bigbuff: phantomcast44: capacity must be a representable nonzero power of two")
	}
	p := &PhantomCast44Qwen[V]{cells: make([]phantom44Cell[V], int(capacity)), mask: capacity - 1, done: make(chan struct{})}
	empty := make([]*phantom44Sub[V], 0)
	p.subs.Store(&empty)
	return p
}

// Subscribe starts at the next publication; prior ring contents are not replayed.
func (p *PhantomCast44Qwen[V]) Subscribe() *PhantomSubscription44Qwen[V] {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		panic("bigbuff: phantomcast44: subscribe on closed broadcaster")
	}
	s := &phantom44Sub[V]{owner: p, notify: make(chan struct{}, 1), done: make(chan struct{})}
	s.next.Store(p.head.Load() + 1)
	old := p.subs.Load()
	next := make([]*phantom44Sub[V], len(*old)+1)
	copy(next, *old)
	next[len(*old)] = s
	p.subs.Store(&next)
	return &PhantomSubscription44Qwen[V]{sub: s}
}

// Send publishes one value and coalesces a wake token to each current subscriber.
func (p *PhantomCast44Qwen[V]) Send(value V) {
	if p.closed.Load() {
		panic("bigbuff: phantomcast44: send on closed broadcaster")
	}
	p.writeMu.Lock()
	seq := p.head.Load() + 1
	if seq == 0 || seq > ^uint64(0)/2 {
		p.writeMu.Unlock()
		panic("bigbuff: phantomcast44: sequence overflow")
	}
	v := new(V)
	*v = value
	cell := &p.cells[seq&p.mask]
	cell.version.Store(seq*2 - 1) // odd: publication in progress
	cell.value.Store(v)
	cell.version.Store(seq * 2) // even: value for seq is stable
	p.head.Store(seq)
	p.writeMu.Unlock()
	for _, s := range *p.subs.Load() {
		if s.closed.Load() {
			continue
		}
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

// Head is the most recently published sequence, or zero if empty.
func (p *PhantomCast44Qwen[V]) Head() uint64 { return p.head.Load() }

// Len reports current active subscribers.
func (p *PhantomCast44Qwen[V]) Len() int { return len(*p.subs.Load()) }

// Close ends all current and future receives and rejects later Subscribe/Send.
func (p *PhantomCast44Qwen[V]) Close() {
	p.mu.Lock()
	if p.closed.CompareAndSwap(false, true) {
		close(p.done)
	}
	p.mu.Unlock()
}

// Done is closed by Close.
func (p *PhantomCast44Qwen[V]) Done() <-chan struct{} { return p.done }

// Next returns the next available value. On overrun it skips to the oldest
// retained value, incrementing Missed by the exact number skipped.
func (s *PhantomSubscription44Qwen[V]) Next() (V, bool) {
	var zero V
	if s == nil || s.sub == nil || s.sub.closed.Load() {
		return zero, false
	}
	x := s.sub.owner
	capacity := uint64(len(x.cells))
	for {
		n := s.sub.next.Load()
		head := x.head.Load()
		if n == 0 {
			return zero, false
		}
		if n > head {
			return zero, false
		}
		oldest := uint64(1)
		if head >= capacity {
			oldest = head - capacity + 1
		}
		if n < oldest {
			s.sub.missed.Add(oldest - n)
			s.sub.next.CompareAndSwap(n, oldest)
			continue
		}
		cell := &x.cells[n&x.mask]
		want := n * 2
		v1 := cell.version.Load()
		if v1 != want {
			// Another publication is replacing this cell; recompute the
			// retention floor from the new head rather than reading torn data.
			if latest := x.head.Load(); latest >= capacity && n <= latest-capacity {
				continue
			}
			continue
		}
		ptr := cell.value.Load()
		v2 := cell.version.Load()
		if ptr == nil || v1 != v2 {
			continue
		}
		if !s.sub.next.CompareAndSwap(n, n+1) {
			continue
		}
		return *ptr, true
	}
}

// TryRecv aliases Next for select-driven consumers.
func (s *PhantomSubscription44Qwen[V]) TryRecv() (V, bool) { return s.Next() }

// Missed reports the cumulative number of overwritten values skipped.
func (s *PhantomSubscription44Qwen[V]) Missed() uint64 {
	if s == nil || s.sub == nil {
		return 0
	}
	return s.sub.missed.Load()
}

// Chan returns a coalescing wake channel. Always call Next before parking and
// after waking; the channel carries no data.
func (s *PhantomSubscription44Qwen[V]) Chan() <-chan struct{} {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.notify
}

// Done is closed when the subscription is removed or its owner is closed.
func (s *PhantomSubscription44Qwen[V]) Done() <-chan struct{} {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.done
}

// Unsubscribe is idempotent and wakes a parked receiver.
func (s *PhantomSubscription44Qwen[V]) Unsubscribe() {
	if s == nil || s.sub == nil {
		return
	}
	sub := s.sub
	if !sub.closed.CompareAndSwap(false, true) {
		return
	}
	close(sub.done)
	p := sub.owner
	p.mu.Lock()
	old := p.subs.Load()
	next := make([]*phantom44Sub[V], 0, len(*old))
	for _, x := range *old {
		if x != sub {
			next = append(next, x)
		}
	}
	p.subs.Store(&next)
	p.mu.Unlock()
}

// Receive blocks until a value, context cancellation, subscription removal,
// or broadcaster Close. It attempts a drain before parking to avoid lost wakeups.
func (s *PhantomSubscription44Qwen[V]) Receive(ctx context.Context) (V, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if v, ok := s.Next(); ok {
			return v, true
		}
		if s == nil || s.sub == nil {
			return zeroValue[V](), false
		}
		select {
		case <-ctx.Done():
			return zeroValue[V](), false
		case <-s.sub.done:
			return zeroValue[V](), false
		case <-s.sub.owner.done:
			return zeroValue[V](), false
		case <-s.sub.notify:
		}
	}
}

func zeroValue[V any]() V { var zero V; return zero }
