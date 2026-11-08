package bigbuff

// Provenance: 24-meta-ai-prompt-mini.md, "RCU Fanout + Mailbox". The source
// called its type Broadcaster and its prose called it coalescing, but neither
// is accurate in this flat package: it is a lossless queued fan-out, renamed
// QueuedFanout24Meta. Its identity is an immutable COW membership snapshot,
// a capacity-one selectable mailbox per subscriber, and a private FIFO
// overflow queue pumped by the receiver (no forwarder goroutine).
//
// Repairs in spirit:
//   - The original fast path could put a NEW value in the mailbox ahead of an
//     existing overflow queue. An atomic queue length now gates that path;
//     pump publishes the length only after moving the oldest queued value.
//   - pump's "intentionally unreachable" receive-and-panic branch could be
//     reached concurrently and also stole a value. It is removed; a failed
//     nonblocking refill leaves the queue head intact.
//   - The declared refs field was unused. It is now a per-subscriber send
//     lease: Unsubscribe closes admission and waits for outstanding leases,
//     so no value can be enqueued after Unsubscribe returns. Send still takes
//     no membership lock and churn does not block on a slow consumer.
//   - Recv had no cancellation escape because mailboxes are never closed.
//     RecvContext and Done supply that escape without closing a channel that
//     a sender might still reference. Already queued values remain drainable.
//   - Send returns the number of subscribers that actually accepted a value,
//     not the size of a snapshot whose members may have concurrently left.
//   - Concurrent publishers could place a newer value in the mailbox ahead of
//     an older overflow append. A short per-subscriber enqueue mutex now makes
//     mailbox admission and overflow append one FIFO step. It is never held
//     while waiting for a consumer, so Send remains nonblocking; the source's
//     lock-free fast-path claim is not retained.
//
// Structural trade-off retained deliberately: overflow is UNBOUNDED. This is
// not a bounded/backpressured or latest-value broadcaster; it is attached to
// the separate nonblocking-lossless-overflow scenario and benchmarked with
// exact queue accounting. There is no hub-wide Close in the response.

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

type QueuedFanout24Meta[V any] struct {
	snap atomic.Pointer[queuedFanout24Snapshot[V]]
	mu   sync.Mutex // serializes membership edits only
}

type queuedFanout24Snapshot[V any] struct{ subs []*queuedFanout24Sub[V] }

type queuedFanout24Sub[V any] struct {
	ch        chan V
	enqueueMu sync.Mutex // serializes mailbox/overflow placement only
	mu        sync.Mutex
	queue     []V
	queued    atomic.Int64
	closed    atomic.Bool
	refs      atomic.Int64
	done      chan struct{}
}

type QueuedFanout24Subscription[V any] struct {
	sub  *queuedFanout24Sub[V]
	b    *QueuedFanout24Meta[V]
	once sync.Once
}

func NewQueuedFanout24Meta[V any]() *QueuedFanout24Meta[V] {
	b := &QueuedFanout24Meta[V]{}
	b.snap.Store(&queuedFanout24Snapshot[V]{})
	return b
}

func (b *QueuedFanout24Meta[V]) Subscribe() *QueuedFanout24Subscription[V] {
	s := &queuedFanout24Sub[V]{ch: make(chan V, 1), done: make(chan struct{})}
	b.mu.Lock()
	defer b.mu.Unlock()
	old := b.snap.Load()
	next := make([]*queuedFanout24Sub[V], len(old.subs)+1)
	copy(next, old.subs)
	next[len(old.subs)] = s
	b.snap.Store(&queuedFanout24Snapshot[V]{subs: next})
	return &QueuedFanout24Subscription[V]{sub: s, b: b}
}

func (s *QueuedFanout24Subscription[V]) C() <-chan V           { return s.sub.ch }
func (s *QueuedFanout24Subscription[V]) Done() <-chan struct{} { return s.sub.done }

func (s *QueuedFanout24Subscription[V]) Unsubscribe() {
	s.once.Do(func() {
		b, sub := s.b, s.sub
		b.mu.Lock()
		old := b.snap.Load()
		for i, x := range old.subs {
			if x == sub {
				next := make([]*queuedFanout24Sub[V], 0, len(old.subs)-1)
				next = append(next, old.subs[:i]...)
				next = append(next, old.subs[i+1:]...)
				b.snap.Store(&queuedFanout24Snapshot[V]{subs: next})
				break
			}
		}
		b.mu.Unlock()
		sub.enqueueMu.Lock()
		sub.mu.Lock()
		sub.closed.Store(true)
		sub.mu.Unlock()
		sub.enqueueMu.Unlock()
		for sub.refs.Load() != 0 {
			runtime.Gosched()
		}
		close(sub.done)
	})
}

func (b *QueuedFanout24Meta[V]) Send(v V) int {
	subs := b.snap.Load().subs
	accepted := 0
	for _, s := range subs {
		s.refs.Add(1)
		if s.closed.Load() {
			s.refs.Add(-1)
			continue
		}
		delivered := false
		s.enqueueMu.Lock()
		if !s.closed.Load() {
			if s.queued.Load() == 0 {
				select {
				case s.ch <- v:
					delivered = true
				default:
				}
			}
			if !delivered {
				s.mu.Lock()
				s.queue = append(s.queue, v)
				s.queued.Store(int64(len(s.queue)))
				s.mu.Unlock()
				delivered = true
			}
		}
		s.enqueueMu.Unlock()
		if delivered {
			accepted++
		}
		s.refs.Add(-1)
	}
	return accepted
}

func (s *queuedFanout24Sub[V]) pump() bool {
	if s.queued.Load() == 0 {
		return false
	}
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return false
	}
	v := s.queue[0]
	select {
	case s.ch <- v:
		var zero V
		s.queue[0] = zero
		s.queue = s.queue[1:]
		if len(s.queue) == 0 {
			s.queue = nil
		}
		s.queued.Store(int64(len(s.queue)))
		return true
	default:
		return false
	}
}

func (s *QueuedFanout24Subscription[V]) Pump() bool    { return s.sub.pump() }
func (s *QueuedFanout24Subscription[V]) QueueLen() int { return int(s.sub.queued.Load()) }

// RecvContext returns queued values in FIFO order, including values accepted
// before Unsubscribe. ok is false on cancellation or once a withdrawn
// subscription's retained backlog is exhausted. After receiving from C in a
// select, call Pump to refill the mailbox from overflow.
func (s *QueuedFanout24Subscription[V]) RecvContext(ctx context.Context) (V, bool) {
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return zero, false
		}
		s.sub.pump()
		select {
		case v := <-s.sub.ch:
			s.sub.pump()
			return v, true
		default:
		}
		select {
		case <-ctx.Done():
			return zero, false
		case <-s.sub.done:
			// Drain accepted backlog, then report EOF.
			select {
			case v := <-s.sub.ch:
				s.sub.pump()
				return v, true
			default:
				return zero, false
			}
		case v := <-s.sub.ch:
			s.sub.pump()
			return v, true
		}
	}
}

func (s *QueuedFanout24Subscription[V]) Recv() (V, bool) {
	return s.RecvContext(context.Background())
}

func (s *QueuedFanout24Subscription[V]) TryRecv() (V, bool) {
	s.sub.pump()
	select {
	case v := <-s.sub.ch:
		s.sub.pump()
		return v, true
	default:
		return zero24[V](), false
	}
}

func zero24[V any]() V                    { var zero V; return zero }
func (b *QueuedFanout24Meta[V]) Len() int { return len(b.snap.Load().subs) }
