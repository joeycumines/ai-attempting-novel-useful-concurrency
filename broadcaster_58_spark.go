package bigbuff

// Provenance: 58-arena-muse-spark-1.3-xhigh-prompt-safe.md, section 3
// (Broadcaster: power-of-two ring + swap-then-close generation).
//
// The response's version-before/version-after reads do NOT make a plain Go
// struct assignment atomic: a racing write is a data race even if validation
// subsequently fails. To preserve the shared-ring/seqlock design rather than
// replace it with immutable nodes, each cell has an RWMutex protecting its
// payload copy; atomic sequence validation and wrap-safe cursor arithmetic
// remain. This repairs wide-payload tearing and forward-progress starvation
// without changing loss policy, O(1) publication, or close-broadcast wakeup.
// Other repairs: overflow-checked sizing, idempotent Close, cancellation-safe
// iterator cleanup, and a closed/caught-up distinction in the harness adapter.
// The ring retains up to capacity values; a new subscription starts live.
// Next reports exact skipped sequence numbers. SubscribeContext deliberately
// hides that metadata and is therefore for loss-tolerant consumers only.

import (
	"context"
	"iter"
	"math/bits"
	"sync"
	"sync/atomic"
)

type broadcaster58Seq struct {
	v atomic.Uint64
	_ [56]byte
}

type broadcaster58Cell[V any] struct {
	mu    sync.RWMutex
	seq   broadcaster58Seq
	value V
}

type Broadcaster58Spark[V any] struct {
	cells       []broadcaster58Cell[V]
	mask        uint64
	closedChan  chan struct{}
	done        chan struct{}
	next        atomic.Uint64
	_           [56]byte
	published   atomic.Uint64
	_           [56]byte
	cur         atomic.Value // chan struct{}; one channel allocation per Send
	closed      atomic.Bool
	subscribers atomic.Int32
	sendMu      sync.Mutex
}

// Broadcaster58Subscription is a single-owner cursor. Notify is shared by all
// subscribers; Next, unlike Notify, advances only the calling cursor.
type Broadcaster58Subscription[V any] struct {
	b    *Broadcaster58Spark[V]
	next uint64
	done atomic.Bool
}

func NewBroadcaster58Spark[V any](size int) *Broadcaster58Spark[V] {
	if size <= 0 {
		size = 64
	}
	maxInt := int(^uint(0) >> 1)
	if size > maxInt/2+1 {
		panic("bigbuff: broadcaster58: capacity overflow")
	}
	n := 1 << bits.Len(uint(size-1))
	b := &Broadcaster58Spark[V]{
		cells: make([]broadcaster58Cell[V], n), mask: uint64(n - 1),
		closedChan: make(chan struct{}), done: make(chan struct{}),
	}
	close(b.closedChan)
	b.cur.Store(make(chan struct{}))
	return b
}

func (b *Broadcaster58Spark[V]) Send(v V) uint64 {
	b.sendMu.Lock()
	if b.closed.Load() {
		b.sendMu.Unlock()
		panic("bigbuff: broadcaster58: send on closed broadcaster")
	}
	seq := b.next.Add(1)
	cell := &b.cells[seq&b.mask]
	cell.mu.Lock()
	cell.value = v
	cell.seq.v.Store(seq)
	cell.mu.Unlock()
	b.published.Store(seq)
	fresh := make(chan struct{})
	old := b.cur.Swap(fresh)
	b.sendMu.Unlock()
	close(old.(chan struct{}))
	return seq
}

func (b *Broadcaster58Spark[V]) Subscribe() *Broadcaster58Subscription[V] {
	pub := b.published.Load()
	b.subscribers.Add(1)
	return &Broadcaster58Subscription[V]{b: b, next: pub + 1}
}

func (s *Broadcaster58Subscription[V]) Unsubscribe() {
	if s.done.CompareAndSwap(false, true) {
		s.b.subscribers.Add(-1)
	}
}

func (b *Broadcaster58Spark[V]) Notify() <-chan struct{}        { return b.cur.Load().(chan struct{}) }
func (s *Broadcaster58Subscription[V]) Notify() <-chan struct{} { return s.b.Notify() }
func (b *Broadcaster58Spark[V]) IsClosed() bool                 { return b.closed.Load() }
func (b *Broadcaster58Spark[V]) Done() <-chan struct{}          { return b.done }
func (b *Broadcaster58Spark[V]) Subscribers() int               { return int(b.subscribers.Load()) }

// Next returns the next retained value and the exact number of publications
// skipped since the cursor's previous position. A caught-up cursor returns
// false even after Close; callers distinguish EOF using Done/IsClosed.
func (s *Broadcaster58Subscription[V]) Next() (v V, ok bool, missed uint64) {
	var zero V
	if s.done.Load() {
		return zero, false, 0
	}
	b, original := s.b, s.next
	next, size := original, b.mask+1
	for {
		pub := b.published.Load()
		if pub-next == ^uint64(0) { // wrap-safe caught-up test
			return zero, false, 0
		}
		if pub-next >= size {
			next = pub - size + 1
		}
		cell := &b.cells[next&b.mask]
		cell.mu.RLock()
		if cell.seq.v.Load() != next {
			cell.mu.RUnlock()
			continue
		}
		v = cell.value
		valid := cell.seq.v.Load() == next
		cell.mu.RUnlock()
		if !valid {
			continue
		}
		s.next = next + 1
		return v, true, next - original
	}
}

// Close stops publication and wakes all selectors. Retained values remain
// readable by existing cursors. Repeated Close is a no-op; Send after Close
// panics, matching the response's explicit failure contract.
func (b *Broadcaster58Spark[V]) Close() {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	if b.closed.Swap(true) {
		return
	}
	old := b.cur.Swap(b.closedChan)
	close(b.done)
	close(old.(chan struct{}))
}

func (b *Broadcaster58Spark[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := b.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)
	return func(yield func(V) bool) {
		defer sub.Unsubscribe()
		defer stop()
		if yield == nil {
			panic("bigbuff: broadcaster58: nil iterator yield")
		}
		for {
			notify := sub.Notify()
			for {
				v, ok, _ := sub.Next()
				if !ok {
					break
				}
				if !yield(v) {
					return
				}
			}
			if b.IsClosed() || ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-b.Done():
			case <-notify:
			}
		}
	}
}
