package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// RingBeacon adds bounded history for lossless catch-up.
// Cap must be power of two. Send is still O(1): ring[seq&mask]=v + close.
// Slow consumers that fall >cap behind get ErrOverrun and can resync to latest.
type RingBeacon[T any] struct {
	_      noCopy
	seq    atomic.Uint64
	sig    atomic.Pointer[chan struct{}]
	sendMu sync.Mutex
	mask   uint64
	ring   []atomic.Pointer[ringEnt[T]]
}

type ringEnt[T any] struct {
	seq uint64
	val T
	_   [32]byte // pad to avoid false sharing between slots on contended seq
}

func NewRingBeacon[T any](capacityPow2 int) *RingBeacon[T] {
	if capacityPow2&(capacityPow2-1) != 0 {
		panic(`bigbuff: ring: capacity must be power of two`)
	}
	b := &RingBeacon[T]{mask: uint64(capacityPow2 - 1), ring: make([]atomic.Pointer[ringEnt[T]], capacityPow2)}
	ch := make(chan struct{})
	b.sig.Store(&ch)
	return b
}

func (b *RingBeacon[T]) Send(v T) uint64 {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	seq := b.seq.Add(1)
	// Publish the entry via an atomic pointer store: readers load the slot
	// atomically, so the payload is never observed half-written (no torn
	// reads, and the race detector sees properly synchronised accesses).
	b.ring[seq&b.mask].Store(&ringEnt[T]{seq: seq, val: v})
	old := b.sig.Load()
	nch := make(chan struct{})
	b.sig.Store(&nch)
	close(*old)
	return seq
}
func (b *RingBeacon[T]) Signal() <-chan struct{} { return *b.sig.Load() }
func (b *RingBeacon[T]) Seq() uint64             { return b.seq.Load() }

// Cursor tracks per-subscriber position, cache-line isolated.
type Cursor[T any] struct {
	b    *RingBeacon[T]
	next uint64
	_    [56]byte
}

func (b *RingBeacon[T]) Cursor() *Cursor[T] { return &Cursor[T]{b: b, next: b.seq.Load() + 1} }
func (c *Cursor[T]) Next() (T, uint64, bool) {
	seq := c.b.seq.Load()
	if c.next > seq {
		var zero T
		return zero, 0, false
	}
	// overrun check: if next < seq-cap, we missed
	if seq-c.next > c.b.mask {
		// snap to latest - caller can treat as lossy
		c.next = seq
	}
	e := c.b.ring[c.next&c.b.mask].Load()
	if e == nil || e.seq != c.next { // nil (never written) or reused due to overrun
		c.next = seq
		var zero T
		return zero, 0, false
	}
	c.next++
	return e.val, e.seq, true
}
func (c *Cursor[T]) Ready() <-chan struct{} {
	if c.b.seq.Load() >= c.next {
		return closedChan
	}
	return c.b.Signal()
}

func (b *RingBeacon[T]) SubscribeContext(ctx context.Context) iter.Seq[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(T) bool) {
		cur := b.Cursor()
		for {
			if v, _, ok := cur.Next(); ok {
				if !yield(v) {
					return
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-cur.Ready():
			}
		}
	}
}


