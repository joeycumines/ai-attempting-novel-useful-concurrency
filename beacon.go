/*
 Copyright 2026 Joseph Cumines
 Licensed under Apache-2.0
*/
package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

var closedChan chan struct{} = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type noCopy struct{}
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

type slot[T any] struct {
	seq uint64
	val T
}

// Beacon is an O(1) selectable broadcast.
//
// Send is O(1): store value via RCU, swap signal channel, close old.
// Receivers select on Signal(); close wakes all waiters inside runtime.closechan
// with a single hchan.lock iteration - the same batch wake the callback design
// does in user space, but scheduler-aware and churn-invariant.
//
// Allocation: 1 hchan (96B) + 1 slot per Send. Stable, predictable, no per-
// subscriber allocation. Pool to 0 with sync.Pool if >500k/s.
type Beacon[T any] struct {
	_ noCopy
	// hot atomics on own line
	seq atomic.Uint64
	val atomic.Pointer[slot[T]]
	sig atomic.Pointer[chan struct{}]
	_ [56]byte // pad sig from sendMu
	sendMu sync.Mutex
	pool   sync.Pool // *slot[T] reuse
}

func NewBeacon[T any]() *Beacon[T] {
	b := &Beacon[T]{}
	b.pool.New = func() any { return new(slot[T]) }
	ch := make(chan struct{})
	b.sig.Store(&ch)
	return b
}

// Send publishes v to all current and future select waiters.
// Returns monotonic sequence. Concurrency: one Send at a time (sendMu),
// concurrent Signal/Load are lock-free (acquire/release via atomic).
func (b *Beacon[T]) Send(v T) uint64 {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	seq := b.seq.Add(1)
	s := b.pool.Get().(*slot[T])
	s.seq = seq
	s.val = v
	b.val.Store(s) // release: must be before swap so waker sees value
	old := b.sig.Load()
	nch := make(chan struct{})
	b.sig.Store(&nch) // new waiters park here
	close(*old)       // batch wake: runtime iterates sudog waitq once
	return seq
}

func (b *Beacon[T]) Load() (T, uint64) {
	s := b.val.Load()
	if s == nil {
		var zero T
		return zero, 0
	}
	return s.val, s.seq
}
func (b *Beacon[T]) Seq() uint64               { return b.seq.Load() }
func (b *Beacon[T]) Signal() <-chan struct{}   { return *b.sig.Load() }

// ReadyFor returns a chan that is ready if seq >= next.
// Use in select to avoid lost wakeup between Load and Signal:
//
//	next := beacon.Seq()+1
//	for {
//	  select {
//	  case <-beacon.ReadyFor(next):
//	  case <-ctx.Done(): return
//	  }
//	  v,seq := beacon.Load()
//	  // ...
//	}
func (b *Beacon[T]) ReadyFor(next uint64) <-chan struct{} {
	if b.seq.Load() >= next {
		return closedChan
	}
	return b.Signal()
}

// SubscribeContext yields every published value from now until ctx cancel.
// No registration - churn is free. 0 allocs beyond Send.
func (b *Beacon[T]) SubscribeContext(ctx context.Context) iter.Seq[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(T) bool) {
		if yield == nil {
			panic(`bigbuff: beacon: nil yield`)
		}
		next := b.seq.Load() + 1
		// if no value yet, wait; else catch up immediately
		for {
			s := b.val.Load()
			if s != nil && s.seq >= next {
				next = s.seq + 1
				if !yield(s.val) {
					return
				}
				continue
			}
			sig := b.Signal()
			// re-check after getting sig to close race where Send swapped between Load and Signal
			if s := b.val.Load(); s != nil && s.seq >= next {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-sig:
			}
		}
	}
}


