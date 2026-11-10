// Source: 74-deepseek transcript (doc.go, fanout.go, iter.go). Identifiers
// carry the 74Deepseek suffix; behaviour is as generated. Integration review
// notes (verified against this code, see 01-readme.md):
//
//   - The doc sentence "a subscriber that abandons Next releases the
//     broadcaster" does not hold: only Sub74Deepseek.Close (or the sender's own
//     context) releases a Send that is waiting for that subscriber's
//     acknowledgement. Cancelling the context passed to Next does not.
//   - Acknowledgement is implicit in Next / Take (read == ack), so Send is a
//     synchronous hand-off, not a processing barrier.
//   - When the sender's context expires, the unread value stays in the
//     single-slot mailbox and is overwritten by the next Send (documented
//     loss on sender cancellation).
//   - In the non-race build the mailbox is a seqlock
//     (fanout_74_deepseek_fast.go): a reader that observes the new pending
//     epoch before the slot write completes retries without yielding for up
//     to 1000 iterations.
//
// Fanout74Deepseek is a synchronous broadcast
// coordinator with bounded-wait delivery and mid-flight cancellation
// compensation.
//
// Fanout74Deepseek targets the pain point where N subscribers must receive a value
// synchronously and one of them may cancel (via context or explicit Close)
// while the broadcaster is mid-broadcast. Unlike a naive []chan V + mutex
// loop — which deadlocks on cancelled subscribers and stalls all publishers
// on any slow subscriber — Fanout74Deepseek's two-phase offer/ack protocol lets the
// broadcaster bail out (or wake instantly) on cancellation.
//
// Semantics:
//
//   - Send(ctx, v) offers v to every subscriber live at the moment Send
//     begins, then waits for each to ack or cancel. If ctx is cancelled
//     before all acks arrive, Send returns ctx.Err() and the un-acked
//     subscribers are skipped (bounded-loss best-effort).
//   - A subscriber that does not call Next/Take within the sender's deadline
//     misses that epoch. Its next Next/Take will see only the most recent
//     value (the mailbox is single-slot).
//   - A subscriber that calls Close() while a Send is in flight wakes the
//     waiting broadcaster immediately; the value is dropped for that sub.
//   - Multiple concurrent Sends are serialized by an internal mutex (as in
//     the idiomatic baseline), but per-send progress never blocks on a
//     cancelled or slow subscriber beyond ctx.
//
// Zero steady-state allocations on the Send/Next hot path after subscriber
// registration.
package bigbuff

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
)

const cacheLine74Deepseek = 64

var (
	ErrFanoutClosed74Deepseek = errors.New("bigbuff: fanout closed")
	ErrSubClosed74Deepseek    = errors.New("bigbuff: subscriber already closed")
)

// Fanout74Deepseek is a synchronous broadcast coordinator with bounded-wait delivery
// and mid-flight cancellation compensation.
type Fanout74Deepseek[V any] struct {
	_      [cacheLine74Deepseek]byte
	sendMu sync.Mutex
	_      [cacheLine74Deepseek - 8]byte
	seq    atomic.Uint64
	_      [cacheLine74Deepseek - 8]byte
	subMu  sync.Mutex
	_      [cacheLine74Deepseek - 8]byte
	subs   atomic.Pointer[[]*Sub74Deepseek[V]]
	_      [cacheLine74Deepseek - 8]byte
	closed atomic.Bool
	_      [cacheLine74Deepseek - 1]byte
}

func NewFanout74Deepseek[V any]() *Fanout74Deepseek[V] {
	f := &Fanout74Deepseek[V]{}
	empty := []*Sub74Deepseek[V]{}
	f.subs.Store(&empty)
	return f
}

// Subscribe registers a new subscriber. Returns ErrFanoutClosed74Deepseek if the
// fanout has already been closed.
func (f *Fanout74Deepseek[V]) Subscribe() (*Sub74Deepseek[V], error) {
	if f.closed.Load() {
		return nil, ErrFanoutClosed74Deepseek
	}
	s := &Sub74Deepseek[V]{
		fanout:  f,
		subWake: make(chan struct{}, 1),
		bcWake:  make(chan struct{}, 1),
	}
	f.subMu.Lock()
	if f.closed.Load() {
		f.subMu.Unlock()
		return nil, ErrFanoutClosed74Deepseek
	}
	old := *f.subs.Load()
	newSubs := make([]*Sub74Deepseek[V], len(old)+1)
	copy(newSubs, old)
	newSubs[len(old)] = s
	f.subs.Store(&newSubs)
	f.subMu.Unlock()
	return s, nil
}

func (f *Fanout74Deepseek[V]) removeSub(s *Sub74Deepseek[V]) {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	old := *f.subs.Load()
	for i, x := range old {
		if x == s {
			newSubs := make([]*Sub74Deepseek[V], 0, len(old)-1)
			newSubs = append(newSubs, old[:i]...)
			newSubs = append(newSubs, old[i+1:]...)
			f.subs.Store(&newSubs)
			return
		}
	}
}

// Len returns the number of currently registered subscribers.
func (f *Fanout74Deepseek[V]) Len() int { return len(*f.subs.Load()) }

// Send synchronously offers v to every live subscriber and waits for each to
// ack or cancel. Returns ctx.Err() if ctx is cancelled before all acks are
// received; the un-acked subscribers are skipped for this epoch.
func (f *Fanout74Deepseek[V]) Send(ctx context.Context, v V) error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if f.closed.Load() {
		return ErrFanoutClosed74Deepseek
	}
	epoch := f.seq.Add(1)
	subs := *f.subs.Load()
	for _, s := range subs {
		s.offer(v, epoch)
	}
	var firstErr error
	for _, s := range subs {
		if err := s.waitAck(ctx, epoch); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close unregisters all subscribers and marks the fanout closed. Idempotent:
// the second call returns ErrFanoutClosed74Deepseek.
func (f *Fanout74Deepseek[V]) Close() error {
	if f.closed.Swap(true) {
		return ErrFanoutClosed74Deepseek
	}
	f.subMu.Lock()
	subs := *f.subs.Load()
	empty := []*Sub74Deepseek[V]{}
	f.subs.Store(&empty)
	f.subMu.Unlock()
	for _, s := range subs {
		s.Close()
	}
	return nil
}

// Sub74Deepseek is a single-consumer view of a Fanout74Deepseek. Call Next/Take from one
// goroutine only.
type Sub74Deepseek[V any] struct {
	_       [cacheLine74Deepseek]byte
	pending atomic.Uint64
	_       [cacheLine74Deepseek - 8]byte
	acked   atomic.Uint64
	_       [cacheLine74Deepseek - 8]byte
	closed  atomic.Bool
	_       [cacheLine74Deepseek - 1]byte

	slot slot74Deepseek[V]

	subWake chan struct{}
	bcWake  chan struct{}
	fanout  *Fanout74Deepseek[V]
}

func (s *Sub74Deepseek[V]) offer(v V, epoch uint64) bool {
	if s.closed.Load() {
		return false
	}
	for {
		if s.closed.Load() {
			return false
		}
		p := s.pending.Load()
		if p >= epoch {
			return false
		}
		if s.pending.CompareAndSwap(p, epoch) {
			break
		}
	}
	s.slot.write(v, epoch)
	select {
	case s.subWake <- struct{}{}:
	default:
	}
	return true
}

func (s *Sub74Deepseek[V]) waitAck(ctx context.Context, epoch uint64) error {
	for i := 0; i < 32; i++ {
		if s.closed.Load() || s.acked.Load() >= epoch {
			return nil
		}
	}
	for {
		if s.closed.Load() || s.acked.Load() >= epoch {
			return nil
		}
		select {
		case <-s.bcWake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Next blocks until either a value is delivered or ctx is cancelled (or the
// subscriber/fanout is closed). Returns (zero, false) on cancellation.
func (s *Sub74Deepseek[V]) Next(ctx context.Context) (V, bool) {
	var zero V
	for {
		if s.closed.Load() {
			return zero, false
		}
		pending := s.pending.Load()
		if pending != 0 && pending > s.acked.Load() {
			v, slotEpoch := s.slot.read()
			if slotEpoch == pending {
				s.acked.Store(pending)
				s.pending.CompareAndSwap(pending, 0)
				select {
				case s.bcWake <- struct{}{}:
				default:
				}
				return v, true
			}
			continue
		}
		select {
		case <-s.subWake:
		case <-ctx.Done():
			return zero, false
		}
	}
}

// Take is the non-blocking counterpart of Next. Composable with Ready():
//
//	select {
//	case <-sub.Ready():
//	    if v, ok := sub.Take(); ok { handle(v) }
//	case <-ctx.Done():
//	    return
//	}
func (s *Sub74Deepseek[V]) Take() (V, bool) {
	var zero V
	if s.closed.Load() {
		return zero, false
	}
	pending := s.pending.Load()
	if pending == 0 || pending <= s.acked.Load() {
		return zero, false
	}
	v, slotEpoch := s.slot.read()
	if slotEpoch != pending {
		return zero, false
	}
	s.acked.Store(pending)
	s.pending.CompareAndSwap(pending, 0)
	select {
	case s.bcWake <- struct{}{}:
	default:
	}
	return v, true
}

// Ready returns a channel that becomes readable when a pending offer is
// available. Use with Take() to build a select-composable receive loop.
func (s *Sub74Deepseek[V]) Ready() <-chan struct{} { return s.subWake }

// Closed reports whether this subscriber has been closed.
func (s *Sub74Deepseek[V]) Closed() bool { return s.closed.Load() }

// Close unregisters the subscriber and wakes any broadcaster waiting on it.
// Idempotent.
func (s *Sub74Deepseek[V]) Close() error {
	if s.closed.Swap(true) {
		return ErrSubClosed74Deepseek
	}
	select {
	case s.bcWake <- struct{}{}:
	default:
	}
	select {
	case s.subWake <- struct{}{}:
	default:
	}
	if s.fanout != nil {
		s.fanout.removeSub(s)
	}
	return nil
}

// Iter returns a range-over-function iterator that yields values until ctx is
// cancelled, the subscriber is closed, or the caller breaks out of the loop.
// Breaking out does not auto-close the subscriber; call Close explicitly.
func (s *Sub74Deepseek[V]) Iter(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		for {
			v, ok := s.Next(ctx)
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
