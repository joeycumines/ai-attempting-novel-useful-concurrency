/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

// Provenance: 41-arena-claude-sonnet-5.5-high-prompt-mini.md (Claude Sonnet, response #41).
// Extracted EpochCast names are response-scoped for the flat package.

import (
	"context"
	"errors"
	"iter"
	"math"
	"math/bits"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	// ErrEpochClosed41Sonnet is returned by publishes after EpochCast41Sonnet.Close, and by
	// EpochSub41Sonnet.Recv once a closed broadcaster has been fully drained.
	ErrEpochClosed41Sonnet = errors.New("bigbuff: epochcast: closed")
	// ErrEpochFull41Sonnet is returned by TryPublish when the ring cannot accept a value.
	ErrEpochFull41Sonnet = errors.New("bigbuff: epochcast: full")
	// ErrEpochLossy41Sonnet is returned by operations that only make sense in lossless mode.
	ErrEpochLossy41Sonnet = errors.New("bigbuff: epochcast: operation requires lossless mode")
)

// EpochLag41Sonnet is returned (as an error) by EpochSub41Sonnet.Recv when a lossy subscriber
// fell behind; the value is the number of values that were skipped.
type EpochLag41Sonnet uint64

func (e EpochLag41Sonnet) Error() string {
	return "bigbuff: epochcast: subscriber lagged, lost " + strconv.FormatUint(uint64(e), 10) + " value(s)"
}

// EpochMode41Sonnet selects the overflow policy of an EpochCast41Sonnet.
type EpochMode41Sonnet uint8

const (
	// EpochLossless41Sonnet never drops: publishers wait (or fail, for TryPublish)
	// until the slowest subscriber has released the slot being reused.
	EpochLossless41Sonnet EpochMode41Sonnet = iota
	// EpochLossy41Sonnet never blocks publishers: subscribers that fall more than
	// the ring capacity behind skip ahead and are told how much they lost.
	EpochLossy41Sonnet
)

// EpochStatus41Sonnet is the result class of EpochSub41Sonnet.TryRecv.
type EpochStatus41Sonnet uint8

const (
	EpochEmpty41Sonnet  EpochStatus41Sonnet = iota // nothing pending right now
	EpochValue41Sonnet                             // a value was received
	EpochLagged41Sonnet                            // (lossy) values were skipped; see the lost count
	EpochClosed41Sonnet                            // subscription closed, or broadcaster closed and drained
)

// epochDetached41Sonnet is the cursor sentinel of a closed subscription.
const epochDetached41Sonnet = math.MaxUint64

// epochClosedChan41Sonnet doubles as "the gate is not armed" and "you are ready".
var epochClosedChan41Sonnet = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// epochGate41Sonnet is a lazily-armed, one-shot-per-epoch broadcast latch.
//
//   - arm: returns the live channel for the current epoch, allocating one
//     only if none is live (i.e. only when somebody actually intends to park).
//   - open: closes the live channel, if any, and returns to the unarmed state.
//     Unarmed open() is a single atomic load.
//
// Storing a chan in an atomic.Value does not allocate (chans are
// pointer-shaped), and the sentinel avoids a nil state.
type epochGate41Sonnet struct{ v atomic.Value }

func (g *epochGate41Sonnet) init() { g.v.Store(epochClosedChan41Sonnet) }

func (g *epochGate41Sonnet) arm() <-chan struct{} {
	cur := g.v.Load().(chan struct{})
	if cur != epochClosedChan41Sonnet {
		return cur
	}
	fresh := make(chan struct{})
	for {
		if g.v.CompareAndSwap(epochClosedChan41Sonnet, fresh) {
			return fresh
		}
		if cur = g.v.Load().(chan struct{}); cur != epochClosedChan41Sonnet {
			return cur
		}
	}
}

func (g *epochGate41Sonnet) open() {
	if g.v.Load().(chan struct{}) == epochClosedChan41Sonnet {
		return
	}
	if old := g.v.Swap(epochClosedChan41Sonnet).(chan struct{}); old != epochClosedChan41Sonnet {
		close(old)
	}
}

type epochSlot41Sonnet[V any] struct {
	// tag is seq+1 while the slot holds seq (0 = invalid / being overwritten).
	tag atomic.Uint64
	// refs: lossless = subscribers that have yet to release this slot;
	//       lossy    = readers currently copying the value (Dekker guard).
	refs atomic.Int32
	v    V
}

// EpochCast41Sonnet is a single-ring, multi-subscriber broadcaster whose subscribers
// are selectable without being registered anywhere.
//
// Allocation profile:
//   - Publish / TryPublish / TryRecv / Recv: 0 allocations.
//   - Ready: 0 allocations, except that the first parker of an epoch allocates
//     one channel, shared by every other parker until the next publish.
//   - Subscribe: 1 allocation (80 bytes). Close: 0.
//
// Use NewEpochCast to construct.
type EpochCast41Sonnet[V any] struct {
	_     [64]byte
	head  atomic.Uint64 // next sequence to publish; read by every consumer
	_     [64]byte
	wake  epochGate41Sonnet // opened on publish / close / cancel
	space epochGate41Sonnet // opened when a slot is fully released (lossless)

	nsubs  atomic.Int32
	closed atomic.Bool
	lossy  bool

	mask, capacity uint64
	ring           []epochSlot41Sonnet[V]

	mu  sync.Mutex    // orders subscribe/detach/close against commit
	pub chan struct{} // lossless publisher semaphore (ctx-aware, FIFO-ish)
}

// NewEpochCast creates a broadcaster; capacity is rounded up to a power of two.
func NewEpochCast41Sonnet[V any](capacity int, mode EpochMode41Sonnet) *EpochCast41Sonnet[V] {
	if capacity < 1 || capacity > 1<<30 {
		panic(`bigbuff: epochcast: capacity out of bounds`)
	}
	n := uint64(1) << bits.Len64(uint64(capacity-1))
	x := &EpochCast41Sonnet[V]{
		lossy:    mode == EpochLossy41Sonnet,
		mask:     n - 1,
		capacity: n,
		ring:     make([]epochSlot41Sonnet[V], n),
		pub:      make(chan struct{}, 1),
	}
	x.wake.init()
	x.space.init()
	return x
}

// Subscribers returns the current number of live subscriptions.
func (x *EpochCast41Sonnet[V]) Subscribers() int { return int(x.nsubs.Load()) }

// Subscribe creates a subscription that observes every value published after
// this call. It must eventually be closed (or its slots held forever, in
// lossless mode).
func (x *EpochCast41Sonnet[V]) Subscribe() *EpochSub41Sonnet[V] {
	s := &EpochSub41Sonnet[V]{x: x}
	x.mu.Lock()
	if x.closed.Load() {
		s.cursor.Store(epochDetached41Sonnet)
	} else {
		s.cursor.Store(x.head.Load())
		x.nsubs.Add(1)
	}
	x.mu.Unlock()
	return s
}

// Close stops all publishing and wakes everyone. Subscribers drain whatever
// is pending and then observe EpochClosed41Sonnet.
func (x *EpochCast41Sonnet[V]) Close() {
	x.mu.Lock()
	already := x.closed.Swap(true)
	x.mu.Unlock()
	if already {
		return
	}
	x.wake.open()
	x.space.open()
}

// Publish delivers v to every current subscriber and returns how many that was.
// Lossless: waits for ring space (or ctx). Lossy: never waits.
func (x *EpochCast41Sonnet[V]) Publish(ctx context.Context, v V) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return x.publish(ctx, v, true)
}

// TryPublish is a non-blocking Publish; lossless mode may return ErrEpochFull41Sonnet.
func (x *EpochCast41Sonnet[V]) TryPublish(v V) (int, error) {
	return x.publish(nil, v, false)
}

func (x *EpochCast41Sonnet[V]) publish(ctx context.Context, v V, block bool) (int, error) {
	if x.closed.Load() {
		return 0, ErrEpochClosed41Sonnet
	}
	if x.nsubs.Load() == 0 {
		return 0, nil // nobody to deliver to (fast path)
	}
	if x.lossy {
		return x.publishLossy(v)
	}

	if block {
		select {
		case x.pub <- struct{}{}:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	} else {
		select {
		case x.pub <- struct{}{}:
		default:
			return 0, ErrEpochFull41Sonnet
		}
	}
	defer func() { <-x.pub }()

	// We are the only publisher: head only moves under this semaphore.
	for {
		if x.closed.Load() {
			return 0, ErrEpochClosed41Sonnet
		}
		s := x.head.Load()
		sl := &x.ring[s&x.mask]
		if sl.refs.Load() != 0 { // previous occupant not fully consumed
			if !block {
				return 0, ErrEpochFull41Sonnet
			}
			ch := x.space.arm()
			if sl.refs.Load() != 0 { // re-check AFTER arming (no lost wakeup)
				select {
				case <-ch:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			continue
		}

		x.mu.Lock()
		if x.closed.Load() {
			x.mu.Unlock()
			return 0, ErrEpochClosed41Sonnet
		}
		n := x.nsubs.Load()
		if n == 0 {
			x.mu.Unlock()
			return 0, nil
		}
		sl.v = v
		sl.tag.Store(s + 1)
		sl.refs.Store(n)
		x.head.Store(s + 1) // publish AFTER the slot is complete...
		x.mu.Unlock()
		x.wake.open() // ...and wake AFTER head moved (see Ready)
		return int(n), nil
	}
}

func (x *EpochCast41Sonnet[V]) publishLossy(v V) (int, error) {
	x.mu.Lock()
	if x.closed.Load() {
		x.mu.Unlock()
		return 0, ErrEpochClosed41Sonnet
	}
	n := int(x.nsubs.Load())
	if n == 0 {
		x.mu.Unlock()
		return 0, nil
	}
	s := x.head.Load()
	sl := &x.ring[s&x.mask]
	if s >= x.capacity {
		// Dekker pairing with lossyRead: either the reader sees tag==0 and
		// backs off, or we see its refs increment and wait for the copy.
		sl.tag.Store(0)
		for sl.refs.Load() != 0 {
			runtime.Gosched()
		}
	}
	sl.v = v
	sl.tag.Store(s + 1)
	x.head.Store(s + 1)
	x.mu.Unlock()
	x.wake.open()
	return n, nil
}

// Flush (lossless only) waits until every value published so far has been
// consumed or abandoned by every subscriber that was owed it. Publish+Flush
// reproduces ChanPubSub.Send's "returns once everyone has it" rendezvous,
// but without per-consumer work.
func (x *EpochCast41Sonnet[V]) Flush(ctx context.Context) error {
	if x.lossy {
		return ErrEpochLossy41Sonnet
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h := x.head.Load()
	if h == 0 {
		return nil
	}
	last := h - 1
	sl := &x.ring[last&x.mask]
	drained := func() bool {
		// Order matters: refs first, then tag. If tag still says `last`, the
		// refs we read belonged to `last`; if not, `last` was long released.
		r := sl.refs.Load()
		t := sl.tag.Load()
		return t != last+1 || r == 0
	}
	// Subscribers consume in order, so the newest slot draining implies all
	// older ones drained for every subscriber that was owed them.
	for !drained() {
		ch := x.space.arm()
		if drained() {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// unref releases one subscriber's claim on a slot (lossless); reports whether
// the slot became free. The last releaser best-effort clears the value so the
// ring does not pin garbage.
func (x *EpochCast41Sonnet[V]) unref(sl *epochSlot41Sonnet[V]) bool {
	if sl.refs.Load() == 1 { // only we remain: nobody else can be reading
		var zero V
		sl.v = zero
	}
	return sl.refs.Add(-1) == 0
}

func (x *EpochCast41Sonnet[V]) lossyRead(sl *epochSlot41Sonnet[V], c uint64) (v V, ok bool) {
	sl.refs.Add(1)
	if sl.tag.Load() == c+1 {
		v, ok = sl.v, true
	}
	sl.refs.Add(-1)
	return
}

// Iter eagerly subscribes and returns a single-use iterator over the stream.
// Lag in lossy mode is skipped silently. The subscription is released when
// iteration stops or ctx is done; the iterator MUST be ranged over, or ctx
// cancelled, to avoid leaking the subscription.
func (x *EpochCast41Sonnet[V]) Iter(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	sub := x.Subscribe()
	stop := context.AfterFunc(ctx, sub.Close)
	return func(yield func(V) bool) {
		if !stop() {
			return // ctx already done; AfterFunc closed the subscription
		}
		defer sub.Close()
		done := ctx.Done()
		for {
			v, _, st := sub.TryRecv()
			switch st {
			case EpochValue41Sonnet:
				if !yield(v) {
					return
				}
				continue
			case EpochLagged41Sonnet:
				continue
			case EpochClosed41Sonnet:
				return
			}
			select {
			case <-done:
				return
			case <-sub.Ready():
			}
		}
	}
}

// EpochSub41Sonnet is one consumer's cursor. TryRecv/Recv/Ready/Close are all safe
// for concurrent use; multiple goroutines sharing one EpochSub41Sonnet act as
// competing consumers of a single logical subscriber.
type EpochSub41Sonnet[V any] struct {
	_      [64]byte
	cursor atomic.Uint64 // next sequence to read, or epochDetached41Sonnet
	x      *EpochCast41Sonnet[V]
}

// Ready returns a channel suitable for select. It is closed (or already
// closed) whenever TryRecv would not return EpochEmpty41Sonnet. Call it afresh on
// every select iteration; it is cheap, allocation-free in steady state, and
// never loses a wakeup:
//
//	for {
//	    select {
//	    case <-sub.Ready():
//	        v, lost, st := sub.TryRecv() // st may be Empty on a spurious wake
//	    case <-other:
//	    }
//	}
func (s *EpochSub41Sonnet[V]) Ready() <-chan struct{} {
	if s.ready() {
		return epochClosedChan41Sonnet
	}
	ch := s.x.wake.arm()
	if s.ready() { // re-check AFTER arming: head moves before the gate opens
		return epochClosedChan41Sonnet
	}
	return ch
}

func (s *EpochSub41Sonnet[V]) ready() bool {
	// A detached cursor (MaxUint64) never equals head, so it reads as ready.
	return s.cursor.Load() != s.x.head.Load() || s.x.closed.Load()
}

// Pending returns the number of values currently waiting for this subscriber.
func (s *EpochSub41Sonnet[V]) Pending() uint64 {
	c := s.cursor.Load()
	h := s.x.head.Load()
	if c == epochDetached41Sonnet || c >= h {
		return 0
	}
	if p := h - c; !s.x.lossy || p <= s.x.capacity {
		return p
	}
	return s.x.capacity
}

// TryRecv never blocks. On EpochLagged41Sonnet (lossy only) v is the zero value and
// lost is the number of values skipped; the next call resumes at the oldest
// value still in the ring.
func (s *EpochSub41Sonnet[V]) TryRecv() (v V, lost uint64, st EpochStatus41Sonnet) {
	x := s.x
	for {
		c := s.cursor.Load()
		if c == epochDetached41Sonnet {
			return v, 0, EpochClosed41Sonnet
		}
		h := x.head.Load()
		if c >= h {
			// Re-read head after seeing closed: no publish can follow it.
			if x.closed.Load() && c >= x.head.Load() {
				return v, 0, EpochClosed41Sonnet
			}
			return v, 0, EpochEmpty41Sonnet
		}
		sl := &x.ring[c&x.mask]

		if !x.lossy {
			// Claim first, then read: our un-released ref pins the slot, and
			// a racing Close only releases from the cursor it wins.
			if !s.cursor.CompareAndSwap(c, c+1) {
				continue
			}
			v = sl.v
			if x.unref(sl) {
				x.space.open()
			}
			return v, 0, EpochValue41Sonnet
		}

		if h-c > x.capacity { // definitely overwritten
			target := h - x.capacity
			if s.cursor.CompareAndSwap(c, target) {
				return v, 0 + target - c, EpochLagged41Sonnet
			}
			continue
		}
		if !s.cursor.CompareAndSwap(c, c+1) {
			continue
		}
		if got, ok := x.lossyRead(sl, c); ok {
			return got, 0, EpochValue41Sonnet
		}
		// Lost the race with the publisher: slot c was overwritten, which
		// implies head >= c+capacity, so this cannot underflow.
		want := x.head.Load() - x.capacity + 1
		lost = 1
		for {
			cur := s.cursor.Load()
			if cur >= want { // also covers detached
				break
			}
			if s.cursor.CompareAndSwap(cur, want) {
				lost += want - cur
				break
			}
		}
		return v, lost, EpochLagged41Sonnet
	}
}

// Recv blocks until a value, lag, closure, or ctx. Lag is reported as an
// EpochLag41Sonnet error (the stream remains usable); closure as ErrEpochClosed41Sonnet.
func (s *EpochSub41Sonnet[V]) Recv(ctx context.Context) (V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		v, lost, st := s.TryRecv()
		switch st {
		case EpochValue41Sonnet:
			return v, nil
		case EpochLagged41Sonnet:
			return v, EpochLag41Sonnet(lost)
		case EpochClosed41Sonnet:
			return v, ErrEpochClosed41Sonnet
		}
		select {
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		case <-s.Ready():
		}
	}
}

// Close releases the subscription. Idempotent; safe against concurrent
// TryRecv. It does NOT wake other parked consumers (churn stays O(1) with no
// thundering herd); use Cancel if another goroutine is parked on THIS
// subscription's Ready channel and must be woken.
func (s *EpochSub41Sonnet[V]) Close() { s.detach(false) }

// Cancel is Close plus a wakeup of everything parked on the gate (spurious for
// other subscribers, who simply re-check).
func (s *EpochSub41Sonnet[V]) Cancel() { s.detach(true) }

func (s *EpochSub41Sonnet[V]) detach(wake bool) {
	x := s.x
	x.mu.Lock() // freezes head: no commit can add slots we would miss
	c := s.cursor.Load()
	for c != epochDetached41Sonnet && !s.cursor.CompareAndSwap(c, epochDetached41Sonnet) {
		c = s.cursor.Load() // lost a race with a claiming TryRecv
	}
	if c == epochDetached41Sonnet {
		x.mu.Unlock()
		if wake {
			x.wake.open()
		}
		return
	}
	x.nsubs.Add(-1)
	released := false
	if !x.lossy {
		// Abandon everything unread: O(lag) <= O(capacity).
		for h := x.head.Load(); c < h; c++ {
			if x.unref(&x.ring[c&x.mask]) {
				released = true
			}
		}
	}
	x.mu.Unlock()
	if released {
		x.space.open()
	}
	if wake {
		x.wake.open()
	}
}
