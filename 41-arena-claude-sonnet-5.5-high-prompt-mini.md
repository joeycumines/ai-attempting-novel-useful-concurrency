# EpochCast: a lazily-armed epoch gate over a ref-counted broadcast ring

This is a third, complementary primitive. It is not derived from `ChanCaster` or `ChanPubSub`. I have not compiled or run any of this, so run the tests with `-race` before relying on it.

## The core idea

The callback-channel design scales badly because every consumer is an object the producer must touch: N sends per publish, plus registration churn. Select-compatibility is the only reason a consumer needs a channel at all. EpochCast therefore separates the two concerns:

| Plane | Mechanism | Cost |
|---|---|---|
| **Data** | Power-of-two ring, with a per-slot refcount (lossless) or a per-slot Dekker reader guard (lossy). Each subscriber is just an atomic cursor. | Publish is O(1) in the number of consumers. Subscribe and unsubscribe are O(1), plus O(lag ≤ cap) to release unread slots. |
| **Wake** | A **lazily-armed epoch gate**: one shared `chan struct{}`, created only when a consumer is about to park, and closed (not sent to) on publish. | At most **1 channel allocation per publish that has parked consumers**, shared by all N of them. Zero when nobody is parked. |

Several properties follow from this:

- **Select works without registration.** `sub.Ready()` returns a channel to put in a `select`, and nothing is registered anywhere. A consumer that never parks costs the producer nothing, and a consumer that vanishes costs nothing either.
- **`Ready()` is level-triggered on top of an edge-triggered primitive.** If the subscriber already has pending data, `Ready()` returns a global pre-closed channel. Otherwise it returns the shared gate. It arms the gate first and then re-checks, so no wakeup can be lost. The argument is that `head` is advanced before the gate is opened. A consumer that armed after the open must therefore see the new head, and one that armed before it gets its channel closed.
- **Repeated selects allocate nothing.** If another `select` case keeps firing, `Ready()` keeps returning the same still-open gate.
- **Backpressure is O(1).** The slot refcount means the publisher never scans subscribers. The last reader of a slot releases it, and a publisher parked on the space gate is woken only then. The same space gate gives `Flush` (the old `Send`-waits-for-everyone rendezvous) for free.
- **A subscription can be shared.** The cursor is advanced by claim-then-read CAS, so several goroutines can use one `EpochSub` as competing consumers. `Close` can also race with `TryRecv` safely.

Two modes are supported:

- **Lossless:** the publisher blocks, or `TryPublish` returns `ErrEpochFull`.
- **Lossy:** the publisher never blocks, and slow consumers get `EpochLagged` with the exact number of lost values.

## `epochcast.go`

```go
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
	// ErrEpochClosed is returned by publishes after EpochCast.Close, and by
	// EpochSub.Recv once a closed broadcaster has been fully drained.
	ErrEpochClosed = errors.New("bigbuff: epochcast: closed")
	// ErrEpochFull is returned by TryPublish when the ring cannot accept a value.
	ErrEpochFull = errors.New("bigbuff: epochcast: full")
	// ErrEpochLossy is returned by operations that only make sense in lossless mode.
	ErrEpochLossy = errors.New("bigbuff: epochcast: operation requires lossless mode")
)

// EpochLag is returned (as an error) by EpochSub.Recv when a lossy subscriber
// fell behind; the value is the number of values that were skipped.
type EpochLag uint64

func (e EpochLag) Error() string {
	return "bigbuff: epochcast: subscriber lagged, lost " + strconv.FormatUint(uint64(e), 10) + " value(s)"
}

// EpochMode selects the overflow policy of an EpochCast.
type EpochMode uint8

const (
	// EpochLossless never drops: publishers wait (or fail, for TryPublish)
	// until the slowest subscriber has released the slot being reused.
	EpochLossless EpochMode = iota
	// EpochLossy never blocks publishers: subscribers that fall more than
	// the ring capacity behind skip ahead and are told how much they lost.
	EpochLossy
)

// EpochStatus is the result class of EpochSub.TryRecv.
type EpochStatus uint8

const (
	EpochEmpty  EpochStatus = iota // nothing pending right now
	EpochValue                     // a value was received
	EpochLagged                    // (lossy) values were skipped; see the lost count
	EpochClosed                    // subscription closed, or broadcaster closed and drained
)

// epochDetached is the cursor sentinel of a closed subscription.
const epochDetached = math.MaxUint64

// epochClosedChan doubles as "the gate is not armed" and "you are ready".
var epochClosedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// epochGate is a lazily-armed, one-shot-per-epoch broadcast latch.
//
//   - arm: returns the live channel for the current epoch, allocating one
//     only if none is live (i.e. only when somebody actually intends to park).
//   - open: closes the live channel, if any, and returns to the unarmed state.
//     Unarmed open() is a single atomic load.
//
// Storing a chan in an atomic.Value does not allocate (chans are
// pointer-shaped), and the sentinel avoids a nil state.
type epochGate struct{ v atomic.Value }

func (g *epochGate) init() { g.v.Store(epochClosedChan) }

func (g *epochGate) arm() <-chan struct{} {
	cur := g.v.Load().(chan struct{})
	if cur != epochClosedChan {
		return cur
	}
	fresh := make(chan struct{})
	for {
		if g.v.CompareAndSwap(epochClosedChan, fresh) {
			return fresh
		}
		if cur = g.v.Load().(chan struct{}); cur != epochClosedChan {
			return cur
		}
	}
}

func (g *epochGate) open() {
	if g.v.Load().(chan struct{}) == epochClosedChan {
		return
	}
	if old := g.v.Swap(epochClosedChan).(chan struct{}); old != epochClosedChan {
		close(old)
	}
}

type epochSlot[V any] struct {
	// tag is seq+1 while the slot holds seq (0 = invalid / being overwritten).
	tag atomic.Uint64
	// refs: lossless = subscribers that have yet to release this slot;
	//       lossy    = readers currently copying the value (Dekker guard).
	refs atomic.Int32
	v    V
}

// EpochCast is a single-ring, multi-subscriber broadcaster whose subscribers
// are selectable without being registered anywhere.
//
// Allocation profile:
//   - Publish / TryPublish / TryRecv / Recv: 0 allocations.
//   - Ready: 0 allocations, except that the first parker of an epoch allocates
//     one channel, shared by every other parker until the next publish.
//   - Subscribe: 1 allocation (80 bytes). Close: 0.
//
// Use NewEpochCast to construct.
type EpochCast[V any] struct {
	_     [64]byte
	head  atomic.Uint64 // next sequence to publish; read by every consumer
	_     [64]byte
	wake  epochGate // opened on publish / close / cancel
	space epochGate // opened when a slot is fully released (lossless)

	nsubs  atomic.Int32
	closed atomic.Bool
	lossy  bool

	mask, capacity uint64
	ring           []epochSlot[V]

	mu  sync.Mutex    // orders subscribe/detach/close against commit
	pub chan struct{} // lossless publisher semaphore (ctx-aware, FIFO-ish)
}

// NewEpochCast creates a broadcaster; capacity is rounded up to a power of two.
func NewEpochCast[V any](capacity int, mode EpochMode) *EpochCast[V] {
	if capacity < 1 || capacity > 1<<30 {
		panic(`bigbuff: epochcast: capacity out of bounds`)
	}
	n := uint64(1) << bits.Len64(uint64(capacity-1))
	x := &EpochCast[V]{
		lossy:    mode == EpochLossy,
		mask:     n - 1,
		capacity: n,
		ring:     make([]epochSlot[V], n),
		pub:      make(chan struct{}, 1),
	}
	x.wake.init()
	x.space.init()
	return x
}

// Subscribers returns the current number of live subscriptions.
func (x *EpochCast[V]) Subscribers() int { return int(x.nsubs.Load()) }

// Subscribe creates a subscription that observes every value published after
// this call. It must eventually be closed (or its slots held forever, in
// lossless mode).
func (x *EpochCast[V]) Subscribe() *EpochSub[V] {
	s := &EpochSub[V]{x: x}
	x.mu.Lock()
	if x.closed.Load() {
		s.cursor.Store(epochDetached)
	} else {
		s.cursor.Store(x.head.Load())
		x.nsubs.Add(1)
	}
	x.mu.Unlock()
	return s
}

// Close stops all publishing and wakes everyone. Subscribers drain whatever
// is pending and then observe EpochClosed.
func (x *EpochCast[V]) Close() {
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
func (x *EpochCast[V]) Publish(ctx context.Context, v V) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return x.publish(ctx, v, true)
}

// TryPublish is a non-blocking Publish; lossless mode may return ErrEpochFull.
func (x *EpochCast[V]) TryPublish(v V) (int, error) {
	return x.publish(nil, v, false)
}

func (x *EpochCast[V]) publish(ctx context.Context, v V, block bool) (int, error) {
	if x.closed.Load() {
		return 0, ErrEpochClosed
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
			return 0, ErrEpochFull
		}
	}
	defer func() { <-x.pub }()

	// We are the only publisher: head only moves under this semaphore.
	for {
		if x.closed.Load() {
			return 0, ErrEpochClosed
		}
		s := x.head.Load()
		sl := &x.ring[s&x.mask]
		if sl.refs.Load() != 0 { // previous occupant not fully consumed
			if !block {
				return 0, ErrEpochFull
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
			return 0, ErrEpochClosed
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

func (x *EpochCast[V]) publishLossy(v V) (int, error) {
	x.mu.Lock()
	if x.closed.Load() {
		x.mu.Unlock()
		return 0, ErrEpochClosed
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
func (x *EpochCast[V]) Flush(ctx context.Context) error {
	if x.lossy {
		return ErrEpochLossy
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
func (x *EpochCast[V]) unref(sl *epochSlot[V]) bool {
	if sl.refs.Load() == 1 { // only we remain: nobody else can be reading
		var zero V
		sl.v = zero
	}
	return sl.refs.Add(-1) == 0
}

func (x *EpochCast[V]) lossyRead(sl *epochSlot[V], c uint64) (v V, ok bool) {
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
func (x *EpochCast[V]) Iter(ctx context.Context) iter.Seq[V] {
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
			case EpochValue:
				if !yield(v) {
					return
				}
				continue
			case EpochLagged:
				continue
			case EpochClosed:
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

// EpochSub is one consumer's cursor. TryRecv/Recv/Ready/Close are all safe
// for concurrent use; multiple goroutines sharing one EpochSub act as
// competing consumers of a single logical subscriber.
type EpochSub[V any] struct {
	_      [64]byte
	cursor atomic.Uint64 // next sequence to read, or epochDetached
	x      *EpochCast[V]
}

// Ready returns a channel suitable for select. It is closed (or already
// closed) whenever TryRecv would not return EpochEmpty. Call it afresh on
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
func (s *EpochSub[V]) Ready() <-chan struct{} {
	if s.ready() {
		return epochClosedChan
	}
	ch := s.x.wake.arm()
	if s.ready() { // re-check AFTER arming: head moves before the gate opens
		return epochClosedChan
	}
	return ch
}

func (s *EpochSub[V]) ready() bool {
	// A detached cursor (MaxUint64) never equals head, so it reads as ready.
	return s.cursor.Load() != s.x.head.Load() || s.x.closed.Load()
}

// Pending returns the number of values currently waiting for this subscriber.
func (s *EpochSub[V]) Pending() uint64 {
	c := s.cursor.Load()
	h := s.x.head.Load()
	if c == epochDetached || c >= h {
		return 0
	}
	if p := h - c; !s.x.lossy || p <= s.x.capacity {
		return p
	}
	return s.x.capacity
}

// TryRecv never blocks. On EpochLagged (lossy only) v is the zero value and
// lost is the number of values skipped; the next call resumes at the oldest
// value still in the ring.
func (s *EpochSub[V]) TryRecv() (v V, lost uint64, st EpochStatus) {
	x := s.x
	for {
		c := s.cursor.Load()
		if c == epochDetached {
			return v, 0, EpochClosed
		}
		h := x.head.Load()
		if c >= h {
			// Re-read head after seeing closed: no publish can follow it.
			if x.closed.Load() && c >= x.head.Load() {
				return v, 0, EpochClosed
			}
			return v, 0, EpochEmpty
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
			return v, 0, EpochValue
		}

		if h-c > x.capacity { // definitely overwritten
			target := h - x.capacity
			if s.cursor.CompareAndSwap(c, target) {
				return v, 0 + target - c, EpochLagged
			}
			continue
		}
		if !s.cursor.CompareAndSwap(c, c+1) {
			continue
		}
		if got, ok := x.lossyRead(sl, c); ok {
			return got, 0, EpochValue
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
		return v, lost, EpochLagged
	}
}

// Recv blocks until a value, lag, closure, or ctx. Lag is reported as an
// EpochLag error (the stream remains usable); closure as ErrEpochClosed.
func (s *EpochSub[V]) Recv(ctx context.Context) (V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		v, lost, st := s.TryRecv()
		switch st {
		case EpochValue:
			return v, nil
		case EpochLagged:
			return v, EpochLag(lost)
		case EpochClosed:
			return v, ErrEpochClosed
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
func (s *EpochSub[V]) Close() { s.detach(false) }

// Cancel is Close plus a wakeup of everything parked on the gate (spurious for
// other subscribers, who simply re-check).
func (s *EpochSub[V]) Cancel() { s.detach(true) }

func (s *EpochSub[V]) detach(wake bool) {
	x := s.x
	x.mu.Lock() // freezes head: no commit can add slots we would miss
	c := s.cursor.Load()
	for c != epochDetached && !s.cursor.CompareAndSwap(c, epochDetached) {
		c = s.cursor.Load() // lost a race with a claiming TryRecv
	}
	if c == epochDetached {
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
```

## `epochcast_test.go`

```go
package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestEpochCastLosslessFanout(t *testing.T) {
	const subs, msgs = 8, 5000
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	x := NewEpochCast[int](16, EpochLossless)
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := x.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Close()
			for want := 0; want < msgs; want++ {
				got, err := s.Recv(ctx)
				if err != nil || got != want {
					t.Errorf("want %d got %d err %v", want, got, err)
					return
				}
			}
		}()
	}
	for i := 0; i < msgs; i++ {
		if n, err := x.Publish(ctx, i); err != nil || n != subs {
			t.Fatalf("publish %d: n=%d err=%v", i, n, err)
		}
	}
	wg.Wait()
}

func TestEpochCastSelect(t *testing.T) {
	x := NewEpochCast[string](4, EpochLossless)
	s := x.Subscribe()
	defer s.Close()
	other := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = x.Publish(context.Background(), "hello")
	}()
	select {
	case <-other:
		t.Fatal("unexpected")
	case <-s.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	if v, _, st := s.TryRecv(); st != EpochValue || v != "hello" {
		t.Fatalf("got %q %v", v, st)
	}
	if _, _, st := s.TryRecv(); st != EpochEmpty {
		t.Fatalf("got %v", st)
	}
}

func TestEpochCastLossyLag(t *testing.T) {
	x := NewEpochCast[int](4, EpochLossy)
	s := x.Subscribe()
	defer s.Close()
	for i := 0; i < 10; i++ {
		if n, err := x.TryPublish(i); n != 1 || err != nil {
			t.Fatal(n, err)
		}
	}
	if _, lost, st := s.TryRecv(); st != EpochLagged || lost != 6 {
		t.Fatalf("lost=%d st=%v", lost, st)
	}
	for want := 6; want < 10; want++ {
		if v, _, st := s.TryRecv(); st != EpochValue || v != want {
			t.Fatalf("want %d got %d %v", want, v, st)
		}
	}
	if _, _, st := s.TryRecv(); st != EpochEmpty {
		t.Fatal(st)
	}
}

func TestEpochCastFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	x := NewEpochCast[int](4, EpochLossless)
	s := x.Subscribe()
	defer s.Close()
	for i := 0; i < 3; i++ {
		if _, err := x.Publish(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- x.Flush(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("flush returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	for i := 0; i < 3; i++ {
		if _, _, st := s.TryRecv(); st != EpochValue {
			t.Fatal(st)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEpochCastZeroAlloc(t *testing.T) {
	ctx := context.Background()
	x := NewEpochCast[int](8, EpochLossless)
	s := x.Subscribe()
	defer s.Close()
	if a := testing.AllocsPerRun(1000, func() {
		_, _ = x.Publish(ctx, 1)
		_, _, _ = s.TryRecv()
	}); a != 0 {
		t.Fatalf("allocs=%v", a)
	}
}

func TestEpochCastChurn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	x := NewEpochCast[int](8, EpochLossless)
	stop := make(chan struct{})
	var pubWG, churnWG sync.WaitGroup
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = x.Publish(ctx, i)
			runtime.Gosched()
		}
	}()
	for g := 0; g < 4; g++ {
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			for i := 0; i < 300; i++ {
				s := x.Subscribe()
				for j := 0; j < 3; j++ {
					if _, err := s.Recv(ctx); err != nil {
						t.Error(err)
						break
					}
				}
				s.Close()
			}
		}()
	}
	churnWG.Wait()
	close(stop)
	pubWG.Wait()
	x.Close()
	if x.Subscribers() != 0 {
		t.Fatalf("subscribers=%d", x.Subscribers())
	}
}
```

## How the invariants hold

- **No lost wakeups.**
  - Publisher: write slot, `head.Store`, then `wake.open()`.
  - Consumer: check pending, `arm()`, check pending again, then park on the armed channel.
  - If the consumer's `arm()` came after the publisher's `open()`, the second check sees the new `head`. If it came before, `open()` closes the consumer's channel.
  - The space gate (publisher and `Flush`) uses the same pattern against `refs`.
- **Lossless slot safety.**
  - A publisher only writes a slot when `refs == 0`.
  - A reader copies the value before releasing its ref, and the atomics provide the happens-before edge, so there is no data race.
  - `Subscribe`, `detach` and the commit step all run under `mu`. A new subscriber's cursor is therefore consistent with the `refs` count of every slot it will read.
- **Lossy slot safety.**
  - The writer stores `tag=0` and then loads `refs`. The reader adds to `refs` and then loads `tag`. Under sequential consistency, either the reader backs off or the writer waits for the copy to finish.
  - A reader that loses the race computes the exact skip count.
- **Churn.**
  - `Subscribe` and `Close` are O(1), plus O(lag ≤ cap) for lossless slot release.
  - A close wakes no one unless you call `Cancel`, so N subscribers churning cannot trigger an O(N²) herd.

## Where it sits relative to the existing designs

| | ChanCaster | ChanPubSub | **EpochCast** |
|---|---|---|---|
| Publish cost | N channel sends | N sends + 2 barriers | O(1) plus one `close()` if anyone is parked |
| Steady allocation | 0 | 0 | 0 (≤1 channel per publish with parked waiters, shared by all) |
| Select support | yes | yes | yes |
| Consumer churn | RW-lock plus unbuffered-channel draining | RW-locks, panics on misuse | O(1), no herd, no panic paths |
| Slow consumer | blocks everyone | blocks everyone | blocks (lossless, O(1) gating) **or** lags with an exact count (lossy) |
| Rendezvous | no | `Send` waits for all | opt-in `Flush` |

Two caveats:

- **Lossy publisher.** A lossy publisher spins with `Gosched` only for the few nanoseconds a reader needs to copy a value out of the slot it is about to overwrite.
- **Value size.** `V` is copied by value under both protocols, so keep `V` small or use pointers.
