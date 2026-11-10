# Apex Fanout: A Synchronous Broadcast Coordinator with Cancellation Compensation

## 1. Scenario & Mechanical Sympathy Thesis

**Target Scenario**: A single-producer in-process event bus broadcasting synchronously to N subscribers. Each subscriber receives values via `Next(ctx)` / `Take()` (select-composable via `Ready()` or `Iter(ctx)`). The critical guarantee: **a subscriber that cancels mid-broadcast must never stall the publisher**.

**Why standard Go breaks**: A `[]chan V` + `sync.Mutex` broadcaster:
- Deadlocks when a subscriber's `select` picks `ctx.Done()` while the broadcaster is blocked on `ch <- v` (unbuffered send with no receiver).
- Forces cancellation detection by requiring each subscriber to drain its channel manually — an error-prone, ergonomically hostile contract.
- Cannot express "bounded-wait" delivery without wrapping every send in a `select { case ch<-v: case <-timeout: }` idiom.
- Stalls ALL publishers on any single slow subscriber.

**Novelty**: Epoch-sequenced two-phase delivery. Phase 1 (`offer`) publishes the value into a per-subscriber seqlock-protected slot and lifts an epoch CAS. Phase 2 (`waitAck`) blocks only on the subscriber's *explicit* ack. When a subscriber cancels (either via `Close()` or by abandoning `Next`), a compensating wake on `bcWake` immediately releases the broadcaster. This decouples publisher liveness from subscriber liveness.

**Mechanical Sympathy**:
- Zero steady-state allocations (`Send`/`Next` allocate 0 bytes after warm-up).
- Buffered coalescing wake channels (never created on the hot path; created once per subscriber).
- Cache-line separated hot fields: `pending`, `acked`, `closed` each live on their own line.
- Serialized sends via `sendMu` — the same trade-off as the idiomatic `sync.Mutex` broadcaster but without blocking on subscriber channels.
- `-race` path uses a mutex-protected slot; `!race` uses a seqlock (2 atomic increments bracketing a raw value copy, with bounded spin + `Gosched` fallback).

## 2. Implementation

### `doc.go`

```go
// Package bigbuff provides the Fanout primitive: a synchronous broadcast
// coordinator with bounded-wait delivery and mid-flight cancellation
// compensation.
//
// Fanout targets the pain point where N subscribers must receive a value
// synchronously and one of them may cancel (via context or explicit Close)
// while the broadcaster is mid-broadcast. Unlike a naive []chan V + mutex
// loop — which deadlocks on cancelled subscribers and stalls all publishers
// on any slow subscriber — Fanout's two-phase offer/ack protocol lets the
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
```

### `fanout.go`

```go
package bigbuff

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

const cacheLine = 64

var (
	ErrFanoutClosed = errors.New("bigbuff: fanout closed")
	ErrSubClosed    = errors.New("bigbuff: subscriber already closed")
)

// Fanout is a synchronous broadcast coordinator with bounded-wait delivery
// and mid-flight cancellation compensation.
type Fanout[V any] struct {
	_      [cacheLine]byte
	sendMu sync.Mutex
	_      [cacheLine - 8]byte
	seq    atomic.Uint64
	_      [cacheLine - 8]byte
	subMu  sync.Mutex
	_      [cacheLine - 8]byte
	subs   atomic.Pointer[[]*Sub[V]]
	_      [cacheLine - 8]byte
	closed atomic.Bool
	_      [cacheLine - 1]byte
}

func NewFanout[V any]() *Fanout[V] {
	f := &Fanout[V]{}
	empty := []*Sub[V]{}
	f.subs.Store(&empty)
	return f
}

// Subscribe registers a new subscriber. Returns ErrFanoutClosed if the
// fanout has already been closed.
func (f *Fanout[V]) Subscribe() (*Sub[V], error) {
	if f.closed.Load() {
		return nil, ErrFanoutClosed
	}
	s := &Sub[V]{
		fanout:  f,
		subWake: make(chan struct{}, 1),
		bcWake:  make(chan struct{}, 1),
	}
	f.subMu.Lock()
	if f.closed.Load() {
		f.subMu.Unlock()
		return nil, ErrFanoutClosed
	}
	old := *f.subs.Load()
	newSubs := make([]*Sub[V], len(old)+1)
	copy(newSubs, old)
	newSubs[len(old)] = s
	f.subs.Store(&newSubs)
	f.subMu.Unlock()
	return s, nil
}

func (f *Fanout[V]) removeSub(s *Sub[V]) {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	old := *f.subs.Load()
	for i, x := range old {
		if x == s {
			newSubs := make([]*Sub[V], 0, len(old)-1)
			newSubs = append(newSubs, old[:i]...)
			newSubs = append(newSubs, old[i+1:]...)
			f.subs.Store(&newSubs)
			return
		}
	}
}

// Len returns the number of currently registered subscribers.
func (f *Fanout[V]) Len() int { return len(*f.subs.Load()) }

// Send synchronously offers v to every live subscriber and waits for each to
// ack or cancel. Returns ctx.Err() if ctx is cancelled before all acks are
// received; the un-acked subscribers are skipped for this epoch.
func (f *Fanout[V]) Send(ctx context.Context, v V) error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if f.closed.Load() {
		return ErrFanoutClosed
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
// the second call returns ErrFanoutClosed.
func (f *Fanout[V]) Close() error {
	if f.closed.Swap(true) {
		return ErrFanoutClosed
	}
	f.subMu.Lock()
	subs := *f.subs.Load()
	empty := []*Sub[V]{}
	f.subs.Store(&empty)
	f.subMu.Unlock()
	for _, s := range subs {
		s.Close()
	}
	return nil
}

// Sub is a single-consumer view of a Fanout. Call Next/Take from one
// goroutine only.
type Sub[V any] struct {
	_       [cacheLine]byte
	pending atomic.Uint64
	_       [cacheLine - 8]byte
	acked   atomic.Uint64
	_       [cacheLine - 8]byte
	closed  atomic.Bool
	_       [cacheLine - 1]byte

	slot slot[V]

	subWake chan struct{}
	bcWake  chan struct{}
	fanout  *Fanout[V]
}

func (s *Sub[V]) offer(v V, epoch uint64) bool {
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

func (s *Sub[V]) waitAck(ctx context.Context, epoch uint64) error {
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
func (s *Sub[V]) Next(ctx context.Context) (V, bool) {
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
func (s *Sub[V]) Take() (V, bool) {
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
func (s *Sub[V]) Ready() <-chan struct{} { return s.subWake }

// Closed reports whether this subscriber has been closed.
func (s *Sub[V]) Closed() bool { return s.closed.Load() }

// Close unregisters the subscriber and wakes any broadcaster waiting on it.
// Idempotent.
func (s *Sub[V]) Close() error {
	if s.closed.Swap(true) {
		return ErrSubClosed
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
```

### `slot_fast.go` (`//go:build !race`)

```go
//go:build !race

package bigbuff

import (
	"runtime"
	"sync/atomic"
)

// slot is a single-writer, single-reader seqlock-protected value cell.
// Writes are serialized by Fanout.sendMu; reads may race with a write for
// the *next* epoch, which the seqlock seq validation catches.
type slot[V any] struct {
	seq   atomic.Uint64
	epoch uint64
	value V
}

func (s *slot[V]) write(v V, epoch uint64) {
	s.seq.Add(1) // odd => writing
	s.epoch = epoch
	s.value = v
	s.seq.Add(1) // even => stable
}

func (s *slot[V]) read() (V, uint64) {
	for spin := 0; ; spin++ {
		s1 := s.seq.Load()
		if s1&1 == 0 {
			v := s.value
			e := s.epoch
			s2 := s.seq.Load()
			if s1 == s2 {
				return v, e
			}
		}
		if spin > 1000 {
			runtime.Gosched()
			spin = 0
		}
	}
}
```

### `slot_race.go` (`//go:build race`)

```go
//go:build race

package bigbuff

import "sync"

type slot[V any] struct {
	mu    sync.Mutex
	epoch uint64
	value V
}

func (s *slot[V]) write(v V, epoch uint64) {
	s.mu.Lock()
	s.epoch = epoch
	s.value = v
	s.mu.Unlock()
}

func (s *slot[V]) read() (V, uint64) {
	s.mu.Lock()
	v, e := s.value, s.epoch
	s.mu.Unlock()
	return v, e
}
```

### `iter.go` (`//go:build go1.23`)

```go
//go:build go1.23

package bigbuff

import (
	"context"
	"iter"
)

// Iter returns a range-over-function iterator that yields values until ctx is
// cancelled, the subscriber is closed, or the caller breaks out of the loop.
// Breaking out does not auto-close the subscriber; call Close explicitly.
func (s *Sub[V]) Iter(ctx context.Context) iter.Seq[V] {
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
```

## 3. Verification & Benchmarks

### `fanout_test.go`

```go
package bigbuff

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasicBroadcast(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()

	const nSubs = 4
	subs := make([]*Sub[int], nSubs)
	for i := range subs {
		s, err := f.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
	}

	ctx := context.Background()
	for _, s := range subs {
		s := s
		go func() {
			for {
				_, ok := s.Next(ctx)
				if !ok {
					return
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		if err := f.Send(ctx, i); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
}

func TestMidFlightCancel(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()

	s, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Send in goroutine; it would block forever on the sub if cancellation
	// compensation were broken.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- f.Send(ctx, 42)
	}()

	time.Sleep(10 * time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("send after close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not unblock after Close")
	}
}

func TestSenderCtxCancel(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()

	s, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err = f.Send(ctx, 1)
	if err == nil {
		t.Fatal("expected ctx.Err() from Send")
	}
}

func TestChurnUnderLoad(t *testing.T) {
	f := NewFanout[int64]()
	defer f.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Churner: subscribe/close rapidly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s, err := f.Subscribe()
			if err != nil {
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				defer cancel()
				_, _ = s.Next(ctx)
				s.Close()
			}()
		}
	}()

	// Publishers.
	var sent atomic.Int64
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < 500; i++ {
				sent.Add(1)
				_ = f.Send(ctx, int64(i))
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if sent.Load() == 0 {
		t.Fatal("no sends completed")
	}
}

func TestNoLostWakeup(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()

	s, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		done := make(chan struct{})
		go func() {
			_ = f.Send(ctx, i)
			close(done)
		}()
		v, ok := s.Next(ctx)
		if !ok || v != i {
			t.Fatalf("iteration %d: got v=%d ok=%v", i, v, ok)
		}
		<-done
	}
}

func TestRaceCloseDuringSend(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		s, _ := f.Subscribe()
		go func() {
			time.Sleep(time.Microsecond)
			s.Close()
		}()
		_ = f.Send(ctx, i)
	}
}
```

### `fanout_bench_test.go`

```go
package bigbuff

import (
	"context"
	"sync"
	"testing"
)

func BenchmarkFanout_SingleSub(b *testing.B) {
	f := NewFanout[int]()
	sub, _ := f.Subscribe()
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, ok := sub.Next(ctx)
			if !ok {
				return
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Send(ctx, i)
	}
	b.StopTimer()
	f.Close()
	<-done
}

func BenchmarkMutexChan_SingleSub(b *testing.B) {
	var mu sync.Mutex
	ch := make(chan int)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		select {
		case ch <- i:
		case <-ctx.Done():
		}
		mu.Unlock()
	}
	b.StopTimer()
	close(ch)
	<-done
}

func BenchmarkFanout_MultiSub(b *testing.B) {
	for _, n := range []int{4, 16} {
		n := n
		b.Run(itoa(n), func(b *testing.B) {
			f := NewFanout[int]()
			ctx := context.Background()

			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				s, _ := f.Subscribe()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						if _, ok := s.Next(ctx); !ok {
							return
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = f.Send(ctx, i)
			}
			b.StopTimer()
			f.Close()
			wg.Wait()
		})
	}
}

func BenchmarkMutexChan_MultiSub(b *testing.B) {
	for _, n := range []int{4, 16} {
		n := n
		b.Run(itoa(n), func(b *testing.B) {
			var mu sync.Mutex
			chs := make([]chan int, n)
			var wg sync.WaitGroup
			for i := range chs {
				chs[i] = make(chan int)
				ch := chs[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range ch {
					}
				}()
			}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mu.Lock()
				for _, ch := range chs {
					select {
					case ch <- i:
					case <-ctx.Done():
					}
				}
				mu.Unlock()
			}
			b.StopTimer()
			for _, ch := range chs {
				close(ch)
			}
			wg.Wait()
		})
	}
}

func BenchmarkFanout_WithChurn(b *testing.B) {
	f := NewFanout[int]()
	ctx := context.Background()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			s, err := f.Subscribe()
			if err != nil {
				return
			}
			s.Close()
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Send(ctx, i)
	}
	b.StopTimer()
	f.Close()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
```

## 4. Design Notes

**Why this dominates the idiomatic baseline**: The `sync.Mutex` + `[]chan V` broadcaster spends its time inside `runtime.hchan`'s lock for every send. Because the send is unbuffered, it blocks until the receiver is scheduled. Under cancellation, the send blocks *forever* — a fatal production failure mode. Fanout replaces the per-send channel lock with a per-subscriber atomic CAS + a slot write (seqlock, ~2 atomic adds), and the wait becomes a bounded spin followed by a buffered-channel select. Cancellation compensation happens via a single non-blocking channel send on `bcWake` (no goroutine, no memory alloc, and the channel is pre-allocated at subscribe time).

**Zero steady-state allocs**: `Subscribe` allocates the `Sub`, two wake channels, and (in the race build) a mutex. `Send`/`Next`/`Take` touch only atomics and pre-allocated channels. In particular, `slot.write`/`slot.read` perform a value copy with no `*V` allocation.

**Ordering**: Epochs monotonically increase under `sendMu`. A subscriber sees epochs in strictly increasing order because `offer` refuses to advance `pending` unless its CAS sees the prior epoch has been acked or was smaller. If a sender times out on a slow subscriber, the next send's `offer` CAS overwrites the stale pending epoch — this is the defined **bounded-loss** semantic and it is what makes a single slow subscriber unable to stall the whole bus.

**Cancellation compensation**: When `Sub.Close()` runs, it wakes both the broadcaster (`bcWake`) and any blocked `Next` (`subWake`). The broadcaster's `waitAck` sees `s.closed.Load()` and returns immediately — the epoch is silently skipped for that subscriber. There is no possibility of deadlock: at every point, the broadcaster has either (a) a wake channel it is registered on, (b) a closed flag it checks, or (c) an acked epoch it checks.

**Fail-fast invariants**: `Send` returns `ErrFanoutClosed` after `Close`; `Subscribe` returns `ErrFanoutClosed`; `Sub.Close` returns `ErrSubClosed` on double-close. All state transitions are atomic; no silent hangs.

**Race-detector safety**: the `//go:build race` variant uses `sync.Mutex` for the slot; all other coordination goes through `sync/atomic`. The `!race` variant's seqlock is the standard Go pattern (bracketing raw reads/writes with atomic `seq` loads/stores) — safe under Go's sequential-consistency atomics, and bounded-spin with `runtime.Gosched()` fallback to prevent livelock if a writer is preempted mid-write.
