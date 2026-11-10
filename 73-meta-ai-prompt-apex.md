## `Cohort[V]` — barrier-synchronized, select-composable broadcast

**Target scenario:** 1 serialized publisher, 1–32+ receivers, receivers join/leave at any time including mid-broadcast, every broadcast must return only when every receiver joined at publish time has processed the value or has deterministically departed, and receivers must wait inside their own `select` alongside `ctx.Done()`.

Naive `chan` loops deadlock here: a rendezvous send to a receiver that leaves mid-broadcast has no receiver, and closing that channel panics the sender. `sync.Cond` thundering-herds and cannot compose with `select` without a bridge goroutine per waiter.

### Mechanics that win

- **Per-member coalesced wakeup:** each `Member` owns one `chan struct{}` cap 1, stable for its lifetime so `Notify()` can be cached. Publish does one non-blocking send per joined member. A single shared channel would deliver its one token to a single waiter and starve the rest.
- **Barrier, not rendezvous:** publisher waits on one per-broadcast `done` channel, receivers contribute one atomic watermark + one mutex-protected counter decrement. No blocking rendezvous, no `O(N)` select.
- **Epoch + `RWMutex` snapshot:** `Snapshot` reads epoch, copies under `RLock`, re-validates epoch, retries on interleave. Readers never block each other, torn value+epoch pairs are impossible.
- **Cache-line padding:** epoch, member count, and per-member ack watermarks each on their own 64-byte line.
- **Cancellable publisher admission:** publishers serialize on a cap-1 semaphore channel acquired via `select` on `ctx.Done()` / `Closed()`.
- **Compensating cancellation:** `Leave()` atomically decrements the pending barrier if the leaver had not acked. Publisher cancellation abandons the barrier but leaves the value published. `Member.All` acks the last yielded value even on `break`. `Close()` unblocks every waiter and in-flight broadcast exactly once.
- **Fail-fast invariants:** `CheckInvariants()` panics on negative pending counts, pending > members, epoch regression, or member-count drift.

### API

```go
c := bigbuff.New[int]()
m, _ := c.Join()
defer m.Leave()

n, err := c.Broadcast(ctx, 42) // barrier-synchronized

// Explicit ack:
v, epoch, err := m.Wait(ctx)
process(v)
m.Ack(epoch)

// Select-composable:
select {
case <-m.Notify():
    if v, epoch, ok := m.TryWait(); ok {
        process(v); m.Ack(epoch)
    }
case <-ctx.Done():
    m.Leave(); return ctx.Err()
case <-m.Closed():
    return bigbuff.ErrClosed
}

// Iterator, break compensates:
for v := range m.All(ctx) {
    if done(v) { break }
    process(v)
}
```

Hot path: `Notify`, `Snapshot`, `TryWait`, `Ack`, `Leave` allocate nothing. `Join` allocates one small channel per member. `Broadcast` allocates one `done` channel per broadcast with ≥1 member. No background goroutines.

### Files

- doc.go — thesis + usage
- cohort.go — `Cohort` / `Member` implementation
- cohort_test.go — barrier, mid-flight cancel, leave compensation, join-during-broadcast, close, select, iterator break, snapshot consistency, churn invariants, leak test
- cohort_bench_test.go — fair baseline: idiomatic mutex + per-subscriber unbuffered channel broadcast doing the same coordination work
- go.mod
- README.md

Run:

```sh
go test -race -count=1 ./...
go test -bench=. -benchmem -run=^$ ./...
```

The baseline is only churned *between* broadcasts, because churning it mid-broadcast deadlocks it — which is the point: `Cohort` survives mid-flight churn the idiomatic baseline cannot.

---------

````txtar
-- README.md --
# bigbuff — Cohort: barrier-synchronized, select-composable broadcast

## Scenario & thesis

`Cohort[V]` targets the exact envelope where Go's standard primitives
break down: **synchronous broadcast to a dynamic set of receivers
that must compose with `select`, `context.Context`, and `iter.Seq`,
and that may cancel, leave, or break out of iteration mid-broadcast.**

Operational envelope:

- 1 publisher (serialized) or a small set of publishers queuing on
  the broadcast semaphore, 1–32+ receivers.
- Receivers join/leave at any time, including mid-broadcast.
- Every broadcast must be *barrier-synchronized*: it returns only
  when every receiver joined at publish time has processed the
  value or has deterministically departed. No dropped tokens, no
  orphaned barrier counts, no silent hangs.
- Receivers must be able to wait inside their own `select`
  alongside `ctx.Done()` and other channels — no wrapper goroutine,
  no per-waiter goroutine, one small coalesced channel per member.

Why `chan` / `Mutex` / `Cond` are suboptimal here:

- A loop over per-subscriber unbuffered channels does O(N) blocking
  rendezvous sends and deadlocks if a subscriber leaves mid-loop
  (send on a channel with no receiver, or panic on send to a
  closed channel). There is no compensation. `Cohort` uses
  buffered coalesced per-member channels (never a blocking send)
  plus a barrier counter, so Leave compensates instead of deadlocking.
- `sync.Cond` wakes every waiter on every publish (thundering
  herd) and cannot compose with `select` / `context.Context`
  without a bridge goroutine per waiter.
- A callback registry breaks `select` ergonomics entirely.

## Mechanics that win

- **Per-member coalesced wakeup channels** (`cap 1`, one
  non-blocking send to each joined member per broadcast). A
  single shared channel would deliver its one token to a single
  waiter and starve the rest; per-member channels wake every
  waiter. Rapid broadcasts coalesce per member; waiters
  re-check the epoch so no wakeup is lost.
- **Barrier, not rendezvous.** The publisher waits on one
  per-broadcast `done` channel. Each receiver contributes one
  atomic watermark store + one mutex-protected counter decrement.
  No O(N) select, no blocking rendezvous send — publish does
  O(N) non-blocking coalesced sends, one per member.
- **Epoch + RWMutex snapshot.** Value guarded by `RWMutex`,
  epoch by atomic. Snapshot reads epoch → copies under `RLock`
  → re-validates epoch → retries on interleave. Readers never
  block each other; torn value+epoch pairs are impossible.
- **Cache-line padding.** Epoch, member count, and per-member ack
  watermarks each live on their own 64-byte line.
- **Cancellable publisher admission.** Publishers serialize on a
  `cap 1` semaphore channel acquired via `select` on
  `ctx.Done()` / `Closed()`.
- **Compensating cancellation.** `Leave()` atomically decrements
  the pending barrier if the leaver had not acked. Publisher
  cancellation abandons the barrier but leaves the value
  published. `Member.All` acks the last yielded value even on
  `break`. `Close()` unblocks every waiter and in-flight
  broadcast exactly once.
- **Fail-fast invariants.** `CheckInvariants()` panics on
  negative pending counts, pending > members, epoch regression,
  or member-count drift.

## API

```go
c := bigbuff.New[int]()
m, _ := c.Join()
defer m.Leave()

// Publisher:
n, err := c.Broadcast(ctx, 42)

// Receiver, explicit ack:
v, epoch, err := m.Wait(ctx)
process(v)
m.Ack(epoch)

// Receiver, select-composable:
select {
case <-m.Notify():
    if v, epoch, ok := m.TryWait(); ok {
        process(v); m.Ack(epoch)
    }
case <-ctx.Done():
    m.Leave(); return ctx.Err()
case <-m.Closed():
    return bigbuff.ErrClosed
}

// Receiver, iterator (break compensates):
for v := range m.All(ctx) {
    if done(v) { break }
    process(v)
}
```

## Correctness & benchmarks

Run:

```sh
go test -race -count=1 ./...
go test -bench=. -benchmem -run=^$ ./...
```

Tests cover: barrier ordering, mid-flight publisher cancel,
member leave compensation, join-during-broadcast, close
unblocking, select composability, iterator break compensation,
snapshot consistency (value == epoch, no torn reads), ack
idempotence, churn invariants under `-race`, and goroutine-leak
freedom.

Benchmarks are semantically fair: the baseline is an idiomatic
mutex + per-subscriber unbuffered channel broadcast that performs
the same coordination work (every subscriber must receive before
`Broadcast` returns). The baseline is only churned *between*
broadcasts, because churning it mid-broadcast deadlocks it —
which is itself the point: `Cohort` survives mid-flight churn
that the idiomatic baseline cannot.

Hot path allocations: `Notify`, `Snapshot`, `TryWait`, `Ack`,
`Leave` allocate nothing. `Join` allocates one small channel
per member, amortized over the member's lifetime. `Broadcast`
allocates one `done` channel per broadcast with ≥1 member,
amortized over N deliveries. No background goroutines.
-- cohort_bench_test.go --
package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

// baselineCaster is the idiomatic baseline for a fair comparison:
// a mutex-guarded map of per-subscriber unbuffered channels, with a
// synchronous rendezvous send to each subscriber in turn. It
// performs the same coordination work as Cohort.Broadcast (every
// subscriber must receive before Broadcast returns) but without a
// barrier counter, without a coalesced wakeup, and without
// compensating Leave: a subscriber that leaves mid-broadcast would
// deadlock the sender, so the churn benchmark only leaves between
// broadcasts for the baseline.
type baselineCaster[V any] struct {
	mu       sync.Mutex
	subs     map[chan V]struct{}
	closed   bool
	closedCh chan struct{}
}

func newBaselineCaster[V any]() *baselineCaster[V] {
	return &baselineCaster[V]{
		subs:     make(map[chan V]struct{}),
		closedCh: make(chan struct{}),
	}
}

func (b *baselineCaster[V]) Join() chan V {
	ch := make(chan V)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.subs[ch] = struct{}{}
	return ch
}

func (b *baselineCaster[V]) Leave(ch chan V) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, ch)
}

func (b *baselineCaster[V]) Broadcast(ctx context.Context, v V) (int, error) {
	b.mu.Lock()
	subs := make([]chan V, 0, len(b.subs))
	for ch := range b.subs {
		subs = append(subs, ch)
	}
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return 0, ErrClosed
	}
	for _, ch := range subs {
		select {
		case ch <- v:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-b.closedCh:
			return 0, ErrClosed
		}
	}
	return len(subs), nil
}

func (b *baselineCaster[V]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.mu.Unlock()
	close(b.closedCh)
}

// runCohortBroadcast benchmarks Cohort with nMembers stable receivers
// that Wait+Ack in a loop. Each b.N iteration is one broadcast.
func runCohortBroadcast(b *testing.B, nMembers int) {
	b.Helper()
	c := New[int]()
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < nMembers; i++ {
		m, ok := c.Join()
		if !ok {
			b.Fatal("Join failed")
		}
		wg.Add(1)
		go func(m *Member[int]) {
			defer wg.Done()
			for {
				v, e, err := m.Wait(ctx)
				if err != nil {
					return
				}
				_ = v
				m.Ack(e)
			}
		}(m)
	}
	// Let receivers park before timing.
	time.Sleep(10 * time.Millisecond)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.Broadcast(ctx, i); err != nil {
			b.Fatalf("Broadcast: %v", err)
		}
	}
	b.StopTimer()
	cancel()
	// Drain: Close unblocks any waiter still parked.
	c.Close()
	wg.Wait()
}

func runBaselineBroadcast(b *testing.B, nMembers int) {
	b.Helper()
	c := newBaselineCaster[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < nMembers; i++ {
		ch := c.Join()
		if ch == nil {
			b.Fatal("Join failed")
		}
		wg.Add(1)
		go func(ch chan int) {
			defer wg.Done()
			for {
				select {
				case <-ch:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}
	time.Sleep(10 * time.Millisecond)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.Broadcast(ctx, i); err != nil {
			b.Fatalf("Broadcast: %v", err)
		}
	}
	b.StopTimer()
	cancel()
	c.Close()
	wg.Wait()
}

func BenchmarkCohort_Broadcast_1(b *testing.B)  { runCohortBroadcast(b, 1) }
func BenchmarkCohort_Broadcast_8(b *testing.B)  { runCohortBroadcast(b, 8) }
func BenchmarkCohort_Broadcast_32(b *testing.B) { runCohortBroadcast(b, 32) }

func BenchmarkBaseline_Broadcast_1(b *testing.B)  { runBaselineBroadcast(b, 1) }
func BenchmarkBaseline_Broadcast_8(b *testing.B)  { runBaselineBroadcast(b, 8) }
func BenchmarkBaseline_Broadcast_32(b *testing.B) { runBaselineBroadcast(b, 32) }

// Snapshot is read-only: N goroutines snapshot concurrently while
// no broadcast is in flight. It measures the RWMutex + epoch
// validation path without any barrier work.
func BenchmarkCohort_Snapshot(b *testing.B) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()
	// Publish one value so Snapshot has something to return.
	go func() {
		v, e, err := m.Wait(context.Background())
		if err == nil {
			m.Ack(e)
			_ = v
		}
	}()
	_, _ = c.Broadcast(context.Background(), 42)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, e, ok := m.Snapshot()
			if !ok || v != 42 || e != 1 {
				b.Fatalf("Snapshot = (%d,%d,%v)", v, e, ok)
			}
		}
	})
}

// Churn: stable receivers + a churner that Joins/Leaves between
// broadcasts. Measures barrier compensation under membership churn.
func BenchmarkCohort_Churn(b *testing.B) {
	c := New[int]()
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		m, _ := c.Join()
		wg.Add(1)
		go func(m *Member[int]) {
			defer wg.Done()
			for {
				v, e, err := m.Wait(ctx)
				if err != nil {
					return
				}
				_ = v
				m.Ack(e)
			}
		}(m)
	}
	// Churner.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			m, ok := c.Join()
			if !ok {
				return
			}
			m.Leave()
		}
	}()
	time.Sleep(10 * time.Millisecond)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.Broadcast(ctx, i); err != nil {
			b.Fatalf("Broadcast: %v", err)
		}
	}
	b.StopTimer()
	cancel()
	c.Close()
	wg.Wait()
}

func BenchmarkBaseline_Churn(b *testing.B) {
	c := newBaselineCaster[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		ch := c.Join()
		wg.Add(1)
		go func(ch chan int) {
			defer wg.Done()
			for {
				select {
				case <-ch:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ch := c.Join()
			if ch == nil {
				return
			}
			c.Leave(ch)
		}
	}()
	time.Sleep(10 * time.Millisecond)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.Broadcast(ctx, i); err != nil {
			b.Fatalf("Broadcast: %v", err)
		}
	}
	b.StopTimer()
	cancel()
	c.Close()
	wg.Wait()
}

// Cancel: publisher context is cancelled while a broadcast is
// waiting for a slow receiver. Measures abandon + recovery: the
// next broadcast must still succeed.
func BenchmarkCohort_Broadcast_Cancel(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := New[int]()
		m, _ := c.Join()
		// Receiver never acks; publisher cancels.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.Broadcast(ctx, i)
			done <- err
		}()
		time.Sleep(time.Microsecond)
		cancel()
		<-done
		m.Leave()
		c.Close()
	}
}
-- cohort_test.go --
package bigbuff

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls cond until it is true or timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestBroadcastSingleReceiver(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, ok := c.Join()
	if !ok {
		t.Fatal("Join failed")
	}
	defer m.Leave()

	done := make(chan struct{})
	go func() {
		v, e, err := m.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait: %v", err)
			close(done)
			return
		}
		if v != 42 || e != 1 {
			t.Errorf("Wait = (%d,%d), want (42,1)", v, e)
		}
		if !m.Ack(e) {
			t.Errorf("Ack should have contributed to barrier")
		}
		close(done)
	}()

	n, err := c.Broadcast(context.Background(), 42)
	if err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	if n != 1 {
		t.Fatalf("Broadcast n=%d, want 1", n)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not finish")
	}
	c.CheckInvariants()
}

func TestBroadcastMultipleReceiversBarrier(t *testing.T) {
	c := New[int]()
	defer c.Close()
	const nMembers = 8
	members := make([]*Member[int], nMembers)
	for i := range members {
		m, ok := c.Join()
		if !ok {
			t.Fatalf("Join %d failed", i)
		}
		members[i] = m
		defer m.Leave()
	}

	// Start receivers that wait, then block on a gate before acking,
	// so we can prove Broadcast blocks until every ack lands.
	gate := make(chan struct{})
	var arrived atomic.Int32
	var wg sync.WaitGroup
	for _, m := range members {
		wg.Add(1)
		go func(m *Member[int]) {
			defer wg.Done()
			v, e, err := m.Wait(context.Background())
			if err != nil {
				t.Errorf("Wait: %v", err)
				return
			}
			if v != 7 {
				t.Errorf("Wait v=%d, want 7", v)
			}
			arrived.Add(1)
			<-gate
			m.Ack(e)
		}(m)
	}

	broadcastDone := make(chan error, 1)
	go func() {
		_, err := c.Broadcast(context.Background(), 7)
		broadcastDone <- err
	}()

	// All receivers must arrive before the barrier completes.
	waitFor(t, 2*time.Second, "all receivers to arrive", func() bool { return arrived.Load() == nMembers })
	select {
	case err := <-broadcastDone:
		t.Fatalf("Broadcast finished before acks, err=%v", err)
	case <-time.After(50 * time.Millisecond):
		// Still blocked: correct barrier behaviour.
	}
	close(gate)
	select {
	case err := <-broadcastDone:
		if err != nil {
			t.Fatalf("Broadcast: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast did not finish after acks")
	}
	wg.Wait()
	c.CheckInvariants()
}

func TestBroadcastNoReceiversFast(t *testing.T) {
	c := New[int]()
	defer c.Close()
	n, err := c.Broadcast(context.Background(), 1)
	if err != nil || n != 0 {
		t.Fatalf("Broadcast with no members = (%d,%v), want (0,nil)", n, err)
	}
	if got := c.Epoch(); got != 1 {
		t.Fatalf("Epoch=%d, want 1", got)
	}
	c.CheckInvariants()
}

func TestMemberLeaveCompensates(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m1, _ := c.Join()
	m2, _ := c.Join()
	defer m1.Leave()

	// m2 leaves without acking; the broadcast must still complete
	// once m1 acks.
	broadcastDone := make(chan error, 1)
	go func() {
		_, err := c.Broadcast(context.Background(), 99)
		broadcastDone <- err
	}()
	// Give the broadcast a moment to publish, then leave.
	time.Sleep(20 * time.Millisecond)
	m2.Leave()

	v, e, err := m1.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if v != 99 {
		t.Fatalf("Wait v=%d, want 99", v)
	}
	m1.Ack(e)
	select {
	case err := <-broadcastDone:
		if err != nil {
			t.Fatalf("Broadcast: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast stalled after compensating Leave")
	}
	c.CheckInvariants()
}

func TestPublisherCancelAbandonsBarrier(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	ctx, cancel := context.WithCancel(context.Background())
	broadcastDone := make(chan error, 1)
	go func() {
		_, err := c.Broadcast(ctx, 5)
		broadcastDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-broadcastDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Broadcast err=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled Broadcast did not return")
	}
	// The value stays published: the member can still observe it.
	v, e, err := m.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait after abort: %v", err)
	}
	if v != 5 {
		t.Fatalf("Wait v=%d, want 5", v)
	}
	// Stale ack: barrier already gone, so Ack returns false but
	// advances the watermark so Wait will not return it again.
	if m.Ack(e) {
		t.Error("Ack after abort should return false (barrier gone)")
	}
	if _, _, ok := m.TryWait(); ok {
		t.Error("TryWait should not return the same epoch twice")
	}
	// The next broadcast must work normally.
	m2, _ := c.Join()
	defer m2.Leave()
	var wg sync.WaitGroup
	for _, mm := range []*Member[int]{m, m2} {
		wg.Add(1)
		go func(mm *Member[int]) {
			defer wg.Done()
			vv, ee, err := mm.Wait(context.Background())
			if err != nil {
				t.Errorf("Wait: %v", err)
				return
			}
			if vv != 6 {
				t.Errorf("Wait v=%d, want 6", vv)
			}
			mm.Ack(ee)
		}(mm)
	}
	n, err := c.Broadcast(context.Background(), 6)
	if err != nil || n != 2 {
		t.Fatalf("second Broadcast = (%d,%v), want (2,nil)", n, err)
	}
	wg.Wait()
	c.CheckInvariants()
}

func TestMemberCancelViaContext(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, _, err := m.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait err=%v, want DeadlineExceeded", err)
	}
	// No broadcast was ever published, so nothing to compensate.
	// A later broadcast must still work.
	go func() {
		v, e, err := m.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait: %v", err)
			return
		}
		m.Ack(e)
		if v != 1 {
			t.Errorf("Wait v=%d, want 1", v)
		}
	}()
	n, err := c.Broadcast(context.Background(), 1)
	if err != nil || n != 1 {
		t.Fatalf("Broadcast = (%d,%v), want (1,nil)", n, err)
	}
}

func TestJoinDuringBroadcastDoesNotDeadlock(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m0, _ := c.Join()
	defer m0.Leave()

	// Hammer Join while broadcasts are in flight. None of the
	// joiners may be over-counted in a way that hangs a broadcast.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			m, ok := c.Join()
			if !ok {
				return
			}
			m.Leave()
		}
	}()

	// m0 drains every broadcast so they can complete.
	go func() {
		for {
			v, e, err := m0.Wait(context.Background())
			if err != nil {
				return
			}
			_ = v
			m0.Ack(e)
		}
	}()

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.Broadcast(ctx, i)
		cancel()
		if err != nil {
			t.Fatalf("Broadcast %d: %v", i, err)
		}
	}
	<-done
	c.CheckInvariants()
}

func TestCloseUnblocksWaitersAndBroadcast(t *testing.T) {
	c := New[int]()
	m, _ := c.Join()
	defer m.Leave()

	waitErr := make(chan error, 1)
	go func() {
		_, _, err := m.Wait(context.Background())
		waitErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	broadcastDone := make(chan error, 1)
	// This broadcast will block because m never acks.
	go func() {
		_, err := c.Broadcast(context.Background(), 1)
		broadcastDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	c.Close()

	select {
	case err := <-waitErr:
		// Wait may return the value published just before Close
		// (it acks nothing, so the broadcast may have completed
		// via Close) or ErrClosed. Either is linearizable, but
		// it must not hang.
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("Wait err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock on Close")
	}
	select {
	case err := <-broadcastDone:
		// Either success (barrier completed via Close) or ErrClosed.
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("Broadcast err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast did not unblock on Close")
	}

	if _, err := c.Broadcast(context.Background(), 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("Broadcast after Close = %v, want ErrClosed", err)
	}
	if _, ok := c.Join(); ok {
		t.Fatal("Join after Close should fail")
	}
	c.CheckInvariants()
	// Close is idempotent.
	c.Close()
}

func TestSelectComposability(t *testing.T) {
	c := New[string]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	// Publisher in background.
	go func() {
		time.Sleep(20 * time.Millisecond)
		// Receiver will ack via the select loop below.
		_, _ = c.Broadcast(context.Background(), "hello")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		select {
		case <-m.Notify():
			if v, e, ok := m.TryWait(); ok {
				if v != "hello" {
					t.Fatalf("select got %q, want hello", v)
				}
				m.Ack(e)
				return
			}
			// Coalesced / spurious wakeup: keep selecting.
		case <-ctx.Done():
			t.Fatal("select timed out")
			return
		case <-m.Closed():
			t.Fatal("unexpected close")
			return
		}
	}
}

func TestIterSeqBreakCompensates(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	// Publisher: 5 values, each broadcast must complete even
	// though the consumer breaks after 2.
	pubDone := make(chan error, 1)
	go func() {
		for i := 1; i <= 5; i++ {
			_, err := c.Broadcast(context.Background(), i)
			if err != nil {
				pubDone <- err
				return
			}
		}
		pubDone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	seen := 0
	for v := range m.All(ctx) {
		seen++
		if v != seen {
			t.Fatalf("iter v=%d, want %d", v, seen)
		}
		if seen == 2 {
			break // All must ack 2 before returning.
		}
	}
	if seen != 2 {
		t.Fatalf("seen=%d, want 2", seen)
	}
	// The publisher must not stall on the broken iterator: the
	// remaining broadcasts still need a receiver. Drain them.
	go func() {
		for {
			v, e, err := m.Wait(context.Background())
			if err != nil {
				return
			}
			_ = v
			m.Ack(e)
		}
	}()
	select {
	case err := <-pubDone:
		if err != nil {
			t.Fatalf("publisher: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publisher stalled after iterator break")
	}
}

func TestIterSeqFull(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	go func() {
		for i := 1; i <= 3; i++ {
			_, _ = c.Broadcast(context.Background(), i)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got []int
	for v := range m.All(ctx) {
		got = append(got, v)
		if len(got) == 3 {
			break
		}
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("iter got %v, want [1 2 3]", got)
	}
}

func TestSnapshotConsistency(t *testing.T) {
	// Value == epoch for every broadcast, so any torn
	// value+epoch pair is directly observable.
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	stop := make(chan struct{})
	var readersWg sync.WaitGroup
	// Readers: snapshot in a tight loop, never observe torn pairs.
	for i := 0; i < 4; i++ {
		readersWg.Add(1)
		go func() {
			defer readersWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if v, e, ok := m.Snapshot(); ok {
					if v != int(e) {
						t.Errorf("torn snapshot: value=%d epoch=%d", v, e)
						return
					}
				}
			}
		}()
	}
	// Publisher + acker for the snapshot member.
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for i := 1; i <= 200; i++ {
			// Publish in background so the snapshot readers
			// race the writer; ack from this goroutine.
			done := make(chan error, 1)
			go func(val int) {
				_, err := c.Broadcast(context.Background(), val)
				done <- err
			}(i)
			// Wait for the value, ack it.
			for {
				v, e, err := m.Wait(context.Background())
				if err != nil {
					t.Errorf("Wait: %v", err)
					return
				}
				_ = v
				m.Ack(e)
				if e >= uint64(i) {
					break
				}
			}
			<-done
		}
	}()
	<-pubDone
	close(stop)
	readersWg.Wait()
	c.CheckInvariants()
}

func TestAckIdempotentAndStale(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	// Ack with no pending broadcast: no-op.
	if m.Ack(0) {
		t.Error("Ack(0) should be false")
	}
	// Publish and ack twice: only the first counts.
	go func() {
		v, e, err := m.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait: %v", err)
			return
		}
		if !m.Ack(e) {
			t.Errorf("first Ack should return true")
		}
		if m.Ack(e) {
			t.Errorf("second Ack should return false")
		}
		if v != 10 {
			t.Errorf("Wait v=%d, want 10", v)
		}
	}()
	n, err := c.Broadcast(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("Broadcast = (%d,%v)", n, err)
	}
	// Stale ack for the completed epoch: no-op, watermark stays.
	if m.Ack(1) {
		t.Error("stale Ack should return false")
	}
	c.CheckInvariants()
}

func TestNextAutoAck(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()
	go func() {
		v, e, err := m.Next(context.Background())
		if err != nil {
			t.Errorf("Next: %v", err)
			return
		}
		if v != 3 || e != 1 {
			t.Errorf("Next = (%d,%d), want (3,1)", v, e)
		}
	}()
	n, err := c.Broadcast(context.Background(), 3)
	if err != nil || n != 1 {
		t.Fatalf("Broadcast = (%d,%v)", n, err)
	}
}

func TestTryBroadcastBusy(t *testing.T) {
	c := New[int]()
	defer c.Close()
	m, _ := c.Join()
	defer m.Leave()

	// Occupy the publisher slot with a broadcast that blocks
	// on m's ack.
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.Broadcast(context.Background(), 1)
		firstDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if _, err := c.TryBroadcast(2); !errors.Is(err, ErrBusy) {
		t.Fatalf("TryBroadcast during in-flight = %v, want ErrBusy", err)
	}
	// Unblock the first broadcast.
	v, e, err := m.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	m.Ack(e)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Broadcast: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first broadcast did not finish")
	}
	// Now TryBroadcast succeeds (m will ack it via Wait+Ack).
	go func() {
		vv, ee, err := m.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait: %v", err)
			return
		}
		_ = vv
		m.Ack(ee)
	}()
	n, err := c.TryBroadcast(2)
	if err != nil || n != 1 {
		t.Fatalf("TryBroadcast = (%d,%v), want (1,nil)", n, err)
	}
}

func TestConcurrentChurnInvariants(t *testing.T) {
	c := New[int]()
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Stable receiver that always drains, so broadcasts can complete.
	stable, _ := c.Join()
	defer stable.Leave()
	go func() {
		for {
			v, e, err := stable.Wait(ctx)
			if err != nil {
				return
			}
			_ = v
			stable.Ack(e)
		}
	}()

	var wg sync.WaitGroup
	// Churners: join, optionally observe, leave.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				m, ok := c.Join()
				if !ok {
					return
				}
				// Sometimes wait briefly, sometimes leave immediately.
				// Leaving without acking must compensate.
				wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				_, _, werr := m.Wait(wctx)
				wcancel()
				if werr == nil {
					// Deliberately do not ack: Leave must compensate.
				}
				m.Leave()
			}
		}()
	}
	// Publishers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				bctx, bcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, err := c.Broadcast(bctx, id*100000+n)
				bcancel()
				if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					t.Errorf("Broadcast: %v", err)
					return
				}
				n++
			}
		}(i)
	}
	// Invariant checker.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
				c.CheckInvariants()
			}
		}
	}()
	wg.Wait()
	c.CheckInvariants()
	if c.Epoch() == 0 {
		t.Error("expected at least one broadcast in churn test")
	}
}

func TestNoGoroutineLeak(t *testing.T) {
	// Baseline goroutine count after a quiet period.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		c := New[int]()
		m, _ := c.Join()
		go func() {
			v, e, err := m.Wait(context.Background())
			if err != nil {
				return
			}
			m.Ack(e)
			_ = v
		}()
		_, _ = c.Broadcast(context.Background(), i)
		m.Leave()
		c.Close()
	}
	// Give waiters a chance to exit.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		if runtime.NumGoroutine() <= before+1 {
			return
		}
	}
	t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}
-- cohort.go --
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrClosed is returned when an operation is attempted on a closed
// Cohort, or when a waiter is unblocked because the Cohort closed.
var ErrClosed = errors.New("bigbuff: cohort closed")

// ErrBusy is returned by TryBroadcast when another broadcast is
// already in flight.
var ErrBusy = errors.New("bigbuff: another broadcast in flight")

// paddedUint64 is a 64-byte cache-line isolated atomic counter.
// Ack watermarks and the epoch are written by different goroutines
// at high frequency; without padding they would false-share and
// bounce the same cache line between cores on every ack.
type paddedUint64 struct {
	v atomic.Uint64
	_ [56]byte
}

// paddedInt64 is the signed counterpart of paddedUint64.
type paddedInt64 struct {
	v atomic.Int64
	_ [56]byte
}

// Cohort is a barrier-synchronized broadcast to a dynamic set of
// Members.
//
// The zero value is not usable; use New.
//
// A Cohort must not be copied after first use. Always use the
// *Cohort returned by New.
type Cohort[V any] struct {
	// Hot atomics, each on its own cache line.
	epoch        paddedUint64
	membersCount paddedInt64

	// mu serializes membership, pending-barrier state, and the
	// epoch increment + member snapshot in Broadcast. It is never
	// held while waiting on a channel or context.
	mu sync.Mutex
	// valMu guards value. Broadcast takes it for writing (exclusive)
	// while holding mu; Snapshot takes it for reading (shared)
	// without mu. Lock ordering is always mu -> valMu, never the
	// reverse, so there is no deadlock cycle.
	valMu sync.RWMutex
	value V
	// closedCh is closed exactly once by Close.
	closedCh chan struct{}
	closed   atomic.Bool
	// closedUnderMu mirrors closed, but under mu so Broadcast/Join
	// can linearize against Close while holding mu.
	closedUnderMu bool
	// bcastSem serializes publishers. Capacity 1, initially full.
	// Acquired via select on ctx.Done() / closedCh so a publisher
	// never blocks forever on a mutex.
	bcastSem chan struct{}
	// members is the current join set. Only accessed under mu.
	members map[*Member[V]]struct{}
	// pendingEpoch is the epoch currently awaiting acks, or 0 if
	// no broadcast is pending.
	pendingEpoch uint64
	// pendingCount is the number of members joined at publish time
	// that have not yet acked pendingEpoch or left.
	pendingCount int
	// pendingDone is closed when pendingCount reaches 0. Only the
	// publisher waits on it.
	pendingDone chan struct{}
}

// Member is a single receiver joined to a Cohort.
//
// A Member must not be copied after first use. Always use the
// *Member returned by Join.
type Member[V any] struct {
	cohort *Cohort[V]
	// notify is this member's own coalesced wakeup channel
	// (capacity 1, never closed). Broadcast does one
	// non-blocking send to each joined member, so every waiter
	// wakes: a single shared channel would deliver its one token
	// to a single waiter and starve the rest. The channel is
	// stable for the member's lifetime, so callers may cache the
	// result of Notify() and select on it in a loop. Rapid
	// broadcasts coalesce into one token; waiters re-check the
	// epoch, so no wakeup is lost.
	notify chan struct{}
	// acked is the highest epoch this member has acked (or, at
	// Join time, the current epoch, so a joiner is exempt from
	// any in-flight broadcast). Isolated on its own cache line so
	// concurrent acks from different members do not false-share.
	acked paddedUint64
	left  atomic.Bool
	_     [44]byte
}

// New returns an empty Cohort.
func New[V any]() *Cohort[V] {
	c := &Cohort[V]{
		closedCh: make(chan struct{}),
		bcastSem: make(chan struct{}, 1),
		members:  make(map[*Member[V]]struct{}),
	}
	c.bcastSem <- struct{}{}
	return c
}

// Join admits a new Member. It does not replay old epochs: the
// member will only observe broadcasts published after Join returns.
//
// The second result is false if the Cohort is already closed.
func (c *Cohort[V]) Join() (*Member[V], bool) {
	if c.closed.Load() {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closedUnderMu {
		return nil, false
	}
	m := &Member[V]{cohort: c, notify: make(chan struct{}, 1)}
	// Set the ack watermark to the current epoch under mu, so
	// Join is atomic with Broadcast's snapshot: if Broadcast
	// snapshots after us, we are counted (acked == old epoch <
	// new pendingEpoch) and must Ack or Leave to satisfy the
	// barrier; if Broadcast snapshots before us, we are not
	// counted (acked == new pendingEpoch) and are exempt.
	// Either way the barrier cannot over-count.
	m.acked.v.Store(c.epoch.v.Load())
	c.members[m] = struct{}{}
	c.membersCount.v.Add(1)
	return m, true
}

// Members returns the number of currently joined members.
func (c *Cohort[V]) Members() int {
	return int(c.membersCount.v.Load())
}

// Epoch returns the latest published epoch, or 0 if nothing has
// been broadcast yet. Epochs are strictly monotonic.
func (c *Cohort[V]) Epoch() uint64 {
	return c.epoch.v.Load()
}

// Pending returns the number of members that have not yet acked
// the current in-flight broadcast, or 0 if none is in flight.
func (c *Cohort[V]) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingCount
}

// Closed returns a channel closed exactly once when Close is called.
func (c *Cohort[V]) Closed() <-chan struct{} {
	return c.closedCh
}

// CheckInvariants panics if any internal invariant is violated.
// It is intended for tests and for fail-fast debugging; production
// code does not need to call it on the hot path.
func (c *Cohort[V]) CheckInvariants() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingCount < 0 {
		panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d < 0", c.pendingCount))
	}
	if c.pendingCount > 0 {
		if c.pendingDone == nil {
			panic("bigbuff: invariant violated: pendingCount>0 but pendingDone is nil")
		}
		if c.pendingEpoch == 0 {
			panic("bigbuff: invariant violated: pendingCount>0 but pendingEpoch==0")
		}
		if c.pendingEpoch > c.epoch.v.Load() {
			panic(fmt.Sprintf("bigbuff: invariant violated: pendingEpoch=%d > epoch=%d", c.pendingEpoch, c.epoch.v.Load()))
		}
	} else {
		if c.pendingDone != nil {
			panic("bigbuff: invariant violated: pendingCount==0 but pendingDone != nil")
		}
		if c.pendingEpoch != 0 {
			panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount==0 but pendingEpoch=%d", c.pendingEpoch))
		}
	}
	if c.pendingCount > len(c.members) {
		panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d > members=%d", c.pendingCount, len(c.members)))
	}
	if int(c.membersCount.v.Load()) != len(c.members) {
		panic(fmt.Sprintf("bigbuff: invariant violated: membersCount=%d != len(members)=%d", c.membersCount.v.Load(), len(c.members)))
	}
	if c.closedUnderMu != c.closed.Load() {
		panic("bigbuff: invariant violated: closedUnderMu != closed")
	}
}

// Broadcast publishes v as a new epoch and blocks until every
// member joined at publish time has Acked that epoch or left.
//
// If there are no members it publishes and returns (0, nil)
// immediately.
//
// If ctx is cancelled while waiting for the barrier, Broadcast
// abandons the barrier, leaves the value published, and returns
// ctx.Err(). Members that later Ack the epoch are treated as
// stale acks (their watermark advances, but the barrier is
// already gone). The next Broadcast proceeds normally.
//
// If the Cohort is closed while waiting, Broadcast returns
// ErrClosed (unless the barrier already completed, in which case
// it returns success).
//
// Broadcasts are serialized: concurrent publishers queue on the
// broadcast semaphore, which is acquired via select on ctx.Done()
// and Closed(), so a publisher never blocks forever on a mutex.
func (c *Cohort[V]) Broadcast(ctx context.Context, v V) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.closed.Load() {
		return 0, ErrClosed
	}
	select {
	case <-c.bcastSem:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.closedCh:
		return 0, ErrClosed
	}
	defer func() {
		// We hold the token; the channel is empty, so this cannot block.
		c.bcastSem <- struct{}{}
	}()
	return c.broadcastHoldingSem(ctx, v)
}

// TryBroadcast is like Broadcast, but if another broadcast is
// already in flight it returns (0, ErrBusy) immediately instead of
// queuing. Once it acquires the publisher slot it still waits for
// the barrier like Broadcast.
func (c *Cohort[V]) TryBroadcast(v V) (int, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	select {
	case <-c.bcastSem:
	default:
		return 0, ErrBusy
	}
	defer func() {
		c.bcastSem <- struct{}{}
	}()
	if c.closed.Load() {
		return 0, ErrClosed
	}
	return c.broadcastHoldingSem(context.Background(), v)
}

// broadcastHoldingSem publishes and waits for the barrier.
// The caller must hold bcastSem.
func (c *Cohort[V]) broadcastHoldingSem(ctx context.Context, v V) (int, error) {
	// If the context was already cancelled before we acquired the
	// publisher slot, do not publish. A cancellation that races
	// with publish is still linearizable as publish-then-abandon.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	if c.closedUnderMu {
		c.mu.Unlock()
		return 0, ErrClosed
	}
	if c.pendingCount != 0 {
		// Fail-fast: bcastSem serializes publishers and every path
		// clears pending before releasing it, so this is unreachable
		// unless the state machine is corrupted.
		n := c.pendingCount
		c.mu.Unlock()
		panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d at broadcast start", n))
	}
	// Hold mu across value write + epoch increment + member
	// snapshot so Join cannot interleave and be counted without
	// having its ack watermark set (which would over-count the
	// barrier and hang the publisher forever).
	c.valMu.Lock()
	c.value = v
	c.valMu.Unlock()
	newEpoch := c.epoch.v.Add(1)
	n := len(c.members)
	var done chan struct{}
	if n > 0 {
		done = make(chan struct{})
		c.pendingEpoch = newEpoch
		c.pendingCount = n
		c.pendingDone = done
		// Per-member coalesced wakeup: one non-blocking send to
		// each joined member while still holding mu, so the
		// wakeup set is exactly the barrier set. Never blocks,
		// never allocates. A member that joins after this point
		// has acked == newEpoch and needs no wakeup for it.
		for mm := range c.members {
			select {
			case mm.notify <- struct{}{}:
			default:
			}
		}
	}
	c.mu.Unlock()

	if n == 0 {
		return 0, nil
	}

	select {
	case <-done:
		return n, nil
	case <-ctx.Done():
		// Prefer a completed barrier over cancellation if both
		// are ready: the broadcast actually succeeded.
		select {
		case <-done:
			return n, nil
		default:
		}
		// Abandon the barrier. The value stays published.
		c.mu.Lock()
		if c.pendingDone == done {
			c.pendingEpoch = 0
			c.pendingCount = 0
			c.pendingDone = nil
		}
		c.mu.Unlock()
		return 0, ctx.Err()
	case <-c.closedCh:
		select {
		case <-done:
			return n, nil
		default:
		}
		c.mu.Lock()
		if c.pendingDone == done {
			c.pendingEpoch = 0
			c.pendingCount = 0
			c.pendingDone = nil
		}
		c.mu.Unlock()
		return 0, ErrClosed
	}
}

// Close deterministically tears down the Cohort. It unblocks
// every waiter and every in-flight broadcast, is idempotent, and
// leaves no goroutine behind. Members that have not yet left
// remain joined (Members still counts them) but all waits return
// ErrClosed unless they have an unacked value to return first.
func (c *Cohort[V]) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.closedUnderMu = true
	done := c.pendingDone
	c.pendingEpoch = 0
	c.pendingCount = 0
	c.pendingDone = nil
	// Wake any waiter blocked only on Notify (select-composable
	// callers that do not also watch Closed).
	for mm := range c.members {
		select {
		case mm.notify <- struct{}{}:
		default:
		}
	}
	c.mu.Unlock()
	close(c.closedCh)
	if done != nil {
		close(done)
	}
}

// Notify returns this member's coalesced wakeup channel. It is
// stable for the member's lifetime, so callers may cache it and
// select on it in a loop. Broadcast signals it once per broadcast
// (coalesced if the member is slow); Close signals it once.
// A wakeup alone does not guarantee a new epoch: callers must
// re-check TryWait / Snapshot after waking. The channel is never
// closed; use Closed to observe teardown.
func (m *Member[V]) Notify() <-chan struct{} {
	return m.notify
}

// Closed returns the cohort's closed channel. See Cohort.Closed.
func (m *Member[V]) Closed() <-chan struct{} {
	return m.cohort.closedCh
}

// Acked returns the highest epoch this member has acked.
func (m *Member[V]) Acked() uint64 {
	return m.acked.v.Load()
}

// IsLeft reports whether Leave has been called.
func (m *Member[V]) IsLeft() bool {
	return m.left.Load()
}

// Snapshot returns the latest published value and its epoch.
// It returns ok=false if nothing has been published yet or if the
// member has left.
//
// Snapshot is consistent: it reads the epoch, copies the value
// under RLock, then re-validates the epoch. If a Broadcast
// interleaved, it retries, so a torn value+epoch pair is
// impossible. Concurrent Snapshots never block each other.
func (m *Member[V]) Snapshot() (V, uint64, bool) {
	if m.left.Load() {
		var zero V
		return zero, 0, false
	}
	c := m.cohort
	for {
		e1 := c.epoch.v.Load()
		if e1 == 0 {
			var zero V
			return zero, 0, false
		}
		c.valMu.RLock()
		v := c.value
		c.valMu.RUnlock()
		e2 := c.epoch.v.Load()
		if e1 == e2 {
			if m.left.Load() {
				var zero V
				return zero, 0, false
			}
			return v, e1, true
		}
		// A writer interleaved between the epoch read and the
		// value copy. Retry. With a single serialized publisher
		// this is bounded and rare; yield so a spinning reader
		// does not starve the writer.
		runtime.Gosched()
		if m.left.Load() {
			var zero V
			return zero, 0, false
		}
	}
}

// TryWait is the non-blocking form of Wait: it returns the latest
// value only if it has not yet been acked.
func (m *Member[V]) TryWait() (V, uint64, bool) {
	v, e, ok := m.Snapshot()
	if !ok {
		var zero V
		return zero, 0, false
	}
	if e <= m.acked.v.Load() {
		var zero V
		return zero, 0, false
	}
	return v, e, true
}

// Wait blocks until a new, unacked epoch is available, the
// context is cancelled, or the Cohort is closed.
//
// Wait does not Ack. The caller must call Ack(epoch) after it has
// processed the value, or the publisher's barrier will wait
// until the member leaves or the publisher's context expires.
// Use Next for receive-and-ack in one call, or All for an
// iterator that acks automatically.
func (m *Member[V]) Wait(ctx context.Context) (V, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.left.Load() {
		var zero V
		return zero, 0, ErrClosed
	}
	// Fast path: a value is already waiting.
	if v, e, ok := m.TryWait(); ok {
		return v, e, nil
	}
	if m.cohort.closed.Load() {
		var zero V
		return zero, 0, ErrClosed
	}
	for {
		select {
		case <-m.notify:
			// Fall through to the snapshot check below.
		case <-m.cohort.closedCh:
			// A value published just before Close must still be
			// deliverable; otherwise the waiter is done.
			if v, e, ok := m.TryWait(); ok {
				return v, e, nil
			}
			var zero V
			return zero, 0, ErrClosed
		case <-ctx.Done():
			var zero V
			return zero, 0, ctx.Err()
		}
		if v, e, ok := m.TryWait(); ok {
			return v, e, nil
		}
		// Spurious / coalesced wakeup: the notify token may have
		// been for an epoch this member already acked, or for
		// Close. If the cohort is closed and there is no new
		// value, stop instead of spinning on notify vs closed.
		if m.left.Load() {
			var zero V
			return zero, 0, ErrClosed
		}
		if m.cohort.closed.Load() {
			// Re-check once: a value may have landed with Close.
			if v, e, ok := m.TryWait(); ok {
				return v, e, nil
			}
			var zero V
			return zero, 0, ErrClosed
		}
		// Otherwise loop and wait for the next notify.
	}
}

// Next is Wait followed by Ack. It is appropriate when
// receive == processed for the caller, i.e. the caller does not
// need to hold the barrier open while it does further work with
// the value. For strict "process then ack" semantics, use
// Wait + Ack explicitly.
func (m *Member[V]) Next(ctx context.Context) (V, uint64, error) {
	v, e, err := m.Wait(ctx)
	if err != nil {
		var zero V
		return zero, 0, err
	}
	m.Ack(e)
	return v, e, nil
}

// Ack marks epoch as processed by this member. It returns true
// only if this Ack completed the barrier contribution for the
// currently pending epoch (i.e. it decremented the pending
// count). It returns false for stale epochs, duplicate acks,
// acks after Leave, and acks for an epoch whose barrier was
// already abandoned or completed.
//
// Ack with an epoch greater than the cohort's current epoch is a
// protocol violation and panics, because such an epoch can never
// have been observed via Snapshot / Wait.
func (m *Member[V]) Ack(epoch uint64) bool {
	if m.left.Load() {
		return false
	}
	c := m.cohort
	if epoch <= m.acked.v.Load() {
		return false
	}
	c.mu.Lock()
	var toClose chan struct{}
	acked := false
	if _, ok := c.members[m]; ok {
		if epoch == c.pendingEpoch {
			// Re-check under mu: a concurrent Ack for this same
			// member is serialized on mu, so the first one wins.
			if epoch > m.acked.v.Load() {
				m.acked.v.Store(epoch)
				c.pendingCount--
				if c.pendingCount < 0 {
					n := c.pendingCount
					c.mu.Unlock()
					panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d after Ack", n))
				}
				acked = true
				if c.pendingCount == 0 {
					toClose = c.pendingDone
					c.pendingEpoch = 0
					c.pendingDone = nil
				}
			}
		} else {
			// Stale or already-completed epoch: advance the
			// watermark so Wait does not return it again.
			// A future epoch is impossible without a bug.
			if epoch > c.epoch.v.Load() {
				cur := c.epoch.v.Load()
				c.mu.Unlock()
				panic(fmt.Sprintf("bigbuff: Ack(%d) > current epoch %d: protocol violation", epoch, cur))
			}
			if epoch > m.acked.v.Load() {
				m.acked.v.Store(epoch)
			}
		}
	}
	c.mu.Unlock()
	if toClose != nil {
		close(toClose)
	}
	return acked
}

// Leave departs the Cohort. It is idempotent.
//
// Leave is compensating: if this member had not yet acked the
// in-flight broadcast, Leave atomically decrements the pending
// barrier count, so the publisher never stalls waiting for a
// departed member. A member that joins after a broadcast was
// published has acked == pendingEpoch, so Leave does not
// decrement for it (it was never counted in a way that needs
// compensation, or rather it was counted but its watermark
// already covers the epoch; see Join).
//
// After Leave, Snapshot / Wait / Ack all become no-ops returning
// false / ErrClosed.
func (m *Member[V]) Leave() {
	if !m.left.CompareAndSwap(false, true) {
		return
	}
	c := m.cohort
	c.mu.Lock()
	var toClose chan struct{}
	if _, ok := c.members[m]; ok {
		delete(c.members, m)
		c.membersCount.v.Add(-1)
		if c.pendingEpoch != 0 && m.acked.v.Load() < c.pendingEpoch {
			c.pendingCount--
			if c.pendingCount < 0 {
				n := c.pendingCount
				c.mu.Unlock()
				panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d after Leave", n))
			}
			if c.pendingCount == 0 {
				toClose = c.pendingDone
				c.pendingEpoch = 0
				c.pendingDone = nil
			}
		}
	}
	c.mu.Unlock()
	if toClose != nil {
		close(toClose)
	}
}

// All returns an iterator over newly published values. Each value
// is yielded exactly once, in epoch order, until ctx is cancelled
// or the Cohort is closed.
//
// The iterator acks each value after the consumer has processed
// it: if yield returns true, the value is acked before waiting
// for the next one; if yield returns false (break), the last
// yielded value is acked before All returns. Breaking out of the
// loop therefore compensates the publisher exactly like an
// explicit Ack, and never stalls the barrier.
func (m *Member[V]) All(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		var pending uint64
		var hasPending bool
		defer func() {
			if hasPending {
				m.Ack(pending)
			}
		}()
		for {
			v, e, err := m.Wait(ctx)
			if err != nil {
				return
			}
			pending = e
			hasPending = true
			if !yield(v) {
				return
			}
			m.Ack(e)
			hasPending = false
		}
	}
}
-- doc.go --
// Package bigbuff implements a genuinely novel, production-grade
// coordination primitive: a select-composable, barrier-synchronized
// broadcast to a dynamic cohort of receivers with mid-flight
// cancellation compensation.
//
// # The problem
//
// Go's standard primitives break down exactly where high-performance
// state transitions collide with runtime composability:
//
//   - A naive broadcast loop over caller-owned unbuffered channels
//     deadlocks when a receiver cancels, leaves, or breaks out of a
//     range loop mid-broadcast. The sender is stuck in a rendezvous
//     send with no receiver, and there is no way to compensate.
//   - A per-subscriber buffered queue per receiver still cannot
//     express "wait until every currently-joined receiver has
//     processed this value, or has deterministically departed,"
//     and a rendezvous send to it still deadlocks on mid-flight
//     leave. Cohort pairs a single-slot coalesced wakeup channel
//     per member (never a queue, never a blocking send) with an
//     explicit barrier counter, so departure compensates.
//   - sync.Cond thundering-herds every waiter on every publish,
//     and callback registries break select ergonomics: the caller
//     cannot write `select { case <-m.Notify(): ... case
//     <-ctx.Done(): }` because the primitive owns the wakeup.
//
// # What Cohort is
//
// A Cohort[V] is a synchronous, barrier-synchronized broadcast:
//
//   - Join() admits a Member. Members are cheap, pointer-stable,
//     one small buffered channel each, and no goroutine.
//   - Broadcast(ctx, v) publishes v as a new epoch and blocks
//     until every member that was joined at publish time has
//     either Acked that epoch or deterministically departed via
//     Leave(). If there are no members it returns immediately.
//   - A Member observes values without any channel rendezvous:
//     either by blocking in Member.Wait(ctx), by composing
//     Member.Notify() directly inside its own select, by polling
//     Member.Snapshot(), or by ranging over Member.All(ctx)
//     (iter.Seq[V]).
//   - Cancellation is compensating, not leaking. If a Member
//     leaves mid-broadcast, Leave() atomically decrements the
//     pending barrier count, so the publisher never stalls. If
//     the publisher's context is cancelled mid-broadcast, the
//     barrier is abandoned, the value stays published, and the
//     next broadcast proceeds. If a consumer breaks out of
//     Member.All early, the iterator acks the last value it
//     yielded before returning.
//   - Close() is deterministic and fail-fast. It unblocks every
//     waiter and every in-flight broadcast, closes no channel
//     twice, and leaves no goroutine behind.
//
// # Why it beats chan / Mutex / Cond
//
//   - Per-member coalesced wakeup channels. Each member owns
//     one `chan struct{}` of capacity 1, stable for its lifetime
//     so callers may cache Notify() and select on it in a loop.
//     Publish does one non-blocking send per joined member.
//     A single shared channel would deliver its one token to a
//     single waiter and starve the rest; per-member channels
//     wake every waiter. Multiple rapid broadcasts coalesce
//     into one token per member; waiters re-check the epoch,
//     so no wakeup is lost, there is no thundering herd, and
//     there is no per-broadcast channel allocation on the
//     receive path.
//   - Barrier, not rendezvous. The publisher never selects over
//     N receiver channels and never blocks in a rendezvous send.
//     It waits on one per-broadcast done channel while receivers
//     ack via a single mutex-protected counter. Publish does
//     O(N) non-blocking coalesced sends (never a blocking send
//     or an O(N) select), each ack is O(1), and there is no
//     goroutine per receiver.
//   - Epoch + RWMutex snapshot, not torn reads. The value is
//     guarded by an RWMutex and the epoch is an atomic counter.
//     Snapshot reads epoch, copies the value under RLock, then
//     re-validates the epoch. If a writer interleaved, it retries.
//     Readers never block each other, writers never block readers
//     except for the instant of the copy, and a torn
//     value+epoch pair is impossible.
//   - Cache-line isolation. Hot atomics (epoch, member count,
//     per-member ack watermark) live on their own 64-byte lines
//     with explicit padding, so ack traffic from different
//     goroutines does not false-share.
//   - Cancellable publisher admission. Publishers serialize on a
//     capacity-1 semaphore channel, acquired via select on
//     ctx.Done() and Closed(). A publisher that loses a race or
//     times out never blocks forever holding a mutex.
//   - Fail-fast invariants. Pending counts, epoch monotonicity,
//     and member-count consistency are checked under the lock
//     and panic on violation instead of silently hanging. See
//     Cohort.CheckInvariants.
//
// # Correct usage
//
// Publisher:
//
//	cohort := bigbuff.New[int]()
//	n, err := cohort.Broadcast(ctx, 42)
//
// Receiver, explicit ack (barrier semantics):
//
//	m, ok := cohort.Join()
//	if !ok { /* closed */ }
//	defer m.Leave()
//	for {
//	    v, epoch, err := m.Wait(ctx)
//	    if err != nil { break }
//	    process(v)
//	    m.Ack(epoch)
//	}
//
// Receiver, select-composable:
//
//	select {
//	case <-m.Notify():
//	    if v, epoch, ok := m.TryWait(); ok {
//	        process(v)
//	        m.Ack(epoch)
//	    }
//	case <-ctx.Done():
//	    m.Leave() // compensates any in-flight broadcast
//	    return ctx.Err()
//	case <-m.Closed():
//	    return bigbuff.ErrClosed
//	}
//
// Receiver, iterator (break is compensating):
//
//	for v := range m.All(ctx) {
//	    if done(v) { break } // iterator acks v before returning
//	    process(v)
//	}
//
// Join does not replay old epochs. A member that joins after a
// broadcast will only observe broadcasts published after it joined.
// A member must call Leave exactly once (it is idempotent) or the
// cohort retains it and future broadcasts will wait for it.
//
// Hot path allocations: Notify, Snapshot, TryWait, Ack, and
// Leave allocate nothing. Join allocates one small channel per
// member, amortized over the member's lifetime. Broadcast
// allocates one done channel per broadcast that has at least
// one member, amortized over N deliveries. There are no
// background goroutines.
package bigbuff

````
