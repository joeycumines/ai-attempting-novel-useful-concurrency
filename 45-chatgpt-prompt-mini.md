I implemented two independent, complementary broadcast architectures, rather than adapting either of your existing implementations.



Tests and race detector passing

0 allocations per publication



## Complete implementation

Full source bundle

Includes both implementations, tests, documentation and a standalone Go module.

Download bigbuff-fanout.zip

Individual source files

fanout.go — exact channel fanout

ringfanout.go — shared ring fanout

fanout_test.go — tests and benchmarks

README.md — architecture and API documentation

## 1. `Fanout[T]` — exact delivery with independent select channels

This is the implementation for the particularly valuable property you identified: subscribers receive through their own channels.

```
f := bigbuff.NewFanout[Event]()

sub, err := f.Subscribe()
if err != nil {
    panic(err)
}
defer sub.Close()

for {
    select {
    case event, ok := <-sub.C():
        if !ok {
            return
        }
        handle(event)

    case <-ctx.Done():
        return
    }
}
```

The interesting implementation characteristics:

- Intrusive doubly linked subscriber list.
- O(1) insertion and removal.
- Atomic forward traversal without taking the membership mutex.
- No subscriber snapshots, temporary slices, callbacks, or per-publication goroutines.
- Two-bit lifecycle state: active and closed.
- Serialized publishers, eliminating the need for a reference counter.
- A two-stage send path, keeping the immediately-ready send path particularly small.
- Cancellation of blocked sends on unsubscribe.
- Optional independent bounded subscriber channels.

The important optimization is that subscription removal doesn't require acquiring the publisher mutex. A subscriber can close its acquisition gate, unlink itself, and cancel a blocked delivery independently.

## 2. `RingFanout[T]` — shared storage with coalesced readiness

This is the more adventurous implementation, and arguably the more interesting complementary design.

Instead of delivering the payload separately to every subscriber, it stores each value once in a fixed-capacity ring.

Each subscriber maintains an independent sequence cursor and owns a single-slot readiness channel.

```
ring, err := bigbuff.NewRingFanout[Event](4096)
if err != nil {
    panic(err)
}

sub, err := ring.SubscribeReplay()
if err != nil {
    panic(err)
}
defer sub.Close()

for {
    select {
    case _, ok := <-sub.Ready():
        if !ok {
            return
        }

        for {
            event, missed, ok := sub.TryNext()
            if !ok {
                break
            }

            if missed != 0 {
                recordGap(missed)
            }

            handle(event)
        }

    case <-ctx.Done():
        return
    }
}
```

Its particularly valuable properties:

- Payload stored once, irrespective of subscriber count.
- Fixed-size payload storage.
- Nonblocking publication, independent of consumer speed.
- Coalesced notifications rather than one notification per publication.
- Per-subscriber sequence cursors.
- Replay of retained history.
- Explicit missed-message accounting.
- Concurrent blocking `Next` readers, with notification hand-off when unread entries remain.
- Lifecycle leases protecting both notification channels and concurrent slot reads.
- Shutdown releases references held by retained payloads.

The interesting characteristic is that the notification channel is not the data transport. It merely advertises that the shared log may contain unread entries.

That decoupling is what makes this architecture substantially different from ordinary channel fanout.

## 3. Actual benchmark results

Go 1.23.2 · linux/amd64 · AMD EPYC 9V74 · local measurements

| Operation                     | Time/op  | Allocations |
| ----------------------------- | -------- | ----------- |
| Fanout, 1 subscriber          | 30.94 ns | 0           |
| Fanout, 64 subscribers        | 1,841 ns | 0           |
| RingFanout, 64 subscribers    | 385.8 ns | 0           |
| RingFanout, 1,000 subscribers | 7,860 ns | 0           |

Publication cost

Measured nanoseconds per publication; lower is better.

0 ns2.5K ns5K ns7.5K ns10K nsFanout · 1Fanout · 64Ring · 64Ring · 1,000

These are standalone measurements, not yet a head-to-head comparison against your original implementations.

## 4. Validation

All of the following completed successfully:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go test -count=100 ./...`
- 32-bit architecture compilation using `GOARCH=386 go test -c`.
- Zero-allocation assertions for both steady-state publication paths.

## My architectural recommendation

I would retain both implementations.

`Fanout[T]` is the natural solution when exact delivery, direct channel selection and conventional Go channel semantics matter.

`RingFanout[T]` is the more unusual and potentially more broadly applicable solution when very large subscriber populations, slow consumers, bounded memory, and publisher independence matter.

One important distinction: the ring intentionally sacrifices guaranteed retention for arbitrarily slow consumers. It makes that sacrifice observable rather than silently hiding it.

The next important performance validation is a head-to-head benchmark against your existing `ChanCaster` and `ChanPubSub`, particularly under subscription churn and increasing subscriber counts. The implementations and standalone benchmark harness are included in the bundle.

---------

````txtar
-- README.md --
# `bigbuff` fanout primitives

Two independent broadcast designs for `package bigbuff`. Neither uses a
shared unbuffered payload channel with a receiver counter, nor a callback
registration/counting protocol.

- **`Fanout[T]`** — exact delivery to independent Go channels. Use when ordinary
  `select` receives `T` directly and a slow consumer must exert backpressure.
- **`RingFanout[T]`** — one shared bounded payload ring plus coalesced per-reader
  readiness. Use when publishers must not wait for readers, bounded retention is
  acceptable, and lag must be reported.

Both APIs have explicit shutdown, O(1) membership edits, no per-publication
subscriber snapshots, and zero steady-state publication allocations in the
included benchmarks.

## 1. Exact per-subscriber channels: `Fanout[T]`

```go
f := bigbuff.NewFanout[Event]()
sub, err := f.Subscribe() // or SubscribeBuffered(64)
if err != nil {
    return err
}
defer sub.Close()

for {
    select {
    case event, ok := <-sub.C():
        if !ok { return nil }
        handle(event)
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

### Mechanics

`Fanout` maintains an intrusive doubly linked list. Subscribe inserts at the
head; unsubscribe removes using the node's `prev` and atomic `next` links. Both
edits are O(1) under a short membership mutex. Publishers traverse only atomic
forward links and do not lock the membership mutex or allocate a snapshot.

Publishers are serialized to preserve a global publication order and reduce
each node's lifecycle state to two bits: `active` and `closed`. The active bit
protects the channel during a send. The closed bit prevents new sends after
unsubscription starts. This is a single-flight lease, not a reference count,
because only one publisher can be in the traversal at once.

The send hot path has two stages:

1. A send/default select handles a receiver that is immediately ready.
2. Only on the default path does a second select include the node's stop
   channel, so a blocked send can be cancelled by `Subscription.Close`.

This avoids paying for the stop/cancellation select on the immediate hand-off
path. The lease ensures that the receive channel is not closed while either
send select is using it.

### Semantics

- Every publication traverses the subscriptions present in its head snapshot.
  A subscription inserted after that snapshot is not included.
- Subscribers are visited newest-first. Every subscriber observes the same
  publication order because publishers are serialized.
- `Subscribe` is unbuffered and provides rendezvous/backpressure semantics.
  `SubscribeBuffered(n)` gives that subscriber an independent bounded queue.
- A slow unbuffered or full-buffer subscriber can hold up `Broadcast`.
  `BroadcastContext` can abort a blocked send on context cancellation and
  returns the number already delivered; completed deliveries are not rolled
  back.
- `Subscription.Close` closes `C()` before it returns. A hand-off whose channel
  case is already ready may still complete first. Queued values in a buffered
  channel remain readable after close, as with ordinary Go channels.
- `Fanout.Close` rejects new subscriptions, signals all active nodes to abort
  blocked sends, then waits for the current publisher to quiesce.
- The zero value is usable. Do not copy a `Fanout` or `Subscription` after
  first use.

The list is designed for churn: an insertion or removal changes only the
neighbour links and count. It does not rebuild a subscriber slice or leave
inactive tombstones for every historical subscriber.

## 2. Shared bounded ring: `RingFanout[T]`

```go
ring, err := bigbuff.NewRingFanout[Event](4096)
if err != nil { return err }
sub, err := ring.SubscribeReplay()
if err != nil { return err }
defer sub.Close()

for {
    select {
    case _, ok := <-sub.Ready():
        if !ok { return nil }
        for {
            event, missed, ok := sub.TryNext()
            if !ok { break }
            if missed != 0 { recordGap(missed) }
            handle(event)
        }
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

Alternatively, use `sub.Next(ctx)` for a blocking pull API. Multiple concurrent
`Next` waiters are supported: after a waiter consumes an entry, it re-signals
if more entries remain. Choose either the `Ready` + `TryNext` protocol or the
`Next` protocol for a subscription; do not consume `Ready` independently while
other goroutines are blocked in `Next` on that same subscription.

### Mechanics

The ring has a fixed array of `capacity` slots. Each slot contains a sequence
number, value, and RWMutex. A publisher serializes sequence assignment, writes
one slot, publishes the sequence, and traverses the subscriber list. Each
subscriber has a capacity-one `chan struct{}`. A non-blocking send means only
"there may be unread values"; if the token is already present, another signal
is redundant. The payload is never copied into N subscriber channels.

Each subscriber owns a sequence cursor. `TryNext` checks the global published
sequence, validates the selected slot's sequence under its read lock, and
recalculates the oldest retained sequence if a slot was overwritten during the
read. Its `missed` return value tells the consumer exactly how many sequence
positions were skipped before the value it receives.

The node's lease state uses a closed bit plus a small reference count. Unlike
the exact fanout, a ring node may be leased concurrently by a publisher
notifier and one or more cursor reads. The lease prevents `Ready` from closing
while a notification send is active and lets subscription close wait for
cursor readers before dropping its hub reference.

`RingFanout.Close` closes publication/subscription admission, waits for the
admitted publisher, closes reader admission and waits for in-progress slot
copies, clears every slot's value (releasing references held by retained
payloads), and finally closes all readiness channels. `Published()` remains the
lifetime total after close.

### Semantics

- `Publish` does not wait for a consumer. A slow consumer can lose old values
  when the ring wraps; `missed` reports that loss.
- `Subscribe` starts at the current tail. `SubscribeReplay` starts at the oldest
  entry currently retained, up to `Capacity()` entries.
- `Ready()` notifications coalesce. One notification may cover many published
  values; after receiving it, drain `TryNext` until `ok` is false.
- `Next(ctx)` returns one value, missed count, and `ok`; cancellation returns
  `ctx.Err()`, and close returns `ok=false`.
- `Close` rejects new subscriptions/publications, closes `Ready`, and releases
  references held in ring slots.
- The zero value is intentionally unusable; construct with
  `NewRingFanout(capacity)`. Do not copy a `RingFanout` or
  `RingSubscription` after first use.

## Choosing between them

| Requirement | `Fanout[T]` | `RingFanout[T]` |
|---|---|---|
| Direct `select` receives `T` | Yes, `sub.C()` | No, select `Ready()` then pull |
| Exact delivery to every active subscriber | Yes, subject to close/context cancellation | No, lagging readers can miss overwritten values |
| Publisher blocks on slow consumers | Yes, when unbuffered/full | No |
| Payload stored once | No, one channel hand-off per reader | Yes, one shared ring slot |
| Bounded payload retention | Per-subscription buffers | Fixed-capacity ring |
| Missed-entry accounting | Not applicable | Yes |
| Steady-state publication allocations | 0 | 0 |
| O(1) subscribe/unsubscribe list edits | Yes | Yes |

Both remain O(N) in the number of subscribers: fanout cannot notify N independent
receivers without visiting them. The ring's advantage is that the O(N) pass
sends only a coalesced `struct{}` signal rather than copying the payload or
waiting for the reader. The direct-channel fanout is the better fit when
selecting the actual payload and preserving every hand-off are the primary
requirements.

## Validation

```sh
go test ./...
go test -race ./...
go test -count=100 ./...
go test -run '^$' -bench . -benchmem
```

The tests cover exact delivery, direct select, buffering, context cancellation,
concurrent subscription churn, blocked-send shutdown, concurrent `Next`
waiters, replay, overwrite accounting, close races, and zero-allocation
publication paths.
-- fanout_test.go --
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

func TestFanoutExactDeliveryAndSelect(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()
	const count = 8
	subs := make([]*Subscription[int], count)
	for i := range subs {
		var err error
		subs[i], err = f.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		defer subs[i].Close()
	}

	var wg sync.WaitGroup
	got := make([]atomic.Int64, count)
	wg.Add(count)
	for i, sub := range subs {
		go func(i int, sub *Subscription[int]) {
			defer wg.Done()
			select {
			case v := <-sub.C():
				if v == 42 {
					got[i].Add(1)
				}
			case <-time.After(time.Second):
			}
		}(i, sub)
	}
	if n := f.Broadcast(42); n != count {
		t.Fatalf("delivered=%d want %d", n, count)
	}
	wg.Wait()
	for i := range got {
		if got[i].Load() != 1 {
			t.Fatalf("subscriber %d got %d", i, got[i].Load())
		}
	}
}

func TestFanoutBufferedAndContext(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()
	sub, err := f.SubscribeBuffered(2)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if n := f.Broadcast(1); n != 1 {
		t.Fatalf("delivered=%d", n)
	}
	if n := f.Broadcast(2); n != 1 {
		t.Fatalf("delivered=%d", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, err := f.BroadcastContext(ctx, 3); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("BroadcastContext=(%d,%v)", n, err)
	}
	if a, b := <-sub.C(), <-sub.C(); a != 1 || b != 2 {
		t.Fatalf("values=(%d,%d)", a, b)
	}
}

func TestFanoutCloseCancelsBlockedSend(t *testing.T) {
	f := NewFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	broadcastDone := make(chan int, 1)
	go func() { close(started); broadcastDone <- f.Broadcast(7) }()
	<-started
	closeDone := make(chan struct{})
	go func() { sub.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("subscription close blocked")
	}
	select {
	case n := <-broadcastDone:
		if n != 0 {
			t.Fatalf("delivered=%d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher remained blocked")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("channel remains open")
	}
	f.Close()
}

func TestFanoutHubCloseAndChurn(t *testing.T) {
	f := NewFanout[int]()
	a, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.SubscribeBuffered(2)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	pubDone := make(chan struct{})
	go func() { close(started); f.Broadcast(1); close(pubDone) }()
	<-started
	closeDone := make(chan struct{})
	go func() { f.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("hub close blocked")
	}
	<-pubDone
	for name, sub := range map[string]*Subscription[int]{"a": a, "b": b} {
		for range sub.C() { /* drain buffered values */
		}
		select {
		case _, ok := <-sub.C():
			if ok {
				t.Fatalf("%s remained open", name)
			}
		default:
			t.Fatalf("%s channel not closed", name)
		}
		sub.Close()
	}
	if _, err := f.Subscribe(); !errors.Is(err, ErrFanoutClosed) {
		t.Fatalf("Subscribe err=%v", err)
	}
}

func TestFanoutConcurrentCloseAndChurn(t *testing.T) {
	f := NewFanout[int]()
	const workers, loops = 4, 100
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				sub, err := f.SubscribeBuffered(2)
				if err != nil {
					t.Error(err)
					return
				}
				go func() {
					for range sub.C() {
					}
				}()
				f.Broadcast(j)
				sub.Close()
			}
		}()
	}
	wg.Wait()
	if got := f.SubscriberCount(); got != 0 {
		t.Fatalf("count=%d", got)
	}
	f.Close()
}

func TestFanoutConcurrentCloseCallsWait(t *testing.T) {
	f := NewFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sub.Close() }()
	}
	wg.Wait()
	if _, ok := <-sub.C(); ok {
		t.Fatal("channel remains open")
	}
	f.Close()
}

func TestFanoutBroadcastNoAllocations(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()
	sub, err := f.SubscribeBuffered(1)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	f.Broadcast(0)
	<-sub.C()
	allocs := testing.AllocsPerRun(1000, func() {
		if f.Broadcast(1) != 1 {
			panic("wrong delivery count")
		}
		<-sub.C()
	})
	if allocs != 0 {
		t.Fatalf("allocs/op=%v", allocs)
	}
}

func TestRingFanoutOverwriteAndReplay(t *testing.T) {
	f, err := NewRingFanout[int](4)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := 1; i <= 10; i++ {
		if seq, err := f.Publish(i); err != nil || seq != uint64(i) {
			t.Fatalf("Publish=(%d,%v)", seq, err)
		}
	}
	v, missed, ok := sub.TryNext()
	if !ok || v != 7 || missed != 6 {
		t.Fatalf("TryNext=(%d,%d,%v)", v, missed, ok)
	}
	for want := 8; want <= 10; want++ {
		v, missed, ok = sub.TryNext()
		if !ok || v != want || missed != 0 {
			t.Fatalf("TryNext=(%d,%d,%v), want %d", v, missed, ok, want)
		}
	}
	if _, _, ok := sub.TryNext(); ok {
		t.Fatal("unexpected value")
	}

	replay, err := f.SubscribeReplay()
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	for _, want := range []int{7, 8, 9, 10} {
		v, missed, ok = replay.TryNext()
		if !ok || v != want || missed != 0 {
			t.Fatalf("replay=(%d,%d,%v) want %d", v, missed, ok, want)
		}
	}
}

func TestRingFanoutReadyAndNext(t *testing.T) {
	f, err := NewRingFanout[string](8)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := f.Publish("hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.Ready():
		if !ok {
			t.Fatal("ready unexpectedly closed")
		}
	case <-time.After(time.Second):
		t.Fatal("no ready notification")
	}
	v, missed, ok := sub.TryNext()
	if !ok || v != "hello" || missed != 0 {
		t.Fatalf("TryNext=(%q,%d,%v)", v, missed, ok)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, ok, err := sub.Next(ctx); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next=(%v,%v)", ok, err)
	}
}

func TestRingFanoutConcurrentNextWaiters(t *testing.T) {
	f, err := NewRingFanout[int](32)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	const readers = 8
	var wg sync.WaitGroup
	values := make(chan int, readers)
	started := make(chan struct{}, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			v, _, ok, err := sub.Next(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				values <- v
			}
		}()
	}
	for i := 0; i < readers; i++ {
		<-started
	}
	for i := 0; i < readers; i++ {
		if _, err := f.Publish(i); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	close(values)
	seen := make(map[int]bool, readers)
	for v := range values {
		seen[v] = true
	}
	if len(seen) != readers {
		t.Fatalf("received %d unique values; want %d", len(seen), readers)
	}
}

func TestRingFanoutCloseWakesNext(t *testing.T) {
	f, err := NewRingFanout[int](4)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	result := make(chan bool, 1)
	go func() { close(started); _, _, ok, err := sub.Next(context.Background()); result <- ok || err != nil }()
	<-started
	sub.Close()
	select {
	case got := <-result:
		if got {
			t.Fatal("Next returned value/error")
		}
	case <-time.After(time.Second):
		t.Fatal("Next not woken")
	}
	f.Close()
}

func TestRingFanoutConcurrentPublishReadAndChurn(t *testing.T) {
	f, err := NewRingFanout[int](32)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	const total = 3000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			if _, err := f.Publish(i); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	var received, missed uint64
	for {
		_, skip, ok := sub.TryNext()
		if ok {
			received++
			missed += skip
			continue
		}
		select {
		case <-done:
			for {
				_, skip, ok = sub.TryNext()
				if !ok {
					break
				}
				received++
				missed += skip
			}
			if received+missed != total {
				t.Fatalf("received %d + missed %d != %d", received, missed, total)
			}
			return
		default:
			runtime.Gosched()
		}
	}
}

func TestRingFanoutCloseRace(t *testing.T) {
	for r := 0; r < 20; r++ {
		f, err := NewRingFanout[int](16)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if _, err := f.Publish(j); err != nil && !errors.Is(err, ErrRingFanoutClosed) {
						t.Error(err)
						return
					}
					runtime.Gosched()
				}
			}()
		}
		for i := 0; i < 10; i++ {
			sub, err := f.Subscribe()
			if err != nil {
				break
			}
			sub.Close()
		}
		wg.Add(1)
		go func() { defer wg.Done(); f.Close() }()
		wg.Wait()
		if _, err := f.Publish(1); !errors.Is(err, ErrRingFanoutClosed) {
			t.Fatalf("Publish after close: %v", err)
		}
	}
}

func TestRingFanoutNoAllocations(t *testing.T) {
	f, err := NewRingFanout[int](16)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := 0; i < 64; i++ {
		if _, err := f.Subscribe(); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := f.Publish(99); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs/op=%v", allocs)
	}
}

func BenchmarkFanoutBroadcast1(b *testing.B) {
	f := NewFanout[int]()
	sub, err := f.SubscribeBuffered(1)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer sub.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Broadcast(i)
		<-sub.C()
	}
}
func BenchmarkFanoutBroadcast64(b *testing.B) {
	f := NewFanout[int]()
	subs := make([]*Subscription[int], 64)
	for i := range subs {
		sub, err := f.SubscribeBuffered(1)
		if err != nil {
			b.Fatal(err)
		}
		subs[i] = sub
	}
	defer f.Close()
	for _, s := range subs {
		defer s.Close()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Broadcast(i)
		for _, s := range subs {
			<-s.C()
		}
	}
}
func BenchmarkRingFanoutPublish64(b *testing.B) {
	f, err := NewRingFanout[int](256)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := f.Subscribe(); err != nil {
			b.Fatal(err)
		}
	}
	defer f.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Publish(i); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkRingFanoutPublish1000(b *testing.B) {
	f, err := NewRingFanout[int](256)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := f.Subscribe(); err != nil {
			b.Fatal(err)
		}
	}
	defer f.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Publish(i); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRingFanoutCloseReleasesPayloadReferences(t *testing.T) {
	f, err := NewRingFanout[*int](4)
	if err != nil {
		t.Fatal(err)
	}
	v := 123
	if _, err := f.Publish(&v); err != nil {
		t.Fatal(err)
	}
	f.Close()
	for i := range f.slots {
		if f.slots[i].value != nil {
			t.Fatalf("slot %d retained payload after Close", i)
		}
	}
	if f.Published() != 1 {
		t.Fatalf("Published after Close=%d; want 1", f.Published())
	}
}

func TestRingFanoutZeroValueClose(t *testing.T) {
	var f RingFanout[int]
	f.Close()
	f.Close()
	if _, err := f.Subscribe(); !errors.Is(err, ErrRingFanoutCapacity) {
		t.Fatalf("Subscribe err=%v", err)
	}
}
-- ringfanout.go --
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
	"sync"
	"sync/atomic"
)

const (
	ringFanoutClosedBit uint32 = 1 << 31
	ringFanoutRefsMask  uint32 = ringFanoutClosedBit - 1

	ringReaderClosedBit uint64 = 1 << 63
	ringReaderRefsMask  uint64 = ringReaderClosedBit - 1
)

var (
	ErrRingFanoutCapacity = errors.New("bigbuff: ringfanout: capacity must be positive")
	ErrRingFanoutClosed   = errors.New("bigbuff: ringfanout: closed")
)

// RingFanout is a bounded overwrite-oldest broadcast log. A publication is
// stored once in a shared ring; each subscriber has a one-slot Ready channel
// carrying coalesced notifications, not payloads. A slow reader can lose
// overwritten entries; TryNext and Next report the number missed. Publish
// never waits for readers and has zero steady-state heap allocations.
//
// This is complementary to Fanout: use Fanout for exact direct channel
// delivery, and RingFanout when shared storage, bounded memory, and nonblocking
// publishers matter more than guaranteed retention for arbitrarily slow
// subscribers. The zero value is not usable; use NewRingFanout. Do not copy
// after first use.
type RingFanout[T any] struct {
	mu        sync.Mutex // protects closed and intrusive list edits
	publishMu sync.Mutex // serializes slot writes, sequence assignment, signals

	head  atomic.Pointer[ringFanoutNode[T]]
	count atomic.Int64

	published atomic.Uint64
	slots     []ringFanoutSlot[T]

	// readerState gates slot reads during Close. Its high bit closes admission;
	// low bits count readers that already hold a slot-access lease.
	readerState atomic.Uint64
	readerDone  chan struct{}

	closed    bool // protected by mu
	closeOnce sync.Once
}

type ringFanoutSlot[T any] struct {
	mu    sync.RWMutex
	seq   uint64
	value T
}

type ringFanoutNode[T any] struct {
	hub   *RingFanout[T]
	ready chan struct{} // capacity one; notifications coalesce

	next atomic.Pointer[ringFanoutNode[T]]
	prev *ringFanoutNode[T] // protected by hub.mu

	// Closed bit gates new leases; low bits count concurrent notification
	// sends and cursor reads. A count is needed because these can overlap.
	state atomic.Uint32

	doneMu   sync.Mutex
	done     chan struct{}
	finished bool // protected by doneMu
	linked   bool // protected by hub.mu
}

// RingSubscription is a cursor into a RingFanout. Ready is an edge-notification
// channel, not a value channel. After it fires, drain TryNext until it returns
// ok=false. Next combines cursor reads with a context-aware wait.
//
// TryNext and Next serialize access to the cursor and may be called by multiple
// goroutines. Concurrent Next waiters are supported: a waiter re-signals when
// unread values remain. Do not mix Next waiters with independent consumers of
// Ready; choose one receive protocol per subscription.
type RingSubscription[T any] struct {
	node atomic.Pointer[ringFanoutNode[T]]
	hub  *RingFanout[T]

	ready <-chan struct{}

	readMu sync.Mutex
	cursor uint64

	closeOnce sync.Once
}

// NewRingFanout allocates a ring with exactly capacity slots. Capacity must be
// positive. The slot array never grows.
func NewRingFanout[T any](capacity int) (*RingFanout[T], error) {
	if capacity < 1 {
		return nil, ErrRingFanoutCapacity
	}
	return &RingFanout[T]{slots: make([]ringFanoutSlot[T], capacity), readerDone: make(chan struct{})}, nil
}

func (f *RingFanout[T]) Capacity() int        { return len(f.slots) }
func (f *RingFanout[T]) Published() uint64    { return f.published.Load() }
func (f *RingFanout[T]) SubscriberCount() int { return int(f.count.Load()) }

// Subscribe starts at the current tail and does not replay existing entries.
func (f *RingFanout[T]) Subscribe() (*RingSubscription[T], error) {
	return f.subscribe(false)
}

// SubscribeReplay starts at the oldest entry currently retained, up to
// Capacity entries.
func (f *RingFanout[T]) SubscribeReplay() (*RingSubscription[T], error) {
	return f.subscribe(true)
}

func (f *RingFanout[T]) subscribe(replay bool) (*RingSubscription[T], error) {
	if len(f.slots) == 0 {
		return nil, ErrRingFanoutCapacity
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrRingFanoutClosed
	}

	published := f.published.Load()
	cursor := published
	if replay {
		cursor = ringFanoutOldest(published, uint64(len(f.slots))) - 1
	}
	n := &ringFanoutNode[T]{hub: f, ready: make(chan struct{}, 1), linked: true}
	s := &RingSubscription[T]{hub: f, ready: n.ready, cursor: cursor}
	s.node.Store(n)

	oldHead := f.head.Load()
	n.next.Store(oldHead)
	if oldHead != nil {
		oldHead.prev = n
	}
	f.head.Store(n)
	f.count.Add(1)
	if cursor < published {
		n.ready <- struct{}{}
	}
	return s, nil
}

// Ready returns a coalesced receive-only signal channel. It is safe to use in
// select. The channel is closed when the subscription or hub closes.
func (s *RingSubscription[T]) Ready() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ready
}

// TryNext returns the next retained value, the number of entries skipped since
// the previous read, and whether a value was available. A closed subscription
// returns ok=false.
func (s *RingSubscription[T]) TryNext() (value T, missed uint64, ok bool) {
	if s == nil {
		return value, 0, false
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.hub == nil {
		return value, 0, false
	}
	n := s.node.Load()
	if n == nil || !n.acquire() {
		return value, 0, false
	}
	defer n.release()

	f := s.hub
	if !f.acquireReader() {
		return value, 0, false
	}
	defer f.releaseReader()
	capacity := uint64(len(f.slots))
	published := f.published.Load()
	target := s.cursor + 1
	if target == 0 || target > published {
		return value, 0, false
	}
	oldest := ringFanoutOldest(published, capacity)
	if target < oldest {
		missed = oldest - target
		target = oldest
	}

	for {
		slot := &f.slots[(target-1)%capacity]
		slot.mu.RLock()
		slotSeq := slot.seq
		if slotSeq == target {
			value = slot.value
			slot.mu.RUnlock()
			s.cursor = target
			return value, missed, true
		}
		slot.mu.RUnlock()

		// A greater slot sequence means an overwrite raced our published
		// snapshot. Recompute the retention window from the newer sequence.
		if slotSeq < target {
			panic("bigbuff: ringfanout: slot sequence regressed")
		}
		latest := f.published.Load()
		if latest < slotSeq {
			latest = slotSeq // includes a slot write before published.Store
		}
		newOldest := ringFanoutOldest(latest, capacity)
		if target < newOldest {
			missed += newOldest - target
			target = newOldest
		}
	}
}

// signalPending hands another coalesced wake-up to a concurrent Next waiter if
// there is still unread data after this caller consumed one item.
func (s *RingSubscription[T]) signalPending() {
	s.readMu.Lock()
	if s.hub == nil {
		s.readMu.Unlock()
		return
	}
	pending := s.cursor < s.hub.published.Load()
	n := s.node.Load()
	s.readMu.Unlock()
	if pending && n != nil {
		n.signal()
	}
}

// Next waits for a retained value or context cancellation. It returns ok=false
// if the subscription closes. missed reports overwritten entries. Concurrent
// Next waiters are supported; do not also consume Ready directly while using
// Next on the same subscription. A nil context panics.
func (s *RingSubscription[T]) Next(ctx context.Context) (value T, missed uint64, ok bool, err error) {
	if ctx == nil {
		panic("bigbuff: ringfanout: nil context")
	}
	if s == nil {
		return value, 0, false, nil
	}
	for {
		if value, missed, ok = s.TryNext(); ok {
			s.signalPending()
			return value, missed, true, nil
		}
		n := s.node.Load()
		if n == nil || n.state.Load()&ringFanoutClosedBit != 0 {
			return value, 0, false, nil
		}
		select {
		case _, open := <-s.ready:
			if !open {
				return value, 0, false, nil
			}
		case <-ctx.Done():
			return value, 0, false, ctx.Err()
		}
	}
}

// Close removes this subscription, closes Ready after active readers and
// notifiers release their leases, and drops its reference to the hub. It is
// idempotent; all concurrent callers wait for completion.
func (s *RingSubscription[T]) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		n := s.node.Swap(nil)
		if n == nil {
			return
		}
		n.deactivate()
		n.hub.unlink(n)
		n.waitFinished()
		s.readMu.Lock()
		s.hub = nil
		s.readMu.Unlock()
	})
}

// Publish stores value once, assigns a monotonically increasing sequence, and
// non-blockingly signals each linked subscription. A full Ready channel
// already represents unread work, so notifications coalesce. It never waits
// for a reader and performs no per-publication allocation.
func (f *RingFanout[T]) Publish(value T) (uint64, error) {
	if len(f.slots) == 0 {
		return 0, ErrRingFanoutCapacity
	}
	f.publishMu.Lock()
	defer f.publishMu.Unlock()

	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return f.published.Load(), ErrRingFanoutClosed
	}
	seq := f.published.Load() + 1
	if seq == 0 {
		panic("bigbuff: ringfanout: sequence overflow")
	}
	slot := &f.slots[(seq-1)%uint64(len(f.slots))]
	slot.mu.Lock()
	slot.value = value
	slot.seq = seq
	slot.mu.Unlock()
	f.published.Store(seq)

	f.mu.Lock()
	head := f.head.Load()
	f.mu.Unlock()
	for n := head; n != nil; {
		next := n.next.Load()
		n.signal()
		n = next
	}
	return seq, nil
}

// Close rejects new publications/subscriptions, lets the admitted publication
// finish, closes all Ready channels, and unlinks the entire membership chain.
func (f *RingFanout[T]) Close() {
	f.closeOnce.Do(func() {
		if f.readerDone == nil { // zero value: no storage or subscriptions exist
			f.mu.Lock()
			f.closed = true
			f.mu.Unlock()
			return
		}
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()

		f.publishMu.Lock()

		// No new publications can pass the closed check. Close reader admission
		// and wait for any cursor operation already copying a slot before
		// clearing payload references from the fixed ring.
		f.closeReaders()
		var zero T
		for i := range f.slots {
			slot := &f.slots[i]
			slot.mu.Lock()
			slot.value = zero
			slot.seq = 0
			slot.mu.Unlock()
		}

		f.mu.Lock()
		head := f.head.Load()
		f.head.Store(nil)
		f.count.Store(0)
		for n := head; n != nil; n = n.next.Load() {
			n.prev = nil
			n.linked = false
			n.deactivate()
		}
		f.mu.Unlock()

		for n := head; n != nil; {
			n.waitFinished()
			next := n.next.Load()
			n.next.Store(nil)
			n = next
		}
		f.publishMu.Unlock()
	})
}

func (f *RingFanout[T]) unlink(n *ringFanoutNode[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !n.linked {
		return
	}
	prev, next := n.prev, n.next.Load()
	if prev == nil {
		f.head.Store(next)
	} else {
		prev.next.Store(next)
	}
	if next != nil {
		next.prev = prev
	}
	n.prev = nil
	n.linked = false
	f.count.Add(-1)
}

func (f *RingFanout[T]) acquireReader() bool {
	for {
		state := f.readerState.Load()
		if state&ringReaderClosedBit != 0 {
			return false
		}
		if state&ringReaderRefsMask == ringReaderRefsMask {
			panic("bigbuff: ringfanout: reader lease counter overflow")
		}
		if f.readerState.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (f *RingFanout[T]) releaseReader() {
	if state := f.readerState.Add(^uint64(0)); state == ringReaderClosedBit {
		close(f.readerDone)
	}
}

func (f *RingFanout[T]) closeReaders() {
	for {
		state := f.readerState.Load()
		if state&ringReaderClosedBit != 0 {
			break
		}
		if f.readerState.CompareAndSwap(state, state|ringReaderClosedBit) {
			if state&ringReaderRefsMask == 0 {
				close(f.readerDone)
			}
			break
		}
	}
	<-f.readerDone
}

func (n *ringFanoutNode[T]) acquire() bool {
	for {
		state := n.state.Load()
		if state&ringFanoutClosedBit != 0 {
			return false
		}
		if state&ringFanoutRefsMask == ringFanoutRefsMask {
			panic("bigbuff: ringfanout: lease counter overflow")
		}
		if n.state.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (n *ringFanoutNode[T]) release() {
	if state := n.state.Add(^uint32(0)); state == ringFanoutClosedBit {
		n.finish()
	}
}

func (n *ringFanoutNode[T]) deactivate() {
	for {
		state := n.state.Load()
		if state&ringFanoutClosedBit != 0 {
			return
		}
		if n.state.CompareAndSwap(state, state|ringFanoutClosedBit) {
			if state&ringFanoutRefsMask == 0 {
				n.finish()
			}
			return
		}
	}
}

func (n *ringFanoutNode[T]) signal() {
	if !n.acquire() {
		return
	}
	if len(n.ready) == 0 {
		select {
		case n.ready <- struct{}{}:
		default:
			// Another notification/receiver won the single token slot.
		}
	}
	n.release()
}

func (n *ringFanoutNode[T]) finish() {
	close(n.ready)
	n.doneMu.Lock()
	n.finished = true
	if n.done != nil {
		close(n.done)
	}
	n.doneMu.Unlock()
}

func (n *ringFanoutNode[T]) waitFinished() {
	n.doneMu.Lock()
	if n.finished {
		n.doneMu.Unlock()
		return
	}
	done := n.done
	if done == nil {
		done = make(chan struct{})
		n.done = done
	}
	n.doneMu.Unlock()
	<-done
}

func ringFanoutOldest(published, capacity uint64) uint64 {
	if published < capacity {
		return 1
	}
	return published - capacity + 1
}
-- fanout.go --
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
	"sync"
	"sync/atomic"
)

var (
	ErrFanoutClosed         = errors.New("bigbuff: fanout: closed")
	ErrFanoutNegativeBuffer = errors.New("bigbuff: fanout: negative buffer capacity")
)

const (
	fanoutActiveBit uint32 = 1 << 0
	fanoutClosedBit uint32 = 1 << 1
)

// Fanout broadcasts independently to each subscription's channel.
//
// Its zero value is usable. Do not copy a Fanout after first use. Each
// subscription gets its own channel and can therefore receive directly in an
// ordinary select. Publishers are serialized, so every subscriber observes
// publications in the same order. The steady-state Broadcast path allocates
// nothing and takes no membership mutex.
//
// The intrusive list is edited under mu. Publishers traverse atomic forward
// links without copying a subscriber slice. A two-bit node lease prevents a
// channel from being closed while a send is active. Because sendMu serializes
// publishers, one active bit (rather than a reference counter) suffices.
//
// Exact delivery has an unavoidable cost: an unbuffered or full subscriber
// can hold up the publisher. SubscribeBuffered gives each subscriber an
// independent bounded queue when that trade-off is undesirable.
type Fanout[T any] struct {
	mu     sync.Mutex // protects closed, node.prev, and node.linked
	sendMu sync.Mutex // serializes publications

	head  atomic.Pointer[fanoutNode[T]]
	count atomic.Int64

	closed    bool // protected by mu
	closeOnce sync.Once
}

type fanoutNode[T any] struct {
	hub  *Fanout[T]
	ch   chan T
	stop chan struct{}

	next atomic.Pointer[fanoutNode[T]]
	prev *fanoutNode[T] // protected by hub.mu

	// active protects ch from closure during a send; closed bars new leases.
	state atomic.Uint32

	doneMu   sync.Mutex
	done     chan struct{} // allocated only if a closer must wait
	finished bool          // protected by doneMu
	linked   bool          // protected by hub.mu
}

// Subscription owns one receive channel in a Fanout.
type Subscription[T any] struct {
	node      atomic.Pointer[fanoutNode[T]]
	ch        <-chan T
	closeOnce sync.Once
}

func NewFanout[T any]() *Fanout[T] { return new(Fanout[T]) }

// Subscribe adds an unbuffered subscription.
func (f *Fanout[T]) Subscribe() (*Subscription[T], error) {
	return f.SubscribeBuffered(0)
}

// SubscribeBuffered adds a subscription with a bounded queue. A capacity of
// zero has rendezvous semantics. Negative capacities are rejected.
func (f *Fanout[T]) SubscribeBuffered(capacity int) (*Subscription[T], error) {
	if capacity < 0 {
		return nil, ErrFanoutNegativeBuffer
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrFanoutClosed
	}

	n := &fanoutNode[T]{
		hub: f, ch: make(chan T, capacity), stop: make(chan struct{}), linked: true,
	}
	s := &Subscription[T]{ch: n.ch}
	s.node.Store(n)

	oldHead := f.head.Load()
	n.next.Store(oldHead)
	if oldHead != nil {
		oldHead.prev = n
	}
	f.head.Store(n)
	f.count.Add(1)
	return s, nil
}

// C returns the receive-only channel. A nil Subscription returns nil, which
// is disabled in a select statement.
func (s *Subscription[T]) C() <-chan T {
	if s == nil {
		return nil
	}
	return s.ch
}

// Close prevents new sends, asks a blocked in-flight send to abort, unlinks
// the node in O(1), and waits until C is closed. A send whose channel case is
// already ready may still complete before Close returns. Values already
// queued in a buffered channel remain readable after closure. Concurrent and
// repeated calls wait for the same close operation. A nil receiver is a no-op.
func (s *Subscription[T]) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		n := s.node.Swap(nil)
		if n == nil {
			return
		}
		n.deactivate()
		n.hub.unlink(n)
		n.waitFinished()
	})
}

func (s *Subscription[T]) Unsubscribe() { s.Close() }

// Broadcast sends value once to every subscription it can acquire and returns
// the number of successful hand-offs or buffered enqueues. It visits
// subscribers newest-first. A subscription added after the traversal's head
// snapshot is excluded. A concurrent Close either prevents acquisition or
// causes a blocked send to abort. No per-publication snapshot or allocation is
// used.
func (f *Fanout[T]) Broadcast(value T) (delivered int) {
	if f.head.Load() == nil {
		return 0
	}
	f.sendMu.Lock()
	defer f.sendMu.Unlock()

	for n := f.head.Load(); n != nil; {
		next := n.next.Load() // capture before a potentially blocking send
		if n.acquire() {
			// The first select keeps the immediately-ready path small. Only pay
			// for the stop-aware select when the receiver/queue is not ready.
			select {
			case n.ch <- value:
				delivered++
			default:
				select {
				case n.ch <- value:
					delivered++
				case <-n.stop:
				}
			}
			n.release()
		}
		n = next
	}
	return delivered
}

// BroadcastContext is Broadcast with context cancellation while waiting for an
// individual subscriber. It returns the number already delivered; delivery is
// not rolled back. A nil context panics. As with Broadcast, an immediately
// ready send may win a race with cancellation.
func (f *Fanout[T]) BroadcastContext(ctx context.Context, value T) (delivered int, err error) {
	if ctx == nil {
		panic("bigbuff: fanout: nil context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.head.Load() == nil {
		return 0, nil
	}
	done := ctx.Done()
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	for n := f.head.Load(); n != nil; {
		next := n.next.Load()
		if n.acquire() {
			select {
			case n.ch <- value:
				delivered++
			default:
				select {
				case n.ch <- value:
					delivered++
				case <-n.stop:
				case <-done:
					n.release()
					return delivered, ctx.Err()
				}
			}
			n.release()
		}
		n = next
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
	}
	return delivered, ctx.Err()
}

// SubscriberCount returns the point-in-time number of linked subscriptions.
// It can include a subscription whose Close has started but not unlinked yet.
func (f *Fanout[T]) SubscriberCount() int { return int(f.count.Load()) }

// Close stops new subscriptions, cancels blocked sends, closes all receive
// channels, and waits for the active publication to quiesce. It is idempotent.
func (f *Fanout[T]) Close() {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		head := f.head.Load()
		f.head.Store(nil)
		f.count.Store(0)
		for n := head; n != nil; n = n.next.Load() {
			n.prev = nil
			n.linked = false
			n.deactivate()
		}
		f.mu.Unlock()

		// stop wakes any publisher blocked on a subscriber. Taking sendMu is
		// then a quiescence barrier; only after it can links be reclaimed.
		f.sendMu.Lock()
		for n := head; n != nil; {
			n.waitFinished()
			next := n.next.Load()
			n.next.Store(nil)
			n = next
		}
		f.sendMu.Unlock()
	})
}

func (f *Fanout[T]) unlink(n *fanoutNode[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !n.linked {
		return
	}
	prev, next := n.prev, n.next.Load()
	if prev == nil {
		f.head.Store(next)
	} else {
		prev.next.Store(next)
	}
	if next != nil {
		next.prev = prev
	}
	n.prev = nil
	n.linked = false
	f.count.Add(-1)
}

func (n *fanoutNode[T]) acquire() bool {
	for {
		state := n.state.Load()
		if state&fanoutClosedBit != 0 {
			return false
		}
		if state&fanoutActiveBit != 0 {
			panic("bigbuff: fanout: duplicate delivery lease")
		}
		if n.state.CompareAndSwap(state, state|fanoutActiveBit) {
			return true
		}
	}
}

func (n *fanoutNode[T]) release() {
	if state := n.state.Add(^uint32(0)); state == fanoutClosedBit {
		n.finish()
	}
}

func (n *fanoutNode[T]) deactivate() {
	for {
		state := n.state.Load()
		if state&fanoutClosedBit != 0 {
			return
		}
		if n.state.CompareAndSwap(state, state|fanoutClosedBit) {
			close(n.stop)
			if state&fanoutActiveBit == 0 {
				n.finish()
			}
			return
		}
	}
}

func (n *fanoutNode[T]) finish() {
	close(n.ch)
	n.doneMu.Lock()
	n.finished = true
	if n.done != nil {
		close(n.done)
	}
	n.doneMu.Unlock()
}

func (n *fanoutNode[T]) waitFinished() {
	n.doneMu.Lock()
	if n.finished {
		n.doneMu.Unlock()
		return
	}
	done := n.done
	if done == nil {
		done = make(chan struct{})
		n.done = done
	}
	n.doneMu.Unlock()
	<-done
}

````
