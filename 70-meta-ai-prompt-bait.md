Horizon is a lossy 1:N broadcast ring where publish stays O(1) no matter how many subscribers you have, and slow consumers never stall publishers.

## What you get

Copy these into `/Users/joeyc/dev/go-bigbuff`, alongside your existing `chancaster.go` and `chanpubsub.go`, which stay untouched:

- horizon.go : Horizon + Cursor, publish, subscribe, wait, close
- horizon_slot_fast.go : seqlock slot read/write, `//go:build !race`
- horizon_slot_race.go : same seq protocol plus per-slot mutex, `//go:build race`
- doc.go : thesis and correctness sketch
- horizon_test.go : stress tests and benchmarks vs baselines
- README.md : scenario, API, and verification

The `go.mod` in the same folder is only for standalone checking. Do not overwrite your repo's existing `go.mod` with it.

## Scenario it dominates

**Envelope:** M producers, N consumers where N can be 1 to 1000s, broadcast total order, lossy under backpressure, consumers join any time, catch up from oldest retained, or jump to latest. Publishers never block on consumers. Hot path is poll, blocking is only for idle consumers.

**Semantics:** Every cursor sees every message, not load-balanced competing consumption. If a cursor lags by more than capacity, it skips to the oldest retained message. `Lag()` and `Missed()` expose how far behind it is. If you need at-least-once delivery to a slow consumer, use a bounded queue with backpressure, not Horizon.

## Why chan and Mutex lose here

| Primitive | Publish cost | Publisher contention | Slow consumer |
| --- | --- | --- | --- |
| `chan` fanout | O(N) Sends, one lock per chan | Locks N chans, wakes N waiters | Blocks or forces lossy `select default` per chan |
| `Mutex` + copy per subscriber | O(N) copies, one lock | Serialises all publishers on one mutex | Holds lock while copying to slow subscribers |
| `ChanCaster` / `ChanPubSub` in this package | O(N) Sends + pong round-trip | Same O(N) chan work | Same O(N) cost |
| **Horizon** | **O(1), one Add + one Store + one seq Store** | **One atomic Add, no lock** | **Skipped, never blocks publisher** |

## Design

1. **Fetch-add claim, not CAS.** `tail.Add(1)` is one hardware RMW that always makes progress. A CAS loop on one tail under M producers is a retry storm of re-read, re-compute, re-attempt.
2. **Per-slot seqlock.** Publish is plain store of `V` plus seq Store. Read is seq Load, copy `V`, seq Load again. The double-check discards any read torn by a concurrent overwrite, which is what makes lossy overwrite safe without waiting for readers.
3. **Cache-line isolation.** `seq` alone on a line, `val` padded so the next slot's `seq` can never share `val`'s line. `tail`, `waiters`, and `closed` are each padded onto their own lines. A publish invalidates exactly its own slot's lines, not a neighbour's.
4. **Cursor ownership.** No shared consumer counter, no registration. The Horizon never iterates consumers, never copies per consumer, never touches consumer state. Each `Cursor` owns its own `next` seq and is single-goroutine owned.
5. **Wakeup coalescing.** `waiters` Load gates notify. Only the first concurrent publisher that wins `TryLock` broadcasts. The rest skip, because their seq Store already happened, so the winner's broadcast covers them. Publishers never block on the wakeup lock.
6. **Batching.** `PublishBatch` claims N slots with one Add and wakes once.
7. **GC sympathy.** Slots are a slice of structs with a real `V` field, so the collector scans payload pointers. No `[]byte` arena hiding pointers, no per-message heap box, no per-subscriber channel. Hot path is `0 B/op, 0 allocs/op`.

Capacity must be a power of two, so seq to slot is one AND, `seq & mask`, not a division.

## API

```go
h := bigbuff.NewHorizon[Event](1024)
seq, ok := h.Publish(ev)
first, ok := h.PublishBatch(batch)

c := h.Subscribe()           // only future messages
c := h.SubscribeFromOldest() // replay retained history
c := h.SubscribeFrom(seq)    // clamped to [oldest, tail]

v, ok := c.Next()                 // non-blocking, 0 alloc
v, ok := c.WaitNext()             // blocks, Cond-parked
v, ok := c.WaitNextContext(ctx)   // blocks, ctx-cancellable
v, seq, ok := h.Latest()          // wait-free latest, no cursor

for v := range c.All() { /* until Close */ }
for v := range c.AllContext(ctx) { /* until ctx or Close */ }

c.SkipToLatest()
c.Lag()
c.Missed()
h.Close() // idempotent, wakes all waiters
```

## Correctness

Total order follows claim order, one publisher per seq per cycle. A reader accepts only if `slot.seq == seq+1` before and after copying `val`. Overwrite during copy changes seq to `seq+capacity+1`, so the read is discarded. Claim without publish shows the previous cycle's seq, so the read returns false instead of stale data. Cursor advances by exactly one per success, jumping forward only over provably overwritten seqs. No duplicates, no reordering, no torn values.

The Cond protocol is the standard one: waiter re-checks `tail`/`closed` under the Cond lock before parking, publisher broadcasts under the same lock or skips because another publisher is broadcasting or the waiter will see the data itself. Publish is visible before notify, so no wakeup is lost.

The `-race` build keeps the identical seq protocol and adds a per-slot mutex around `val` only, so ThreadSanitizer sees a happens-before edge it can understand. Same observable semantics, slower only under `-race`.

## Verification

I wrote the code with the race fallback and the full test suite, but this sandbox has no Go toolchain, so run it locally:

```bash
go test -race -count=1 ./...
go test -bench=. -benchmem ./...
go vet ./...
```

Tests cover bad capacity panics, basic publish/subscribe, broadcast where every cursor sees the identical stream, no loss when caught up, lag skip with no torn or duplicate (using an `X == Y` payload where a torn read would show `X != Y`), batch order, `Latest`, `WaitNext` wakeup, `Close` wakeup, context cancel, `SubscribeFrom` clamping, publish after close, `All` / `AllContext` iterators, zero allocs via `testing.AllocsPerRun`, and an 8-producer / 8-consumer adversarial test.

Benchmarks compare Horizon fanout at 1, 16, and 128 against chan fanout at the same sizes, plus publish-only, parallel publish, batch 64, `Latest`, and mutex watch baselines. Expect Horizon fanout to stay roughly flat in N, while chan fanout grows linearly in N in time and lock/wakeup work.

If you want a next step, the natural follow-up is a sharded variant for key-partitioned fanout, or a `PublishBatch` path that uses non-temporal stores for very large payloads.

---------

````txtar
-- README.md --
# bigbuff — Horizon: O(1) 1:N Broadcast Ring

## The bet

`chan` fanout is O(N) per publish. `Mutex + copy-per-subscriber`
is O(N) copies per publish plus one lock. Both make publish cost
grow with the number of subscribers, and both ping-pong the
publisher's cache line across every subscriber's chan / buffer.

Horizon makes publish O(1) in N:

```
Publish(v):
  seq = tail.Add(1) - 1   // one LOCK XADD, no CAS retry storm
  slot[seq & mask].val = v
  slot[seq & mask].seq.Store(seq+1)  // publication point
  if waiters > 0: TryLock + Broadcast  // coalesced, never blocking
```

Consumers never register. Each `Cursor` owns its own `next`
sequence number. The Horizon never iterates consumers, never
copies per consumer, never touches consumer state. A consumer
reads its slot with a seqlock double-check (`seq Load, copy val,
seq Load`), so a concurrent overwrite is detectable and
discardable instead of torn.

## Scenario

- M producers, N consumers, N = 1..1000s
- Broadcast total order: every cursor sees every message, not
  load-balanced competing consumption
- Lossy under backpressure: a lagging cursor skips, it never
  stalls a publisher
- Consumers join any time, catch up from oldest retained, or
  jump to latest
- Hot path is poll (`Next`, `Latest`); blocking (`WaitNext`)
  is only for idle consumers

If you need at-least-once delivery to a slow consumer, use a
bounded queue with backpressure, not Horizon. If you need
competing consumption, shard by key across Horizons.

## API

```go
h := bigbuff.NewHorizon[Event](1024) // capacity must be power of two
seq, ok := h.Publish(ev)
first, ok := h.PublishBatch(batch)    // one Add + one wakeup for N

c := h.Subscribe()                    // only future messages
c := h.SubscribeFromOldest()          // replay retained history
c := h.SubscribeFrom(seq)             // clamped to [oldest, tail]

v, ok := c.Next()                     // non-blocking, 0 alloc
v, ok := c.WaitNext()                 // blocks, Cond-parked
v, ok := c.WaitNextContext(ctx)       // blocks, ctx-cancellable
v, seq, ok := h.Latest()              // wait-free latest, no cursor

for v := range c.All() { /* until Close */ }
for v := range c.AllContext(ctx) { /* until ctx / Close */ }

c.SkipToLatest()
c.Lag()    // published - read
c.Missed() // lost to overwrite

h.Close()  // idempotent, wakes all waiters
```

## Mechanical sympathy

1. **Fetch-add claim, not CAS.** `tail.Add(1)` is one hardware
   RMW that always makes progress. A CAS loop on one tail under
   M producers is a retry storm.
2. **Per-slot seqlock.** No lock, no allocation, no per-subscriber
   copy on publish or poll.
3. **Cache-line isolation.** `seq` alone on a line, `val` padded
   so the next slot's `seq` can never share `val`'s line. `tail`,
   `waiters`, and `closed` are each padded onto their own lines:
   three hot atomics written by different actors must not
   ping-pong on one line.
4. **Cursor ownership.** No shared consumer counter, no shared
   head, no registration. Publish never touches consumer state.
5. **Wakeup coalescing.** `waiters` Load gates notify. Only the
   first concurrent publisher that wins `TryLock` broadcasts.
   The rest skip: their `seq` Store already happened, so the
   winner's broadcast covers them. Publishers never block on
   the wakeup lock.
6. **Batching.** `PublishBatch` claims N slots with one Add and
   wakes once.
7. **GC sympathy.** Slots are a slice of structs with a real `V`
   field, so the collector scans payload pointers. No `[]byte`
   arena hiding pointers, no per-message heap box, no
   per-subscriber channel. Hot path is `0 B/op, 0 allocs/op`.

## Correctness

- Total order = claim order. One publisher per seq per cycle.
- Reader accepts only if `slot.seq == seq+1` before *and* after
  copying `val`. Overwrite during copy changes seq to
  `seq+capacity+1`, so the read is discarded. Claim-without-
  publish shows the previous cycle's seq, so the read returns
  false instead of stale data.
- Cursor advances by exactly one per successful read, jumping
  forward only over provably overwritten seqs. No duplicates,
  no reordering, no torn values (tested with `X==Y` payloads
  under concurrent overwrite).
- Cond protocol: waiter re-checks `tail`/`closed` under the
  Cond lock before parking; publisher broadcasts under the
  same lock (or skips because another publisher is broadcasting
  or the waiter will see the data itself). No wakeup is lost:
  publish is visible before notify, and a waiter that has not
  yet parked still holds the Cond lock.
- `-race` build: same seq protocol, plus a per-slot mutex
  around `val` only, so ThreadSanitizer sees a happens-before
  edge for `val`. Files: `horizon_slot_fast.go`
  (`//go:build !race`) and `horizon_slot_race.go`
  (`//go:build race`). Same observable semantics.

## Verification

```bash
go test -race -count=1 ./...
go test -bench=. -benchmem ./...
go vet ./...
```

Tests cover: bad capacity panics, basic publish/subscribe,
broadcast (every cursor sees the identical stream), no-loss
when caught up, lag skip with no torn/duplicate, batch order,
`Latest`, `WaitNext` wakeup, `Close` wakeup, context cancel,
`SubscribeFrom` clamping, `Publish` after `Close`, `All` /
`AllContext` iterators, zero allocs (`testing.AllocsPerRun`),
and an 8-producer / 8-consumer adversarial test with
`torn = X != Y` detection.

Benchmarks compare Horizon against idiomatic baselines at
fanout 1 / 16 / 128:

- `BenchmarkHorizon_Publish_NoConsumer`
- `BenchmarkHorizon_Publish_Parallel`
- `BenchmarkHorizon_PublishBatch_64`
- `BenchmarkHorizon_Fanout_{1,16,128}` — publish + one poll
  per cursor, same goroutine, deterministic
- `BenchmarkChan_Fanout_{1,16,128}` — O(N) Sends per publish
- `BenchmarkChan_SendRecv`, `BenchmarkMutex_Watch_Publish`

Expectation: Horizon fanout cost is ~flat in N (one publish +
one cheap poll per cursor), while chan fanout grows linearly
in N in both time and lock / wakeup work. Publish with no
consumer is one Add + one Store + one seq Store.

## Files

- `doc.go` — full thesis + correctness sketch
- `horizon.go` — Horizon + Cursor, publish / subscribe / wait / close
- `horizon_slot_fast.go` — seqlock, `!race`
- `horizon_slot_race.go` — seqlock + per-slot mutex, `race`
- `horizon_test.go` — stress + benchmarks vs baselines

## Relation to existing code in this package

`ChanCaster` / `ChanPubSub` are the O(N) baseline: they Send
once per receiver on an unbuffered channel. Horizon is the
replacement for the 1:N broadcast envelope where N is large
or publish frequency is high and lossy semantics are
acceptable. Keep `ChanPubSub` when you need exact delivery to
every subscriber and are willing to pay O(N) Sends + a pong
round-trip per message to prove it.
-- horizon_test.go --
package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testPayload has two copies of the same value. A torn read would
// show X != Y, because an overwriting publisher writes a
// different i while the reader is mid-copy.
type testPayload struct {
	X int64
	Y int64
}

func TestNewHorizon_PanicsOnBadCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1, 3, 6, 100, 1023} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewHorizon(%d): expected panic", capacity)
				}
			}()
			_ = NewHorizon[int64](capacity)
		}()
	}
	for _, capacity := range []int{1, 2, 4, 16, 1024} {
		if h := NewHorizon[int64](capacity); h.Cap() != capacity {
			t.Fatalf("Cap() = %d, want %d", h.Cap(), capacity)
		}
	}
}

func TestHorizon_Basic_PublishSubscribe(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	c := h.Subscribe()
	if _, ok := c.Next(); ok {
		t.Fatal("Next on empty horizon should return false")
	}
	for i := int64(0); i < 10; i++ {
		seq, ok := h.Publish(i)
		if !ok || seq != uint64(i) {
			t.Fatalf("Publish(%d) = (%d,%v), want (%d,true)", i, seq, ok, i)
		}
		v, ok := c.Next()
		if !ok || v != i {
			t.Fatalf("Next() = (%d,%v), want (%d,true)", v, ok, i)
		}
	}
	if _, ok := c.Next(); ok {
		t.Fatal("Next past end should return false")
	}
}

func TestHorizon_Broadcast_EverySubscriberSeesSame(t *testing.T) {
	h := NewHorizon[int64](1024)
	defer h.Close()
	for i := int64(0); i < 100; i++ {
		if _, ok := h.Publish(i); !ok {
			t.Fatal("publish failed")
		}
	}
	// Three cursors from oldest must each see the identical stream.
	// This is broadcast, not competing consumption.
	for k := 0; k < 3; k++ {
		c := h.SubscribeFromOldest()
		for i := int64(0); i < 100; i++ {
			v, ok := c.Next()
			if !ok || v != i {
				t.Fatalf("cursor %d: Next() = (%d,%v), want (%d,true)", k, v, ok, i)
			}
		}
	}
}

func TestHorizon_Order_NoLoss_WhenCaughtUp(t *testing.T) {
	h := NewHorizon[int64](64)
	defer h.Close()
	c := h.Subscribe()
	var want int64
	for i := int64(0); i < 1000; i++ {
		if _, ok := h.Publish(i); !ok {
			t.Fatal("publish failed")
		}
		v, ok := c.Next()
		if !ok {
			t.Fatalf("lost message %d: Next returned false while caught up", i)
		}
		if v != want {
			t.Fatalf("Next() = %d, want %d (out of order / duplicate)", v, want)
		}
		want++
	}
}

func TestHorizon_LagSkipsButNeverTornOrDuplicate(t *testing.T) {
	h := NewHorizon[testPayload](16)
	defer h.Close()
	c := h.SubscribeFromOldest()
	// Publish far more than capacity without reading: the cursor
	// must skip, never see a torn payload, never see a duplicate.
	const n = 10000
	for i := int64(0); i < n; i++ {
		if _, ok := h.Publish(testPayload{X: i, Y: i}); !ok {
			t.Fatal("publish failed")
		}
	}
	var prev int64 = -1
	seen := 0
	for {
		p, ok := c.Next()
		if !ok {
			break
		}
		if p.X != p.Y {
			t.Fatalf("torn read: X=%d Y=%d", p.X, p.Y)
		}
		if p.X <= prev {
			t.Fatalf("duplicate / reorder: got %d after %d", p.X, prev)
		}
		prev = p.X
		seen++
	}
	// At most capacity messages can survive; at least 1 must.
	if seen == 0 || seen > 16 {
		t.Fatalf("seen = %d, want 1..16 after lagging by %d over cap 16", seen, n)
	}
	// The last message must be the newest one.
	if prev != n-1 {
		t.Fatalf("last seen = %d, want %d (newest must survive)", prev, n-1)
	}
	if c.Missed() == 0 {
		t.Fatal("Missed() should be > 0 after lagging")
	}
}

func TestHorizon_PublishBatch_Order(t *testing.T) {
	h := NewHorizon[int64](64)
	defer h.Close()
	c := h.Subscribe()
	batch := []int64{10, 20, 30, 40}
	first, ok := h.PublishBatch(batch)
	if !ok || first != 0 {
		t.Fatalf("PublishBatch = (%d,%v), want (0,true)", first, ok)
	}
	for _, want := range batch {
		v, ok := c.Next()
		if !ok || v != want {
			t.Fatalf("Next() = (%d,%v), want (%d,true)", v, ok, want)
		}
	}
	if _, ok := h.PublishBatch(nil); !ok {
		t.Fatal("empty batch should succeed")
	}
}

func TestHorizon_Latest(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	if _, _, ok := h.Latest(); ok {
		t.Fatal("Latest on empty should return false")
	}
	for i := int64(0); i < 5; i++ {
		if _, ok := h.Publish(i); !ok {
			t.Fatal("publish failed")
		}
	}
	v, seq, ok := h.Latest()
	if !ok || v != 4 || seq != 4 {
		t.Fatalf("Latest() = (%d,%d,%v), want (4,4,true)", v, seq, ok)
	}
	// Latest does not consume: a cursor from oldest still sees all.
	c := h.SubscribeFromOldest()
	for i := int64(0); i < 5; i++ {
		if v, ok := c.Next(); !ok || v != i {
			t.Fatalf("cursor after Latest: Next() = (%d,%v), want (%d,true)", v, ok, i)
		}
	}
}

func TestHorizon_WaitNext_Wakes(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	c := h.Subscribe()
	done := make(chan int64, 1)
	go func() {
		v, ok := c.WaitNext()
		if !ok {
			done <- -1
			return
		}
		done <- v
	}()
	// Give the waiter time to park, then publish.
	time.Sleep(50 * time.Millisecond)
	if _, ok := h.Publish(42); !ok {
		t.Fatal("publish failed")
	}
	select {
	case v := <-done:
		if v != 42 {
			t.Fatalf("WaitNext got %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitNext did not wake")
	}
}

func TestHorizon_WaitNext_CloseWakes(t *testing.T) {
	h := NewHorizon[int64](16)
	c := h.Subscribe()
	done := make(chan bool, 1)
	go func() {
		_, ok := c.WaitNext()
		done <- ok
	}()
	time.Sleep(50 * time.Millisecond)
	h.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("WaitNext after Close should return false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitNext did not wake on Close")
	}
	// Close is idempotent.
	h.Close()
}

func TestHorizon_WaitNextContext_Cancel(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	c := h.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		_, ok := c.WaitNextContext(ctx)
		done <- ok
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("WaitNextContext after cancel should return false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitNextContext did not wake on cancel")
	}
}

func TestHorizon_WaitNextContext_DataWins(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	c := h.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Publish while the waiter is parked.
		time.Sleep(20 * time.Millisecond)
		h.Publish(7)
	}()
	v, ok := c.WaitNextContext(ctx)
	if !ok || v != 7 {
		t.Fatalf("WaitNextContext = (%d,%v), want (7,true)", v, ok)
	}
}

func TestHorizon_SubscribeFrom_Clamping(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	for i := int64(0); i < 100; i++ {
		h.Publish(i)
	}
	// seq 0 is long overwritten: clamped to oldest retained (84).
	c := h.SubscribeFrom(0)
	if c.Seq() != 84 {
		t.Fatalf("SubscribeFrom(0).Seq() = %d, want 84", c.Seq())
	}
	// Future seq clamped to tail.
	c2 := h.SubscribeFrom(10000)
	if c2.Seq() != 100 {
		t.Fatalf("SubscribeFrom(future).Seq() = %d, want 100", c2.Seq())
	}
	// In-range seq preserved.
	c3 := h.SubscribeFrom(90)
	if c3.Seq() != 90 {
		t.Fatalf("SubscribeFrom(90).Seq() = %d, want 90", c3.Seq())
	}
}

func TestHorizon_Close_PublishAfterClose(t *testing.T) {
	h := NewHorizon[int64](16)
	h.Close()
	if _, ok := h.Publish(1); ok {
		t.Fatal("Publish after Close should return false")
	}
	if _, ok := h.PublishBatch([]int64{1, 2}); ok {
		t.Fatal("PublishBatch after Close should return false")
	}
}

func TestHorizon_All_Iterator(t *testing.T) {
	h := NewHorizon[int64](16)
	c := h.Subscribe()
	go func() {
		for i := int64(0); i < 5; i++ {
			h.Publish(i)
			time.Sleep(5 * time.Millisecond)
		}
		h.Close()
	}()
	var got []int64
	for v := range c.All() {
		got = append(got, v)
	}
	if len(got) != 5 {
		t.Fatalf("All() yielded %v, want 0..4", got)
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("All()[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestHorizon_AllContext_Iterator(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	c := h.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		h.Publish(99)
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	var got []int64
	for v := range c.AllContext(ctx) {
		got = append(got, v)
	}
	if len(got) != 1 || got[0] != 99 {
		t.Fatalf("AllContext yielded %v, want [99]", got)
	}
}

func TestHorizon_ZeroAllocs(t *testing.T) {
	h := NewHorizon[int64](1024)
	defer h.Close()
	c := h.Subscribe()
	allocs := testing.AllocsPerRun(100, func() {
		h.Publish(1)
		c.Next()
	})
	if allocs != 0 {
		t.Fatalf("Publish+Next allocs = %v, want 0", allocs)
	}
	allocs = testing.AllocsPerRun(100, func() {
		h.Latest()
	})
	if allocs != 0 {
		t.Fatalf("Latest allocs = %v, want 0", allocs)
	}
	// PublishBatch of a reused slice must also be allocation-free.
	batch := []int64{1, 2, 3, 4}
	allocs = testing.AllocsPerRun(100, func() {
		h.PublishBatch(batch)
	})
	if allocs != 0 {
		t.Fatalf("PublishBatch allocs = %v, want 0", allocs)
	}
}

// TestHorizon_Concurrent is the adversarial test: M producers
// hammering one tail, N consumers each with their own cursor,
// all polling. No duplicate / reorder / torn per cursor, no
// deadlock, no race (run with -race).
func TestHorizon_Concurrent(t *testing.T) {
	const (
		capacity   = 1024
		producers  = 8
		consumers  = 8
		perProducer = 5000
	)
	h := NewHorizon[testPayload](capacity)
	defer h.Close()

	var wg sync.WaitGroup
	var published atomic.Int64

	// Consumers start before producers so they can try to keep up.
	// They stop when they have seen the newest seq and Close has
	// been called, via polling + Lag check in the main goroutine
	// below. To avoid a flaky "consumer never finishes" hang, each
	// consumer runs until producers are done and it has drained to
	// within capacity of the tail, then it exits; the main test
	// verifies per-cursor invariants, not full delivery (lossy).
	doneProducing := make(chan struct{})
	consumerErr := make(chan error, consumers)
	for ci := 0; ci < consumers; ci++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := h.SubscribeFromOldest()
			var prev int64 = -1
			for {
				p, ok := c.Next()
				if !ok {
					select {
					case <-doneProducing:
						// Producers done. If we are within
						// capacity of tail, the remaining
						// messages are the newest ones and we
						// have already verified them on the way;
						// drain what is left, then exit.
						for {
							p2, ok2 := c.Next()
							if !ok2 {
								return
							}
							if p2.X != p2.Y {
								consumerErr <- fmt.Errorf("torn: %+v", p2)
								return
							}
							if p2.X <= prev {
								consumerErr <- fmt.Errorf("duplicate/reorder: %d after %d", p2.X, prev)
								return
							}
							prev = p2.X
						}
					default:
						// Not done yet, poll again. Yield so
						// producers get CPU.
						time.Sleep(time.Microsecond)
						continue
					}
				}
				if p.X != p.Y {
					consumerErr <- fmt.Errorf("torn: %+v", p)
					return
				}
				if p.X <= prev {
					consumerErr <- fmt.Errorf("duplicate/reorder: %d after %d", p.X, prev)
					return
				}
				prev = p.X
			}
		}()
	}

	for pi := 0; pi < producers; pi++ {
		wg.Add(1)
		go func(pi int) {
			defer wg.Done()
			base := int64(pi * perProducer)
			for i := int64(0); i < perProducer; i++ {
				v := base + i
				if _, ok := h.Publish(testPayload{X: v, Y: v}); !ok {
					consumerErr <- fmt.Errorf("publish failed")
					return
				}
				published.Add(1)
			}
		}(pi)
	}

	// Wait for producers by polling published count in a helper:
	// producers are in wg, but consumers are also in wg, so we
	// cannot wg.Wait() here. Instead wait for the count, then
	// close the done channel.
	go func() {
		for published.Load() < producers*perProducer {
			time.Sleep(time.Millisecond)
		}
		close(doneProducing)
	}()

	// Wait with a timeout so a deadlock fails fast, not via the
	// test timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent test timed out (deadlock / livelock)")
	}
	select {
	case err := <-consumerErr:
		t.Fatal(err)
	default:
	}
	if got := h.Tail(); got != uint64(producers*perProducer) {
		t.Fatalf("Tail() = %d, want %d", got, producers*perProducer)
	}
}

func TestHorizon_SkipToLatest(t *testing.T) {
	h := NewHorizon[int64](16)
	defer h.Close()
	for i := int64(0); i < 10; i++ {
		h.Publish(i)
	}
	c := h.SubscribeFromOldest()
	if c.Lag() != 10 {
		t.Fatalf("Lag() = %d, want 10", c.Lag())
	}
	c.SkipToLatest()
	if c.Lag() != 0 {
		t.Fatalf("Lag after SkipToLatest = %d, want 0", c.Lag())
	}
	h.Publish(10)
	v, ok := c.Next()
	if !ok || v != 10 {
		t.Fatalf("Next after SkipToLatest = (%d,%v), want (10,true)", v, ok)
	}
}

// ---- Benchmarks: Horizon vs idiomatic baselines ----

func BenchmarkHorizon_Publish_NoConsumer(b *testing.B) {
	h := NewHorizon[int64](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Publish(int64(i))
	}
}

func BenchmarkHorizon_Publish_Parallel(b *testing.B) {
	h := NewHorizon[int64](8192)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			h.Publish(i)
			i++
		}
	})
}

func BenchmarkHorizon_PublishBatch_64(b *testing.B) {
	h := NewHorizon[int64](8192)
	batch := make([]int64, 64)
	for i := range batch {
		batch[i] = int64(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.PublishBatch(batch)
	}
}

func BenchmarkHorizon_Latest(b *testing.B) {
	h := NewHorizon[int64](1024)
	h.Publish(42)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Latest()
	}
}

func BenchmarkHorizon_PublishConsume_1(b *testing.B) {
	h := NewHorizon[int64](1024)
	c := h.Subscribe()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Publish(int64(i))
		c.Next()
	}
}

func BenchmarkHorizon_Fanout_1(b *testing.B) { benchmarkHorizonFanout(b, 1) }
func BenchmarkHorizon_Fanout_16(b *testing.B) { benchmarkHorizonFanout(b, 16) }
func BenchmarkHorizon_Fanout_128(b *testing.B) { benchmarkHorizonFanout(b, 128) }

func benchmarkHorizonFanout(b *testing.B, n int) {
	h := NewHorizon[int64](4096)
	cursors := make([]*Cursor[int64], n)
	for i := range cursors {
		cursors[i] = h.Subscribe()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Publish(int64(i))
		for _, c := range cursors {
			c.Next()
		}
	}
}

// Baseline: single chan send/recv.
func BenchmarkChan_SendRecv(b *testing.B) {
	ch := make(chan int64, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- int64(i)
		<-ch
	}
}

// Baseline: chan fanout. Publisher must Send once per subscriber,
// non-blocking (lossy, like Horizon). This is the O(N) cost
// Horizon avoids: compare Fanout_16 / Fanout_128 against this.
func BenchmarkChan_Fanout_1(b *testing.B)   { benchmarkChanFanout(b, 1) }
func BenchmarkChan_Fanout_16(b *testing.B)  { benchmarkChanFanout(b, 16) }
func BenchmarkChan_Fanout_128(b *testing.B) { benchmarkChanFanout(b, 128) }

func benchmarkChanFanout(b *testing.B, n int) {
	chans := make([]chan int64, n)
	for i := range chans {
		chans[i] = make(chan int64, 1024)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := int64(i)
		for _, ch := range chans {
			select {
			case ch <- v:
			default:
			}
		}
		for _, ch := range chans {
			select {
			case <-ch:
			default:
			}
		}
	}
}

// Baseline: Mutex-protected latest value + Cond broadcast. The
// classic "watch" pattern. Publisher serialises on one mutex.
func BenchmarkMutex_Watch_Publish(b *testing.B) {
	var mu sync.Mutex
	var val int64
	cond := sync.NewCond(&mu)
	_ = cond
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		val = int64(i)
		mu.Unlock()
	}
}
-- doc.go --
// Package bigbuff provides genuinely specialised concurrent
// primitives that out-specialise the Go runtime for narrow,
// well-defined envelopes, instead of re-implementing general
// purpose channels behind a mutex.
//
// This file states the thesis for Horizon, the flagship primitive
// in this package. The existing ChanCaster / ChanPubSub in this
// package are the O(N) baseline Horizon replaces for the 1:N
// broadcast envelope: they Send once per receiver on an unbuffered
// channel, so publish cost, lock contention, and wakeup work all
// grow with N. Horizon publish is O(1) in N.
//
// Scenario Horizon dominates:
//
//	M producers, N consumers (N = 1..1000s), broadcast total order,
//	lossy under backpressure, consumers join / lag / catch up /
//	skip to latest, publishers never block on consumers.
//
// Mechanical sympathy thesis, in order of impact:
//
//  1. One atomic Add to claim, not a CAS loop. Claiming a slot
//     via tail.Add(1) is a single hardware RMW. A CAS loop on one
//     tail under M producers is a retry storm: every loser
//     re-reads, re-computes, and re-attempts, burning interconnect
//     bandwidth and widening the tail latency. Add always makes
//     progress.
//  2. Per-slot seqlock publication. Publish is plain-store val +
//     seq Store. Read is seq Load, copy val, seq Load again.
//     No lock, no allocation, no per-subscriber copy. The seq
//     double-check makes torn reads from a concurrent overwrite
//     detectable and discardable, which is what makes lossy
//     overwrite safe without blocking the publisher to wait for
//     readers.
//  3. Cache-line isolation. seq alone on a line, val padded so
//     the next slot's seq can never share val's line. A publish
//     invalidates exactly one seq line + one val line for its own
//     slot, not a neighbour's. The tail counter is padded away
//     from the waiter counter and the closed flag for the same
//     reason: three hot atomics that are written by different
//     actors must not ping-pong on one line.
//  4. Consumers own their cursor. No shared consumer counter, no
//     shared queue head, no per-consumer registration in the
//     Horizon. Publish never iterates consumers, never copies per
//     consumer, never touches consumer state at all.
//  5. Wakeup coalescing via TryLock. Parked waiters are woken
//     with sync.Cond Broadcast, but only if a waiter is actually
//     parked (waiters Load fast path), and only the first
//     concurrent publisher that wins TryLock broadcasts. The rest
//     skip: their seq is already visible, so the winner's
//     broadcast covers them. Publishers never block on the wakeup
//     lock.
//  6. Batching amortises the last two costs. PublishBatch claims
//     N slots with one Add and wakes once.
//  7. GC sympathy. Slots are a slice of structs with a real V
//     field, so the collector scans payload pointers. No []byte
//     arena hiding pointers, no per-message heap box, no
//     per-subscriber channel allocation. Hot path is 0 B/op,
//     0 allocs/op.
//
// What Horizon deliberately does NOT do:
//
//   - It is not a queue: it does not guarantee delivery to a
//     lagging consumer. If you need at-least-once delivery to a
//     slow consumer, use a bounded queue with backpressure, not
//     Horizon.
//   - It is not a competing-consumer work queue: every cursor
//     sees every message. If you need load balancing, shard by
//     key across multiple Horizons or use a channel.
//   - Cursors are not goroutine-safe: one goroutine per cursor.
//   - It does not preserve a message that was overwritten before
//     a slow cursor read it. Lag() / Missed() expose that.
//
// Correctness argument (sketch):
//
//   Total order follows claim order. For a fixed seq, at most one
//   publisher ever writes that slot for that cycle, because seq
//   is claimed by exactly one Add. A reader for seq only accepts
//   a value if slot seq == seq+1 both before and after copying
//   val. If a later cycle (seq+capacity) overwrites the slot
//   during the copy, the second seq Load sees seq+capacity+1 !=
//   seq+1 and discards. If a publisher is between claim and seq
//   Store, the first seq Load sees the previous cycle's seq !=
//   seq+1 and returns false. Hence a reader never returns a torn
//   or stale value, never returns a value twice for the same seq
//   (cursor advances only on success), and never returns values
//   out of order (cursor advances by exactly one per success,
//   jumping forward only over provably overwritten seqs).
//
//   The Cond protocol is the standard one: waiter re-checks
//   tail/closed under the Cond lock before parking, publisher
//   broadcasts under the same lock (or skips because another
//   publisher is broadcasting or the waiter will see the data
//   itself). No wakeup is lost: a publish is visible before its
//   notify attempt, and a waiter that has not yet parked has not
//   yet released the Cond lock, so a publisher's broadcast
//   cannot slip between the waiter's check and its park.
//
//   The -race build adds a per-slot mutex around val only, to
//   give ThreadSanitizer a happens-before edge it can see. The
//   seq protocol is identical, so both builds have the same
//   observable semantics.
package bigbuff
-- horizon_slot_race.go --
//go:build race

package bigbuff

// storeSlot / loadSlot for -race: same seqlock protocol, but the
// val access is additionally serialised by the per-slot mutex in
// h.mus so ThreadSanitizer sees a mutex happens-before edge for
// the val field. The mutex is per-slot, not global, so -race
// correctness tests still exercise concurrent publishers and
// readers on different slots without serialising them. The seq
// double-check is retained so the two builds have identical
// observable semantics (lag / skip / torn detection).
//
// This file is the only difference between the two builds. The
// fast build is strictly faster (no mutex on the hot path) and
// the race build is strictly more conservative; both are
// linearisable under the same seq protocol.
func (h *Horizon[V]) storeSlot(seq uint64, v V) {
	idx := seq & h.mask
	mu := &h.mus[idx]
	mu.Lock()
	h.slots[idx].val = v
	mu.Unlock()
	h.slots[idx].seq.Store(seq + 1)
}

func (h *Horizon[V]) loadSlot(seq uint64, want uint64) (V, bool) {
	var zero V
	idx := seq & h.mask
	if h.slots[idx].seq.Load() != want {
		return zero, false
	}
	mu := &h.mus[idx]
	mu.Lock()
	v := h.slots[idx].val
	mu.Unlock()
	if h.slots[idx].seq.Load() != want {
		return zero, false
	}
	return v, true
}
-- horizon_slot_fast.go --
//go:build !race

package bigbuff

// storeSlot publishes v at seq.
//
// Fast path (!race): plain store of val, then a sequentially
// consistent seq Store. The seq Store is the publication point:
// a reader that observes seq == want via its seq Load is
// guaranteed (Go memory model, atomic synchronisation) to see
// the val store that happened before it. The reader's second
// seq Load detects a concurrent overwrite that tore the val read.
//
// This is an intentional seqlock. It is race-detector-clean in
// the logical sense, but ThreadSanitizer cannot infer the
// seqlock happens-before from the seq values alone, so the
// race build below uses a per-slot mutex instead.
func (h *Horizon[V]) storeSlot(seq uint64, v V) {
	slot := &h.slots[seq&h.mask]
	slot.val = v
	slot.seq.Store(seq + 1)
}

// loadSlot tries to read seq. want must be seq+1 for a
// published slot. Returns false on any mismatch, including a
// seq change between the two Loads (torn / overwritten read).
func (h *Horizon[V]) loadSlot(seq uint64, want uint64) (V, bool) {
	var zero V
	slot := &h.slots[seq&h.mask]
	if slot.seq.Load() != want {
		return zero, false
	}
	v := slot.val
	if slot.seq.Load() != want {
		return zero, false
	}
	return v, true
}
-- horizon.go --
package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// horizonSlot is one broadcast slot.
//
// Layout is deliberate, not incidental:
//
//   - seq lives alone on its own 64-byte cache line. The publisher's
//     seq Store is the only write to that line for this slot, and the
//     reader's seq Loads are the only reads. No neighbouring val or
//     seq shares the line, so a publish does not invalidate a reader's
//     line for a different slot.
//   - val lives on the following line(s), followed by 64 bytes of pad.
//     The 64-byte pad after val guarantees the next slot's seq can
//     never share a cache line with this slot's val, even for tiny V
//     (e.g. bool / uint16) where a smaller pad would still alias.
//     For the common case V <= 8 bytes the slot is 8+56+8+64 = 136
//     bytes: seq line, val line, pad. The cost is ~136 KiB for a
//     1024-slot ring, which is L2-resident on modern cores and is
//     paid once at construction, never per message.
//
// GC correctness: val is a real Go field in a real slice, so the GC
// scans it. We deliberately do NOT hide V in a []byte arena via
// unsafe: that would hide pointers (string, slice, *T, interface)
// from the collector and could free a payload while a slow reader
// still holds its slot.
type horizonSlot[V any] struct {
	seq     atomic.Uint64
	_padSeq [56]byte
	val     V
	_padVal [64]byte
}

// Horizon is a wait-free-publish, lossy, 1:N broadcast ring.
//
// Target scenario (the envelope it dominates):
//
//   - M producers, N consumers, N can be 1..1000s.
//   - Producers publish at very high frequency and must never block
//     on a slow consumer. A lagging consumer skips, it never stalls
//     the publisher.
//   - Every consumer wants the same total order (broadcast, not
//     competing consumption). A message is delivered to every cursor,
//     not load-balanced across cursors.
//   - Consumers join at any time, catch up from the oldest retained
//     message, or jump to latest. They poll in the hot path and
//     block only when idle.
//   - Payloads are values (typically small: int64, a small struct,
//     a pointer / string header). Copying V by value is the transfer.
//
// Why chan / Mutex / sync.Map lose here:
//
//   - chan fanout is O(N) per publish: the publisher must Send once
//     per subscriber, taking each chan's lock and waking each
//     waiter. Publish cost grows with N and the publisher's cache
//     line for every chan is ping-ponged.
//   - A Mutex + slice copy per subscriber is O(N) copies per publish
//     plus lock contention on the single mutex.
//   - sync.Map / sync.Cond broadcast still serialise publishers on
//     one lock and copy per subscriber.
//
// Horizon publish is O(1) in N and O(1) in the number of consumers:
// one LOCK XADD to claim a sequence number, one plain store of V,
// one seq Store to publish, and (only if someone is actually
// parked) one TryLock + Broadcast. Consumers are completely
// independent: each Cursor owns its own next sequence number, so
// consumers never contend with each other or with publishers except
// for the single tail Add and the per-slot seq line they are
// reading.
//
// Ordering / loss semantics:
//
//   - Total order is the claim order (tail Add order). A cursor
//     sees messages in increasing seq, never duplicated, never
//     reordered.
//   - If a cursor lags by more than capacity, the messages it missed
//     are gone. Next() silently jumps to the oldest retained seq
//     (tail-capacity). Lag() / Missed() let the caller observe how
//     far behind it is.
//   - If a cursor is caught up but the next slot is claimed and not
//     yet published (publisher between claim and seq Store), Next()
//     returns false. The caller polls again or blocks in WaitNext.
//     It never returns a torn value.
//
// Concurrency contract:
//
//   - Publish / PublishBatch / Latest are safe for concurrent use
//     by any number of goroutines.
//   - A Cursor is single-goroutine owned, like an iterator. Do not
//     share one Cursor across goroutines. Create one Cursor per
//     consumer goroutine; creating a Cursor is cheap (one struct,
//     no goroutine, no channel, no allocation beyond the struct
//     itself when it escapes).
//   - Horizon must be created with NewHorizon. The zero value is
//     not usable. Horizon must not be copied after first use.
//   - Close must be called after publishers have stopped, as with
//     close(chan). Publish after Close returns (0, false) on a
//     best-effort basis. Close is idempotent and wakes all parked
//     WaitNext callers.
//
// Zero-allocation hot path: Publish, PublishBatch, Next, Latest,
// SkipToLatest, Lag all allocate 0 bytes. Blocking waits use
// sync.Cond and context.AfterFunc only on the slow path.
type Horizon[V any] struct {
	slots    []horizonSlot[V]
	mus      []sync.Mutex // race-build only: per-slot mutex for TSan; unused on fast path
	capacity uint64
	mask     uint64

	_padHeader [48]byte // read-only header separated from hot tail

	tail       atomic.Uint64 // next seq to claim; the ONLY hot publisher counter
	_padTail   [56]byte
	waiters    atomic.Int32 // number of goroutines currently parked in Wait
	_padWaiter [60]byte
	closed     atomic.Bool
	_padClosed [56]byte

	mu   sync.Mutex // Cond mutex; only touched when someone parks / wakes
	cond *sync.Cond
}

// NewHorizon creates a broadcast ring that retains capacity messages.
// Capacity must be a power of two (>0). Power-of-two lets seq->slot
// be a single AND (seq & mask) instead of a division, and keeps the
// slot index branchless on the hot path.
func NewHorizon[V any](capacity int) *Horizon[V] {
	if capacity <= 0 {
		panic("bigbuff: horizon: capacity must be > 0")
	}
	if capacity&(capacity-1) != 0 {
		panic("bigbuff: horizon: capacity must be a power of two")
	}
	h := &Horizon[V]{
		slots:    make([]horizonSlot[V], capacity),
		mus:      make([]sync.Mutex, capacity),
		capacity: uint64(capacity),
		mask:     uint64(capacity - 1),
	}
	h.cond = sync.NewCond(&h.mu)
	// Slot i initially expects seq i: seq == i means "empty, awaiting i".
	// After publishing seq i, slot seq becomes i+1.
	for i := range h.slots {
		h.slots[i].seq.Store(uint64(i))
	}
	return h
}

func (h *Horizon[V]) checkInit() {
	if h == nil || h.cond == nil {
		panic("bigbuff: horizon: must use NewHorizon")
	}
}

// Cap returns the retention capacity.
func (h *Horizon[V]) Cap() int {
	h.checkInit()
	return int(h.capacity)
}

// Tail returns the next sequence number to be claimed (i.e. the
// number of messages ever claimed). It is for observability only;
// cursors should use Seq / Lag.
func (h *Horizon[V]) Tail() uint64 {
	h.checkInit()
	return h.tail.Load()
}

// Closed reports whether Close has been called.
func (h *Horizon[V]) Closed() bool {
	h.checkInit()
	return h.closed.Load()
}

// Publish publishes one value. It is wait-free in the practical
// sense: one atomic Add (LOCK XADD on amd64, LDXR/STXR loop on
// arm64, always succeeds, no CAS retry storm), one value store,
// one seq Store. No lock, no allocation, no per-subscriber work.
//
// It returns the sequence number assigned to v, and false if the
// Horizon is closed.
func (h *Horizon[V]) Publish(v V) (uint64, bool) {
	h.checkInit()
	if h.closed.Load() {
		return 0, false
	}
	// Claim: fetch-add, not CAS. A CAS loop on a single tail under
	// M producers degenerates into retry storms; Add is a single
	// hardware RMW that always makes progress.
	seq := h.tail.Add(1) - 1
	h.storeSlot(seq, v)
	h.notify()
	return seq, true
}

// PublishBatch publishes len(vals) values with a single tail Add
// and a single wakeup, amortising the atomic + broadcast cost over
// the whole batch. Values are published in slice order. If
// len(vals) > capacity, early values in the batch may be overwritten
// by later values in the same batch before a slow reader sees them;
// that is the same lossy rule as single Publish, applied to the
// batch. Returns the first seq in the batch.
func (h *Horizon[V]) PublishBatch(vals []V) (uint64, bool) {
	h.checkInit()
	if len(vals) == 0 {
		return h.tail.Load(), true
	}
	if h.closed.Load() {
		return 0, false
	}
	first := h.tail.Add(uint64(len(vals))) - uint64(len(vals))
	for i, v := range vals {
		h.storeSlot(first+uint64(i), v)
	}
	h.notify()
	return first, true
}

// notify wakes parked waiters, coalesced.
//
// Fast path (no waiters): one atomic Load, no lock, no futex.
// Slow path (waiters > 0): TryLock the Cond mutex. If TryLock
// fails, another publisher is already holding it to broadcast, or
// a waiter is holding it while re-checking and will see the new
// data itself. In both cases our publish is already visible
// (seq Store happened before notify), so the other broadcast /
// the waiter's own re-check covers us. We must NOT block here:
// blocking on the Cond mutex would serialise all publishers
// behind one slow waiter wakeup and re-introduce the lock
// contention we removed from the publish path.
func (h *Horizon[V]) notify() {
	if h.waiters.Load() == 0 {
		return
	}
	if h.mu.TryLock() {
		h.cond.Broadcast()
		h.mu.Unlock()
	}
}

// Latest returns the most recent fully-published value without
// allocating a cursor and without touching any consumer state.
// It is a wait-free optimistic read: read tail, read the slot for
// tail-1 with the seqlock double-check, retry only if tail moved
// underneath us. If the latest publisher is mid-publish (claimed
// but not yet stored seq), it returns false rather than blocking.
func (h *Horizon[V]) Latest() (V, uint64, bool) {
	h.checkInit()
	var zero V
	for attempt := 0; attempt < 8; attempt++ {
		tail := h.tail.Load()
		if tail == 0 {
			return zero, 0, false
		}
		seq := tail - 1
		if v, ok := h.loadSlot(seq, seq+1); ok {
			return v, seq, true
		}
		// If tail did not move, the latest slot is mid-publish.
		// If tail moved, retry with the newer tail.
		if h.tail.Load() == tail {
			return zero, 0, false
		}
	}
	return zero, 0, false
}

// Subscribe returns a cursor positioned at the current tail: it
// will see only messages published after this call.
func (h *Horizon[V]) Subscribe() *Cursor[V] {
	h.checkInit()
	return &Cursor[V]{h: h, next: h.tail.Load()}
}

// SubscribeFromLatest is Subscribe.
func (h *Horizon[V]) SubscribeFromLatest() *Cursor[V] {
	return h.Subscribe()
}

// SubscribeFromOldest returns a cursor positioned at the oldest
// retained message (tail-capacity, or 0 if fewer than capacity
// messages have ever been published).
func (h *Horizon[V]) SubscribeFromOldest() *Cursor[V] {
	h.checkInit()
	tail := h.tail.Load()
	var next uint64
	if tail > h.capacity {
		next = tail - h.capacity
	}
	return &Cursor[V]{h: h, next: next}
}

// SubscribeFrom returns a cursor positioned at seq, clamped to
// [oldest retained, tail]. A seq that is already overwritten is
// clamped forward to the oldest retained message; Next() would
// have skipped there anyway.
func (h *Horizon[V]) SubscribeFrom(seq uint64) *Cursor[V] {
	h.checkInit()
	tail := h.tail.Load()
	if seq > tail {
		seq = tail
	}
	if tail-seq > h.capacity {
		if tail > h.capacity {
			seq = tail - h.capacity
		} else {
			seq = 0
		}
	}
	return &Cursor[V]{h: h, next: seq}
}

// Close marks the Horizon closed, wakes every parked WaitNext /
// WaitNextContext caller, and is idempotent. Call it after
// publishers have stopped.
func (h *Horizon[V]) Close() {
	h.checkInit()
	if !h.closed.CompareAndSwap(false, true) {
		return
	}
	h.mu.Lock()
	h.cond.Broadcast()
	h.mu.Unlock()
}

// Cursor is one consumer's position in a Horizon. It is
// single-goroutine owned. Creating a cursor is cheap and does
// not register with the Horizon: the Horizon never tracks
// cursors, which is exactly why publish stays O(1) in N.
type Cursor[V any] struct {
	h    *Horizon[V]
	next uint64 // next seq to read
}

func (c *Cursor[V]) checkInit() {
	if c == nil || c.h == nil || c.h.cond == nil {
		panic("bigbuff: horizon: cursor: must use Subscribe")
	}
}

// Next is the non-blocking hot path. It returns the next message
// in total order, or false if none is available yet. It never
// blocks, never allocates, and never returns a torn value: the
// slot seqlock double-check discards any read that raced an
// overwrite.
//
// If the cursor has lagged by more than capacity, Next jumps
// forward to the oldest retained message instead of returning
// stale / duplicate data.
func (c *Cursor[V]) Next() (V, bool) {
	c.checkInit()
	h := c.h
	var zero V
	for {
		tail := h.tail.Load()
		if c.next >= tail {
			return zero, false
		}
		if tail-c.next > h.capacity {
			if tail > h.capacity {
				c.next = tail - h.capacity
			} else {
				c.next = 0
			}
			continue
		}
		if v, ok := h.loadSlot(c.next, c.next+1); ok {
			c.next++
			return v, true
		}
		// Slot not yet published (publisher between claim and
		// seq Store) or overwritten while we were reading it.
		// If tail moved far enough that we are now lagging, loop
		// and let the lag branch skip us forward. Otherwise the
		// publisher is still in flight: return false and let the
		// caller poll or block. Spinning here would burn a core
		// for a publisher that may be descheduled.
		if tail2 := h.tail.Load(); tail2-c.next > h.capacity {
			continue
		}
		return zero, false
	}
}

// WaitNext blocks until the next message is available or the
// Horizon is closed. It is the slow path: Next() is tried first,
// then the goroutine parks on sync.Cond. The waiter count gates
// publisher notify, so a parked waiter costs publishers nothing
// until it actually parks, and a busy poller costs publishers
// nothing at all.
func (c *Cursor[V]) WaitNext() (V, bool) {
	c.checkInit()
	h := c.h
	var zero V
	for {
		if v, ok := c.Next(); ok {
			return v, true
		}
		if h.closed.Load() {
			return zero, false
		}
		h.waiters.Add(1)
		h.mu.Lock()
		// Re-check under the Cond lock: this closes the race
		// where a publish + broadcast happened between our
		// Next() above and our park below. If data arrived, we
		// must not park; if Close happened, we must not park.
		if h.closed.Load() || c.next < h.tail.Load() {
			h.mu.Unlock()
			h.waiters.Add(-1)
			continue
		}
		h.cond.Wait()
		h.mu.Unlock()
		h.waiters.Add(-1)
	}
}

// WaitNextContext is WaitNext with context cancellation. It uses
// context.AfterFunc to broadcast on cancellation, so no goroutine
// is spawned per wait and no goroutine leaks if the wait returns
// via data or Close: AfterFunc is stopped on every exit path.
// If data and cancellation race, data wins on the next loop
// iteration because Next() is tried before the ctx check.
func (c *Cursor[V]) WaitNextContext(ctx context.Context) (V, bool) {
	c.checkInit()
	if ctx == nil {
		ctx = context.Background()
	}
	h := c.h
	var zero V
	for {
		if v, ok := c.Next(); ok {
			return v, true
		}
		if h.closed.Load() {
			return zero, false
		}
		select {
		case <-ctx.Done():
			return zero, false
		default:
		}
		stop := context.AfterFunc(ctx, func() {
			h.mu.Lock()
			h.cond.Broadcast()
			h.mu.Unlock()
		})
		h.waiters.Add(1)
		h.mu.Lock()
		if h.closed.Load() || c.next < h.tail.Load() || ctx.Err() != nil {
			h.mu.Unlock()
			h.waiters.Add(-1)
			stop()
			continue
		}
		h.cond.Wait()
		h.mu.Unlock()
		h.waiters.Add(-1)
		stop()
	}
}

// SkipToLatest jumps the cursor to the current tail, discarding
// any buffered messages it had not yet read.
func (c *Cursor[V]) SkipToLatest() {
	c.checkInit()
	c.next = c.h.tail.Load()
}

// SkipToOldest jumps the cursor to the oldest retained message.
func (c *Cursor[V]) SkipToOldest() {
	c.checkInit()
	tail := c.h.tail.Load()
	if tail > c.h.capacity {
		c.next = tail - c.h.capacity
	} else {
		c.next = 0
	}
}

// Seq returns the next sequence number this cursor will read.
func (c *Cursor[V]) Seq() uint64 {
	c.checkInit()
	return c.next
}

// Lag returns how many messages have been published but not yet
// read by this cursor (0 if caught up).
func (c *Cursor[V]) Lag() uint64 {
	c.checkInit()
	tail := c.h.tail.Load()
	if tail > c.next {
		return tail - c.next
	}
	return 0
}

// Missed returns how many messages this cursor has permanently
// lost to overwrite (lag beyond capacity). 0 means it can still
// catch up without loss.
func (c *Cursor[V]) Missed() uint64 {
	c.checkInit()
	lag := c.Lag()
	if lag > c.h.capacity {
		return lag - c.h.capacity
	}
	return 0
}

// All returns a blocking iterator that yields messages until the
// Horizon is closed or the yield function returns false. It is
// the range-over equivalent of repeatedly calling WaitNext.
// Example:
//
//	for v := range cursor.All() {
//		// handle v
//	}
func (c *Cursor[V]) All() iter.Seq[V] {
	c.checkInit()
	return func(yield func(V) bool) {
		for {
			v, ok := c.WaitNext()
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

// AllContext is All with context cancellation.
func (c *Cursor[V]) AllContext(ctx context.Context) iter.Seq[V] {
	c.checkInit()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		for {
			v, ok := c.WaitNextContext(ctx)
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

````
