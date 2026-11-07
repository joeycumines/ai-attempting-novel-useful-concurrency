I'll check the sandbox toolchain first. I can't reach `/Users/joeyc/dev/go-bigbuff` from here, so I'll rebuild the two files in a sandbox module and benchmark against them.

Go 1.24 is available, with 2 cores, so benchmark numbers will be indicative only. I'll set up the module and the two existing files verbatim as the comparison baseline.

The sandbox now has the two existing files verbatim. Next is the new design. The core idea: one shared value ring, and each subscriber gets a select-able doorbell that the sender rings only while that subscriber is parked. Membership is a set of packed bitmaps, so busy subscribers cost the sender a bit test, not a channel operation.

I wrote "size bytes beyond" in the join comment, which should say sequence numbers. Fixing that before moving on.

Now the tests. They cover the invariants the design depends on: slack-bounded lag, no lost wakeups, stale-handle isolation, join safety under churn, and zero steady-state allocation. Two small build-tagged files let the allocation assertions skip under `-race`, which instruments allocations.

The directory-growth test has a garbled loop I need to clean up before running anything.

Now the benchmark suite. It compares Carillon against `ChanCaster`, `ChanPubSub`, and a mutex-plus-per-subscriber-channel linear fan-out as the callback-style reference.

`gofmt` and `go vet` are clean. Running the functional tests first, without the race detector.

Everything passes on the first run. The churn stress finishing in 10 ms is fast enough to be suspicious, so I'll check it under the race detector and repeat it many times to shake out interleavings.

The race detector is clean across three runs. Next, a much heavier soak: the stress and select tests repeated at several `GOMAXPROCS` settings, which changes the interleavings.

The soak turned up two things. The `GOMAXPROCS=1` failure is a test-design issue: the sender never yields, so churners never run before it finishes. The `GOMAXPROCS=4` run got killed at the 500 s timeout, which looks like a hang, and I need to find it. Checking the background run first.

The GOMAXPROCS=4 run hung, and the GOMAXPROCS=8 run is still going. I'll kill the background run, then reproduce the hang with a short `-timeout` so Go prints a goroutine dump.

The background run is finished. Next I'll reproduce the hang at GOMAXPROCS=4 with a 90 s test timeout, which makes Go dump every goroutine if it stalls.

That run passed in 19.5 s, so the hang is intermittent. Looping at GOMAXPROCS=4 with a 60 s per-run timeout, so Go prints the stacks when one stalls.

It reproduces in most batches. Reading one of the timeout dumps.

The sender is stuck in `reserve` on `<-c.writerBell`, and the stable receiver is parked in `Recv`. Reasoning through the handshake hasn't found the bug, so I'll instrument instead: a watchdog that dumps the caster's atomic state when the churn scenario stalls.

The stack shows the sender blocked on `writerBell` while churners are polling, so the stuck subscriber is probably one whose cursor isn't advancing. I'll write a temporary watchdog test that dumps every slot's word, cursor, and bitmap bits when the stress scenario stalls.

The dump explains the hang. Every subscriber is at cursor 9236, but the sender is still waiting on `waitFor = 9205`, and two slots have their pending bits set, meaning they are fresh joiners. The sequence is:

1. A joiner stores a stale cursor (`p1 < waitFor`), and the sender's scan reads it and blocks.
2. The join check fails, so the joiner retries with a fresh cursor (`p2`) and stores it.
3. Nothing signals the sender. Only cursor advances made through `TryRecv` signal it, and the retry moves the cursor without going through `TryRecv`.

So this is a real lost wakeup, and the GOMAXPROCS=1 failure was a separate test-design problem. Fixing the join path: after the final cursor store, the joiner wakes a waiting sender.

I'll add the `wakeWriter` helper that call uses, and point `Unsubscribe` at the same helper.

Next, the GOMAXPROCS=1 test-design problem. The sender should wait until churners have joined, and yield periodically so single-P schedulers interleave the two sides.

Rebuilding the test binary and re-running the same soak loop at several `GOMAXPROCS` values, to check the hang is gone and the single-P failure is fixed.

Zero failures across 18 batches at GOMAXPROCS 1, 4, and 8. Next I'll run the stress tests under the race detector repeatedly, then delete the temporary debug harness.

Race-clean across 25 repetitions. The join and absorb handshake is the subtlest part of the design, so I want more adversarial coverage. I'll parameterise the churn test over tiny slacks (1, 2, 4), where the sender blocks constantly and the join-versus-sender windows get hit far more often.

I'll move the churn test into its own file, parameterised over slack and churn intensity, and delete the old single-configuration copy.

Running the parameterised churn test without the race detector first, then with it.

The `join_without_read` variant hangs: churners join and leave without ever reading, which is a new pattern. I'll isolate it and capture the goroutine dump with a shorter timeout.

The goroutines are all runnable, not blocked. The sender is spinning in `reserve` and churners are spinning through join/unsubscribe, so this looks like a livelock rather than a lost wakeup. I'll bring back the state dump with a watchdog for this exact scenario.

The dump shows no lost wakeup. Every live cursor is at 36, which is the published sequence, yet the sender keeps spinning. My reading is starvation, not a logic bug. Eight churners join and leave in tight loops that never block or yield. Each join or leave signals the sender, which goes into the churner's run slot but can't run until the churner is preempted, roughly every 10 ms on 2 Ps. Testing that hypothesis by adding a `Gosched` to the churner loop.

With the churner yielding, all 20,000 sends finish in 106 ms. So this is a scheduler artifact of a backpressured design under oversubscription, not a deadlock, and the test's tight loop was unrealistic.

Two changes follow. First, the join path should only wake the sender when a retry actually happened. Only a failed attempt can publish a cursor below `waitFor`, so the unconditional wake is unnecessary churn. Second, the churn test's churners should yield between cycles.

Now the churn test: churners yield after each cycle, which models real subscribers that block or yield between joins.

All six churn configurations pass in under half a second. Now the full soak, this time with more iterations per configuration and the race detector on separately.

Race-clean across 30 repetitions of the full suite. Since the retry path is what hid the bug, I want evidence the stress tests actually execute it, along with the blocking and absorb paths. Checking line coverage on `carillon.go`.

Line coverage is effectively complete. The retry path (`retried = true` and the wake) is exercised, and the two uncovered blocks are an `absorb` branch and a rare `grow` race. Checking what the absorb block is.

The uncovered `absorb` branch is the one that matters most. It is the only thing stopping the sender from overwriting a joiner's window when the joiner's cursor is older than the sender's cached bound. The stress tests never hit it because joiners rarely load a stale cursor. I'll make both join races deterministic with a test-only hook that runs between the cursor load and its publication.

Now the two deterministic join tests. One forces the retry path, with the overwrite already in progress. The other forces the absorb path, where the sender's cached bound is ahead of the joiner and only absorbing keeps it from overwriting the joiner's window.

Before trusting these, I'll confirm they pass on the real code and then fail when I deliberately break the mechanism. Running them as-is first.

Both pass. Now mutation checks in a scratch copy, so the real sources stay untouched. Mutation A removes the absorb's bound update. Mutation B removes the reserved-sequence retry.

Both mutations are caught, each by the test aimed at it. One more regression case: the lost wakeup I found in the retry path. A joiner's stale cursor blocks the sender, and the retry has to wake it. I'll add a deterministic test for that and check it against a mutation that removes the wake.

All three mutations are caught by their targeted tests. I'll re-run the full suite under the race detector, then move on to benchmarks.

The full suite is race-clean and the plain run passes. Now the benchmarks, with `-benchmem` so allocation per operation is visible.

The ChanCaster fan-out benchmark deadlocked. I'll read the top of the output for the cause.

The Carillon fan-out at one subscriber runs at 62 ns/op with zero allocations. The ChanCaster sub-benchmark deadlocks, and the trace shows the sends returned without delivering anything, which means `Send` saw zero receivers. Reproducing that in a minimal test to find out why.

In isolation `Send` delivers correctly, so the difference must be in my benchmark code. Reading the ChanCaster block as written.

Nothing in the code as written looks wrong, so I'll run just that sub-benchmark with a single iteration to see whether it's reproducible.

Found it, and it's my misuse of the API, not a ChanCaster bug. `Send` clears the receiver count to zero when it finishes, so receivers have to re-register with `Add(1)` before each message. The package doc describes that per-message pattern, and my benchmark registered once. I'll fix the benchmark so each consumer registers before every message, with a per-round handshake so the sender only sends once all receivers are registered. That handshake is inherent to ChanCaster's model, so the benchmark's cost includes it, and I'll document that.

I'll check ChanCaster's receiver-reset semantics for a possible race in the benchmark, and run the full test suite at the same time.

ChanCaster's `Send` holds the write lock until it resets the count, and `Add(1)` takes the read lock. So a receiver that re-registers after receiving blocks until `Send` finishes, and the benchmark's per-message registration is race-free. The full suite passes with gofmt and vet clean. Next I'll run the full benchmark set.

The benchmarks ran cleanly. Before I report these numbers, I need to confirm what the Fanout and Churn loops actually measure, so the comparisons are fair.

The benchmark methodology holds up. Both sides of each comparison do end-to-end delivery, and the Churn numbers need a caveat I'll cover in the write-up. Last, I'll run the race detector over the package and confirm no debug files are left behind.

Everything passes. I'll send the new Carillon files.

Carillon is finished and delivered. It passes the full test suite, the race detector (three runs), gofmt and vet. The benchmark suite now runs to completion, and on this machine Carillon is the fastest of the four designs at every fan-out size.

## The design

Carillon is a broadcast primitive built on a shared value ring with per-subscriber doorbells:

- **Ring with gated reuse:** The sender writes into a ring of `slack` slots. It reuses a slot only after every live subscriber's cursor has passed it (LMAX/Disruptor-style gating). It never waits on a subscriber that is keeping up.
- **Packed per-subscriber word:** Each subscriber owns a 64-byte slot holding one atomic word, a live bit plus its cursor. Only the owner writes it, and the sender only reads it.
- **Selective doorbells:** A doorbell channel is rung only for parked subscribers. Parked state is tracked in per-chunk 64-bit bitmaps, so `Send` costs O(parked subscribers), not O(all subscribers). A subscriber that isn't parked costs the sender nothing per message.
- **Allocation-free subscribe:** Handles are value types with generation checks. A late joiner starts at the published sequence. A Dekker-style handshake with the sender handles the join, and it retries if the sender has already overwritten its starting cursor.
- **Select support:** `Ready()` returns a channel that composes with other `select` cases.

The novelty is combining sequence-gated ring reuse with select-compatible, selectively rung notification and a handshake-based join.

## Bugs found along the way

- **Lost wakeup in the join retry path.** A joiner stored a stale cursor, the sender blocked on it, then the joiner retried with a fresh cursor and sent no signal. The fix wakes the writer after a retried join. A deterministic test reproduces the bug, and reverting the fix makes it fail.
- **Scheduler starvation at GOMAXPROCS=2.** Spinning churners starved the sender. Churners now yield after each cycle.
- **ChanCaster deadlock was my misuse.** `Send` resets the receiver count when it finishes, so each receiver must re-register before every message. My benchmark registered once. It now re-registers per message, and that handshake is included in ChanCaster's cost.

## Validation

- `go test`, `go test -race -count=3`, gofmt and vet are all clean.
- Churn stress covers slack 1, 2, 4 and 32, plus join-heavy and join-without-read variants.
- Soak runs at GOMAXPROCS 1, 2, 4 and 8 had zero failures.
- Mutation tests show that each of the three join tests and the wake fix catches a real regression.

## Benchmarks

Two-vCPU sandbox, two runs each, ns/op (lower is better). Every Fanout and SendTryRecv row reports 0 allocs/op.

Fan-out, end to end (every consumer receives every value):

| Subscribers | Carillon | Linear | ChanPubSub | ChanCaster |
|---|---|---|---|---|
| 1 | 62 | 88 | 447 | 347 |
| 8 | 315 | 1,644 | 1,695 | 2,580 |
| 64 | 2,580 | 19,530 | 14,080 | 21,815 |

Single goroutine, one send plus one TryRecv per subscriber:

| Subscribers | Carillon | Linear |
|---|---|---|
| 1 | 36 | 71 |
| 8 | 137 | 499 |
| 64 | 923 | 3,869 |

Churn, subscribe plus unsubscribe while a sender runs:

| Carillon | Linear | ChanCaster |
|---|---|---|
| 405 ns, 0 allocs | 1,916 ns, 768 B, 3 allocs | 53 ns, 0 allocs (not comparable) |

What I make of these:

- Your bar was "comparable, if lesser," but Carillon was slightly ahead at one subscriber and clearly ahead at 8 and 64. That surprised me, and I wouldn't rely on it until it's measured on more cores. My best explanation is that linear does a select and a channel operation for every subscriber on every message, while Carillon does one only for parked subscribers. At 64 subscribers, that works out to about 40 ns per subscriber for Carillon and about 305 ns for linear.
- Linear's churn allocations come from creating a channel and a done channel per subscriber. Carillon's subscribe path doesn't allocate.
- The ChanCaster churn row isn't comparable. Its `Add` is a counter with no per-subscriber resource, and each registration is consumed by exactly one `Send`. Its real cost is the per-message handshake shown in the fan-out row, which makes that row pessimistic for ChanCaster.

Caveats:

- These are two vCPUs on a shared VM, so treat them as relative. Run-to-run spread was small, around 5%, but I haven't tested many-core hardware. I'd expect the shared cache lines (sequence words, chunk bitmaps) to change the picture at higher core counts.
- The fan-out numbers assume most consumers keep up. If every consumer is always parked, `Send` rings every doorbell, and the cost moves toward linear's O(n) channel operations. I didn't benchmark that case, and it's the worst case for Carillon.

## Limitations

- **Backpressure is slack-bounded.** A subscriber that stops consuming stalls `Send` once it falls `slack` behind, until it catches up or unsubscribes.
- **Owner-only handles.** A `Subscription` must be used by one goroutine.
- **Values stay in the ring until overwritten.** Pointer-heavy `V` types can retain memory.
- **63-bit sequence space.** There is a hard wrap limit at 2^63 sends.
- **`Send` takes a mutex,** so concurrent senders serialise.
- **Shared parked-bit words.** The 64 subscribers in a chunk share one bitmap word, so they contend on it.
- **Stale count.** `Send` returns the active subscriber count as a snapshot, which may be stale by the time you read it.

## Files

I've sent seven files: `carillon.go`, `carillon_test.go`, `carillon_churn_test.go`, `carillon_join_test.go`, `carillon_bench_test.go`, and the build-tag helpers `carillon_race_test.go` and `carillon_norace_test.go`. `chancaster.go` and `chanpubsub.go` are unchanged. I couldn't reach `/Users/joeyc/dev/go-bigbuff` from the sandbox, so the files are in `/home/claude/go-bigbuff` for you to copy in. I haven't committed anything.

````txtar
-- carillon_join_test.go --
package bigbuff

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// The two tests below force the two dangerous interleavings of Subscribe with
// the sender, using joinHook to pause a joiner between loading the published
// sequence and publishing its cursor. Random stress rarely reaches either.

// TestCarillonJoinRetriesWhenCursorWasOverwritten covers the reserved-sequence
// check. The joiner loads p=4, then the sender runs ahead to sequence 9, so
// slot 0 (which holds sequence 4) is overwritten by sequence 8 before the
// joiner publishes. The joiner must notice and retry from the fresh cursor.
func TestCarillonJoinRetriesWhenCursorWasOverwritten(t *testing.T) {
	const slack = 4
	c := NewCarillon[uint64](slack)
	a := c.Subscribe()
	for i := range 4 {
		c.Send(uint64(i))
		a.TryRecv()
	}

	c.joinHook = func() {
		c.joinHook = nil
		for i := 4; i < 10; i++ {
			c.Send(uint64(i))
			a.TryRecv()
		}
	}
	b := c.Subscribe()
	if lag := b.Lag(); lag != 0 {
		t.Fatalf("joiner landed %d behind published; want a fresh cursor", lag)
	}

	c.Send(10)
	a.TryRecv()
	if v, ok := b.TryRecv(); !ok || v != 10 {
		t.Fatalf("joiner TryRecv = (%d, %v), want (10, true); a stale cursor would read an overwritten slot", v, ok)
	}
}

// TestCarillonJoinIsAbsorbedBeforeOverwrite covers the absorb path. The joiner
// loads p=10 and then stalls. The sender advances, and its cached bound moves
// to 16, so without absorbing the joiner it would think it may write 18 (slot
// 2) freely, even though the joiner still needs sequence 10 (also slot 2). The
// sender must absorb the joiner, so it can write 17 but must block before 18
// until the joiner has consumed 10.
func TestCarillonJoinIsAbsorbedBeforeOverwrite(t *testing.T) {
	const slack = 8
	c := NewCarillon[uint64](slack)
	a := c.Subscribe()
	for i := range 10 {
		c.Send(uint64(i))
		a.TryRecv()
	}

	c.joinHook = func() {
		c.joinHook = nil
		for i := 10; i < 17; i++ {
			c.Send(uint64(i))
			a.TryRecv()
		}
	}
	b := c.Subscribe()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Send(17)
		c.Send(18)
	}()

	select {
	case <-done:
		t.Fatal("sender wrote sequence 18 over the joiner's window before the joiner consumed it")
	case <-time.After(50 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for want := uint64(10); want <= 18; want++ {
		v, err := b.Recv(ctx)
		if err != nil {
			t.Fatalf("recv %d: %v", want, err)
		}
		if v != want {
			t.Fatalf("joiner read %d, want %d: the sender overwrote its window", v, want)
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sender still blocked after the joiner consumed its window")
	}
}

// TestCarillonJoinRetryWakesBlockedSender covers the lost wakeup that stress
// testing found. The first join attempt publishes a stale cursor (4), which is
// the cursor the sender blocks on once it tries to write 9. The retry then
// publishes a fresh cursor (9) without consuming anything, so only the join
// itself can wake the sender.
func TestCarillonJoinRetryWakesBlockedSender(t *testing.T) {
	const slack = 4
	c := NewCarillon[uint64](slack)
	a := c.Subscribe()
	for i := range 4 {
		c.Send(uint64(i))
		a.TryRecv()
	}

	calls := 0
	sent := make(chan struct{})
	c.joinHook = func() {
		calls++
		switch calls {
		case 1:
			// Advance to reserved=8 == p+slack, so the first attempt's check fails.
			for i := 4; i <= 8; i++ {
				c.Send(uint64(i))
				a.TryRecv()
			}
		case 2:
			// The stale cursor 4 is visible. Block the sender on it.
			go func() {
				c.Send(9)
				close(sent)
			}()
			deadline := time.Now().Add(5 * time.Second)
			for c.writerWaiting.Load() == 0 {
				if time.Now().After(deadline) {
					t.Errorf("sender never blocked on the stale cursor")
					return
				}
				runtime.Gosched()
			}
			c.joinHook = nil
		}
	}

	b := c.Subscribe()
	if calls != 2 {
		t.Fatalf("join took %d attempts, want 2", calls)
	}
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("sender stayed blocked on a stale cursor after the joiner published a fresh one")
	}
	if v, ok := b.TryRecv(); !ok || v != 9 {
		t.Fatalf("joiner TryRecv = (%d, %v), want (9, true)", v, ok)
	}
}
-- carillon_test.go --
package bigbuff

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func carillonMustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	fn()
}

func carillonEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCarillonSlotIsOneCacheLine(t *testing.T) {
	if got := unsafe.Sizeof(slot{}); got != 64 {
		t.Fatalf("slot size = %d, want 64", got)
	}
}

func TestCarillonSlackRounding(t *testing.T) {
	for in, want := range map[int]uint64{1: 1, 2: 2, 3: 4, 64: 64, 65: 128} {
		if got := NewCarillon[int](in).size; got != want {
			t.Errorf("slack %d: size %d, want %d", in, got, want)
		}
	}
	carillonMustPanic(t, "slack 0", func() { NewCarillon[int](0) })
}

func TestCarillonBroadcastOrdered(t *testing.T) {
	const n, nsubs = 5000, 8
	c := NewCarillon[int](16)
	subs := make([]Subscription[int], nsubs)
	for i := range subs {
		subs[i] = c.Subscribe()
	}
	if got := c.Len(); got != nsubs {
		t.Fatalf("Len = %d, want %d", got, nsubs)
	}

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range n {
				v, err := s.Recv(context.Background())
				if err != nil {
					t.Errorf("recv %d: %v", i, err)
					return
				}
				if v != i {
					t.Errorf("recv %d: got %d", i, v)
					return
				}
			}
		}()
	}
	for i := range n {
		if got := c.Send(i); got != nsubs {
			t.Fatalf("Send(%d) reported %d subscribers, want %d", i, got, nsubs)
		}
	}
	wg.Wait()
	for _, s := range subs {
		s.Unsubscribe()
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after unsubscribe = %d, want 0", got)
	}
}

func TestCarillonLateJoinStartsAtPublished(t *testing.T) {
	c := NewCarillon[int](4)
	for i := range 5 {
		c.Send(i) // no subscribers: nothing is retained for later joiners
	}
	s := c.Subscribe()
	if lag := s.Lag(); lag != 0 {
		t.Fatalf("lag at join = %d, want 0", lag)
	}
	for i := 5; i < 8; i++ {
		c.Send(i)
	}
	if lag := s.Lag(); lag != 3 {
		t.Fatalf("lag = %d, want 3", lag)
	}
	for want := 5; want < 8; want++ {
		if v, ok := s.TryRecv(); !ok || v != want {
			t.Fatalf("TryRecv = (%d, %v), want (%d, true)", v, ok, want)
		}
	}
	if _, ok := s.TryRecv(); ok {
		t.Fatal("TryRecv returned a value after draining")
	}
}

func TestCarillonReadyAndTryRecv(t *testing.T) {
	c := NewCarillon[string](2)
	s := c.Subscribe()
	if _, ok := s.TryRecv(); ok {
		t.Fatal("empty subscription yielded a value")
	}
	select {
	case <-s.Ready():
		t.Fatal("doorbell fired with nothing published")
	default:
	}
	c.Send("a")
	// The doorbell was armed before the send, so the sender rang it.
	select {
	case <-s.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("doorbell did not fire after Send")
	}
	if v, ok := s.TryRecv(); !ok || v != "a" {
		t.Fatalf("TryRecv = (%q, %v), want (\"a\", true)", v, ok)
	}

	// Ready on a non-empty subscription must fire without any further send,
	// because the recheck inside Ready rings the doorbell itself.
	c.Send("b")
	select {
	case <-s.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Ready did not fire with a value pending")
	}
	if v, ok := s.TryRecv(); !ok || v != "b" {
		t.Fatalf("TryRecv = (%q, %v), want (\"b\", true)", v, ok)
	}
}

func TestCarillonBackpressureStallsAtSlackUntilUnsubscribe(t *testing.T) {
	const slack, total = 2, 10
	c := NewCarillon[int](slack)
	slow := c.Subscribe()

	var sent atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range total {
			c.Send(i)
			sent.Add(1)
		}
	}()

	carillonEventually(t, "sender to reach slack", func() bool { return sent.Load() == slack })
	time.Sleep(20 * time.Millisecond)
	if got := sent.Load(); got != slack {
		t.Fatalf("sender ran past slack: sent %d, want %d", got, slack)
	}

	slow.Unsubscribe()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sender still blocked after the lagging subscription unsubscribed")
	}
	if got := sent.Load(); got != total {
		t.Fatalf("sent %d, want %d", got, total)
	}
}

func TestCarillonLagIsBoundedBySlack(t *testing.T) {
	const slack, total = 4, 20000
	c := NewCarillon[int](slack)
	s := c.Subscribe()

	go func() {
		for i := range total {
			c.Send(i)
		}
	}()

	var maxLag uint64
	for i := range total {
		if i%251 == 0 {
			time.Sleep(50 * time.Microsecond) // let the sender reach the bound
		}
		if lag := s.Lag(); lag > maxLag {
			maxLag = lag
		}
		v, err := s.Recv(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if v != i {
			t.Fatalf("got %d, want %d", v, i)
		}
	}
	if maxLag > slack {
		t.Fatalf("observed lag %d exceeds slack %d", maxLag, slack)
	}
	if maxLag == 0 {
		t.Fatal("never observed any lag; the test did not exercise pipelining")
	}
}

func TestCarillonSlotReuseIsolatesSubscriptions(t *testing.T) {
	c := NewCarillon[int](8)
	a := c.Subscribe()
	for i := range 5 {
		c.Send(i)
	}
	if v, ok := a.TryRecv(); !ok || v != 0 {
		t.Fatalf("a.TryRecv = (%d, %v), want (0, true)", v, ok)
	}
	a.Unsubscribe()
	for i := 5; i < 7; i++ {
		c.Send(i) // nobody is subscribed
	}

	b := c.Subscribe() // very likely reuses a's slot
	if lag := b.Lag(); lag != 0 {
		t.Fatalf("reused slot: lag = %d, want 0", lag)
	}
	c.Send(7)
	if v, ok := b.TryRecv(); !ok || v != 7 {
		t.Fatalf("b.TryRecv = (%d, %v), want (7, true)", v, ok)
	}

	// The stale handle must neither consume b's values nor change the count.
	if _, ok := a.TryRecv(); ok {
		t.Fatal("stale handle yielded a value")
	}
	if lag := a.Lag(); lag != 0 {
		t.Fatalf("stale handle lag = %d, want 0", lag)
	}
	a.Unsubscribe()
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d after stale unsubscribe, want 1", got)
	}
	if v, ok := b.TryRecv(); ok {
		t.Fatalf("b yielded %d after its own drain", v)
	}
	c.Send(8)
	if v, ok := b.TryRecv(); !ok || v != 8 {
		t.Fatalf("b.TryRecv = (%d, %v), want (8, true)", v, ok)
	}
	b.Unsubscribe()
}

func TestCarillonDirectoryGrowsAndReuses(t *testing.T) {
	const n = 200
	c := NewCarillon[int](4)
	subs := make([]Subscription[int], 0, n)
	for range n {
		subs = append(subs, c.Subscribe())
	}
	if got := len(*c.chunks.Load()); got != 4 {
		t.Fatalf("chunks = %d, want 4 for %d subscriptions", got, n)
	}
	c.Send(1)
	for i, s := range subs {
		if v, ok := s.TryRecv(); !ok || v != 1 {
			t.Fatalf("sub %d: TryRecv = (%d, %v), want (1, true)", i, v, ok)
		}
	}
	for _, s := range subs {
		s.Unsubscribe()
	}
	for range n {
		c.Subscribe().Unsubscribe() // churn through the free slots
	}
	subs = subs[:0]
	for range n {
		subs = append(subs, c.Subscribe()) // re-fill from the existing chunks
	}
	if got := len(*c.chunks.Load()); got != 4 {
		t.Fatalf("chunks after churn = %d, want 4 (slots must be reused)", got)
	}
}

func TestCarillonSelectNoLostWakeups(t *testing.T) {
	const nsubs, total = 4, 20000
	c := NewCarillon[int](8)
	subs := make([]Subscription[int], nsubs)
	for i := range subs {
		subs[i] = c.Subscribe()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range total {
				var v int
				for {
					got, ok := s.TryRecv()
					if ok {
						v = got
						break
					}
					select {
					case <-s.Ready():
					case <-ctx.Done():
						t.Errorf("timed out waiting for value %d", i)
						return
					}
				}
				if v != i {
					t.Errorf("got %d, want %d", v, i)
					return
				}
			}
		}()
	}
	for i := range total {
		c.Send(i)
		if i%509 == 0 {
			runtime.Gosched()
		}
	}
	wg.Wait()
	for _, s := range subs {
		s.Unsubscribe()
	}
}

func TestCarillonRecvContextAndUnsubscribe(t *testing.T) {
	c := NewCarillon[int](2)
	s := c.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := s.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv error = %v, want DeadlineExceeded", err)
	}
	s.Unsubscribe()
	if _, err := s.Recv(context.Background()); !errors.Is(err, ErrCarillonUnsubscribed) {
		t.Fatalf("Recv after unsubscribe = %v, want ErrCarillonUnsubscribed", err)
	}

	var zero Subscription[int]
	zero.Unsubscribe()
	if lag := zero.Lag(); lag != 0 {
		t.Fatalf("zero Lag = %d", lag)
	}
	if _, ok := zero.TryRecv(); ok {
		t.Fatal("zero TryRecv yielded a value")
	}
	if zero.Ready() != nil {
		t.Fatal("zero Ready returned a non-nil channel")
	}
	if _, err := zero.Recv(context.Background()); !errors.Is(err, ErrCarillonUnsubscribed) {
		t.Fatalf("zero Recv = %v, want ErrCarillonUnsubscribed", err)
	}
}

func TestCarillonAllIterator(t *testing.T) {
	c := NewCarillon[int](8)
	s := c.Subscribe()
	for i := range 5 {
		c.Send(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []int
	for v := range s.All(ctx) {
		got = append(got, v)
		if len(got) == 5 {
			break
		}
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("All yielded %v", got)
		}
	}
	if len(got) != 5 {
		t.Fatalf("All yielded %d values, want 5", len(got))
	}
}

func TestCarillonSteadyStateZeroAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector instruments allocations")
	}

	t.Run("send_and_tryrecv", func(t *testing.T) {
		c := NewCarillon[int](64)
		subs := make([]Subscription[int], 8)
		for i := range subs {
			subs[i] = c.Subscribe()
		}
		var n int
		allocs := testing.AllocsPerRun(1000, func() {
			n++
			c.Send(n)
			for _, s := range subs {
				s.TryRecv()
			}
		})
		if allocs != 0 {
			t.Fatalf("Send+TryRecv allocs/op = %v, want 0", allocs)
		}
	})

	t.Run("parked_ready_path", func(t *testing.T) {
		c := NewCarillon[int](64)
		s := c.Subscribe()
		var n int
		allocs := testing.AllocsPerRun(1000, func() {
			n++
			s.Ready() // arm, then the sender rings the doorbell
			c.Send(n)
			s.TryRecv() // disarm and consume
		})
		if allocs != 0 {
			t.Fatalf("Ready+Send+TryRecv allocs/op = %v, want 0", allocs)
		}
	})

	t.Run("subscribe_unsubscribe", func(t *testing.T) {
		c := NewCarillon[int](64)
		c.Subscribe().Unsubscribe() // warm: first claim allocates the chunk
		allocs := testing.AllocsPerRun(1000, func() {
			s := c.Subscribe()
			s.Unsubscribe()
		})
		if allocs != 0 {
			t.Fatalf("Subscribe+Unsubscribe allocs/op = %v, want 0", allocs)
		}
	})
}
-- carillon_churn_test.go --
package bigbuff

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// carillonChurn runs one sender against one stable subscription, which must
// observe every value in order, and several churning subscriptions. Each churner
// repeatedly joins, reads up to perSub values, and leaves. Its joins must start
// at or after the sequence published before it subscribed, and its values must
// be consecutive. A small slack keeps the sender blocked, which drives the
// join-versus-sender window much harder than a large one.
func carillonChurn(t *testing.T, slack, total, churners, perSub int) {
	t.Helper()
	c := NewCarillon[uint64](slack)
	stable := c.Subscribe()

	var (
		sent       atomic.Uint64 // values whose Send has returned
		senderDone atomic.Bool
		joins      atomic.Int64
		wg         sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range total {
			v, err := stable.Recv(context.Background())
			if err != nil {
				t.Errorf("stable recv %d: %v", i, err)
				return
			}
			if v != uint64(i) {
				t.Errorf("stable: got %d, want %d", v, i)
				return
			}
		}
	}()

	for range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !senderDone.Load() {
				base := sent.Load()
				sub := c.Subscribe()
				joins.Add(1)
				first := true
				var prev uint64
				for got := 0; got < perSub; {
					v, ok := sub.TryRecv()
					if !ok {
						if senderDone.Load() && sub.Lag() == 0 {
							break
						}
						select {
						case <-sub.Ready():
						case <-time.After(time.Millisecond):
						}
						continue
					}
					if first {
						if v < base {
							t.Errorf("joined after %d but first value was %d", base, v)
						}
						first = false
					} else if v != prev+1 {
						t.Errorf("non-consecutive values: %d after %d", v, prev)
					}
					prev = v
					got++
				}
				sub.Unsubscribe()
				// Real subscribers block or yield between joins. A churner that
				// spins without yielding can hold a P for a whole scheduler
				// quantum while the sender waits for it to leave, which measures
				// the scheduler rather than the design.
				runtime.Gosched()
			}
		}()
	}

	// Let the churners get going before the sender runs flat out. Otherwise a
	// single-P scheduler can finish the whole sequence before any churner runs.
	for joins.Load() < int64(churners) {
		runtime.Gosched()
	}
	for i := range total {
		c.Send(uint64(i))
		sent.Store(uint64(i + 1))
		if i%64 == 0 {
			runtime.Gosched()
		}
	}
	senderDone.Store(true)
	wg.Wait()

	if got := joins.Load(); got < int64(churners) {
		t.Fatalf("only %d joins happened; churn did not exercise the join path", got)
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d before stable unsubscribe, want 1", got)
	}
	stable.Unsubscribe()
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d after all unsubscribed, want 0", got)
	}
}

func TestCarillonChurnStress(t *testing.T) {
	for _, slack := range []int{1, 2, 4, 32} {
		t.Run(fmt.Sprintf("slack=%d", slack), func(t *testing.T) {
			carillonChurn(t, slack, 30000, 6, 40)
		})
	}
	t.Run("join_heavy", func(t *testing.T) {
		carillonChurn(t, 2, 30000, 8, 1)
	})
	t.Run("join_without_read", func(t *testing.T) {
		carillonChurn(t, 1, 20000, 8, 0)
	})
}
-- carillon.go --
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
	"math/bits"
	"sync"
	"sync/atomic"
)

// ErrCarillonUnsubscribed is returned by [Subscription.Recv] when the
// subscription is no longer live, because it was unsubscribed (or is a zero
// value).
var ErrCarillonUnsubscribed = errors.New("bigbuff: carillon: subscription is not live")

const (
	// carillonLive is set in a slot's word while the slot is bound to a live
	// subscription. The low 63 bits of the word are the owner's cursor.
	carillonLive uint64 = 1 << 63
	// carillonSeqMask extracts the cursor from a slot's word.
	carillonSeqMask = carillonLive - 1
	// carillonChunk is the number of slots per chunk, i.e. the width of the
	// bitmaps. It must match the word size.
	carillonChunk = 64
)

// Carillon broadcasts every value sent to every live [Subscription], in
// sequence order, through a single ring of values. It is an alternative to
// channel fan-out with three properties that the channel designs in this
// package trade against each other:
//
//   - Allocation is zero per Send, per receive, per Subscribe and per
//     Unsubscribe once the subscriber table has warmed up. The only
//     allocation is a 64-slot chunk, amortised over 64 concurrent
//     subscribers.
//   - Each subscription is select-compatible: [Subscription.Ready] returns a
//     channel which can be used as a case in a select statement.
//   - The cost of a Send is proportional to the number of subscriptions that
//     are parked (blocked waiting for a value), not the number that exist.
//     Busy subscriptions cost the sender one bit test per 64 of them.
//     Subscribing and unsubscribing take no lock and never wait on the
//     sender, so churn does not contend with broadcasting.
//
// # Semantics
//
// A subscription receives each value published at or after its join point
// (the published sequence observed by [Carillon.Subscribe]) exactly once and in
// order. [Carillon.Send] blocks only while the slowest live subscription is
// `slack` values behind, so `slack` is the pipelining depth: the sender may
// run that far ahead of its slowest consumer. A subscription that stops
// consuming without unsubscribing stalls the sender, just as a stalled
// receiver stalls a channel send.
//
// # Mechanism
//
// Values are stored once, in a power-of-two ring. Each subscription owns a
// slot, which holds a packed word (live bit | cursor) and a 1-buffered
// doorbell channel. Slots are grouped into chunks of 64, each with three
// bitmaps: claimed (slot is bound), parked (owner is blocked, or about to
// block, in [Subscription.Ready]), and pending (slot joined since the sender
// last absorbed it).
//
// The sender publishes a value, then performs a non-blocking send on the
// doorbell of every parked owner. Owners that are keeping up never park, so
// they are never rung. An owner parks by setting its parked bit and then
// rechecking the published sequence before blocking. Go's atomics are
// sequentially consistent, so either the owner observes the new value, or the
// sender observes the parked bit and rings the doorbell. Doorbells are
// coalescing (capacity 1), so a stale or duplicate ring only causes a spurious
// wake-up, which is harmless.
//
// Flow control uses the ring as a bounded window. The sender may write
// sequence w only once every live cursor exceeds w-size, which is the same
// slot-reuse rule used by LMAX-style disruptor rings. The sender caches a
// lower bound on the cursors and rescans only when that bound is exhausted.
// When blocked, it publishes the cursor value it needs in waitFor, then
// sleeps on a single writer doorbell. A subscription wakes the sender only
// when its cursor reaches exactly that value.
//
// Joining is lock-free. A joiner stores its cursor (the published sequence it
// loaded) together with the live bit, sets its pending bit, and then checks
// the sender's reserved sequence. If the sender has already reserved a
// sequence at or beyond the joiner's cursor plus slack, the slot for that
// cursor may have been overwritten, so the joiner reloads and retries.
// Otherwise every future write that could overwrite the joiner's window is
// preceded, in sequentially consistent order, by a read of the pending
// flag, so the sender absorbs the joiner into its cached bound before it writes
// there. See the comments on [Carillon.Subscribe] and [Carillon.reserve].
//
// # Concurrency
//
// [Carillon.Send] is safe for concurrent use; senders are serialised by an
// internal mutex. Subscribe, Len and the methods of [Subscription] are safe for
// concurrent use across subscriptions. A single [Subscription] must be driven by
// one goroutine at a time, and must not be used concurrently with its own
// Unsubscribe.
type Carillon[V any] struct {
	// Sender state, guarded by mu.
	mu        sync.Mutex
	head      uint64 // next sequence to write
	minCached uint64 // lower bound on the cursors of all live subscriptions

	ring []V
	mask uint64
	size uint64 // == len(ring) == slack

	_ [64]byte // keep sender-written fields off the readers' cache lines

	published     atomic.Uint64 // sequences below this may be read
	reserved      atomic.Uint64 // sequence the sender has begun to write (join guard)
	pendingFlag   atomic.Uint32 // non-zero: a subscription joined since the last absorb
	writerWaiting atomic.Uint32 // non-zero: the sender is (or is about to be) blocked
	waitFor       atomic.Uint64 // cursor value that unblocks the sender
	active        atomic.Int64  // number of live subscriptions

	// writerBell wakes a blocked sender. Capacity 1, so signals coalesce.
	writerBell chan struct{}

	growMu sync.Mutex
	chunks atomic.Pointer[[]*chunk] // copy-on-grow directory of chunks

	// joinHook, when non-nil, runs in Subscribe after the joiner has loaded the
	// published sequence and before it publishes its cursor. Tests use it to
	// interleave a sender at the exact point where a join is most fragile. It is
	// always nil in production.
	joinHook func()
}

// slot is one subscription's control block. It is exactly one cache line, so
// an owner updating its cursor does not false-share with its neighbours.
type slot struct {
	// word is carillonLive | cursor while bound, and 0 otherwise. The owner
	// writes it; the sender reads it.
	word atomic.Uint64
	// bell is the doorbell. It is created with the chunk and never replaced.
	bell chan struct{}

	// Owner-only state. The sender never touches these fields.
	cur   uint64 // the owner's cursor, mirrored to avoid atomic loads
	gen   uint64 // incremented on every Subscribe, to detect stale handles
	armed bool   // the owner's parked bit is currently set
	bound bool   // the slot belongs to a live subscription

	_ [24]byte
}

// chunk is 64 slots plus their summary bitmaps. Each bitmap bit corresponds to
// the slot at the same index.
type chunk struct {
	claimed atomic.Uint64 // slot is bound, or is being bound
	parked  atomic.Uint64 // owner is parked (or arming to park)
	pending atomic.Uint64 // slot joined since the sender last absorbed it

	_     [40]byte
	slots [carillonChunk]slot
}

func newCarillonChunk() *chunk {
	ch := new(chunk)
	for i := range ch.slots {
		ch.slots[i].bell = make(chan struct{}, 1)
	}
	return ch
}

// NewCarillon constructs a [Carillon] whose ring holds slack values, rounded up
// to a power of two. slack is the number of values the sender may run ahead
// of its slowest subscription. It panics if slack is less than 1.
func NewCarillon[V any](slack int) *Carillon[V] {
	if slack < 1 {
		panic("bigbuff: carillon: slack must be at least 1")
	}
	n := uint64(1)
	for n < uint64(slack) {
		n <<= 1
	}
	c := &Carillon[V]{
		ring:       make([]V, n),
		mask:       n - 1,
		size:       n,
		writerBell: make(chan struct{}, 1),
	}
	empty := make([]*chunk, 0)
	c.chunks.Store(&empty)
	return c
}

// Len returns the number of live subscriptions. It is a snapshot, and may be
// stale by the time the caller acts on it.
func (c *Carillon[V]) Len() int {
	return int(c.active.Load())
}

// Send publishes v to every live subscription, and returns the number of live
// subscriptions at the time of publication. The value is visible to
// subscriptions as soon as Send publishes it, without waiting for them to
// consume it. Send blocks only while the slowest live subscription is slack
// values behind.
func (c *Carillon[V]) Send(v V) int {
	c.mu.Lock()
	w := c.head
	c.reserved.Store(w)
	c.reserve(w)
	c.ring[w&c.mask] = v
	c.head = w + 1
	c.published.Store(w + 1)
	c.mu.Unlock()

	c.ringParked()
	return int(c.active.Load())
}

// Subscribe binds a new subscription to the carillon. The subscription's join
// point is the sequence published when Subscribe loads it. The subscription
// receives every value published at or after that point. Subscribe never
// blocks on the sender, and performs no allocation once the chunk directory
// has grown to cover the number of concurrent subscriptions.
func (c *Carillon[V]) Subscribe() Subscription[V] {
	ch, i := c.claim()
	bit := uint64(1) << uint(i)
	sl := &ch.slots[i]

	// Publish the cursor, then check that the sender has not already run
	// far enough ahead to have overwritten it. The order matters, and the
	// reasoning is in the package doc. Briefly: the pending bit and flag are
	// set before the reserved check, so any write that could affect this
	// joiner is preceded by a sender-side absorb that sees the joiner.
	var p uint64
	retried := false
	for {
		p = c.published.Load()
		if c.joinHook != nil {
			c.joinHook()
		}
		sl.word.Store(carillonLive | p)
		ch.pending.Or(bit)
		c.pendingFlag.Store(1)
		if c.reserved.Load() < p+c.size {
			break // the sender has not reached the danger zone for p
		}
		// The sender has reserved sequence >= p+size, so the slot for p may
		// already have been reused. Reload and try again.
		retried = true
	}
	if retried {
		// A failed attempt left a cursor that is too low to be safe, and the
		// sender may have observed it while blocked. Only a TryRecv advance
		// signals the sender, and this cursor was not advanced by TryRecv, so
		// signal it here. A join that succeeds first time never needs this: its
		// cursor is at or above any threshold the sender could be waiting on.
		c.wakeWriter()
	}

	sl.cur, sl.armed, sl.bound = p, false, true
	sl.gen++
	c.active.Add(1)
	return Subscription[V]{c: c, ch: ch, sl: sl, bit: bit, gen: sl.gen}
}

// claim binds a free slot, growing the directory if necessary.
func (c *Carillon[V]) claim() (*chunk, int) {
	for {
		chunks := *c.chunks.Load()
		for _, ch := range chunks {
			for a := ch.claimed.Load(); a != ^uint64(0); a = ch.claimed.Load() {
				i := bits.TrailingZeros64(^a)
				if ch.claimed.CompareAndSwap(a, a|uint64(1)<<uint(i)) {
					return ch, i
				}
			}
		}
		c.grow(len(chunks))
	}
}

// grow appends one chunk, unless another goroutine has already done so since
// the caller observed len seen.
func (c *Carillon[V]) grow(seen int) {
	c.growMu.Lock()
	defer c.growMu.Unlock()
	old := *c.chunks.Load()
	if len(old) != seen {
		return
	}
	next := make([]*chunk, len(old)+1)
	copy(next, old)
	next[len(old)] = newCarillonChunk()
	c.chunks.Store(&next)
}

// reserve blocks until sequence w may be written, which means every live
// cursor is greater than w-size. It must be called with mu held, after
// storing reserved = w.
func (c *Carillon[V]) reserve(w uint64) {
	if c.pendingFlag.Load() != 0 {
		c.absorb()
	}
	for w-c.minCached >= c.size {
		c.minCached = c.scanMin(w)
		if w-c.minCached < c.size {
			return
		}
		// Blocked. Publish the threshold before raising writerWaiting, so that a
		// subscription which crosses the threshold observes both values.
		c.waitFor.Store(w + 1 - c.size)
		c.writerWaiting.Store(1)
		c.minCached = c.scanMin(w)
		if w-c.minCached < c.size {
			c.writerWaiting.Store(0)
			return
		}
		<-c.writerBell
		c.writerWaiting.Store(0)
	}
}

// scanMin returns the minimum cursor over all live slots, or w if there are
// none. Cursors never exceed the published sequence, which is at most w.
func (c *Carillon[V]) scanMin(w uint64) uint64 {
	m := w
	for _, ch := range *c.chunks.Load() {
		for a := ch.claimed.Load(); a != 0; a &= a - 1 {
			x := ch.slots[bits.TrailingZeros64(a)].word.Load()
			if x&carillonLive != 0 {
				if cur := x & carillonSeqMask; cur < m {
					m = cur
				}
			}
		}
	}
	return m
}

// absorb folds subscriptions that joined since the last absorb into the
// cached lower bound. It must be called with mu held. Clearing the flag before
// reading the pending bitmaps is what makes the handshake with Subscribe work.
func (c *Carillon[V]) absorb() {
	c.pendingFlag.Store(0)
	for _, ch := range *c.chunks.Load() {
		if ch.pending.Load() == 0 {
			continue
		}
		for a := ch.pending.Swap(0); a != 0; a &= a - 1 {
			x := ch.slots[bits.TrailingZeros64(a)].word.Load()
			if x&carillonLive != 0 {
				if cur := x & carillonSeqMask; cur < c.minCached {
					c.minCached = cur
				}
			}
		}
	}
}

// ringParked performs a non-blocking send to the doorbell of every parked
// owner. Doorbells are capacity 1, so the send never blocks, and it does not
// allocate.
func (c *Carillon[V]) ringParked() {
	for _, ch := range *c.chunks.Load() {
		for a := ch.parked.Load(); a != 0; a &= a - 1 {
			select {
			case ch.slots[bits.TrailingZeros64(a)].bell <- struct{}{}:
			default:
			}
		}
	}
}

// signalWriter wakes a blocked sender. Signals coalesce.
func (c *Carillon[V]) signalWriter() {
	select {
	case c.writerBell <- struct{}{}:
	default:
	}
}

// wakeWriter signals a blocked sender, if one has announced that it is
// waiting. Subscribers call it at the points where their cursor can change in
// a way the sender needs to see: unsubscribe, and the final store of a join.
func (c *Carillon[V]) wakeWriter() {
	if c.writerWaiting.Load() != 0 {
		c.signalWriter()
	}
}

// Subscription is a handle to one subscriber's position in a [Carillon]. It
// is a small value, and copies refer to the same subscription. The zero value
// is an unsubscribed handle. Its methods are no-ops, except that Recv returns
// [ErrCarillonUnsubscribed].
type Subscription[V any] struct {
	c   *Carillon[V]
	ch  *chunk
	sl  *slot
	bit uint64
	gen uint64
}

// live reports whether the handle still refers to the subscription that
// created it.
func (s Subscription[V]) live() bool {
	return s.sl != nil && s.sl.bound && s.sl.gen == s.gen
}

// TryRecv returns the next value, if one is available, without blocking. If
// it returns false, call [Subscription.Ready] before blocking, so that the
// sender can wake this subscription.
func (s Subscription[V]) TryRecv() (V, bool) {
	var zero V
	if !s.live() {
		return zero, false
	}
	sl, c := s.sl, s.c
	if sl.armed {
		s.ch.parked.And(^s.bit)
		sl.armed = false
	}
	if sl.cur >= c.published.Load() {
		return zero, false
	}
	v := c.ring[sl.cur&c.mask]
	sl.cur++
	sl.word.Store(carillonLive | sl.cur)
	if c.writerWaiting.Load() != 0 && sl.cur == c.waitFor.Load() {
		c.signalWriter()
	}
	return v, true
}

// Ready arms the subscription and returns its doorbell. The doorbell is ready
// when a value might be available. It can fire spuriously, so after it fires,
// call [Subscription.TryRecv] and loop. Use it in a select, for example:
//
//	for {
//		if v, ok := sub.TryRecv(); ok {
//			use(v)
//			continue
//		}
//		select {
//		case <-sub.Ready():
//		case <-ctx.Done():
//			return ctx.Err()
//		}
//	}
//
// Ready returns nil for an unsubscribed handle, which never becomes ready.
func (s Subscription[V]) Ready() <-chan struct{} {
	if !s.live() {
		return nil
	}
	sl := s.sl
	if !sl.armed {
		s.ch.parked.Or(s.bit)
		sl.armed = true
	}
	// Recheck after arming. If a value was published before the bit became
	// visible, the sender did not ring us, so ring ourselves now.
	if sl.cur < s.c.published.Load() {
		select {
		case sl.bell <- struct{}{}:
		default:
		}
	}
	return sl.bell
}

// Recv returns the next value, blocking until one is available or ctx is done.
// It returns [ErrCarillonUnsubscribed] if the subscription is not live, and
// ctx.Err() if ctx is done first. A nil ctx never cancels.
func (s Subscription[V]) Recv(ctx context.Context) (V, error) {
	var zero V
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	for {
		if !s.live() {
			return zero, ErrCarillonUnsubscribed
		}
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		select {
		case <-s.Ready():
		case <-done:
			return zero, ctx.Err()
		}
	}
}

// All returns an iterator over values received by the subscription. It stops
// when ctx is done, or when the subscription is not live.
func (s Subscription[V]) All(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		for {
			v, err := s.Recv(ctx)
			if err != nil || !yield(v) {
				return
			}
		}
	}
}

// Lag returns the number of values published but not yet received by this
// subscription. It returns 0 for an unsubscribed handle.
func (s Subscription[V]) Lag() uint64 {
	if !s.live() {
		return 0
	}
	return s.c.published.Load() - s.sl.cur
}

// Unsubscribe releases the subscription's slot for reuse. It is idempotent
// for a given subscription, and a no-op for stale or zero handles. It must not
// run concurrently with the other methods of the same subscription. It may be
// called from any goroutine if the owner is not using the subscription at the
// time, which is the usual case for cancellation.
func (s Subscription[V]) Unsubscribe() {
	if !s.live() {
		return
	}
	sl := s.sl
	if sl.armed {
		s.ch.parked.And(^s.bit)
		sl.armed = false
	}
	sl.bound = false
	// Clear the word before releasing the claim, so a concurrent scan that
	// still sees the claim also sees the slot as dead.
	sl.word.Store(0)
	s.ch.claimed.And(^s.bit)
	s.c.active.Add(-1)
	select {
	case <-sl.bell: // discard a stale ring, so the next owner starts clean
	default:
	}
	s.c.wakeWriter() // the sender may be waiting on this cursor
}
-- carillon_bench_test.go --
package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// linearFanout is the reference "linear" design: membership is a mutex-guarded
// slice, and Send performs one channel send per subscriber, in a loop. Each
// subscriber has its own buffered channel, so the sender is not a rendezvous.
type linearFanout[V any] struct {
	mu    sync.RWMutex
	slack int
	subs  []*linearSub[V]
}

type linearSub[V any] struct {
	ch   chan V
	done chan struct{}
}

func (f *linearFanout[V]) subscribe() *linearSub[V] {
	s := &linearSub[V]{ch: make(chan V, f.slack), done: make(chan struct{})}
	f.mu.Lock()
	f.subs = append(f.subs, s)
	f.mu.Unlock()
	return s
}

func (f *linearFanout[V]) unsubscribe(s *linearSub[V]) {
	close(s.done) // release a sender blocked on s.ch before taking the write lock
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, x := range f.subs {
		if x == s {
			f.subs = append(f.subs[:i], f.subs[i+1:]...)
			return
		}
	}
}

func (f *linearFanout[V]) send(v V) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, s := range f.subs {
		select {
		case s.ch <- v:
		case <-s.done:
		}
	}
	return len(f.subs)
}

var benchFanoutSubs = []int{1, 8, 64}

// BenchmarkFanout measures end-to-end delivery: one sender, n consumers that
// each receive every value. Consumers run concurrently with the sender.
func BenchmarkFanout(b *testing.B) {
	for _, n := range benchFanoutSubs {
		b.Run(fmt.Sprintf("carillon/subs=%d", n), func(b *testing.B) {
			c := NewCarillon[int](64)
			subs := make([]Subscription[int], n)
			for i := range subs {
				subs[i] = c.Subscribe()
			}
			var wg sync.WaitGroup
			for _, s := range subs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						if _, err := s.Recv(context.Background()); err != nil {
							panic(err)
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
			}
			wg.Wait()
		})

		// ChanCaster is registration-per-message: Send clears the receiver count
		// when it completes, so every receiver must Add(1) before each message.
		// The sender therefore waits for n registrations (ready) before each
		// Send. This handshake is part of ChanCaster's cost in this benchmark.
		b.Run(fmt.Sprintf("chancaster/subs=%d", n), func(b *testing.B) {
			c := NewChanCaster(make(chan int))
			ready := make(chan struct{}, n)
			var wg sync.WaitGroup
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						c.Add(1)
						ready <- struct{}{}
						<-c.C
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				for range n {
					<-ready
				}
				c.Send(i)
			}
			wg.Wait()
		})

		b.Run(fmt.Sprintf("chanpubsub/subs=%d", n), func(b *testing.B) {
			ps := NewChanPubSub(make(chan int))
			for range n {
				ps.Subscribe()
			}
			ch := ps.C()
			var wg sync.WaitGroup
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						<-ch
						ps.Wait()
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				ps.Send(i)
			}
			wg.Wait()
		})

		b.Run(fmt.Sprintf("linear/subs=%d", n), func(b *testing.B) {
			f := &linearFanout[int]{slack: 64}
			var wg sync.WaitGroup
			for range n {
				s := f.subscribe()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						<-s.ch
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				f.send(i)
			}
			wg.Wait()
		})
	}
}

// BenchmarkSendTryRecv measures the single-goroutine cost of a send plus one
// non-blocking receive per subscriber. It isolates per-subscriber overhead on
// the busy path, where no subscriber is parked.
func BenchmarkSendTryRecv(b *testing.B) {
	for _, n := range benchFanoutSubs {
		b.Run(fmt.Sprintf("carillon/subs=%d", n), func(b *testing.B) {
			c := NewCarillon[int](64)
			subs := make([]Subscription[int], n)
			for i := range subs {
				subs[i] = c.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
				for _, s := range subs {
					s.TryRecv()
				}
			}
		})

		b.Run(fmt.Sprintf("linear/subs=%d", n), func(b *testing.B) {
			f := &linearFanout[int]{slack: 64}
			subs := make([]*linearSub[int], n)
			for i := range subs {
				subs[i] = f.subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				f.send(i)
				for _, s := range subs {
					<-s.ch
				}
			}
		})
	}
}

// BenchmarkChurn measures a subscribe followed by an unsubscribe, in parallel
// with a continuously running sender. This is where a shared membership lock
// shows its cost.
func BenchmarkChurn(b *testing.B) {
	b.Run("carillon", func(b *testing.B) {
		c := NewCarillon[int](64)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				c.Send(i)
			}
		}()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s := c.Subscribe()
				s.Unsubscribe()
			}
		})
		close(stop)
		wg.Wait()
	})

	b.Run("linear", func(b *testing.B) {
		f := &linearFanout[int]{slack: 64}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				f.send(i)
			}
		}()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				f.unsubscribe(f.subscribe())
			}
		})
		close(stop)
		wg.Wait()
	})

	b.Run("chancaster", func(b *testing.B) {
		c := NewChanCaster(make(chan int))
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				c.Send(i)
			}
		}()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
				c.Add(-1)
			}
		})
		close(stop)
		wg.Wait()
	})
}
-- carillon_norace_test.go --
//go:build !race

package bigbuff

const raceEnabled = false
-- carillon_race_test.go --
//go:build race

package bigbuff

const raceEnabled = true

````
