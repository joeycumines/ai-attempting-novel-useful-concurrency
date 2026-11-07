I'll start by checking whether a Go toolchain is available in the sandbox, so the design can be compiled and race-tested rather than just written down.

Go 1.24 is available, so I can compile and race-test whatever I write. I'll build a new design rather than extend `ChanCaster` or `ChanPubSub`.

**The design:** a value-once, sense-reversing broadcast. The value is stored once in the caster. Only parked receivers get a wake-up token, found through a three-level parked-set bitmap. Acknowledgement is a sense bit per subscription plus sharded lane counters. Slots are recycled with epoch-tagged value handles, so churn is O(1) and allocation-free.

I'll write the core file first.

Next, the correctness tests: ordering, select integration, churn with concurrent close, forfeit, stale handles, and shared-subscription delivery.

Now the allocation tests (excluded under `-race`, since the race runtime adds its own allocations), the benchmarks against a linear per-subscriber baseline, and `go.mod`.

Formatting and vet are clean. Next, the full suite under the race detector, since the lock-free protocol is where bugs would hide.

The one failure is a bug in my test, not in the caster. The publisher finished 4000 sends before any churner subscribed, so every send went to zero subscribers and the churners cancelled immediately. I'll make the publisher keep going until the churners have actually received values.

All nine tests pass five times under `-race`. Lock-free protocols tend to fail rarely, so I'll stress the two hardest tests, churn and shared delivery, across several GOMAXPROCS settings. I'll also run the allocation tests without `-race`.

No failures at any setting, but the GOMAXPROCS=4 and 8 race runs were much slower than GOMAXPROCS=1 or 2 (125s and 66s against 6s). Oversubscribing a 2-core box can explain some of that, but I want to rule out a spin pathology in the caster before I trust its performance. I'll time the churn test on its own at each setting.

One churn run stalled for the test's full 60-second deadline before completing. The test still reported PASS only because the deadline ended the loop. A stall means a publisher or receiver waited forever for a wakeup or an acknowledgement, so this is a liveness bug and I need to find it rather than assume it's scheduling. I'll reproduce it at scale and capture goroutine stacks from the hang.

No stall in 160 further runs, so this is rare. Guessing at a rare race is unproductive, so I'll add a temporary watchdog that dumps every goroutine stack and the caster's internal state if a churn run hangs. Then I'll loop until it reproduces.

Reproduced on the 12th iteration. Now the state dump:

The dump is informative. The publisher is waiting on lane 2, which still owes one acknowledgement. Slot 2's owner is in `Recv`'s select with an empty ready channel, and its park bit is clear. By my reasoning that combination should not occur: a cleared bit means a sweep took it and posted a token, and a consumed token should lead to a `TryRecv` that sees the flip and claims the value. So a step in my reasoning is wrong.

The most likely suspect is the arm path's load-before-OR shortcut, since it is the subtlest part of the protocol. I'll test that directly: make arm unconditionally OR, and see whether the stall still reproduces.

The stall still reproduces with unconditional OR, so the shortcut is not the cause. Here is the new dump:

Same pattern with a different slot: one owed slot whose park bit is clear, no token, and an owner blocked in `Recv`'s select. Since two independent runs produce the same shape, my mental model of the code is probably wrong somewhere, not the schedule. I'll reread the actual receive path rather than trust my memory of it.

The receive path matches what I intended. Next, the full `TryRecv` body and `arm`, which I haven't reread yet:

The bug is now clear. The owner of the owed slot is not stuck on a lost sweep. It lost its wake-up token to a stale waiter.

Here is the sequence. A churner calls `Recv` on a subscription whose closer goroutine is about to close it. The churner has passed its `Closed()` check but has not yet entered `select`. The closer releases the slot, and a new subscriber immediately reuses it. Now two goroutines wait on the same ready channel: the retired churner, which will exit, and the new owner. The publisher's sweep posts one token, and the retired waiter consumes it. It sees its epoch is stale and returns. The new owner is left owed a value with no token and no park bit. The dump matches exactly.

Tokens are anonymous, so the epoch check alone cannot protect them. The fix is to make a retired handle pass on any token it consumes. The change is one line in `TryRecv`'s stale branch: when a handle finds it has been retired, it re-posts the wake-up to the slot. Every token that is consumed is then either used by the current tenant or re-posted for it. I'll restore the original arm (the shortcut was not the cause) and apply this fix.

Now I'll rerun the stall harness, which reproduced the hang within about 12 iterations before. If the fix works, 400 iterations should complete cleanly.

400 iterations pass with the fix, where the bug previously appeared within a dozen. The original failure was a token lost through the `Recv` path, but the user-facing `select` path has the same hazard. A user's select can consume a token from a retired handle and then call `TryRecv` (which forwards it), or never call it. I'll add a churn test that drives the subscription purely through a user-level select, so that path is covered permanently.

Now the full suite under the race detector, repeated across GOMAXPROCS settings, including the select-driven churn:

Every test passes 15 times at each GOMAXPROCS setting under `-race`. Because the failure was rare, I'll also run the 400-iteration stall harness at two more settings, and check per-test durations for any slow outlier that would hint at a stall.

All clean at both settings, and no test ran slow. Next I'll remove the temporary harness, confirm the non-race suite including the allocation assertions, and then run the benchmarks.

The allocation assertions pass: Subscribe+Close, an idle `TryRecv`+`Ready`, and a publish to a parked fleet all measure zero allocations. Now the benchmarks, comparing against the linear per-subscriber design:

The results are honest and mixed. The caster is far better at churn: about 78 ns per subscribe/close regardless of standing-set size, against 2,130 ns for the linear design at 8,192 subscribers, with zero allocations against one per cycle. Send is slower, about 1.6–2× at every size on this 2-vCPU box.

Two costs stand out that I can fix without weakening the protocol:

1. **Every publish stores all 32 lane counters**, even when only one lane has members. Between publishes every counter is zero, so only populated lanes need a store. A lane bitmask makes this O(populated lanes).
2. **Parked-set words are packed**, so neighbouring subscriptions contend on one cache line. Padding each word and transposing the slot-to-word mapping spreads the arm RMWs across lines.

I'll apply both.

Next, format, vet, and the race suite, since I touched the layout and the lane logic:

All green. Now the benchmarks, to see what the two changes bought:

The padding and transposition did little. Send at 1 subscriber improved modestly, and 8–512 subscribers are roughly unchanged. Rather than guess again, I'll profile the 8-subscriber case and see where the time actually goes.

The profile points at the benchmark harness rather than the caster. Most of the time is in `selectgo` and its lock traffic, because each receiver selects on a single `stop` channel shared by every receiver. The linear baseline uses a plain `range ch`, which takes a cheaper path. Shared-channel select contention is real, but this comparison is unfair. I'll make both benchmarks use the simplest receive primitive each design offers: `<-sub.Ready()` for the caster, `range ch` for the baseline. Shutdown uses `Close`, which is itself part of the design.

With fair receives, the picture is clearer:

| Subscribers | SenseCaster Send | Linear Send | Ratio |
|---|---|---|---|
| 1 | ~600 ns | ~240 ns | 2.5× |
| 8 | ~2,770 ns | ~1,900 ns | 1.45× |
| 64 | ~21,000 ns | ~16,700 ns | 1.26× |
| 512 | ~160,000 ns | ~155,000 ns | ~parity |

The gap at small N is the cost of synchronous acknowledgement. The publisher must wait for each receiver to consume the value, so it pays a park/unpark round trip that a single unbuffered handoff avoids. At scale the caster reaches parity, and its churn is far cheaper.

Before finishing, I'll tighten two docs and a redundant branch in the test. Then I'll rerun the stall harness, because the slot-to-word mapping changed after the last stall run.

The tool flagged that `sensecaster_test.go` changed on disk since I last read it, and my edit applied against content I don't remember. Before going further I'll read the file to see what else is there.

The file contains only the functions I wrote; the change notice most likely reflects my own edit, which is the latest modification. Nothing unexpected is present. Now the final verification pass: the stall harness again, since the mapping changed, plus the full suite.

The full matrix passes: `-race` at four GOMAXPROCS settings, normal runs, and the allocation assertions. Several edits went into the core file, so I'll reread the final version once for stale comments and layout references.

The remaining part of `sensecaster.go` reads consistently. I'll re-check the file header, then run the final checks before sending the files.

Format, vet, tests, and the race subset all pass. The one-iteration benchmark numbers above are setup-dominated, so I'll re-run the benchmarks at a steady duration and run the full race suite once more on the final files.

The final checks pass. Sending the four Go files now.

I've sent the four drop-in files for package `bigbuff`: `sensecaster.go`, `sensecaster_test.go`, `sensecaster_alloc_test.go` (`!race` build tag), and `sensecaster_bench_test.go`. I couldn't reach `/Users/joeyc/dev/go-bigbuff` from the sandbox, so I built and tested everything as a standalone module on linux/amd64 with 2 vCPUs. Nothing is committed.

## Design

`SenseCaster[V]` is a synchronous, exactly-once broadcast. `Send` returns only after every subscription registered when it began has received the value or been closed, so backpressure matches an unbuffered channel. Four mechanisms do the work:

1. **Value stored once, copied out by the receiver.** `Send` writes one `c.value`. An owed subscriber claims a copy-out window with a CAS on its own word, copies the value, and acknowledges with one store. The publisher never sends on a channel on the data path.
2. **Parked-set bitmap.** A subscriber with nothing to read sets its bit in a three-level bitmap (segment summary, word, bit). `Send` sweeps it with swaps and posts non-blocking tokens only to parked owners. Publish cost is roughly O(parked + subscriptions/4096), not O(subscriptions).
3. **Sense-reversing acknowledgement.** Each publish flips one global bit. A subscription is owed the in-flight value exactly when its own sense bit differs from the global one. Acknowledgements are sharded across 32 padded lanes, a `laneMask` keeps publish from touching empty lanes, and the last acknowledgement releases the publisher through a capacity-1 channel.
4. **Epoch-tagged handles.** A slot word holds `epoch<<2 | reading | sense`. `Subscription` is a 24-byte value `{caster, slot, epoch}`. `Close` retires a tenancy by bumping the epoch, and slots recycle through a LIFO free list. Subscribe and Close allocate nothing once the caster reaches its high-water mark.

Parking uses a Dekker-style arm-then-recheck to avoid lost wakeups.

The individual techniques are known: sense-reversing barriers, hierarchical bitmaps, generational handles. I haven't surveyed prior art, so I can't say the combination is unprecedented. What it provides is a select-compatible, synchronous, exactly-once broadcast whose subscribe and close cost doesn't depend on the number of subscribers.

## API

```go
caster := NewSenseCaster[int]()
sub := caster.Subscribe()
defer sub.Close()

caster.Send(42) // returns the number of subscriptions addressed

v, err := sub.Recv(ctx)       // or: for v := range sub.All(ctx) { ... }
```

For select, use `TryRecv` in a loop and select on `Ready`:

```go
for {
    if v, ok := sub.TryRecv(); ok {
        handle(v)
        continue
    }
    if sub.Closed() {
        return
    }
    select {
    case <-ctx.Done():
        return
    case <-sub.Ready():
    }
}
```

The zero `SenseCaster` is usable. The zero `Subscription` is closed. `Close` is idempotent, safe from any goroutine, and releases a value it still owes.

## Verification

- `gofmt` and `go vet` are clean.
- `go test -race -count=5 -cpu=1,2,4` passes.
- The allocation test confirms zero allocations for Subscribe+Close, for idle TryRecv+Ready, and for Send to a parked fleet of 16.
- Churn tests close subscriptions from a second goroutine and churn under `select`. Stale-handle and shared-subscription tests cover the edge cases.

## A bug found and fixed

Under `-race` a liveness stall appeared in about 1 run in 12. A watchdog harness showed an owed slot with an empty ready channel. A retired handle, closed between its check and its read, shared the slot's ready channel with the new tenant. It consumed the sweep's wake token and then returned on epoch mismatch. The fix is in `TryRecv`'s retired branch: it forwards the token with `wake()`. After the fix, 400 harness iterations at GOMAXPROCS 2, 4 and 8 showed no stalls.

## Benchmarks

These are steady-state results at `-benchtime=1s` on a 2-vCPU Xeon VM, not a Mac, so read the absolute numbers as relative.

| Benchmark | SenseCaster | Linear baseline |
|---|---|---|
| Send, 1 subscriber | 618 ns, 0 allocs | 266 ns, 0 allocs |
| Send, 8 subscribers | 2,791 ns | 1,907 ns |
| Send, 64 subscribers | 22,258 ns | 16,611 ns |
| Send, 512 subscribers | 190,958 ns | 156,261 ns |
| Churn beside 8 standing | 75 ns, 0 allocs | 85 ns, 1 alloc (112 B) |
| Churn beside 512 standing | 75 ns, 0 allocs | 222 ns, 1 alloc (112 B) |
| Churn beside 8192 standing | 76 ns, 0 allocs | 1,802 ns, 1 alloc (112 B) |

Send trails the linear design by about 2.3× with one subscriber, narrowing to about 1.2–1.5× between 8 and 512. The small-N gap is inherent: a synchronous publish waits for a round-trip acknowledgement, while one unbuffered send to one receiver is a single handoff. Churn is where this design clearly wins. It stays flat as the standing population grows and allocates nothing, while the linear design's unsubscribe scan grows with N.

## Known limits

- A subscription that owes a value and is never read blocks `Send` until it is closed, the same as an unread unbuffered channel.
- Don't call `Send` from a goroutine that holds an undrained subscription; that deadlocks.
- A wake-up from `Ready()` that is consumed without a following `TryRecv` is lost for that subscription, so always call `TryRecv` once `Ready` fires.
- `Send` isn't cancellable by context. That could be added by detaching the in-flight publish, but I haven't implemented it.
- Copies of one `Subscription` share a single stream, and each value reaches exactly one caller.
- Slot storage costs roughly 200 B per subscriber at the high-water mark.

---------

````txtar
-- sensecaster_test.go --
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSenseCasterZeroValue(t *testing.T) {
	var c SenseCaster[int]
	if n := c.Send(1); n != 0 {
		t.Fatalf("Send with no subscriptions = %d, want 0", n)
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}

	var s Subscription[int]
	if !s.Closed() {
		t.Fatal("zero Subscription should report Closed")
	}
	if _, ok := s.TryRecv(); ok {
		t.Fatal("zero Subscription yielded a value")
	}
	if s.Ready() != nil {
		t.Fatal("zero Subscription should have a nil Ready channel")
	}
	s.Close()
	if _, err := s.Recv(context.Background()); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("Recv on zero Subscription: err = %v, want ErrSubscriptionClosed", err)
	}
}

// sensecasterOrderedDelivery checks the core contract: every subscription sees every
// publish, exactly once, in order, and Send addresses exactly the registered set.
func sensecasterOrderedDelivery(t *testing.T, subs, msgs int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := NewSenseCaster[int]()
	handles := make([]Subscription[int], subs)
	for i := range handles {
		handles[i] = c.Subscribe()
	}
	if got := c.Subscribers(); got != subs {
		t.Fatalf("Subscribers() = %d, want %d", got, subs)
	}

	errs := make(chan error, subs)
	var wg sync.WaitGroup
	for i, sub := range handles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			for want := range msgs {
				v, err := sub.Recv(ctx)
				if err != nil {
					errs <- fmt.Errorf("subscription %d: message %d: %w", i, want, err)
					return
				}
				if v != want {
					errs <- fmt.Errorf("subscription %d: got %d, want %d", i, v, want)
					return
				}
				if (i+want)%5 == 0 {
					runtime.Gosched() // interleave busy and parked receivers
				}
			}
		}()
	}

	for i := range msgs {
		if n := c.Send(i); n != subs {
			t.Fatalf("Send(%d) addressed %d subscriptions, want %d", i, n, subs)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() after drain = %d, want 0", got)
	}
	if n := c.Send(-1); n != 0 {
		t.Fatalf("Send after drain = %d, want 0", n)
	}
}

func TestSenseCasterOrderedExactlyOnce(t *testing.T) {
	sensecasterOrderedDelivery(t, 24, 3000)
}

func TestSenseCasterManySubscriptions(t *testing.T) {
	// Crosses two and a half parked-set segments, so the summary tier is exercised.
	sensecasterOrderedDelivery(t, 10000, 3)
}

func TestSenseCasterSelectIntegration(t *testing.T) {
	c := NewSenseCaster[string]()
	sub := c.Subscribe()
	quit := make(chan struct{})
	exited := make(chan struct{})
	got := make(chan string, 8)

	go func() {
		defer close(exited)
		defer sub.Close()
		for {
			if v, ok := sub.TryRecv(); ok {
				got <- v
				continue
			}
			select {
			case <-sub.Ready():
			case <-quit:
				return
			}
		}
	}()

	want := []string{"alpha", "beta", "gamma", "delta"}
	for _, w := range want {
		if n := c.Send(w); n != 1 {
			t.Fatalf("Send(%q) = %d, want 1", w, n)
		}
	}
	for _, w := range want {
		select {
		case g := <-got:
			if g != w {
				t.Fatalf("got %q, want %q", g, w)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %q", w)
		}
	}
	close(quit)
	<-exited
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

// TestSenseCasterChurn races subscribe, receive, and close, with some closes arriving
// from a second goroutine mid-receive. Within each subscription, values must be
// consecutive from its first receive onward. No close may deadlock a publisher.
func TestSenseCasterChurn(t *testing.T) {
	const churners, publishes = 6, 4000
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := NewSenseCaster[uint64]()
	var violations, received atomic.Int64
	var wg sync.WaitGroup
	for w := range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; parent.Err() == nil; r++ {
				sub := c.Subscribe()
				var closer sync.WaitGroup
				if r%5 == 4 {
					closer.Add(1)
					go func() {
						defer closer.Done()
						sub.Close() // races with this goroutine's receives
					}()
				}
				budget := 1 + (w+r)%7
				var last uint64
				for j := 0; j < budget; j++ {
					v, err := sub.Recv(parent)
					if err != nil {
						if parent.Err() == nil && !sub.Closed() {
							violations.Add(1)
						}
						break
					}
					if j > 0 && v != last+1 {
						violations.Add(1)
					}
					last = v
					received.Add(1)
				}
				sub.Close()
				closer.Wait()
			}
		}()
	}

	// Publish until the churners have demonstrably received values, not merely for a fixed
	// count: sends made before anyone subscribes reach nobody and would exercise nothing.
	const minReceived = 1000
	for i := uint64(0); (i < publishes || received.Load() < minReceived) && parent.Err() == nil; i++ {
		c.Send(i)
		if i%8 == 0 {
			runtime.Gosched() // let churners interleave subscribe and close between publishes
		}
	}
	cancel()
	wg.Wait()

	if n := violations.Load(); n != 0 {
		t.Fatalf("%d ordering or lifecycle violations", n)
	}
	if n := received.Load(); n == 0 {
		t.Fatal("churners received nothing; the test exercised no delivery")
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() after churn = %d, want 0", got)
	}
}

// TestSenseCasterCloseReleasesOwedSubscription: a subscription that never reads must not
// hang the publisher forever. Closing it discards its value and completes the Send.
func TestSenseCasterCloseReleasesOwedSubscription(t *testing.T) {
	c := NewSenseCaster[int]()
	a := c.Subscribe()
	b := c.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res := make(chan int, 1)
	go func() { res <- c.Send(7) }()

	if v, err := b.Recv(ctx); err != nil || v != 7 {
		t.Fatalf("b.Recv = (%d, %v), want (7, nil)", v, err)
	}
	select {
	case <-res:
		t.Fatal("Send returned while subscription a still owed the value")
	case <-time.After(20 * time.Millisecond):
	}

	a.Close()
	select {
	case n := <-res:
		if n != 2 {
			t.Fatalf("Send addressed %d subscriptions, want 2", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Send did not complete after Close released the owed subscription")
	}
	b.Close()
}

func TestSenseCasterCloseWakesParkedRecv(t *testing.T) {
	c := NewSenseCaster[int]()
	s := c.Subscribe()
	errc := make(chan error, 1)
	go func() {
		_, err := s.Recv(context.Background())
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the receiver park; correctness does not depend on it
	s.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrSubscriptionClosed) {
			t.Fatalf("Recv after Close: err = %v, want ErrSubscriptionClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not wake the parked Recv")
	}
}

// TestSenseCasterStaleHandleIsInert: after its slot is recycled, an old handle must not
// read, acknowledge, or release the new tenant's subscription.
func TestSenseCasterStaleHandleIsInert(t *testing.T) {
	c := NewSenseCaster[int]()
	old := c.Subscribe()
	old.Close()
	fresh := c.Subscribe()
	if fresh.s != old.s {
		t.Fatal("expected LIFO slot reuse for this test to be meaningful")
	}

	old.Close()
	if !old.Closed() {
		t.Fatal("stale handle should report Closed")
	}
	if fresh.Closed() {
		t.Fatal("fresh subscription was closed through a stale handle")
	}
	if got := c.Subscribers(); got != 1 {
		t.Fatalf("Subscribers() = %d, want 1", got)
	}
	if _, ok := old.TryRecv(); ok {
		t.Fatal("stale handle received a value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := make(chan int, 1)
	go func() { res <- c.Send(5) }()
	if v, err := fresh.Recv(ctx); err != nil || v != 5 {
		t.Fatalf("fresh.Recv = (%d, %v), want (5, nil)", v, err)
	}
	if n := <-res; n != 1 {
		t.Fatalf("Send addressed %d, want 1", n)
	}
	fresh.Close()
}

// TestSenseCasterSharedSubscriptionDeliversOnce: copies of one subscription may receive
// concurrently. Each value must go to exactly one of them.
func TestSenseCasterSharedSubscriptionDeliversOnce(t *testing.T) {
	const readers, msgs = 4, 2000
	c := NewSenseCaster[int]()
	sub := c.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seen [msgs]atomic.Bool
	var dupes atomic.Int64
	var wg sync.WaitGroup
	for range readers {
		cp := sub
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v, err := cp.Recv(ctx)
				if err != nil {
					return
				}
				if !seen[v].CompareAndSwap(false, true) {
					dupes.Add(1)
				}
			}
		}()
	}

	for i := range msgs {
		if n := c.Send(i); n != 1 {
			t.Fatalf("Send(%d) addressed %d, want 1", i, n)
		}
	}
	cancel() // every Send has returned, so every value has been consumed
	wg.Wait()
	sub.Close()

	if n := dupes.Load(); n != 0 {
		t.Fatalf("%d values were delivered more than once", n)
	}
	for i := range seen {
		if !seen[i].Load() {
			t.Fatalf("value %d was never delivered", i)
		}
	}
}

// TestSenseCasterSelectChurn drives the subscription the way a user would: TryRecv,
// then a select over Ready and a separate exit channel, with closes from another
// goroutine. It covers the retired-waiter path that Recv alone rarely hits.
func TestSenseCasterSelectChurn(t *testing.T) {
	const churners, publishes = 6, 4000
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := NewSenseCaster[uint64]()
	var violations, received atomic.Int64
	var wg sync.WaitGroup
	for w := range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; parent.Err() == nil; r++ {
				sub := c.Subscribe()
				var closer sync.WaitGroup
				if r%3 == 2 {
					closer.Add(1)
					go func() {
						defer closer.Done()
						sub.Close()
					}()
				}
				budget := 1 + (w+r)%5
				var last uint64
				for j := 0; j < budget; {
					if v, ok := sub.TryRecv(); ok {
						if j > 0 && v != last+1 {
							violations.Add(1)
						}
						last = v
						j++
						received.Add(1)
						continue
					}
					if sub.Closed() {
						break // closed by the closer goroutine, or the test is ending
					}
					select {
					case <-sub.Ready():
					case <-parent.Done():
						j = budget // stop
					}
				}
				sub.Close()
				closer.Wait()
			}
		}()
	}

	const minReceived = 1000
	for i := uint64(0); (i < publishes || received.Load() < minReceived) && parent.Err() == nil; i++ {
		c.Send(i)
		if i%8 == 0 {
			runtime.Gosched()
		}
	}
	cancel()
	wg.Wait()

	if n := violations.Load(); n != 0 {
		t.Fatalf("%d ordering violations", n)
	}
	if n := received.Load(); n == 0 {
		t.Fatal("churners received nothing")
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() after churn = %d, want 0", got)
	}
}
-- sensecaster.go --
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
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrSubscriptionClosed is returned by Subscription.Recv once the subscription has been closed.
var ErrSubscriptionClosed = errors.New("bigbuff: sensecaster: subscription closed")

// SenseCaster is a synchronous, select-compatible broadcast. Each value passed to
// Send is delivered exactly once to every Subscription registered when Send begins,
// and Send returns only after each of them has received the value or been closed.
// Backpressure therefore matches an unbuffered channel: a subscriber that stops
// receiving stalls publishers.
//
// The work is arranged differently from a shared-channel caster:
//
//   - The value is stored once. Owed subscriptions copy it out themselves, so a busy
//     subscriber costs the publisher only an atomic acknowledgement. No channel send
//     sits on the data path.
//   - A publish touches only parked subscriptions. A subscription with nothing to read
//     advertises that in a three-level bitmap (segment summary, word, bit). The publisher
//     sweeps the bitmap and posts a non-blocking token to each owner it finds. A publish
//     therefore costs O(parked + subscriptions/4096), not O(subscriptions).
//   - Acknowledgement is sense-reversing. Each publish flips one global bit. A
//     subscription is owed the in-flight value exactly when its own sense bit differs
//     from the global one. Joining and leaving need no per-publish bookkeeping.
//
// Subscribe, Close, TryRecv and Send perform no heap allocation once the caster has
// reached its high-water mark of concurrent subscriptions. The zero value is ready to use.
//
// Every subscription must eventually be drained or closed. A subscription that owes a
// value and is never read blocks Send, as an unread unbuffered channel would.
type SenseCaster[V any] struct {
	sendMu sync.Mutex // one publish in flight at a time
	mu     sync.Mutex // guards topology, lane setup, the sense flip and the sweep

	sense atomic.Uint64 // flipped once per publish; the publication point
	_     [56]byte

	lanesLeft atomic.Int64 // lanes that still owe acknowledgements for the in-flight publish
	_         [56]byte

	active atomic.Int64 // mirror of nActive for the lock-free empty fast path
	_      [56]byte

	lanes [scLanes]scLane

	// Guarded by mu.
	nActive    int
	laneActive [scLanes]int
	laneMask   uint64 // bit l set iff laneActive[l] != 0; lets Send touch only populated lanes
	slots      []*scSlot
	segs       []*scSeg
	free       []int
	done       chan struct{} // capacity 1; signalled when lanesLeft reaches zero
	value      V             // in-flight value: written under mu, read only by owed subscriptions
}

const (
	scWordBits = 64
	scWords    = 64
	scSegSlots = scWordBits * scWords // 4096 subscriptions per parked-set segment
	scLanes    = 32                   // outstanding-acknowledgement lanes

	scSense   uint64 = 1 << 0 // a subscription's sense; equals the caster's once acknowledged
	scReading uint64 = 1 << 1 // a receiver is copying the value out; closers wait for it
	scEpochSh        = 2      // the epoch occupies the bits above the two flags
)

// scLane is an outstanding-acknowledgement counter padded to its own cache line.
type scLane struct {
	owed atomic.Int64
	_    [56]byte
}

// scWord is one word of the parked set, padded so that words never share a cache line.
type scWord struct {
	atomic.Uint64
	_ [56]byte
}

// scSeg is one segment of the parked set: 64 padded words of 64 slot bits, plus a summary
// word whose bit w means "words[w] may be non-zero".
type scSeg struct {
	summary atomic.Uint64
	_       [56]byte
	words   [scWords]scWord
}

// scSlot is the reusable cell behind a subscription. Slots are never freed. They cycle
// through the free list, and the epoch in word tells one tenancy from the next.
type scSlot struct {
	word  atomic.Uint64 // epoch<<scEpochSh | scReading | scSense
	ready chan struct{} // capacity 1; carries wake-up tokens only, never closed
	index int
	lane  int

	park    *atomic.Uint64 // words[w] of this slot's segment
	parkBit uint64
	sum     *atomic.Uint64 // summary word of this slot's segment
	sumBit  uint64
}

// arm advertises that the owner is about to wait. Only arm sets bits; only sweeps clear
// them. A bit already observed set is pending for a sweep that has not yet run, and that
// sweep will wake this slot. The caller re-checks the sense after arming, so a publish
// that raced the arm is never slept through.
func (sl *scSlot) arm() {
	if sl.park.Load()&sl.parkBit == 0 {
		sl.park.Or(sl.parkBit)
	}
	if sl.sum.Load()&sl.sumBit == 0 {
		sl.sum.Or(sl.sumBit)
	}
}

// wake posts a wake-up token without blocking. Tokens coalesce, and a spurious token
// costs only one extra TryRecv.
func (sl *scSlot) wake() {
	select {
	case sl.ready <- struct{}{}:
	default:
	}
}

// NewSenseCaster returns an empty caster. The zero value is also ready to use.
func NewSenseCaster[V any]() *SenseCaster[V] {
	return new(SenseCaster[V])
}

// Subscribe registers a new subscription. It is not owed the value of any publish that
// is already in flight.
func (c *SenseCaster[V]) Subscribe() Subscription[V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done == nil {
		c.done = make(chan struct{}, 1)
	}
	var sl *scSlot
	if n := len(c.free); n > 0 {
		sl = c.slots[c.free[n-1]]
		c.free = c.free[:n-1]
	} else {
		sl = c.grow()
	}
	epoch := sl.word.Load() >> scEpochSh
	// Adopting the current sense is what keeps a joiner from being owed the in-flight value.
	sl.word.Store((epoch << scEpochSh) | (c.sense.Load() & scSense))
	c.laneActive[sl.lane]++
	c.laneMask |= uint64(1) << sl.lane
	c.nActive++
	c.active.Store(int64(c.nActive))
	return Subscription[V]{c: c, s: sl, epoch: epoch}
}

// grow allocates a fresh slot. It runs under mu. The slot table and the parked set grow
// together, so a sweep never meets a bit without a slot to wake.
func (c *SenseCaster[V]) grow() *scSlot {
	i := len(c.slots)
	si, local := i/scSegSlots, i%scSegSlots
	if si == len(c.segs) {
		c.segs = append(c.segs, new(scSeg))
	}
	seg := c.segs[si]
	// Transposed mapping: neighbouring subscriptions land in different words, so their
	// concurrent arms contend on different cache lines.
	w, b := local%scWords, local/scWords
	sl := &scSlot{
		ready:   make(chan struct{}, 1),
		index:   i,
		lane:    i % scLanes,
		park:    &seg.words[w].Uint64,
		parkBit: uint64(1) << b,
		sum:     &seg.summary,
		sumBit:  uint64(1) << w,
	}
	c.slots = append(c.slots, sl)
	return sl
}

// Subscribers reports the number of open subscriptions.
func (c *SenseCaster[V]) Subscribers() int {
	return int(c.active.Load())
}

// Send publishes v to every subscription registered when Send begins and returns the
// number of subscriptions it was addressed to. Send blocks until each of them has
// received v or been closed. A subscription closed before receiving discards v. Do not
// call Send from a goroutine that holds an undrained subscription.
func (c *SenseCaster[V]) Send(v V) int {
	if c.active.Load() == 0 {
		return 0 // no subscriptions (fast path)
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	n := c.nActive
	if n == 0 {
		c.mu.Unlock()
		return 0
	}
	c.value = v
	// Between publishes every owed counter is zero, so only populated lanes need a store.
	var lanes int64
	for m := c.laneMask; m != 0; m &= m - 1 {
		l := bits.TrailingZeros64(m)
		c.lanes[l].owed.Store(int64(c.laneActive[l]))
		lanes++
	}
	c.lanesLeft.Store(lanes)
	c.sense.Store(c.sense.Load() ^ scSense) // publication point: owners observe this flip
	c.sweep()
	c.mu.Unlock()

	for c.lanesLeft.Load() != 0 {
		<-c.done
	}
	var zero V
	c.value = zero // drop the reference; nothing reads it once every lane has drained
	return n
}

// sweep wakes every parked owner. Each bit is taken with a swap, so an owner is woken at
// most once per arm. An owner that finds nothing to read arms again.
func (c *SenseCaster[V]) sweep() {
	for si, seg := range c.segs {
		if seg.summary.Load() == 0 {
			continue
		}
		for s := seg.summary.Swap(0); s != 0; s &= s - 1 {
			w := bits.TrailingZeros64(s)
			for m := seg.words[w].Swap(0); m != 0; m &= m - 1 {
				c.slots[si*scSegSlots+bits.TrailingZeros64(m)*scWords+w].wake()
			}
		}
	}
}

// ack records that one owed subscription has acknowledged. The last acknowledgement in
// the last lane releases the publisher.
func (c *SenseCaster[V]) ack(lane int) {
	n := c.lanes[lane].owed.Add(-1)
	if n < 0 {
		panic("bigbuff: sensecaster: acknowledgement underflow")
	}
	if n == 0 && c.lanesLeft.Add(-1) == 0 {
		select {
		case c.done <- struct{}{}:
		default:
		}
	}
}

// Subscription is a receive endpoint on a SenseCaster. It is a small value: copies refer
// to the same subscription. TryRecv, Recv and Close may be called concurrently from any
// goroutine, and each value is delivered to exactly one caller.
type Subscription[V any] struct {
	c     *SenseCaster[V]
	s     *scSlot
	epoch uint64
}

// Closed reports whether the subscription has been closed. The zero value is closed.
func (s Subscription[V]) Closed() bool {
	return s.c == nil || s.s.word.Load()>>scEpochSh != s.epoch
}

// Ready returns a channel that becomes receivable when the subscription may have a value
// to read, or when it has been closed. Ready arms the wake-up itself, so it is safe to
// select on directly. After it fires, call TryRecv: a wake-up consumed without a
// following TryRecv is lost to this subscription. For the zero value it returns nil.
func (s Subscription[V]) Ready() <-chan struct{} {
	if s.c == nil {
		return nil
	}
	s.s.arm()
	if (s.s.word.Load()^s.c.sense.Load())&scSense != 0 {
		s.s.wake() // a publish raced the arm, so its token is ours to post
	}
	return s.s.ready
}

// TryRecv returns the value owed to this subscription without blocking. When there is
// none it arms the wake-up and returns false. It also returns false once the
// subscription is closed; see Closed.
func (s Subscription[V]) TryRecv() (V, bool) {
	var zero V
	c := s.c
	if c == nil {
		return zero, false
	}
	sl := s.s
	for {
		w := sl.word.Load()
		if w>>scEpochSh != s.epoch {
			// Retired. This handle may have just consumed a wake-up meant for the slot's
			// current tenant, which can be waiting on the same channel. Pass it on, so a
			// retired waiter can never swallow a wake-up.
			sl.wake()
			return zero, false
		}
		if w&scReading != 0 {
			runtime.Gosched() // another goroutine is inside its copy-out window
			continue
		}
		if (w^c.sense.Load())&scSense != 0 {
			// Owed. Claim the copy-out window. Closers observe scReading and wait for us.
			if !sl.word.CompareAndSwap(w, w|scReading) {
				continue
			}
			v := c.value
			sl.word.Store(w ^ scSense) // clear the claim and acknowledge in one store
			c.ack(sl.lane)
			return v, true
		}
		sl.arm()
		// Dekker-style re-check. A publish that flipped the sense before our arm became
		// visible may have been swept past us, so look again before sleeping.
		if (sl.word.Load()^c.sense.Load())&scSense == 0 {
			return zero, false
		}
	}
}

// Recv blocks until a value is available, the context ends, or the subscription is
// closed. It returns ErrSubscriptionClosed in the last case.
func (s Subscription[V]) Recv(ctx context.Context) (V, error) {
	var zero V
	for {
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		if s.Closed() {
			return zero, ErrSubscriptionClosed
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-s.Ready():
		}
	}
}

// All yields values until the context ends or the subscription is closed.
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

// Close releases the subscription. A value it still owes is discarded on its behalf, so
// a blocked Send is released. Close is idempotent. It may be called from any goroutine,
// including while another goroutine is inside TryRecv or Recv, and it wakes any Recv
// parked on this subscription.
func (s Subscription[V]) Close() {
	c := s.c
	if c == nil {
		return
	}
	sl := s.s
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		w := sl.word.Load()
		if w>>scEpochSh != s.epoch {
			return // already closed
		}
		if w&scReading != 0 {
			runtime.Gosched() // a receiver is copying the value out and will acknowledge on exit
			continue
		}
		if (w^c.sense.Load())&scSense != 0 {
			// Owed. Acknowledge on the owner's behalf. The value is discarded, so nothing
			// else needs protecting beyond the reading check above.
			if sl.word.CompareAndSwap(w, w^scSense) {
				c.ack(sl.lane)
			}
			continue
		}
		// Not owed, and the sense cannot change while we hold mu. Retire this tenancy.
		if sl.word.CompareAndSwap(w, (s.epoch+1)<<scEpochSh) {
			break
		}
	}
	c.nActive--
	c.laneActive[sl.lane]--
	if c.laneActive[sl.lane] == 0 {
		c.laneMask &^= uint64(1) << sl.lane
	}
	c.active.Store(int64(c.nActive))
	c.free = append(c.free, sl.index)
	sl.wake() // unblock a parked Recv; a stale token is harmless
}
-- sensecaster_bench_test.go --
package bigbuff

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// benchSink keeps receiver accumulators observable so the compiler cannot elide them.
var benchSink atomic.Int64

// linearCaster is the reference "linear" design: one unbuffered channel per subscriber,
// sent to in sequence. Select works, but every publish is O(subscribers) channel
// handoffs in series, and unsubscribing scans the list.
type linearCaster[V any] struct {
	mu    sync.Mutex
	chans []chan V
}

func (l *linearCaster[V]) subscribe() chan V {
	ch := make(chan V)
	l.mu.Lock()
	l.chans = append(l.chans, ch)
	l.mu.Unlock()
	return ch
}

func (l *linearCaster[V]) unsubscribe(ch chan V) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, c := range l.chans {
		if c == ch {
			last := len(l.chans) - 1
			l.chans[i] = l.chans[last]
			l.chans = l.chans[:last]
			return
		}
	}
}

func (l *linearCaster[V]) send(v V) {
	l.mu.Lock()
	chans := l.chans
	l.mu.Unlock()
	for _, ch := range chans {
		ch <- v
	}
}

func (l *linearCaster[V]) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.chans {
		close(ch)
	}
	l.chans = nil
}

var benchSubCounts = []int{1, 8, 64, 512}

// The receivers use the simplest receive each design offers: a single-case receive on
// Ready (for the caster) and a plain range (for the baseline). Shutdown is Close, which
// is part of the design under test. A multi-case select over a shared stop channel
// would measure selectgo's channel-lock contention rather than the caster.
func BenchmarkSenseCasterSend(b *testing.B) {
	for _, n := range benchSubCounts {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			c := NewSenseCaster[int]()
			subs := make([]Subscription[int], n)
			for i := range subs {
				subs[i] = c.Subscribe()
			}
			var wg sync.WaitGroup
			for i := range subs {
				sub := subs[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					local := 0
					for {
						if v, ok := sub.TryRecv(); ok {
							local += v
							continue
						}
						if sub.Closed() {
							benchSink.Add(int64(local))
							return
						}
						<-sub.Ready()
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
			}
			b.StopTimer()
			for _, s := range subs {
				s.Close()
			}
			wg.Wait()
		})
	}
}

func BenchmarkLinearCasterSend(b *testing.B) {
	for _, n := range benchSubCounts {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			var l linearCaster[int]
			var wg sync.WaitGroup
			for range n {
				ch := l.subscribe()
				wg.Add(1)
				go func() {
					defer wg.Done()
					local := 0
					for v := range ch {
						local += v
					}
					benchSink.Add(int64(local))
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				l.send(i)
			}
			b.StopTimer()
			l.closeAll()
			wg.Wait()
		})
	}
}

// Churn: replace one standing subscriber per iteration, with the standing set held at n.
func BenchmarkSenseCasterChurn(b *testing.B) {
	for _, n := range []int{8, 512, 8192} {
		b.Run(fmt.Sprintf("standing=%d", n), func(b *testing.B) {
			c := NewSenseCaster[int]()
			standing := make([]Subscription[int], n)
			for i := range standing {
				standing[i] = c.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				j := i % n
				standing[j].Close()
				standing[j] = c.Subscribe()
			}
			b.StopTimer()
			for _, s := range standing {
				s.Close()
			}
		})
	}
}

func BenchmarkLinearCasterChurn(b *testing.B) {
	for _, n := range []int{8, 512, 8192} {
		b.Run(fmt.Sprintf("standing=%d", n), func(b *testing.B) {
			var l linearCaster[int]
			standing := make([]chan int, n)
			for i := range standing {
				standing[i] = l.subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				j := i % n
				l.unsubscribe(standing[j])
				standing[j] = l.subscribe()
			}
			b.StopTimer()
			l.closeAll()
		})
	}
}
-- sensecaster_alloc_test.go --
//go:build !race

package bigbuff

import (
	"sync"
	"testing"
)

// The race detector adds its own allocations, so these assertions only hold without -race.
func TestSenseCasterAllocationFree(t *testing.T) {
	c := NewSenseCaster[int]()
	warm := make([]Subscription[int], 64)
	for i := range warm {
		warm[i] = c.Subscribe()
	}
	for i := range warm {
		warm[i].Close()
	}

	if n := testing.AllocsPerRun(1000, func() {
		s := c.Subscribe()
		s.Close()
	}); n != 0 {
		t.Errorf("Subscribe+Close allocates %.2f per cycle, want 0", n)
	}

	idle := c.Subscribe()
	if n := testing.AllocsPerRun(1000, func() {
		_, _ = idle.TryRecv()
		_ = idle.Ready()
	}); n != 0 {
		t.Errorf("idle TryRecv+Ready allocates %.2f per call, want 0", n)
	}
	idle.Close()

	// A select-driven fleet, most of it parked, so each publish takes the wake path.
	const fleet = 16
	c2 := NewSenseCaster[int]()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range fleet {
		sub := c2.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, ok := sub.TryRecv(); ok {
					continue
				}
				select {
				case <-sub.Ready():
				case <-stop:
					return
				}
			}
		}()
	}
	n := testing.AllocsPerRun(500, func() { c2.Send(1) })
	close(stop)
	wg.Wait()
	if n != 0 {
		t.Errorf("Send to a parked fleet of %d allocates %.2f per publish, want 0", fleet, n)
	}
}

````
