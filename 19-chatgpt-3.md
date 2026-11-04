I implemented **two complementary designs** that preserve native `select` and constant broadcaster-owned storage while removing the blocking post-receive barrier:

- **`PhasePubSub`** uses two permanent channels and two shared value slots. The next publication supplies the synchronization needed to reuse the previous slot. Its reader path adds **no mutex acquisition and no atomic operation** beyond the channel receive.
- **`RelayPubSub`** uses two permanent notification channels, one shared value slot, and a configurable number of circulating credits. The publisher issues only `min(width, subscribers)` notifications; readers distribute the remainder. `Send` returns after every recipient has copied the value or unsubscribed.

The measurements give both designs a reason to exist. Phase substantially improves large direct-receive broadcasts. A **width-one relay** performs particularly well when many consumers select against a shared cancellation channel.

**Deliverables at the end of this files.** These are new files for the `bigbuff` package. `/Users/joeyc/dev/go-bigbuff` was not mounted in this environment, so I built and tested a self-contained module from the implementations you supplied.

## 1. The substantive change: make duplicate exclusion local

Your `ChanPubSub` must prevent a fast subscriber from receiving twice during one broadcast. With everyone receiving from the same channel, the post-receive `Wait` supplies that exclusion.

The new designs give each subscriber a small, caller-owned handle containing its broadcaster and expected publication phase. After receiving a publication, that handle switches to the other shared channel:

- A subscriber awaiting publication `E` receives on one channel.
- After `Read`, it awaits `E+1` on the other channel.
- It cannot consume another notification from `E`.
- A subscriber joining during `E` starts on the channel for `E+1`.

This removes the reason for the blocking `Wait`.

There is still no package-owned subscriber registry. Registration creates a value handle and updates membership under a short mutex. It does not allocate a channel, register a callback, create a worker, or insert a heap object into a collection.

The further step is to make the same alternation solve **value lifetime**, which is where `PhasePubSub` becomes more interesting than simply alternating two transports.

## 2. `PhasePubSub`: the intervening publication protects the previous value

`PhasePubSub` has:

- Two permanent **unbuffered** `chan struct{}` channels.
- Two shared `V` slots.
- A publisher mutex.
- A short membership mutex.
- An atomic subscriber count for the empty-send fast path.
- A local publication number in each subscription.

Its publisher takes a membership snapshot, writes one shared slot, and performs exactly one notification send for each snapshot member. Cancellation settles an owed notification by receiving and discarding it.

The essential reader operation is:

```go
value := s.pub.value[s.epoch&1]
s.epoch++
return value
```

That is the complete synchronization-independent part of the reader path: copy the value and advance local state. The implementation also checks for an inactive handle.

### Why the shared value cannot be overwritten prematurely

Suppose publication `E` uses slot zero.

A subscriber can receive its notification and then be preempted before calling `Read`. Consequently, completing publication `E` does **not** establish that slot zero can be overwritten.

The publisher can, however, begin `E+1`, using slot one.

Before the delayed subscriber can receive `E+1`, it must finish copying `E` and advance its handle. Because `E+1` uses an unbuffered channel, its receive is ordered before completion of the corresponding send. That reverse synchronization guarantee is part of Go’s memory model. [The Go Programming Language](https://go.dev/ref/mem?utm_source=chatgpt.com)

Applying that guarantee to this implementation gives:

1. The subscriber copies `E`.
2. It advances and receives the notification for `E+1`.
3. The publisher completes that unbuffered handoff.
4. After completing all `E+1` handoffs, the serialized publisher can begin `E+2`.
5. Only then does it overwrite the slot previously used by `E`.

**The intervening publication is the acknowledgement needed to reclaim the older slot.**

A departing subscriber supplies equivalent ordering through its mutex-protected removal, or through the intervening notification that its cancellation drains.

That gives a useful combination:

- No reader acknowledgement counter.
- No condition variable.
- No second reader blocking point.
- No per-publication channel allocation.
- No subscriber traversal.
- Safe shared-slot reuse.

The constructor fixes the notification channels as unbuffered. Making them buffered would invalidate this reclamation argument.

### Completion semantics

`PhasePubSub.Send` returns after all notification handoffs have completed, excluding those consumed by cancellation. Some corresponding `Read` calls may still be pending.

That is a deliberate distinction from the supplied `ChanPubSub`, whose acknowledgement phase establishes that recipients have already received their `V` values.

Phase can therefore overlap the beginning of the next publication with delayed copies of the preceding publication, while the two-slot rule prevents overwrite.

It retains at most two payload values while subscriptions remain. Both slots are cleared when membership reaches zero safely. Clearing an older slot after every publication would also be possible, but would add another payload-sized write; the implementation avoids that work.

### Membership changes stay simple

The local publication number also makes in-flight cancellation much easier to identify:

- A handle still expecting the active publication owes one notification.
- A handle already advanced to the following publication owes none.
- A newly registered handle also expects the following publication and owes none.

An owing cancellation records itself under the membership mutex **before** draining its notification. The publisher consequently cannot finish the last handoff before that cancellation is reflected in its return count.

There is no `TryRLock` polling loop and no nested `ChanCaster` state machine in the final implementation.

## 3. `RelayPubSub`: distribute notification work through reusable credits

Phase still makes the publisher issue all `N` notifications.

Relay changes who performs that work. It uses one shared value slot, two buffered `chan struct{}` notification channels, an atomic remaining-participant count, and a permanent completion channel.

For a publication with `N` snapshot members:

```text
W = configured width
K = min(W, N)
```

The publisher writes the value, initializes `remaining = N`, and sends `K` credits.

Each receiver:

1. Copies the shared value.
2. Advances its local phase.
3. Decrements `remaining`.
4. Forwards or retires its credit.

The implemented release rule is:

```go
r := x.remaining.Add(-1)

switch {
case r >= x.width:
    c <- struct{}{} // Forward the credit.

case r == 0:
    x.done <- struct{}{} // Final participant.

case r < 0:
    panic("bigbuff: relaypubsub: credit invariant violation")
}

// For 0 < r < width, retire the credit.
```

Here, `c` is the captured channel for the publication being completed. The receiver does not consult mutable publication state after releasing its participation.

### Why this particular retirement rule matters

The decrement results are exactly `N-1` through `0`.

Exactly `N-K` of them satisfy `r >= W`, so exactly `N-K` credits are forwarded:

\[
K\text{ initial credits}+(N-K)\text{ forwards}=N\text{ receives}.
\]

For example, with five subscribers and width two, decrement results `4`, `3`, and `2` forward their credits. Results `1` and `0` retire them, with zero additionally notifying the publisher. Two initial credits plus three forwards produce exactly five deliveries.

This remains valid when initial seeding and forwarding interleave, and when a goroutine is preempted between its decrement and forwarding send.

The subtle property is that **completion cannot leave a delayed old credit waiting to appear**. Reaching zero requires all `N` receives to have occurred. Those receives require all `N` initial-or-forwarded notifications to have become visible. An unissued credit would make completion impossible.

Alternating channels then prevent an already-served subscriber, or a subscriber joining the next publication, from consuming a second current-publication credit.

### Why `Read` does not wait for another subscriber

At most `K` credits exist, counting credits in the channel and credits held by readers.

A reader holding a credit necessarily leaves space for its forwarding send in a buffer of capacity `W >= K`. Initial seeding has the same property: an unissued seed means fewer than `K` credits have been introduced.

Therefore forwarding and seeding cannot park **because the notification buffer is full**. They still perform ordinary runtime channel synchronization.

That distinction lets `Read` finish without a receiver barrier.

Width controls circulating notifications and the initial issuance burst. It does **not** limit the number of application handlers that may run concurrently after their reads complete.

### Why the shared value is safe

Every value copy precedes its atomic decrement. The sequence of atomic read-modify-write operations joins those copies to the last decrement. The final completion-channel handoff then orders the publisher’s next write after the copies.

This uses Go’s documented sequentially consistent atomic synchronization, rather than architecture-specific assumptions or runtime internals. [The Go Programming Language](https://go.dev/ref/mem?utm_source=chatgpt.com)

Relay can consequently clear its single shared slot before `Send` returns.

Cancellation consumes a credit and follows exactly the same release protocol. It cannot merely decrement the count: doing that could leave a buffered or in-flight credit to contaminate a subsequent publication.

### The work reduction is specific

Relay performs:

- `K` publisher-issued notification sends.
- `N-K` receiver-issued forwarding sends.
- `N` reader/cancellation decrements.
- One completion send and receive.

The complete broadcast still requires work proportional to `N`. The improvement is **bounded publisher issuance and distributed notification work**, with reusable channels and constant broadcaster-owned storage.

The current Go runtime gives zero-sized buffered channels an especially useful representation: when element storage has zero size, channel construction allocates the channel header without a payload buffer proportional to capacity. Thus increasing the credit width does not allocate `W` payload slots in this runtime. [go.dev](https://go.dev/src/runtime/chan.go?utm_source=chatgpt.com)

## 4. A broader capability: several subscriptions in one event loop

Removing the blocking `Wait` also changes what applications can express.

One goroutine can own multiple subscriptions, including several subscriptions to the **same broadcaster**:

```go
a, b := bus.Subscribe(), bus.Subscribe()
defer a.Unsubscribe()
defer b.Unsubscribe()

for {
    select {
    case <-ctx.Done():
        return

    case <-a.C():
        handleA(a.Read())

    case <-b.C():
        handleB(b.Read())
    }
}
```

Initially, both cases may reference the same shared channel. Only the selected handle advances. The other handle remains eligible for that publication on the next iteration.

With the supplied blocking acknowledgement protocol, receiving one of those subscriptions and entering `Wait` would prevent the same goroutine from receiving the other subscription needed to complete the publication.

Both new implementations pass deterministic tests in which one goroutine selects across three subscriptions to the same broadcaster and receives every publication exactly once through each handle.

This is useful for event loops and multiplexers that want several logical subscriptions without a goroutine or channel for each logical subscription. Handlers still need to return so the event loop can continue receiving.

## 5. Measurements: substantial gains, with meaningful differences

The complete measurements are included in the linked design document and bundle. They were recorded on **Go 1.27.3, Linux/amd64, AMD EPYC 9V74**, with an eight-CPU cgroup quota.

The tables below show **median microseconds per publication**, using five samples at `GOMAXPROCS=8`.

Setup, registration, warmup, and teardown are outside the throughput timer. The timer includes a final join after all value copies and validation, so Phase’s earlier `Send` return does not exclude the final copies from the comparison.

The per-subscriber-channel reference is a static array of channels with a linear send loop. It isolates delivery performance; it does not purport to reproduce your unseen callback implementation or its churn behavior.

### Direct receives, `uint64` values

The relay column uses width one consistently.

| Subscribers | Supplied `ChanPubSub` | `PhasePubSub` | `RelayPubSub`, W=1 | Per-subscriber-channel reference |
|---:|---:|---:|---:|---:|
| 1 | 0.500 | **0.202** | 0.388 | 0.177 |
| 8 | 2.936 | 2.497 | **1.884** | 2.477 |
| 64 | 27.078 | 22.539 | **13.930** | 15.741 |
| 512 | 262.041 | 152.040 | **117.186** | 89.676 |
| 4,096 | 3,076.048 | **1,006.128** | 1,120.944 | 559.556 |

At 4,096 subscribers, Phase used approximately **one third** of the supplied implementation’s time. The per-subscriber-channel reference remained faster in that eight-P case.

Processor count changes the comparison. With one P and 4,096 subscribers, the medians were:

- Supplied implementation: **1.344 ms**
- Phase: **0.886 ms**
- Relay, width one: **1.175 ms**
- Per-subscriber channels: **1.208 ms**

Those results support the structural improvement without establishing one universal ranking.

### Native `select` with a shared cancellation channel

Every receiver selects between its subscription and the same never-cancelled shutdown channel.

The 64-subscriber row is a focused repeat using five **400 ms** samples. The 512-subscriber row comes from the larger matrix using five **100 ms** samples.

| Subscribers | Supplied `ChanPubSub` | `PhasePubSub` | `RelayPubSub`, W=1 | Per-subscriber-channel reference |
|---:|---:|---:|---:|---:|
| 64 | 45.912 | 46.976 | **17.886** | 31.519 |
| 512 | 937.136 | 577.491 | **161.023** | 255.192 |

The width-one relay was approximately **5.82× faster than the supplied implementation** in the 512-subscriber case.

Width matters substantially. At 512 subscribers, width four measured **623.773 µs**, compared with **161.023 µs** for width one. Choosing the width from `GOMAXPROCS` automatically would have been poorly justified by these results.

The initial shorter 64-subscriber run showed a larger Phase regression. The longer repeat narrowed the supplied-versus-Phase difference considerably, while retaining the narrow relay’s advantage. Both sets of samples are preserved.

My interpretation is that a narrow notification chain can reduce contention sufficiently to outweigh the extra dependency chain. Go’s `select` implementation locks the participating channels, so the channels shared by the select cases are a relevant part of the workload. The measurements establish the performance difference; they do not isolate its precise scheduler or lock-contention cause. [go.dev](https://go.dev/src/runtime/select.go?utm_source=chatgpt.com)

### Larger inline values

For a 1 KiB value and 512 subscribers, direct-receive medians were:

- Supplied implementation: **327.689 µs**
- Phase: **166.830 µs**
- Relay, width one: **164.519 µs**
- Per-subscriber channels: **138.285 µs**

Both new designs copy values from shared storage in reader code. That moves the payload copy out of the notification channel operation.

They also avoid requiring `V` to be a channel element type. Tests successfully broadcast **64 KiB inline array values** through both implementations.

## 6. Allocation and memory behavior

Construction measurements used `V=uint64`:

| Implementation | Bytes per construction | Allocations per construction | Idle subscribe/unsubscribe pair |
|---|---:|---:|---:|
| Supplied `ChanPubSub` | 408 B | 5 | 27.08 ns, 0 allocs |
| `PhasePubSub` | 304 B | 3 | 15.58 ns, 0 allocs |
| `RelayPubSub` | 416 B | 4 | 16.85 ns, 0 allocs |

Neither new implementation contains an explicit operation-time allocation in publication, subscription, `C`, `Read`, or unsubscription.

Across the measured publication configurations, both reported a median of **0 allocs/op**. Median bytes were zero except for the 4,096-subscriber, eight-P, width-one relay configuration, which reported **1 B/op**. Some individual samples also showed small nonzero byte counts.

The precise claim is therefore:

> **Fixed broadcaster construction, no explicit per-operation allocation, and no internal allocation proportional to subscriber churn.**

Compiler escape decisions, very large value temporaries, goroutine stacks, and runtime waiter allocation remain relevant. Go caches its waiter records, but a cache miss can allocate another record. Avoiding a second, package-owned registry does not eliminate that runtime state. [go.dev](https://go.dev/src/runtime/proc.go?utm_source=chatgpt.com)

There are three separate storage costs:

1. **Broadcaster-owned storage:** fixed channels, synchronization fields, and one or two `V` slots.
2. **Caller-owned subscriptions:** small value handles, which can be placed in existing objects or slices.
3. **Runtime execution state:** goroutines and blocked channel/select registrations.

Only the first is independent of subscriber count. The design does not hide per-subscriber channels or registry entries in a pool.

The allocation tests use warmed operations, and the concurrent benchmarks separately exercise one and eight Ps. That separation matters because `testing.AllocsPerRun` forces `GOMAXPROCS=1` and returns an integral average. [Go Packages](https://pkg.go.dev/testing?utm_source=chatgpt.com)

## 7. API and completion contracts

Both implementations expose the same subscription shape:

```go
sub := bus.Subscribe()

select {
case <-ctx.Done():
    sub.Unsubscribe()

case <-sub.C():
    value := sub.Read()
    use(value)
}
```

For a continuing subscription, place that select in a loop and defer `Unsubscribe`.

The important completion distinction is:

| Operation | What successful `Send` completion establishes |
|---|---|
| Supplied `ChanPubSub.Send` | Counted recipients received their values and completed the required acknowledgement participation |
| `PhasePubSub.Send` | Counted recipients received notifications; their value copies can still be pending |
| `RelayPubSub.Send` | Counted recipients copied the value |
| All three | Application processing may still be running |

Neither new implementation returns an internal value pointer. `Read` returns a copy of `V`.

For slices, maps, pointers, and structs containing references, this remains a shallow copy, just as with channel messages. Reusing referenced buffers after `Send` requires separate application synchronization or an immutable-payload convention.

### The subscription rules

The manual protocol is small, but essential:

- **Reevaluate `C()` after each `Read`.** The expected channel changes.
- **After receiving a notification, call `Read` exactly once before other subscription operations.** If cancellation becomes ready simultaneously, a notification already selected still needs its `Read`.
- **Each handle has one owner.** Do not duplicate a live handle or receive/read/unsubscribe through it concurrently.
- **Continue receiving or unsubscribe.** These are backpressured broadcasts; an abandoned subscription can stall publication.
- **Use cancellation for termination.** The internal notification channels remain open. An inactive handle returns a nil channel.

Registration during an active publication starts with the next publication and does not wait for the active one to finish. It can briefly contend on the membership mutex.

An unsubscribe that owes the current publication can block while draining its notification or credit. It never needs a future publication to begin. This preserves the same fundamental obligation as your current cancellation-by-receiving mechanism, with the obligation identified directly from the handle’s phase.

## 8. Verification performed

The complete package passed:

```sh
go test -race -cpu=1,2,8 -count=5 -timeout=90s ./...
go vet ./...
python3 model/credit_model.py
```

The suite contains **20 top-level correctness tests plus an example**, including:

- Ordered delivery and identical ordering across concurrent publishers.
- A receiver preempted between notification receipt and `Read`.
- Joining during an active publication.
- Unsubscribing before receipt and after an early receipt.
- Complete cancellation of a publication followed by channel reuse.
- Exact sent-versus-observed accounting under concurrent churn.
- Pointer-bearing payload visibility.
- Publication-number wraparound.
- 64 KiB value copies.
- Multiple subscriptions to one broadcaster serviced by one goroutine.
- Warmed allocation checks.

The independent credit checker exhaustively explored **50,011 abstract states** for `N=1..12` and every `K=1..N`. It separates seeding, receiving, copying, decrementing, and delayed forwarding, specifically checking the stale-credit and capacity concerns.

That checker covers the credit protocol for an enrolled publication. Membership admission, phase reuse, Go memory ordering, and scheduler fairness are separate parts of the reasoning and test coverage.

The package also compiled for **Darwin/arm64** and **Linux/386**. Those were compilation checks; execution and performance measurement were on Linux/amd64. The host could not execute the 386 binary, and that result is recorded explicitly.

The bundle includes **840 benchmark samples across 168 configurations**, plus the 25 longer select follow-up samples. It also includes the source hashes, environment details, raw outputs, and commands for reproducing the matrix.

For the select-heavy use you described, I would start by measuring **`NewRelayPubSub[V](1)`**. `PhasePubSub` is the complementary choice when the minimal reader path and notification-based completion fit the application. Both production files have only standard-library dependencies and can be added independently to the existing package.

## Deliverables

````txtar
-- broadcast_bench_test.go --
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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// All implementations use this same adapter, warmup, goroutine lifecycle and
// value validation. Setup, registration, and teardown are outside the timer.
// LinearChannels is a static per-subscriber-channel reference, not a claim to
// reproduce an unseen callback implementation or its churn behavior.
type broadcastBench[V any] struct {
	send        func(V) int
	receive     []func() V
	unsubscribe []func()
}

func makeBroadcastBench[V any](kind string, n int, done <-chan struct{}) broadcastBench[V] {
	x := broadcastBench[V]{receive: make([]func() V, n), unsubscribe: make([]func(), n)}
	switch kind {
	case "ChanPubSub":
		p := NewChanPubSub(make(chan V))
		x.send = p.Send
		c := p.C()
		for i := range n {
			p.Subscribe()
			x.receive[i] = func() V { v := <-c; p.Wait(); return v }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case v := <-c:
						p.Wait()
						return v
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = p.Unsubscribe
		}
	case "PhasePubSub":
		p := NewPhasePubSub[V]()
		x.send = p.Send
		for i := range n {
			s := p.Subscribe()
			x.receive[i] = func() V { <-s.C(); return s.Read() }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case <-s.C():
						return s.Read()
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = s.Unsubscribe
		}
	case "LinearChannels":
		cs := make([]chan V, n)
		for i := range n {
			c := make(chan V)
			cs[i] = c
			x.receive[i] = func() V { return <-c }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case v := <-c:
						return v
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = func() {}
		}
		x.send = func(v V) int {
			for _, c := range cs {
				c <- v
			}
			return n
		}
	default:
		var width int
		if _, err := fmt.Sscanf(kind, "RelayW%d", &width); err != nil {
			panic(err)
		}
		p := NewRelayPubSub[V](width)
		x.send = p.Send
		for i := range n {
			s := p.Subscribe()
			x.receive[i] = func() V { <-s.C(); return s.Read() }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case <-s.C():
						return s.Read()
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = s.Unsubscribe
		}
	}
	return x
}

func runBroadcastBench[V any](b *testing.B, kind string, n int, useSelect bool, value func(uint64) V, sequence func(V) uint64) {
	const warmup = 32
	var done <-chan struct{}
	if useSelect {
		done = make(chan struct{})
	}
	x := makeBroadcastBench[V](kind, n, done)
	var wg, started, copied sync.WaitGroup
	var bad atomic.Int64
	cleanup := make(chan struct{})
	wg.Add(n)
	started.Add(n)
	copied.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			defer x.unsubscribe[i]()
			started.Done()
			for j := range b.N + warmup {
				if sequence(x.receive[i]()) != uint64(j) {
					bad.Add(1)
				}
			}
			copied.Done()
			<-cleanup
		}()
	}
	started.Wait()
	for i := range warmup {
		if got := x.send(value(uint64(i))); got != n {
			b.Fatalf("warmup send count %d, want %d", got, n)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if got := x.send(value(uint64(i + warmup))); got != n {
			b.Fatalf("send count %d, want %d", got, n)
		}
	}
	// PhasePubSub returns after notification handoffs, before copies finish.
	// Drain the final copies for every variant before stopping the timer, but
	// keep unsubscribe/goroutine teardown outside it. This is one final join,
	// not an extra per-publication barrier.
	copied.Wait()
	b.StopTimer()
	close(cleanup)
	wg.Wait()
	if got := bad.Load(); got != 0 {
		b.Fatalf("%d incorrect deliveries", got)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/delivery")
}

func BenchmarkBroadcast(b *testing.B) {
	for _, n := range []int{1, 8, 64, 512, 4096} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			for _, kind := range []string{"ChanPubSub", "PhasePubSub", "LinearChannels", "RelayW1", "RelayW4", "RelayW16", "RelayW64"} {
				b.Run(kind, func(b *testing.B) {
					runBroadcastBench(b, kind, n, false, func(i uint64) uint64 { return i }, func(i uint64) uint64 { return i })
				})
			}
		})
	}
}

// Every receiver selects between its subscription and the same never-canceled
// shutdown channel, as with a shared context.Done in an application.
func BenchmarkSelect(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			for _, kind := range []string{"ChanPubSub", "PhasePubSub", "LinearChannels", "RelayW1", "RelayW4", "RelayW16", "RelayW64"} {
				b.Run(kind, func(b *testing.B) {
					runBroadcastBench(b, kind, n, true, func(i uint64) uint64 { return i }, func(i uint64) uint64 { return i })
				})
			}
		})
	}
}

type benchPayload struct {
	Sequence uint64
	Padding  [1016]byte
}

func BenchmarkPayload1KiB(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			for _, kind := range []string{"ChanPubSub", "PhasePubSub", "LinearChannels", "RelayW1", "RelayW4", "RelayW16", "RelayW64"} {
				b.Run(kind, func(b *testing.B) {
					runBroadcastBench(b, kind, n, false,
						func(i uint64) benchPayload { return benchPayload{Sequence: i} },
						func(v benchPayload) uint64 { return v.Sequence })
				})
			}
		})
	}
}

var idleChurnSink chan uint64

var (
	constructorPhaseSink *PhasePubSub[uint64]
	constructorRelaySink *RelayPubSub[uint64]
	constructorChanSink  *ChanPubSub[chan uint64, uint64]
)

func BenchmarkConstruct(b *testing.B) {
	b.Run("ChanPubSub", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			constructorChanSink = NewChanPubSub(make(chan uint64))
		}
	})
	b.Run("PhasePubSub", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			constructorPhaseSink = NewPhasePubSub[uint64]()
		}
	})
	b.Run("RelayPubSub", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			constructorRelaySink = NewRelayPubSub[uint64](16)
		}
	})
}

func BenchmarkIdleChurn(b *testing.B) {
	b.Run("ChanPubSub", func(b *testing.B) {
		p := NewChanPubSub(make(chan uint64))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			p.Subscribe()
			p.Unsubscribe()
		}
	})
	b.Run("PhasePubSub", func(b *testing.B) {
		p := NewPhasePubSub[uint64]()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			s := p.Subscribe()
			s.Unsubscribe()
		}
	})
	b.Run("RelayPubSub", func(b *testing.B) {
		p := NewRelayPubSub[uint64](4)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			s := p.Subscribe()
			s.Unsubscribe()
		}
	})
	// This measures the channel allocation for fresh subscribers only. It is
	// deliberately not a registry-removal benchmark or a pooled alternative.
	b.Run("FreshChannel", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			idleChurnSink = make(chan uint64)
		}
	})
}
-- chancaster.go --
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
	"math"
	"sync"
	"sync/atomic"
)

// ChanCaster supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// The underlying channel C MUST be unbuffered (cap == 0). This is a hard
// precondition of the implementation: [ChanCaster.Send] performs exactly one
// synchronous send per registered receiver, relying on the one-to-one
// send/receive semantics of an unbuffered channel to guarantee delivery. A
// buffered channel would violate these invariants and can cause deadlocks or
// undetected missed receives.
//
// A nil channel is permitted. It satisfies the cap == 0 requirement and
// behaves according to standard Go semantics: sends and receives block
// forever, and select cases on a nil channel never fire. This allows a
// ChanCaster to be used in a "disabled" state — [ChanCaster.Send] returns 0
// when there are no receivers, and the channel can be included in a select
// statement without ever being selected.
//
// Construction is "softly" enforced: [NewChanCaster] validates the
// precondition and panics on a buffered channel, but a value may also be
// built directly via a struct literal, which bypasses that check. Callers
// that construct a [ChanCaster] directly are responsible for honouring the
// unbuffered requirement themselves. The channel requirement matches
// ChanPubSub; unlike ChanPubSub, direct ChanCaster construction remains
// supported and relies on the caller to preserve the invariant.
//
// See also [NewChanCaster] (optional).
type ChanCaster[C ~chan V, V any] struct {
	// C is the channel that will be used to broadcast messages.
	//
	// See [ChanCaster.Add] and [ChanCaster.Send] for usage details.
	C C

	// mutex synchronises [ChanCaster.Send] and [ChanCaster.Add] calls with
	// positive deltas. Ensuring sends are mutually exclusive is fairly
	// important for consistency, but not strictly necessary. The primary
	// purpose of this mutex is to avoid incrementing the number of receivers
	// while any send is in progress, to facilitate removing receivers while
	// send is in progress.
	mutex sync.RWMutex
	// state synchronises the number of receivers, between [ChanCaster.Send]
	// calls, and [ChanCaster.Add] calls with negative deltas.
	// The high 32 bits are number of receivers (modeled as int32), while the
	// low 32 bits are either identical, or [math.MaxInt32] + the number of
	// receivers being (the value of the high 32 bits), if sending is in
	// progress. Using an atomic value for synchronisation requires having the
	// "is sending" information embedded in said value, and having two copies
	// of the number of receivers makes invariant and overflow checks easier.
	state atomic.Uint64
}

// NewChanCaster constructs a [ChanCaster] for the provided channel. It exists
// primarily for type inference convenience, but also enforces the
// [ChanCaster] precondition: the channel must be unbuffered (cap == 0). A nil
// channel is permitted. It panics if the channel is buffered.
func NewChanCaster[C ~chan V, V any](channel C) *ChanCaster[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster[C, V]{C: channel}
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster.C], then returns the number of sends performed, less any
// removed mid-flight by [ChanCaster.Add].
//
// If the contract of [ChanCaster] is obeyed, this call will never block for
// any significant time. This function may panic if state invariants have been
// violated, e.g. by misuse of [ChanCaster.Add], or if unregistered receives
// are detected.
//
// See [ChanCaster.Add] for usage details.
func (x *ChanCaster[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // no receivers (fast path)
	}

	// prevent receivers being added while sending + order values by send call
	x.mutex.Lock()
	defer x.mutex.Unlock()

	// load our state, guard no receivers (early exit), and set tracker to the
	// value of `maxInt32 + receivers`, using CAS to sync with negative adds
	var (
		state              uint64
		receivers, tracker uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0 // no receivers (slow path)
		}

		receivers = uint32(state >> 32) // initialize from hi
		tracker = uint32(state)         // initialize from lo
		if tracker != receivers || receivers > math.MaxInt32 {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}

		// attempt to set tracker to `maxInt32 + receivers`, with CAS used to
		// synchronise with decrements of receivers
		tracker += math.MaxInt32
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(tracker)) {
			break
		}
	}

	// broadcast involves sending to all receivers - with the total actually
	// received being in range [0, receivers], due to potential decrements
	for range receivers {
		x.C <- value // may end up received by negative Add calls
	}

	// now, we can retrieve, validate, then reset the state (to 0 - all broadcast + we locked so none added)
	// note: it should be stable - if it isn't, invariants were violated
	state = x.state.Load()
	tracker = uint32(state >> 32) // actually the final receivers (used as scratch)
	if tracker > receivers ||     // receivers should be unchanged or decreased (and also lower than math.MaxInt32)
		uint32(state) != tracker+math.MaxInt32 || // lo still exactly math.MaxInt32 more than hi
		!x.state.CompareAndSwap(state, 0) { // failing this indicates one or more unregistered receivers
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}

	// returns number of sends less any received by negative Add calls
	return int(tracker)
}

// Add increments or decrements the number of receivers, for [ChanCaster.C],
// then returns the number of receivers, after the operation. Calling with a
// delta of 0 is allowed, and will load and validate then return the number
// of receivers, without changing it.
//
// Each added receiver represents the intent to receive exactly one value, from
// [ChanCaster.C]. Each receiver removed (via a negative delta) represents
// exactly one added receiver, which has not yet been removed, and has not and
// will not receive a value. The valid range for the number of receivers is
// [0, math.MaxInt32], and any add which results in a number of receivers
// outside this range will cause a panic.
//
// The typical usage pattern is to call [ChanCaster.Add] with a delta of 1,
// then immediately receive, or attempt to receive, e.g. within a `select`
// statement. If the next-received value (e.g. within said `select` statement)
// is not from [ChanCaster.C], and receive won't be promptly re-attempted, then
// [ChanCaster.Add] should be called again, with the inverse of the previous
// delta.
//
// Using a delta greater than 1 indicates multiple separate receivers, which
// will all receive the same value, from the same [ChanCaster.Send] call. These
// receivers should independently decrement the number of receivers, if
// necessary, as described above.
func (x *ChanCaster[C, V]) Add(delta int) int {
	const maxReceivers = math.MaxInt32

	switch {
	case delta >= 0:
		var state uint64
		if delta == 0 {
			state = x.state.Load() // no change
		} else if delta > maxReceivers {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		} else {
			// increasing num receivers not allowed concurrently with sending
			x.mutex.RLock()
			defer x.mutex.RUnlock()

			// add delta to both hi and lo
			state = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
		}

		// validate to ensure we did not overflow + sanity check invariants
		// note: if this was an add (and we acquired the lock), the lo value
		// should never be the +maxReceivers case
		if receivers, tracker := uint32(state>>32), uint32(state); receivers <= maxReceivers &&
			receivers >= uint32(delta) &&
			(receivers == tracker || (delta == 0 && receivers+maxReceivers == tracker)) {
			return int(receivers)
		}

	case delta < -maxReceivers:
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		// flip delta to positive, from negative
		delta = -delta

		// note: same delta calc as above, subtracted using two's complement rules
		state := x.state.Add(^(uint64(delta)<<32 | uint64(uint32(delta)) - 1))

		// validate, and, if necessary, receive any channel sends that would
		// otherwise never be received (to avoid Send hanging)
		if receivers := uint32(state >> 32); receivers <= maxReceivers &&
			maxReceivers-receivers >= uint32(delta) {
			switch uint32(state) { // lo
			case receivers:
				// not sending
				return int(receivers) // note: already subtracted delta

			case maxReceivers + receivers:
				// sending - perform the requisite number of receives
				// note: receivers = tracker-maxReceivers (per the above)
				for range delta {
					<-x.C
				}
				return int(receivers) // note: already subtracted delta
			}
		}
	}

	// invariant violation, e.g. due to OoB add, or previous violation
	panic(`bigbuff: chancaster: add: state invariant violation`)
}
-- chanpubsub.go --
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
	"iter"
	"math"
	"sync"
	"sync/atomic"
)

const chanPubSubStateInvariantViolation = `bigbuff: chanpubsub: state invariant violation`

// ChanPubSub is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, and support for use
// in select statements.
//
// # Usage
//
// The [ChanPubSub.SubscribeContext] method provides a simple, [iter.Seq]-based
// (iterable) mechanism, to subscribe and consume values. If greater
// flexibility is required (e.g. it is desirable to leverage select
// statements), please ensure you understand the contract, documented below.
//
// The underlying implementation relies on strict adherence to the following
// contract:
//
//  1. The [NewChanPubSub] factory MUST be used to create instances.
//  2. Values MUST NOT be received prior to incrementing the subscriber count,
//     e.g. by calling [ChanPubSub.Add] with `1`.
//  3. Subscribers - any logical unit that receives values, after incrementing
//     the subscriber count - MUST call [ChanPubSub.Wait], immediately after
//     each value received, UNLESS the value was the result of closing the
//     underlying channel (subscribers MUST handle this case).
//  4. Subscribers MUST be able to receive then block on [ChanPubSub.Wait]
//     calls, that is, the number of subscribers MUST be exactly equal to what
//     was registered, e.g. via [ChanPubSub.Add].
//  5. [ChanPubSub.Wait] MUST NOT be called in any other circumstance.
//  6. Subscribers MAY call [ChanPubSub.Add] with a negative delta, to
//     unsubscribe, but MUST ensure that the net total remains >= 0, at all
//     times.
//  7. Subscribers MUST promptly and _continually_ operate on a cycle of
//     receiving (from [ChanPubSub.C]) then waiting (on [ChanPubSub.Wait]),
//     until such a time as they are no longer interested in receiving values,
//     in which case they MUST promptly unsubscribe. Subscribers MAY use the
//     channel in a select statement, to trigger unsubscribe, as an example.
//     After unsubscribing, values MUST NOT be received, from the channel.
type ChanPubSub[C ~chan V, V any] struct {
	// ping is used to orchestrate sends.
	ping ChanCaster[C, V]
	// pongC is used to orchestrate waits.
	pongC *sync.Cond
	pongN int
	// sendMu is used to prevent concurrent sends. It serves no other purpose.
	sendMu sync.Mutex
	// sendingMu has two very important roles:
	//
	//	1. Synchronises reading subscribers then incrementing ping, with
	//	   incrementing subscribers, to avoid subscribers from stealing
	//	   messages intended for others (N.B. can still subscribe concurrently)
	//	2. Wait until ping has been incremented (if sends are in progress, i.e.
	//	   attempting to acquire or holding the write lock), or briefly
	//	   prevent sends from progressing (both are about avoiding wonky state)
	sendingMu sync.RWMutex
	// subscribers tracks the total number of subscribers.
	subscribers atomic.Int32
	// broken is used to indicate misuse of the API, and is closed only if
	// such misuse is detected.
	broken chan struct{}
}

// NewChanPubSub constructs a new instance of [ChanPubSub], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub[C ~chan V, V any](c C) *ChanPubSub[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub[C, V]
	x.ping.C = c
	x.pongC = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel, used by the receiver.
// This channel MAY be closed, but MUST NOT be sent to (except by
// [ChanPubSub.Send]), and receiving from it MUST obey the contract.
// See also [ChanPubSub], for the contract.
func (x *ChanPubSub[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

// SubscribeContext immediately subscribes to the receiver, and returns a
// single-use iterator, which will yield all values sent to the receiver, and
// will automatically unsubscribe when the context is cancelled, the iterator
// stops early, or the receiver is closed.
// Behavior is undefined if the iterator is used more than once.
//
// WARNING: The returned iterator MUST be used immediately, OR, the context
// MUST be promptly cancelled, to avoid leaking resources, and/or potentially
// causing a deadlock.
func (x *ChanPubSub[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // N.B. handles guarding on check* functions

	// support nil (consistency - not best practice, but this pkg tends to)
	if ctx == nil {
		ctx = context.Background()
	}

	// For if we never call the iterator. Also used to handle multiple calls.
	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			// ensure we unsubscribe on all code paths
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}

		if !stop() {
			// Context was cancelled, or called more than once.
			// WARNING: Does not block on any still-running iterator.
			return
		}

		// always unsubscribe, exactly once, or never, if the context is never
		// canceled, AND the iterator is either never called, or is called at
		// least once, and the (single) call to get past stop() never completes
		defer x.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			case v, ok := <-x.ping.C:
				if !ok {
					return
				}

				x.Wait()

				if !yield(v) {
					return
				}
			}
		}
	}
}

// Send sends a value to all subscribers, returning the number of subscribers
// that received the value.
// It should not be necessary to actively consider dynamics such as the risk of
// deadlock, so long as subscribers obey the contract, as documented on
// [ChanPubSub].
func (x *ChanPubSub[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}

	// for sanity of the ping-pong communication pattern
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// N.B. released after sending (after pings, before waiting for pongs)
	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken() // again (attempt to mitigate deadlocks caused by borked state)

	// we need to know the subscribers, so we can add to x.ping
	// synchronisation is important here, so INCREMENTS are mutually exclusive
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0 // no subscribers (slow path)
	}

	x.sanityCheckSubscribersDelta(subscribers, 0) // just because

	var success bool
	defer func() {
		if !success {
			x.markBroken() // example: closed channel caused panic
		}
	}()

	// if nothing goes wrong, we always decrement x.ping back to 0 (send)
	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}

	// ping! (send to channel)
	sent = x.ping.Send(value) // N.B. supports concurrent decrements

	skipSendingUnlock = true
	x.sendingMu.Unlock() // we can add subscribers while waiting for pongs

	// pong! (await appropriate number of calls to Wait)
	if sent != 0 {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()

		x.checkBroken() // AFTER lock (broken state is only broadcast once)

		x.pongN = sent
		x.pongC.Broadcast() // wake up any blocking Wait calls

		// wait for our pongs to be consumed
		for x.pongN != 0 {
			x.pongC.Wait()
			x.checkBroken() // ALWAYS checkBroken after a wait
		}
	}

	success = true

	return
}

// Add returns the number of subscribers, after adding delta to the count. It
// is intended for more manual use, which requires the caller to be aware of
// and conform to the contract, as documented on [ChanPubSub].
// This method acts as a low-level means to subscribe or unsubscribe from the
// receiver.
// A panic may occur if the input is invalid (e.g. decrementing subscribers
// below zero), or if the receiver is in a broken state, indicating misuse.
func (x *ChanPubSub[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		// Inspect case. No change.
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		// Unsubscribe case. If the contract was obeyed, after we attempt to
		// acquire the (read) lock, we are able to narrow down the state into
		// one of two scenarios:
		//
		//	1. Send in progress
		//	2. Send not in progress
		//
		// In the first scenario, we then pass through to the Add of x.ping, to
		// notify the sender, that we have adjusted number of subscribers.
		//
		// Restating because I am guaranteed to forget this:
		// Part of the API contract is that callers MUST NOT decrement past the
		// number of increments which they themselves have added. Ergo, there
		// MUST be at least one subscriber, and that subscriber MUST NOT go
		// away, since that would violate the contract (as just stated above).
		// An internal instance of [ChanCaster] is used to handle the delicate
		// dance that is sending as many messages as there are subscribers, and
		// [ChanCaster.Add] is used to increment said instance, as part of
		// [ChanPubSub.Add], having acquired the write side of x.sendingMu.
		// If we succeed in acquiring the read side of x.sendingMu, then x.ping
		// is guaranteed to be zero. If x.ping is _non-zero_, then, as an
		// unobvious consequence of the ping-pong communication pattern, it is
		// GUARANTEED that THIS subscriber HAS NOT received a message, and it
		// is therefore GUARANTEED that x.ping will not be decremented past
		// zero, since we only add to x.ping immediately before sending, send
		// blocks on the same number of receives (aside from decrements) being
		// performed, as x.ping is incremented by, and adding and sending using
		// x.ping is guarded by both x.sendMu and x.sendingMu.
		//
		// ... rather complex, yes.
		ok := x.sendingMu.TryRLock()
		// N.B. this loop is to handle state transition (send in progress)
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken() // attempts to mitigate deadlock risk on misuse...
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock() // unlock, before possible panics
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
			// decrement, per delta
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			// WARNING: Relies on the caller ensuring no concurrent receives.
			x.ping.Add(delta)
			success = true
		}

	default:
		// Subscribe case. Mutually exclusive with sending, but able to run
		// concurrently with other attempts to subscribe.
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

// Wait is mandatory after each message received by each subscriber, serving as
// means to synchronise the caller (a subscriber, that received a message) with
// the sender, for the purpose of ensuring that all messages are broadcast, to
// all subscribers.
func (x *ChanPubSub[C, V]) Wait() {
	x.checkUsedFactoryFunction()

	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()

	x.checkBroken() // AFTER lock (broken state is only broadcast once)

	// wait for a pong to consume
	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken() // ALWAYS checkBroken after a wait
	}

	x.pongN-- // consume a pong

	if x.pongN == 0 {
		x.pongC.Broadcast() // wake up Send call
	}
}

// Subscribe is an alias for calling [ChanPubSub.Add] with a delta of 1.
func (x *ChanPubSub[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub.Add] with a delta of -1.
func (x *ChanPubSub[C, V]) Unsubscribe() {
	x.Add(-1)
}

// checkBroken panics if the receiver is in a broken state, indicating misuse.
// Called as part of all public methods, as a mitigation to reduce the risk of
// deadlocks, brought about by undefined behavior, caused by API misuse.
func (x *ChanPubSub[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

// markBroken ensures that the broken channel is closed.
// The janky simplistic implementation is deliberate, as this indicates API
// misuse, and is an exceptional scenario, surfacing as panics on method calls.
func (x *ChanPubSub[C, V]) markBroken() {
	defer func() {
		recover() // we're going to panic shortly
	}()
	x.pongC.L.Lock() // avoid missing the broadcast
	defer x.pongC.L.Unlock()
	close(x.broken)
	x.pongC.Broadcast() // wake up any waiting goroutines (once)
}

// sanityCheckSubscribersDelta checks conditions such as overflow, underflow,
// and valid bounds, for the subscribers atomic counter, assessing both old and
// new values, after ChanPubSub.addSubscribers.
func (x *ChanPubSub[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	// 1. Compute old value
	oldSubscribers := int(int32(subscribers) - int32(delta))

	// 2. Detect overflow or underflow
	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}

	// 3. Negative subscribers are not allowed (new value)
	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}

	// 4. Negative subscribers are not allowed (old value)
	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative old subscribers`)
	}
}

// addSubscribers performs an atomic operation to add delta to the subscribers.
func (x *ChanPubSub[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
-- go.mod --
module example.com/bigbuff-evolution

go 1.25.0
-- LICENSE --

                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "[]"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright [yyyy] [name of copyright owner]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-- MANIFEST.sha256 --
cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30  LICENSE
8dced41485cbddde09fef8b4ad4e0aac6d1d7a1f1268e0d828cc7155ad1ab707  README.md
43e5c699f7fd994d0613fd3dd177ad2f8850c5a6df43b90a36a36a08bbef005a  broadcast_bench_test.go
85a37f62e0938b8b72acf5bfda849fa5232e8f3d7e04d62b89638fb12025f77d  chancaster.go
bc8a0148232fabfc1b6a13c624b8637a25643512cdb07122244291ec2c2b0ece  chanpubsub.go
d2ed9e5031e4bedde8ae9e3a13969307c29d420ee7b1ad4259fcbd97a5f699ac  go.mod
95c149f5d37af659c8cbe6c89a7401483a611faf00fd58cf803bcbe54aed5f20  model/credit_model.py
fe9a45b1508b89fd0d42558fe984f253c07fe7fa98832f6b8a9a7ef0afa7ee11  model/summarize_benchmarks.py
00010c452c251bc4511bac04fc80d702fe9d8dd4a46f30e65bbe90ae377cbd0a  phasepubsub.go
dccfbb7b4080ca8fac9d6c7b0ca85d1b768a2ff221c00e5cdb9b5c6d87555c0b  phasepubsub_test.go
ace3116b1914d90616ada5ea7da3e4097663b03c1aa1eda73d2c37ba92c01669  relaypubsub.go
e7f37f154dad3774bcf41a52b81056415740d10465157de36060319407ce61f2  relaypubsub_test.go
051c8cdeaea40e3a7055ca203addbe12426ee1d32f53e447209292f2b35ad7ae  results/bench-allocation.txt
2adb68247b626fd2dfe9efaeddd76cb208395fa41f8a7292dc9e99abceeb2d5c  results/bench-broadcast.txt
89a0918e77507fa375783565eb8223291eafeae6e53989ec2f6b976370030c68  results/bench-n1-clean.txt
15c6e14fb478ce3b1a2b34fb5190c307c85e2d331b367e993f8f1f6220e4154c  results/bench-select-payload.txt
efc65235db6e97cd26241ddf5a9c8879a1c90ee63a01903c24825e649fb8d8e2  results/check-select64.txt
b5f8b50ff969eb06029c5ab362cb36719eae68571cc4436e0c3b9acbba31474f  results/environment.txt
8c6bce2845e261ce286bc72e067325f00ae81c342c485407a19f6afb84416ba5  results/portability.txt
428ef91fd0393f5f4c1dd180fac515cc749c314937d1434ae50c77127d403d8e  results/summary.json
92ce4ee61ce17e084baf01abfddf79fd8331f398418b948636b598cbc33ff709  results/summary.md
048973d992d6644707d4bc74db78c2080f09bbd91c45817409ace85afbf3c30b  results/validation.txt
-- model/credit_model.py --
#!/usr/bin/env python3
"""Exhaustively check the bounded-credit relay's abstract publication protocol.

Run with Python 3, for example::

    python3 model/credit_model.py
    python3 model/credit_model.py --max-n 16

Scope: one publication with N already-enrolled participants and K initial
credits, for every 1 <= K <= N <= --max-n. Participants are interchangeable.
The state separates a receive, its value copy, its atomic decrement, and a
potentially arbitrarily delayed forwarding send. Publisher seed sends can be
interleaved with all of those steps. Cancellation is represented by a normal
participant whose copy step does no work; its token accounting is identical.

Assertions establish, within these finite bounds:
* No seed or forwarding send needs more than K channel slots.
* No reachable nonterminal state lacks an enabled transition.
* A zero remaining count implies all seeds and forwards are visible, the
  channel is empty, and all value copies have finished.

This deliberately does NOT model membership admission, the alternating phase
channels, Go's memory model, misuse of the API, scheduler fairness, or runtime
channel internals. It checks the credit-conservation argument independently of
the Go implementation; it is neither a proof nor a replacement for race tests.
"""

from argparse import ArgumentParser
from collections import deque
from typing import NamedTuple


class State(NamedTuple):
    unseeded: int
    buffered: int
    copying: int
    ack_pending: int
    forward_pending: int
    remaining: int


def audit(n: int, k: int) -> int:
    initial = State(k, 0, 0, 0, 0, n)
    queue = deque([initial])
    seen = {initial}
    while queue:
        state = queue.popleft()
        u, b, c, a, f, r = state
        assert 0 <= b <= k, ("channel overflow", n, k, state)
        assert b + c + a + f <= k - u, ("credit creation", n, k, state)
        if r == 0:
            assert (u, b, c, a, f) == (0, 0, 0, 0, 0), (
                "completion with unfinished work", n, k, state
            )
            continue

        received = n - r + c + a
        next_states = []
        if u:
            # A seed cannot encounter a full channel, even if forwarding races it.
            assert b < k, ("blocked seed", n, k, state)
            next_states.append(State(u - 1, b + 1, c, a, f, r))
        if b and received < n:
            # Some previously unserved participant acquires one credit.
            next_states.append(State(u, b - 1, c + 1, a, f, r))
        if c:
            # That participant completes its local value copy (or discards).
            next_states.append(State(u, b, c - 1, a + 1, f, r))
        if a:
            # Phase advancement precedes this atomic remaining.Add(-1).
            nr = r - 1
            next_states.append(State(u, b, c, a - 1, f + (nr >= k), nr))
        if f:
            # A reader may have been delayed after its decrement, before forwarding.
            assert b < k, ("blocked forward", n, k, state)
            next_states.append(State(u, b + 1, c, a, f - 1, r))
        assert next_states, ("nonterminal deadlock", n, k, state)
        for next_state in next_states:
            if next_state not in seen:
                seen.add(next_state)
                queue.append(next_state)
    return len(seen)


def main() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--max-n", type=int, default=12)
    args = parser.parse_args()
    if args.max_n < 1:
        parser.error("--max-n must be positive")
    states = sum(audit(n, k) for n in range(1, args.max_n + 1) for k in range(1, n + 1))
    print(
        f"PASS: N=1..{args.max_n}, every K=1..N; "
        f"{states:,} reachable states; no credit-capacity, progress, "
        "or completion invariant violations."
    )


if __name__ == "__main__":
    main()
-- model/summarize_benchmarks.py --
#!/usr/bin/env python3
"""Summarize saved Go benchmark samples, using only the standard library.

Run from the module root. Each group is kept separate by benchmark, fanout,
implementation, and GOMAXPROCS. Min/max are descriptive, not confidence bounds.
"""

from collections import defaultdict
import json
from pathlib import Path
import re
from statistics import median
import sys


def main():
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    groups = defaultdict(list)
    for path in sorted(directory.glob("bench-*.txt")):
        for line in path.read_text().splitlines():
            if not line.startswith("Benchmark"):
                continue
            fields = line.split()
            if len(fields) < 4 or fields[3] != "ns/op":
                continue
            name = fields[0]
            match = re.search(r"-(\d+)$", name)
            procs = int(match[1]) if match else 1
            if match:
                name = name[: match.start()]
            # The clean, separately recorded N1 run replaces the initial N1
            # measurements, which overlapped a brief cross-compilation step.
            if ("/N1/" in name and path.name == "bench-broadcast.txt"
                    and (directory / "bench-n1-clean.txt").exists()):
                continue
            metrics = {fields[i + 1]: float(fields[i])
                       for i in range(2, len(fields) - 1, 2)}
            groups[(name, procs)].append(metrics)

    result = []
    for (name, procs), samples in sorted(groups.items()):
        row = {"benchmark": name, "gomaxprocs": procs, "samples": len(samples)}
        for metric in sorted(set.intersection(*(set(s) for s in samples))):
            values = [s[metric] for s in samples]
            row[metric] = {"median": median(values), "min": min(values), "max": max(values)}
        result.append(row)

    (directory / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = ["# Benchmark medians", "", "Raw samples are in the adjacent bench-*.txt files.", "",
             "| Benchmark | Ps | Samples | Median ns/op | Min | Max | Median B/op | Median allocs/op |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in result:
        ns = r["ns/op"]
        lines.append(f'| {r["benchmark"]} | {r["gomaxprocs"]} | {r["samples"]} '
                     f'| {ns["median"]:g} | {ns["min"]:g} | {ns["max"]:g} '
                     f'| {r.get("B/op", {}).get("median", 0):g} '
                     f'| {r.get("allocs/op", {}).get("median", 0):g} |')
    (directory / "summary.md").write_text("\n".join(lines) + "\n")
    print(f"Summarized {sum(len(s) for s in groups.values())} samples in {len(result)} groups.")


if __name__ == "__main__":
    main()
-- phasepubsub_test.go --
package bigbuff

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func awaitPhasePublication[V any](t *testing.T, x *PhasePubSub[V], epoch uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		x.mu.Lock()
		active, current := x.active, x.epoch
		x.mu.Unlock()
		if active && current == epoch {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("publication %d did not become active; current=%d active=%v", epoch, current, active)
		}
		runtime.Gosched()
	}
}

func awaitPhaseSend(t *testing.T, done <-chan int, want int) {
	t.Helper()
	select {
	case got := <-done:
		if got != want {
			t.Fatalf("Send returned %d, want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not complete")
	}
}

func phaseSend[V any](x *PhasePubSub[V], value V) <-chan int {
	done := make(chan int, 1)
	go func() { done <- x.Send(value) }()
	return done
}

func TestPhasePubSubOrderedDelivery(t *testing.T) {
	const readers, publications = 12, 300
	x := NewPhasePubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range readers {
		s := x.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			for want := range publications {
				select {
				case <-s.C():
					if got := s.Read(); got != want {
						t.Errorf("received %d, want %d", got, want)
						return
					}
				case <-ctx.Done():
					t.Error(ctx.Err())
					return
				}
			}
		}()
	}
	for value := range publications {
		if got := x.Send(value); got != readers {
			t.Errorf("publication %d: sent %d, want %d", value, got, readers)
			break
		}
	}
	cancel()
	wg.Wait()
	if got := x.Send(-1); got != 0 {
		t.Fatalf("empty Send returned %d", got)
	}
}

func TestPhasePubSubSharedSlotGracePeriod(t *testing.T) {
	x := NewPhasePubSub[int]()
	s := x.Subscribe()
	defer s.Unsubscribe()

	first := phaseSend(x, 101)
	<-s.C() // Deliberately pause before Read.
	awaitPhaseSend(t, first, 1)

	second := phaseSend(x, 202)
	awaitPhasePublication(t, x, 1)
	thirdStarted := make(chan struct{})
	third := make(chan int, 1)
	go func() {
		close(thirdStarted)
		third <- x.Send(303)
	}()
	<-thirdStarted

	// The next publication can start, but the third cannot reuse our slot
	// while we have not even read the first publication.
	select {
	case <-third:
		t.Fatal("third publication completed before the first Read")
	case <-time.After(10 * time.Millisecond):
	}
	if got := s.Read(); got != 101 {
		t.Fatalf("first shared value overwritten: got %d", got)
	}

	<-s.C() // Pause before Read again; the next slot may now be reused.
	awaitPhaseSend(t, second, 1)
	awaitPhasePublication(t, x, 2)
	if got := s.Read(); got != 202 {
		t.Fatalf("second shared value overwritten: got %d", got)
	}
	<-s.C()
	if got := s.Read(); got != 303 {
		t.Fatalf("third shared value: got %d", got)
	}
	awaitPhaseSend(t, third, 1)
}

func TestPhasePubSubMembershipDuringPublication(t *testing.T) {
	x := NewPhasePubSub[int]()
	a, b := x.Subscribe(), x.Subscribe()
	first := phaseSend(x, 10)
	awaitPhasePublication(t, x, 0)

	<-a.C()
	if got := a.Read(); got != 10 {
		t.Fatalf("first value: got %d", got)
	}
	a.Unsubscribe() // Already received: must not drain b's notification.

	c := x.Subscribe() // Joins publication 1 while b still owes publication 0.
	d := x.Subscribe()
	d.Unsubscribe() // Has no debt to the current publication.
	select {
	case <-c.C():
		t.Fatal("new subscriber stole a notification from the current publication")
	default:
	}
	b.Unsubscribe() // Has not received: must settle its current debt.
	awaitPhaseSend(t, first, 1)

	second := phaseSend(x, 20)
	<-c.C()
	if got := c.Read(); got != 20 {
		t.Fatalf("new subscriber's first value: got %d", got)
	}
	c.Unsubscribe()
	awaitPhaseSend(t, second, 1)
	if got := x.Send(30); got != 0 {
		t.Fatalf("Send after all subscriptions ended: got %d", got)
	}
}

func TestPhasePubSubConcurrentCancellation(t *testing.T) {
	const count = 32
	x := NewPhasePubSub[int]()
	s := make([]PhaseSubscription[int], count)
	for i := range s {
		s[i] = x.Subscribe()
	}
	done := phaseSend(x, 1)
	awaitPhasePublication(t, x, 0)
	var wg sync.WaitGroup
	for i := range s {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s[i].Unsubscribe()
			s[i].Unsubscribe()
		}()
	}
	awaitPhaseSend(t, done, 0)
	wg.Wait()
	for i := range s {
		if s[i].C() != nil {
			t.Fatalf("unsubscribed handle %d still has an enabled channel", i)
		}
	}
}

func TestPhasePubSubConcurrentSenders(t *testing.T) {
	const readers, writers, perWriter = 4, 4, 100
	const publications = writers * perWriter
	x := NewPhasePubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan []int, readers)
	for range readers {
		s := x.Subscribe()
		go func() {
			defer s.Unsubscribe()
			values := make([]int, 0, publications)
			for range publications {
				select {
				case <-s.C():
					values = append(values, s.Read())
				case <-ctx.Done():
					t.Error(ctx.Err())
					results <- values
					return
				}
			}
			results <- values
		}()
	}
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				if n := x.Send(writer*perWriter + i); n != readers {
					t.Errorf("Send returned %d, want %d", n, readers)
					return
				}
			}
		}()
	}
	wg.Wait()
	first := <-results
	if len(first) != publications {
		t.Fatalf("received %d publications, want %d", len(first), publications)
	}
	seen := make(map[int]bool, publications)
	for _, value := range first {
		if seen[value] {
			t.Fatalf("duplicate publication %d", value)
		}
		seen[value] = true
	}
	for range readers - 1 {
		other := <-results
		if !slices.Equal(other, first) {
			t.Fatal("subscribers observed different publication orders")
		}
	}
}

func TestPhasePubSubEpochWrap(t *testing.T) {
	x := NewPhasePubSub[int]()
	x.epoch = math.MaxUint64 - 1
	s := x.Subscribe()
	defer s.Unsubscribe()
	for want := range 4 {
		done := phaseSend(x, want)
		<-s.C()
		if got := s.Read(); got != want {
			t.Fatalf("across epoch wrap: got %d, want %d", got, want)
		}
		awaitPhaseSend(t, done, 1)
	}
}

func TestPhasePubSubAllocationFreeChurn(t *testing.T) {
	x := NewPhasePubSub[int]()
	if got := testing.AllocsPerRun(1000, func() {
		s := x.Subscribe()
		s.Unsubscribe()
	}); got != 0 {
		t.Fatalf("subscribe/unsubscribe: got %g allocations, want zero", got)
	}
}

// Copying a large inline value stresses the interval between handoff and Read,
// which is intentionally not part of PhasePubSub.Send's completion contract.
func TestPhasePubSubValueLargerThanChannelElement(t *testing.T) {
	const readers, rounds = 5, 7
	x := NewPhasePubSub[[65536]byte]()
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		s := x.Subscribe()
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			for sequence := 1; sequence <= rounds; sequence++ {
				<-s.C()
				value := s.Read()
				for j, got := range value {
					if want := byte(j*13 + sequence); got != want {
						t.Errorf("sequence %d byte %d = %d, want %d", sequence, j, got, want)
						break
					}
				}
			}
		}()
	}
	for sequence := 1; sequence <= rounds; sequence++ {
		var value [65536]byte
		for j := range value {
			value[j] = byte(j*13 + sequence)
		}
		if got := x.Send(value); got != readers {
			t.Errorf("Send = %d, want %d", got, readers)
		}
	}
	wg.Wait()
}

func ExamplePhasePubSub() {
	x := NewPhasePubSub[string]()
	s := x.Subscribe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		x.Send("hello")
	}()
	select {
	case <-s.C():
		fmt.Println(s.Read())
	}
	s.Unsubscribe()
	<-done
	// Output: hello
}
-- phasepubsub.go --
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
	"math"
	"sync"
	"sync/atomic"
)

// PhasePubSub broadcasts values using two permanently allocated notification
// channels and two shared value slots. It has no subscriber registry, no
// per-subscriber channel, and no post-receive barrier. A subscriber's local
// publication number determines which channel and value slot it uses next.
//
// Construct instances with NewPhasePubSub. A PhasePubSub must not be copied.
// Each subscription belongs to one goroutine at a time. After receiving one
// notification from its C method, that goroutine MUST call Read exactly once,
// before receiving again, unsubscribing, or handing the subscription to another
// goroutine. Read copies the value and advances the local publication number.
//
// Send applies backpressure: each subscriber present at the start of a
// publication must receive its notification or unsubscribe before Send returns.
// A subscriber joining during a publication starts with the next publication.
// Subscribers must therefore keep receiving or promptly unsubscribe. Send waits
// for notification handoffs, not for Read calls or application processing.
//
// The alternating channels also protect the shared slots. Before a slot can be
// overwritten two publications later, every remaining reader of its old value
// must receive the intervening publication. Its earlier Read must finish before
// that receive. For an unbuffered channel, the receive happens before completion
// of the corresponding send, providing the required reclamation ordering. A
// departing reader supplies that ordering through Unsubscribe instead.
//
// The channels carry only struct{}, so each value is stored once by Send and
// copied directly by readers. As with ordinary channels, reference-containing
// values do not imply a deep copy or permit concurrent mutation of their data.
// There are no explicit allocations after construction, including subscription
// churn; compiler/runtime allocations and allocation by V itself are separate.
type PhasePubSub[V any] struct {
	sendMu sync.Mutex
	mu     sync.Mutex

	// All writes to subscribers, epoch, and active hold mu. The atomic count
	// also supplies the empty-publisher fast path without acquiring either lock.
	subscribers atomic.Int32
	epoch       uint64
	active      bool
	canceled    int

	notify [2]chan struct{}
	value  [2]V
}

// PhaseSubscription is a caller-owned subscription to a PhasePubSub. Its zero
// value is unsubscribed. It must not be copied after first use or used
// concurrently. Moving an unused value into its eventual owner is permitted.
//
// There is no registry entry or channel associated with this handle. C and Read
// only access the handle and the current shared slot; they acquire no mutex and
// perform no atomic read-modify-write operation.
type PhaseSubscription[V any] struct {
	pub   *PhasePubSub[V]
	epoch uint64
}

// NewPhasePubSub constructs a broadcaster with two unbuffered notification
// channels. The channels remain open for the lifetime of the broadcaster.
// Consumers select on their own cancellation signal and then Unsubscribe.
func NewPhasePubSub[V any]() *PhasePubSub[V] {
	x := new(PhasePubSub[V])
	x.notify[0] = make(chan struct{})
	x.notify[1] = make(chan struct{})
	return x
}

// Subscribe immediately registers a subscriber. If a publication is already
// in progress, the subscriber joins the next publication without waiting for
// the current one to finish. The returned handle need not be heap allocated.
func (x *PhasePubSub[V]) Subscribe() PhaseSubscription[V] {
	x.checkInitialized()
	x.mu.Lock()
	n := x.subscribers.Load()
	if n == math.MaxInt32 {
		x.mu.Unlock()
		panic("bigbuff: phasepubsub: too many subscribers")
	}
	x.subscribers.Store(n + 1)
	epoch := x.epoch
	if x.active {
		epoch++
	}
	x.mu.Unlock()
	return PhaseSubscription[V]{pub: x, epoch: epoch}
}

// Send publishes value and returns the number of subscribers that received its
// notification, excluding notifications drained by Unsubscribe. It does not
// wait for those subscribers to call Read. Publications are serialized.
// With no subscribers, Send returns zero without acquiring a lock.
func (x *PhasePubSub[V]) Send(value V) int {
	x.checkInitialized()
	if x.subscribers.Load() == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	n := int(x.subscribers.Load())
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	i := x.epoch & 1
	x.active = true
	x.canceled = 0
	x.mu.Unlock()

	// The preceding publication's handoffs/unsubscriptions protect this slot
	// from old readers. This write precedes all current notification handoffs.
	x.value[i] = value
	for range n {
		x.notify[i] <- struct{}{}
	}

	x.mu.Lock()
	sent := n - x.canceled
	x.active = false
	x.epoch++
	if x.subscribers.Load() == 0 {
		// All readers copied before leaving; release references in both slots.
		x.value = [2]V{}
	}
	x.mu.Unlock()
	return sent
}

// C returns the channel for this subscription's next notification. It returns
// nil for an unsubscribed handle, disabling its case in a select statement.
// Do not cache the channel across Read calls: it alternates on every Read.
func (s *PhaseSubscription[V]) C() <-chan struct{} {
	if s.pub == nil {
		return nil
	}
	return s.pub.notify[s.epoch&1]
}

// Read copies the current value and advances to the next notification channel.
// It MUST be called exactly once after each receive from C and before any
// further use of the subscription. Calling it without receiving first, or
// receiving again/unsubscribing before Read, violates the protocol.
func (s *PhaseSubscription[V]) Read() V {
	if s.pub == nil {
		panic("bigbuff: phasepubsub: read of unsubscribed handle")
	}
	// Copy before advancing: a subsequent receive or Unsubscribe permits this
	// slot to be reused. No reference to the slot is exposed to the caller.
	value := s.pub.value[s.epoch&1]
	s.epoch++
	return value
}

// Unsubscribe removes this subscription. Repeated calls are harmless, but
// concurrent calls or concurrent receives/Read calls on the same handle are
// not permitted. A notification already received MUST be followed by Read
// before calling Unsubscribe.
//
// If an unreceived notification is already owed, Unsubscribe receives and
// discards it. It can therefore wait for the active publisher
// to progress, but never waits for another publication to begin.
func (s *PhaseSubscription[V]) Unsubscribe() {
	x := s.pub
	if x == nil {
		return
	}

	x.mu.Lock()
	epoch := x.epoch
	n := x.subscribers.Load()
	if n <= 0 || (s.epoch != epoch && (!x.active || s.epoch != epoch+1)) {
		x.mu.Unlock()
		panic("bigbuff: phasepubsub: subscription invariant violation")
	}
	owed := x.active && s.epoch == epoch
	x.subscribers.Store(n - 1)
	if owed {
		x.canceled++
	}
	s.pub = nil
	if n == 1 && !x.active {
		x.value = [2]V{}
	}
	x.mu.Unlock()

	if owed {
		// This publication cannot finish while our receipt is still owed.
		// Therefore it cannot recycle this phase before this operation has
		// completed the cancelling receive. Record cancellation before the
		// receive so Send's final count cannot race ahead of that accounting.
		<-x.notify[epoch&1]
	}
}

func (x *PhasePubSub[V]) checkInitialized() {
	if x == nil || x.notify[0] == nil || x.notify[1] == nil {
		panic("bigbuff: phasepubsub: must use factory function")
	}
}
-- README.md --
# Two-phase Go broadcast

**Use `PhasePubSub` when minimizing additional reader synchronization is the priority.** It removes the second receive barrier from the supplied `ChanPubSub` design while keeping native `select`, ordered delivery, immediate registration, and broadcaster storage independent of subscriber count. Readers copy from one of two shared value slots and advance their own publication number. `Read` takes no mutex and performs no atomic read-modify-write operation. It is a useful starting point for direct-receive workloads, with no claim of being uniformly fastest.

`RelayPubSub` is a complementary implementation for controlling how much notification work the publisher performs. It seeds a bounded number of reusable credits; readers forward or retire them. It also provides a stronger completion condition: `Send` waits until every recipient has copied the value or cancelled its participation. Benchmark both implementations with the actual receive/select structure: relative performance changes with fan-out, processor count, width, and which additional channels the selects share.

Both implementations use ordinary Go channels, mutexes, and atomics. They contain no `unsafe`, `go:linkname`, runtime modifications, background goroutines, subscriber registry, or channel allocated per subscriber. The supplied [`ChanCaster`](chancaster.go) and [`ChanPubSub`](chanpubsub.go) remain available as baselines. This standalone module uses an example module path; the new production files can be added to the existing `bigbuff` package.

## APIs and completion contracts

```go
func NewPhasePubSub[V any]() *PhasePubSub[V]
func (*PhasePubSub[V]) Subscribe() PhaseSubscription[V]
func (*PhasePubSub[V]) Send(V) int

func NewRelayPubSub[V any](width int) *RelayPubSub[V]
func (*RelayPubSub[V]) Subscribe() RelaySubscription[V]
func (*RelayPubSub[V]) Send(V) int
```

Both subscription types expose `C() <-chan struct{}`, `Read() V`, and `Unsubscribe()`. Constructors are mandatory. Relay width must be between `1` and `math.MaxInt32`; it does not limit the subscriber count. Both implementations permit up to `math.MaxInt32` subscriptions.

| Property | Supplied `ChanPubSub` | `PhasePubSub` | `RelayPubSub` |
| --- | --- | --- | --- |
| Channel payload | `V` | Notification | Notification credit |
| Required action after receipt | `Wait()` | `Read()` | `Read()` |
| Completion of `Send` | All counted recipients received and entered the acknowledgement protocol | All counted notification handoffs completed | All counted recipients copied the value |
| Recipient coordination after receipt | Shared condition-variable barrier | Local publication increment | One atomic decrement; possible credit forwarding |
| Publisher channel sends for `N` snapshot members | `N`, subject to cancellation accounting | Exactly `N` | `min(width, N)` |
| Broadcaster payload slots | Channel transport | Two | One, cleared before `Send` returns |

Neither new `Send` waits for application handlers to finish. Their returned count excludes snapshot members that cancel before consuming their value. For Phase, that count acknowledges notification receipt; a corresponding `Read` can still be pending. Publications are serialized, although an empty fast-path call can return without waiting for another publisher.

## Native `select` usage

Given a context, an event, and a handler, register before starting publication:

```go
bus := bigbuff.NewPhasePubSub[Event]()
sub := bus.Subscribe()

go func() {
    defer sub.Unsubscribe()
    for {
        select {
        case <-ctx.Done():
            return
        case <-sub.C():
            value := sub.Read()
            handle(value)
        }
    }
}()

sent := bus.Send(event)
```

Use `bigbuff.NewRelayPubSub[Event](1)` for the same subscription loop with the relay implementation. Width one is worth measuring first: in the recorded concurrent-select workloads it performed better than wider relays. It is a tuning parameter, not a universal recommendation or an application-concurrency limit.

Evaluate `C()` for every receive. `Read` changes which permanent channel the handle uses; caching the old channel across reads breaks the protocol. After selecting the notification case, call `Read` exactly once, before application code, cancellation, or another subscription operation. A context becoming cancelled at the same time does not remove this obligation. Handle cancellation in the owning goroutine's `select`.

The notification channels never close. Terminate through a context or another select case, followed by `Unsubscribe`. A zero or unsubscribed handle returns a nil channel, so its select case is disabled. `Unsubscribe` is idempotent.

### Several subscriptions in one goroutine

One goroutine may own several handles, including handles for the same broadcaster:

```go
a, b := bus.Subscribe(), bus.Subscribe()
defer a.Unsubscribe()
defer b.Unsubscribe()

for {
    select {
    case <-ctx.Done():
        return
    case <-a.C():
        handleA(a.Read())
    case <-b.C():
        handleB(b.Read())
    }
}
```

Initially the cases can reference the same channel. Only the selected handle advances. The remaining handle can receive the same publication on the next iteration because `Read` does not wait for other recipients. The publisher must run independently; synchronously calling `Send` from the sole goroutine responsible for receiving its own subscriptions would block progress. Handlers must also return so that remaining subscriptions can continue receiving.

## Membership, cancellation, and ownership

A short membership mutex establishes each publication's snapshot. A subscription registered before that snapshot participates in the publication. One registered while it is active begins with the following publication. Registration does not wait for the active publication to finish, although it can briefly contend on the mutex. Empty sends publish nothing and do not advance Phase's publication number.

Unsubscription removes a handle from future snapshots immediately under that mutex. If the handle still owes the active publication, it records a cancellation and receives exactly one notification or credit after unlocking. That operation can wait for the current publisher or relay to progress. It does not require a future publication. If the handle already completed `Read`, or joined after the snapshot, it has no current notification to drain.

Recording cancellation before receiving its notification ensures the sender cannot finish its final handoff before accounting for that cancellation. Phase still performs exactly the snapshotted number of sends. Relay uses the same total participation count: each original member eventually reads or drains one credit. A caller must never unsubscribe between receiving a notification and calling `Read`; doing so would attempt to settle an already-claimed notification twice.

Subscriptions are caller-owned mutable values. The value returned by `Subscribe` can be placed in its eventual owner, but an active handle must not be duplicated. Calling methods through two copies would manufacture two consumers for one registration. Do not concurrently receive, read, or unsubscribe through the same handle. Ownership can move between goroutines using ordinary synchronization, with one owner at a time and no outstanding notification claim. Broadcaster instances must not be copied either.

Each live subscription must continue receiving or unsubscribe. One abandoned subscription can stall all publishers. These are backpressured broadcast primitives, with no dropping, replay log, detached cleanup worker, or publisher timeout after publication starts.

## Why the Phase slots are safe

Publication `E` writes slot `E & 1` before sending its notifications. Channel communication publishes that write to each recipient. A recipient copies the slot in `Read`, then increments its local publication number. This prevents it from receiving the same publication twice.

Slot `E & 1` is next overwritten by publication `E+2`. Before starting that publication, the serialized publisher must finish all notification handoffs for `E+1`. Every continuing recipient must finish `Read(E)` before receiving `E+1`. An unbuffered receive synchronizes before completion of its corresponding send. Therefore the `E+1` handoffs order all those earlier copies before the write for `E+2`. This uses the [Go memory model's channel guarantees](https://go.dev/ref/mem#chan), including the unbuffered reverse synchronization rule.

A departing recipient supplies the ordering through its mutex-protected removal after `Read`, or through the intervening notification that `Unsubscribe` drains. New subscribers cannot claim an older notification: their local publication number selects the next channel, and the previous use of that channel has already completed its handoffs. Unsigned publication-number wrap is harmless because the protocol uses equality, increment, and parity, rather than numeric ordering.

The unbuffered channels are essential. Buffering them would let a publication finish before recipients performed the intervening receives, removing the slot-reuse guarantee. The constructor fixes this property.

Phase cannot clear its just-published slot at `Send` return because a valid `Read` may still be pending. It retains at most two `V` values while members remain and clears both when membership reaches zero safely. The implementation intentionally avoids an additional payload-sized clearing write after every publication.

## Why the relay credits are safe

For a snapshot of `N` members, let `W` be the configured width and `K = min(W, N)`. The publisher writes its shared slot, sets `remaining = N`, and seeds `K` credits. After receiving one, a reader copies the value, advances its local phase, and computes:

```go
r := remaining.Add(-1)
switch {
case r >= W:
    // Forward this credit to another recipient of the same publication.
case r == 0:
    // Notify the publisher that this publication is complete.
default:
    // Retire the credit.
}
```

Exactly `N-K` decrements require forwarding. The initial seeds plus those forwards therefore produce exactly `K + (N-K) = N` receives. Alternating channels prevent a reader that already advanced, or a new subscriber, from consuming another current-publication credit.

Reaching zero requires all `N` receives. Consequently every required seed and forward must already be visible: there cannot be a delayed old credit arriving after completion. At most `K` credits exist, including those held by readers. A reader holding one leaves buffer capacity for its forwarding send. Seeding and forwarding therefore do not park because the notification buffer is full; they still use normal channel synchronization.

Every value copy precedes its atomic decrement. The read-modify-write sequence connects those reads to the final decrement, then to the completion-channel handoff. The publisher can consequently clear or overwrite the single slot after receiving completion. This relies on [Go's sequentially consistent atomic operations](https://pkg.go.dev/sync/atomic), not relaxed atomics or assumptions about one processor architecture.

Cancellation consumes and releases a credit through the same protocol. It must not decrement `remaining` without receiving: that could leave a stale credit behind. Width controls circulating notifications and the publisher's initial wake burst. It does not bound concurrently running application handlers. Small widths introduce longer notification dependency chains; larger widths increase initial publisher work.

## Allocation, payloads, and runtime costs

After construction, neither implementation contains an explicit allocation in publication, registration, read, channel selection, or unsubscription. Each handle holds a broadcaster pointer and its local publication state. Broadcaster-owned storage does not grow with subscriber count or churn.

This is not a universal zero-heap-allocation guarantee for arbitrary `V` or surrounding code. Compiler escape decisions, very large value temporaries, goroutine stacks, and runtime waiter allocation remain relevant. Measure the concrete instantiation and call pattern. Because channels carry `struct{}` rather than `V`, these implementations also support values larger than Go's channel-element limit; the included tests exercise this case without claiming those large copies are allocation-free.

`Read` returns a value copy, never a pointer to an internal slot. For pointers, slices, maps, and reference-containing structs, that copy still aliases the referenced objects. Their lifetime and mutation require the same care as ordinary channel messages. Neither `Send` completion nor slot clearing proves that application handlers finished using those objects; use immutable payloads or separate application synchronization before mutating or reusing them.

Removing a package-owned subscriber registry does not remove runtime waiter state. Native channel/select operations register blocked goroutines and select cases in runtime queues; see [`runtime/chan.go`](https://go.dev/src/runtime/chan.go) and [`runtime/select.go`](https://go.dev/src/runtime/select.go). Caller handles, goroutines, and blocked select cases still consume memory proportional to their number. Delivery to `N` consumers still requires `O(N)` total work. Relay bounds publisher-issued notifications, not completed-broadcast complexity.

## Validation and measurements

The test module requires Go 1.25 or newer because deterministic relay tests use `testing/synctest`. Recorded execution used **Go 1.27.3 on Linux/amd64**. Darwin/arm64 and Linux/386 were compilation checks only; the execution host could not run the Linux/386 binary. See [validation output](results/validation.txt) and [portability output](results/portability.txt).

```sh
go test -count=1 ./...
go test -race -cpu=1,2,8 -count=5 -timeout=90s ./...
go vet ./...
python3 model/credit_model.py

GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c -o /tmp/bigbuff-darwin-arm64.test .
GOOS=linux GOARCH=386 CGO_ENABLED=0 go test -c -o /tmp/bigbuff-linux-386.test .
```

The [finite-state checker](model/credit_model.py) exhaustively explores 50,011 abstract states for `N=1..12` and every `K=1..N`, separating seeding, receiving, copying, decrementing, and delayed forwarding. It checks credit capacity and completion invariants. Membership admission, phase reuse, the Go memory model, misuse, and scheduler fairness are outside that model; the source reasoning and concurrency tests address different parts of the argument.

To rerun the measured matrix without overwriting its recorded evidence:

```sh
mkdir -p rerun-results
go test -run='^$' -bench='^BenchmarkBroadcast$' -benchmem -benchtime=150ms -count=5 -cpu=1,8 . > rerun-results/bench-broadcast.txt
go test -run='^$' -bench='^Benchmark(Select|Payload1KiB)$' -benchmem -benchtime=100ms -count=5 -cpu=1,8 . > rerun-results/bench-select-payload.txt
go test -run='^$' -bench='^Benchmark(IdleChurn|Construct)$' -benchmem -benchtime=150ms -count=5 -cpu=1,8 . > rerun-results/bench-allocation.txt
python3 model/summarize_benchmarks.py rerun-results
```

See the [measurement summary](results/summary.md), [raw broadcast log](results/bench-broadcast.txt), and other files in [`results`](results/). The harness validates delivery order, excludes setup and teardown, and includes a final join of all value copies before stopping its timer. That join accounts for Phase's earlier return without adding a barrier to every publication. The select benchmark adds one shared, never-cancelled shutdown channel to every receiver's select. Its results need not follow the ordering of direct-receive results; benchmark the application's actual channel sharing. `LinearChannels` is a static per-subscriber-channel reference, not a reproduction of an unseen callback implementation or its churn costs. Results from this host do not establish performance on Darwin/arm64 or other schedulers and workloads.

## Recorded results

Measured on an AMD EPYC 9V74 virtual machine with an eight-CPU cgroup quota, using Go 1.27.3 on Linux/amd64. The following are medians of five samples at `GOMAXPROCS=8`, in **microseconds per publication**, with the final value-copy join included. These are throughput measurements of the supplied harness, not latency bounds. Full raw samples, ranges, processor-count variants, widths 1/4/16/64, and environment details are in `results/`.

### Direct receives, `uint64` payload

| Subscribers | Supplied ChanPubSub | PhasePubSub | Relay, width 1 | Static per-subscriber channels |
|---:|---:|---:|---:|---:|
| 1 | 0.500 | 0.202 | 0.388 | 0.177 |
| 8 | 2.936 | 2.497 | 1.884 | 2.477 |
| 64 | 27.078 | 22.539 | 13.930 | 15.741 |
| 512 | 262.041 | 152.040 | 117.186 | 89.676 |
| 4096 | 3076.048 | 1006.128 | 1120.944 | 559.556 |

At 4,096 subscribers, Phase used about one third of the supplied implementation’s time; the static per-subscriber-channel reference remained faster. At 64 subscribers the one-credit relay also beat the static reference in this run. At one processor, 4,096-subscriber medians were 1,343.569 us (supplied), 886.439 us (Phase), 1,174.925 us (Relay width one), and 1,208.368 us (static channels). There is no consistent universal ranking.

### Native `select` with one shared cancellation channel

The 64-subscriber row below is a focused follow-up with five **400 ms** samples; the 512-subscriber row is from the original matrix with five **100 ms** samples.

| Subscribers | Supplied ChanPubSub | PhasePubSub | Relay, width 1 | Static per-subscriber channels |
|---:|---:|---:|---:|---:|
| 64 | 45.912 | 46.976 | 17.886 | 31.519 |
| 512 | 937.136 | 577.491 | 161.023 | 255.192 |

The initial short 64-subscriber matrix measured 29.129 us (supplied), 57.928 us (Phase), and 19.786 us (Relay width one). The longer repeat narrowed the supplied/Phase difference considerably, while preserving the narrow relay’s advantage. This variability is why the full samples are retained. Wider relay settings were substantially slower for some shared-cancellation workloads: at 512 subscribers, width four measured 623.773 us, versus 161.023 us for width one. A narrower wake chain can be beneficial; these timings alone do not establish the exact scheduler or lock-contention cause.

### 1 KiB inline payload, direct receives

| Subscribers | Supplied ChanPubSub | PhasePubSub | Relay, width 1 | Static per-subscriber channels |
|---:|---:|---:|---:|---:|
| 8 | 5.285 | 3.676 | 2.490 | 3.672 |
| 64 | 31.883 | 24.058 | 19.306 | 19.536 |
| 512 | 327.689 | 166.830 | 164.519 | 138.285 |

### Allocation and idle churn

Construction figures use `V=uint64`; churn is one subscribe/unsubscribe pair, without a publication. Counts exclude caller-created goroutines and subscription storage.

| Implementation | Bytes per construction | Allocations per construction | ns per idle subscribe/unsubscribe pair | Churn allocations |
|---|---:|---:|---:|---:|
| ChanPubSub | 408 | 5 | 27.08 | 0 |
| PhasePubSub | 304 | 3 | 15.58 | 0 |
| RelayPubSub | 416 | 4 | 16.85 | 0 |

All measured new-implementation publication configurations reported a median of **0 allocs/op**. Median bytes were zero except for the 4,096-subscriber, eight-P, width-one relay run, which reported 1 B/op. Some individual samples also had small nonzero byte counts. Runtime allocation and the integer presentation/averaging of benchmark metrics therefore matter: the defensible implementation property is no explicit operation-time allocation, not a guarantee that every scheduler activity allocates nothing. Constructor allocations are fixed per broadcaster; subscriber count and churn do not add internal channels or registry entries.

The summary contains 840 benchmark samples across 168 configurations. The 25 longer select follow-up samples remain in `results/check-select64.txt` separately. Initial N1 broadcast samples overlapped a brief compilation and are preserved but replaced by clean samples in the summary. No measurements from the initial exploratory timing run are included.

### Integration

Copy `phasepubsub.go` and/or `relaypubsub.go` into the existing `bigbuff` package. Neither new implementation depends on the supplied baselines or on each other. The tests use `package bigbuff` and no module-specific import. Copying the complete benchmark harness additionally requires both new implementations and the two supplied baseline files. The existing checkout at `/Users/joeyc/dev/go-bigbuff` was not available in this environment; no changes to that checkout or its upstream repository were made.
-- relaypubsub_test.go --
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
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func relayAssertBlocked[T any](t *testing.T, c <-chan T, operation string) {
	t.Helper()
	select {
	case got := <-c:
		t.Fatalf("%s completed unexpectedly: %v", operation, got)
	default:
	}
}

func relayAssertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected a panic")
		}
	}()
	f()
}

func TestRelayPubSubEmptyAndHandleLifecycle(t *testing.T) {
	overflowWidth := int64(math.MaxInt32) + 1
	for _, width := range []int{0, -1, int(overflowWidth)} {
		t.Run(fmt.Sprintf("invalid-width=%d", width), func(t *testing.T) {
			relayAssertPanics(t, func() { NewRelayPubSub[int](width) })
		})
	}
	var uninitialized RelayPubSub[int]
	relayAssertPanics(t, func() { uninitialized.Subscribe() })
	relayAssertPanics(t, func() { uninitialized.Send(1) })

	var zero RelaySubscription[int]
	if zero.C() != nil {
		t.Fatal("zero subscription has a nonnil channel")
	}
	zero.Unsubscribe()
	relayAssertPanics(t, func() { zero.Read() })

	bus := NewRelayPubSub[int](1)
	if got := bus.Send(1); got != 0 {
		t.Fatalf("empty Send = %d, want 0", got)
	}
	sub := bus.Subscribe()
	if sub.C() == nil {
		t.Fatal("active subscription has a nil channel")
	}
	sub.Unsubscribe()
	sub.Unsubscribe()
	if sub.C() != nil {
		t.Fatal("unsubscribed handle has a nonnil channel")
	}
	if got := bus.Send(2); got != 0 {
		t.Fatalf("Send after unsubscribe = %d, want 0", got)
	}
	relayAssertPanics(t, func() { sub.Read() })
}

// Holding a received notification simulates preemption between the select
// operation and Read. Delivery is incomplete until the payload has been copied.
func TestRelayPubSubWaitsForRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewRelayPubSub[int](1)
		sub := bus.Subscribe()
		defer sub.Unsubscribe()
		for _, value := range []int{101, 202, 303} {
			sent := make(chan int, 1)
			go func() { sent <- bus.Send(value) }()
			<-sub.C()
			synctest.Wait()
			relayAssertBlocked(t, sent, "Send before Read")
			if got := sub.Read(); got != value {
				t.Fatalf("Read = %d, want %d", got, value)
			}
			if got := <-sent; got != 1 {
				t.Fatalf("Send = %d, want 1", got)
			}
			relayAssertBlocked(t, sub.C(), "next publication notification")
		}
	})
}

// A cancellation cannot merely decrement the reader count: with width one,
// the only credit may already belong to a different, preempted reader.
func TestRelayPubSubCancellationWaitsForCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewRelayPubSub[int](1)
		held := bus.Subscribe()
		defer held.Unsubscribe()
		canceled := bus.Subscribe()
		sent := make(chan int, 1)
		go func() { sent <- bus.Send(41) }()
		<-held.C()

		unsubscribed := make(chan struct{})
		go func() {
			canceled.Unsubscribe()
			close(unsubscribed)
		}()
		synctest.Wait()
		relayAssertBlocked(t, unsubscribed, "Unsubscribe without a credit")
		relayAssertBlocked(t, sent, "Send with one unread payload")

		if got := held.Read(); got != 41 {
			t.Fatalf("Read = %d, want 41", got)
		}
		<-unsubscribed
		if canceled.C() != nil {
			t.Fatal("canceled handle remains active")
		}
		if got := <-sent; got != 1 {
			t.Fatalf("Send = %d, want one copied value", got)
		}

		go func() { sent <- bus.Send(42) }()
		<-held.C()
		if got := held.Read(); got != 42 {
			t.Fatalf("next Read = %d, want 42", got)
		}
		if got := <-sent; got != 1 {
			t.Fatalf("next Send = %d, want 1", got)
		}
	})
}

func TestRelayPubSubJoiningSkipsInProgressPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewRelayPubSub[int](2)
		first := bus.Subscribe()
		defer first.Unsubscribe()
		sent := make(chan int, 1)
		go func() { sent <- bus.Send(11) }()
		oldChannel := first.C()
		<-oldChannel

		// The first publication has certainly started, but cannot finish.
		second := bus.Subscribe()
		defer second.Unsubscribe()
		if second.C() == oldChannel {
			t.Fatal("new subscriber joined the in-progress phase")
		}
		relayAssertBlocked(t, second.C(), "new subscriber's first notification")
		temporary := bus.Subscribe()
		temporary.Unsubscribe() // Does not owe a credit from the old phase.
		if got := first.Read(); got != 11 {
			t.Fatalf("first Read = %d, want 11", got)
		}
		if got := <-sent; got != 1 {
			t.Fatalf("first Send = %d, want 1", got)
		}

		go func() { sent <- bus.Send(22) }()
		<-second.C()
		if got := second.Read(); got != 22 {
			t.Fatalf("new subscriber Read = %d, want 22", got)
		}
		synctest.Wait()
		relayAssertBlocked(t, sent, "next Send before first subscriber Read")
		<-first.C()
		if got := first.Read(); got != 22 {
			t.Fatalf("continuing subscriber Read = %d, want 22", got)
		}
		if got := <-sent; got != 2 {
			t.Fatalf("next Send = %d, want 2", got)
		}
	})
}

func TestRelayPubSubAllSubscribersCancel(t *testing.T) {
	for _, width := range []int{1, 3, 32} {
		for _, n := range []int{1, 2, 7, 31, 32, 33, 64} {
			t.Run(fmt.Sprintf("width=%d/subscribers=%d", width, n), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					bus := NewRelayPubSub[int](width)
					subs := make([]RelaySubscription[int], n)
					for i := range subs {
						subs[i] = bus.Subscribe()
					}
					sent := make(chan int, 1)
					go func() { sent <- bus.Send(10) }()
					synctest.Wait()
					relayAssertBlocked(t, sent, "Send without any receivers")
					var wg sync.WaitGroup
					wg.Add(n)
					for i := range subs {
						go func(s *RelaySubscription[int]) {
							defer wg.Done()
							s.Unsubscribe()
						}(&subs[i])
					}
					wg.Wait()
					if got := <-sent; got != 0 {
						t.Fatalf("Send = %d, want 0 copied values", got)
					}
					for i := range subs {
						if subs[i].C() != nil {
							t.Fatalf("subscription %d remains active", i)
						}
					}

					// A fully canceled round must leave no reusable-channel residue.
					next := bus.Subscribe()
					defer next.Unsubscribe()
					go func() { sent <- bus.Send(20) }()
					<-next.C()
					if got := next.Read(); got != 20 {
						t.Fatalf("Read after cancellation = %d, want 20", got)
					}
					if got := <-sent; got != 1 {
						t.Fatalf("Send after cancellation = %d, want 1", got)
					}
				})
			})
		}
	}
}

type relayTestPayload struct {
	Sequence uint64
	Words    [128]uint64
	Name     string
	Pointer  *uint64
	Bytes    []byte
}

func TestRelayPubSubOrderedPayloadDelivery(t *testing.T) {
	for _, tc := range []struct{ n, width int }{
		{1, 1}, {2, 1}, {7, 1}, {7, 3}, {31, 32}, {32, 32},
		{33, 32}, {65, 8}, {257, 32},
	} {
		t.Run(fmt.Sprintf("width=%d/subscribers=%d", tc.width, tc.n), func(t *testing.T) {
			const rounds = 128
			const marker = uint64(0x43d6f728ab52ec91)
			pointed := marker
			immutableBytes := []byte("immutable pointed data")
			bus := NewRelayPubSub[relayTestPayload](tc.width)
			subs := make([]RelaySubscription[relayTestPayload], tc.n)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(tc.n)
			for i := range subs {
				subs[i] = bus.Subscribe()
				go func(s *RelaySubscription[relayTestPayload]) {
					defer wg.Done()
					defer s.Unsubscribe()
					for sequence := uint64(1); sequence <= rounds; sequence++ {
						select {
						case <-stop:
							t.Error("stopped before all publications")
							return
						case <-s.C():
							v := s.Read()
							if v.Sequence != sequence || v.Name != "published value" ||
								v.Pointer != &pointed || !slices.Equal(v.Bytes, immutableBytes) {
								t.Errorf("payload header corrupted at sequence %d: sequence=%d, name=%q, pointer=%p, bytes=%q",
									sequence, v.Sequence, v.Name, v.Pointer, v.Bytes)
							}
							for j, word := range v.Words {
								if want := sequence ^ (uint64(j) * marker); word != want {
									t.Errorf("payload word %d at sequence %d = %x, want %x", j, sequence, word, want)
									break
								}
							}
						}
					}
				}(&subs[i])
			}
			for sequence := uint64(1); sequence <= rounds; sequence++ {
				v := relayTestPayload{Sequence: sequence, Name: "published value", Pointer: &pointed, Bytes: immutableBytes}
				for j := range v.Words {
					v.Words[j] = sequence ^ (uint64(j) * marker)
				}
				if got := bus.Send(v); got != tc.n {
					t.Errorf("Send at sequence %d = %d, want %d", sequence, got, tc.n)
				}
			}
			wg.Wait()
			close(stop)
		})
	}
}

func TestRelayPubSubConcurrentPublishers(t *testing.T) {
	for _, width := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			const subscribers, publishers, perPublisher = 17, 4, 64
			const total = publishers * perPublisher
			bus := NewRelayPubSub[int](width)
			subs := make([]RelaySubscription[int], subscribers)
			observed := make([][]int, subscribers)
			var readers sync.WaitGroup
			readers.Add(subscribers)
			for i := range subs {
				subs[i] = bus.Subscribe()
				observed[i] = make([]int, 0, total)
				go func(i int) {
					defer readers.Done()
					defer subs[i].Unsubscribe()
					for range total {
						<-subs[i].C()
						observed[i] = append(observed[i], subs[i].Read())
					}
				}(i)
			}
			start := make(chan struct{})
			var writers sync.WaitGroup
			writers.Add(publishers)
			for p := range publishers {
				go func(p int) {
					defer writers.Done()
					<-start
					for j := range perPublisher {
						if got := bus.Send(p*perPublisher + j); got != subscribers {
							t.Errorf("Send = %d, want %d", got, subscribers)
						}
					}
				}(p)
			}
			close(start)
			writers.Wait()
			readers.Wait()
			seen := make([]bool, total)
			for _, value := range observed[0] {
				if value < 0 || value >= total || seen[value] {
					t.Fatalf("unexpected or duplicate publication %d", value)
				}
				seen[value] = true
			}
			for i := 1; i < subscribers; i++ {
				if !slices.Equal(observed[0], observed[i]) {
					t.Fatalf("subscriber %d observed a different global publication order", i)
				}
			}
		})
	}
}

func TestRelayPubSubConcurrentChurnAccounting(t *testing.T) {
	for _, width := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			const rounds, workers, cycles = 1200, 24, 256
			bus := NewRelayPubSub[uint64](width)
			anchor := bus.Subscribe()
			stop := make(chan struct{})
			abandon := make(chan struct{})
			close(abandon)
			var observed atomic.Int64
			var wg sync.WaitGroup
			wg.Add(workers + 1)
			go func() {
				defer wg.Done()
				defer anchor.Unsubscribe()
				for want := uint64(1); want <= rounds; want++ {
					<-anchor.C()
					if got := anchor.Read(); got != want {
						t.Errorf("anchor Read = %d, want %d", got, want)
					}
					observed.Add(1)
				}
			}()
			for worker := range workers {
				go func(worker int) {
					defer wg.Done()
					var last uint64
					seed := uint64(worker + 1)
					for cycle := range cycles {
						seed ^= seed << 13
						seed ^= seed >> 7
						seed ^= seed << 17
						sub := bus.Subscribe()
						if seed&3 == 0 {
							sub.Unsubscribe()
							continue
						}
						var cancellation <-chan struct{}
						if seed&3 == 1 {
							cancellation = abandon
						}
						for range 1 + int((seed>>2)&3) {
							select {
							case <-stop:
								sub.Unsubscribe()
								return
							case <-cancellation:
								goto leave
							case <-sub.C():
								got := sub.Read()
								observed.Add(1)
								if got <= last || got > rounds {
									t.Errorf("worker %d Read = %d after %d", worker, got, last)
								}
								last = got
							}
						}
					leave:
						sub.Unsubscribe()
						if cycle&7 == 0 {
							runtime.Gosched()
						}
					}
				}(worker)
			}
			var reported int64
			for value := uint64(1); value <= rounds; value++ {
				reported += int64(bus.Send(value))
			}
			close(stop)
			wg.Wait()
			if got := observed.Load(); got != reported {
				t.Fatalf("Send reported %d copies, subscribers observed %d", reported, got)
			}
			if got := bus.Send(rounds + 1); got != 0 {
				t.Fatalf("Send after all subscribers left = %d, want 0", got)
			}
		})
	}
}

// AllocsPerRun measures a warmed, single-P path. It is not a claim about
// first-use runtime waiter allocations, stack growth, or caller-created data.
func TestRelayPubSubSteadyStateAllocations(t *testing.T) {
	bus := NewRelayPubSub[int](8)
	sub := bus.Subscribe()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sub.Unsubscribe()
		for {
			select {
			case <-stop:
				return
			case <-sub.C():
				if got := sub.Read(); got != 1 {
					t.Errorf("Read = %d, want 1", got)
				}
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	for range 100 {
		bus.Send(1)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		transient := bus.Subscribe()
		transient.Unsubscribe()
		if got := bus.Send(1); got != 1 {
			t.Errorf("Send = %d, want 1", got)
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed Subscribe/Unsubscribe/Send/Read = %g allocations, want 0", allocations)
	}
}

// A chan carrying this value directly is rejected by the compiler because its
// element is 64 KiB. Notification channels impose no such limit on the payload.
func TestRelayPubSubValueLargerThanChannelElement(t *testing.T) {
	const subscribers, rounds = 3, 3
	bus := NewRelayPubSub[[65536]byte](2)
	subs := make([]RelaySubscription[[65536]byte], subscribers)
	var wg sync.WaitGroup
	wg.Add(subscribers)
	for i := range subs {
		subs[i] = bus.Subscribe()
		go func(s *RelaySubscription[[65536]byte]) {
			defer wg.Done()
			defer s.Unsubscribe()
			for sequence := 1; sequence <= rounds; sequence++ {
				<-s.C()
				value := s.Read()
				for j, got := range value {
					if want := byte(j*13 + sequence); got != want {
						t.Errorf("sequence %d byte %d = %d, want %d", sequence, j, got, want)
						break
					}
				}
			}
		}(&subs[i])
	}
	for sequence := 1; sequence <= rounds; sequence++ {
		var value [65536]byte
		for j := range value {
			value[j] = byte(j*13 + sequence)
		}
		if got := bus.Send(value); got != subscribers {
			t.Errorf("Send = %d, want %d", got, subscribers)
		}
	}
	wg.Wait()
}

// A single goroutine owns all three handles. The selected Read must return
// before that goroutine can select and receive the other two notifications.
func TestRelayPubSubOneGoroutineOwnsSeveralSubscriptions(t *testing.T) {
	for _, width := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const rounds = 12
				bus := NewRelayPubSub[int](width)
				a, b, c := bus.Subscribe(), bus.Subscribe(), bus.Subscribe()
				defer a.Unsubscribe()
				defer b.Unsubscribe()
				defer c.Unsubscribe()
				sent := make(chan int, rounds)
				go func() {
					for value := 1; value <= rounds; value++ {
						sent <- bus.Send(value)
					}
				}()
				var counts [3]int
				read := func(s *RelaySubscription[int], index int) {
					got := s.Read()
					counts[index]++
					if got != counts[index] {
						t.Errorf("handle %d Read = %d, want %d", index, got, counts[index])
					}
				}
				for range 3 * rounds {
					select {
					case <-a.C():
						read(&a, 0)
					case <-b.C():
						read(&b, 1)
					case <-c.C():
						read(&c, 2)
					}
				}
				for range rounds {
					if got := <-sent; got != 3 {
						t.Errorf("Send = %d, want 3", got)
					}
				}
				if want := [3]int{rounds, rounds, rounds}; counts != want {
					t.Fatalf("per-handle deliveries = %v, want %v", counts, want)
				}
			})
		})
	}
}

func TestPhasePubSubOneGoroutineOwnsSeveralSubscriptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const rounds = 12
		bus := NewPhasePubSub[int]()
		a, b, c := bus.Subscribe(), bus.Subscribe(), bus.Subscribe()
		defer a.Unsubscribe()
		defer b.Unsubscribe()
		defer c.Unsubscribe()
		sent := make(chan int, rounds)
		go func() {
			for value := 1; value <= rounds; value++ {
				sent <- bus.Send(value)
			}
		}()
		var counts [3]int
		read := func(s *PhaseSubscription[int], index int) {
			got := s.Read()
			counts[index]++
			if got != counts[index] {
				t.Errorf("handle %d Read = %d, want %d", index, got, counts[index])
			}
		}
		for range 3 * rounds {
			select {
			case <-a.C():
				read(&a, 0)
			case <-b.C():
				read(&b, 1)
			case <-c.C():
				read(&c, 2)
			}
		}
		for range rounds {
			if got := <-sent; got != 3 {
				t.Errorf("Send = %d, want 3", got)
			}
		}
		if want := [3]int{rounds, rounds, rounds}; counts != want {
			t.Fatalf("per-handle deliveries = %v, want %v", counts, want)
		}
	})
}
-- relaypubsub.go --
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
	"math"
	"sync"
	"sync/atomic"
)

// RelayPubSub broadcasts a value using a bounded number of circulating wake
// credits. It uses two permanent notification channels, one shared value slot,
// and caller-owned subscriptions. It retains no subscriber list and starts no
// goroutines. Use NewRelayPubSub to construct one; do not copy it after use.
//
// Each publication snapshots its subscribers. A subscriber added during a
// publication starts with the next publication. Send waits until each member of
// its snapshot has copied the value or unsubscribed. Delivery is ordered and
// lossless for subscribers that remain subscribed. One slow subscriber can
// therefore delay every sender. Send does not wait for application processing.
//
// The width passed to NewRelayPubSub controls concurrent notification credits,
// not buffering of publications. The publisher seeds min(width, subscribers)
// credits. Readers forward or retire them. All readers together still perform
// O(subscribers) work, but the publisher performs only O(min(width, subscribers))
// channel sends and receives one completion notification.
//
// Send, Subscribe, Read, C, and Unsubscribe contain no explicit allocation.
// Compiler escape decisions (including for very large V), caller allocation,
// and Go's scheduler/stack allocations are separate from this property. Storage
// owned by the broadcaster is independent of subscriber count and churn. Each
// caller owns a small subscription value.
type RelayPubSub[V any] struct {
	// remaining counts readers whose copy or cancellation has not completed.
	// Its atomic modification order also joins all value reads before reuse.
	remaining atomic.Int32

	sendMu sync.Mutex
	mu     sync.Mutex

	// Membership changes hold mu. The atomic permits the empty Send fast path.
	members atomic.Int32
	width   int32

	// phase, active and canceled are protected by mu. phase names the last
	// started publication, even when inactive. New members use phase ^ 1.
	phase    uint8
	active   bool
	canceled int32

	ready [2]chan struct{}
	done  chan struct{}
	value V
}

// RelaySubscription is a single caller's participation in a RelayPubSub.
// Obtain it from Subscribe. Do not copy an active subscription, use it from
// multiple goroutines concurrently, or receive from its C more than once
// without the corresponding Read. A zero or unsubscribed value has a nil C.
//
// A subscription must keep receiving or promptly Unsubscribe. In particular,
// call Read immediately after its channel is selected, before invoking user
// code or Unsubscribe. The raw channel receive claims a credit; Read completes
// that claim. Unsubscribe is for the branch that did not receive a credit.
//
//	sub := bus.Subscribe()
//	defer sub.Unsubscribe()
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case <-sub.C():
//			value := sub.Read()
//			use(value)
//		}
//	}
//
// Evaluate C again for every receive: Read changes which of the broadcaster's
// two channels this subscriber must use. Neither notification channel closes;
// use a context or another select case for termination.
type RelaySubscription[V any] struct {
	owner *RelayPubSub[V]
	phase uint8
}

// NewRelayPubSub constructs a broadcaster with the specified maximum number of
// circulating credits. width must be in [1, math.MaxInt32]. A small width limits
// the wakeup burst; larger widths expose more concurrent readers. There is no
// universally optimal width. It does not limit the number of subscribers.
// Width is not a bound on concurrently running application handlers.
func NewRelayPubSub[V any](width int) *RelayPubSub[V] {
	if width < 1 || uint64(width) > math.MaxInt32 {
		panic("bigbuff: relaypubsub: width out of bounds")
	}
	return &RelayPubSub[V]{
		width: int32(width),
		ready: [2]chan struct{}{make(chan struct{}, width), make(chan struct{}, width)},
		done:  make(chan struct{}, 1),
	}
}

// Subscribe immediately registers a subscriber and returns its caller-owned
// handle. Registration does not wait for an in-progress publication, which this
// new subscriber will not receive. Up to math.MaxInt32 subscribers are allowed.
func (x *RelayPubSub[V]) Subscribe() RelaySubscription[V] {
	x.checkInitialized()
	x.mu.Lock()
	n := x.members.Load()
	if n == math.MaxInt32 {
		x.mu.Unlock()
		panic("bigbuff: relaypubsub: too many subscribers")
	}
	x.members.Store(n + 1)
	s := RelaySubscription[V]{owner: x, phase: x.phase ^ 1}
	x.mu.Unlock()
	return s
}

// Send broadcasts value and returns the number of subscribers that copied it.
// Concurrent Send calls are serialized, except that an empty fast-path call
// can return immediately. A concurrent unsubscribe may abandon this value and
// is excluded from the returned count. A concurrent subscribe starts with the
// next publication if this publication has already taken its snapshot.
//
// Like a channel send, this copies V, not the objects reachable through V.
// Callers must synchronize mutation of referenced slices, maps, or objects.
func (x *RelayPubSub[V]) Send(value V) int {
	x.checkInitialized()
	if x.members.Load() == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	n := x.members.Load()
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	x.phase ^= 1
	c := x.ready[x.phase]
	x.value = value
	x.remaining.Store(n)
	x.canceled = 0
	x.active = true
	x.mu.Unlock()

	// Credit conservation makes these sends nonblocking: at most k credits
	// exist, including credits in readers and the seeds still being issued.
	k := min(n, x.width)
	for range k {
		c <- struct{}{}
	}
	<-x.done

	x.mu.Lock()
	sent := n - x.canceled
	x.active = false
	// Do not retain a pointer-bearing payload while idle. All reads precede
	// their decrements; the last decrement and done receive join those reads.
	var zero V
	x.value = zero
	x.mu.Unlock()
	return int(sent)
}

// C returns the notification channel for this subscriber's next publication,
// or nil for a zero/unsubscribed handle. It must be reevaluated after every Read.
func (s *RelaySubscription[V]) C() <-chan struct{} {
	if s.owner == nil {
		return nil
	}
	return s.owner.ready[s.phase]
}

// Read must be called exactly once, immediately after receiving from C. It
// copies the published value, advances this subscriber to the opposite phase,
// and forwards or retires its credit. It does not wait for other subscribers.
// Calling Read without first receiving from C violates the protocol.
func (s *RelaySubscription[V]) Read() V {
	x := s.owner
	if x == nil {
		panic("bigbuff: relaypubsub: read on inactive subscription")
	}
	c := x.ready[s.phase]
	value := x.value
	// Advance before the decrement: the last decrement permits the next Send.
	s.phase ^= 1
	x.release(c)
	return value
}

// Unsubscribe deregisters this handle. It is idempotent. If this subscriber
// owes an in-progress publication, it consumes one credit without reading the
// value. It can block until that credit arrives, just as cancellation of an
// in-progress ChanCaster receive can block. It must not race a receive or Read
// on the same handle, and must not be called between a receive and its Read.
func (s *RelaySubscription[V]) Unsubscribe() {
	x := s.owner
	if x == nil {
		return
	}
	x.mu.Lock()
	n := x.members.Load()
	if n <= 0 {
		x.mu.Unlock()
		panic("bigbuff: relaypubsub: membership invariant violation")
	}
	x.members.Store(n - 1)
	owes := x.active && s.phase == x.phase
	if owes {
		x.canceled++
	}
	x.mu.Unlock()
	c := x.ready[s.phase]
	s.owner = nil
	if owes {
		<-c
		x.release(c)
	}
}

func (x *RelayPubSub[V]) release(c chan struct{}) {
	r := x.remaining.Add(-1)
	switch {
	case r >= x.width:
		c <- struct{}{}
	case r == 0:
		x.done <- struct{}{}
	case r < 0:
		panic("bigbuff: relaypubsub: credit invariant violation")
	}
	// If 0 < r < width, retire this credit. Exactly min(width,n) credits
	// retire, so no old-phase credit survives publication completion.
}

func (x *RelayPubSub[V]) checkInitialized() {
	if x.width == 0 {
		panic("bigbuff: relaypubsub: must use factory function")
	}
}
-- results/bench-allocation.txt --
goos: linux
goarch: amd64
pkg: example.com/bigbuff-evolution
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkConstruct/ChanPubSub           	 1240011	       135.8 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub           	 1437475	       123.2 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub           	 1454265	       124.0 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub           	 1443954	       123.7 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub           	 1442005	       124.3 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub-8         	  886606	       197.0 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub-8         	  708866	       237.4 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub-8         	  492063	       366.5 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub-8         	  783486	       557.0 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/ChanPubSub-8         	  497214	       325.8 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruct/PhasePubSub          	 1371726	       135.8 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub          	 1099748	       176.7 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub          	 1153920	       176.5 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub          	 1108155	       194.0 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub          	  848036	       181.9 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub-8        	  583650	       328.1 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub-8        	  640056	       293.7 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub-8        	  630721	       364.8 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub-8        	 1398138	       132.6 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/PhasePubSub-8        	 1000000	       168.4 ns/op	     304 B/op	       3 allocs/op
BenchmarkConstruct/RelayPubSub          	 1199971	       151.1 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub          	 1134994	       135.0 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub          	 1154571	       172.4 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub          	 1243958	       142.3 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub          	 1287193	       153.6 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub-8        	  917100	       198.8 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub-8        	 1233685	       149.6 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub-8        	 1246762	       160.2 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub-8        	 1130212	       155.1 ns/op	     416 B/op	       4 allocs/op
BenchmarkConstruct/RelayPubSub-8        	 1160124	       147.6 ns/op	     416 B/op	       4 allocs/op
BenchmarkIdleChurn/ChanPubSub           	 7995044	        23.38 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub           	 7689076	        23.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub           	 6963219	        22.86 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub           	 7221376	        23.19 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub           	 7241377	        24.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub-8         	 6037563	        25.14 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub-8         	 6621508	        26.04 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub-8         	 6106898	        27.08 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub-8         	 8252419	        28.59 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/ChanPubSub-8         	 5543109	        31.75 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub          	13289118	        19.78 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub          	 6984386	        21.86 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub          	 7676010	        21.80 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub          	13363174	        20.92 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub          	13567951	        13.80 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub-8        	12410510	        18.26 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub-8        	13161285	        15.58 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub-8        	10498773	        14.92 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub-8        	10894104	        15.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/PhasePubSub-8        	11472889	        15.81 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub          	11178528	        15.94 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub          	12047485	        15.41 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub          	11430031	        15.71 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub          	12696708	        15.01 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub          	12255541	        16.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub-8        	10802265	        18.69 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub-8        	11731546	        15.88 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub-8        	11445189	        16.62 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub-8        	11649192	        16.85 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/RelayPubSub-8        	11606691	        17.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkIdleChurn/FreshChannel         	 4622433	        42.21 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel         	 5295014	        35.25 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel         	 4855764	        37.08 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel         	 5120421	        35.46 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel         	 4710208	        36.35 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel-8       	 4746241	        40.17 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel-8       	 4711126	        41.05 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel-8       	 4586949	        45.19 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel-8       	 4424343	        45.17 ns/op	     112 B/op	       1 allocs/op
BenchmarkIdleChurn/FreshChannel-8       	 4241329	        43.02 ns/op	     112 B/op	       1 allocs/op
PASS
ok  	example.com/bigbuff-evolution	17.493s
-- results/bench-broadcast.txt --
goos: linux
goarch: amd64
pkg: example.com/bigbuff-evolution
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/N1/ChanPubSub           	  368006	       501.4 ns/op	       501.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  392061	       456.5 ns/op	       456.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  409129	       415.1 ns/op	       415.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  481426	       475.7 ns/op	       475.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  442035	       478.2 ns/op	       478.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  436260	       488.7 ns/op	       488.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  377365	       448.5 ns/op	       448.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  392316	       434.2 ns/op	       434.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  381776	       451.0 ns/op	       451.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  399037	       435.0 ns/op	       435.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1000000	       185.4 ns/op	       185.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1000000	       154.4 ns/op	       154.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1000000	       158.8 ns/op	       158.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1000000	       172.4 ns/op	       172.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	  943364	       165.6 ns/op	       165.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	  924537	       191.1 ns/op	       191.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	 1000000	       196.0 ns/op	       196.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	 1000000	       198.0 ns/op	       198.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	 1000000	       214.1 ns/op	       214.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	  986170	       205.1 ns/op	       205.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	  850417	       245.6 ns/op	       245.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	  747376	       215.5 ns/op	       215.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       191.6 ns/op	       191.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	  810493	       189.0 ns/op	       189.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       193.4 ns/op	       193.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	  735066	       227.6 ns/op	       227.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	  946966	       208.8 ns/op	       208.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	 1000000	       205.2 ns/op	       205.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	  908044	       224.9 ns/op	       224.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	  788262	       203.8 ns/op	       203.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  499429	       342.1 ns/op	       342.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  512818	       369.0 ns/op	       369.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  576325	       352.6 ns/op	       352.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  539361	       383.3 ns/op	       383.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  468124	       353.9 ns/op	       353.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  455624	       442.7 ns/op	       442.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  469446	       422.8 ns/op	       422.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  498644	       413.4 ns/op	       413.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  489237	       403.2 ns/op	       403.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  532190	       440.9 ns/op	       440.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  457846	       401.0 ns/op	       401.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  507910	       408.0 ns/op	       408.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  464949	       344.8 ns/op	       344.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  448846	       345.6 ns/op	       345.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  586869	       371.3 ns/op	       371.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  423951	       427.8 ns/op	       427.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  397236	       411.2 ns/op	       411.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  505346	       396.6 ns/op	       396.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  476498	       428.7 ns/op	       428.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  412765	       410.8 ns/op	       410.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  459468	       346.2 ns/op	       346.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  546451	       355.8 ns/op	       355.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  450243	       343.6 ns/op	       343.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  542145	       343.7 ns/op	       343.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  513441	       400.5 ns/op	       400.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  366873	       458.0 ns/op	       458.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  393808	       416.4 ns/op	       416.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  421945	       416.5 ns/op	       416.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  499621	       417.3 ns/op	       417.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  422020	       421.0 ns/op	       421.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  512005	       379.1 ns/op	       379.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  559135	       365.0 ns/op	       365.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  473632	       375.1 ns/op	       375.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  413502	       386.5 ns/op	       386.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  484206	       394.9 ns/op	       394.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  393897	       421.3 ns/op	       421.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  385146	       439.6 ns/op	       439.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  348813	       455.0 ns/op	       455.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  426757	       421.4 ns/op	       421.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  391671	       420.6 ns/op	       420.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub           	   93571	      1880 ns/op	       235.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub           	  117511	      1913 ns/op	       239.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub           	   91287	      2145 ns/op	       268.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub           	  109380	      1834 ns/op	       229.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub           	   94312	      1788 ns/op	       223.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub-8         	   58339	      2979 ns/op	       372.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub-8         	   62440	      3179 ns/op	       397.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub-8         	   51825	      2924 ns/op	       365.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub-8         	   62422	      2936 ns/op	       367.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/ChanPubSub-8         	   66380	      2834 ns/op	       354.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub          	  133616	      1293 ns/op	       161.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub          	  132276	      1440 ns/op	       180.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub          	  134467	      1403 ns/op	       175.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub          	  126454	      1349 ns/op	       168.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub          	  121034	      1396 ns/op	       174.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub-8        	   73868	      2413 ns/op	       301.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub-8        	   70957	      2516 ns/op	       314.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub-8        	   68202	      2497 ns/op	       312.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub-8        	   73354	      2522 ns/op	       315.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/PhasePubSub-8        	   83542	      2426 ns/op	       303.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels       	  131637	      1689 ns/op	       211.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels       	  139226	      1468 ns/op	       183.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels       	  132553	      1750 ns/op	       218.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels       	  115430	      1645 ns/op	       205.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels       	  139761	      1407 ns/op	       175.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels-8     	   69763	      2445 ns/op	       305.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels-8     	   78303	      2524 ns/op	       315.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels-8     	   76875	      2477 ns/op	       309.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels-8     	   89334	      2510 ns/op	       313.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/LinearChannels-8     	   74782	      2438 ns/op	       304.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1              	  103327	      1659 ns/op	       207.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1              	   94926	      1696 ns/op	       212.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1              	  113355	      1639 ns/op	       204.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1              	  107046	      1722 ns/op	       215.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1              	  114226	      1718 ns/op	       214.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1-8            	   90442	      2152 ns/op	       269.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1-8            	  102325	      1884 ns/op	       235.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1-8            	   93157	      1814 ns/op	       226.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1-8            	   99958	      1765 ns/op	       220.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW1-8            	   93463	      1977 ns/op	       247.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4              	  102318	      1665 ns/op	       208.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4              	  120718	      1523 ns/op	       190.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4              	  108615	      1552 ns/op	       194.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4              	   95542	      1655 ns/op	       206.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4              	  107102	      1680 ns/op	       210.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4-8            	   60114	      2732 ns/op	       341.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4-8            	   74956	      2418 ns/op	       302.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4-8            	   73034	      2335 ns/op	       291.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4-8            	   70798	      2398 ns/op	       299.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW4-8            	  106380	      2172 ns/op	       271.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16             	  125324	      1477 ns/op	       184.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16             	  109966	      1697 ns/op	       212.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16             	  123498	      1614 ns/op	       201.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16             	  129030	      1519 ns/op	       189.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16             	  118471	      1632 ns/op	       204.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16-8           	   77856	      2457 ns/op	       307.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16-8           	   81097	      2364 ns/op	       295.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16-8           	   73222	      2288 ns/op	       286.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16-8           	   76550	      2287 ns/op	       285.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW16-8           	   87354	      2298 ns/op	       287.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64             	  118896	      1547 ns/op	       193.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64             	  125341	      1562 ns/op	       195.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64             	  120073	      1545 ns/op	       193.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64             	  127858	      1822 ns/op	       227.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64             	  110727	      1722 ns/op	       215.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64-8           	   78937	      2315 ns/op	       289.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64-8           	   72008	      2588 ns/op	       323.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64-8           	   76752	      2610 ns/op	       326.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64-8           	   70396	      2298 ns/op	       287.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N8/RelayW64-8           	   74698	      2383 ns/op	       297.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub          	   10000	     15076 ns/op	       235.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub          	   15102	     14944 ns/op	       233.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub          	   12243	     14987 ns/op	       234.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub          	   13212	     18344 ns/op	       286.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub          	   12004	     15119 ns/op	       236.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub-8        	    8098	     22323 ns/op	       348.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub-8        	    9105	     28867 ns/op	       451.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub-8        	    5716	     29139 ns/op	       455.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub-8        	    6280	     24985 ns/op	       390.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/ChanPubSub-8        	    6506	     27078 ns/op	       423.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub         	   12471	     14410 ns/op	       225.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub         	   13672	     12490 ns/op	       195.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub         	   10000	     15439 ns/op	       241.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub         	   14380	     16065 ns/op	       251.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub         	   10000	     16482 ns/op	       257.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub-8       	   10000	     21261 ns/op	       332.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub-8       	    8335	     19970 ns/op	       312.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub-8       	    8043	     22859 ns/op	       357.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub-8       	   10000	     23161 ns/op	       361.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/PhasePubSub-8       	   11455	     22539 ns/op	       352.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels      	   13760	     12823 ns/op	       200.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels      	   14691	     12577 ns/op	       196.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels      	   14336	     14254 ns/op	       222.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels      	   13990	     12498 ns/op	       195.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels      	   14145	     11792 ns/op	       184.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels-8    	   10000	     15597 ns/op	       243.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels-8    	    9625	     17533 ns/op	       274.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels-8    	   10000	     15882 ns/op	       248.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels-8    	   10000	     15741 ns/op	       246.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/LinearChannels-8    	   14431	     12892 ns/op	       201.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1             	   13612	     11265 ns/op	       176.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1             	   14385	     12679 ns/op	       198.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1             	   15760	     11229 ns/op	       175.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1             	   15225	     12290 ns/op	       192.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1             	   13726	     12909 ns/op	       201.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1-8           	   10000	     17486 ns/op	       273.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1-8           	   10000	     15009 ns/op	       234.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1-8           	   12735	     13930 ns/op	       217.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1-8           	   12067	     12799 ns/op	       200.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW1-8           	   13953	     12713 ns/op	       198.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4             	   14013	     13479 ns/op	       210.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4             	   14416	     12364 ns/op	       193.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4             	   14793	     12481 ns/op	       195.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4             	   16436	     13304 ns/op	       207.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4             	   13842	     12352 ns/op	       193.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4-8           	    9868	     16734 ns/op	       261.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4-8           	   10472	     18035 ns/op	       281.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4-8           	    9585	     19803 ns/op	       309.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4-8           	    9538	     17643 ns/op	       275.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW4-8           	   10000	     17801 ns/op	       278.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16            	   16071	     12786 ns/op	       199.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16            	   14349	     12923 ns/op	       201.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16            	   13742	     13966 ns/op	       218.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16            	   15348	     16103 ns/op	       251.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16            	    7816	     19490 ns/op	       304.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16-8          	    5702	     28405 ns/op	       443.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16-8          	   10000	     22203 ns/op	       346.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16-8          	    7268	     22585 ns/op	       352.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16-8          	   10000	     23471 ns/op	       366.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW16-8          	    6979	     23092 ns/op	       360.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64            	   14714	     11328 ns/op	       177.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64            	   16370	     11642 ns/op	       181.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64            	   16369	     11213 ns/op	       175.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64            	   15805	     11223 ns/op	       175.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64            	   16167	     11216 ns/op	       175.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64-8          	    9386	     16804 ns/op	       262.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64-8          	   10000	     17283 ns/op	       270.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64-8          	    9970	     17452 ns/op	       272.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64-8          	   10000	     19961 ns/op	       311.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N64/RelayW64-8          	    9825	     18597 ns/op	       290.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub         	    1464	    118180 ns/op	       230.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub         	    1725	    116407 ns/op	       227.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub         	    1845	    121179 ns/op	       236.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub         	    1591	    106547 ns/op	       208.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub         	    1575	    100860 ns/op	       197.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub-8       	     726	    262041 ns/op	       511.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub-8       	     618	    243659 ns/op	       475.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub-8       	     614	    277496 ns/op	       542.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub-8       	     907	    239216 ns/op	       467.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/ChanPubSub-8       	     772	    269499 ns/op	       526.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub        	    1902	     95999 ns/op	       187.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub        	    1951	     94019 ns/op	       183.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub        	    2139	    106734 ns/op	       208.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub        	    2036	     95264 ns/op	       186.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub        	    2190	     94741 ns/op	       185.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub-8      	    1300	    140667 ns/op	       274.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub-8      	    1180	    152040 ns/op	       297.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub-8      	    1273	    162993 ns/op	       318.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub-8      	    1179	    151387 ns/op	       295.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/PhasePubSub-8      	    1254	    156250 ns/op	       305.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels     	    1609	    136814 ns/op	       267.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels     	    1868	     98263 ns/op	       191.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels     	    2011	    100153 ns/op	       195.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels     	    2010	     94350 ns/op	       184.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels     	    1902	    103736 ns/op	       202.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels-8   	    2232	     80590 ns/op	       157.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels-8   	    2229	     95280 ns/op	       186.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels-8   	    1897	     99715 ns/op	       194.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels-8   	    2150	     81006 ns/op	       158.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/LinearChannels-8   	    2402	     89676 ns/op	       175.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1            	    1930	     90018 ns/op	       175.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1            	    2119	     88902 ns/op	       173.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1            	    2173	     87556 ns/op	       171.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1            	    1975	     96179 ns/op	       187.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1            	    2198	    106569 ns/op	       208.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1-8          	    1836	    129806 ns/op	       253.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1-8          	    1683	    117186 ns/op	       228.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1-8          	    1581	    104597 ns/op	       204.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1-8          	    1657	    119487 ns/op	       233.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW1-8          	    1848	    115635 ns/op	       225.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4            	    1570	    102424 ns/op	       200.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4            	    2068	     98991 ns/op	       193.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4            	    1458	    112969 ns/op	       220.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4            	    1972	    119992 ns/op	       234.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4            	    1491	    105454 ns/op	       206.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4-8          	    1285	    136639 ns/op	       266.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4-8          	    1172	    155704 ns/op	       304.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4-8          	    1408	    138473 ns/op	       270.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4-8          	    1330	    183403 ns/op	       358.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW4-8          	    1236	    129535 ns/op	       253.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16           	    2001	    104049 ns/op	       203.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16           	    1916	     97290 ns/op	       190.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16           	    1696	     90138 ns/op	       176.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16           	    1986	     89263 ns/op	       174.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16           	    1850	     96187 ns/op	       187.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16-8         	    1074	    165647 ns/op	       323.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16-8         	    1047	    169744 ns/op	       331.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16-8         	    1118	    177338 ns/op	       346.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16-8         	    1026	    158927 ns/op	       310.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW16-8         	    1000	    154625 ns/op	       302.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64           	    1974	    134569 ns/op	       262.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64           	    1814	     97899 ns/op	       191.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64           	    1746	    119609 ns/op	       233.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64           	    1491	    107795 ns/op	       210.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64           	    1579	    109798 ns/op	       214.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64-8         	     886	    231655 ns/op	       452.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64-8         	     906	    207292 ns/op	       404.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64-8         	     642	    247616 ns/op	       483.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64-8         	     799	    233664 ns/op	       456.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N512/RelayW64-8         	    1353	    144479 ns/op	       282.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub        	     140	   1355755 ns/op	       331.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub        	     136	   1309029 ns/op	       319.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub        	     153	   1272624 ns/op	       310.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub        	     130	   1343569 ns/op	       328.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub        	     129	   1763203 ns/op	       430.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub-8      	      51	   3049096 ns/op	       744.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub-8      	      63	   3076048 ns/op	       751.0 ns/delivery	      30 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub-8      	      67	   3331593 ns/op	       813.4 ns/delivery	     112 B/op	       1 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub-8      	     100	   3611817 ns/op	       881.8 ns/delivery	     155 B/op	       1 allocs/op
BenchmarkBroadcast/N4096/ChanPubSub-8      	      56	   2794811 ns/op	       682.3 ns/delivery	       2 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub       	     183	    879664 ns/op	       214.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub       	     220	   1000363 ns/op	       244.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub       	     169	   1132512 ns/op	       276.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub       	     222	    886439 ns/op	       216.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub       	     225	    840631 ns/op	       205.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub-8     	     177	   1005511 ns/op	       245.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub-8     	     175	   1006128 ns/op	       245.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub-8     	     172	    996207 ns/op	       243.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub-8     	     176	   1130273 ns/op	       275.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/PhasePubSub-8     	     154	   1011661 ns/op	       247.0 ns/delivery	       2 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels    	     184	   1227229 ns/op	       299.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels    	     100	   1525469 ns/op	       372.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels    	     189	    860153 ns/op	       210.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels    	     189	    980349 ns/op	       239.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels    	     174	   1208368 ns/op	       295.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels-8  	     343	    584962 ns/op	       142.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels-8  	     351	    589950 ns/op	       144.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels-8  	     344	    501982 ns/op	       122.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels-8  	     295	    556142 ns/op	       135.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/LinearChannels-8  	     295	    559556 ns/op	       136.6 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1           	     199	   1230828 ns/op	       300.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1           	     168	   1007043 ns/op	       245.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1           	     162	   1042693 ns/op	       254.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1           	     195	   1174925 ns/op	       286.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1           	     192	   1327432 ns/op	       324.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1-8         	     162	   1120944 ns/op	       273.7 ns/delivery	       2 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1-8         	     142	   1169435 ns/op	       285.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1-8         	     141	   1099823 ns/op	       268.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1-8         	     145	   1085525 ns/op	       265.0 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW1-8         	     178	   1594540 ns/op	       389.3 ns/delivery	       2 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4           	     201	    915128 ns/op	       223.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4           	     162	   1063162 ns/op	       259.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4           	     189	   1013198 ns/op	       247.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4           	     201	   1027802 ns/op	       250.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4           	     196	    977385 ns/op	       238.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4-8         	     162	   1405831 ns/op	       343.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4-8         	     134	   1553105 ns/op	       379.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4-8         	     160	   1247954 ns/op	       304.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4-8         	     170	   1310068 ns/op	       319.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW4-8         	     171	   1155378 ns/op	       282.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16          	     175	   1130841 ns/op	       276.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16          	     190	    930964 ns/op	       227.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16          	     100	   1888903 ns/op	       461.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16          	     144	   1211891 ns/op	       295.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16          	     175	   1561827 ns/op	       381.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16-8        	      92	   1930490 ns/op	       471.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16-8        	     100	   2118273 ns/op	       517.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16-8        	     100	   1544160 ns/op	       377.0 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16-8        	     140	   1085062 ns/op	       264.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW16-8        	     172	   1107573 ns/op	       270.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64          	     211	    866165 ns/op	       211.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64          	     196	    827937 ns/op	       202.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64          	     204	    905328 ns/op	       221.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64          	     210	   1013909 ns/op	       247.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64          	     134	   1355735 ns/op	       331.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64-8        	     129	   1603973 ns/op	       391.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64-8        	     100	   1538330 ns/op	       375.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64-8        	      75	   2146039 ns/op	       523.9 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64-8        	     138	   1217329 ns/op	       297.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N4096/RelayW64-8        	     158	   1318471 ns/op	       321.9 ns/delivery	       0 B/op	       0 allocs/op
PASS
ok  	example.com/bigbuff-evolution	95.958s
-- results/bench-n1-clean.txt --
goos: linux
goarch: amd64
pkg: example.com/bigbuff-evolution
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/N1/ChanPubSub           	  424556	       398.3 ns/op	       398.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  452090	       415.2 ns/op	       415.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  486645	       435.1 ns/op	       435.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  435828	       441.1 ns/op	       441.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub           	  414334	       447.3 ns/op	       447.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  386568	       455.9 ns/op	       455.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  385388	       503.6 ns/op	       503.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  351482	       500.3 ns/op	       500.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  393657	       502.5 ns/op	       502.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/ChanPubSub-8         	  348964	       461.3 ns/op	       461.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	  838161	       224.8 ns/op	       224.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1103992	       168.9 ns/op	       168.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1000000	       169.0 ns/op	       169.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	  792492	       207.0 ns/op	       207.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub          	 1000000	       190.2 ns/op	       190.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	  892338	       202.0 ns/op	       202.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	 1000000	       202.1 ns/op	       202.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	  759860	       260.0 ns/op	       260.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	  806661	       193.9 ns/op	       193.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/PhasePubSub-8        	 1000000	       205.1 ns/op	       205.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       182.8 ns/op	       182.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       165.1 ns/op	       165.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       152.2 ns/op	       152.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       150.0 ns/op	       150.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels       	 1000000	       155.7 ns/op	       155.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	 1000000	       184.4 ns/op	       184.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	 1000000	       176.7 ns/op	       176.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	 1000000	       184.4 ns/op	       184.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	 1027591	       171.0 ns/op	       171.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/LinearChannels-8     	 1096944	       173.4 ns/op	       173.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  551844	       327.5 ns/op	       327.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  523908	       305.7 ns/op	       305.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  610274	       312.6 ns/op	       312.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  598215	       340.5 ns/op	       340.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1              	  587112	       356.6 ns/op	       356.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  622558	       401.0 ns/op	       401.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  557781	       388.4 ns/op	       388.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  599113	       371.6 ns/op	       371.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  475017	       389.7 ns/op	       389.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW1-8            	  468704	       337.4 ns/op	       337.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  515888	       318.3 ns/op	       318.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  626070	       300.1 ns/op	       300.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  651850	       300.5 ns/op	       300.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  588757	       307.6 ns/op	       307.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4              	  599541	       325.3 ns/op	       325.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  584748	       332.1 ns/op	       332.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  507226	       343.3 ns/op	       343.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  590842	       347.8 ns/op	       347.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  485800	       359.7 ns/op	       359.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW4-8            	  600618	       359.8 ns/op	       359.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  571063	       407.8 ns/op	       407.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  587862	       378.4 ns/op	       378.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  595789	       339.2 ns/op	       339.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  490454	       345.0 ns/op	       345.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16             	  498890	       334.0 ns/op	       334.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  546948	       362.8 ns/op	       362.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  618447	       359.9 ns/op	       359.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  492735	       403.3 ns/op	       403.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  471033	       464.9 ns/op	       464.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW16-8           	  535694	       419.1 ns/op	       419.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  445186	       407.2 ns/op	       407.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  486211	       326.1 ns/op	       326.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  620176	       384.3 ns/op	       384.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  550152	       337.3 ns/op	       337.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64             	  618507	       363.8 ns/op	       363.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  466384	       386.0 ns/op	       386.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  540290	       538.3 ns/op	       538.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  356641	       475.4 ns/op	       475.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  350480	       486.1 ns/op	       486.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkBroadcast/N1/RelayW64-8           	  412437	       467.3 ns/op	       467.3 ns/delivery	       0 B/op	       0 allocs/op
PASS
ok  	example.com/bigbuff-evolution	15.105s
-- results/bench-select-payload.txt --
goos: linux
goarch: amd64
pkg: example.com/bigbuff-evolution
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkSelect/N8/ChanPubSub           	   45333	      2757 ns/op	       344.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub           	   43705	      2958 ns/op	       369.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub           	   41128	      2657 ns/op	       332.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub           	   42326	      2713 ns/op	       339.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub           	   45507	      2807 ns/op	       350.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub-8         	   25568	      5417 ns/op	       677.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub-8         	   23713	      4931 ns/op	       616.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub-8         	   26060	      5055 ns/op	       631.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub-8         	   25030	      4544 ns/op	       567.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/ChanPubSub-8         	   22958	      4926 ns/op	       615.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub          	   69226	      1606 ns/op	       200.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub          	   75367	      1955 ns/op	       244.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub          	   74972	      1607 ns/op	       200.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub          	   73864	      2349 ns/op	       293.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub          	   46468	      2565 ns/op	       320.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub-8        	   33612	      3581 ns/op	       447.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub-8        	   37346	      3277 ns/op	       409.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub-8        	   27856	      4466 ns/op	       558.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub-8        	   31155	      3969 ns/op	       496.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/PhasePubSub-8        	   28179	      4722 ns/op	       590.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels       	   43435	      2534 ns/op	       316.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels       	   35362	      3194 ns/op	       399.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels       	   40952	      3436 ns/op	       429.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels       	   34606	      3795 ns/op	       474.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels       	   45427	      2396 ns/op	       299.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels-8     	   26200	      5385 ns/op	       673.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels-8     	   33626	      3205 ns/op	       400.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels-8     	   26508	      4641 ns/op	       580.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels-8     	   32547	      3196 ns/op	       399.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/LinearChannels-8     	   35990	      3943 ns/op	       492.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1              	   47522	      2556 ns/op	       319.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1              	   53518	      2458 ns/op	       307.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1              	   54830	      2592 ns/op	       324.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1              	   50823	      2733 ns/op	       341.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1              	   50055	      2811 ns/op	       351.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1-8            	   32216	      4063 ns/op	       507.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1-8            	   26155	      4404 ns/op	       550.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1-8            	   26305	      4631 ns/op	       578.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1-8            	   34597	      4290 ns/op	       536.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW1-8            	   33337	      3236 ns/op	       404.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4              	   33337	      3252 ns/op	       406.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4              	   39771	      3188 ns/op	       398.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4              	   45219	      2617 ns/op	       327.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4              	   43274	      2842 ns/op	       355.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4              	   48675	      2678 ns/op	       334.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4-8            	   27825	      5276 ns/op	       659.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4-8            	   21952	      4838 ns/op	       604.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4-8            	   26446	      4959 ns/op	       619.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4-8            	   22893	      5177 ns/op	       647.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW4-8            	   25160	      4503 ns/op	       562.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16             	   43918	      2356 ns/op	       294.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16             	   43315	      2877 ns/op	       359.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16             	   49050	      2486 ns/op	       310.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16             	   45496	      2925 ns/op	       365.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16             	   43072	      2955 ns/op	       369.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16-8           	   31843	      4373 ns/op	       546.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16-8           	   24577	      4071 ns/op	       508.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16-8           	   39547	      3796 ns/op	       474.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16-8           	   26180	      4687 ns/op	       585.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW16-8           	   26820	      4628 ns/op	       578.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64             	   37960	      2942 ns/op	       367.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64             	   49196	      2948 ns/op	       368.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64             	   44875	      2598 ns/op	       324.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64             	   44583	      2718 ns/op	       339.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64             	   48866	      2606 ns/op	       325.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64-8           	   22696	      5605 ns/op	       700.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64-8           	   29763	      5208 ns/op	       651.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64-8           	   32719	      4230 ns/op	       528.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64-8           	   28315	      4459 ns/op	       557.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N8/RelayW64-8           	   23715	      4377 ns/op	       547.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub          	    7504	     19918 ns/op	       311.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub          	    7768	     17966 ns/op	       280.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub          	    6338	     19402 ns/op	       303.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub          	    6945	     20045 ns/op	       313.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub          	    6607	     19167 ns/op	       299.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8        	    4093	     29129 ns/op	       455.1 ns/delivery	       5 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8        	    3985	     26632 ns/op	       416.1 ns/delivery	       3 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8        	    2574	     57146 ns/op	       892.9 ns/delivery	       9 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8        	    1818	     56068 ns/op	       876.1 ns/delivery	      10 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8        	    4010	     28742 ns/op	       449.1 ns/delivery	       4 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub         	    8349	     17171 ns/op	       268.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub         	    8696	     15034 ns/op	       234.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub         	    6928	     14931 ns/op	       233.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub         	    8059	     16254 ns/op	       254.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub         	    8847	     15471 ns/op	       241.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8       	    2204	     57928 ns/op	       905.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8       	    2372	     64021 ns/op	      1000 ns/delivery	       2 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8       	    2131	     67194 ns/op	      1050 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8       	    6067	     52398 ns/op	       818.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8       	    2402	     51035 ns/op	       797.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels      	    3553	     29526 ns/op	       461.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels      	    6999	     17434 ns/op	       272.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels      	    8431	     14639 ns/op	       228.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels      	    9129	     14779 ns/op	       230.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels      	    8806	     16405 ns/op	       256.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8    	    5288	     23481 ns/op	       366.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8    	    4843	     28988 ns/op	       452.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8    	    4504	     37129 ns/op	       580.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8    	    3948	     34055 ns/op	       532.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8    	    3418	     36537 ns/op	       570.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1             	    6910	     19961 ns/op	       311.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1             	    7706	     21040 ns/op	       328.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1             	    4830	     24942 ns/op	       389.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1             	    7572	     14619 ns/op	       228.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1             	    8230	     14418 ns/op	       225.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8           	    4833	     21218 ns/op	       331.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8           	    6975	     20583 ns/op	       321.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8           	    8553	     19786 ns/op	       309.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8           	    6202	     17587 ns/op	       274.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8           	    8340	     17565 ns/op	       274.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4             	    6216	     16711 ns/op	       261.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4             	    8203	     18032 ns/op	       281.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4             	    7632	     17005 ns/op	       265.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4             	    7826	     16166 ns/op	       252.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4             	    7548	     16069 ns/op	       251.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8           	    3592	     29623 ns/op	       462.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8           	    3728	     29701 ns/op	       464.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8           	    4309	     32601 ns/op	       509.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8           	    3078	     36211 ns/op	       565.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8           	    3477	     31971 ns/op	       499.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16            	    7722	     17477 ns/op	       273.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16            	    8368	     15900 ns/op	       248.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16            	    7645	     18701 ns/op	       292.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16            	    6548	     18984 ns/op	       296.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16            	    7778	     17136 ns/op	       267.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16-8          	    2869	     42630 ns/op	       666.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16-8          	    2774	     40968 ns/op	       640.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16-8          	    3091	     43355 ns/op	       677.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16-8          	    2859	     46278 ns/op	       723.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW16-8          	    2853	     45398 ns/op	       709.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64            	    6640	     16696 ns/op	       260.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64            	    7000	     15573 ns/op	       243.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64            	    8606	     15776 ns/op	       246.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64            	    8367	     16128 ns/op	       252.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64            	    8230	     15935 ns/op	       249.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64-8          	    2667	     40234 ns/op	       628.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64-8          	    2161	     47982 ns/op	       749.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64-8          	    2718	     50642 ns/op	       791.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64-8          	    2774	     42300 ns/op	       660.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW64-8          	    2922	     45344 ns/op	       708.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub         	     825	    145580 ns/op	       284.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub         	     850	    150089 ns/op	       293.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub         	     799	    144954 ns/op	       283.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub         	     745	    141791 ns/op	       276.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub         	     709	    142422 ns/op	       278.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub-8       	     127	    937136 ns/op	      1830 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub-8       	     135	    804679 ns/op	      1572 ns/delivery	      87 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub-8       	     120	   1017423 ns/op	      1987 ns/delivery	      97 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub-8       	     135	    966596 ns/op	      1888 ns/delivery	       6 B/op	       0 allocs/op
BenchmarkSelect/N512/ChanPubSub-8       	     139	    908394 ns/op	      1774 ns/delivery	      30 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub        	     883	    148852 ns/op	       290.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub        	     812	    142677 ns/op	       278.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub        	     945	    131141 ns/op	       256.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub        	     942	    136788 ns/op	       267.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub        	     900	    128959 ns/op	       251.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub-8      	     217	    636760 ns/op	      1244 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub-8      	     231	    577491 ns/op	      1128 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub-8      	     220	    622804 ns/op	      1216 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub-8      	     198	    517097 ns/op	      1010 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/PhasePubSub-8      	     236	    485240 ns/op	       947.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels     	     927	    136231 ns/op	       266.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels     	     988	    134342 ns/op	       262.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels     	     960	    128386 ns/op	       250.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels     	     984	    125879 ns/op	       245.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels     	     921	    123722 ns/op	       241.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels-8   	     494	    255192 ns/op	       498.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels-8   	     480	    264117 ns/op	       515.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels-8   	     499	    246807 ns/op	       482.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels-8   	     486	    255898 ns/op	       499.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/LinearChannels-8   	     472	    241286 ns/op	       471.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1            	     982	    130940 ns/op	       255.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1            	     969	    132560 ns/op	       258.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1            	     957	    128981 ns/op	       251.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1            	     931	    129588 ns/op	       253.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1            	     960	    134736 ns/op	       263.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1-8          	     787	    144345 ns/op	       281.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1-8          	     808	    163277 ns/op	       318.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1-8          	     850	    153219 ns/op	       299.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1-8          	     946	    161023 ns/op	       314.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW1-8          	     730	    168287 ns/op	       328.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4            	     777	    155912 ns/op	       304.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4            	     786	    143283 ns/op	       279.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4            	     792	    141979 ns/op	       277.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4            	     933	    203887 ns/op	       398.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4            	     829	    162695 ns/op	       317.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4-8          	     188	    623773 ns/op	      1218 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4-8          	     243	    546077 ns/op	      1067 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4-8          	     152	    676974 ns/op	      1322 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4-8          	     212	    676815 ns/op	      1322 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW4-8          	     184	    577704 ns/op	      1128 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16           	     928	    149953 ns/op	       292.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16           	     900	    137691 ns/op	       268.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16           	     936	    137652 ns/op	       268.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16           	     871	    135328 ns/op	       264.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16           	     940	    134731 ns/op	       263.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16-8         	     154	    803739 ns/op	      1570 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16-8         	     164	    674056 ns/op	      1317 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16-8         	     152	    761767 ns/op	      1488 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16-8         	     194	    541106 ns/op	      1057 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW16-8         	     151	   1089796 ns/op	      2129 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64           	     846	    148690 ns/op	       290.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64           	     808	    147630 ns/op	       288.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64           	     871	    157946 ns/op	       308.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64           	     832	    150284 ns/op	       293.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64           	     882	    141281 ns/op	       275.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64-8         	     277	    634888 ns/op	      1240 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64-8         	     244	    800456 ns/op	      1563 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64-8         	     175	    677942 ns/op	      1324 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64-8         	     308	    520136 ns/op	      1016 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N512/RelayW64-8         	     210	    597576 ns/op	      1167 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub      	   39868	      2794 ns/op	       349.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub      	   42716	      2760 ns/op	       345.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub      	   44106	      3026 ns/op	       378.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub      	   41287	      2764 ns/op	       345.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub      	   44618	      2797 ns/op	       349.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub-8    	   22874	      5261 ns/op	       657.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub-8    	   22010	      5285 ns/op	       660.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub-8    	   29967	      5395 ns/op	       674.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub-8    	   27094	      5321 ns/op	       665.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/ChanPubSub-8    	   23811	      5275 ns/op	       659.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub     	   54312	      2269 ns/op	       283.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub     	   53773	      2243 ns/op	       280.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub     	   57968	      2101 ns/op	       262.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub     	   54676	      1977 ns/op	       247.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub     	   61359	      2000 ns/op	       250.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub-8   	   36918	      3219 ns/op	       402.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub-8   	   37150	      3385 ns/op	       423.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub-8   	   35034	      3993 ns/op	       499.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub-8   	   35262	      3707 ns/op	       463.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/PhasePubSub-8   	   38013	      3676 ns/op	       459.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels           	   56815	      2291 ns/op	       286.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels           	   58269	      2048 ns/op	       256.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels           	   56058	      2066 ns/op	       258.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels           	   59449	      1975 ns/op	       246.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels           	   58968	      2170 ns/op	       271.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels-8         	   34432	      3672 ns/op	       458.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels-8         	   30753	      3738 ns/op	       467.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels-8         	   31630	      3489 ns/op	       436.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels-8         	   33273	      3599 ns/op	       449.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/LinearChannels-8         	   31216	      3843 ns/op	       480.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1                  	   45920	      2436 ns/op	       304.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1                  	   48804	      2383 ns/op	       297.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1                  	   54160	      2185 ns/op	       273.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1                  	   56683	      2158 ns/op	       269.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1                  	   53660	      2133 ns/op	       266.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1-8                	   50425	      2490 ns/op	       311.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1-8                	   45301	      2220 ns/op	       277.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1-8                	   48670	      2688 ns/op	       336.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1-8                	   48451	      2590 ns/op	       323.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW1-8                	   53338	      2323 ns/op	       290.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4                  	   52057	      2219 ns/op	       277.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4                  	   54904	      2231 ns/op	       278.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4                  	   54464	      2231 ns/op	       278.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4                  	   53506	      2162 ns/op	       270.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4                  	   54846	      2365 ns/op	       295.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4-8                	   32341	      3858 ns/op	       482.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4-8                	   57549	      3897 ns/op	       487.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4-8                	   34242	      3991 ns/op	       498.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4-8                	   26182	      4216 ns/op	       527.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW4-8                	   24603	      4676 ns/op	       584.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16                 	   56202	      2306 ns/op	       288.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16                 	   45120	      2657 ns/op	       332.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16                 	   44180	      2388 ns/op	       298.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16                 	   54490	      2665 ns/op	       333.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16                 	   49969	      2273 ns/op	       284.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16-8               	   24541	      4494 ns/op	       561.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16-8               	   32995	      4634 ns/op	       579.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16-8               	   31116	      4908 ns/op	       613.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16-8               	   37854	      4604 ns/op	       575.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW16-8               	   25437	      4304 ns/op	       538.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64                 	   47918	      2642 ns/op	       330.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64                 	   50859	      2804 ns/op	       350.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64                 	   56312	      2076 ns/op	       259.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64                 	   51074	      2276 ns/op	       284.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64                 	   50961	      2244 ns/op	       280.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64-8               	   33620	      3604 ns/op	       450.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64-8               	   32672	      3994 ns/op	       499.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64-8               	   26947	      3871 ns/op	       483.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64-8               	   30255	      3636 ns/op	       454.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N8/RelayW64-8               	   30663	      4426 ns/op	       553.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub              	    6148	     19022 ns/op	       297.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub              	    6200	     26305 ns/op	       411.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub              	    4309	     25915 ns/op	       404.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub              	    7010	     17374 ns/op	       271.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub              	    7035	     19758 ns/op	       308.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub-8            	    5191	     30588 ns/op	       477.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub-8            	    3531	     32314 ns/op	       504.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub-8            	    4077	     30313 ns/op	       473.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub-8            	    3456	     31883 ns/op	       498.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/ChanPubSub-8            	    3600	     32704 ns/op	       511.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub             	    7444	     22205 ns/op	       347.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub             	    8612	     14245 ns/op	       222.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub             	    7552	     15150 ns/op	       236.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub             	    8625	     15694 ns/op	       245.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub             	    7822	     14994 ns/op	       234.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub-8           	    5944	     24984 ns/op	       390.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub-8           	    5301	     22777 ns/op	       355.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub-8           	    4934	     27514 ns/op	       429.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub-8           	    4618	     21706 ns/op	       339.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/PhasePubSub-8           	    5060	     24058 ns/op	       375.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels          	    7605	     18460 ns/op	       288.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels          	    6615	     17403 ns/op	       271.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels          	    7156	     16820 ns/op	       262.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels          	    7353	     15864 ns/op	       247.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels          	    8180	     18810 ns/op	       293.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels-8        	    6114	     19536 ns/op	       305.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels-8        	    6027	     19583 ns/op	       306.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels-8        	    6296	     18316 ns/op	       286.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels-8        	    8353	     21164 ns/op	       330.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/LinearChannels-8        	    5871	     19443 ns/op	       303.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1                 	    6748	     16554 ns/op	       258.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1                 	    6272	     16955 ns/op	       264.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1                 	    8401	     16386 ns/op	       256.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1                 	    7544	     16990 ns/op	       265.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1                 	    6837	     15819 ns/op	       247.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1-8               	    8468	     19306 ns/op	       301.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1-8               	    8331	     17762 ns/op	       277.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1-8               	    5722	     17662 ns/op	       276.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1-8               	    6259	     20578 ns/op	       321.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW1-8               	    5524	     19668 ns/op	       307.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4                 	    6870	     17221 ns/op	       269.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4                 	    7162	     16197 ns/op	       253.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4                 	    6907	     16292 ns/op	       254.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4                 	    7334	     19409 ns/op	       303.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4                 	    6853	     17664 ns/op	       276.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4-8               	    4848	     28115 ns/op	       439.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4-8               	    5923	     24710 ns/op	       386.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4-8               	    6200	     24042 ns/op	       375.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4-8               	    5370	     23375 ns/op	       365.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW4-8               	    5037	     21939 ns/op	       342.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16                	    6226	     18715 ns/op	       292.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16                	    7874	     16225 ns/op	       253.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16                	    7538	     17020 ns/op	       265.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16                	    8136	     16634 ns/op	       259.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16                	    6582	     16070 ns/op	       251.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16-8              	    6018	     19145 ns/op	       299.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16-8              	    5814	     23659 ns/op	       369.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16-8              	    4548	     37515 ns/op	       586.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16-8              	    4580	     24636 ns/op	       384.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW16-8              	    4854	     24024 ns/op	       375.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64                	    6766	     17172 ns/op	       268.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64                	    7826	     17652 ns/op	       275.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64                	    7393	     18455 ns/op	       288.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64                	    8253	     16505 ns/op	       257.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64                	    7101	     16414 ns/op	       256.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64-8              	    5623	     25021 ns/op	       390.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64-8              	    5516	     21003 ns/op	       328.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64-8              	    5751	     21634 ns/op	       338.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64-8              	    7194	     23889 ns/op	       373.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N64/RelayW64-8              	    5832	     26561 ns/op	       415.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub             	     531	    225810 ns/op	       441.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub             	     542	    199821 ns/op	       390.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub             	     645	    241690 ns/op	       472.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub             	     577	    190268 ns/op	       371.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub             	     625	    190726 ns/op	       372.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub-8           	     505	    299075 ns/op	       584.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub-8           	     372	    327689 ns/op	       640.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub-8           	     397	    293544 ns/op	       573.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub-8           	     380	    533788 ns/op	      1043 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/ChanPubSub-8           	     266	    486723 ns/op	       950.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub            	     792	    177582 ns/op	       346.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub            	     583	    205590 ns/op	       401.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub            	     600	    214191 ns/op	       418.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub            	     704	    183298 ns/op	       358.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub            	     670	    162565 ns/op	       317.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub-8          	     508	    239966 ns/op	       468.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub-8          	     669	    170651 ns/op	       333.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub-8          	     780	    162547 ns/op	       317.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub-8          	     664	    152243 ns/op	       297.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/PhasePubSub-8          	     861	    166830 ns/op	       325.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels         	     819	    160326 ns/op	       313.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels         	     814	    147721 ns/op	       288.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels         	     820	    167425 ns/op	       327.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels         	     554	    190526 ns/op	       372.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels         	     790	    187228 ns/op	       365.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels-8       	     792	    128447 ns/op	       250.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels-8       	     936	    138285 ns/op	       270.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels-8       	     818	    148510 ns/op	       290.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels-8       	    1059	    152686 ns/op	       298.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/LinearChannels-8       	     957	    110333 ns/op	       215.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1                	     736	    168359 ns/op	       328.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1                	     742	    166247 ns/op	       324.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1                	     694	    160628 ns/op	       313.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1                	     748	    170178 ns/op	       332.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1                	     639	    169284 ns/op	       330.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1-8              	     721	    161518 ns/op	       315.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1-8              	     762	    168441 ns/op	       329.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1-8              	     637	    190038 ns/op	       371.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1-8              	     728	    157918 ns/op	       308.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW1-8              	     765	    164519 ns/op	       321.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4                	     739	    162758 ns/op	       317.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4                	     782	    162602 ns/op	       317.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4                	     832	    153505 ns/op	       299.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4                	     698	    162865 ns/op	       318.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4                	     831	    152303 ns/op	       297.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4-8              	     766	    181938 ns/op	       355.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4-8              	     748	    185710 ns/op	       362.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4-8              	     752	    180723 ns/op	       353.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4-8              	     762	    155385 ns/op	       303.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW4-8              	     788	    178329 ns/op	       348.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16               	     793	    198758 ns/op	       388.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16               	     772	    149831 ns/op	       292.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16               	     790	    168684 ns/op	       329.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16               	     625	    188064 ns/op	       367.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16               	     669	    169555 ns/op	       331.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16-8             	     684	    203937 ns/op	       398.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16-8             	     660	    194507 ns/op	       379.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16-8             	     688	    172398 ns/op	       336.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16-8             	     556	    203635 ns/op	       397.7 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW16-8             	     714	    197802 ns/op	       386.3 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64               	     793	    156904 ns/op	       306.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64               	     740	    155381 ns/op	       303.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64               	     848	    193231 ns/op	       377.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64               	     735	    169507 ns/op	       331.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64               	     810	    168064 ns/op	       328.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64-8             	     643	    200226 ns/op	       391.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64-8             	     681	    210834 ns/op	       411.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64-8             	     663	    227965 ns/op	       445.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64-8             	     484	    238776 ns/op	       466.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkPayload1KiB/N512/RelayW64-8             	     588	    258402 ns/op	       504.7 ns/delivery	       0 B/op	       0 allocs/op
PASS
ok  	example.com/bigbuff-evolution	74.252s
-- results/check-select64.txt --
goos: linux
goarch: amd64
pkg: example.com/bigbuff-evolution
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkSelect/N64/ChanPubSub-8         	   10000	     48094 ns/op	       751.5 ns/delivery	       3 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8         	    9214	     49645 ns/op	       775.7 ns/delivery	       2 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8         	   17378	     39670 ns/op	       619.8 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8         	   14205	     32509 ns/op	       507.9 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkSelect/N64/ChanPubSub-8         	   13285	     45912 ns/op	       717.4 ns/delivery	       1 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8        	    9118	     46976 ns/op	       734.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8        	   10000	     40327 ns/op	       630.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8        	   12496	     47922 ns/op	       748.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8        	    9252	     46847 ns/op	       732.0 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/PhasePubSub-8        	   10000	     50552 ns/op	       789.9 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8     	   15721	     31519 ns/op	       492.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8     	   13107	     32289 ns/op	       504.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8     	   16455	     30792 ns/op	       481.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8     	   14839	     32459 ns/op	       507.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/LinearChannels-8     	   14311	     29554 ns/op	       461.8 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8            	   27668	     17886 ns/op	       279.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8            	   28959	     16729 ns/op	       261.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8            	   28045	     17037 ns/op	       266.2 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8            	   26829	     18272 ns/op	       285.5 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW1-8            	   24950	     19302 ns/op	       301.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8            	    9913	     40540 ns/op	       633.4 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8            	   15558	     34665 ns/op	       541.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8            	   10000	     48615 ns/op	       759.6 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8            	   10000	     46086 ns/op	       720.1 ns/delivery	       0 B/op	       0 allocs/op
BenchmarkSelect/N64/RelayW4-8            	   10000	     61759 ns/op	       965.0 ns/delivery	       0 B/op	       0 allocs/op
PASS
ok  	example.com/bigbuff-evolution	16.828s
-- results/environment.txt --
$ go version
go version go1.27.3 linux/amd64

$ uname -srmo
Linux 6.18.57 x86_64 GNU/Linux

$ lscpu
Architecture:                            x86_64
CPU op-mode(s):                          32-bit, 64-bit
Address sizes:                           46 bits physical, 48 bits virtual
Byte Order:                              Little Endian
CPU(s):                                  9
On-line CPU(s) list:                     0-8
Vendor ID:                               AuthenticAMD
Model name:                              AMD EPYC 9V74 80-Core Processor
BIOS Model name:                           CPU @ 0.0GHz
BIOS CPU family:                         0
CPU family:                              25
Model:                                   17
Thread(s) per core:                      1
Core(s) per socket:                      9
Socket(s):                               1
Stepping:                                1
BogoMIPS:                                5192.26
Flags:                                   fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology nonstop_tsc cpuid extd_apicid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw topoext vmmcall fsgsbase tsc_adjust bmi1 avx2 smep bmi2 erms invpcid avx512f avx512dq rdseed adx smap avx512ifma clflushopt clwb avx512cd sha_ni avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves avx512_bf16 clzero xsaveerptr arat npt nrip_save tsc_scale vmcb_clean flushbyasid pausefilter pfthreshold v_vmsave_vmload avx512vbmi umip avx512_vbmi2 gfni vaes vpclmulqdq avx512_vnni avx512_bitalg avx512_vpopcntdq rdpid fsrm arch_capabilities
Hypervisor vendor:                       KVM
Virtualization type:                     full
L1d cache:                               160 KiB (5 instances)
L1i cache:                               160 KiB (5 instances)
L2 cache:                                5 MiB (5 instances)
L3 cache:                                32 MiB (1 instance)
NUMA node(s):                            1
NUMA node0 CPU(s):                       0-8
Vulnerability Gather data sampling:      Not affected
Vulnerability Ghostwrite:                Not affected
Vulnerability Indirect target selection: Not affected
Vulnerability Itlb multihit:             Not affected
Vulnerability L1tf:                      Not affected
Vulnerability Mds:                       Not affected
Vulnerability Meltdown:                  Not affected
Vulnerability Mmio stale data:           Not affected
Vulnerability Old microcode:             Not affected
Vulnerability Reg file data sampling:    Not affected
Vulnerability Retbleed:                  Not affected
Vulnerability Spec rstack overflow:      Vulnerable
Vulnerability Spec store bypass:         Vulnerable
Vulnerability Spectre v1:                Vulnerable: __user pointer sanitization and usercopy barriers only; no swapgs barriers
Vulnerability Spectre v2:                Vulnerable; STIBP: disabled; PBRSB-eIBRS: Not affected; BHI: Not affected
Vulnerability Srbds:                     Not affected
Vulnerability Tsa:                       Vulnerable
Vulnerability Tsx async abort:           Not affected
Vulnerability Vmscape:                   Not affected

cgroup cpu.max: 800000 100000

Measurements were run serially, after race tests completed.
Initial N1 fanout samples overlapped a brief compilation; clean N1 samples replace them in summary.json.
Fanout: -benchtime=150ms -count=5 -cpu=1,8
Select and 1KiB payload: -benchtime=100ms -count=5 -cpu=1,8
Idle churn and construction: -benchtime=150ms -count=5 -cpu=1,8
No performance runs used the race detector.
All steady-stream throughput runs include a final join after value copying and validation, before stopping the timer.
Registration, goroutine setup, 32 warmup publications, and teardown are outside the steady-stream timer.
-- results/portability.txt --
$ GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c -o /tmp/bigbuff-evolution-darwin-arm64.test .
exit_code=0

$ GOOS=linux GOARCH=386 CGO_ENABLED=0 go test -count=1 -timeout=90s .
fork/exec /tmp/go-build2801847981/b001/bigbuff-evolution.test: exec format error
FAIL	example.com/bigbuff-evolution	0.000s
FAIL
exit_code=1

The execution host cannot run linux/386 executables (exec format error above). Compilation-only check follows.
$ GOOS=linux GOARCH=386 CGO_ENABLED=0 go test -c -o /tmp/bigbuff-evolution-linux-386.test .
exit_code=0
-- results/summary.json --
[
  {
    "benchmark": "BenchmarkBroadcast/N1/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 435.1,
      "min": 398.3,
      "max": 447.3
    },
    "ns/op": {
      "median": 435.1,
      "min": 398.3,
      "max": 447.3
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 500.3,
      "min": 455.9,
      "max": 503.6
    },
    "ns/op": {
      "median": 500.3,
      "min": 455.9,
      "max": 503.6
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 155.7,
      "min": 150.0,
      "max": 182.8
    },
    "ns/op": {
      "median": 155.7,
      "min": 150.0,
      "max": 182.8
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 176.7,
      "min": 171.0,
      "max": 184.4
    },
    "ns/op": {
      "median": 176.7,
      "min": 171.0,
      "max": 184.4
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 190.2,
      "min": 168.9,
      "max": 224.8
    },
    "ns/op": {
      "median": 190.2,
      "min": 168.9,
      "max": 224.8
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 202.1,
      "min": 193.9,
      "max": 260.0
    },
    "ns/op": {
      "median": 202.1,
      "min": 193.9,
      "max": 260.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 327.5,
      "min": 305.7,
      "max": 356.6
    },
    "ns/op": {
      "median": 327.5,
      "min": 305.7,
      "max": 356.6
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 388.4,
      "min": 337.4,
      "max": 401.0
    },
    "ns/op": {
      "median": 388.4,
      "min": 337.4,
      "max": 401.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 345.0,
      "min": 334.0,
      "max": 407.8
    },
    "ns/op": {
      "median": 345.0,
      "min": 334.0,
      "max": 407.8
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 403.3,
      "min": 359.9,
      "max": 464.9
    },
    "ns/op": {
      "median": 403.3,
      "min": 359.9,
      "max": 464.9
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 307.6,
      "min": 300.1,
      "max": 325.3
    },
    "ns/op": {
      "median": 307.6,
      "min": 300.1,
      "max": 325.3
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 347.8,
      "min": 332.1,
      "max": 359.8
    },
    "ns/op": {
      "median": 347.8,
      "min": 332.1,
      "max": 359.8
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 363.8,
      "min": 326.1,
      "max": 407.2
    },
    "ns/op": {
      "median": 363.8,
      "min": 326.1,
      "max": 407.2
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N1/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 475.4,
      "min": 386.0,
      "max": 538.3
    },
    "ns/op": {
      "median": 475.4,
      "min": 386.0,
      "max": 538.3
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 328.0,
      "min": 310.7,
      "max": 430.5
    },
    "ns/op": {
      "median": 1343569.0,
      "min": 1272624.0,
      "max": 1763203.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 30.0,
      "min": 0.0,
      "max": 155.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 1.0
    },
    "ns/delivery": {
      "median": 751.0,
      "min": 682.3,
      "max": 881.8
    },
    "ns/op": {
      "median": 3076048.0,
      "min": 2794811.0,
      "max": 3611817.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 295.0,
      "min": 210.0,
      "max": 372.4
    },
    "ns/op": {
      "median": 1208368.0,
      "min": 860153.0,
      "max": 1525469.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 1.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 136.6,
      "min": 122.6,
      "max": 144.0
    },
    "ns/op": {
      "median": 559556.0,
      "min": 501982.0,
      "max": 589950.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 216.4,
      "min": 205.2,
      "max": 276.5
    },
    "ns/op": {
      "median": 886439.0,
      "min": 840631.0,
      "max": 1132512.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 2.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 245.6,
      "min": 243.2,
      "max": 275.9
    },
    "ns/op": {
      "median": 1006128.0,
      "min": 996207.0,
      "max": 1130273.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 286.8,
      "min": 245.9,
      "max": 324.1
    },
    "ns/op": {
      "median": 1174925.0,
      "min": 1007043.0,
      "max": 1327432.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 1.0,
      "min": 0.0,
      "max": 2.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 273.7,
      "min": 265.0,
      "max": 389.3
    },
    "ns/op": {
      "median": 1120944.0,
      "min": 1085525.0,
      "max": 1594540.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 295.9,
      "min": 227.3,
      "max": 461.2
    },
    "ns/op": {
      "median": 1211891.0,
      "min": 930964.0,
      "max": 1888903.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 1.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 377.0,
      "min": 264.9,
      "max": 517.2
    },
    "ns/op": {
      "median": 1544160.0,
      "min": 1085062.0,
      "max": 2118273.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 247.4,
      "min": 223.4,
      "max": 259.6
    },
    "ns/op": {
      "median": 1013198.0,
      "min": 915128.0,
      "max": 1063162.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 319.8,
      "min": 282.1,
      "max": 379.2
    },
    "ns/op": {
      "median": 1310068.0,
      "min": 1155378.0,
      "max": 1553105.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 221.0,
      "min": 202.1,
      "max": 331.0
    },
    "ns/op": {
      "median": 905328.0,
      "min": 827937.0,
      "max": 1355735.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N4096/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 1.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 375.6,
      "min": 297.2,
      "max": 523.9
    },
    "ns/op": {
      "median": 1538330.0,
      "min": 1217329.0,
      "max": 2146039.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 227.4,
      "min": 197.0,
      "max": 236.7
    },
    "ns/op": {
      "median": 116407.0,
      "min": 100860.0,
      "max": 121179.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 511.8,
      "min": 467.2,
      "max": 542.0
    },
    "ns/op": {
      "median": 262041.0,
      "min": 239216.0,
      "max": 277496.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 195.6,
      "min": 184.3,
      "max": 267.2
    },
    "ns/op": {
      "median": 100153.0,
      "min": 94350.0,
      "max": 136814.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 175.1,
      "min": 157.4,
      "max": 194.8
    },
    "ns/op": {
      "median": 89676.0,
      "min": 80590.0,
      "max": 99715.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 186.1,
      "min": 183.6,
      "max": 208.5
    },
    "ns/op": {
      "median": 95264.0,
      "min": 94019.0,
      "max": 106734.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 297.0,
      "min": 274.7,
      "max": 318.3
    },
    "ns/op": {
      "median": 152040.0,
      "min": 140667.0,
      "max": 162993.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 175.8,
      "min": 171.0,
      "max": 208.1
    },
    "ns/op": {
      "median": 90018.0,
      "min": 87556.0,
      "max": 106569.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 228.9,
      "min": 204.3,
      "max": 253.5
    },
    "ns/op": {
      "median": 117186.0,
      "min": 104597.0,
      "max": 129806.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 187.9,
      "min": 174.3,
      "max": 203.2
    },
    "ns/op": {
      "median": 96187.0,
      "min": 89263.0,
      "max": 104049.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 323.5,
      "min": 302.0,
      "max": 346.4
    },
    "ns/op": {
      "median": 165647.0,
      "min": 154625.0,
      "max": 177338.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 206.0,
      "min": 193.3,
      "max": 234.4
    },
    "ns/op": {
      "median": 105454.0,
      "min": 98991.0,
      "max": 119992.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 270.5,
      "min": 253.0,
      "max": 358.2
    },
    "ns/op": {
      "median": 138473.0,
      "min": 129535.0,
      "max": 183403.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 214.4,
      "min": 191.2,
      "max": 262.8
    },
    "ns/op": {
      "median": 109798.0,
      "min": 97899.0,
      "max": 134569.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N512/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 452.5,
      "min": 282.2,
      "max": 483.6
    },
    "ns/op": {
      "median": 231655.0,
      "min": 144479.0,
      "max": 247616.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 235.6,
      "min": 233.5,
      "max": 286.6
    },
    "ns/op": {
      "median": 15076.0,
      "min": 14944.0,
      "max": 18344.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 423.1,
      "min": 348.8,
      "max": 455.3
    },
    "ns/op": {
      "median": 27078.0,
      "min": 22323.0,
      "max": 29139.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 196.5,
      "min": 184.2,
      "max": 222.7
    },
    "ns/op": {
      "median": 12577.0,
      "min": 11792.0,
      "max": 14254.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 246.0,
      "min": 201.4,
      "max": 274.0
    },
    "ns/op": {
      "median": 15741.0,
      "min": 12892.0,
      "max": 17533.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 241.2,
      "min": 195.2,
      "max": 257.5
    },
    "ns/op": {
      "median": 15439.0,
      "min": 12490.0,
      "max": 16482.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 352.2,
      "min": 312.0,
      "max": 361.9
    },
    "ns/op": {
      "median": 22539.0,
      "min": 19970.0,
      "max": 23161.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 192.0,
      "min": 175.5,
      "max": 201.7
    },
    "ns/op": {
      "median": 12290.0,
      "min": 11229.0,
      "max": 12909.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 217.7,
      "min": 198.6,
      "max": 273.2
    },
    "ns/op": {
      "median": 13930.0,
      "min": 12713.0,
      "max": 17486.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 218.2,
      "min": 199.8,
      "max": 304.5
    },
    "ns/op": {
      "median": 13966.0,
      "min": 12786.0,
      "max": 19490.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 360.8,
      "min": 346.9,
      "max": 443.8
    },
    "ns/op": {
      "median": 23092.0,
      "min": 22203.0,
      "max": 28405.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 195.0,
      "min": 193.0,
      "max": 210.6
    },
    "ns/op": {
      "median": 12481.0,
      "min": 12352.0,
      "max": 13479.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 278.1,
      "min": 261.5,
      "max": 309.4
    },
    "ns/op": {
      "median": 17801.0,
      "min": 16734.0,
      "max": 19803.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 175.4,
      "min": 175.2,
      "max": 181.9
    },
    "ns/op": {
      "median": 11223.0,
      "min": 11213.0,
      "max": 11642.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N64/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 272.7,
      "min": 262.6,
      "max": 311.9
    },
    "ns/op": {
      "median": 17452.0,
      "min": 16804.0,
      "max": 19961.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 235.0,
      "min": 223.5,
      "max": 268.1
    },
    "ns/op": {
      "median": 1880.0,
      "min": 1788.0,
      "max": 2145.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 367.0,
      "min": 354.2,
      "max": 397.3
    },
    "ns/op": {
      "median": 2936.0,
      "min": 2834.0,
      "max": 3179.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 205.6,
      "min": 175.8,
      "max": 218.8
    },
    "ns/op": {
      "median": 1645.0,
      "min": 1407.0,
      "max": 1750.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 309.6,
      "min": 304.7,
      "max": 315.5
    },
    "ns/op": {
      "median": 2477.0,
      "min": 2438.0,
      "max": 2524.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 174.5,
      "min": 161.6,
      "max": 180.0
    },
    "ns/op": {
      "median": 1396.0,
      "min": 1293.0,
      "max": 1440.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 312.1,
      "min": 301.6,
      "max": 315.2
    },
    "ns/op": {
      "median": 2497.0,
      "min": 2413.0,
      "max": 2522.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 212.0,
      "min": 204.9,
      "max": 215.3
    },
    "ns/op": {
      "median": 1696.0,
      "min": 1639.0,
      "max": 1722.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 235.6,
      "min": 220.6,
      "max": 269.0
    },
    "ns/op": {
      "median": 1884.0,
      "min": 1765.0,
      "max": 2152.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 201.7,
      "min": 184.6,
      "max": 212.1
    },
    "ns/op": {
      "median": 1614.0,
      "min": 1477.0,
      "max": 1697.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 287.2,
      "min": 285.9,
      "max": 307.1
    },
    "ns/op": {
      "median": 2298.0,
      "min": 2287.0,
      "max": 2457.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 206.9,
      "min": 190.3,
      "max": 210.0
    },
    "ns/op": {
      "median": 1655.0,
      "min": 1523.0,
      "max": 1680.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 299.7,
      "min": 271.5,
      "max": 341.5
    },
    "ns/op": {
      "median": 2398.0,
      "min": 2172.0,
      "max": 2732.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 195.2,
      "min": 193.1,
      "max": 227.8
    },
    "ns/op": {
      "median": 1562.0,
      "min": 1545.0,
      "max": 1822.0
    }
  },
  {
    "benchmark": "BenchmarkBroadcast/N8/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 297.9,
      "min": 287.3,
      "max": 326.3
    },
    "ns/op": {
      "median": 2383.0,
      "min": 2298.0,
      "max": 2610.0
    }
  },
  {
    "benchmark": "BenchmarkConstruct/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 408.0,
      "min": 408.0,
      "max": 408.0
    },
    "allocs/op": {
      "median": 5.0,
      "min": 5.0,
      "max": 5.0
    },
    "ns/op": {
      "median": 124.0,
      "min": 123.2,
      "max": 135.8
    }
  },
  {
    "benchmark": "BenchmarkConstruct/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 408.0,
      "min": 408.0,
      "max": 408.0
    },
    "allocs/op": {
      "median": 5.0,
      "min": 5.0,
      "max": 5.0
    },
    "ns/op": {
      "median": 325.8,
      "min": 197.0,
      "max": 557.0
    }
  },
  {
    "benchmark": "BenchmarkConstruct/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 304.0,
      "min": 304.0,
      "max": 304.0
    },
    "allocs/op": {
      "median": 3.0,
      "min": 3.0,
      "max": 3.0
    },
    "ns/op": {
      "median": 176.7,
      "min": 135.8,
      "max": 194.0
    }
  },
  {
    "benchmark": "BenchmarkConstruct/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 304.0,
      "min": 304.0,
      "max": 304.0
    },
    "allocs/op": {
      "median": 3.0,
      "min": 3.0,
      "max": 3.0
    },
    "ns/op": {
      "median": 293.7,
      "min": 132.6,
      "max": 364.8
    }
  },
  {
    "benchmark": "BenchmarkConstruct/RelayPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 416.0,
      "min": 416.0,
      "max": 416.0
    },
    "allocs/op": {
      "median": 4.0,
      "min": 4.0,
      "max": 4.0
    },
    "ns/op": {
      "median": 151.1,
      "min": 135.0,
      "max": 172.4
    }
  },
  {
    "benchmark": "BenchmarkConstruct/RelayPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 416.0,
      "min": 416.0,
      "max": 416.0
    },
    "allocs/op": {
      "median": 4.0,
      "min": 4.0,
      "max": 4.0
    },
    "ns/op": {
      "median": 155.1,
      "min": 147.6,
      "max": 198.8
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/op": {
      "median": 23.38,
      "min": 22.86,
      "max": 24.51
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/op": {
      "median": 27.08,
      "min": 25.14,
      "max": 31.75
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/FreshChannel",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 112.0,
      "min": 112.0,
      "max": 112.0
    },
    "allocs/op": {
      "median": 1.0,
      "min": 1.0,
      "max": 1.0
    },
    "ns/op": {
      "median": 36.35,
      "min": 35.25,
      "max": 42.21
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/FreshChannel",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 112.0,
      "min": 112.0,
      "max": 112.0
    },
    "allocs/op": {
      "median": 1.0,
      "min": 1.0,
      "max": 1.0
    },
    "ns/op": {
      "median": 43.02,
      "min": 40.17,
      "max": 45.19
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/op": {
      "median": 20.92,
      "min": 13.8,
      "max": 21.86
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/op": {
      "median": 15.58,
      "min": 14.92,
      "max": 18.26
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/RelayPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/op": {
      "median": 15.71,
      "min": 15.01,
      "max": 16.89
    }
  },
  {
    "benchmark": "BenchmarkIdleChurn/RelayPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/op": {
      "median": 16.85,
      "min": 15.88,
      "max": 18.69
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 390.3,
      "min": 371.6,
      "max": 472.1
    },
    "ns/op": {
      "median": 199821.0,
      "min": 190268.0,
      "max": 241690.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 640.0,
      "min": 573.3,
      "max": 1043.0
    },
    "ns/op": {
      "median": 327689.0,
      "min": 293544.0,
      "max": 533788.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 327.0,
      "min": 288.5,
      "max": 372.1
    },
    "ns/op": {
      "median": 167425.0,
      "min": 147721.0,
      "max": 190526.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 270.1,
      "min": 215.5,
      "max": 298.2
    },
    "ns/op": {
      "median": 138285.0,
      "min": 110333.0,
      "max": 152686.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 358.0,
      "min": 317.5,
      "max": 418.3
    },
    "ns/op": {
      "median": 183298.0,
      "min": 162565.0,
      "max": 214191.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 325.8,
      "min": 297.3,
      "max": 468.7
    },
    "ns/op": {
      "median": 166830.0,
      "min": 152243.0,
      "max": 239966.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 328.8,
      "min": 313.7,
      "max": 332.4
    },
    "ns/op": {
      "median": 168359.0,
      "min": 160628.0,
      "max": 170178.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 321.3,
      "min": 308.4,
      "max": 371.2
    },
    "ns/op": {
      "median": 164519.0,
      "min": 157918.0,
      "max": 190038.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 331.2,
      "min": 292.6,
      "max": 388.2
    },
    "ns/op": {
      "median": 169555.0,
      "min": 149831.0,
      "max": 198758.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 386.3,
      "min": 336.7,
      "max": 398.3
    },
    "ns/op": {
      "median": 197802.0,
      "min": 172398.0,
      "max": 203937.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 317.6,
      "min": 297.5,
      "max": 318.1
    },
    "ns/op": {
      "median": 162602.0,
      "min": 152303.0,
      "max": 162865.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 353.0,
      "min": 303.5,
      "max": 362.7
    },
    "ns/op": {
      "median": 180723.0,
      "min": 155385.0,
      "max": 185710.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 328.2,
      "min": 303.5,
      "max": 377.4
    },
    "ns/op": {
      "median": 168064.0,
      "min": 155381.0,
      "max": 193231.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N512/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 445.2,
      "min": 391.1,
      "max": 504.7
    },
    "ns/op": {
      "median": 227965.0,
      "min": 200226.0,
      "max": 258402.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 308.7,
      "min": 271.5,
      "max": 411.0
    },
    "ns/op": {
      "median": 19758.0,
      "min": 17374.0,
      "max": 26305.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 498.2,
      "min": 473.6,
      "max": 511.0
    },
    "ns/op": {
      "median": 31883.0,
      "min": 30313.0,
      "max": 32704.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 271.9,
      "min": 247.9,
      "max": 293.9
    },
    "ns/op": {
      "median": 17403.0,
      "min": 15864.0,
      "max": 18810.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 305.3,
      "min": 286.2,
      "max": 330.7
    },
    "ns/op": {
      "median": 19536.0,
      "min": 18316.0,
      "max": 21164.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 236.7,
      "min": 222.6,
      "max": 347.0
    },
    "ns/op": {
      "median": 15150.0,
      "min": 14245.0,
      "max": 22205.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 375.9,
      "min": 339.2,
      "max": 429.9
    },
    "ns/op": {
      "median": 24058.0,
      "min": 21706.0,
      "max": 27514.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 258.7,
      "min": 247.2,
      "max": 265.5
    },
    "ns/op": {
      "median": 16554.0,
      "min": 15819.0,
      "max": 16990.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 301.7,
      "min": 276.0,
      "max": 321.5
    },
    "ns/op": {
      "median": 19306.0,
      "min": 17662.0,
      "max": 20578.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 259.9,
      "min": 251.1,
      "max": 292.4
    },
    "ns/op": {
      "median": 16634.0,
      "min": 16070.0,
      "max": 18715.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 375.4,
      "min": 299.1,
      "max": 586.2
    },
    "ns/op": {
      "median": 24024.0,
      "min": 19145.0,
      "max": 37515.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 269.1,
      "min": 253.1,
      "max": 303.3
    },
    "ns/op": {
      "median": 17221.0,
      "min": 16197.0,
      "max": 19409.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 375.7,
      "min": 342.8,
      "max": 439.3
    },
    "ns/op": {
      "median": 24042.0,
      "min": 21939.0,
      "max": 28115.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 268.3,
      "min": 256.5,
      "max": 288.4
    },
    "ns/op": {
      "median": 17172.0,
      "min": 16414.0,
      "max": 18455.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N64/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 373.3,
      "min": 328.2,
      "max": 415.0
    },
    "ns/op": {
      "median": 23889.0,
      "min": 21003.0,
      "max": 26561.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 349.2,
      "min": 345.0,
      "max": 378.2
    },
    "ns/op": {
      "median": 2794.0,
      "min": 2760.0,
      "max": 3026.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 660.6,
      "min": 657.6,
      "max": 674.4
    },
    "ns/op": {
      "median": 5285.0,
      "min": 5261.0,
      "max": 5395.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 258.2,
      "min": 246.9,
      "max": 286.3
    },
    "ns/op": {
      "median": 2066.0,
      "min": 1975.0,
      "max": 2291.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 458.9,
      "min": 436.1,
      "max": 480.4
    },
    "ns/op": {
      "median": 3672.0,
      "min": 3489.0,
      "max": 3843.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 262.7,
      "min": 247.1,
      "max": 283.6
    },
    "ns/op": {
      "median": 2101.0,
      "min": 1977.0,
      "max": 2269.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 459.5,
      "min": 402.3,
      "max": 499.1
    },
    "ns/op": {
      "median": 3676.0,
      "min": 3219.0,
      "max": 3993.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 273.1,
      "min": 266.7,
      "max": 304.5
    },
    "ns/op": {
      "median": 2185.0,
      "min": 2133.0,
      "max": 2436.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 311.3,
      "min": 277.5,
      "max": 336.0
    },
    "ns/op": {
      "median": 2490.0,
      "min": 2220.0,
      "max": 2688.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 298.5,
      "min": 284.2,
      "max": 333.1
    },
    "ns/op": {
      "median": 2388.0,
      "min": 2273.0,
      "max": 2665.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 575.5,
      "min": 538.0,
      "max": 613.5
    },
    "ns/op": {
      "median": 4604.0,
      "min": 4304.0,
      "max": 4908.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 278.8,
      "min": 270.3,
      "max": 295.6
    },
    "ns/op": {
      "median": 2231.0,
      "min": 2162.0,
      "max": 2365.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 498.9,
      "min": 482.3,
      "max": 584.5
    },
    "ns/op": {
      "median": 3991.0,
      "min": 3858.0,
      "max": 4676.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 284.5,
      "min": 259.5,
      "max": 350.5
    },
    "ns/op": {
      "median": 2276.0,
      "min": 2076.0,
      "max": 2804.0
    }
  },
  {
    "benchmark": "BenchmarkPayload1KiB/N8/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 483.8,
      "min": 450.6,
      "max": 553.3
    },
    "ns/op": {
      "median": 3871.0,
      "min": 3604.0,
      "max": 4426.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 283.1,
      "min": 276.9,
      "max": 293.1
    },
    "ns/op": {
      "median": 144954.0,
      "min": 141791.0,
      "max": 150089.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 30.0,
      "min": 0.0,
      "max": 97.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 1830.0,
      "min": 1572.0,
      "max": 1987.0
    },
    "ns/op": {
      "median": 937136.0,
      "min": 804679.0,
      "max": 1017423.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 250.8,
      "min": 241.6,
      "max": 266.1
    },
    "ns/op": {
      "median": 128386.0,
      "min": 123722.0,
      "max": 136231.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 498.4,
      "min": 471.3,
      "max": 515.9
    },
    "ns/op": {
      "median": 255192.0,
      "min": 241286.0,
      "max": 264117.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 267.2,
      "min": 251.9,
      "max": 290.7
    },
    "ns/op": {
      "median": 136788.0,
      "min": 128959.0,
      "max": 148852.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 1128.0,
      "min": 947.7,
      "max": 1244.0
    },
    "ns/op": {
      "median": 577491.0,
      "min": 485240.0,
      "max": 636760.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 255.7,
      "min": 251.9,
      "max": 263.2
    },
    "ns/op": {
      "median": 130940.0,
      "min": 128981.0,
      "max": 134736.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 314.5,
      "min": 281.9,
      "max": 328.7
    },
    "ns/op": {
      "median": 161023.0,
      "min": 144345.0,
      "max": 168287.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 268.9,
      "min": 263.1,
      "max": 292.9
    },
    "ns/op": {
      "median": 137652.0,
      "min": 134731.0,
      "max": 149953.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 1488.0,
      "min": 1057.0,
      "max": 2129.0
    },
    "ns/op": {
      "median": 761767.0,
      "min": 541106.0,
      "max": 1089796.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 304.5,
      "min": 277.3,
      "max": 398.2
    },
    "ns/op": {
      "median": 155912.0,
      "min": 141979.0,
      "max": 203887.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 1218.0,
      "min": 1067.0,
      "max": 1322.0
    },
    "ns/op": {
      "median": 623773.0,
      "min": 546077.0,
      "max": 676974.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 290.4,
      "min": 275.9,
      "max": 308.5
    },
    "ns/op": {
      "median": 148690.0,
      "min": 141281.0,
      "max": 157946.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N512/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 1240.0,
      "min": 1016.0,
      "max": 1563.0
    },
    "ns/op": {
      "median": 634888.0,
      "min": 520136.0,
      "max": 800456.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 303.2,
      "min": 280.7,
      "max": 313.2
    },
    "ns/op": {
      "median": 19402.0,
      "min": 17966.0,
      "max": 20045.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 5.0,
      "min": 3.0,
      "max": 10.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 455.1,
      "min": 416.1,
      "max": 892.9
    },
    "ns/op": {
      "median": 29129.0,
      "min": 26632.0,
      "max": 57146.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 256.3,
      "min": 228.7,
      "max": 461.3
    },
    "ns/op": {
      "median": 16405.0,
      "min": 14639.0,
      "max": 29526.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 532.1,
      "min": 366.9,
      "max": 580.1
    },
    "ns/op": {
      "median": 34055.0,
      "min": 23481.0,
      "max": 37129.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 241.7,
      "min": 233.3,
      "max": 268.3
    },
    "ns/op": {
      "median": 15471.0,
      "min": 14931.0,
      "max": 17171.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 2.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 905.1,
      "min": 797.4,
      "max": 1050.0
    },
    "ns/op": {
      "median": 57928.0,
      "min": 51035.0,
      "max": 67194.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 311.9,
      "min": 225.3,
      "max": 389.7
    },
    "ns/op": {
      "median": 19961.0,
      "min": 14418.0,
      "max": 24942.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 309.2,
      "min": 274.5,
      "max": 331.5
    },
    "ns/op": {
      "median": 19786.0,
      "min": 17565.0,
      "max": 21218.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 273.1,
      "min": 248.4,
      "max": 296.6
    },
    "ns/op": {
      "median": 17477.0,
      "min": 15900.0,
      "max": 18984.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 677.4,
      "min": 640.1,
      "max": 723.1
    },
    "ns/op": {
      "median": 43355.0,
      "min": 40968.0,
      "max": 46278.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 261.1,
      "min": 251.1,
      "max": 281.8
    },
    "ns/op": {
      "median": 16711.0,
      "min": 16069.0,
      "max": 18032.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 499.6,
      "min": 462.9,
      "max": 565.8
    },
    "ns/op": {
      "median": 31971.0,
      "min": 29623.0,
      "max": 36211.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 249.0,
      "min": 243.3,
      "max": 260.9
    },
    "ns/op": {
      "median": 15935.0,
      "min": 15573.0,
      "max": 16696.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N64/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 708.5,
      "min": 628.7,
      "max": 791.3
    },
    "ns/op": {
      "median": 45344.0,
      "min": 40234.0,
      "max": 50642.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/ChanPubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 344.6,
      "min": 332.1,
      "max": 369.8
    },
    "ns/op": {
      "median": 2757.0,
      "min": 2657.0,
      "max": 2958.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/ChanPubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 616.3,
      "min": 567.9,
      "max": 677.1
    },
    "ns/op": {
      "median": 4931.0,
      "min": 4544.0,
      "max": 5417.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/LinearChannels",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 399.2,
      "min": 299.5,
      "max": 474.3
    },
    "ns/op": {
      "median": 3194.0,
      "min": 2396.0,
      "max": 3795.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/LinearChannels",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 492.8,
      "min": 399.5,
      "max": 673.2
    },
    "ns/op": {
      "median": 3943.0,
      "min": 3196.0,
      "max": 5385.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/PhasePubSub",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 244.3,
      "min": 200.7,
      "max": 320.6
    },
    "ns/op": {
      "median": 1955.0,
      "min": 1606.0,
      "max": 2565.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/PhasePubSub",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 496.1,
      "min": 409.6,
      "max": 590.2
    },
    "ns/op": {
      "median": 3969.0,
      "min": 3277.0,
      "max": 4722.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW1",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 324.0,
      "min": 307.2,
      "max": 351.4
    },
    "ns/op": {
      "median": 2592.0,
      "min": 2458.0,
      "max": 2811.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW1",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 536.3,
      "min": 404.6,
      "max": 578.9
    },
    "ns/op": {
      "median": 4290.0,
      "min": 3236.0,
      "max": 4631.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW16",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 359.7,
      "min": 294.5,
      "max": 369.4
    },
    "ns/op": {
      "median": 2877.0,
      "min": 2356.0,
      "max": 2955.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW16",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 546.6,
      "min": 474.5,
      "max": 585.9
    },
    "ns/op": {
      "median": 4373.0,
      "min": 3796.0,
      "max": 4687.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW4",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 355.3,
      "min": 327.1,
      "max": 406.5
    },
    "ns/op": {
      "median": 2842.0,
      "min": 2617.0,
      "max": 3252.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW4",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 619.9,
      "min": 562.9,
      "max": 659.5
    },
    "ns/op": {
      "median": 4959.0,
      "min": 4503.0,
      "max": 5276.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW64",
    "gomaxprocs": 1,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 339.7,
      "min": 324.7,
      "max": 368.5
    },
    "ns/op": {
      "median": 2718.0,
      "min": 2598.0,
      "max": 2948.0
    }
  },
  {
    "benchmark": "BenchmarkSelect/N8/RelayW64",
    "gomaxprocs": 8,
    "samples": 5,
    "B/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "allocs/op": {
      "median": 0.0,
      "min": 0.0,
      "max": 0.0
    },
    "ns/delivery": {
      "median": 557.3,
      "min": 528.8,
      "max": 700.6
    },
    "ns/op": {
      "median": 4459.0,
      "min": 4230.0,
      "max": 5605.0
    }
  }
]
-- results/summary.md --
# Benchmark medians

Raw samples are in the adjacent bench-*.txt files.

| Benchmark | Ps | Samples | Median ns/op | Min | Max | Median B/op | Median allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| BenchmarkBroadcast/N1/ChanPubSub | 1 | 5 | 435.1 | 398.3 | 447.3 | 0 | 0 |
| BenchmarkBroadcast/N1/ChanPubSub | 8 | 5 | 500.3 | 455.9 | 503.6 | 0 | 0 |
| BenchmarkBroadcast/N1/LinearChannels | 1 | 5 | 155.7 | 150 | 182.8 | 0 | 0 |
| BenchmarkBroadcast/N1/LinearChannels | 8 | 5 | 176.7 | 171 | 184.4 | 0 | 0 |
| BenchmarkBroadcast/N1/PhasePubSub | 1 | 5 | 190.2 | 168.9 | 224.8 | 0 | 0 |
| BenchmarkBroadcast/N1/PhasePubSub | 8 | 5 | 202.1 | 193.9 | 260 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW1 | 1 | 5 | 327.5 | 305.7 | 356.6 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW1 | 8 | 5 | 388.4 | 337.4 | 401 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW16 | 1 | 5 | 345 | 334 | 407.8 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW16 | 8 | 5 | 403.3 | 359.9 | 464.9 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW4 | 1 | 5 | 307.6 | 300.1 | 325.3 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW4 | 8 | 5 | 347.8 | 332.1 | 359.8 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW64 | 1 | 5 | 363.8 | 326.1 | 407.2 | 0 | 0 |
| BenchmarkBroadcast/N1/RelayW64 | 8 | 5 | 475.4 | 386 | 538.3 | 0 | 0 |
| BenchmarkBroadcast/N4096/ChanPubSub | 1 | 5 | 1.34357e+06 | 1.27262e+06 | 1.7632e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/ChanPubSub | 8 | 5 | 3.07605e+06 | 2.79481e+06 | 3.61182e+06 | 30 | 0 |
| BenchmarkBroadcast/N4096/LinearChannels | 1 | 5 | 1.20837e+06 | 860153 | 1.52547e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/LinearChannels | 8 | 5 | 559556 | 501982 | 589950 | 0 | 0 |
| BenchmarkBroadcast/N4096/PhasePubSub | 1 | 5 | 886439 | 840631 | 1.13251e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/PhasePubSub | 8 | 5 | 1.00613e+06 | 996207 | 1.13027e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW1 | 1 | 5 | 1.17492e+06 | 1.00704e+06 | 1.32743e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW1 | 8 | 5 | 1.12094e+06 | 1.08552e+06 | 1.59454e+06 | 1 | 0 |
| BenchmarkBroadcast/N4096/RelayW16 | 1 | 5 | 1.21189e+06 | 930964 | 1.8889e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW16 | 8 | 5 | 1.54416e+06 | 1.08506e+06 | 2.11827e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW4 | 1 | 5 | 1.0132e+06 | 915128 | 1.06316e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW4 | 8 | 5 | 1.31007e+06 | 1.15538e+06 | 1.5531e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW64 | 1 | 5 | 905328 | 827937 | 1.35574e+06 | 0 | 0 |
| BenchmarkBroadcast/N4096/RelayW64 | 8 | 5 | 1.53833e+06 | 1.21733e+06 | 2.14604e+06 | 0 | 0 |
| BenchmarkBroadcast/N512/ChanPubSub | 1 | 5 | 116407 | 100860 | 121179 | 0 | 0 |
| BenchmarkBroadcast/N512/ChanPubSub | 8 | 5 | 262041 | 239216 | 277496 | 0 | 0 |
| BenchmarkBroadcast/N512/LinearChannels | 1 | 5 | 100153 | 94350 | 136814 | 0 | 0 |
| BenchmarkBroadcast/N512/LinearChannels | 8 | 5 | 89676 | 80590 | 99715 | 0 | 0 |
| BenchmarkBroadcast/N512/PhasePubSub | 1 | 5 | 95264 | 94019 | 106734 | 0 | 0 |
| BenchmarkBroadcast/N512/PhasePubSub | 8 | 5 | 152040 | 140667 | 162993 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW1 | 1 | 5 | 90018 | 87556 | 106569 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW1 | 8 | 5 | 117186 | 104597 | 129806 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW16 | 1 | 5 | 96187 | 89263 | 104049 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW16 | 8 | 5 | 165647 | 154625 | 177338 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW4 | 1 | 5 | 105454 | 98991 | 119992 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW4 | 8 | 5 | 138473 | 129535 | 183403 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW64 | 1 | 5 | 109798 | 97899 | 134569 | 0 | 0 |
| BenchmarkBroadcast/N512/RelayW64 | 8 | 5 | 231655 | 144479 | 247616 | 0 | 0 |
| BenchmarkBroadcast/N64/ChanPubSub | 1 | 5 | 15076 | 14944 | 18344 | 0 | 0 |
| BenchmarkBroadcast/N64/ChanPubSub | 8 | 5 | 27078 | 22323 | 29139 | 0 | 0 |
| BenchmarkBroadcast/N64/LinearChannels | 1 | 5 | 12577 | 11792 | 14254 | 0 | 0 |
| BenchmarkBroadcast/N64/LinearChannels | 8 | 5 | 15741 | 12892 | 17533 | 0 | 0 |
| BenchmarkBroadcast/N64/PhasePubSub | 1 | 5 | 15439 | 12490 | 16482 | 0 | 0 |
| BenchmarkBroadcast/N64/PhasePubSub | 8 | 5 | 22539 | 19970 | 23161 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW1 | 1 | 5 | 12290 | 11229 | 12909 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW1 | 8 | 5 | 13930 | 12713 | 17486 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW16 | 1 | 5 | 13966 | 12786 | 19490 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW16 | 8 | 5 | 23092 | 22203 | 28405 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW4 | 1 | 5 | 12481 | 12352 | 13479 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW4 | 8 | 5 | 17801 | 16734 | 19803 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW64 | 1 | 5 | 11223 | 11213 | 11642 | 0 | 0 |
| BenchmarkBroadcast/N64/RelayW64 | 8 | 5 | 17452 | 16804 | 19961 | 0 | 0 |
| BenchmarkBroadcast/N8/ChanPubSub | 1 | 5 | 1880 | 1788 | 2145 | 0 | 0 |
| BenchmarkBroadcast/N8/ChanPubSub | 8 | 5 | 2936 | 2834 | 3179 | 0 | 0 |
| BenchmarkBroadcast/N8/LinearChannels | 1 | 5 | 1645 | 1407 | 1750 | 0 | 0 |
| BenchmarkBroadcast/N8/LinearChannels | 8 | 5 | 2477 | 2438 | 2524 | 0 | 0 |
| BenchmarkBroadcast/N8/PhasePubSub | 1 | 5 | 1396 | 1293 | 1440 | 0 | 0 |
| BenchmarkBroadcast/N8/PhasePubSub | 8 | 5 | 2497 | 2413 | 2522 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW1 | 1 | 5 | 1696 | 1639 | 1722 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW1 | 8 | 5 | 1884 | 1765 | 2152 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW16 | 1 | 5 | 1614 | 1477 | 1697 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW16 | 8 | 5 | 2298 | 2287 | 2457 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW4 | 1 | 5 | 1655 | 1523 | 1680 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW4 | 8 | 5 | 2398 | 2172 | 2732 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW64 | 1 | 5 | 1562 | 1545 | 1822 | 0 | 0 |
| BenchmarkBroadcast/N8/RelayW64 | 8 | 5 | 2383 | 2298 | 2610 | 0 | 0 |
| BenchmarkConstruct/ChanPubSub | 1 | 5 | 124 | 123.2 | 135.8 | 408 | 5 |
| BenchmarkConstruct/ChanPubSub | 8 | 5 | 325.8 | 197 | 557 | 408 | 5 |
| BenchmarkConstruct/PhasePubSub | 1 | 5 | 176.7 | 135.8 | 194 | 304 | 3 |
| BenchmarkConstruct/PhasePubSub | 8 | 5 | 293.7 | 132.6 | 364.8 | 304 | 3 |
| BenchmarkConstruct/RelayPubSub | 1 | 5 | 151.1 | 135 | 172.4 | 416 | 4 |
| BenchmarkConstruct/RelayPubSub | 8 | 5 | 155.1 | 147.6 | 198.8 | 416 | 4 |
| BenchmarkIdleChurn/ChanPubSub | 1 | 5 | 23.38 | 22.86 | 24.51 | 0 | 0 |
| BenchmarkIdleChurn/ChanPubSub | 8 | 5 | 27.08 | 25.14 | 31.75 | 0 | 0 |
| BenchmarkIdleChurn/FreshChannel | 1 | 5 | 36.35 | 35.25 | 42.21 | 112 | 1 |
| BenchmarkIdleChurn/FreshChannel | 8 | 5 | 43.02 | 40.17 | 45.19 | 112 | 1 |
| BenchmarkIdleChurn/PhasePubSub | 1 | 5 | 20.92 | 13.8 | 21.86 | 0 | 0 |
| BenchmarkIdleChurn/PhasePubSub | 8 | 5 | 15.58 | 14.92 | 18.26 | 0 | 0 |
| BenchmarkIdleChurn/RelayPubSub | 1 | 5 | 15.71 | 15.01 | 16.89 | 0 | 0 |
| BenchmarkIdleChurn/RelayPubSub | 8 | 5 | 16.85 | 15.88 | 18.69 | 0 | 0 |
| BenchmarkPayload1KiB/N512/ChanPubSub | 1 | 5 | 199821 | 190268 | 241690 | 0 | 0 |
| BenchmarkPayload1KiB/N512/ChanPubSub | 8 | 5 | 327689 | 293544 | 533788 | 0 | 0 |
| BenchmarkPayload1KiB/N512/LinearChannels | 1 | 5 | 167425 | 147721 | 190526 | 0 | 0 |
| BenchmarkPayload1KiB/N512/LinearChannels | 8 | 5 | 138285 | 110333 | 152686 | 0 | 0 |
| BenchmarkPayload1KiB/N512/PhasePubSub | 1 | 5 | 183298 | 162565 | 214191 | 0 | 0 |
| BenchmarkPayload1KiB/N512/PhasePubSub | 8 | 5 | 166830 | 152243 | 239966 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW1 | 1 | 5 | 168359 | 160628 | 170178 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW1 | 8 | 5 | 164519 | 157918 | 190038 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW16 | 1 | 5 | 169555 | 149831 | 198758 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW16 | 8 | 5 | 197802 | 172398 | 203937 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW4 | 1 | 5 | 162602 | 152303 | 162865 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW4 | 8 | 5 | 180723 | 155385 | 185710 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW64 | 1 | 5 | 168064 | 155381 | 193231 | 0 | 0 |
| BenchmarkPayload1KiB/N512/RelayW64 | 8 | 5 | 227965 | 200226 | 258402 | 0 | 0 |
| BenchmarkPayload1KiB/N64/ChanPubSub | 1 | 5 | 19758 | 17374 | 26305 | 0 | 0 |
| BenchmarkPayload1KiB/N64/ChanPubSub | 8 | 5 | 31883 | 30313 | 32704 | 0 | 0 |
| BenchmarkPayload1KiB/N64/LinearChannels | 1 | 5 | 17403 | 15864 | 18810 | 0 | 0 |
| BenchmarkPayload1KiB/N64/LinearChannels | 8 | 5 | 19536 | 18316 | 21164 | 0 | 0 |
| BenchmarkPayload1KiB/N64/PhasePubSub | 1 | 5 | 15150 | 14245 | 22205 | 0 | 0 |
| BenchmarkPayload1KiB/N64/PhasePubSub | 8 | 5 | 24058 | 21706 | 27514 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW1 | 1 | 5 | 16554 | 15819 | 16990 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW1 | 8 | 5 | 19306 | 17662 | 20578 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW16 | 1 | 5 | 16634 | 16070 | 18715 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW16 | 8 | 5 | 24024 | 19145 | 37515 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW4 | 1 | 5 | 17221 | 16197 | 19409 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW4 | 8 | 5 | 24042 | 21939 | 28115 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW64 | 1 | 5 | 17172 | 16414 | 18455 | 0 | 0 |
| BenchmarkPayload1KiB/N64/RelayW64 | 8 | 5 | 23889 | 21003 | 26561 | 0 | 0 |
| BenchmarkPayload1KiB/N8/ChanPubSub | 1 | 5 | 2794 | 2760 | 3026 | 0 | 0 |
| BenchmarkPayload1KiB/N8/ChanPubSub | 8 | 5 | 5285 | 5261 | 5395 | 0 | 0 |
| BenchmarkPayload1KiB/N8/LinearChannels | 1 | 5 | 2066 | 1975 | 2291 | 0 | 0 |
| BenchmarkPayload1KiB/N8/LinearChannels | 8 | 5 | 3672 | 3489 | 3843 | 0 | 0 |
| BenchmarkPayload1KiB/N8/PhasePubSub | 1 | 5 | 2101 | 1977 | 2269 | 0 | 0 |
| BenchmarkPayload1KiB/N8/PhasePubSub | 8 | 5 | 3676 | 3219 | 3993 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW1 | 1 | 5 | 2185 | 2133 | 2436 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW1 | 8 | 5 | 2490 | 2220 | 2688 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW16 | 1 | 5 | 2388 | 2273 | 2665 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW16 | 8 | 5 | 4604 | 4304 | 4908 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW4 | 1 | 5 | 2231 | 2162 | 2365 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW4 | 8 | 5 | 3991 | 3858 | 4676 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW64 | 1 | 5 | 2276 | 2076 | 2804 | 0 | 0 |
| BenchmarkPayload1KiB/N8/RelayW64 | 8 | 5 | 3871 | 3604 | 4426 | 0 | 0 |
| BenchmarkSelect/N512/ChanPubSub | 1 | 5 | 144954 | 141791 | 150089 | 0 | 0 |
| BenchmarkSelect/N512/ChanPubSub | 8 | 5 | 937136 | 804679 | 1.01742e+06 | 30 | 0 |
| BenchmarkSelect/N512/LinearChannels | 1 | 5 | 128386 | 123722 | 136231 | 0 | 0 |
| BenchmarkSelect/N512/LinearChannels | 8 | 5 | 255192 | 241286 | 264117 | 0 | 0 |
| BenchmarkSelect/N512/PhasePubSub | 1 | 5 | 136788 | 128959 | 148852 | 0 | 0 |
| BenchmarkSelect/N512/PhasePubSub | 8 | 5 | 577491 | 485240 | 636760 | 0 | 0 |
| BenchmarkSelect/N512/RelayW1 | 1 | 5 | 130940 | 128981 | 134736 | 0 | 0 |
| BenchmarkSelect/N512/RelayW1 | 8 | 5 | 161023 | 144345 | 168287 | 0 | 0 |
| BenchmarkSelect/N512/RelayW16 | 1 | 5 | 137652 | 134731 | 149953 | 0 | 0 |
| BenchmarkSelect/N512/RelayW16 | 8 | 5 | 761767 | 541106 | 1.0898e+06 | 0 | 0 |
| BenchmarkSelect/N512/RelayW4 | 1 | 5 | 155912 | 141979 | 203887 | 0 | 0 |
| BenchmarkSelect/N512/RelayW4 | 8 | 5 | 623773 | 546077 | 676974 | 0 | 0 |
| BenchmarkSelect/N512/RelayW64 | 1 | 5 | 148690 | 141281 | 157946 | 0 | 0 |
| BenchmarkSelect/N512/RelayW64 | 8 | 5 | 634888 | 520136 | 800456 | 0 | 0 |
| BenchmarkSelect/N64/ChanPubSub | 1 | 5 | 19402 | 17966 | 20045 | 0 | 0 |
| BenchmarkSelect/N64/ChanPubSub | 8 | 5 | 29129 | 26632 | 57146 | 5 | 0 |
| BenchmarkSelect/N64/LinearChannels | 1 | 5 | 16405 | 14639 | 29526 | 0 | 0 |
| BenchmarkSelect/N64/LinearChannels | 8 | 5 | 34055 | 23481 | 37129 | 0 | 0 |
| BenchmarkSelect/N64/PhasePubSub | 1 | 5 | 15471 | 14931 | 17171 | 0 | 0 |
| BenchmarkSelect/N64/PhasePubSub | 8 | 5 | 57928 | 51035 | 67194 | 0 | 0 |
| BenchmarkSelect/N64/RelayW1 | 1 | 5 | 19961 | 14418 | 24942 | 0 | 0 |
| BenchmarkSelect/N64/RelayW1 | 8 | 5 | 19786 | 17565 | 21218 | 0 | 0 |
| BenchmarkSelect/N64/RelayW16 | 1 | 5 | 17477 | 15900 | 18984 | 0 | 0 |
| BenchmarkSelect/N64/RelayW16 | 8 | 5 | 43355 | 40968 | 46278 | 0 | 0 |
| BenchmarkSelect/N64/RelayW4 | 1 | 5 | 16711 | 16069 | 18032 | 0 | 0 |
| BenchmarkSelect/N64/RelayW4 | 8 | 5 | 31971 | 29623 | 36211 | 0 | 0 |
| BenchmarkSelect/N64/RelayW64 | 1 | 5 | 15935 | 15573 | 16696 | 0 | 0 |
| BenchmarkSelect/N64/RelayW64 | 8 | 5 | 45344 | 40234 | 50642 | 0 | 0 |
| BenchmarkSelect/N8/ChanPubSub | 1 | 5 | 2757 | 2657 | 2958 | 0 | 0 |
| BenchmarkSelect/N8/ChanPubSub | 8 | 5 | 4931 | 4544 | 5417 | 0 | 0 |
| BenchmarkSelect/N8/LinearChannels | 1 | 5 | 3194 | 2396 | 3795 | 0 | 0 |
| BenchmarkSelect/N8/LinearChannels | 8 | 5 | 3943 | 3196 | 5385 | 0 | 0 |
| BenchmarkSelect/N8/PhasePubSub | 1 | 5 | 1955 | 1606 | 2565 | 0 | 0 |
| BenchmarkSelect/N8/PhasePubSub | 8 | 5 | 3969 | 3277 | 4722 | 0 | 0 |
| BenchmarkSelect/N8/RelayW1 | 1 | 5 | 2592 | 2458 | 2811 | 0 | 0 |
| BenchmarkSelect/N8/RelayW1 | 8 | 5 | 4290 | 3236 | 4631 | 0 | 0 |
| BenchmarkSelect/N8/RelayW16 | 1 | 5 | 2877 | 2356 | 2955 | 0 | 0 |
| BenchmarkSelect/N8/RelayW16 | 8 | 5 | 4373 | 3796 | 4687 | 0 | 0 |
| BenchmarkSelect/N8/RelayW4 | 1 | 5 | 2842 | 2617 | 3252 | 0 | 0 |
| BenchmarkSelect/N8/RelayW4 | 8 | 5 | 4959 | 4503 | 5276 | 0 | 0 |
| BenchmarkSelect/N8/RelayW64 | 1 | 5 | 2718 | 2598 | 2948 | 0 | 0 |
| BenchmarkSelect/N8/RelayW64 | 8 | 5 | 4459 | 4230 | 5605 | 0 | 0 |
-- results/validation.txt --
$ go version
go version go1.27.3 linux/amd64
exit_code=0

$ go test -race -cpu=1,2,8 -count=5 -timeout=90s ./...
ok  	example.com/bigbuff-evolution	4.872s
exit_code=0

$ go vet ./...
exit_code=0

$ python3 model/credit_model.py
PASS: N=1..12, every K=1..N; 50,011 reachable states; no credit-capacity, progress, or completion invariant violations.
exit_code=0


````

