Implemented a complementary primitive: **`RingPubSub[V]`**, a bounded, lossless broadcast log with reusable select notification channels, no subscriber registry, and an ordinary receive path that avoids the hub mutex.

Its strongest characteristics are:

- **Fixed shared infrastructure:** `B+1` payload slots and `B+2` channels for a backlog capacity of `B`, independent of subscriber count and churn.
- **No per-subscription channel or background goroutine.** Each subscription is a caller-owned cursor—two machine words on the tested 64-bit target.
- **Zero measured allocation for ordinary publication, reception, and subscription churn.**
- **Native `select` support**, including multiple subscriptions consumed by one goroutine.
- **Bounded, lossless backpressure**, concurrent publishers, cancellation, orderly shutdown, and prompt payload-reference clearing.

There is one deliberate semantic change: **`Send` acknowledges publication into the log, rather than completed receipt by every subscriber.** This is an additional implementation, with its own contract.

## Deliverable details

Deliverable at the end of this file.

The archive contains the complete implementation, an additive patch, adversarial tests, runnable examples, comparative benchmarks, raw results, and a detailed design explanation. The supplied Mac path was not mounted here, so I validated the patch against a fetched repository snapshot at `04f9b9f58d17938686a4d5014f8ed5531c97ac34`; the existing implementations match your supplied logic.

**The measured result:** at 64 subscribers, with `select` on every receive, the new implementation took a median **6.77 µs per complete broadcast**, versus **45.04 µs** for the supplied `ChanPubSub`. At 16,384 subscribers, the corresponding medians were **2.00 ms** and **46.17 ms**. These measurements include delivery to every receiver and the final drain.

## 1. The central idea: use one spare slot to make channel reuse safe

The useful advance is a particular combination of a shared log, cursor ownership, reference counts, and reusable notification channels.

Each physical slot contains:

```go
type ringPubSubSlot[V any] struct {
	value     V
	remaining atomic.Int64
	ready     chan struct{} // capacity 1, reused
}
```

Publication stores the value once and initializes `remaining` to the number of subscriptions active at that publication boundary. Every subscription holds its own next-slot index.

For a capacity of three outstanding messages, the physical arrangement can be:

| Physical slot | Contents | Relevant position |
|---|---|---|
| 0 | Oldest outstanding value | Slow reader’s next value |
| 1 | Second value | |
| 2 | Newest value | |
| 3 | Empty | Publication tail, caught-up reader, new subscriber |

**The fourth slot is essential.**

With only three physical slots, a caught-up reader’s next position would wrap onto slot 0. Its `select` could consume a notification belonging to the oldest message, even though that reader is waiting for a future publication.

With four slots, the caught-up reader waits on the empty slot. It cannot consume another generation’s notification.

The general invariant is:

> At most `B` published positions occupy `B+1` physical slots. Every valid cursor position, including the future tail, therefore identifies the correct notification channel without aliasing an older outstanding publication.

An outstanding reader obligation prevents publication from advancing far enough to violate that invariant.

That gives the implementation several useful properties simultaneously:

- Channels are reused across publications.
- No channel is allocated or closed for each new generation.
- Subscribers need no private notification channel.
- The publisher needs no subscriber registry.
- A native Go `select` can wait on a cursor’s next position.

The indices are physical positions with explicit wraparound. There is no ever-increasing sequence number whose overflow must be compared correctly.

Broadcast rings and per-slot reference counts are established techniques; Tokio’s broadcast implementation is one relevant example. The distinctive contribution here is the combination of **reusable Go-select channels, the spare-slot invariant, reference-protected notification handoff, registry-free membership, and shutdown at the unused tail**. I am not claiming to have established worldwide novelty. [source](https://docs.rs/tokio/latest/src/tokio/sync/broadcast.rs.html?utm_source=chatgpt.com)

## 2. The important ordering: notify before releasing the reference

The ordinary receive path is essentially:

```go
value = slot.value
signalNonblocking(slot.ready)
remaining := slot.remaining.Add(-1)
advanceCursor()

if remaining == 0 {
	reclaimCurrentCompletedPrefix()
}
```

The order of those operations is critical.

A reader still owns an obligation while it copies the payload and passes on the notification. Its obligation therefore keeps the slot alive through both operations.

Only afterward does it decrement `remaining`.

If notification happened after decrementing, the final release could allow reclamation before an earlier reader finished signaling. That delayed notification could then affect a later use of the same channel.

Here, observing a zero remaining count establishes that **all payload copies and all notification sends for that generation have finished**. Reclamation can safely:

1. Clear the stored payload, including any pointers it retains.
2. Drain a leftover buffered notification.
3. Make the slot available for reuse.

The publisher also retains the hub mutex through its initial notification. A fast polling reader may finish before that initial notification is sent, but reclamation cannot pass the publisher’s mutex and run too early.

### The notification is not an exclusively owned token

This distinction makes the design more flexible.

A reader can poll `TryRecv` without first receiving a notification. Several readers can also have received notifications while still owing their payload copies.

Correctness follows from their outstanding obligations and cursor positions. It does not require keeping track of which goroutine currently “owns” a notification.

That matters for cancellation: a subscription that consumed a notification can close, release its unread obligations, and restore notification for remaining readers. It does not need to complete a rendezvous with the publisher.

### Ordinary readers avoid the hub mutex

The publisher initializes the slot before atomically advancing the tail. Readers observe publication through that atomic tail.

After copying the value, each reader performs the notification attempt and an atomic decrement. A reader whose decrement leaves other obligations outstanding returns without acquiring the hub mutex.

Only a final release enters the mutex to reclaim completed positions.

The ordering relies on Go’s documented atomic and mutex synchronization, including the ordering of ordinary payload accesses around those operations. The implementation uses public Go APIs; it has no `unsafe` operations or runtime linknames. [The Go Programming Language](https://go.dev/ref/mem?utm_source=chatgpt.com)

This is **not a lock-free claim**. Channel operations still use runtime synchronization, the shared reference count can contend, and final reclamation uses a mutex. The precise claim is that the common non-final reader avoids the **hub’s** mutex.

### Publishers can help a delayed final reader

There is another useful refinement.

A final reader can be descheduled after decrementing the count to zero but before acquiring the reclamation mutex. A publisher that encounters a full ring checks for a completed prefix and reclaims it itself.

The publisher therefore need not wait for that particular reader to run again.

When the delayed reader resumes, it reclaims from the **current head**. It never treats its previously captured slot as permission to retire something again.

The tests force this interleaving: they pause the final reader after its atomic release, let the publisher help and publish another value, then verify that the delayed reaper preserves the new publication.

## 3. The spare slot also gives orderly shutdown

At shutdown, the implementation closes:

- The notification channel at the unused tail.
- The shared publisher-capacity notification channel.

It leaves occupied data channels open.

Readers can therefore continue consuming their backlog and passing notifications safely. Each cursor eventually reaches the same terminal tail position and observes a closed channel through its existing `select`.

This avoids needing a separate shutdown case in every consumer select, while preserving drain-before-EOF behavior.

There are only two channel-close operations and no application-level subscriber scan. However, **closing a channel is not constant-time overall**: the Go runtime walks its blocked waiters and makes them runnable. The implementation does not hide that work behind an “O(1) broadcast” claim. [go.dev](https://go.dev/src/runtime/chan.go?utm_source=chatgpt.com)

## 4. Membership has no subscriber registry

Subscribing takes the publication mutex, increments the subscriber count, and returns a cursor at the current tail.

Existing slots keep their original counts. The new subscriber receives future publications.

Unsubscribing takes the same mutex, removes the subscription from future publication counts, and releases its obligation in every unread slot. There are at most `B` such slots.

Consequently:

- Joining takes constant work.
- Leaving takes work proportional to that cursor’s unread backlog.
- Neither operation searches or modifies a collection of other subscribers.
- The hub never retains a pointer to the subscription.
- Historical subscriber churn does not grow a map, slice, pool, or cache owned by the implementation.

The cursor includes a zero-size `go vet` copy marker. It deliberately does not contain a self-pointer for runtime copy detection: such a pointer would undermine the ability to keep the cursor on the caller’s stack.

This is where the allocation behavior becomes particularly useful. The implementation does not need a heap-allocated identity object simply to represent a subscription.

## 5. Using it from `select`

The ordinary pattern is:

```go
func consume(
	ctx context.Context,
	hub *bigbuff.RingPubSub[Event],
) error {
	sub := hub.Subscribe()
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case _, open := <-sub.Ready():
			if !open {
				return nil // closed hub, fully drained cursor
			}

			value, ok := sub.TryRecv()
			if !ok {
				panic("subscription contract violated")
			}

			handle(value)
		}
	}
}
```

The producer uses:

```go
recipients, err := hub.Send(ctx, event)
```

A successful result means the value was published for those subscriptions. It does not mean all of them have consumed it, and a subscription can subsequently abandon it by closing.

The available operations are:

| Operation | Behavior |
|---|---|
| `NewRingPubSub[V](capacity)` | Allocate fixed shared infrastructure |
| `Subscribe()` | Register immediately for future publications; return a caller-owned cursor |
| `TrySend(value)` | Publish, return `ErrRingFull`, or return `io.ErrClosedPipe` |
| `Send(ctx, value)` | Wait for capacity, with cancellation |
| `Ready()` | Return the notification channel for this cursor’s next value |
| `TryRecv()` | Consume an available value without waiting for one |
| `Read(dst)` | Drain available values into a reusable caller-owned slice |
| `Recv(ctx)` | Receive with cancellation; return `io.EOF` after draining |
| Subscription `Close()` | Unregister and abandon that cursor’s backlog |
| Hub `Close()` | Stop publication and allow existing cursors to drain |

There are three important cursor rules.

**First, each subscription has one owner.** Its methods and receives from `Ready()` must be serialized. Pass its address when transferring ownership to another goroutine; do not copy an active cursor.

**Second, evaluate `Ready()` again after advancing.** Its channel changes as the cursor moves through the ring. A cached channel remains valid only until that cursor is mutated.

**Third, after successfully receiving a notification, promptly consume or close.** Calling `TryRecv`, nonempty `Read`, `Recv`, or `Close` completes that obligation. Holding a consumed notification indefinitely can delay peers.

If another select case wins, nothing needs repairing: that select did not consume the notification.

### Cancellation preserves the subscription

`Recv(ctx)` returning a context error leaves the subscription active and preserves its backlog. The owner should call `Close` when it is finished with the subscription.

A canceled `Recv` also restores notification when necessary. That includes the case where the caller consumed `Ready()` before entering `Recv`.

The publisher side has an analogous detail. Several publishers share one coalesced capacity notification. A publisher that succeeds passes the notification on if capacity remains. A canceled publisher that consumed the only notification repairs it before returning. Otherwise, available capacity could remain stranded while another publisher sleeps.

Both cases have directed tests.

### Multiple subscriptions can belong to one goroutine

This is a significant increase in applicability.

Your persistent implementation requires each registered subscriber to receive and enter the collective `Wait` protocol. The new implementation lets one goroutine select among several logical subscriptions and advance each independently.

The included external-package example does exactly that: one goroutine consumes two subscriptions, refreshes their notification channels as it progresses, and verifies both ordered sequences.

It can also drain existing backlog with `TryRecv` or `Read(dst)` and enter `select` only when caught up. That is the faster consumption mode in the benchmarks, while retaining select support when idle.

## 6. Measured performance

The benchmarks ran with **Go 1.27.3 on Linux AMD64**, in a shared environment with an eight-CPU quota. The table below uses `GOMAXPROCS=8`.

Each receiver checks every value’s order and calculates a checksum. **Every measurement includes complete delivery and the final drain.** Setup, goroutine creation, construction, warmup, and final unsubscription are outside the timed region.

The first four rows are medians of five samples. The two larger fan-outs are medians of three samples.

**Microseconds per complete broadcast; lower is better:**

| Subscribers | Supplied `ChanPubSub` | Ring: select every value, B=64 | Ring: poll, then select when empty, B=64 | Per-subscriber channels, B=64 each |
|---:|---:|---:|---:|---:|
| 1 | 0.533 | 0.150 | 0.074 | 0.088 |
| 8 | 4.40 | 0.742 | 0.191 | 1.07 |
| 64 | 45.04 | 6.77 | 1.09 | 8.84 |
| 512 | 823.00 | 55.24 | 7.44 | 118.99* |
| 4,096 | 9,521.66 | 410.48 | 66.15 | 508.71 |
| 16,384 | 46,165.33 | 2,004.49 | 412.17 | 4,856.77 |

\* The 512-subscriber per-channel control was particularly noisy, ranging from approximately 60 to 214 µs in its complete rerun. The archive documents the capture issue, rerun, and all observations.

At 64 subscribers, the five always-select ring observations ranged from **6.24 to 7.62 µs**. The supplied `ChanPubSub` ranged from **43.63 to 47.82 µs**, and the buffered per-subscriber-channel control from **7.81 to 9.35 µs**.

Those are useful differences, but their interpretation matters:

- The ring permits bounded reader lag and removes the collective per-message receipt barrier.
- Both implementations still perform every required delivery.
- These are throughput measurements, not individual-message latency percentiles.
- Capacity one removes much of the buffering benefit.
- More execution threads do not consistently improve performance; the archive also contains `GOMAXPROCS=1` measurements.

The callback-channel implementation you referred to was not supplied. The comparison therefore uses a **synthetic fixed-slice loop that sends directly to per-subscriber channels**, without registry lookup or callback dispatch overhead. It is an optimistic channel-fanout control, not a claimed reconstruction of your other implementation.

I also included an inline-callback control. It is faster, as expected for callbacks executed synchronously inside the publisher goroutine, but it does not provide independent receiving goroutines or the select behavior being measured.

## 7. Allocation behavior and the fixed-memory trade

Every ring observation in the main matrix, through 512 subscribers, reported:

```text
0 B/op
0 allocs/op
```

Separate allocation assertions also passed for:

- Stack-resident subscribe/close churn.
- Publication followed by selected reception.
- Subscribing, queuing values, and abandoning them.
- Batch reception into a reused slice.

The empty subscription lifecycle measured approximately:

| Lifecycle | Median time | Allocated bytes | Allocations |
|---|---:|---:|---:|
| `ChanCaster.Add(1)` / `Add(-1)` | 14.12 ns | 0 | 0 |
| `ChanPubSub` subscribe / unsubscribe | 28.72 ns | 0 | 0 |
| `RingPubSub` subscribe / close | 18.51 ns | 0 | 0 |
| Fresh unbuffered channel | 53.43 ns | 112 | 1 |
| Fresh channel with buffer 64 | 208.20 ns | 640 | 1 |

The fresh-channel controls omit registry changes, context setup, callbacks, and goroutine creation. They are lower bounds on a complete per-subscriber-channel lifecycle.

Construction deliberately costs more than your very compact existing shared infrastructure:

| Shared infrastructure | Measured construction bytes | Allocations |
|---|---:|---:|
| `ChanCaster`, including supplied channel | 160 | 2 |
| `ChanPubSub`, including supplied channel | 408 | 5 |
| Ring, capacity 1 | 464 | 5 |
| Ring, capacity 64 | 9,264 | 68 |
| 512 per-subscriber channels, capacity 64 each, plus slice | Approximately 333,000 | 513 |

The ring’s cost stays fixed as subscriptions join and leave. Caller-owned cursor storage remains additional—about 16 bytes each on the measured target—as do goroutines, stacks, and runtime wait records.

**I would not describe the entire process as allocation-free.** The largest concurrent runs recorded small nonzero byte rates. For example, the always-select ring at 16,384 subscribers had a median of **22 B/op**, while the rounded allocations-per-operation column showed zero. That is not evidence of exact zero process-wide allocation.

The precise implementation property is that it creates **no per-publication, per-receive, or per-subscription infrastructure**. Escape analysis, large value temporaries, runtime parking, and application-owned values can still allocate.

Payload storage is also shared rather than duplicated into every subscriber’s buffer. There is one retained payload per publication, followed by a value copy into each receiver. Referenced objects remain shared, with the same mutation-synchronization requirements as ordinary channel values.

## 8. An optimization I tested and rejected

I tested a more aggressive notification handoff.

After a successful notification, each reader attempted to wake a second peer. This can wake two parked receivers even with a capacity-one channel, because sends can hand directly to waiting receivers without occupying the buffer. Both sends still preceded reference release, preserving the correctness argument.

It was substantially worse in the multicore measurements.

For 512 subscribers, capacity one, and select on every value:

| Variant | Median complete-broadcast time |
|---|---:|
| One handoff, before the experiment | 168.9 µs |
| Up to two handoffs | 534.1 µs |
| Two handoffs, with known-final-reader suppression | 557.1 µs |
| One handoff, repeated afterward | 154.8 µs |

I retained one handoff.

Additional contention is a plausible explanation, but I did not establish the cause with profiling. The measured finding is narrower: **extra wakeup attempts hurt this design in those workloads**, and a repeat of the original configuration recovered its earlier performance.

The prototype diffs and before/after measurements are included. They are excluded from the delivered implementation.

## 9. Correctness evidence and practical limits

The completed verification includes:

- **The entire existing repository test suite:** passed with the addition.
- **The entire repository under the race detector:** passed.
- **The new suite under the race detector:** 25 repetitions each at `GOMAXPROCS=1` and `8`, passed.
- **An independent FIFO-per-subscription model:** 40 deterministic, 2,000-operation traces per normal execution.
- **Model fuzzing:** 174,037 executions without a failure.
- **Directed concurrency tests:** publication ordering, concurrent churn, capacity-notification handoff, cancellation, full-ring late joins, multiple outstanding notification holders, and shutdown while draining.
- **Payload-lifetime tests:** references remain while owed and clear after final receipt or abandonment.
- **A forced delayed-final-reader interleaving:** publisher-assisted reclamation does not cause double retirement.
- **`go vet`:** passed.
- **macOS ARM64 and Linux 386 compilation:** passed.
- **Additive patch application:** checked and verified against the delivered source.

Execution and performance measurements were on Linux AMD64. macOS ARM64 was compile-checked, and the available environment could not execute the Linux 386 binary. The Go 1.23.4 runtime itself was not available for execution.

The operational limits are explicit:

| Property | Limit |
|---|---|
| Total delivery work | Still O(number of recipients) |
| Publication bookkeeping | O(1), with amortized reclamation when helping |
| Ordinary non-final receive | O(1), without the hub mutex |
| Final reclamation | A completed-prefix sweep; O(B) worst individual sweep |
| Subscription close | Bounded by unread backlog and reclamation work |
| Slow subscribers | Eventually backpressure publishers |
| Fairness | No scheduler or publisher-starvation bound |
| Subscriber ownership | One owner per cursor; active cursors must not be copied |
| Send result | Publication count, not receipt acknowledgement |
| Delivery policy | Lossless while subscribed; closing explicitly abandons backlog |

I would use this implementation where **bounded buffering, high subscriber churn, shared payload storage, and selectable independent cursors** are valuable. The existing `ChanPubSub` remains useful when its synchronous receipt barrier is part of the required semantics.

To apply the addition:

```sh
git apply ringpubsub.patch
go test ./...
go test -race -run 'RingPubSub|RingSubscription' ./...
```

The patch adds five Go files. It preserves the existing public APIs and leaves the original implementations unchanged.

### Deliverable

````txtar
-- DESIGN.md --
# RingPubSub: bounded broadcast with reusable selectable notifications

This addition complements `ChanCaster` and `ChanPubSub`. It provides a lossless,
bounded broadcast log with caller-owned cursors, no subscriber registry, no
per-subscriber channel, and no background goroutine. Its ordinary receive path
does not acquire the hub mutex. Construction allocates fixed infrastructure;
publication, consumption, and stack-resident subscription churn allocate no
library objects.

**Semantic difference:** `RingPubSub.Send` acknowledges publication into the
bounded log. It does not acknowledge completed delivery. Its count is the number
of subscriptions active at publication, including subscriptions that later
abandon the value. `ChanPubSub.Send` has a stronger receipt-barrier contract.

## Applying the addition

The supplied patch only adds files. It does not edit either existing primitive,
the module definition, or dependencies. Apply it at the root of `go-bigbuff`:

```sh
git apply ringpubsub.patch
go test ./...
go test -race -run 'RingPubSub|RingSubscription' ./...
```

The accompanying `source/` directory is a standalone, standard-library-only
validation harness. Its `chancaster.go` and `chanpubsub.go` are the supplied
baselines, verified against repository revision
`04f9b9f58d17938686a4d5014f8ed5531c97ac34`. Its minimal `go.mod` is for the
harness; do not replace your repository's module files with it.

The public implementation is entirely in `ringpubsub.go`. The other new Go
files contain tests, examples, and benchmarks. It uses public Go APIs only:
there is no `unsafe`, runtime linkname, spinning receive loop, pool, channel
reopening, or dependency on private runtime layouts.

## Using it

```go
hub := bigbuff.NewRingPubSub[Event](64)
sub := hub.Subscribe()
defer sub.Close()

// In a producer goroutine:
subscribers, err := hub.Send(ctx, event)
// Success means publication; subscribers is NOT a completed-delivery count.

// In the subscription's owner goroutine:
for {
    select {
    case <-ctx.Done():
        return
    case _, open := <-sub.Ready():
        if !open {
            return // the hub is closed and this cursor has drained
        }
        value, ok := sub.TryRecv()
        if !ok {
            panic("subscription contract violated")
        }
        handle(value)
    }
}
```

Evaluate `Ready()` again after advancing the cursor. Each subscription has one
owner, and an active cursor must not be copied. Pass its address when handing
ownership to a goroutine. A successful receive from `Ready()` must promptly be
followed by `TryRecv`, a nonempty `Read`, `Recv`, or `Close`. Choosing another
select case does not consume a wake and needs no repair.

`Recv(ctx)` supplies the usual blocking API. Cancellation preserves the cursor
and its backlog; call `Close` when abandoning it. Cancellation already observed
at the start of an attempt takes precedence over available data. If cancellation
occurs after the attempt starts, successful consumption can win the race.

For throughput, first try `TryRecv` or drain into a reusable caller-owned slice
with `Read(dst)` and enter select only when caught up. There is no need to obtain
a notification before polling. This avoids channel operations for selecting
each already-available value while preserving select support when idle.

`TrySend` returns `ErrRingFull` without publication when full, and
`io.ErrClosedPipe` after hub shutdown. `Send` waits for capacity and supports
context cancellation. With no active subscribers, publication returns zero
without retaining the value. Subscription begins with the next publication;
there is no historical replay.

Hub `Close` is idempotent and permits each active cursor to drain before EOF.
Subscription `Close` is idempotent and abandons its own unread backlog. The
external-package example demonstrates two subscriptions handled by a single
goroutine using select. No broadcast-wide `Wait` is necessary.

## The mechanism

### One extra physical slot prevents generation aliasing

For a capacity of B retained values, the hub allocates B+1 physical slots. Every
slot contains a payload, an atomic remaining-reader count, and a reusable
capacity-one `chan struct{}`. The hub has one additional capacity-one channel
for publisher capacity hints.

Suppose B=3 and three values are outstanding:

| Physical slot | Contents | Cursor or boundary |
|---|---|---|
| 0 | oldest value | slow reader's next value |
| 1 | second value | |
| 2 | newest value | |
| 3 | empty | publication tail; caught-up reader; new subscriber |

A caught-up or newly joined reader must wait on slot 3. With only three physical
slots, that cursor would map to slot 0 and could consume a notification belonging
to an older publication. That would make a raw Go select ambiguous.

The extra slot makes all positions from the oldest outstanding value through the
future tail distinct. A reader's outstanding obligation prevents publication
from lapping it. Channel identity therefore distinguishes the generations that
can currently matter, without allocating a new channel for each generation or
storing an unbounded sequence counter.

Indices advance explicitly and wrap at the physical slice length. There is no
integer sequence rollover to compare, and no assumption that B+1 divides 2^64.

### The channel carries a notification, not the payload

Publication stores V once and snapshots the active subscriber count under the
hub mutex. The publisher initializes the slot and then atomically publishes the
next tail index. It seeds the slot's notification channel once.

Each cursor knows its own next slot. A reader copies that slot's immutable V,
passes on a nonblocking notification, and atomically releases its obligation.
Each receiver still receives its own value copy; sharing the retained payload
does not remove the language-level copy out to each recipient. Pointees are
shared just as they are through ordinary channels, so callers must synchronize
mutations to referenced data.

The notification is a coalesced hint, not a uniquely owned token. Multiple
readers can have consumed notifications while still owing the value. Polling
readers need not have received a notification at all. Correctness rests on the
remaining-reader count and each cursor, rather than token ownership.

### Notification must finish before reference release

The essential receive ordering is:

```go
value = slot.value
signalNonblocking(slot.ready)
remaining := slot.remaining.Add(-1)
advanceCursor()
if remaining == 0 {
    reclaimCurrentCompletedPrefix()
}
```

Sending the notification after the decrement would permit reclamation before
the notification completed. A delayed signal could then contaminate a future
use of the channel. In this implementation the reader's own obligation pins
both the payload and channel generation through the entire signal operation.

The final decrement certifies that every payload copy and every notification
for that generation has finished. A reaper can clear the payload reference,
drain a stale buffered hint, and make the slot reusable. The publisher retains
the mutex through its initial signal so that even extremely fast polling readers
cannot reclaim before that initial signal finishes.

### Ordinary readers avoid the hub mutex

The tail is atomic. Observing publication through the tail makes initialized
slot data visible to a reader. Each read then needs a nonblocking notification
attempt and one atomic decrement. Only a final release enters the hub mutex to
reclaim the completed prefix. A publisher encountering a full ring also helps
reclaim any completed prefix, so it need not await a final reader descheduled
between its atomic decrement and mutex acquisition.

A delayed final reader must examine the **current head**, not retire the old
slot it captured before acquiring the mutex. A helper may already have retired
that slot. The test suite forces this exact interleaving and checks that the
subsequent publication remains intact.

Avoiding the hub mutex is not a lock-free claim. Go channel operations have
their own runtime synchronization; reference counts and notification queues can
still contend. Last-reader reclamation and publication use an ordinary mutex.

### Membership does not need a subscriber registry

Joining only increments the subscriber count and returns a cursor at the tail.
Old slots keep their original recipient counts. Leaving decrements membership
under the publication mutex and releases one obligation in every slot the
cursor still owes. That is at most B slots, irrespective of the number of other
subscribers.

The hub never retains a pointer to a subscription. On the tested 64-bit target a
cursor is two machine words. A zero-size vet marker discourages accidental
copying without introducing a self-pointer that would force heap escape.

### The unused slot also gives orderly shutdown

Shutdown closes the unused tail channel and the publisher-capacity channel.
Occupied data channels remain open, so readers can safely continue passing
notifications while draining. Every cursor eventually reaches the same terminal
tail position and receives EOF from its existing select expression.

There are two channel-close operations and no application-level subscriber
scan. That does not make shutdown constant-time overall: the Go runtime wakes
the goroutines already parked on a closed channel.

### Publisher hints also need handoff

The space channel is a notification, not a reservation. A publisher always
rechecks capacity under the mutex. After a successful publication it relays a
hint if capacity remains. This matters when one cancellation frees several
slots while several publishers are asleep.

A canceled publisher may have consumed the only capacity hint just before
observing cancellation. It repairs the hint under the mutex before returning.
Similarly, a canceled `Recv` that retains its subscription repairs the data
notification if it may have consumed one. These paths are covered explicitly.

## Correctness invariants

1. **Membership and publication are ordered.** The hub mutex orders subscribing,
   unsubscribing, publishing, and closing. Every accepted value receives exactly
   the subscriber count active at its publication boundary.
2. **An active cursor owns a contiguous suffix.** It owes exactly one obligation
   in every publication from its next position through the current tail.
   Consumption advances one position; cancellation releases that suffix.
3. **An occupied slot cannot be reused early.** Outstanding obligations include
   readers that consumed a notification but have not yet copied/released their
   value. At most B published positions occupy B+1 lanes, so their generation
   and the future tail cannot alias for a valid cursor.
4. **The payload stays immutable while read.** Initialization precedes tail
   publication. Value copies and notifications precede atomic decrements.
   The final zero observation precedes clearing and reuse under the hub mutex.
5. **Reclamation is performed once for the current prefix.** Reapers examine
   current head and occupancy. Old captured positions never authorize another
   retirement. Clearing and draining happen before capacity becomes reusable.
6. **EOF cannot close a live data lane.** The final tail is unused. No live
   obligation can authorize a notification to it, and no future publication is
   allowed after it is closed.

The Go memory model's ordering for atomic operations and mutexes supplies the
publication and reclamation synchronization. These are reasoning arguments
about the implementation, supported by tests; they are not a formal proof of
all possible executions or a promise that the runtime scheduler is fair.

## Costs and limits

| Operation | Library work, excluding scheduling and contention |
|---|---|
| Subscribe | O(1) |
| Admitted publication | O(1), with amortized reclamation if helping a full ring |
| Non-final receive | O(1), no hub-mutex acquisition |
| Final receive | O(1) plus completed-prefix sweep; O(B) worst individual sweep |
| Subscription Close | O(unread backlog), bounded by B, plus reclamation |
| Hub Close | Two channel closes; runtime work depends on blocked waiters |
| Hub storage | O(B), independent of current and historical subscriber count |
| Caller cursor storage | O(N); two machine words each on the tested 64-bit target |

The hub-storage bound excludes the Go runtime's wait records and goroutine
stacks. Blocked consumers still need those resources. No application-level
allocation per subscription does not imply constant total process memory as
the subscriber count grows.

The whole broadcast still does O(N) recipient work. A slow or abandoned cursor
eventually blocks publication. There is no drop-oldest mode, automatic eviction,
subscription replay, processing acknowledgement, or fairness bound. A caller
that requires immediate publication completion despite stalled subscribers
needs different delivery semantics.

The extra bounded storage is a deliberate trade: capacity B permits readers to
be up to B messages apart. Capacity one does not have the same throughput as a
large burst allowance. Choose capacity from the application's acceptable lag
and memory budget, not solely a benchmark maximum.

## What is distinctive, and what is prior art

Bounded broadcast rings, receiver cursors, and per-slot reference counts are
established techniques; Tokio's broadcast implementation is one relevant
example. This is not a claim to have invented broadcast ring buffers or to have
proven worldwide novelty.

The distinctive combination here is reusable native Go-select notification
channels, a spare-lane proof that eliminates visible generation aliasing,
reference-pinned notification handoff, registry-free churn, an atomic ordinary
receive path, cooperative reclamation, and EOF encoded in the unused tail lane.
Those pieces support one another. Removing the spare lane or reversing the
signal/release order breaks the reasoning.

## Runtime references

- [Go memory model](https://go.dev/ref/mem): atomic operations, mutex ordering,
  and synchronization of otherwise ordinary payload accesses.
- [Go channel runtime](https://go.dev/src/runtime/chan.go): buffered-channel
  handoff, waiter queues, and work performed while closing a channel.
- [Go select runtime](https://go.dev/src/runtime/select.go): blocked selects
  register runtime waiters on participating channels.
- [Go condition variables](https://go.dev/src/sync/cond.go): the collective
  release and mutex reacquisition used by the original ping/pong design.
- [Tokio broadcast source](https://docs.rs/tokio/latest/src/tokio/sync/broadcast.rs.html):
  prior art for shared retained values, cursors, and bounded broadcast storage.

See `RESULTS.md` for measured results, exact commands, validation outcomes, and
the rejected extra-wakeup experiment. Raw evidence is included with the bundle.
-- evidence/allocation-and-example.txt --
=== RUN   TestRingPubSubZeroSteadyStateAllocations
=== RUN   TestRingPubSubZeroSteadyStateAllocations/subscription_churn
=== RUN   TestRingPubSubZeroSteadyStateAllocations/publication_and_selected_receive
=== RUN   TestRingPubSubZeroSteadyStateAllocations/queued_unsubscribe
=== RUN   TestRingPubSubZeroSteadyStateAllocations/batch_read
--- PASS: TestRingPubSubZeroSteadyStateAllocations (0.00s)
    --- PASS: TestRingPubSubZeroSteadyStateAllocations/subscription_churn (0.00s)
    --- PASS: TestRingPubSubZeroSteadyStateAllocations/publication_and_selected_receive (0.00s)
    --- PASS: TestRingPubSubZeroSteadyStateAllocations/queued_unsubscribe (0.00s)
    --- PASS: TestRingPubSubZeroSteadyStateAllocations/batch_read (0.00s)
=== RUN   ExampleRingPubSub_select
--- PASS: ExampleRingPubSub_select (0.00s)
PASS
ok  	github.com/joeycumines/go-bigbuff	0.006s
-- evidence/bench-final-supplement.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/LinearChannels/B=64/N=512-8         	    4527	     71930 ns/op	  56.94 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8         	    3312	    118990 ns/op	  34.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8         	    4939	     60475 ns/op	  67.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8         	    4852	    206086 ns/op	  19.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8         	    5426	    214417 ns/op	  19.10 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	3.352s
-- evidence/bench-final.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/ChanPubSub/N=1           	  576187	       461.0 ns/op	  17.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1           	  701890	       449.0 ns/op	  17.82 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1           	  628826	       451.3 ns/op	  17.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1           	  730332	       462.9 ns/op	  17.28 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1           	  670142	       452.5 ns/op	  17.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1-8         	  620919	       542.0 ns/op	  14.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1-8         	  657244	       537.4 ns/op	  14.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1-8         	  534307	       483.0 ns/op	  16.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1-8         	  650982	       518.0 ns/op	  15.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1-8         	  490068	       532.6 ns/op	  15.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1         	  693572	       445.6 ns/op	  17.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1         	  621930	       457.5 ns/op	  17.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1         	  658767	       458.4 ns/op	  17.45 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1         	  620352	       483.9 ns/op	  16.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1         	  633280	       454.8 ns/op	  17.59 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1-8       	  517822	       555.3 ns/op	  14.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1-8       	  544486	       541.7 ns/op	  14.77 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1-8       	  506584	       530.0 ns/op	  15.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1-8       	  652065	       515.1 ns/op	  15.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1-8       	  525110	       540.0 ns/op	  14.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1       	  615278	       483.5 ns/op	  16.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1       	  571922	       461.5 ns/op	  17.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1       	  671799	       463.6 ns/op	  17.26 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1       	  647760	       466.5 ns/op	  17.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1       	  650163	       463.0 ns/op	  17.28 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1-8     	  563574	       545.6 ns/op	  14.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1-8     	  620364	       487.1 ns/op	  16.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1-8     	  606505	       507.9 ns/op	  15.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1-8     	  637574	       506.9 ns/op	  15.78 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1-8     	  664778	       503.0 ns/op	  15.91 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1        	 5247152	        63.01 ns/op	 126.96 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1        	 4962604	        57.56 ns/op	 138.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1        	 5238528	        58.23 ns/op	 137.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1        	 5215977	        57.67 ns/op	 138.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1        	 5199242	        59.20 ns/op	 135.13 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1-8      	 4410228	        72.94 ns/op	 109.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1-8      	 3945550	        74.53 ns/op	 107.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1-8      	 3847914	        74.34 ns/op	 107.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1-8      	 4150346	        67.27 ns/op	 118.92 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1-8      	 4597735	        76.02 ns/op	 105.24 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1      	 2266473	       120.1 ns/op	  66.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1      	 2371008	       118.8 ns/op	  67.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1      	 2498978	       122.0 ns/op	  65.59 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1      	 2494965	       122.9 ns/op	  65.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1      	 2567953	       122.6 ns/op	  65.26 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1-8    	 1896300	       150.9 ns/op	  53.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1-8    	 2052882	       153.4 ns/op	  52.17 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1-8    	 1824264	       137.1 ns/op	  58.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1-8    	 1932038	       144.4 ns/op	  55.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1-8    	 2130106	       150.1 ns/op	  53.28 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1   	 1601965	       195.8 ns/op	  40.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1   	 1505214	       211.6 ns/op	  37.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1   	 1426164	       212.5 ns/op	  37.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1   	 1000000	       259.8 ns/op	  30.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1   	 1320250	       229.9 ns/op	  34.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1-8 	 1000000	       266.4 ns/op	  30.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1-8 	 1268420	       253.8 ns/op	  31.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1-8 	 1252695	       218.4 ns/op	  36.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1-8 	 1326957	       234.4 ns/op	  34.13 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1-8 	 1406841	       215.4 ns/op	  37.14 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1           	 4425663	        67.86 ns/op	 117.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1           	 3917940	        75.69 ns/op	 105.70 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1           	 4058962	        73.46 ns/op	 108.90 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1           	 4201375	        70.71 ns/op	 113.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1           	 4071271	        70.96 ns/op	 112.74 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1-8         	 3246232	        87.91 ns/op	  91.00 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1-8         	 3554494	        92.86 ns/op	  86.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1-8         	 3707266	        80.12 ns/op	  99.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1-8         	 3337647	        87.74 ns/op	  91.18 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1-8         	 3761514	        82.15 ns/op	  97.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8                    	  127645	      2061 ns/op	  31.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8                    	  138154	      2302 ns/op	  27.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8                    	  151706	      2088 ns/op	  30.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8                    	  143337	      2076 ns/op	  30.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8                    	  135250	      2291 ns/op	  27.94 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8-8                  	  120470	      4108 ns/op	  15.58 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8-8                  	   82117	      4399 ns/op	  14.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8-8                  	   79290	      4427 ns/op	  14.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8-8                  	   69375	      4369 ns/op	  14.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8-8                  	   86944	      4423 ns/op	  14.47 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8                  	  150600	      2017 ns/op	  31.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8                  	  148666	      2056 ns/op	  31.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8                  	  141678	      2140 ns/op	  29.91 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8                  	  140149	      2288 ns/op	  27.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8                  	  151782	      2147 ns/op	  29.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8-8                	  115854	      2470 ns/op	  25.91 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8-8                	  120787	      2377 ns/op	  26.93 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8-8                	  132727	      2314 ns/op	  27.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8-8                	  136932	      2355 ns/op	  27.17 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8-8                	  120522	      2403 ns/op	  26.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8                	  154898	      2019 ns/op	  31.70 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8                	  136586	      2230 ns/op	  28.70 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8                	  120249	      2619 ns/op	  24.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8                	  119810	      2313 ns/op	  27.67 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8                	  142602	      2236 ns/op	  28.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8-8              	  135536	      2684 ns/op	  23.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8-8              	  125637	      2522 ns/op	  25.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8-8              	  130246	      2465 ns/op	  25.96 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8-8              	  113983	      2419 ns/op	  26.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8-8              	  131469	      2451 ns/op	  26.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8                 	 1970953	       158.1 ns/op	 404.91 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8                 	 1876616	       149.5 ns/op	 428.23 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8                 	 1983404	       166.1 ns/op	 385.36 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8                 	 1772488	       183.3 ns/op	 349.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8                 	 1922515	       154.3 ns/op	 414.78 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8-8               	 1471498	       190.9 ns/op	 335.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8-8               	 1480537	       181.4 ns/op	 352.84 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8-8               	 1451518	       195.3 ns/op	 327.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8-8               	 1635150	       186.3 ns/op	 343.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8-8               	 1631886	       199.7 ns/op	 320.49 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8               	  454237	       678.6 ns/op	  94.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8               	  420703	       682.6 ns/op	  93.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8               	  476974	       690.2 ns/op	  92.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8               	  430992	       667.7 ns/op	  95.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8               	  459300	       683.1 ns/op	  93.70 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8-8             	  468007	       739.8 ns/op	  86.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8-8             	  400428	       741.9 ns/op	  86.26 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8-8             	  439755	       713.1 ns/op	  89.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8-8             	  460816	       748.9 ns/op	  85.45 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8-8             	  388158	       744.1 ns/op	  86.01 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8            	  161262	      1662 ns/op	  38.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8            	  160059	      1885 ns/op	  33.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8            	  179648	      1708 ns/op	  37.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8            	  169398	      1676 ns/op	  38.19 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8            	  174907	      1623 ns/op	  39.43 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8-8          	   89328	      3134 ns/op	  20.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8-8          	  150126	      3046 ns/op	  21.01 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8-8          	   90396	      3082 ns/op	  20.77 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8-8          	  102865	      3030 ns/op	  21.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8-8          	   99708	      3312 ns/op	  19.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8           	  390810	       666.5 ns/op	  96.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8           	  585538	       772.1 ns/op	  82.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8           	  397363	       825.0 ns/op	  77.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8           	  438642	       732.9 ns/op	  87.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8           	  494923	       743.4 ns/op	  86.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8-8         	  319180	      1219 ns/op	  52.51 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8-8         	  250544	      1077 ns/op	  59.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8-8         	  315672	      1070 ns/op	  59.82 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8-8         	  340435	      1013 ns/op	  63.19 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8-8         	  359050	       833.2 ns/op	  76.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64                   	   18211	     14065 ns/op	  36.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64                   	   21176	     13909 ns/op	  36.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64                   	   21574	     14019 ns/op	  36.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64                   	   21757	     16743 ns/op	  30.58 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64                   	   18487	     15979 ns/op	  32.04 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64-8                 	    7288	     44387 ns/op	  11.53 MB/s	       4 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64-8                 	    9718	     47820 ns/op	  10.71 MB/s	       3 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64-8                 	    7894	     43627 ns/op	  11.74 MB/s	       2 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64-8                 	    5760	     45043 ns/op	  11.37 MB/s	       3 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64-8                 	    6322	     47734 ns/op	  10.73 MB/s	       4 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64                 	   15748	     18979 ns/op	  26.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64                 	   13774	     19447 ns/op	  26.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64                 	   16593	     17174 ns/op	  29.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64                 	   17743	     18891 ns/op	  27.10 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64                 	   15133	     16630 ns/op	  30.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8               	   14744	     19897 ns/op	  25.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8               	   15846	     22178 ns/op	  23.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8               	   10000	     25955 ns/op	  19.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8               	   12015	     23664 ns/op	  21.64 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8               	   16730	     18974 ns/op	  26.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64               	   16947	     18144 ns/op	  28.22 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64               	   19026	     17183 ns/op	  29.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64               	   16320	     18850 ns/op	  27.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64               	   16026	     17176 ns/op	  29.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64               	   17244	     21717 ns/op	  23.58 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8             	   14550	     18982 ns/op	  26.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8             	   15012	     19274 ns/op	  26.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8             	   15769	     20226 ns/op	  25.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8             	   13863	     19706 ns/op	  25.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8             	   14476	     18359 ns/op	  27.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64                	  353781	       960.7 ns/op	 532.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64                	  350510	       933.3 ns/op	 548.58 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64                	  309346	       904.4 ns/op	 566.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64                	  352844	       873.6 ns/op	 586.05 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64                	  358237	       823.5 ns/op	 621.72 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8              	  318794	      1090 ns/op	 469.64 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8              	  338000	      1115 ns/op	 459.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8              	  241962	      1144 ns/op	 447.67 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8              	  291316	      1032 ns/op	 495.91 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8              	  245422	      1058 ns/op	 483.96 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64              	   52498	      5372 ns/op	  95.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64              	   57549	      5507 ns/op	  92.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64              	   52533	      6331 ns/op	  80.87 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64              	   43064	      6268 ns/op	  81.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64              	   60133	      5628 ns/op	  90.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8            	   43116	      7624 ns/op	  67.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8            	   43646	      6767 ns/op	  75.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8            	   43813	      6238 ns/op	  82.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8            	   44006	      7073 ns/op	  72.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8            	   48039	      6406 ns/op	  79.92 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64           	   18284	     17220 ns/op	  29.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64           	   18206	     14492 ns/op	  35.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64           	   21328	     15827 ns/op	  32.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64           	   23490	     17940 ns/op	  28.54 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64           	   20583	     15190 ns/op	  33.71 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64-8         	   13368	     33981 ns/op	  15.07 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64-8         	    9254	     37711 ns/op	  13.58 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64-8         	   10276	     38435 ns/op	  13.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64-8         	    7603	     32910 ns/op	  15.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64-8         	   10000	     38209 ns/op	  13.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64          	   51123	      5340 ns/op	  95.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64          	   59028	      4607 ns/op	 111.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64          	   69085	      4556 ns/op	 112.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64          	   66380	      4336 ns/op	 118.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64          	   72393	      4707 ns/op	 108.77 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64-8        	   37298	      8836 ns/op	  57.94 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64-8        	   37747	      9351 ns/op	  54.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64-8        	   38742	      8221 ns/op	  62.28 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64-8        	   35202	      7806 ns/op	  65.59 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64-8        	   33081	      9002 ns/op	  56.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512                  	    1999	    156605 ns/op	  26.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512                  	    1906	    150971 ns/op	  27.13 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512                  	    2329	    127773 ns/op	  32.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512                  	    2044	    152834 ns/op	  26.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512                  	    2199	    133792 ns/op	  30.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512-8                	     334	    887326 ns/op	   4.62 MB/s	      88 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512-8                	     363	    818289 ns/op	   5.01 MB/s	      72 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512-8                	     379	    822996 ns/op	   4.98 MB/s	      85 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512-8                	     390	    931117 ns/op	   4.40 MB/s	     101 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512-8                	     435	    689425 ns/op	   5.94 MB/s	     117 B/op	       1 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512                	    2409	    139329 ns/op	  29.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512                	    2196	    152784 ns/op	  26.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512                	    2570	    129069 ns/op	  31.73 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512                	    2100	    137855 ns/op	  29.71 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512                	    2262	    135826 ns/op	  30.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8              	    1892	    138952 ns/op	  29.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8              	    1942	    149076 ns/op	  27.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8              	    1861	    151460 ns/op	  27.04 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8              	    2056	    149190 ns/op	  27.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8              	    2098	    154798 ns/op	  26.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512              	    1888	    140272 ns/op	  29.20 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512              	    2145	    136192 ns/op	  30.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512              	    2060	    136191 ns/op	  30.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512              	    2310	    145574 ns/op	  28.14 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512              	    1951	    144579 ns/op	  28.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8            	    1909	    163046 ns/op	  25.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8            	    2031	    145445 ns/op	  28.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8            	    2416	    146546 ns/op	  27.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8            	    2283	    138665 ns/op	  29.54 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8            	    1840	    138748 ns/op	  29.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512               	   49737	      6185 ns/op	 662.19 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512               	   48470	      6483 ns/op	 631.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512               	   48007	      6435 ns/op	 636.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512               	   47632	      6043 ns/op	 677.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512               	   49689	      7531 ns/op	 543.90 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8             	   42400	      7264 ns/op	 563.87 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8             	   38886	      7099 ns/op	 577.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8             	   35259	      7440 ns/op	 550.56 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8             	   41989	      8184 ns/op	 500.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8             	   42634	      8019 ns/op	 510.80 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512             	    7042	     41480 ns/op	  98.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512             	    7178	     46402 ns/op	  88.27 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512             	    5839	     47037 ns/op	  87.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512             	    5799	     43451 ns/op	  94.27 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512             	    7507	     46889 ns/op	  87.36 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8           	    5928	     57058 ns/op	  71.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8           	    6324	     49916 ns/op	  82.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8           	    5157	     55917 ns/op	  73.25 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8           	    5749	     55237 ns/op	  74.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8           	    6118	     50718 ns/op	  80.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512          	    2257	    130328 ns/op	  31.43 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512          	    2148	    132327 ns/op	  30.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512          	    2422	    148663 ns/op	  27.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512          	    2264	    144319 ns/op	  28.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512          	    2431	    135429 ns/op	  30.24 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512-8        	    1130	    286372 ns/op	  14.30 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512-8        	    1230	    251780 ns/op	  16.27 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512-8        	    1255	    291256 ns/op	  14.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512-8        	    1184	    256301 ns/op	  15.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512-8        	    1194	    262635 ns/op	  15.60 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512         	    8830	     36337 ns/op	 112.72 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512         	    8209	     38460 ns/op	 106.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512         	    7284	     39697 ns/op	 103.18 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512         	    8425	     37687 ns/op	 108.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512         	    8928	     39193 ns/op	 104.51 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8       	    5443	     62255 ns/op	  65.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8       	    4867	     71079 ns/op	  57.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8       	    4521	     69286 ns/op	  59.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8       	
-- evidence/bench-initial.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/ChanPubSub/N=1           	  233841	       511.4 ns/op	  15.64 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=1-8         	  245534	       519.6 ns/op	  15.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1         	  244401	       506.1 ns/op	  15.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=1-8       	  210800	       672.1 ns/op	  11.90 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1       	  224311	       544.6 ns/op	  14.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=1-8     	  222276	       547.8 ns/op	  14.60 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1        	 1772782	        69.29 ns/op	 115.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=1-8      	 1437043	        81.76 ns/op	  97.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1      	  929589	       124.7 ns/op	  64.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=1-8    	  821738	       150.3 ns/op	  53.23 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1   	  490351	       216.4 ns/op	  36.96 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=1-8 	  470082	       263.9 ns/op	  30.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1           	 1586046	        79.87 ns/op	 100.16 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=1-8         	 1282626	        89.47 ns/op	  89.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8                    	   49328	      2495 ns/op	  25.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=8-8                  	   30339	      4998 ns/op	  12.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8                  	   41152	      2474 ns/op	  25.87 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=8-8                	   51439	      2370 ns/op	  27.00 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8                	   57993	      2047 ns/op	  31.26 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=8-8              	   48440	      2928 ns/op	  21.86 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8                 	  828184	       163.3 ns/op	 392.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=8-8               	  551088	       205.9 ns/op	 310.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8               	  177973	       824.6 ns/op	  77.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=8-8             	  170172	       783.8 ns/op	  81.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8            	   61466	      2063 ns/op	  31.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=8-8          	   30290	      3663 ns/op	  17.47 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8           	  167672	       680.6 ns/op	  94.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=8-8         	  117504	      1000 ns/op	  63.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64                   	    6596	     18643 ns/op	  27.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=64-8                 	    5613	     34809 ns/op	  14.71 MB/s	       9 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64                 	    5510	     20158 ns/op	  25.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8               	    4720	     25044 ns/op	  20.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64               	    7558	     19208 ns/op	  26.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8             	    4665	     28311 ns/op	  18.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64                	  136814	       931.9 ns/op	 549.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8              	  114187	      1122 ns/op	 456.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64              	   20947	      5481 ns/op	  93.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8            	   18163	      7317 ns/op	  69.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64           	    8592	     14234 ns/op	  35.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=64-8         	    6300	     22468 ns/op	  22.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64          	   24693	      5174 ns/op	  98.95 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=64-8        	   13855	      7772 ns/op	  65.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512                  	     786	    162869 ns/op	  25.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/ChanPubSub/N=512-8                	     100	   1029331 ns/op	   3.98 MB/s	     359 B/op	       3 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512                	     691	    160175 ns/op	  25.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8              	     739	    207022 ns/op	  19.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512              	     842	    173606 ns/op	  23.59 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8            	     763	    159946 ns/op	  25.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512               	   18826	      7168 ns/op	 571.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8             	   13195	      8365 ns/op	 489.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512             	    3013	     40518 ns/op	 101.09 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8           	    2419	     51131 ns/op	  80.11 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512          	     738	    138337 ns/op	  29.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=0/N=512-8        	     518	    356836 ns/op	  11.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512         	    2638	     43182 ns/op	  94.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/LinearChannels/B=64/N=512-8       	    2088	     77744 ns/op	  52.69 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	9.683s
-- evidence/bench-large.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcastLarge/ChanPubSub/N=4096-8         	      64	  13339297 ns/op	   2.46 MB/s	     413 B/op	       3 allocs/op
BenchmarkBroadcastLarge/ChanPubSub/N=4096-8         	      57	   9521655 ns/op	   3.44 MB/s	    1269 B/op	      11 allocs/op
BenchmarkBroadcastLarge/ChanPubSub/N=4096-8         	      72	   5670054 ns/op	   5.78 MB/s	     735 B/op	       6 allocs/op
BenchmarkBroadcastLarge/RingFast/B=1/N=4096-8       	     172	   1326605 ns/op	  24.70 MB/s	       2 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=1/N=4096-8       	     157	   1444525 ns/op	  22.68 MB/s	       1 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=1/N=4096-8       	     170	   1323704 ns/op	  24.75 MB/s	       6 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=1/N=4096-8     	     171	   1384804 ns/op	  23.66 MB/s	       2 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=1/N=4096-8     	     154	   1723485 ns/op	  19.01 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=1/N=4096-8     	     152	   1422638 ns/op	  23.03 MB/s	       4 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=64/N=4096-8      	    3603	     69701 ns/op	 470.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=64/N=4096-8      	    3489	     63565 ns/op	 515.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=64/N=4096-8      	    4026	     66154 ns/op	 495.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=64/N=4096-8    	     607	    410481 ns/op	  79.83 MB/s	       2 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=64/N=4096-8    	     636	    385335 ns/op	  85.04 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=64/N=4096-8    	     649	    426853 ns/op	  76.77 MB/s	       2 B/op	       0 allocs/op
BenchmarkBroadcastLarge/LinearChannels/B=64/N=4096-8         	     508	    513294 ns/op	  63.84 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/LinearChannels/B=64/N=4096-8         	     614	    485273 ns/op	  67.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/LinearChannels/B=64/N=4096-8         	     679	    508714 ns/op	  64.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/ChanPubSub/N=16384-8                 	       7	  46165334 ns/op	   2.84 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/ChanPubSub/N=16384-8                 	       6	  53716632 ns/op	   2.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/ChanPubSub/N=16384-8                 	       6	  43868841 ns/op	   2.99 MB/s	    2221 B/op	      19 allocs/op
BenchmarkBroadcastLarge/RingFast/B=1/N=16384-8               	      14	  17213516 ns/op	   7.61 MB/s	      64 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=1/N=16384-8               	      15	  15579030 ns/op	   8.41 MB/s	      59 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=1/N=16384-8               	      14	  16461753 ns/op	   7.96 MB/s	      80 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=1/N=16384-8             	      14	  16193320 ns/op	   8.09 MB/s	      32 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=1/N=16384-8             	      13	  15690673 ns/op	   8.35 MB/s	      68 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=1/N=16384-8             	      13	  17319720 ns/op	   7.57 MB/s	      34 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=64/N=16384-8              	     540	    412169 ns/op	 318.01 MB/s	       1 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=64/N=16384-8              	     464	    457762 ns/op	 286.33 MB/s	       1 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingFast/B=64/N=16384-8              	     510	    406487 ns/op	 322.45 MB/s	       2 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=64/N=16384-8            	     127	   1668530 ns/op	  78.56 MB/s	      26 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=64/N=16384-8            	     117	   2358537 ns/op	  55.57 MB/s	      15 B/op	       0 allocs/op
BenchmarkBroadcastLarge/RingSelect/B=64/N=16384-8            	     141	   2004489 ns/op	  65.39 MB/s	      22 B/op	       0 allocs/op
BenchmarkBroadcastLarge/LinearChannels/B=64/N=16384-8        	      57	   4307528 ns/op	  30.43 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/LinearChannels/B=64/N=16384-8        	      40	   6058488 ns/op	  21.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcastLarge/LinearChannels/B=64/N=16384-8        	      56	   4856767 ns/op	  26.99 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	18.148s
-- evidence/bench-last-skip.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/RingFast/B=1/N=64           	    7417	     20622 ns/op	  24.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	   10000	     15231 ns/op	  33.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	   10000	     16594 ns/op	  30.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    4221	     39230 ns/op	  13.05 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    4083	     40180 ns/op	  12.74 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    4447	     43428 ns/op	  11.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     15901 ns/op	  32.20 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     15306 ns/op	  33.45 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     15420 ns/op	  33.20 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    5396	     44677 ns/op	  11.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    5401	     44946 ns/op	  11.39 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    5912	     46422 ns/op	  11.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  199179	       865.6 ns/op	 591.47 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  217732	      1071 ns/op	 478.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  160555	       973.2 ns/op	 526.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  124465	      1357 ns/op	 377.37 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  134269	      1424 ns/op	 359.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  119894	      1529 ns/op	 334.82 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   34677	      5951 ns/op	  86.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   33660	      5742 ns/op	  89.17 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   33202	      6093 ns/op	  84.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	    9759	     17905 ns/op	  28.60 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   10395	     16533 ns/op	  30.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   10000	     16354 ns/op	  31.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1372	    137199 ns/op	  29.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1218	    130775 ns/op	  31.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1372	    140780 ns/op	  29.10 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     583	    389038 ns/op	  10.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     566	    389444 ns/op	  10.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     751	    533627 ns/op	   7.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1306	    131342 ns/op	  31.19 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1483	    121622 ns/op	  33.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1458	    122298 ns/op	  33.49 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	     489	    482982 ns/op	   8.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	     331	    557114 ns/op	   7.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	     330	    557972 ns/op	   7.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   26248	      6682 ns/op	 613.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   24884	      7045 ns/op	 581.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   26017	      6742 ns/op	 607.54 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   10000	     19779 ns/op	 207.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   10000	     18018 ns/op	 227.32 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   10000	     18982 ns/op	 215.79 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4294	     38988 ns/op	 105.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4843	     51414 ns/op	  79.67 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    3739	     42586 ns/op	  96.18 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    1093	    157749 ns/op	  25.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    1143	    152319 ns/op	  26.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    1117	    155385 ns/op	  26.36 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	10.457s
-- evidence/bench-relay-base-after.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/RingFast/B=1/N=64           	    9136	     18749 ns/op	  27.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	   10000	     19912 ns/op	  25.71 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	    8203	     18596 ns/op	  27.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    8136	     21035 ns/op	  24.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	   10000	     19431 ns/op	  26.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    7814	     20526 ns/op	  24.94 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	    9366	     20804 ns/op	  24.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     15497 ns/op	  33.04 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     15979 ns/op	  32.04 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	   10248	     18411 ns/op	  27.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    9556	     16967 ns/op	  30.18 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	   10000	     18471 ns/op	  27.72 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  223984	       826.5 ns/op	 619.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  201007	       901.9 ns/op	 567.71 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  216822	       884.4 ns/op	 578.90 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  220302	       952.8 ns/op	 537.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  162415	       981.0 ns/op	 521.91 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  245124	       888.3 ns/op	 576.35 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   37022	      4826 ns/op	 106.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   34160	      6449 ns/op	  79.39 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   29796	      5467 ns/op	  93.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   24345	      6205 ns/op	  82.51 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   28957	      6330 ns/op	  80.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   27723	      6116 ns/op	  83.72 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1434	    144731 ns/op	  28.30 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1275	    145506 ns/op	  28.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1224	    128554 ns/op	  31.86 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	    1261	    143928 ns/op	  28.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	    1532	    146930 ns/op	  27.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	    1177	    153953 ns/op	  26.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1269	    134681 ns/op	  30.41 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1314	    136405 ns/op	  30.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1455	    126099 ns/op	  32.48 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	    1238	    155684 ns/op	  26.31 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	    1311	    148772 ns/op	  27.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	    1189	    154829 ns/op	  26.46 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   23055	      6815 ns/op	 601.00 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   27271	      6509 ns/op	 629.30 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   23139	      7005 ns/op	 584.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   24100	      7298 ns/op	 561.26 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   23407	      7373 ns/op	 555.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   26103	      7421 ns/op	 551.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4350	     43807 ns/op	  93.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4053	     45246 ns/op	  90.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    3614	     43774 ns/op	  93.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    4081	     44234 ns/op	  92.60 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    4662	     40682 ns/op	 100.68 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    3590	     48014 ns/op	  85.31 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	10.484s
-- evidence/bench-relay-base.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/RingFast/B=1/N=64           	    7887	     19614 ns/op	  26.10 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	   10708	     15164 ns/op	  33.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	   10000	     15968 ns/op	  32.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	   10096	     19565 ns/op	  26.17 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	   10000	     20983 ns/op	  24.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    8084	     20040 ns/op	  25.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	    9440	     19869 ns/op	  25.77 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     19962 ns/op	  25.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     17401 ns/op	  29.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	   10000	     24349 ns/op	  21.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    8005	     21975 ns/op	  23.30 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    9145	     23392 ns/op	  21.89 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  131647	      1175 ns/op	 435.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  213273	      1008 ns/op	 507.98 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  182137	      1047 ns/op	 489.23 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  121860	      1728 ns/op	 296.33 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	   96121	      2060 ns/op	 248.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  159663	      1372 ns/op	 373.24 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   22496	      8097 ns/op	  63.23 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   31574	      6365 ns/op	  80.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   32905	      5387 ns/op	  95.04 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   25147	      7762 ns/op	  65.96 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   26806	      7615 ns/op	  67.24 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   20282	      7772 ns/op	  65.88 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1316	    135594 ns/op	  30.21 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	     950	    173114 ns/op	  23.66 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1100	    147929 ns/op	  27.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	    1254	    189197 ns/op	  21.65 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	    1053	    197415 ns/op	  20.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     963	    174764 ns/op	  23.44 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1279	    155318 ns/op	  26.37 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1165	    164049 ns/op	  24.97 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1363	    149684 ns/op	  27.36 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	    1126	    179407 ns/op	  22.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	    1066	    162589 ns/op	  25.19 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	    1088	    168924 ns/op	  24.25 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   25782	      7191 ns/op	 569.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   24339	      6941 ns/op	 590.08 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   24428	      7018 ns/op	 583.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   18032	      9059 ns/op	 452.14 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   21832	      9301 ns/op	 440.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   21535	      9016 ns/op	 454.30 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    3759	     43195 ns/op	  94.83 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    3840	     41311 ns/op	  99.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4279	     41838 ns/op	  97.90 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    3670	     55950 ns/op	  73.21 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    4185	     53022 ns/op	  77.25 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    4342	     56123 ns/op	  72.98 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	11.409s
-- evidence/bench-relay.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkBroadcast/RingFast/B=1/N=64           	   10000	     19317 ns/op	  26.51 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	    9982	     17951 ns/op	  28.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64           	    9272	     16963 ns/op	  30.18 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    5350	     44468 ns/op	  11.51 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    4095	     44469 ns/op	  11.51 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=64-8         	    4110	     44538 ns/op	  11.50 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     18797 ns/op	  27.24 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10000	     19993 ns/op	  25.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64         	   10069	     18438 ns/op	  27.77 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    3994	     47844 ns/op	  10.70 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    4750	     38391 ns/op	  13.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=64-8       	    3626	     47606 ns/op	  10.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  207782	       900.5 ns/op	 568.55 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  184056	       982.2 ns/op	 521.28 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64          	  196208	      1017 ns/op	 503.61 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  116197	      2369 ns/op	 216.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  116515	      1562 ns/op	 327.87 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=64-8        	  105690	      1472 ns/op	 347.77 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   31429	      5636 ns/op	  90.84 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   34942	      5202 ns/op	  98.42 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64        	   27888	      5588 ns/op	  91.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   11080	     17485 ns/op	  29.28 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   12902	     16261 ns/op	  31.49 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=64-8      	   10000	     16649 ns/op	  30.75 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1372	    155508 ns/op	  26.34 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1351	    151342 ns/op	  27.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512          	    1300	    127525 ns/op	  32.12 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     423	    601709 ns/op	   6.81 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     327	    596208 ns/op	   6.87 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=1/N=512-8        	     283	    584809 ns/op	   7.00 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1345	    124789 ns/op	  32.82 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1383	    127280 ns/op	  32.18 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512        	    1438	    125307 ns/op	  32.69 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	     478	    474924 ns/op	   8.62 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	     330	    574886 ns/op	   7.12 MB/s	      16 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=1/N=512-8      	     324	    534149 ns/op	   7.67 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   24100	      7060 ns/op	 580.14 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   28455	      6492 ns/op	 630.93 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512         	   26919	      6467 ns/op	 633.37 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   10000	     16685 ns/op	 245.49 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   10000	     18242 ns/op	 224.54 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingFast/B=64/N=512-8       	   10000	     25032 ns/op	 163.63 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4778	     39248 ns/op	 104.36 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    4227	     38149 ns/op	 107.37 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512       	    3811	     41061 ns/op	  99.76 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    1687	    142640 ns/op	  28.72 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    1119	    155122 ns/op	  26.40 MB/s	       0 B/op	       0 allocs/op
BenchmarkBroadcast/RingSelect/B=64/N=512-8     	    1176	    165198 ns/op	  24.79 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	10.983s
-- evidence/bench-resources.txt --
goos: linux
goarch: amd64
pkg: github.com/joeycumines/go-bigbuff
cpu: AMD EPYC 9V74 80-Core Processor                
BenchmarkInlineCallbacks/N=1-8   	79907415	         3.611 ns/op	2215.57 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=1-8   	82731243	         2.859 ns/op	2798.22 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=1-8   	89139435	         3.847 ns/op	2079.53 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=8-8   	10857871	        21.66 ns/op	2955.23 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=8-8   	11464339	        23.27 ns/op	2750.07 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=8-8   	10447873	        25.10 ns/op	2550.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=64-8  	 1000000	       212.7 ns/op	2407.03 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=64-8  	 1000000	       217.4 ns/op	2355.27 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=64-8  	 1000000	       233.9 ns/op	2188.52 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=512-8 	  102735	      1995 ns/op	2052.86 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=512-8 	  128223	      1814 ns/op	2258.59 MB/s	       0 B/op	       0 allocs/op
BenchmarkInlineCallbacks/N=512-8 	  114828	      2044 ns/op	2003.82 MB/s	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/ChanCaster-8         	17029786	        13.64 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/ChanCaster-8         	16413244	        15.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/ChanCaster-8         	17459677	        14.12 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/ChanPubSub-8         	 9565543	        27.05 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/ChanPubSub-8         	 7416582	        28.72 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/ChanPubSub-8         	 7016265	        36.19 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/RingPubSub-8         	12956446	        23.09 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/RingPubSub-8         	11261209	        18.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/RingPubSub-8         	17055208	        17.56 ns/op	       0 B/op	       0 allocs/op
BenchmarkSubscriptionChurn/FreshChannel/B=0-8   	 3909789	        57.34 ns/op	     112 B/op	       1 allocs/op
BenchmarkSubscriptionChurn/FreshChannel/B=0-8   	 5486248	        48.00 ns/op	     112 B/op	       1 allocs/op
BenchmarkSubscriptionChurn/FreshChannel/B=0-8   	 4807527	        53.43 ns/op	     112 B/op	       1 allocs/op
BenchmarkSubscriptionChurn/FreshChannel/B=64-8  	 1000000	       292.9 ns/op	     640 B/op	       1 allocs/op
BenchmarkSubscriptionChurn/FreshChannel/B=64-8  	  978314	       208.2 ns/op	     640 B/op	       1 allocs/op
BenchmarkSubscriptionChurn/FreshChannel/B=64-8  	 1191056	       201.9 ns/op	     640 B/op	       1 allocs/op
BenchmarkRingQueuedLifecycle/B=1-8              	 3144204	        81.52 ns/op	         1.000 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=1-8              	 3014649	        76.97 ns/op	         1.000 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=1-8              	 2884432	        81.33 ns/op	         1.000 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=64-8             	   73520	      2814 ns/op	        64.00 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=64-8             	   78501	      3015 ns/op	        64.00 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=64-8             	   71967	      3219 ns/op	        64.00 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=1024-8           	    5050	     56082 ns/op	      1024 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=1024-8           	    4104	     52618 ns/op	      1024 queued/op	       0 B/op	       0 allocs/op
BenchmarkRingQueuedLifecycle/B=1024-8           	    4658	     60049 ns/op	      1024 queued/op	       0 B/op	       0 allocs/op
BenchmarkConstruction/ChanCaster-8              	 2009709	       100.6 ns/op	     160 B/op	       2 allocs/op
BenchmarkConstruction/ChanCaster-8              	 3744158	        82.72 ns/op	     160 B/op	       2 allocs/op
BenchmarkConstruction/ChanCaster-8              	 2986984	        75.47 ns/op	     160 B/op	       2 allocs/op
BenchmarkConstruction/ChanPubSub-8              	 1000000	       207.5 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruction/ChanPubSub-8              	 1302474	       188.8 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruction/ChanPubSub-8              	 1349871	       159.5 ns/op	     408 B/op	       5 allocs/op
BenchmarkConstruction/RingPubSub/B=1-8          	 1167072	       260.1 ns/op	     464 B/op	       5 allocs/op
BenchmarkConstruction/RingPubSub/B=1-8          	 1000000	       270.7 ns/op	     464 B/op	       5 allocs/op
BenchmarkConstruction/RingPubSub/B=1-8          	 1248798	       259.7 ns/op	     464 B/op	       5 allocs/op
BenchmarkConstruction/RingPubSub/B=64-8         	   59258	      4344 ns/op	    9264 B/op	      68 allocs/op
BenchmarkConstruction/RingPubSub/B=64-8         	   61377	      4325 ns/op	    9264 B/op	      68 allocs/op
BenchmarkConstruction/RingPubSub/B=64-8         	   36471	      5755 ns/op	    9264 B/op	      68 allocs/op
BenchmarkConstruction/RingPubSub/B=1024-8       	    4304	     66937 ns/op	  142256 B/op	    1028 allocs/op
BenchmarkConstruction/RingPubSub/B=1024-8       	    3259	     62693 ns/op	  142256 B/op	    1028 allocs/op
BenchmarkConstruction/RingPubSub/B=1024-8       	    4980	     54365 ns/op	  142256 B/op	    1028 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=1-8  	 3940017	        66.49 ns/op	     120 B/op	       2 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=1-8  	 3970134	        56.15 ns/op	     120 B/op	       2 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=1-8  	 3199441	        70.29 ns/op	     120 B/op	       2 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=1-8 	  725313	       296.8 ns/op	     648 B/op	       2 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=1-8 	  857337	       271.8 ns/op	     648 B/op	       2 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=1-8 	  994322	       254.3 ns/op	     648 B/op	       2 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=64-8 	   83269	      3670 ns/op	    7680 B/op	      65 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=64-8 	   74907	      3305 ns/op	    7680 B/op	      65 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=64-8 	   72672	      3757 ns/op	    7680 B/op	      65 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=64-8         	   14970	     14134 ns/op	   41472 B/op	      65 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=64-8         	   13876	     22068 ns/op	   41472 B/op	      65 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=64-8         	   14504	     14469 ns/op	   41472 B/op	      65 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=512-8         	   10168	     32998 ns/op	   62208 B/op	     513 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=512-8         	    6957	     32455 ns/op	   62208 B/op	     513 allocs/op
BenchmarkConstruction/LinearChannels/B=0/N=512-8         	    7304	     39778 ns/op	   62208 B/op	     513 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=512-8        	    1537	    194200 ns/op	  332547 B/op	     513 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=512-8        	    1344	    197449 ns/op	  332547 B/op	     513 allocs/op
BenchmarkConstruction/LinearChannels/B=64/N=512-8        	    1298	    380106 ns/op	  332547 B/op	     513 allocs/op
PASS
ok  	github.com/joeycumines/go-bigbuff	23.341s
-- evidence/benchmark-summary.json --
{
  "main": {
    "BenchmarkBroadcast/ChanPubSub/N=1": {
      "samples": [
        {
          "iterations": 576187,
          "ns/op": 461.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 701890,
          "ns/op": 449.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 628826,
          "ns/op": 451.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 730332,
          "ns/op": 462.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 670142,
          "ns/op": 452.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 452.5,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 449.0,
      "max_ns": 462.9
    },
    "BenchmarkBroadcast/ChanPubSub/N=1-8": {
      "samples": [
        {
          "iterations": 620919,
          "ns/op": 542.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 657244,
          "ns/op": 537.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 534307,
          "ns/op": 483.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 650982,
          "ns/op": 518.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 490068,
          "ns/op": 532.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 532.6,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 483.0,
      "max_ns": 542.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=1": {
      "samples": [
        {
          "iterations": 693572,
          "ns/op": 445.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 621930,
          "ns/op": 457.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 658767,
          "ns/op": 458.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 620352,
          "ns/op": 483.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 633280,
          "ns/op": 454.8,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 457.5,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 445.6,
      "max_ns": 483.9
    },
    "BenchmarkBroadcast/RingFast/B=1/N=1-8": {
      "samples": [
        {
          "iterations": 517822,
          "ns/op": 555.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 544486,
          "ns/op": 541.7,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 506584,
          "ns/op": 530.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 652065,
          "ns/op": 515.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 525110,
          "ns/op": 540.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 540.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 515.1,
      "max_ns": 555.3
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=1": {
      "samples": [
        {
          "iterations": 615278,
          "ns/op": 483.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 571922,
          "ns/op": 461.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 671799,
          "ns/op": 463.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 647760,
          "ns/op": 466.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 650163,
          "ns/op": 463.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 463.6,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 461.5,
      "max_ns": 483.5
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=1-8": {
      "samples": [
        {
          "iterations": 563574,
          "ns/op": 545.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 620364,
          "ns/op": 487.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 606505,
          "ns/op": 507.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 637574,
          "ns/op": 506.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 664778,
          "ns/op": 503.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 506.9,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 487.1,
      "max_ns": 545.6
    },
    "BenchmarkBroadcast/RingFast/B=64/N=1": {
      "samples": [
        {
          "iterations": 5247152,
          "ns/op": 63.01,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4962604,
          "ns/op": 57.56,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5238528,
          "ns/op": 58.23,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5215977,
          "ns/op": 57.67,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5199242,
          "ns/op": 59.2,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 58.23,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 57.56,
      "max_ns": 63.01
    },
    "BenchmarkBroadcast/RingFast/B=64/N=1-8": {
      "samples": [
        {
          "iterations": 4410228,
          "ns/op": 72.94,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3945550,
          "ns/op": 74.53,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3847914,
          "ns/op": 74.34,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4150346,
          "ns/op": 67.27,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4597735,
          "ns/op": 76.02,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 74.34,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 67.27,
      "max_ns": 76.02
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=1": {
      "samples": [
        {
          "iterations": 2266473,
          "ns/op": 120.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2371008,
          "ns/op": 118.8,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2498978,
          "ns/op": 122.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2494965,
          "ns/op": 122.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2567953,
          "ns/op": 122.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 122.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 118.8,
      "max_ns": 122.9
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=1-8": {
      "samples": [
        {
          "iterations": 1896300,
          "ns/op": 150.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2052882,
          "ns/op": 153.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1824264,
          "ns/op": 137.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1932038,
          "ns/op": 144.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2130106,
          "ns/op": 150.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 150.1,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 137.1,
      "max_ns": 153.4
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=1": {
      "samples": [
        {
          "iterations": 1601965,
          "ns/op": 195.8,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1505214,
          "ns/op": 211.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1426164,
          "ns/op": 212.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1000000,
          "ns/op": 259.8,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1320250,
          "ns/op": 229.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 212.5,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 195.8,
      "max_ns": 259.8
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=1-8": {
      "samples": [
        {
          "iterations": 1000000,
          "ns/op": 266.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1268420,
          "ns/op": 253.8,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1252695,
          "ns/op": 218.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1326957,
          "ns/op": 234.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1406841,
          "ns/op": 215.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 234.4,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 215.4,
      "max_ns": 266.4
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=1": {
      "samples": [
        {
          "iterations": 4425663,
          "ns/op": 67.86,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3917940,
          "ns/op": 75.69,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4058962,
          "ns/op": 73.46,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4201375,
          "ns/op": 70.71,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4071271,
          "ns/op": 70.96,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 70.96,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 67.86,
      "max_ns": 75.69
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=1-8": {
      "samples": [
        {
          "iterations": 3246232,
          "ns/op": 87.91,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3554494,
          "ns/op": 92.86,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3707266,
          "ns/op": 80.12,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3337647,
          "ns/op": 87.74,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3761514,
          "ns/op": 82.15,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 87.74,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 80.12,
      "max_ns": 92.86
    },
    "BenchmarkBroadcast/ChanPubSub/N=8": {
      "samples": [
        {
          "iterations": 127645,
          "ns/op": 2061.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 138154,
          "ns/op": 2302.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 151706,
          "ns/op": 2088.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 143337,
          "ns/op": 2076.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 135250,
          "ns/op": 2291.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 2088.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2061.0,
      "max_ns": 2302.0
    },
    "BenchmarkBroadcast/ChanPubSub/N=8-8": {
      "samples": [
        {
          "iterations": 120470,
          "ns/op": 4108.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 82117,
          "ns/op": 4399.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 79290,
          "ns/op": 4427.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 69375,
          "ns/op": 4369.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 86944,
          "ns/op": 4423.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 4399.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 4108.0,
      "max_ns": 4427.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=8": {
      "samples": [
        {
          "iterations": 150600,
          "ns/op": 2017.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 148666,
          "ns/op": 2056.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 141678,
          "ns/op": 2140.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 140149,
          "ns/op": 2288.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 151782,
          "ns/op": 2147.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 2140.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2017.0,
      "max_ns": 2288.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=8-8": {
      "samples": [
        {
          "iterations": 115854,
          "ns/op": 2470.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 120787,
          "ns/op": 2377.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 132727,
          "ns/op": 2314.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 136932,
          "ns/op": 2355.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 120522,
          "ns/op": 2403.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 2377.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2314.0,
      "max_ns": 2470.0
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=8": {
      "samples": [
        {
          "iterations": 154898,
          "ns/op": 2019.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 136586,
          "ns/op": 2230.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 120249,
          "ns/op": 2619.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 119810,
          "ns/op": 2313.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 142602,
          "ns/op": 2236.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 2236.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2019.0,
      "max_ns": 2619.0
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=8-8": {
      "samples": [
        {
          "iterations": 135536,
          "ns/op": 2684.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 125637,
          "ns/op": 2522.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 130246,
          "ns/op": 2465.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 113983,
          "ns/op": 2419.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 131469,
          "ns/op": 2451.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 2465.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2419.0,
      "max_ns": 2684.0
    },
    "BenchmarkBroadcast/RingFast/B=64/N=8": {
      "samples": [
        {
          "iterations": 1970953,
          "ns/op": 158.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1876616,
          "ns/op": 149.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1983404,
          "ns/op": 166.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1772488,
          "ns/op": 183.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1922515,
          "ns/op": 154.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 158.1,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 149.5,
      "max_ns": 183.3
    },
    "BenchmarkBroadcast/RingFast/B=64/N=8-8": {
      "samples": [
        {
          "iterations": 1471498,
          "ns/op": 190.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1480537,
          "ns/op": 181.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1451518,
          "ns/op": 195.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1635150,
          "ns/op": 186.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1631886,
          "ns/op": 199.7,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 190.9,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 181.4,
      "max_ns": 199.7
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=8": {
      "samples": [
        {
          "iterations": 454237,
          "ns/op": 678.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 420703,
          "ns/op": 682.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 476974,
          "ns/op": 690.2,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 430992,
          "ns/op": 667.7,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 459300,
          "ns/op": 683.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 682.6,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 667.7,
      "max_ns": 690.2
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=8-8": {
      "samples": [
        {
          "iterations": 468007,
          "ns/op": 739.8,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 400428,
          "ns/op": 741.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 439755,
          "ns/op": 713.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 460816,
          "ns/op": 748.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 388158,
          "ns/op": 744.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 741.9,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 713.1,
      "max_ns": 748.9
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=8": {
      "samples": [
        {
          "iterations": 161262,
          "ns/op": 1662.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 160059,
          "ns/op": 1885.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 179648,
          "ns/op": 1708.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 169398,
          "ns/op": 1676.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 174907,
          "ns/op": 1623.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 1676.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 1623.0,
      "max_ns": 1885.0
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=8-8": {
      "samples": [
        {
          "iterations": 89328,
          "ns/op": 3134.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 150126,
          "ns/op": 3046.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 90396,
          "ns/op": 3082.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 102865,
          "ns/op": 3030.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 99708,
          "ns/op": 3312.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 3082.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 3030.0,
      "max_ns": 3312.0
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=8": {
      "samples": [
        {
          "iterations": 390810,
          "ns/op": 666.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 585538,
          "ns/op": 772.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 397363,
          "ns/op": 825.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 438642,
          "ns/op": 732.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 494923,
          "ns/op": 743.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 743.4,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 666.5,
      "max_ns": 825.0
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=8-8": {
      "samples": [
        {
          "iterations": 319180,
          "ns/op": 1219.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 250544,
          "ns/op": 1077.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 315672,
          "ns/op": 1070.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 340435,
          "ns/op": 1013.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 359050,
          "ns/op": 833.2,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 1070.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 833.2,
      "max_ns": 1219.0
    },
    "BenchmarkBroadcast/ChanPubSub/N=64": {
      "samples": [
        {
          "iterations": 18211,
          "ns/op": 14065.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 21176,
          "ns/op": 13909.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 21574,
          "ns/op": 14019.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 21757,
          "ns/op": 16743.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 18487,
          "ns/op": 15979.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 14065.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 13909.0,
      "max_ns": 16743.0
    },
    "BenchmarkBroadcast/ChanPubSub/N=64-8": {
      "samples": [
        {
          "iterations": 7288,
          "ns/op": 44387.0,
          "B/op": 4.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 9718,
          "ns/op": 47820.0,
          "B/op": 3.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7894,
          "ns/op": 43627.0,
          "B/op": 2.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5760,
          "ns/op": 45043.0,
          "B/op": 3.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 6322,
          "ns/op": 47734.0,
          "B/op": 4.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 45043.0,
        "B/op": 3.0,
        "allocs/op": 0.0
      },
      "min_ns": 43627.0,
      "max_ns": 47820.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=64": {
      "samples": [
        {
          "iterations": 15748,
          "ns/op": 18979.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 13774,
          "ns/op": 19447.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 16593,
          "ns/op": 17174.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 17743,
          "ns/op": 18891.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 15133,
          "ns/op": 16630.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 18891.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 16630.0,
      "max_ns": 19447.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=64-8": {
      "samples": [
        {
          "iterations": 14744,
          "ns/op": 19897.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 15846,
          "ns/op": 22178.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 10000,
          "ns/op": 25955.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 12015,
          "ns/op": 23664.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 16730,
          "ns/op": 18974.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 22178.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 18974.0,
      "max_ns": 25955.0
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=64": {
      "samples": [
        {
          "iterations": 16947,
          "ns/op": 18144.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 19026,
          "ns/op": 17183.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 16320,
          "ns/op": 18850.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 16026,
          "ns/op": 17176.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 17244,
          "ns/op": 21717.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 18144.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 17176.0,
      "max_ns": 21717.0
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=64-8": {
      "samples": [
        {
          "iterations": 14550,
          "ns/op": 18982.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 15012,
          "ns/op": 19274.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 15769,
          "ns/op": 20226.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 13863,
          "ns/op": 19706.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 14476,
          "ns/op": 18359.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 19274.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 18359.0,
      "max_ns": 20226.0
    },
    "BenchmarkBroadcast/RingFast/B=64/N=64": {
      "samples": [
        {
          "iterations": 353781,
          "ns/op": 960.7,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 350510,
          "ns/op": 933.3,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 309346,
          "ns/op": 904.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 352844,
          "ns/op": 873.6,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 358237,
          "ns/op": 823.5,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 904.4,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 823.5,
      "max_ns": 960.7
    },
    "BenchmarkBroadcast/RingFast/B=64/N=64-8": {
      "samples": [
        {
          "iterations": 318794,
          "ns/op": 1090.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 338000,
          "ns/op": 1115.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 241962,
          "ns/op": 1144.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 291316,
          "ns/op": 1032.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 245422,
          "ns/op": 1058.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 1090.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 1032.0,
      "max_ns": 1144.0
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=64": {
      "samples": [
        {
          "iterations": 52498,
          "ns/op": 5372.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 57549,
          "ns/op": 5507.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 52533,
          "ns/op": 6331.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 43064,
          "ns/op": 6268.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 60133,
          "ns/op": 5628.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 5628.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 5372.0,
      "max_ns": 6331.0
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=64-8": {
      "samples": [
        {
          "iterations": 43116,
          "ns/op": 7624.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 43646,
          "ns/op": 6767.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 43813,
          "ns/op": 6238.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 44006,
          "ns/op": 7073.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 48039,
          "ns/op": 6406.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 6767.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 6238.0,
      "max_ns": 7624.0
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=64": {
      "samples": [
        {
          "iterations": 18284,
          "ns/op": 17220.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 18206,
          "ns/op": 14492.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 21328,
          "ns/op": 15827.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 23490,
          "ns/op": 17940.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 20583,
          "ns/op": 15190.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 15827.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 14492.0,
      "max_ns": 17940.0
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=64-8": {
      "samples": [
        {
          "iterations": 13368,
          "ns/op": 33981.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 9254,
          "ns/op": 37711.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 10276,
          "ns/op": 38435.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7603,
          "ns/op": 32910.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 10000,
          "ns/op": 38209.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 37711.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 32910.0,
      "max_ns": 38435.0
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=64": {
      "samples": [
        {
          "iterations": 51123,
          "ns/op": 5340.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 59028,
          "ns/op": 4607.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 69085,
          "ns/op": 4556.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 66380,
          "ns/op": 4336.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 72393,
          "ns/op": 4707.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 4607.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 4336.0,
      "max_ns": 5340.0
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=64-8": {
      "samples": [
        {
          "iterations": 37298,
          "ns/op": 8836.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 37747,
          "ns/op": 9351.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 38742,
          "ns/op": 8221.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 35202,
          "ns/op": 7806.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 33081,
          "ns/op": 9002.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 8836.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 7806.0,
      "max_ns": 9351.0
    },
    "BenchmarkBroadcast/ChanPubSub/N=512": {
      "samples": [
        {
          "iterations": 1999,
          "ns/op": 156605.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1906,
          "ns/op": 150971.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2329,
          "ns/op": 127773.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2044,
          "ns/op": 152834.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2199,
          "ns/op": 133792.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 150971.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 127773.0,
      "max_ns": 156605.0
    },
    "BenchmarkBroadcast/ChanPubSub/N=512-8": {
      "samples": [
        {
          "iterations": 334,
          "ns/op": 887326.0,
          "B/op": 88.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 363,
          "ns/op": 818289.0,
          "B/op": 72.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 379,
          "ns/op": 822996.0,
          "B/op": 85.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 390,
          "ns/op": 931117.0,
          "B/op": 101.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 435,
          "ns/op": 689425.0,
          "B/op": 117.0,
          "allocs/op": 1.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 822996.0,
        "B/op": 88.0,
        "allocs/op": 0.0
      },
      "min_ns": 689425.0,
      "max_ns": 931117.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=512": {
      "samples": [
        {
          "iterations": 2409,
          "ns/op": 139329.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2196,
          "ns/op": 152784.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2570,
          "ns/op": 129069.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2100,
          "ns/op": 137855.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2262,
          "ns/op": 135826.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 137855.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 129069.0,
      "max_ns": 152784.0
    },
    "BenchmarkBroadcast/RingFast/B=1/N=512-8": {
      "samples": [
        {
          "iterations": 1892,
          "ns/op": 138952.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1942,
          "ns/op": 149076.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1861,
          "ns/op": 151460.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2056,
          "ns/op": 149190.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2098,
          "ns/op": 154798.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 149190.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 138952.0,
      "max_ns": 154798.0
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=512": {
      "samples": [
        {
          "iterations": 1888,
          "ns/op": 140272.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2145,
          "ns/op": 136192.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2060,
          "ns/op": 136191.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2310,
          "ns/op": 145574.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1951,
          "ns/op": 144579.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 140272.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 136191.0,
      "max_ns": 145574.0
    },
    "BenchmarkBroadcast/RingSelect/B=1/N=512-8": {
      "samples": [
        {
          "iterations": 1909,
          "ns/op": 163046.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2031,
          "ns/op": 145445.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2416,
          "ns/op": 146546.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2283,
          "ns/op": 138665.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1840,
          "ns/op": 138748.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 145445.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 138665.0,
      "max_ns": 163046.0
    },
    "BenchmarkBroadcast/RingFast/B=64/N=512": {
      "samples": [
        {
          "iterations": 49737,
          "ns/op": 6185.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 48470,
          "ns/op": 6483.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 48007,
          "ns/op": 6435.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 47632,
          "ns/op": 6043.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 49689,
          "ns/op": 7531.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 6435.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 6043.0,
      "max_ns": 7531.0
    },
    "BenchmarkBroadcast/RingFast/B=64/N=512-8": {
      "samples": [
        {
          "iterations": 42400,
          "ns/op": 7264.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 38886,
          "ns/op": 7099.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 35259,
          "ns/op": 7440.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 41989,
          "ns/op": 8184.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 42634,
          "ns/op": 8019.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 7440.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 7099.0,
      "max_ns": 8184.0
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=512": {
      "samples": [
        {
          "iterations": 7042,
          "ns/op": 41480.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7178,
          "ns/op": 46402.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5839,
          "ns/op": 47037.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5799,
          "ns/op": 43451.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7507,
          "ns/op": 46889.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 46402.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 41480.0,
      "max_ns": 47037.0
    },
    "BenchmarkBroadcast/RingSelect/B=64/N=512-8": {
      "samples": [
        {
          "iterations": 5928,
          "ns/op": 57058.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 6324,
          "ns/op": 49916.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5157,
          "ns/op": 55917.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5749,
          "ns/op": 55237.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 6118,
          "ns/op": 50718.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 55237.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 49916.0,
      "max_ns": 57058.0
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=512": {
      "samples": [
        {
          "iterations": 2257,
          "ns/op": 130328.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2148,
          "ns/op": 132327.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2422,
          "ns/op": 148663.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2264,
          "ns/op": 144319.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2431,
          "ns/op": 135429.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 135429.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 130328.0,
      "max_ns": 148663.0
    },
    "BenchmarkBroadcast/LinearChannels/B=0/N=512-8": {
      "samples": [
        {
          "iterations": 1130,
          "ns/op": 286372.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1230,
          "ns/op": 251780.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1255,
          "ns/op": 291256.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1184,
          "ns/op": 256301.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1194,
          "ns/op": 262635.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 262635.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 251780.0,
      "max_ns": 291256.0
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=512": {
      "samples": [
        {
          "iterations": 8830,
          "ns/op": 36337.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 8209,
          "ns/op": 38460.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7284,
          "ns/op": 39697.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 8425,
          "ns/op": 37687.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 8928,
          "ns/op": 39193.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 38460.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 36337.0,
      "max_ns": 39697.0
    },
    "BenchmarkBroadcast/LinearChannels/B=64/N=512-8": {
      "samples": [
        {
          "iterations": 4527,
          "ns/op": 71930.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3312,
          "ns/op": 118990.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4939,
          "ns/op": 60475.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4852,
          "ns/op": 206086.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 5426,
          "ns/op": 214417.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 5,
      "median": {
        "ns/op": 118990.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 60475.0,
      "max_ns": 214417.0
    }
  },
  "large": {
    "BenchmarkBroadcastLarge/ChanPubSub/N=4096-8": {
      "samples": [
        {
          "iterations": 64,
          "ns/op": 13339297.0,
          "B/op": 413.0,
          "allocs/op": 3.0
        },
        {
          "iterations": 57,
          "ns/op": 9521655.0,
          "B/op": 1269.0,
          "allocs/op": 11.0
        },
        {
          "iterations": 72,
          "ns/op": 5670054.0,
          "B/op": 735.0,
          "allocs/op": 6.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 9521655.0,
        "B/op": 735.0,
        "allocs/op": 6.0
      },
      "min_ns": 5670054.0,
      "max_ns": 13339297.0
    },
    "BenchmarkBroadcastLarge/RingFast/B=1/N=4096-8": {
      "samples": [
        {
          "iterations": 172,
          "ns/op": 1326605.0,
          "B/op": 2.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 157,
          "ns/op": 1444525.0,
          "B/op": 1.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 170,
          "ns/op": 1323704.0,
          "B/op": 6.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 1326605.0,
        "B/op": 2.0,
        "allocs/op": 0.0
      },
      "min_ns": 1323704.0,
      "max_ns": 1444525.0
    },
    "BenchmarkBroadcastLarge/RingSelect/B=1/N=4096-8": {
      "samples": [
        {
          "iterations": 171,
          "ns/op": 1384804.0,
          "B/op": 2.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 154,
          "ns/op": 1723485.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 152,
          "ns/op": 1422638.0,
          "B/op": 4.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 1422638.0,
        "B/op": 2.0,
        "allocs/op": 0.0
      },
      "min_ns": 1384804.0,
      "max_ns": 1723485.0
    },
    "BenchmarkBroadcastLarge/RingFast/B=64/N=4096-8": {
      "samples": [
        {
          "iterations": 3603,
          "ns/op": 69701.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3489,
          "ns/op": 63565.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4026,
          "ns/op": 66154.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 66154.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 63565.0,
      "max_ns": 69701.0
    },
    "BenchmarkBroadcastLarge/RingSelect/B=64/N=4096-8": {
      "samples": [
        {
          "iterations": 607,
          "ns/op": 410481.0,
          "B/op": 2.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 636,
          "ns/op": 385335.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 649,
          "ns/op": 426853.0,
          "B/op": 2.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 410481.0,
        "B/op": 2.0,
        "allocs/op": 0.0
      },
      "min_ns": 385335.0,
      "max_ns": 426853.0
    },
    "BenchmarkBroadcastLarge/LinearChannels/B=64/N=4096-8": {
      "samples": [
        {
          "iterations": 508,
          "ns/op": 513294.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 614,
          "ns/op": 485273.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 679,
          "ns/op": 508714.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 508714.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 485273.0,
      "max_ns": 513294.0
    },
    "BenchmarkBroadcastLarge/ChanPubSub/N=16384-8": {
      "samples": [
        {
          "iterations": 7,
          "ns/op": 46165334.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 6,
          "ns/op": 53716632.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 6,
          "ns/op": 43868841.0,
          "B/op": 2221.0,
          "allocs/op": 19.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 46165334.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 43868841.0,
      "max_ns": 53716632.0
    },
    "BenchmarkBroadcastLarge/RingFast/B=1/N=16384-8": {
      "samples": [
        {
          "iterations": 14,
          "ns/op": 17213516.0,
          "B/op": 64.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 15,
          "ns/op": 15579030.0,
          "B/op": 59.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 14,
          "ns/op": 16461753.0,
          "B/op": 80.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 16461753.0,
        "B/op": 64.0,
        "allocs/op": 0.0
      },
      "min_ns": 15579030.0,
      "max_ns": 17213516.0
    },
    "BenchmarkBroadcastLarge/RingSelect/B=1/N=16384-8": {
      "samples": [
        {
          "iterations": 14,
          "ns/op": 16193320.0,
          "B/op": 32.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 13,
          "ns/op": 15690673.0,
          "B/op": 68.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 13,
          "ns/op": 17319720.0,
          "B/op": 34.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 16193320.0,
        "B/op": 34.0,
        "allocs/op": 0.0
      },
      "min_ns": 15690673.0,
      "max_ns": 17319720.0
    },
    "BenchmarkBroadcastLarge/RingFast/B=64/N=16384-8": {
      "samples": [
        {
          "iterations": 540,
          "ns/op": 412169.0,
          "B/op": 1.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 464,
          "ns/op": 457762.0,
          "B/op": 1.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 510,
          "ns/op": 406487.0,
          "B/op": 2.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 412169.0,
        "B/op": 1.0,
        "allocs/op": 0.0
      },
      "min_ns": 406487.0,
      "max_ns": 457762.0
    },
    "BenchmarkBroadcastLarge/RingSelect/B=64/N=16384-8": {
      "samples": [
        {
          "iterations": 127,
          "ns/op": 1668530.0,
          "B/op": 26.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 117,
          "ns/op": 2358537.0,
          "B/op": 15.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 141,
          "ns/op": 2004489.0,
          "B/op": 22.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 2004489.0,
        "B/op": 22.0,
        "allocs/op": 0.0
      },
      "min_ns": 1668530.0,
      "max_ns": 2358537.0
    },
    "BenchmarkBroadcastLarge/LinearChannels/B=64/N=16384-8": {
      "samples": [
        {
          "iterations": 57,
          "ns/op": 4307528.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 40,
          "ns/op": 6058488.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 56,
          "ns/op": 4856767.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 4856767.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 4307528.0,
      "max_ns": 6058488.0
    }
  },
  "resources": {
    "BenchmarkInlineCallbacks/N=1-8": {
      "samples": [
        {
          "iterations": 79907415,
          "ns/op": 3.611,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 82731243,
          "ns/op": 2.859,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 89139435,
          "ns/op": 3.847,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 3.611,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2.859,
      "max_ns": 3.847
    },
    "BenchmarkInlineCallbacks/N=8-8": {
      "samples": [
        {
          "iterations": 10857871,
          "ns/op": 21.66,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 11464339,
          "ns/op": 23.27,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 10447873,
          "ns/op": 25.1,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 23.27,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 21.66,
      "max_ns": 25.1
    },
    "BenchmarkInlineCallbacks/N=64-8": {
      "samples": [
        {
          "iterations": 1000000,
          "ns/op": 212.7,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1000000,
          "ns/op": 217.4,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 1000000,
          "ns/op": 233.9,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 217.4,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 212.7,
      "max_ns": 233.9
    },
    "BenchmarkInlineCallbacks/N=512-8": {
      "samples": [
        {
          "iterations": 102735,
          "ns/op": 1995.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 128223,
          "ns/op": 1814.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 114828,
          "ns/op": 2044.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 1995.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 1814.0,
      "max_ns": 2044.0
    },
    "BenchmarkSubscriptionChurn/ChanCaster-8": {
      "samples": [
        {
          "iterations": 17029786,
          "ns/op": 13.64,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 16413244,
          "ns/op": 15.51,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 17459677,
          "ns/op": 14.12,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 14.12,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 13.64,
      "max_ns": 15.51
    },
    "BenchmarkSubscriptionChurn/ChanPubSub-8": {
      "samples": [
        {
          "iterations": 9565543,
          "ns/op": 27.05,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7416582,
          "ns/op": 28.72,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 7016265,
          "ns/op": 36.19,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 28.72,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 27.05,
      "max_ns": 36.19
    },
    "BenchmarkSubscriptionChurn/RingPubSub-8": {
      "samples": [
        {
          "iterations": 12956446,
          "ns/op": 23.09,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 11261209,
          "ns/op": 18.51,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 17055208,
          "ns/op": 17.56,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 18.51,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 17.56,
      "max_ns": 23.09
    },
    "BenchmarkSubscriptionChurn/FreshChannel/B=0-8": {
      "samples": [
        {
          "iterations": 3909789,
          "ns/op": 57.34,
          "B/op": 112.0,
          "allocs/op": 1.0
        },
        {
          "iterations": 5486248,
          "ns/op": 48.0,
          "B/op": 112.0,
          "allocs/op": 1.0
        },
        {
          "iterations": 4807527,
          "ns/op": 53.43,
          "B/op": 112.0,
          "allocs/op": 1.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 53.43,
        "B/op": 112.0,
        "allocs/op": 1.0
      },
      "min_ns": 48.0,
      "max_ns": 57.34
    },
    "BenchmarkSubscriptionChurn/FreshChannel/B=64-8": {
      "samples": [
        {
          "iterations": 1000000,
          "ns/op": 292.9,
          "B/op": 640.0,
          "allocs/op": 1.0
        },
        {
          "iterations": 978314,
          "ns/op": 208.2,
          "B/op": 640.0,
          "allocs/op": 1.0
        },
        {
          "iterations": 1191056,
          "ns/op": 201.9,
          "B/op": 640.0,
          "allocs/op": 1.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 208.2,
        "B/op": 640.0,
        "allocs/op": 1.0
      },
      "min_ns": 201.9,
      "max_ns": 292.9
    },
    "BenchmarkRingQueuedLifecycle/B=1-8": {
      "samples": [
        {
          "iterations": 3144204,
          "ns/op": 81.52,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 3014649,
          "ns/op": 76.97,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 2884432,
          "ns/op": 81.33,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 81.33,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 76.97,
      "max_ns": 81.52
    },
    "BenchmarkRingQueuedLifecycle/B=64-8": {
      "samples": [
        {
          "iterations": 73520,
          "ns/op": 2814.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 78501,
          "ns/op": 3015.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 71967,
          "ns/op": 3219.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 3015.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 2814.0,
      "max_ns": 3219.0
    },
    "BenchmarkRingQueuedLifecycle/B=1024-8": {
      "samples": [
        {
          "iterations": 5050,
          "ns/op": 56082.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4104,
          "ns/op": 52618.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        },
        {
          "iterations": 4658,
          "ns/op": 60049.0,
          "B/op": 0.0,
          "allocs/op": 0.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 56082.0,
        "B/op": 0.0,
        "allocs/op": 0.0
      },
      "min_ns": 52618.0,
      "max_ns": 60049.0
    },
    "BenchmarkConstruction/ChanCaster-8": {
      "samples": [
        {
          "iterations": 2009709,
          "ns/op": 100.6,
          "B/op": 160.0,
          "allocs/op": 2.0
        },
        {
          "iterations": 3744158,
          "ns/op": 82.72,
          "B/op": 160.0,
          "allocs/op": 2.0
        },
        {
          "iterations": 2986984,
          "ns/op": 75.47,
          "B/op": 160.0,
          "allocs/op": 2.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 82.72,
        "B/op": 160.0,
        "allocs/op": 2.0
      },
      "min_ns": 75.47,
      "max_ns": 100.6
    },
    "BenchmarkConstruction/ChanPubSub-8": {
      "samples": [
        {
          "iterations": 1000000,
          "ns/op": 207.5,
          "B/op": 408.0,
          "allocs/op": 5.0
        },
        {
          "iterations": 1302474,
          "ns/op": 188.8,
          "B/op": 408.0,
          "allocs/op": 5.0
        },
        {
          "iterations": 1349871,
          "ns/op": 159.5,
          "B/op": 408.0,
          "allocs/op": 5.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 188.8,
        "B/op": 408.0,
        "allocs/op": 5.0
      },
      "min_ns": 159.5,
      "max_ns": 207.5
    },
    "BenchmarkConstruction/RingPubSub/B=1-8": {
      "samples": [
        {
          "iterations": 1167072,
          "ns/op": 260.1,
          "B/op": 464.0,
          "allocs/op": 5.0
        },
        {
          "iterations": 1000000,
          "ns/op": 270.7,
          "B/op": 464.0,
          "allocs/op": 5.0
        },
        {
          "iterations": 1248798,
          "ns/op": 259.7,
          "B/op": 464.0,
          "allocs/op": 5.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 260.1,
        "B/op": 464.0,
        "allocs/op": 5.0
      },
      "min_ns": 259.7,
      "max_ns": 270.7
    },
    "BenchmarkConstruction/RingPubSub/B=64-8": {
      "samples": [
        {
          "iterations": 59258,
          "ns/op": 4344.0,
          "B/op": 9264.0,
          "allocs/op": 68.0
        },
        {
          "iterations": 61377,
          "ns/op": 4325.0,
          "B/op": 9264.0,
          "allocs/op": 68.0
        },
        {
          "iterations": 36471,
          "ns/op": 5755.0,
          "B/op": 9264.0,
          "allocs/op": 68.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 4344.0,
        "B/op": 9264.0,
        "allocs/op": 68.0
      },
      "min_ns": 4325.0,
      "max_ns": 5755.0
    },
    "BenchmarkConstruction/RingPubSub/B=1024-8": {
      "samples": [
        {
          "iterations": 4304,
          "ns/op": 66937.0,
          "B/op": 142256.0,
          "allocs/op": 1028.0
        },
        {
          "iterations": 3259,
          "ns/op": 62693.0,
          "B/op": 142256.0,
          "allocs/op": 1028.0
        },
        {
          "iterations": 4980,
          "ns/op": 54365.0,
          "B/op": 142256.0,
          "allocs/op": 1028.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 62693.0,
        "B/op": 142256.0,
        "allocs/op": 1028.0
      },
      "min_ns": 54365.0,
      "max_ns": 66937.0
    },
    "BenchmarkConstruction/LinearChannels/B=0/N=1-8": {
      "samples": [
        {
          "iterations": 3940017,
          "ns/op": 66.49,
          "B/op": 120.0,
          "allocs/op": 2.0
        },
        {
          "iterations": 3970134,
          "ns/op": 56.15,
          "B/op": 120.0,
          "allocs/op": 2.0
        },
        {
          "iterations": 3199441,
          "ns/op": 70.29,
          "B/op": 120.0,
          "allocs/op": 2.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 66.49,
        "B/op": 120.0,
        "allocs/op": 2.0
      },
      "min_ns": 56.15,
      "max_ns": 70.29
    },
    "BenchmarkConstruction/LinearChannels/B=64/N=1-8": {
      "samples": [
        {
          "iterations": 725313,
          "ns/op": 296.8,
          "B/op": 648.0,
          "allocs/op": 2.0
        },
        {
          "iterations": 857337,
          "ns/op": 271.8,
          "B/op": 648.0,
          "allocs/op": 2.0
        },
        {
          "iterations": 994322,
          "ns/op": 254.3,
          "B/op": 648.0,
          "allocs/op": 2.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 271.8,
        "B/op": 648.0,
        "allocs/op": 2.0
      },
      "min_ns": 254.3,
      "max_ns": 296.8
    },
    "BenchmarkConstruction/LinearChannels/B=0/N=64-8": {
      "samples": [
        {
          "iterations": 83269,
          "ns/op": 3670.0,
          "B/op": 7680.0,
          "allocs/op": 65.0
        },
        {
          "iterations": 74907,
          "ns/op": 3305.0,
          "B/op": 7680.0,
          "allocs/op": 65.0
        },
        {
          "iterations": 72672,
          "ns/op": 3757.0,
          "B/op": 7680.0,
          "allocs/op": 65.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 3670.0,
        "B/op": 7680.0,
        "allocs/op": 65.0
      },
      "min_ns": 3305.0,
      "max_ns": 3757.0
    },
    "BenchmarkConstruction/LinearChannels/B=64/N=64-8": {
      "samples": [
        {
          "iterations": 14970,
          "ns/op": 14134.0,
          "B/op": 41472.0,
          "allocs/op": 65.0
        },
        {
          "iterations": 13876,
          "ns/op": 22068.0,
          "B/op": 41472.0,
          "allocs/op": 65.0
        },
        {
          "iterations": 14504,
          "ns/op": 14469.0,
          "B/op": 41472.0,
          "allocs/op": 65.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 14469.0,
        "B/op": 41472.0,
        "allocs/op": 65.0
      },
      "min_ns": 14134.0,
      "max_ns": 22068.0
    },
    "BenchmarkConstruction/LinearChannels/B=0/N=512-8": {
      "samples": [
        {
          "iterations": 10168,
          "ns/op": 32998.0,
          "B/op": 62208.0,
          "allocs/op": 513.0
        },
        {
          "iterations": 6957,
          "ns/op": 32455.0,
          "B/op": 62208.0,
          "allocs/op": 513.0
        },
        {
          "iterations": 7304,
          "ns/op": 39778.0,
          "B/op": 62208.0,
          "allocs/op": 513.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 32998.0,
        "B/op": 62208.0,
        "allocs/op": 513.0
      },
      "min_ns": 32455.0,
      "max_ns": 39778.0
    },
    "BenchmarkConstruction/LinearChannels/B=64/N=512-8": {
      "samples": [
        {
          "iterations": 1537,
          "ns/op": 194200.0,
          "B/op": 332547.0,
          "allocs/op": 513.0
        },
        {
          "iterations": 1344,
          "ns/op": 197449.0,
          "B/op": 332547.0,
          "allocs/op": 513.0
        },
        {
          "iterations": 1298,
          "ns/op": 380106.0,
          "B/op": 332547.0,
          "allocs/op": 513.0
        }
      ],
      "count": 3,
      "median": {
        "ns/op": 197449.0,
        "B/op": 332547.0,
        "allocs/op": 513.0
      },
      "min_ns": 194200.0,
      "max_ns": 380106.0
    }
  }
}
-- evidence/build-386.txt --
-- evidence/build-darwin-arm64.txt --
-- evidence/fuzz.txt --
fuzz: elapsed: 0s, gathering baseline coverage: 0/3 completed
fuzz: elapsed: 0s, gathering baseline coverage: 3/3 completed, now fuzzing with 4 workers
fuzz: elapsed: 3s, execs: 16439 (5471/sec), new interesting: 57 (total: 60)
fuzz: elapsed: 6s, execs: 16439 (0/sec), new interesting: 57 (total: 60)
fuzz: elapsed: 9s, execs: 16439 (0/sec), new interesting: 57 (total: 60)
fuzz: elapsed: 12s, execs: 16439 (0/sec), new interesting: 57 (total: 60)
fuzz: elapsed: 15s, execs: 174037 (52568/sec), new interesting: 74 (total: 77)
fuzz: elapsed: 18s, execs: 174037 (0/sec), new interesting: 74 (total: 77)
fuzz: elapsed: 21s, execs: 174037 (0/sec), new interesting: 74 (total: 77)
fuzz: elapsed: 21s, execs: 174037 (0/sec), new interesting: 74 (total: 77)
PASS
ok  	github.com/joeycumines/go-bigbuff	21.018s
-- evidence/integration-race.txt --
ok  	github.com/joeycumines/go-bigbuff	78.183s
-- evidence/integration-source-manifest.json --
{
  "repository": "joeycumines/go-bigbuff",
  "revision": "04f9b9f58d17938686a4d5014f8ed5531c97ac34",
  "dependency": {
    "repository": "go-test/deep",
    "version": "v1.1.1",
    "revision": "9e863ff5cf900f95c7e8a5e8248300939257744b",
    "moduleChecksum": "h1:0r/53hagsehfO4bzD2Pgr/+RgHqhmf+k1Bpse2cTu1U=",
    "goModChecksum": "h1:5C2ZWiW0ErCdrYzpqxLbTX7MG14M9iiw8DgHncVwcsE="
  },
  "files": [
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/.gitignore",
      "sha": "d1d5f202b9fc25f8d0b1e3cf862c78e1ed4adcdd",
      "size": 221
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/LICENSE",
      "sha": "abb5b85fb46151e513961d02418c2056ef4a1db2",
      "size": 11344
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/README.md",
      "sha": "c02646254391db38314646cde456d9b28c8a1e71",
      "size": 3850
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/attempt.go",
      "sha": "9d73b3d6e2beecff8d071ee07592e6b1e4079f6f",
      "size": 2564
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/attempt_test.go",
      "sha": "9db78b08ed7ec1dfd2a2928b4f8ebed4fe83557a",
      "size": 5297
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/bigbuff.go",
      "sha": "cb60f0cb09343f20dfa06f45d65f9de8cbf5389a",
      "size": 16183
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/bigbuff_test.go",
      "sha": "2d9c407c5f7a5555428610f363bf1f94570c0bcb",
      "size": 13499
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/buffer.go",
      "sha": "7791fd6214700c6c9d69aedfd702131a58df3191",
      "size": 16821
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/buffer_test.go",
      "sha": "bfce4ca3029b371127859545cb8ee589c60ac9b2",
      "size": 13868
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/callable.go",
      "sha": "c96d46d93e9f3e257730d0b4eb8f8206c0b7cf95",
      "size": 8616
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/callable_test.go",
      "sha": "394ef2f46ad56f9a3c9c1660670d343c9690b9b4",
      "size": 14775
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/chancaster.go",
      "sha": "05901f5b3a8bd2e8552c6709583db5a9b804033b",
      "size": 9684
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/chancaster_test.go",
      "sha": "454ab5546ebb65f87293240ce65e6f1d634ad2b8",
      "size": 31389
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/channel.go",
      "sha": "cbc6496fcece715dac4f52c6193880a16f29918c",
      "size": 6267
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/channel_test.go",
      "sha": "5c13338323113eb7b3640b5d786334740661ba14",
      "size": 22638
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/chanpubsub.go",
      "sha": "5d2a695c394975684561d33a14fec86e757caefc",
      "size": 15028
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/chanpubsub_test.go",
      "sha": "11580d51b6e7c5cfd5bf9a98bef3030d67f720cc",
      "size": 41247
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/consumer.go",
      "sha": "b9baf39dc582dc1d57598ba223ab319cdc7cb617",
      "size": 2958
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/consumer_test.go",
      "sha": "611060196926bd4e1c5bbdf518361b62298e0fe2",
      "size": 10712
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/context.go",
      "sha": "b0e79c2c94fcb3a2c0d4464387d76890af08e162",
      "size": 5487
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/context_test.go",
      "sha": "6617c81a88085a0da7a8b1f1a2ccf87d210afde8",
      "size": 21302
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/exclusive.go",
      "sha": "71ef6b2306b61a6485c638e7257bda590085c6aa",
      "size": 10548
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/exclusive_test.go",
      "sha": "bb62335c995a311fa90ae440a65b68b786620d68",
      "size": 23291
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/go.mod",
      "sha": "32602ee5756686bdf9cc9a14c3ea15cf2b798442",
      "size": 92
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/go.sum",
      "sha": "c52af7383b0173360b91a1a97e516e8790d38b1b",
      "size": 165
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/iter.go",
      "sha": "b369ea01914b2e6438d651274c7a0e87f0e04b45",
      "size": 1195
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/iter_test.go",
      "sha": "0198e18557d3502388225ccef47fe0f5d7dd431a",
      "size": 11129
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/notifier.go",
      "sha": "a1a99440e4cb482cc57e892388d1c5c8991f1a86",
      "size": 7331
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/notifier_test.go",
      "sha": "cf0e66e408b0b82ec3b40ae2b193c2850dc5f546",
      "size": 10623
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/retry.go",
      "sha": "c771046c5ca8113ded027dc432f247a681e2e941",
      "size": 3300
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/retry_test.go",
      "sha": "8e592cca0ae3307e3ca2bd046998ca2308c70fd3",
      "size": 8232
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/sync.go",
      "sha": "67ce91903494f0c3bc4f86a5d128fe8356654e0c",
      "size": 1997
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/sync_test.go",
      "sha": "fc42d15f8f474a201d8abe2bf9d9867c46ee701d",
      "size": 4790
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/util_test.go",
      "sha": "ad17d8ec0aaab2c7b5c91fcb83589ce489734ff0",
      "size": 3823
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/worker.go",
      "sha": "b2327737c2947b76120bd685b22b163a5cb71644",
      "size": 2124
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/worker_test.go",
      "sha": "1b9461d771d54d2218f33976b6679052bffbe538",
      "size": 2720
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/workers.go",
      "sha": "cf59ec88a485861f4045278d0846c232fcb216f4",
      "size": 2851
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/integration-repo/workers_test.go",
      "sha": "67c6eb32a1a42f1a6d71e581c21598ef8c606f90",
      "size": 3323
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/.github/workflows/codeql-analysis.yml",
      "sha": "2c9ebc468331197a9ff48cdf4eb7d46034998afc",
      "size": 2444
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/.github/workflows/go.yml",
      "sha": "c4b41d800964af08d3047bd2c9098b8ed445ca0b",
      "size": 1008
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/.gitignore",
      "sha": "53f12f0f0e813770f45ed049a680c325d9b7f6db",
      "size": 12
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/CHANGES.md",
      "sha": "163d6d5cb5185b012c3e58a9f98abc42fcb9637b",
      "size": 2293
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/LICENSE",
      "sha": "228ef16f74e89a812de8b2cc4e4d7e8efd52c5c9",
      "size": 1072
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/README.md",
      "sha": "08a86070d1a5646b3c6601b13e089ebb954cb0ee",
      "size": 1675
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/SECURITY.md",
      "sha": "d007137ad1d0847ad6272b0b1d8aea6ac116ba51",
      "size": 540
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/deep.go",
      "sha": "4be3e1f4e6ad52c429bc670bc2f239a9fcce1e44",
      "size": 14500
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/deep_test.go",
      "sha": "74377e1c9c2bf6b250c4d276bdeb8319033da155",
      "size": 34226
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/go.mod",
      "sha": "021ffba076a233efb5790acc36dfc008f5209d7d",
      "size": 40
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/go.sum",
      "sha": "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
      "size": 0
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/test/coverage",
      "sha": "e328944e93ed86f82ee5e522395d143aeb4784fc",
      "size": 83
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/test/v1/errors.go",
      "sha": "385be0a3932627073fedd63358cb71fa2c739407",
      "size": 84
    },
    {
      "path": "/workspace/scratch/46fc9b7c7d39/research/deep-v1.1.1/test/v2/errors.go",
      "sha": "385be0a3932627073fedd63358cb71fa2c739407",
      "size": 84
    }
  ]
}
-- evidence/integration-test.txt --
ok  	github.com/joeycumines/go-bigbuff	50.928s
-- evidence/last-skip-experiment.patch --
--- a/ringpubsub.go
+++ b/ringpubsub.go
@@ -49,9 +49,8 @@
 // background goroutine. Construction allocates a fixed ring and capacity+1
 // reusable notification channels. Subscribe returns a small caller-owned value;
 // whether that value escapes to the heap depends on its caller. The methods do
-// not create per-publication, per-receive, or per-subscription infrastructure.
-// Escape analysis, large value temporaries, runtime parking, and caller-owned
-// values can still allocate. See the allocation tests and benchmarks.
+// not allocate per publication, receive, or subscription lifecycle themselves.
+// Runtime parking and caller-owned values can still allocate.
 //
 // Publishers and distinct subscriptions may operate concurrently. Each individual
 // subscription has one owner; see RingSubscription for its select contract.
@@ -278,7 +277,9 @@
 	// immutable value and its notification channel's generation until we are
 	// completely finished with them. In particular, a delayed signal can never
 	// leak into a later reuse of this slot.
-	ringPubSubSignal(slot.ready)
+	if slot.remaining.Load() > 1 {
+		ringPubSubRelay(slot.ready)
+	}
 	remaining := slot.remaining.Add(-1)
 	s.next = x.advance(s.next)
 	if remaining == 0 {
@@ -325,7 +326,7 @@
 				// returning. A pending obligation pins an open data lane;
 				// a caught-up cursor names an empty or terminal lane instead.
 				if x := s.hub; x != nil && s.next != int(x.tail.Load()) {
-					ringPubSubSignal(x.slots[s.next].ready)
+					ringPubSubRelay(x.slots[s.next].ready)
 				}
 				return value, err
 			}
@@ -361,7 +362,9 @@
 		slot := &x.slots[s.next]
 		// Repair a notification even if Close follows a selected wakeup.
 		// As in TryRecv, our obligation protects this signal from slot reuse.
-		ringPubSubSignal(slot.ready)
+		if slot.remaining.Load() > 1 {
+			ringPubSubRelay(slot.ready)
+		}
 		slot.remaining.Add(-1)
 		s.next = x.advance(s.next)
 	}
@@ -423,3 +426,14 @@
 	default:
 	}
 }
+
+// ringPubSubRelay permits two parked readers to make progress from one wake.
+// A full channel already carries a hint, so skip the second attempt when the
+// first cannot send. Both attempts precede releasing the caller's obligation.
+func ringPubSubRelay(c chan struct{}) {
+	select {
+	case c <- struct{}{}:
+		ringPubSubSignal(c)
+	default:
+	}
+}
-- evidence/race-final.txt --
ok  	github.com/joeycumines/go-bigbuff	6.257s
-- evidence/relay-experiment.patch --
--- a/ringpubsub.go
+++ b/ringpubsub.go
@@ -49,9 +49,8 @@
 // background goroutine. Construction allocates a fixed ring and capacity+1
 // reusable notification channels. Subscribe returns a small caller-owned value;
 // whether that value escapes to the heap depends on its caller. The methods do
-// not create per-publication, per-receive, or per-subscription infrastructure.
-// Escape analysis, large value temporaries, runtime parking, and caller-owned
-// values can still allocate. See the allocation tests and benchmarks.
+// not allocate per publication, receive, or subscription lifecycle themselves.
+// Runtime parking and caller-owned values can still allocate.
 //
 // Publishers and distinct subscriptions may operate concurrently. Each individual
 // subscription has one owner; see RingSubscription for its select contract.
@@ -278,7 +277,7 @@
 	// immutable value and its notification channel's generation until we are
 	// completely finished with them. In particular, a delayed signal can never
 	// leak into a later reuse of this slot.
-	ringPubSubSignal(slot.ready)
+	ringPubSubRelay(slot.ready)
 	remaining := slot.remaining.Add(-1)
 	s.next = x.advance(s.next)
 	if remaining == 0 {
@@ -325,7 +324,7 @@
 				// returning. A pending obligation pins an open data lane;
 				// a caught-up cursor names an empty or terminal lane instead.
 				if x := s.hub; x != nil && s.next != int(x.tail.Load()) {
-					ringPubSubSignal(x.slots[s.next].ready)
+					ringPubSubRelay(x.slots[s.next].ready)
 				}
 				return value, err
 			}
@@ -361,7 +360,7 @@
 		slot := &x.slots[s.next]
 		// Repair a notification even if Close follows a selected wakeup.
 		// As in TryRecv, our obligation protects this signal from slot reuse.
-		ringPubSubSignal(slot.ready)
+		ringPubSubRelay(slot.ready)
 		slot.remaining.Add(-1)
 		s.next = x.advance(s.next)
 	}
@@ -423,3 +422,14 @@
 	default:
 	}
 }
+
+// ringPubSubRelay permits two parked readers to make progress from one wake.
+// A full channel already carries a hint, so skip the second attempt when the
+// first cannot send. Both attempts precede releasing the caller's obligation.
+func ringPubSubRelay(c chan struct{}) {
+	select {
+	case c <- struct{}{}:
+		ringPubSubSignal(c)
+	default:
+	}
+}
-- evidence/test-386.txt --
fork/exec /tmp/go-build1676770273/b001/go-bigbuff.test: exec format error
FAIL	github.com/joeycumines/go-bigbuff	0.000s
FAIL
-- evidence/test-last-skip.txt --
ok  	github.com/joeycumines/go-bigbuff	1.310s
-- evidence/test-relay.txt --
ok  	github.com/joeycumines/go-bigbuff	1.378s
-- evidence/vet.txt --
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
-- README.md --
# go-bigbuff RingPubSub addition

Start with DESIGN.md for the API, mechanism, correctness argument, and tradeoffs.
RESULTS.md contains measurements and verification details.

- `ringpubsub.patch`: additive patch for the real repository (five new Go files).
- `source/`: standalone implementation, tests, examples, and supplied baselines.
- `evidence/`: raw benchmark and test results, structured measurement summary,
  source provenance, and the rejected wake-amplification experiment diffs.

Apply the patch at your go-bigbuff checkout root with `git apply ringpubsub.patch`.
Do not copy the harness's minimal go.mod over your repository's go.mod.

The Mac checkout path supplied in the request was not mounted in the execution
environment. This bundle is a tested implementation and additive patch, not a
claim that the Mac checkout or a remote Git branch was modified.
-- RESULTS.md --
# RingPubSub validation and measurements

Date: 5 November 2026 (Australia/Brisbane).

## Environment and interpretation

Executed with **Go 1.27.3, Linux/AMD64**, on a shared environment reporting an AMD
EPYC 9V74 host and an eight-CPU quota. GOMAXPROCS was explicitly set through
`go test -cpu=1,8`. The module language version remains Go 1.23.4. Neither the
Go 1.23.4 runtime nor macOS execution was available. macOS ARM64 and Linux 386
were compile-checked; a Linux 386 execution attempt returned `exec format error`.

The benchmark value is uint64. Each receiver checks every value's order and
computes a checksum. **The timed region includes delivery of every value to every
receiver and the final drain**, not merely admission into the ring. Constructor
work, goroutine creation, warmup, and final unsubscription are outside it.
Main rows are medians of five 250 ms samples; larger fan-outs are medians of three
200 ms samples. Timings measure aggregate throughput per complete broadcast,
not the distribution of individual message delivery latencies.

`LinearChannels` is a synthetic fixed-slice loop sending directly to one channel
per receiver, with no registry or callback-dispatch overhead. It is an optimistic
channel-fanout control, **not a reconstruction of an unspecified callback
implementation**. Its capacity 64 is per receiver; the ring's capacity 64 is one
shared log. RingSelect executes a two-case select for every value. RingFast polls
first, then executes the same select when caught up. Each select includes a live
cancellation channel.

The capture of the final main-matrix case ended with only three observations and
an incomplete line. That entire case, LinearChannels/B=64/N=512 at GOMAXPROCS=8,
was rerun for five samples. The table uses those five new observations; both raw
captures are preserved. This control was particularly noisy: the rerun ranged
from 60.475 to 214.417 microseconds. Do not treat its median as a portable ratio.

## Complete broadcasts, GOMAXPROCS=8

All entries are **microseconds per complete broadcast**.

| Subscribers | ChanPubSub | Ring: select every value, B=64 | Ring: poll then select, B=64 | Linear channels, B=64 each |
|---:|---:|---:|---:|---:|
| 1 | 0.533 | 0.150 | 0.074 | 0.088 |
| 8 | 4.399 | 0.742 | 0.191 | 1.070 |
| 64 | 45.043 | 6.767 | 1.090 | 8.836 |
| 512 | 822.996 | 55.237 | 7.440 | 118.990 |
| 4,096 | 9,521.655 | 410.481 | 66.154 | 508.714 |
| 16,384 | 46,165.334 | 2,004.489 | 412.169 | 4,856.767 |

At N=64, RingSelect/B=64 took 6.238–7.624 us across the five samples, versus
43.627–47.820 us for ChanPubSub and 7.806–9.351 us for LinearChannels/B=64.
At N=512, RingSelect/B=64 took 49.916–57.058 us, versus 689.425–931.117 us for
ChanPubSub. These measured throughput differences incorporate deliberately
different buffering/barrier semantics; they are not a claim that an individual
lossless synchronous broadcast became universally that much cheaper.

## Complete broadcasts, GOMAXPROCS=1

All entries are microseconds per complete broadcast.

| Subscribers | ChanPubSub | Ring: select every value, B=64 | Ring: poll then select, B=64 | Linear channels, B=64 each |
|---:|---:|---:|---:|---:|
| 1 | 0.453 | 0.122 | 0.058 | 0.071 |
| 8 | 2.088 | 0.683 | 0.158 | 0.743 |
| 64 | 14.065 | 5.628 | 0.904 | 4.607 |
| 512 | 150.971 | 46.402 | 6.435 | 38.460 |

More execution threads do not automatically improve these workloads. Shared
channel queues and reference counts still contend, and scheduler/cache behavior
matters. Capacity one also limits useful overlap. The raw matrix includes B=1
for both ring consumption modes and unbuffered per-subscriber channels.

## Allocation and construction

All RingFast and RingSelect observations in the main matrix (N=1 through 512)
reported **0 B/op and 0 allocs/op**. Separate `testing.AllocsPerRun` assertions
passed for stack-resident subscription churn, selected consumption, queued
abandonment, and batch reading.

The large fan-out runs did record small nonzero byte rates: for example,
RingSelect/B=64 at N=16,384 had a median of **22 B/op**, while rounded allocs/op was
zero. That does **not** establish exact zero allocations for the whole process.
Runtime wait records, stack growth, escape analysis, and application-owned
objects are outside the guarantee of allocating no per-message infrastructure.
The source clears pointer-bearing payloads after the final obligation is released.

| Infrastructure | Bytes allocated per construction | Allocations |
|---|---:|---:|
| ChanCaster, supplied channel included | 160 | 2 |
| ChanPubSub, supplied channel included | 408 | 5 |
| RingPubSub, B=1 | 464 | 5 |
| RingPubSub, B=64 | 9,264 | 68 |
| RingPubSub, B=1024 | 142,256 | 1,028 |
| 64 unbuffered per-subscriber channels + slice | 7,680 | 65 |
| 64 per-subscriber channels, B=64 + slice | 41,472 | 65 |
| 512 per-subscriber channels, B=64 + slice | 332,547 | 513 |

Ring infrastructure is independent of subscriber count. Caller-owned cursors
(two machine words on the measured 64-bit target), goroutines, stacks, and runtime
wait records remain additional costs. ChanPubSub's very small constant shared
infrastructure remains valuable. RingPubSub deliberately spends more fixed memory
to permit bounded reader lag and eliminate the collective receive barrier.

| Empty subscription lifecycle | Median ns | B/op | allocs/op |
|---|---:|---:|---:|
| ChanCaster | 14.12 | 0 | 0 |
| ChanPubSub | 28.72 | 0 | 0 |
| RingPubSub | 18.51 | 0 | 0 |
| FreshChannel/B=0 | 53.43 | 112 | 1 |
| FreshChannel/B=64 | 208.20 | 640 | 1 |

Fresh-channel churn is only a lower bound on a per-subscriber-channel lifecycle:
it excludes registry changes, context setup, callbacks, and goroutine creation.
Queued-lifecycle measurements include Subscribe, all publications, and Close;
they are not isolated cancellation-latency measurements.

## Rejected wake-amplification experiment

A prototype retained capacity-one channels but, after the first successful
notification, attempted a second send to wake another already-parked receiver.
This is possible because a send can hand directly to a waiting receiver without
filling the buffer. The second attempt was skipped if the first found a queued
notification. Both attempts still preceded reference release, preserving the
correctness argument. A second prototype also skipped the relay when its own
obligation was the last one. Neither prototype is included in the production code.

At N=512, B=1, RingSelect, GOMAXPROCS=8, three-sample medians were:

| Variant | Median us/broadcast |
|---|---:|
| Single handoff, before experiments | 168.924 |
| Up to two handoffs | 534.149 |
| Up to two handoffs, skip known final obligation | 557.114 |
| Single handoff, repeated afterward | 154.829 |

The extra wakeups were rejected. These measurements support keeping one handoff
in this implementation; they do not by themselves isolate a particular cache or
scheduler mechanism. The suspected added contention is an inference, not a
profiled causal finding. The raw before/after data and prototype source diffs
are included so the result can be reproduced.

The separate inline-callback benchmark is a sequential dispatch floor. At N=64
it took a median 217.4 ns, and at N=512 1,995 ns. Those callbacks run synchronously
inside the producer goroutine and provide neither independent receivers nor the
native select behavior under test; they are not interchangeable with a broadcast
queue.

## Correctness validation

- Original complete repository suite with the additive files: PASS, 50.928 s.
- Complete repository under the race detector: PASS, 78.183 s.
- New test suite under the race detector, 25 repetitions each at
  GOMAXPROCS=1 and 8: PASS, 6.257 s.
- Independent FIFO-per-subscription model: 40 deterministic 2,000-operation
  traces per normal test execution, plus a fuzz target.
- Model fuzzing: PASS, 174,037 executions in 21.018 s, with four workers.
- Directed tests cover spare-lane aliasing, joining on a full ring, multiple
  outstanding notifications, selected notification followed by Close or canceled
  Recv, closed-hub draining, multi-publisher ordering and capacity handoff,
  concurrent churn, payload clearing, and helper reclamation while the final
  reader is deliberately paused.
- The external-package example consumes two subscriptions in one goroutine
  through select and verifies both ordered sequences.
- `go vet ./...`: PASS.
- macOS ARM64 and Linux 386 test-binary compilation: PASS. Neither target was
  executed; the 386 execution attempt is preserved as an environment limitation.

The existing repository snapshot is pinned to
`04f9b9f58d17938686a4d5014f8ed5531c97ac34`. All 38 source files were verified against
their Git blob hashes. The local integration copy of `github.com/go-test/deep`
v1.1.1 was verified against both module checksums in the original go.sum. A
temporary modfile enabled offline integration testing without changing the
repository's go.mod or go.sum. The two supplied primitive implementations are
unchanged in behavior; their fetched source matches the supplied source tokens.

These checks support the implementation and its documented invariants. They do
not prove every possible execution, scheduler fairness, or performance on the
user's Mac. An active subscription must remain single-owner, must not be copied,
and must promptly advance or close after consuming a Ready notification.

## Reproduction

From the included standalone `source` directory:

```sh
go test ./...
go test -race -count=25 -cpu=1,8 ./...
go vet ./...
go test -run '^$' -fuzz '^FuzzRingPubSubModel$' -fuzztime=20s -parallel=4
go test -run '^$' -bench '^BenchmarkBroadcast$' -benchtime=250ms -count=5 -cpu=1,8
go test -run '^$' -bench '^BenchmarkBroadcastLarge$' -benchtime=200ms -count=3 -cpu=8
go test -run '^$' -bench '^Benchmark(SubscriptionChurn|Construction|RingQueuedLifecycle|InlineCallbacks)$' -benchtime=200ms -count=3 -cpu=8
```

Run correctness tests and benchmarks sequentially to avoid contention from the
validation workload. The complete-repository tests can be reproduced by applying
the additive patch to the pinned repository revision and running its normal test
commands with its original dependencies.
-- ringpubsub.patch --
diff --git a/ringpubsub.go b/ringpubsub.go
new file mode 100644
--- /dev/null
+++ b/ringpubsub.go
@@ -0,0 +1,425 @@
+/*
+   Copyright 2026 Joseph Cumines
+
+   Licensed under the Apache License, Version 2.0 (the "License");
+   you may not use this file except in compliance with the License.
+   You may obtain a copy of the License at
+
+       http://www.apache.org/licenses/LICENSE-2.0
+
+   Unless required by applicable law or agreed to in writing, software
+   distributed under the License is distributed on an "AS IS" BASIS,
+   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+   See the License for the specific language governing permissions and
+   limitations under the License.
+*/
+
+package bigbuff
+
+import (
+	"context"
+	"errors"
+	"io"
+	"sync"
+	"sync/atomic"
+)
+
+// ErrRingFull indicates that a RingPubSub cannot admit another value until a
+// reader consumes or abandons an outstanding value. No value was published.
+var ErrRingFull = errors.New("bigbuff: ringpubsub: full")
+
+// A shared terminal channel makes inactive, including zero-value,
+// subscriptions selectable without allocating a channel per subscription.
+var ringPubSubInactive = func() <-chan struct{} {
+	c := make(chan struct{})
+	close(c)
+	return c
+}()
+
+// RingPubSub is a bounded, lossless broadcast log with selectable subscriptions.
+// Each accepted value belongs to every subscription active at publication.
+// Readers consume in publication order, independently, with no broadcast-wide
+// acknowledgement barrier. A slow reader eventually backpressures publishers.
+//
+// Unlike ChanPubSub.Send, Send reports admission, NOT completed delivery. Its
+// result counts subscriptions at publication, including any that subsequently
+// abandon that value. Closing a subscription explicitly abandons its backlog.
+//
+// The hub has no subscriber registry, per-subscriber channel, callback, or
+// background goroutine. Construction allocates a fixed ring and capacity+1
+// reusable notification channels. Subscribe returns a small caller-owned value;
+// whether that value escapes to the heap depends on its caller. The methods do
+// not create per-publication, per-receive, or per-subscription infrastructure.
+// Escape analysis, large value temporaries, runtime parking, and caller-owned
+// values can still allocate. See the allocation tests and benchmarks.
+//
+// Publishers and distinct subscriptions may operate concurrently. Each individual
+// subscription has one owner; see RingSubscription for its select contract.
+// Construct with NewRingPubSub. A hub must not be copied after construction.
+type RingPubSub[V any] struct {
+	mu          sync.Mutex
+	slots       []ringPubSubSlot[V]
+	space       chan struct{}
+	head        int  // oldest unreclaimed slot; guarded by mu
+	used        int  // published but not yet reclaimed slots; guarded by mu
+	subscribers int  // guarded by mu
+	closed      bool // guarded by mu
+	// tail is a physical index, NOT a wrapping message sequence. Release via
+	// Store publishes slot initialization to readers that observe this index.
+	tail atomic.Uint64
+}
+
+type ringPubSubSlot[V any] struct {
+	// value is immutable from publication until the last obligation is released.
+	value     V
+	remaining atomic.Int64
+	ready     chan struct{}
+}
+
+// RingSubscription is a single-owner cursor in a RingPubSub. Its zero value is
+// inactive. An active subscription MUST NOT be copied. Transfer ownership to
+// another goroutine by passing its address, or move a newly returned value once
+// before use and never use the original again. All methods, and receives from
+// Ready, must be serialized for this individual subscription.
+//
+// Every successful receive from Ready MUST be followed promptly by TryRecv,
+// Read, Recv, or Close. A wakeup is a shared notification, not a value or an
+// acknowledgement. Keeping it without advancing or closing can delay other
+// readers. If another select case wins, no repair is needed: the subscription
+// may continue later or Close to abandon its backlog.
+//
+// Close when finished. An abandoned active cursor can retain values and block
+// publishers indefinitely, just like a reader that has stopped consuming.
+type RingSubscription[V any] struct {
+	noCopy ringPubSubNoCopy
+	hub    *RingPubSub[V]
+	next   int
+}
+
+// A vet marker, deliberately not a self-pointer: self-pointer copy checks would
+// force otherwise stack-resident subscriptions to escape.
+type ringPubSubNoCopy struct{}
+
+func (*ringPubSubNoCopy) Lock()   {}
+func (*ringPubSubNoCopy) Unlock() {}
+
+// NewRingPubSub creates a broadcast log retaining at most capacity values.
+// Capacity must be positive and must leave room for capacity+1 physical slots.
+// The extra slot is essential: a caught-up cursor must never select a channel
+// that still contains a notification for an older generation of that slot.
+func NewRingPubSub[V any](capacity int) *RingPubSub[V] {
+	if capacity <= 0 || capacity == int(^uint(0)>>1) {
+		panic("bigbuff: ringpubsub: invalid capacity")
+	}
+	x := &RingPubSub[V]{
+		slots: make([]ringPubSubSlot[V], capacity+1),
+		space: make(chan struct{}, 1),
+	}
+	for i := range x.slots {
+		x.slots[i].ready = make(chan struct{}, 1)
+	}
+	return x
+}
+
+func (x *RingPubSub[V]) checkInitialized() {
+	if len(x.slots) == 0 {
+		panic("bigbuff: ringpubsub: must use factory function")
+	}
+}
+
+// Subscribe immediately registers a reader for future publications. It does not
+// replay existing values. Subscribing to a closed hub returns an inactive cursor.
+// Registration takes constant work and does not wait for readers or publishers
+// blocked on capacity. The returned active value must not be copied.
+func (x *RingPubSub[V]) Subscribe() RingSubscription[V] {
+	x.checkInitialized()
+	x.mu.Lock()
+	defer x.mu.Unlock()
+	if x.closed {
+		return RingSubscription[V]{}
+	}
+	if x.subscribers == int(^uint(0)>>1) {
+		panic("bigbuff: ringpubsub: too many subscribers")
+	}
+	x.subscribers++
+	return RingSubscription[V]{hub: x, next: int(x.tail.Load())}
+}
+
+// TrySend publishes a value without waiting for capacity. It returns the number
+// of subscriptions at publication, ErrRingFull if capacity is exhausted, or
+// io.ErrClosedPipe after Close. With no subscribers it returns (0, nil) without
+// retaining the value. The mutex may still contend; this is not a wait-free API.
+func (x *RingPubSub[V]) TrySend(value V) (int, error) {
+	x.checkInitialized()
+	x.mu.Lock()
+	n, err := x.trySendLocked(value)
+	x.mu.Unlock()
+	return n, err
+}
+
+// Send waits for capacity and publishes value, or returns a context error or
+// io.ErrClosedPipe. A nil context means no cancellation. On an error no value
+// was published. Cancellation concurrent with successful publication may lose
+// the race; a successful result always means publication occurred.
+//
+// This method does not wait for recipients to consume the value. Publishers are
+// ordered by admission under the mutex, not by the start times of their calls.
+func (x *RingPubSub[V]) Send(ctx context.Context, value V) (int, error) {
+	x.checkInitialized()
+	var done <-chan struct{}
+	if ctx != nil {
+		done = ctx.Done()
+	}
+	for {
+		if ctx != nil {
+			if err := ctx.Err(); err != nil {
+				// We might have consumed a space hint on the previous iteration.
+				// Pass it on before cancellation so another writer cannot strand.
+				x.mu.Lock()
+				x.wakeWriterLocked()
+				x.mu.Unlock()
+				return 0, err
+			}
+		}
+		x.mu.Lock()
+		n, err := x.trySendLocked(value)
+		x.mu.Unlock()
+		if err != ErrRingFull {
+			return n, err
+		}
+		select {
+		case <-done:
+			return 0, ctx.Err()
+		case <-x.space:
+		}
+	}
+}
+
+// trySendLocked initializes a slot before exposing it through tail. Keep the
+// mutex through the initial notification: a fast reader can release the last
+// obligation before that notification, but cannot reclaim the slot before it.
+func (x *RingPubSub[V]) trySendLocked(value V) (int, error) {
+	if x.closed {
+		return 0, io.ErrClosedPipe
+	}
+	n := x.subscribers
+	if n == 0 {
+		x.wakeWriterLocked()
+		return 0, nil
+	}
+	if x.used == len(x.slots)-1 {
+		// Help a final reader that released its obligation but was descheduled
+		// before reclaiming. A zero count certifies all old slot accesses are
+		// over; publication need not wait for that reader to run again.
+		x.reapLocked()
+		if x.used == len(x.slots)-1 {
+			return 0, ErrRingFull
+		}
+	}
+	i := int(x.tail.Load())
+	slot := &x.slots[i]
+	slot.value = value
+	slot.remaining.Store(int64(n))
+	x.used++
+	x.tail.Store(uint64(x.advance(i)))
+	ringPubSubSignal(slot.ready)
+	x.wakeWriterLocked()
+	return n, nil
+}
+
+// Close stops publication, wakes blocked publishers, and allows every active
+// subscription to drain its existing values before observing EOF. It returns
+// true exactly once. Only the unused tail notification channel is closed: this
+// is a terminal position that every draining cursor will eventually reach.
+// Close never races a send on an occupied slot's notification channel.
+func (x *RingPubSub[V]) Close() bool {
+	x.checkInitialized()
+	x.mu.Lock()
+	defer x.mu.Unlock()
+	if x.closed {
+		return false
+	}
+	x.closed = true
+	close(x.slots[int(x.tail.Load())].ready)
+	close(x.space)
+	return true
+}
+
+// Ready returns the notification channel for this cursor's next value. Evaluate
+// it again after every successful receive or Read: it changes as the cursor
+// advances. Do not retain a channel across a cursor mutation or transfer it to
+// another reader. A closed channel indicates EOF; unread values precede EOF.
+// An inactive subscription returns a shared, already-closed channel.
+//
+// A successful receive must promptly be followed by TryRecv, Read, Recv, or
+// Close. When the hub is open and the contract is respected, such a receive
+// guarantees that this cursor has a value to consume.
+func (s *RingSubscription[V]) Ready() <-chan struct{} {
+	if s.hub == nil {
+		return ringPubSubInactive
+	}
+	return s.hub.slots[s.next].ready
+}
+
+// TryRecv consumes the next value if available. No prior Ready receive is
+// necessary. It returns false if the cursor is caught up or inactive; Recv or
+// Ready distinguishes a temporarily empty stream from EOF.
+//
+// The common non-final reader does not acquire the hub mutex. The final release
+// reclaims completed slots under that mutex; it may contend with publishers.
+func (s *RingSubscription[V]) TryRecv() (value V, ok bool) {
+	x := s.hub
+	if x == nil || s.next == int(x.tail.Load()) {
+		return
+	}
+	slot := &x.slots[s.next]
+	value = slot.value
+	// Signal BEFORE releasing our obligation. The obligation pins both the
+	// immutable value and its notification channel's generation until we are
+	// completely finished with them. In particular, a delayed signal can never
+	// leak into a later reuse of this slot.
+	ringPubSubSignal(slot.ready)
+	remaining := slot.remaining.Add(-1)
+	s.next = x.advance(s.next)
+	if remaining == 0 {
+		x.mu.Lock()
+		// Do not reclaim the captured slot here: another reaper may have
+		// already reclaimed it while we were acquiring the mutex. Reap only
+		// the CURRENT completed prefix, without retiring an old slot twice.
+		x.reapLocked()
+		x.mu.Unlock()
+	}
+	return value, true
+}
+
+// Read fills as much of dst as is currently available, returning the number of
+// values copied. It never waits for more values. The caller owns and may reuse
+// dst. An empty dst performs no operation and does not satisfy the obligation
+// to advance or close after consuming a Ready notification.
+func (s *RingSubscription[V]) Read(dst []V) int {
+	for i := range dst {
+		value, ok := s.TryRecv()
+		if !ok {
+			return i
+		}
+		dst[i] = value
+	}
+	return len(dst)
+}
+
+// Recv consumes a value, waits for one, or returns io.EOF or a context error.
+// A nil context means no cancellation. A canceled call leaves the subscription
+// active; Close it when no further reads will be made. Cancellation already
+// observed at the start of an attempt takes precedence over available data;
+// cancellation concurrent with consuming a value may lose the race.
+func (s *RingSubscription[V]) Recv(ctx context.Context) (value V, err error) {
+	var done <-chan struct{}
+	if ctx != nil {
+		done = ctx.Done()
+	}
+	for {
+		if ctx != nil {
+			if err := ctx.Err(); err != nil {
+				// Ready may have been consumed by this loop OR by the caller.
+				// Keep our obligation, but pass on its notification before
+				// returning. A pending obligation pins an open data lane;
+				// a caught-up cursor names an empty or terminal lane instead.
+				if x := s.hub; x != nil && s.next != int(x.tail.Load()) {
+					ringPubSubSignal(x.slots[s.next].ready)
+				}
+				return value, err
+			}
+		}
+		if v, ok := s.TryRecv(); ok {
+			return v, nil
+		}
+		select {
+		case <-done:
+			return value, ctx.Err()
+		case _, ok := <-s.Ready():
+			if !ok {
+				return value, io.EOF
+			}
+		}
+	}
+}
+
+// Close unregisters this subscription and abandons its unread values. It is
+// idempotent, including for a zero-value cursor. It never waits for other readers
+// to make progress. Its work is proportional to this cursor's unread backlog,
+// bounded by the hub's capacity, plus amortized completed-prefix reclamation.
+// It must not run concurrently with another operation on this same cursor.
+func (s *RingSubscription[V]) Close() {
+	x := s.hub
+	if x == nil {
+		return
+	}
+	x.mu.Lock()
+	x.subscribers--
+	tail := int(x.tail.Load())
+	for s.next != tail {
+		slot := &x.slots[s.next]
+		// Repair a notification even if Close follows a selected wakeup.
+		// As in TryRecv, our obligation protects this signal from slot reuse.
+		ringPubSubSignal(slot.ready)
+		slot.remaining.Add(-1)
+		s.next = x.advance(s.next)
+	}
+	x.reapLocked()
+	x.mu.Unlock()
+	s.hub = nil
+}
+
+func (x *RingPubSub[V]) advance(i int) int {
+	i++
+	if i == len(x.slots) {
+		return 0
+	}
+	return i
+}
+
+// reapLocked only touches the current published prefix, never an old captured
+// slot. A zero count proves that all value copies AND all notifications for the
+// generation finished. Clear pointer-bearing payloads and stale hints before
+// permitting reuse. A delayed final reader may safely reap nothing at all.
+func (x *RingPubSub[V]) reapLocked() {
+	for x.used != 0 {
+		slot := &x.slots[x.head]
+		if slot.remaining.Load() != 0 {
+			break
+		}
+		var zero V
+		slot.value = zero
+		select {
+		case <-slot.ready:
+		default:
+		}
+		x.head = x.advance(x.head)
+		x.used--
+	}
+	x.wakeWriterLocked()
+}
+
+// Multiple publishers share one coalesced space hint. Every successful writer
+// relays it if more capacity exists. Full transitions drain an obsolete hint;
+// canceled writers that consumed a hint explicitly repair it in Send.
+func (x *RingPubSub[V]) wakeWriterLocked() {
+	if x.closed {
+		return
+	}
+	if x.used < len(x.slots)-1 {
+		ringPubSubSignal(x.space)
+	} else {
+		select {
+		case <-x.space:
+		default:
+		}
+	}
+}
+
+func ringPubSubSignal(c chan struct{}) {
+	select {
+	case c <- struct{}{}:
+	default:
+	}
+}
diff --git a/ringpubsub_test.go b/ringpubsub_test.go
new file mode 100644
--- /dev/null
+++ b/ringpubsub_test.go
@@ -0,0 +1,1046 @@
+/*
+   Copyright 2026 Joseph Cumines
+
+   Licensed under the Apache License, Version 2.0 (the "License");
+   you may not use this file except in compliance with the License.
+   You may obtain a copy of the License at
+
+       http://www.apache.org/licenses/LICENSE-2.0
+
+   Unless required by applicable law or agreed to in writing, software
+   distributed under the License is distributed on an "AS IS" BASIS,
+   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+   See the License for the specific language governing permissions and
+   limitations under the License.
+*/
+
+package bigbuff
+
+import (
+	"context"
+	"errors"
+	"fmt"
+	"io"
+	"math/rand/v2"
+	"runtime"
+	"slices"
+	"sync"
+	"testing"
+	"time"
+)
+
+func ringTestNotReady(t *testing.T, c <-chan struct{}) {
+	t.Helper()
+	select {
+	case <-c:
+		t.Fatal("unexpected readiness for a subscription with no pending value")
+	default:
+	}
+}
+
+func ringTestReceive(t *testing.T, s *RingSubscription[int], want int) {
+	t.Helper()
+	select {
+	case <-s.Ready():
+		if got, ok := s.TryRecv(); !ok || got != want {
+			t.Fatalf("TryRecv after Ready = (%d, %v), want (%d, true)", got, ok, want)
+		}
+	case <-time.After(5 * time.Second):
+		t.Fatal("subscription never became ready")
+	}
+}
+
+func TestRingPubSubCapacity(t *testing.T) {
+	for _, capacity := range []int{0, -1, int(^uint(0) >> 1)} {
+		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
+			defer func() {
+				if recover() == nil {
+					t.Fatal("invalid capacity did not panic")
+				}
+			}()
+			NewRingPubSub[int](capacity)
+		})
+	}
+}
+
+func TestRingPubSubNoSubscribersAndClosedSubscription(t *testing.T) {
+	p := NewRingPubSub[int](1)
+	for i := range 100 {
+		if n, err := p.TrySend(i); n != 0 || err != nil {
+			t.Fatalf("TrySend without subscribers = (%d, %v)", n, err)
+		}
+	}
+	s := p.Subscribe()
+	ringTestNotReady(t, s.Ready())
+	if v, ok := s.TryRecv(); v != 0 || ok {
+		t.Fatalf("new subscriber received historical value: (%d, %v)", v, ok)
+	}
+	s.Close()
+	s.Close()
+	if v, err := s.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
+		t.Fatalf("Recv on closed subscription = (%d, %v)", v, err)
+	}
+	if !p.Close() || p.Close() {
+		t.Fatal("Close must report exactly one transition")
+	}
+	if n, err := p.TrySend(1); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
+		t.Fatalf("TrySend on closed hub = (%d, %v)", n, err)
+	}
+	if n, err := p.Send(nil, 1); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
+		t.Fatalf("Send on closed hub = (%d, %v)", n, err)
+	}
+	late := p.Subscribe()
+	defer late.Close()
+	if v, err := late.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
+		t.Fatalf("Recv on subscription created after hub Close = (%d, %v)", v, err)
+	}
+}
+
+// The extra lane is required when either an existing reader catches up or a
+// reader joins a full ring. Neither may receive an older cohort's wake token.
+func TestRingPubSubSpareLane(t *testing.T) {
+	for _, capacity := range []int{1, 2, 3, 7} {
+		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
+			p := NewRingPubSub[int](capacity)
+			defer p.Close()
+			slow, fast := p.Subscribe(), p.Subscribe()
+			defer slow.Close()
+			defer fast.Close()
+			for i := range capacity {
+				if n, err := p.TrySend(i); n != 2 || err != nil {
+					t.Fatalf("publish %d = (%d, %v)", i, n, err)
+				}
+			}
+			for i := range capacity {
+				ringTestReceive(t, &fast, i)
+			}
+			ringTestNotReady(t, fast.Ready())
+			late := p.Subscribe()
+			defer late.Close()
+			ringTestNotReady(t, late.Ready())
+			if n, err := p.TrySend(99); n != 0 || !errors.Is(err, ErrRingFull) {
+				t.Fatalf("full ring accepted publication: (%d, %v)", n, err)
+			}
+			for i := range capacity {
+				ringTestReceive(t, &slow, i)
+			}
+			for i := range 5 * (capacity + 1) {
+				if n, err := p.TrySend(100 + i); n != 3 || err != nil {
+					t.Fatalf("publish after wrap = (%d, %v)", n, err)
+				}
+				ringTestReceive(t, &late, 100+i)
+				ringTestReceive(t, &fast, 100+i)
+				ringTestReceive(t, &slow, 100+i)
+				ringTestNotReady(t, late.Ready())
+			}
+		})
+	}
+}
+
+func TestRingPubSubSelectedHintThenUnsubscribe(t *testing.T) {
+	p := NewRingPubSub[int](1)
+	defer p.Close()
+	a, b := p.Subscribe(), p.Subscribe()
+	defer b.Close()
+	if n, err := p.TrySend(7); n != 2 || err != nil {
+		t.Fatalf("TrySend = (%d, %v)", n, err)
+	}
+	select {
+	case <-a.Ready():
+	default:
+		t.Fatal("publication did not produce a wake")
+	}
+	// A selected wake has not acknowledged a value. Closing this reader must
+	// hand notification to B, even though A consumed the only queued wake.
+	a.Close()
+	ringTestReceive(t, &b, 7)
+	if n, err := p.TrySend(8); n != 1 || err != nil {
+		t.Fatalf("TrySend after unsubscribe = (%d, %v)", n, err)
+	}
+	ringTestReceive(t, &b, 8)
+}
+
+func TestRingPubSubMultipleInFlightHints(t *testing.T) {
+	p := NewRingPubSub[int](1)
+	defer p.Close()
+	a, b, c := p.Subscribe(), p.Subscribe(), p.Subscribe()
+	defer a.Close()
+	defer b.Close()
+	defer c.Close()
+	if _, err := p.TrySend(11); err != nil {
+		t.Fatal(err)
+	}
+	<-a.Ready()
+	// A still owes this generation. B may poll without owning a notification.
+	if got, ok := b.TryRecv(); !ok || got != 11 {
+		t.Fatalf("polling B = (%d, %v)", got, ok)
+	}
+	select {
+	case <-c.Ready():
+	default:
+		t.Fatal("polling reader did not relay notification")
+	}
+	if _, err := p.TrySend(12); !errors.Is(err, ErrRingFull) {
+		t.Fatalf("in-flight readers failed to pin generation: %v", err)
+	}
+	if got, ok := a.TryRecv(); !ok || got != 11 {
+		t.Fatalf("selected A = (%d, %v)", got, ok)
+	}
+	if _, err := p.TrySend(12); !errors.Is(err, ErrRingFull) {
+		t.Fatalf("last in-flight reader failed to pin generation: %v", err)
+	}
+	c.Close()
+	if n, err := p.TrySend(12); n != 2 || err != nil {
+		t.Fatalf("publish after final retirement = (%d, %v)", n, err)
+	}
+	ringTestReceive(t, &a, 12)
+	ringTestReceive(t, &b, 12)
+	ringTestNotReady(t, a.Ready())
+	ringTestNotReady(t, b.Ready())
+}
+
+func TestRingPubSubCloseDrainsEachCohort(t *testing.T) {
+	p := NewRingPubSub[int](4)
+	a := p.Subscribe()
+	defer a.Close()
+	p.TrySend(1)
+	b := p.Subscribe()
+	defer b.Close()
+	p.TrySend(2)
+	p.TrySend(3)
+	ringTestReceive(t, &a, 1)
+	if !p.Close() {
+		t.Fatal("first close returned false")
+	}
+	for _, s := range []*RingSubscription[int]{&a, &b} {
+		for _, want := range []int{2, 3} {
+			ringTestReceive(t, s, want)
+		}
+		select {
+		case <-s.Ready():
+		default:
+			t.Fatal("drained closed hub did not signal EOF")
+		}
+		if got, err := s.Recv(nil); got != 0 || !errors.Is(err, io.EOF) {
+			t.Fatalf("Recv after drain = (%d, %v)", got, err)
+		}
+	}
+}
+
+func TestRingPubSubCloseWakesCaughtUpReaders(t *testing.T) {
+	p := NewRingPubSub[int](2)
+	const readers = 16
+	subs := make([]RingSubscription[int], readers)
+	for i := range subs {
+		subs[i] = p.Subscribe()
+	}
+	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
+	defer cancel()
+	results := make(chan error, readers)
+	for i := range subs {
+		go func(s *RingSubscription[int]) {
+			defer s.Close()
+			_, err := s.Recv(ctx)
+			results <- err
+		}(&subs[i])
+	}
+	p.Close()
+	for range readers {
+		select {
+		case err := <-results:
+			if !errors.Is(err, io.EOF) {
+				t.Fatalf("waiting reader returned %v, want EOF", err)
+			}
+		case <-ctx.Done():
+			t.Fatal("Close did not wake every reader")
+		}
+	}
+}
+
+func TestRingPubSubCancellationPreservesSubscription(t *testing.T) {
+	p := NewRingPubSub[int](1)
+	defer p.Close()
+	s := p.Subscribe()
+	defer s.Close()
+	ctx, cancel := context.WithCancel(context.Background())
+	cancel()
+	if v, err := s.Recv(ctx); v != 0 || !errors.Is(err, context.Canceled) {
+		t.Fatalf("canceled Recv = (%d, %v)", v, err)
+	}
+	if n, err := p.Send(nil, 4); n != 1 || err != nil {
+		t.Fatalf("Send after canceled Recv = (%d, %v)", n, err)
+	}
+	if n, err := p.Send(ctx, 5); n != 0 || !errors.Is(err, context.Canceled) {
+		t.Fatalf("canceled blocked Send = (%d, %v)", n, err)
+	}
+	ringTestReceive(t, &s, 4)
+	ringTestNotReady(t, s.Ready())
+	if n, err := p.TrySend(6); n != 1 || err != nil {
+		t.Fatalf("subscriber lost after cancellation: (%d, %v)", n, err)
+	}
+	ringTestReceive(t, &s, 6)
+}
+
+func TestRingPubSubCanceledRecvPreservesBacklogAndRelaysWake(t *testing.T) {
+	for _, hubClosed := range []bool{false, true} {
+		for _, selected := range []bool{false, true} {
+			t.Run(fmt.Sprintf("closed=%v/selected=%v", hubClosed, selected), func(t *testing.T) {
+				p := NewRingPubSub[int](2)
+				defer p.Close()
+				a, b := p.Subscribe(), p.Subscribe()
+				defer a.Close()
+				defer b.Close()
+				for _, v := range []int{1, 2} {
+					if _, err := p.TrySend(v); err != nil {
+						t.Fatal(err)
+					}
+				}
+				if selected {
+					select {
+					case <-a.Ready():
+					default:
+						t.Fatal("publication did not signal the reader")
+					}
+				}
+				if hubClosed {
+					p.Close()
+				}
+				ctx, cancel := context.WithCancel(context.Background())
+				cancel()
+				if got, err := a.Recv(ctx); got != 0 || !errors.Is(err, context.Canceled) {
+					t.Fatalf("canceled Recv with backlog = (%d, %v)", got, err)
+				}
+				// A keeps its obligation, but must not keep the only notification.
+				// B can drain both messages while A is paused after cancellation.
+				ringTestReceive(t, &b, 1)
+				ringTestReceive(t, &b, 2)
+				if !hubClosed {
+					if _, err := p.TrySend(3); !errors.Is(err, ErrRingFull) {
+						t.Fatalf("cancellation unexpectedly acknowledged backlog: %v", err)
+					}
+				}
+				ringTestReceive(t, &a, 1)
+				ringTestReceive(t, &a, 2)
+				if hubClosed {
+					if _, err := a.Recv(nil); !errors.Is(err, io.EOF) {
+						t.Fatalf("drained reader returned %v, want EOF", err)
+					}
+				} else {
+					ringTestNotReady(t, a.Ready())
+				}
+			})
+		}
+	}
+}
+
+func TestRingSubscriptionZeroValue(t *testing.T) {
+	var s RingSubscription[int]
+	select {
+	case _, ok := <-s.Ready():
+		if ok {
+			t.Fatal("zero-value subscription notification was not closed")
+		}
+	default:
+		t.Fatal("zero-value subscription is not immediately terminal")
+	}
+	if v, ok := s.TryRecv(); v != 0 || ok {
+		t.Fatalf("zero-value TryRecv = (%d, %v)", v, ok)
+	}
+	output := []int{1, 2}
+	if n := s.Read(output); n != 0 || !slices.Equal(output, []int{1, 2}) {
+		t.Fatalf("zero-value Read = (%v, %d)", output, n)
+	}
+	if v, err := s.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
+		t.Fatalf("zero-value Recv = (%d, %v)", v, err)
+	}
+	s.Close()
+	s.Close()
+}
+
+func TestRingPubSubReleasesPayloadReferences(t *testing.T) {
+	type payload struct{ value int }
+	p := NewRingPubSub[*payload](3)
+	defer p.Close()
+	a, b := p.Subscribe(), p.Subscribe()
+	defer a.Close()
+	defer b.Close()
+	values := []*payload{% raw %}{{1}, {2}, {3}}{% endraw %}
+	for _, v := range values {
+		if _, err := p.TrySend(v); err != nil {
+			t.Fatal(err)
+		}
+	}
+	var output [3]*payload
+	if n := a.Read(output[:]); n != len(output) || !slices.Equal(output[:], values) {
+		t.Fatalf("first reader = %v (%d)", output, n)
+	}
+	for i, v := range values {
+		if p.slots[i].value != v {
+			t.Fatalf("slot %d cleared before the remaining subscriber consumed it", i)
+		}
+	}
+	if v, ok := b.TryRecv(); !ok || v != values[0] {
+		t.Fatalf("final receive = (%v, %v)", v, ok)
+	}
+	if p.slots[0].value != nil {
+		t.Fatal("last receive retained a payload reference")
+	}
+	p.Close()
+	b.Close() // abandon the two remaining values after hub shutdown
+	for i := range p.slots {
+		if p.slots[i].value != nil {
+			t.Fatalf("slot %d retained an abandoned payload reference", i)
+		}
+	}
+}
+
+// Hold the hub mutex to pause the final reader after it has released its atomic
+// obligation. A publisher must be able to help reclaim and publish before that
+// reader runs its reaper. The delayed reaper must then inspect the current
+// prefix, rather than retiring its captured old slot a second time.
+func TestRingPubSubPublisherHelpsDelayedFinalReader(t *testing.T) {
+	p := NewRingPubSub[int](1)
+	defer p.Close()
+	s := p.Subscribe()
+	if _, err := p.TrySend(1); err != nil {
+		t.Fatal(err)
+	}
+	type result struct {
+		value int
+		ok    bool
+	}
+	read := make(chan result, 1)
+	p.mu.Lock()
+	head := p.head
+	go func() {
+		value, ok := s.TryRecv()
+		read <- result{value, ok}
+	}()
+	deadline := time.Now().Add(5 * time.Second)
+	for p.slots[head].remaining.Load() != 0 {
+		if time.Now().After(deadline) {
+			p.mu.Unlock()
+			t.Fatal("last reader did not release its atomic obligation")
+		}
+		runtime.Gosched()
+	}
+	// Test the locked production path while deliberately excluding the
+	// delayed reader's lock acquisition; no test hook is needed in the code.
+	n, err := p.trySendLocked(2)
+	p.mu.Unlock()
+	if n != 1 || err != nil {
+		t.Fatalf("publisher failed to help final reader: (%d, %v)", n, err)
+	}
+	select {
+	case got := <-read:
+		if !got.ok || got.value != 1 {
+			t.Fatalf("delayed reader = %+v", got)
+		}
+	case <-time.After(5 * time.Second):
+		t.Fatal("delayed reader did not finish")
+	}
+	defer s.Close()
+	if _, err := p.TrySend(3); !errors.Is(err, ErrRingFull) {
+		t.Fatalf("delayed reaper corrupted the newly occupied slot: %v", err)
+	}
+	ringTestReceive(t, &s, 2)
+	if _, err := p.TrySend(3); err != nil {
+		t.Fatal(err)
+	}
+	ringTestReceive(t, &s, 3)
+}
+
+// A batch retirement coalesces its space notifications. Publishers must relay
+// that hint while capacity remains, or only one blocked publisher will wake.
+func TestRingPubSubSpaceHintReachesAllPublishers(t *testing.T) {
+	const capacity = 8
+	p := NewRingPubSub[int](capacity)
+	defer p.Close()
+	s := p.Subscribe()
+	defer s.Close()
+	for i := range capacity {
+		if _, err := p.TrySend(i); err != nil {
+			t.Fatal(err)
+		}
+	}
+	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
+	defer cancel()
+	started := make(chan struct{}, capacity)
+	results := make(chan error, capacity)
+	for i := range capacity {
+		go func(v int) {
+			started <- struct{}{}
+			n, err := p.Send(ctx, 100+v)
+			if err == nil && n != 1 {
+				err = fmt.Errorf("recipient count %d, want 1", n)
+			}
+			results <- err
+		}(i)
+	}
+	for range capacity {
+		<-started
+	}
+	// The assertion does not depend on a scheduling deadline; this just makes
+	// it likely that all producers exercise the blocked path in normal runs.
+	for range capacity * 4 {
+		runtime.Gosched()
+	}
+	var out [capacity]int
+	if n := s.Read(out[:]); n != capacity {
+		t.Fatalf("Read freed %d values, want %d", n, capacity)
+	}
+	for i, v := range out {
+		if v != i {
+			t.Fatalf("Read[%d] = %d", i, v)
+		}
+	}
+	for range capacity {
+		select {
+		case err := <-results:
+			if err != nil {
+				t.Fatal(err)
+			}
+		case <-ctx.Done():
+			t.Fatal("free capacity stranded blocked publishers")
+		}
+	}
+	if n := s.Read(out[:]); n != capacity {
+		t.Fatalf("published %d values, want %d", n, capacity)
+	}
+	slices.Sort(out[:])
+	for i, v := range out {
+		if v != 100+i {
+			t.Fatalf("publication[%d] = %d, want %d", i, v, 100+i)
+		}
+	}
+}
+
+func TestRingPubSubCloseWakesBlockedPublishers(t *testing.T) {
+	p := NewRingPubSub[int](1)
+	s := p.Subscribe()
+	defer s.Close()
+	p.TrySend(1)
+	const publishers = 16
+	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
+	defer cancel()
+	results := make(chan error, publishers)
+	for range publishers {
+		go func() {
+			_, err := p.Send(ctx, 2)
+			results <- err
+		}()
+	}
+	p.Close()
+	for range publishers {
+		select {
+		case err := <-results:
+			if !errors.Is(err, io.ErrClosedPipe) {
+				t.Fatalf("blocked Send returned %v, want ErrClosedPipe", err)
+			}
+		case <-ctx.Done():
+			t.Fatal("Close did not wake every publisher")
+		}
+	}
+	ringTestReceive(t, &s, 1)
+}
+
+func TestRingPubSubCanceledPublishersDoNotStrandCapacity(t *testing.T) {
+	for iteration := range 50 {
+		p := NewRingPubSub[int](1)
+		s := p.Subscribe()
+		p.TrySend(-1)
+		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
+		cancelCtx, cancelBlocked := context.WithCancel(ctx)
+		const canceled = 4
+		results := make(chan error, canceled)
+		for range canceled {
+			go func() {
+				_, err := p.Send(cancelCtx, -2)
+				results <- err
+			}()
+		}
+		live := make(chan error, 1)
+		go func() {
+			_, err := p.Send(ctx, iteration)
+			live <- err
+		}()
+		for range 16 {
+			runtime.Gosched()
+		}
+		cancelBlocked()
+		type readResult struct {
+			values []int
+			err    error
+		}
+		read := make(chan readResult, 1)
+		go func() {
+			defer s.Close()
+			var r readResult
+			for {
+				v, err := s.Recv(ctx)
+				if err != nil {
+					if !errors.Is(err, io.EOF) {
+						r.err = err
+					}
+					read <- r
+					return
+				}
+				r.values = append(r.values, v)
+			}
+		}()
+		// A publisher can have passed its cancellation check before being
+		// delayed on the hub mutex. Such an overlapping successful publication
+		// is allowed, and must correspond to exactly one delivered value.
+		committed := 0
+		for range canceled {
+			select {
+			case err := <-results:
+				if err == nil {
+					committed++
+				} else if !errors.Is(err, context.Canceled) {
+					t.Fatalf("canceled blocked publisher returned %v", err)
+				}
+			case <-ctx.Done():
+				t.Fatal("canceled publisher remained blocked")
+			}
+		}
+		select {
+		case err := <-live:
+			if err != nil {
+				t.Fatal(err)
+			}
+		case <-ctx.Done():
+			t.Fatal("canceled publisher consumed the sole space hint")
+		}
+		p.Close()
+		select {
+		case r := <-read:
+			if r.err != nil || len(r.values) != committed+2 {
+				t.Fatalf("drain after cancellation = %v, %v; successful canceled sends=%d", r.values, r.err, committed)
+			}
+			if r.values[0] != -1 {
+				t.Fatalf("initial value = %d, want -1", r.values[0])
+			}
+			seenLive, seenCanceled := 0, 0
+			for _, v := range r.values[1:] {
+				switch v {
+				case iteration:
+					seenLive++
+				case -2:
+					seenCanceled++
+				default:
+					t.Fatalf("unexpected delivered value %d", v)
+				}
+			}
+			if seenLive != 1 || seenCanceled != committed {
+				t.Fatalf("delivered live=%d, canceled=%d; committed canceled=%d", seenLive, seenCanceled, committed)
+			}
+		case <-ctx.Done():
+			t.Fatal("consumer did not drain after cancellation")
+		}
+		cancel()
+	}
+}
+
+func TestRingPubSubConcurrentPublishersAgreeOnOrder(t *testing.T) {
+	const publishers, perPublisher, subscribers = 4, 250, 8
+	p := NewRingPubSub[int](8)
+	defer p.Close()
+	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
+	defer cancel()
+	subs := make([]RingSubscription[int], subscribers)
+	for i := range subs {
+		subs[i] = p.Subscribe()
+	}
+	type result struct {
+		values []int
+		err    error
+	}
+	consumed := make(chan result, subscribers)
+	for i := range subs {
+		go func(s *RingSubscription[int]) {
+			defer s.Close()
+			r := result{values: make([]int, 0, publishers*perPublisher)}
+			for {
+				v, err := s.Recv(ctx)
+				if err != nil {
+					if !errors.Is(err, io.EOF) {
+						r.err = err
+					}
+					consumed <- r
+					return
+				}
+				r.values = append(r.values, v)
+			}
+		}(&subs[i])
+	}
+	produced := make(chan error, publishers)
+	for producer := range publishers {
+		go func(id int) {
+			for i := range perPublisher {
+				n, err := p.Send(ctx, id*perPublisher+i)
+				if err != nil {
+					produced <- err
+					return
+				}
+				if n != subscribers {
+					produced <- fmt.Errorf("Send recipients %d, want %d", n, subscribers)
+					return
+				}
+			}
+			produced <- nil
+		}(producer)
+	}
+	for range publishers {
+		select {
+		case err := <-produced:
+			if err != nil {
+				t.Fatal(err)
+			}
+		case <-ctx.Done():
+			t.Fatal("publishers did not complete")
+		}
+	}
+	p.Close()
+	var first []int
+	for reader := range subscribers {
+		select {
+		case r := <-consumed:
+			if r.err != nil {
+				t.Fatal(r.err)
+			}
+			if len(r.values) != publishers*perPublisher {
+				t.Fatalf("reader %d received %d values, want %d", reader, len(r.values), publishers*perPublisher)
+			}
+			if reader == 0 {
+				first = r.values
+			} else if !slices.Equal(first, r.values) {
+				t.Fatal("subscribers observed different publication orders")
+			}
+		case <-ctx.Done():
+			t.Fatal("readers did not drain")
+		}
+	}
+	seen := make([]bool, publishers*perPublisher)
+	previous := make([]int, publishers)
+	for i := range previous {
+		previous[i] = -1
+	}
+	for _, v := range first {
+		if v < 0 || v >= len(seen) || seen[v] {
+			t.Fatalf("out-of-range or duplicate value %d", v)
+		}
+		seen[v] = true
+		producer, index := v/perPublisher, v%perPublisher
+		if index != previous[producer]+1 {
+			t.Fatalf("publisher %d order: got %d after %d", producer, index, previous[producer])
+		}
+		previous[producer] = index
+	}
+}
+
+func TestRingPubSubConcurrentSubscriptionChurn(t *testing.T) {
+	const publications, churners = 1000, 8
+	p := NewRingPubSub[int](4)
+	defer p.Close()
+	stable := p.Subscribe()
+	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
+	defer cancel()
+	results := make(chan error, churners+2)
+	go func() {
+		defer stable.Close()
+		for want := range publications {
+			got, err := stable.Recv(ctx)
+			if err != nil || got != want {
+				results <- fmt.Errorf("stable reader got (%d, %v), want %d", got, err, want)
+				return
+			}
+		}
+		results <- nil
+	}()
+	var ready sync.WaitGroup
+	ready.Add(churners)
+	for id := range churners {
+		go func(id int) {
+			ready.Done()
+			for iteration := range 100 {
+				s := p.Subscribe()
+				last := -1
+				for range (iteration + id) % 5 {
+					v, err := s.Recv(ctx)
+					if errors.Is(err, io.EOF) {
+						s.Close()
+						results <- nil
+						return
+					}
+					if err != nil || v < 0 || v >= publications || (last >= 0 && v != last+1) {
+						s.Close()
+						results <- fmt.Errorf("churn reader %d got (%d, %v) after %d", id, v, err, last)
+						return
+					}
+					last = v
+				}
+				s.Close()
+			}
+			results <- nil
+		}(id)
+	}
+	ready.Wait()
+	go func() {
+		for i := range publications {
+			if _, err := p.Send(ctx, i); err != nil {
+				results <- err
+				return
+			}
+		}
+		p.Close()
+		results <- nil
+	}()
+	for range churners + 2 {
+		select {
+		case err := <-results:
+			if err != nil {
+				t.Fatal(err)
+			}
+		case <-ctx.Done():
+			t.Fatal("subscription churn stalled progress")
+		}
+	}
+}
+
+// This oracle stores one ordinary FIFO per subscription. It has no knowledge
+// of ring indices, reference counts, notification lanes, or the implementation's
+// state. The longest FIFO determines retention because live readers receive
+// every publication made after their respective subscriptions.
+func ringTestModel(t *testing.T, capacity int, operations []byte) {
+	t.Helper()
+	p := NewRingPubSub[int](capacity)
+	defer p.Close()
+	type reader struct {
+		s       *RingSubscription[int]
+		pending []int
+	}
+	var readers [8]reader
+	defer func() {
+		for _, r := range readers {
+			if r.s != nil {
+				r.s.Close()
+			}
+		}
+	}()
+	serial := 0
+	for step, op := range operations {
+		index := int(op>>3) % len(readers)
+		r := &readers[index]
+		switch op & 7 {
+		case 0, 1:
+			if r.s == nil {
+				s := p.Subscribe()
+				r.s = &s
+			}
+		case 2, 3:
+			active, retained := 0, 0
+			for _, q := range readers {
+				if q.s != nil {
+					active++
+					retained = max(retained, len(q.pending))
+				}
+			}
+			serial++
+			n, err := p.TrySend(serial)
+			if retained == capacity {
+				if n != 0 || !errors.Is(err, ErrRingFull) {
+					t.Fatalf("step %d: full TrySend = (%d, %v)", step, n, err)
+				}
+			} else {
+				if n != active || err != nil {
+					t.Fatalf("step %d: TrySend = (%d, %v), want (%d, nil)", step, n, err, active)
+				}
+				for i := range readers {
+					if readers[i].s != nil {
+						readers[i].pending = append(readers[i].pending, serial)
+					}
+				}
+			}
+		case 4, 5:
+			if r.s == nil {
+				break
+			}
+			if op&7 == 5 {
+				select {
+				case <-r.s.Ready():
+					if len(r.pending) == 0 {
+						t.Fatalf("step %d: empty subscriber %d became ready", step, index)
+					}
+				default:
+					if len(r.pending) != 0 {
+						t.Fatalf("step %d: subscriber %d has data without a wake", step, index)
+					}
+				}
+			}
+			got, ok := r.s.TryRecv()
+			if len(r.pending) == 0 {
+				if got != 0 || ok {
+					t.Fatalf("step %d: empty TryRecv = (%d, %v)", step, got, ok)
+				}
+			} else {
+				if !ok || got != r.pending[0] {
+					t.Fatalf("step %d: TryRecv = (%d, %v), want %d", step, got, ok, r.pending[0])
+				}
+				r.pending = r.pending[1:]
+			}
+		case 6:
+			if r.s != nil {
+				var output [9]int
+				for i := range output {
+					output[i] = -1
+				}
+				size := int(op>>4) % len(output)
+				wantN := min(size, len(r.pending))
+				n := r.s.Read(output[:size])
+				if n != wantN || !slices.Equal(output[:n], r.pending[:wantN]) {
+					t.Fatalf("step %d: Read = %v (%d), want %v", step, output, n, r.pending[:wantN])
+				}
+				for _, v := range output[n:] {
+					if v != -1 {
+						t.Fatalf("step %d: Read changed output beyond returned count", step)
+					}
+				}
+				r.pending = r.pending[n:]
+			}
+		case 7:
+			if r.s != nil {
+				r.s.Close()
+				r.s.Close()
+				r.s, r.pending = nil, nil
+			}
+		}
+		for i := range readers {
+			if readers[i].s != nil && len(readers[i].pending) == 0 {
+				select {
+				case <-readers[i].s.Ready():
+					t.Fatalf("step %d: caught-up subscriber %d received another cohort's wake", step, i)
+				default:
+				}
+			}
+		}
+	}
+	p.Close()
+	for i := range readers {
+		r := &readers[i]
+		if r.s == nil {
+			continue
+		}
+		for _, want := range r.pending {
+			ringTestReceive(t, r.s, want)
+		}
+		if _, err := r.s.Recv(nil); !errors.Is(err, io.EOF) {
+			t.Fatalf("final subscriber %d drain = %v", i, err)
+		}
+	}
+}
+
+func TestRingPubSubModel(t *testing.T) {
+	for _, capacity := range []int{1, 2, 3, 7, 16} {
+		for seed := range uint64(8) {
+			t.Run(fmt.Sprintf("capacity=%d/seed=%d", capacity, seed), func(t *testing.T) {
+				rng := rand.New(rand.NewPCG(seed, seed+1))
+				operations := make([]byte, 2000)
+				for i := range operations {
+					operations[i] = byte(rng.Uint32())
+				}
+				ringTestModel(t, capacity, operations)
+			})
+		}
+	}
+}
+
+func FuzzRingPubSubModel(f *testing.F) {
+	f.Add([]byte{1, 0, 8, 2, 4, 13, 7, 2, 2, 2, 14, 15})
+	f.Add([]byte{3, 0, 8, 16, 2, 2, 2, 5, 13, 21, 7, 15, 23})
+	f.Add([]byte{7})
+	f.Fuzz(func(t *testing.T, input []byte) {
+		if len(input) == 0 {
+			return
+		}
+		if len(input) > 4097 {
+			input = input[:4097]
+		}
+		ringTestModel(t, int(input[0]%16)+1, input[1:])
+	})
+}
+
+func TestRingPubSubZeroSteadyStateAllocations(t *testing.T) {
+	t.Run("subscription churn", func(t *testing.T) {
+		p := NewRingPubSub[int](4)
+		defer p.Close()
+		if n := testing.AllocsPerRun(1000, func() {
+			s := p.Subscribe()
+			s.Close()
+		}); n != 0 {
+			t.Fatalf("Subscribe/Close allocated %.3f objects per cycle", n)
+		}
+	})
+	t.Run("publication and selected receive", func(t *testing.T) {
+		p := NewRingPubSub[int](4)
+		defer p.Close()
+		a, b := p.Subscribe(), p.Subscribe()
+		defer a.Close()
+		defer b.Close()
+		var failed bool
+		allocs := testing.AllocsPerRun(1000, func() {
+			if n, err := p.Send(nil, 42); n != 2 || err != nil {
+				failed = true
+			}
+			<-a.Ready()
+			if got, ok := a.TryRecv(); !ok || got != 42 {
+				failed = true
+			}
+			<-b.Ready()
+			if got, ok := b.TryRecv(); !ok || got != 42 {
+				failed = true
+			}
+		})
+		if failed || allocs != 0 {
+			t.Fatalf("Send/select/TryRecv: failure=%v, allocations=%.3f", failed, allocs)
+		}
+	})
+	t.Run("queued unsubscribe", func(t *testing.T) {
+		p := NewRingPubSub[int](4)
+		defer p.Close()
+		var failed bool
+		allocs := testing.AllocsPerRun(1000, func() {
+			s := p.Subscribe()
+			for i := range 4 {
+				if n, err := p.TrySend(i); n != 1 || err != nil {
+					failed = true
+				}
+			}
+			s.Close()
+		})
+		if failed || allocs != 0 {
+			t.Fatalf("Subscribe/publish/Close: failure=%v, allocations=%.3f", failed, allocs)
+		}
+	})
+	t.Run("batch read", func(t *testing.T) {
+		p := NewRingPubSub[int](4)
+		defer p.Close()
+		s := p.Subscribe()
+		defer s.Close()
+		var output [4]int
+		var failed bool
+		allocs := testing.AllocsPerRun(1000, func() {
+			for i := range output {
+				if _, err := p.TrySend(i); err != nil {
+					failed = true
+				}
+			}
+			if n := s.Read(output[:]); n != len(output) {
+				failed = true
+			}
+		})
+		if failed || allocs != 0 {
+			t.Fatalf("publish/Read: failure=%v, allocations=%.3f", failed, allocs)
+		}
+	})
+}
diff --git a/ringpubsub_example_test.go b/ringpubsub_example_test.go
new file mode 100644
--- /dev/null
+++ b/ringpubsub_example_test.go
@@ -0,0 +1,78 @@
+/*
+   Copyright 2026 Joseph Cumines
+
+   Licensed under the Apache License, Version 2.0 (the "License");
+   you may not use this file except in compliance with the License.
+   You may obtain a copy of the License at
+
+       http://www.apache.org/licenses/LICENSE-2.0
+
+   Unless required by applicable law or agreed to in writing, software
+   distributed under the License is distributed on an "AS IS" BASIS,
+   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+   See the License for the specific language governing permissions and
+   limitations under the License.
+*/
+
+package bigbuff_test
+
+import (
+	"fmt"
+
+	"github.com/joeycumines/go-bigbuff"
+)
+
+// One goroutine can consume multiple subscriptions: advancing one cursor never
+// waits for another cursor to acknowledge the same publication.
+func ExampleRingPubSub_select() {
+	hub := bigbuff.NewRingPubSub[int](2)
+	first, second := hub.Subscribe(), hub.Subscribe()
+	defer first.Close()
+	defer second.Close()
+
+	for _, value := range []int{10, 20} {
+		if _, err := hub.TrySend(value); err != nil {
+			panic(err)
+		}
+	}
+	hub.Close() // Existing subscriptions still receive both queued values.
+
+	firstReady, secondReady := first.Ready(), second.Ready()
+	var firstValues, secondValues []int
+	for firstReady != nil || secondReady != nil {
+		select {
+		case _, open := <-firstReady:
+			if !open {
+				first.Close()
+				firstReady = nil // Disable this case after EOF.
+				continue
+			}
+			value, ok := first.TryRecv()
+			if !ok {
+				panic("ready subscription has no value")
+			}
+			firstValues = append(firstValues, value)
+			firstReady = first.Ready() // The cursor's next lane has changed.
+
+		case _, open := <-secondReady:
+			if !open {
+				second.Close()
+				secondReady = nil // Close alone would leave a ready EOF case.
+				continue
+			}
+			value, ok := second.TryRecv()
+			if !ok {
+				panic("ready subscription has no value")
+			}
+			secondValues = append(secondValues, value)
+			secondReady = second.Ready()
+		}
+	}
+
+	// Select order is unspecified; each subscription's value order is fixed.
+	fmt.Println("first:", firstValues)
+	fmt.Println("second:", secondValues)
+	// Output:
+	// first: [10 20]
+	// second: [10 20]
+}
diff --git a/broadcast_bench_test.go b/broadcast_bench_test.go
new file mode 100644
--- /dev/null
+++ b/broadcast_bench_test.go
@@ -0,0 +1,399 @@
+/*
+   Copyright 2026 Joseph Cumines
+
+   Licensed under the Apache License, Version 2.0 (the "License");
+   you may not use this file except in compliance with the License.
+   You may obtain a copy of the License at
+
+       http://www.apache.org/licenses/LICENSE-2.0
+
+   Unless required by applicable law or agreed to in writing, software
+   distributed under the License is distributed on an "AS IS" BASIS,
+   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+   See the License for the specific language governing permissions and
+   limitations under the License.
+*/
+
+package bigbuff
+
+import (
+	"fmt"
+	"sync"
+	"testing"
+)
+
+// BenchmarkBroadcast measures complete delivery, including the final drain, not
+// merely publication/admission. Every receiver checks every value's order and
+// computes a checksum. Setup, goroutine creation, channel creation, warmup, and
+// final unsubscription are outside the timed region. Receivers use an actual
+// two-case select, with a live cancellation channel, when waiting.
+//
+// LinearChannels is a synthetic best-case linear fan-out over a fixed slice of
+// per-subscriber channels; it has no registry lookup/lock overhead. It is not a
+// reconstruction of an unspecified callback implementation. A buffer of B gives
+// each subscriber B slots, whereas RingPubSub shares B payload slots in total.
+//
+// RingFast attempts TryRecv before waiting. RingSelect always uses Ready in a
+// select first. Both accept false/spurious readiness by retrying TryRecv.
+func BenchmarkBroadcast(b *testing.B) {
+	for _, n := range []int{1, 8, 64, 512} {
+		b.Run(fmt.Sprintf("ChanPubSub/N=%d", n), func(b *testing.B) {
+			benchmarkChanPubSub(b, n)
+		})
+		for _, capacity := range []int{1, 64} {
+			b.Run(fmt.Sprintf("RingFast/B=%d/N=%d", capacity, n), func(b *testing.B) {
+				benchmarkRingPubSub(b, n, capacity, false)
+			})
+			b.Run(fmt.Sprintf("RingSelect/B=%d/N=%d", capacity, n), func(b *testing.B) {
+				benchmarkRingPubSub(b, n, capacity, true)
+			})
+		}
+		for _, capacity := range []int{0, 64} {
+			b.Run(fmt.Sprintf("LinearChannels/B=%d/N=%d", capacity, n), func(b *testing.B) {
+				benchmarkLinearChannels(b, n, capacity)
+			})
+		}
+	}
+}
+
+type broadcastBenchResult struct {
+	sum uint64
+	bad bool
+}
+
+func benchmarkCheckResults(b *testing.B, results []broadcastBenchResult) {
+	b.Helper()
+	n := uint64(b.N)
+	want := n * ((n - 1) / 2)
+	if n%2 == 0 {
+		want = (n / 2) * (n - 1)
+	}
+	for i, got := range results {
+		if got.bad || got.sum != want {
+			b.Fatalf("receiver %d: order failure=%v checksum=%d; want %d", i, got.bad, got.sum, want)
+		}
+	}
+}
+
+func benchmarkChanPubSub(b *testing.B, n int) {
+	x := NewChanPubSub(make(chan uint64))
+	c := x.C()
+	x.Add(n)
+	stop := make(chan struct{})
+	results := make([]broadcastBenchResult, n)
+	var warmed, done sync.WaitGroup
+	warmed.Add(n)
+	done.Add(n)
+	for id := range n {
+		go func() {
+			defer done.Done()
+			select {
+			case <-c:
+				x.Wait()
+			case <-stop:
+				panic("benchmark stopped during warmup")
+			}
+			warmed.Done()
+			var result broadcastBenchResult
+			for i := 0; i < b.N; i++ {
+				select {
+				case v := <-c:
+					x.Wait()
+					result.sum += v
+					if v != uint64(i) {
+						result.bad = true
+					}
+				case <-stop:
+					panic("benchmark stopped before delivery completed")
+				}
+			}
+			results[id] = result
+		}()
+	}
+	x.Send(0)
+	warmed.Wait()
+	b.ReportAllocs()
+	b.SetBytes(int64(n * 8))
+	b.ResetTimer()
+	for i := 0; i < b.N; i++ {
+		if got := x.Send(uint64(i)); got != n {
+			b.Fatalf("sent to %d subscribers; want %d", got, n)
+		}
+	}
+	done.Wait()
+	b.StopTimer()
+	x.Add(-n)
+	close(stop)
+	benchmarkCheckResults(b, results)
+}
+
+func benchmarkRingPubSub(b *testing.B, n, capacity int, alwaysSelect bool) {
+	x := NewRingPubSub[uint64](capacity)
+	subs := make([]RingSubscription[uint64], n)
+	for i := range subs {
+		subs[i] = x.Subscribe()
+	}
+	stop := make(chan struct{})
+	results := make([]broadcastBenchResult, n)
+	var warmed, done sync.WaitGroup
+	warmed.Add(n)
+	done.Add(n)
+	for id := range n {
+		go func() {
+			defer done.Done()
+			s := &subs[id]
+			benchmarkRingReceive(s, stop, alwaysSelect)
+			warmed.Done()
+			var result broadcastBenchResult
+			for i := 0; i < b.N; i++ {
+				v := benchmarkRingReceive(s, stop, alwaysSelect)
+				result.sum += v
+				if v != uint64(i) {
+					result.bad = true
+				}
+			}
+			results[id] = result
+		}()
+	}
+	if _, err := x.Send(nil, 0); err != nil {
+		b.Fatal(err)
+	}
+	warmed.Wait()
+	b.ReportAllocs()
+	b.SetBytes(int64(n * 8))
+	b.ResetTimer()
+	for i := 0; i < b.N; i++ {
+		if got, err := x.Send(nil, uint64(i)); err != nil || got != n {
+			b.Fatalf("sent to %d subscribers, error %v; want %d", got, err, n)
+		}
+	}
+	done.Wait()
+	b.StopTimer()
+	for i := range subs {
+		subs[i].Close()
+	}
+	x.Close()
+	close(stop)
+	benchmarkCheckResults(b, results)
+}
+
+func benchmarkRingReceive(s *RingSubscription[uint64], stop <-chan struct{}, alwaysSelect bool) uint64 {
+	for {
+		if !alwaysSelect {
+			if v, ok := s.TryRecv(); ok {
+				return v
+			}
+		}
+		select {
+		case <-s.Ready():
+			if v, ok := s.TryRecv(); ok {
+				return v
+			}
+		case <-stop:
+			panic("benchmark stopped before delivery completed")
+		}
+	}
+}
+
+func benchmarkLinearChannels(b *testing.B, n, capacity int) {
+	channels := make([]chan uint64, n)
+	for i := range channels {
+		channels[i] = make(chan uint64, capacity)
+	}
+	stop := make(chan struct{})
+	results := make([]broadcastBenchResult, n)
+	var warmed, done sync.WaitGroup
+	warmed.Add(n)
+	done.Add(n)
+	for id, c := range channels {
+		go func() {
+			defer done.Done()
+			select {
+			case <-c:
+			case <-stop:
+				panic("benchmark stopped during warmup")
+			}
+			warmed.Done()
+			var result broadcastBenchResult
+			for i := 0; i < b.N; i++ {
+				select {
+				case v := <-c:
+					result.sum += v
+					if v != uint64(i) {
+						result.bad = true
+					}
+				case <-stop:
+					panic("benchmark stopped before delivery completed")
+				}
+			}
+			results[id] = result
+		}()
+	}
+	for _, c := range channels {
+		c <- 0
+	}
+	warmed.Wait()
+	b.ReportAllocs()
+	b.SetBytes(int64(n * 8))
+	b.ResetTimer()
+	for i := 0; i < b.N; i++ {
+		for _, c := range channels {
+			c <- uint64(i)
+		}
+	}
+	done.Wait()
+	b.StopTimer()
+	close(stop)
+	benchmarkCheckResults(b, results)
+}
+
+// BenchmarkInlineCallbacks is a sequential dispatch floor: every callback runs
+// synchronously inside the publisher goroutine. It has neither independent
+// receiver goroutines nor native per-callback select. It is intentionally kept
+// separate from BenchmarkBroadcast because those are material semantic changes.
+func BenchmarkInlineCallbacks(b *testing.B) {
+	for _, n := range []int{1, 8, 64, 512} {
+		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
+			results := make([]broadcastBenchResult, n)
+			expected := make([]uint64, n)
+			callbacks := make([]func(uint64), n)
+			for id := range callbacks {
+				callbacks[id] = func(v uint64) {
+					results[id].sum += v
+					if v != expected[id] {
+						results[id].bad = true
+					}
+					expected[id]++
+				}
+			}
+			b.ReportAllocs()
+			b.SetBytes(int64(n * 8))
+			b.ResetTimer()
+			for i := 0; i < b.N; i++ {
+				for _, callback := range callbacks {
+					callback(uint64(i))
+				}
+			}
+			b.StopTimer()
+			benchmarkCheckResults(b, results)
+		})
+	}
+}
+
+// BenchmarkSubscriptionChurn isolates membership lifetimes with no backlog.
+// FreshChannel is a deliberately optimistic lower bound for a design creating a
+// channel per lifetime: it omits registry management and any helper goroutine.
+// It is not a claim that all channel-based designs must allocate on each reuse.
+func BenchmarkSubscriptionChurn(b *testing.B) {
+	b.Run("ChanCaster", func(b *testing.B) {
+		x := NewChanCaster(make(chan uint64))
+		b.ReportAllocs()
+		b.ResetTimer()
+		for i := 0; i < b.N; i++ {
+			x.Add(1)
+			x.Add(-1)
+		}
+	})
+	b.Run("ChanPubSub", func(b *testing.B) {
+		x := NewChanPubSub(make(chan uint64))
+		b.ReportAllocs()
+		b.ResetTimer()
+		for i := 0; i < b.N; i++ {
+			x.Subscribe()
+			x.Unsubscribe()
+		}
+	})
+	b.Run("RingPubSub", func(b *testing.B) {
+		x := NewRingPubSub[uint64](64)
+		b.ReportAllocs()
+		b.ResetTimer()
+		for i := 0; i < b.N; i++ {
+			s := x.Subscribe()
+			s.Close()
+		}
+	})
+	for _, capacity := range []int{0, 64} {
+		b.Run(fmt.Sprintf("FreshChannel/B=%d", capacity), func(b *testing.B) {
+			b.ReportAllocs()
+			b.ResetTimer()
+			for i := 0; i < b.N; i++ {
+				c := make(chan uint64, capacity)
+				close(c)
+			}
+		})
+	}
+}
+
+// BenchmarkRingQueuedLifecycle includes Subscribe, filling B messages, and
+// abandoning all B through Close. Thus ns/op is a complete queued lifecycle,
+// not cancellation latency alone. No sleeping receiver or helper goroutine is
+// needed to retire unread obligations.
+func BenchmarkRingQueuedLifecycle(b *testing.B) {
+	for _, capacity := range []int{1, 64, 1024} {
+		b.Run(fmt.Sprintf("B=%d", capacity), func(b *testing.B) {
+			x := NewRingPubSub[uint64](capacity)
+			b.ReportAllocs()
+			b.ResetTimer()
+			for i := 0; i < b.N; i++ {
+				s := x.Subscribe()
+				for j := 0; j < capacity; j++ {
+					if got, err := x.TrySend(uint64(j)); err != nil || got != 1 {
+						b.Fatalf("sent to %d subscribers, error %v; want 1", got, err)
+					}
+				}
+				s.Close()
+			}
+			b.StopTimer()
+			b.ReportMetric(float64(capacity), "queued/op")
+		})
+	}
+}
+
+var (
+	benchmarkConstructedCaster   *ChanCaster[chan uint64, uint64]
+	benchmarkConstructedPubSub   *ChanPubSub[chan uint64, uint64]
+	benchmarkConstructedRing     *RingPubSub[uint64]
+	benchmarkConstructedChannels []chan uint64
+)
+
+// BenchmarkConstruction measures allocation at construction, separately from
+// steady-state operation. Global sinks deliberately model an escaping hub.
+// The originals include construction of their externally supplied channel.
+// LinearChannels includes a slice and one channel per subscriber; RingPubSub
+// includes only shared infrastructure, not caller-owned cursor/goroutine storage.
+func BenchmarkConstruction(b *testing.B) {
+	b.Run("ChanCaster", func(b *testing.B) {
+		b.ReportAllocs()
+		for i := 0; i < b.N; i++ {
+			benchmarkConstructedCaster = NewChanCaster(make(chan uint64))
+		}
+	})
+	b.Run("ChanPubSub", func(b *testing.B) {
+		b.ReportAllocs()
+		for i := 0; i < b.N; i++ {
+			benchmarkConstructedPubSub = NewChanPubSub(make(chan uint64))
+		}
+	})
+	for _, capacity := range []int{1, 64, 1024} {
+		b.Run(fmt.Sprintf("RingPubSub/B=%d", capacity), func(b *testing.B) {
+			b.ReportAllocs()
+			for i := 0; i < b.N; i++ {
+				benchmarkConstructedRing = NewRingPubSub[uint64](capacity)
+			}
+		})
+	}
+	for _, n := range []int{1, 64, 512} {
+		for _, capacity := range []int{0, 64} {
+			b.Run(fmt.Sprintf("LinearChannels/B=%d/N=%d", capacity, n), func(b *testing.B) {
+				b.ReportAllocs()
+				for i := 0; i < b.N; i++ {
+					channels := make([]chan uint64, n)
+					for j := range channels {
+						channels[j] = make(chan uint64, capacity)
+					}
+					benchmarkConstructedChannels = channels
+				}
+			})
+		}
+	}
+}
diff --git a/broadcast_scale_test.go b/broadcast_scale_test.go
new file mode 100644
--- /dev/null
+++ b/broadcast_scale_test.go
@@ -0,0 +1,43 @@
+/*
+   Copyright 2026 Joseph Cumines
+
+   Licensed under the Apache License, Version 2.0 (the "License");
+   you may not use this file except in compliance with the License.
+   You may obtain a copy of the License at
+
+       http://www.apache.org/licenses/LICENSE-2.0
+
+   Unless required by applicable law or agreed to in writing, software
+   distributed under the License is distributed on an "AS IS" BASIS,
+   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+   See the License for the specific language governing permissions and
+   limitations under the License.
+*/
+
+package bigbuff
+
+import (
+	"fmt"
+	"testing"
+)
+
+// The same complete-delivery benchmark at larger subscriber counts. Keep it
+// separate so ordinary benchmark runs can choose the smaller, cheaper matrix.
+func BenchmarkBroadcastLarge(b *testing.B) {
+	for _, n := range []int{4096, 16384} {
+		b.Run(fmt.Sprintf("ChanPubSub/N=%d", n), func(b *testing.B) {
+			benchmarkChanPubSub(b, n)
+		})
+		for _, capacity := range []int{1, 64} {
+			b.Run(fmt.Sprintf("RingFast/B=%d/N=%d", capacity, n), func(b *testing.B) {
+				benchmarkRingPubSub(b, n, capacity, false)
+			})
+			b.Run(fmt.Sprintf("RingSelect/B=%d/N=%d", capacity, n), func(b *testing.B) {
+				benchmarkRingPubSub(b, n, capacity, true)
+			})
+		}
+		b.Run(fmt.Sprintf("LinearChannels/B=64/N=%d", n), func(b *testing.B) {
+			benchmarkLinearChannels(b, n, 64)
+		})
+	}
+}
-- SHA256SUMS --
25b59378614879acb09cfe20c3f38e42058511400bc92e68854c3e4803a5c83a  DESIGN.md
08e1faf9209cf2b9f1c94734ba4fb13d5d145e9fd4d0af3fe2a4f588589d76ee  LICENSE
90d200fa318a35ae4a30ccb351fa1390543b1a1bbea4c4f6a75fb0c23b01a3ae  README.md
3bd5f6904db106489738a89418d14fa9f3fb33bfcf9784dfe9b55e4a4449a9d7  RESULTS.md
7961a14f2e140e74d3ca8a3491e1c305b6c18886341a2f2748f7c0d5271b6552  evidence/allocation-and-example.txt
aa8733875298c799246493d3dd14a10670492f7936dfe38dfc351d9a4776f601  evidence/bench-final-supplement.txt
7e88b86f7fd38d02b4b88500f57c80999fc01ad01e370014560d3fa174810f17  evidence/bench-final.txt
9b99aa6184eb3334ef7553ef5b921f5eb6932a24274e14af63f5056927746ea9  evidence/bench-initial.txt
32eb35cfc9ef91d29431461846956c9e68eaa43d432ce0d18643d46f4de8b8ff  evidence/bench-large.txt
3bf8e246c6bc167725205822d48e17a197af4248449490309b950e63865c09c7  evidence/bench-last-skip.txt
5ae2816c3d2b5a621a75f03c995142304d65b26eaa5aa0f2477a5251c2bbb767  evidence/bench-relay-base-after.txt
e965de769b942359afd8d0a87f82bf96a19c16ff7d6b515d4fc6028a0a109095  evidence/bench-relay-base.txt
c8e950fe5f89e2856736afc19e58081d459f278a7b8631e617313f951a11cb53  evidence/bench-relay.txt
fe24bac38ee18093025fe4687a9cfadb3c76f260c5cd0a116f1af92247ec7479  evidence/bench-resources.txt
f095e5d32751f996e5d544f0f3c61830d29666850e9e81256ab37c1fd7e6d906  evidence/benchmark-summary.json
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  evidence/build-386.txt
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  evidence/build-darwin-arm64.txt
744ec7ed0aa0109cd320aa70a2598b832609abf94ea5b3b628cd33c8d986bf63  evidence/fuzz.txt
5957c4f6cf190372f6a6e1a710c9aacf567e74479a818b05b232850095b50837  evidence/integration-race.txt
f9cd3b3891df3819bb7e6c6d9e75d2226a600e43992e79a139a5e969f018361f  evidence/integration-source-manifest.json
8297b06bf42a3be5c0105456a9f709f467b1b524ea0a571c32ebefdb8f6dce8f  evidence/integration-test.txt
5d338bbc4458620ce0c2e34dad15962f3e9afeb68e7df2472774d2ca2b510166  evidence/last-skip-experiment.patch
ec0313228768f0bf01d75cc8c5999ff1bc6b96193006937619d302466c164f8f  evidence/race-final.txt
273c790af0673a51812cb38754e4cdae39dda1cb133a13e37ed71fdfc7edcde6  evidence/relay-experiment.patch
18b2fe12fd53717fc1080494ca0f99e69dd5f50b5962d5d65f054db8a3c839f5  evidence/test-386.txt
ed403386a59d650e891b6a11fbc9756a3e0765a0d0c6c3ee8edb9d443e37e17b  evidence/test-last-skip.txt
f860e95d042107d54af6e242103a49f6af349a1d37e1a5ed4396459e5bf050e5  evidence/test-relay.txt
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  evidence/vet.txt
defa8ffd263e94b84c9e067703a567dd55ac4df91bdbe0b5587aa44aaf330001  ringpubsub.patch
dbf7676ebe1f282a85243685c9990635e69e54260fe263eaf53ba327b5e26d05  source/broadcast_bench_test.go
33044f372da9b4f4a6e32db4db02e972a4c0732f7994c8de190a7179d0e2a804  source/broadcast_scale_test.go
b40257bb36cb99c0d45338d3f9269f2f05f3c4e124e25fce73ce39c8de60a18a  source/chancaster.go
c10d2c0eefe49a8a62fc6d2dc1126818bf3e2ded7b2b41bd3cff69c58b7ffb31  source/chanpubsub.go
46ec2c790e5631194c81d063fc8abecfe2ac9e19618e462a3e06a82c072dd8bc  source/go.mod
cf96b7d7738173cc5d71e290d8a83782bec3c912abac2baac7305a5b44a5678d  source/ringpubsub.go
2bfbee4044a4fc29c52053a1a66d5e919970b6273d439d13e6b7ec5a41c265ea  source/ringpubsub_example_test.go
20d2efcc3b12869fe8a1dd8949528050206331c8a280d575895714f6d18e8577  source/ringpubsub_test.go
-- source/broadcast_bench_test.go --
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
	"testing"
)

// BenchmarkBroadcast measures complete delivery, including the final drain, not
// merely publication/admission. Every receiver checks every value's order and
// computes a checksum. Setup, goroutine creation, channel creation, warmup, and
// final unsubscription are outside the timed region. Receivers use an actual
// two-case select, with a live cancellation channel, when waiting.
//
// LinearChannels is a synthetic best-case linear fan-out over a fixed slice of
// per-subscriber channels; it has no registry lookup/lock overhead. It is not a
// reconstruction of an unspecified callback implementation. A buffer of B gives
// each subscriber B slots, whereas RingPubSub shares B payload slots in total.
//
// RingFast attempts TryRecv before waiting. RingSelect always uses Ready in a
// select first. Both accept false/spurious readiness by retrying TryRecv.
func BenchmarkBroadcast(b *testing.B) {
	for _, n := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("ChanPubSub/N=%d", n), func(b *testing.B) {
			benchmarkChanPubSub(b, n)
		})
		for _, capacity := range []int{1, 64} {
			b.Run(fmt.Sprintf("RingFast/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, false)
			})
			b.Run(fmt.Sprintf("RingSelect/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, true)
			})
		}
		for _, capacity := range []int{0, 64} {
			b.Run(fmt.Sprintf("LinearChannels/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkLinearChannels(b, n, capacity)
			})
		}
	}
}

type broadcastBenchResult struct {
	sum uint64
	bad bool
}

func benchmarkCheckResults(b *testing.B, results []broadcastBenchResult) {
	b.Helper()
	n := uint64(b.N)
	want := n * ((n - 1) / 2)
	if n%2 == 0 {
		want = (n / 2) * (n - 1)
	}
	for i, got := range results {
		if got.bad || got.sum != want {
			b.Fatalf("receiver %d: order failure=%v checksum=%d; want %d", i, got.bad, got.sum, want)
		}
	}
}

func benchmarkChanPubSub(b *testing.B, n int) {
	x := NewChanPubSub(make(chan uint64))
	c := x.C()
	x.Add(n)
	stop := make(chan struct{})
	results := make([]broadcastBenchResult, n)
	var warmed, done sync.WaitGroup
	warmed.Add(n)
	done.Add(n)
	for id := range n {
		go func() {
			defer done.Done()
			select {
			case <-c:
				x.Wait()
			case <-stop:
				panic("benchmark stopped during warmup")
			}
			warmed.Done()
			var result broadcastBenchResult
			for i := 0; i < b.N; i++ {
				select {
				case v := <-c:
					x.Wait()
					result.sum += v
					if v != uint64(i) {
						result.bad = true
					}
				case <-stop:
					panic("benchmark stopped before delivery completed")
				}
			}
			results[id] = result
		}()
	}
	x.Send(0)
	warmed.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := x.Send(uint64(i)); got != n {
			b.Fatalf("sent to %d subscribers; want %d", got, n)
		}
	}
	done.Wait()
	b.StopTimer()
	x.Add(-n)
	close(stop)
	benchmarkCheckResults(b, results)
}

func benchmarkRingPubSub(b *testing.B, n, capacity int, alwaysSelect bool) {
	x := NewRingPubSub[uint64](capacity)
	subs := make([]RingSubscription[uint64], n)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	stop := make(chan struct{})
	results := make([]broadcastBenchResult, n)
	var warmed, done sync.WaitGroup
	warmed.Add(n)
	done.Add(n)
	for id := range n {
		go func() {
			defer done.Done()
			s := &subs[id]
			benchmarkRingReceive(s, stop, alwaysSelect)
			warmed.Done()
			var result broadcastBenchResult
			for i := 0; i < b.N; i++ {
				v := benchmarkRingReceive(s, stop, alwaysSelect)
				result.sum += v
				if v != uint64(i) {
					result.bad = true
				}
			}
			results[id] = result
		}()
	}
	if _, err := x.Send(nil, 0); err != nil {
		b.Fatal(err)
	}
	warmed.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got, err := x.Send(nil, uint64(i)); err != nil || got != n {
			b.Fatalf("sent to %d subscribers, error %v; want %d", got, err, n)
		}
	}
	done.Wait()
	b.StopTimer()
	for i := range subs {
		subs[i].Close()
	}
	x.Close()
	close(stop)
	benchmarkCheckResults(b, results)
}

func benchmarkRingReceive(s *RingSubscription[uint64], stop <-chan struct{}, alwaysSelect bool) uint64 {
	for {
		if !alwaysSelect {
			if v, ok := s.TryRecv(); ok {
				return v
			}
		}
		select {
		case <-s.Ready():
			if v, ok := s.TryRecv(); ok {
				return v
			}
		case <-stop:
			panic("benchmark stopped before delivery completed")
		}
	}
}

func benchmarkLinearChannels(b *testing.B, n, capacity int) {
	channels := make([]chan uint64, n)
	for i := range channels {
		channels[i] = make(chan uint64, capacity)
	}
	stop := make(chan struct{})
	results := make([]broadcastBenchResult, n)
	var warmed, done sync.WaitGroup
	warmed.Add(n)
	done.Add(n)
	for id, c := range channels {
		go func() {
			defer done.Done()
			select {
			case <-c:
			case <-stop:
				panic("benchmark stopped during warmup")
			}
			warmed.Done()
			var result broadcastBenchResult
			for i := 0; i < b.N; i++ {
				select {
				case v := <-c:
					result.sum += v
					if v != uint64(i) {
						result.bad = true
					}
				case <-stop:
					panic("benchmark stopped before delivery completed")
				}
			}
			results[id] = result
		}()
	}
	for _, c := range channels {
		c <- 0
	}
	warmed.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, c := range channels {
			c <- uint64(i)
		}
	}
	done.Wait()
	b.StopTimer()
	close(stop)
	benchmarkCheckResults(b, results)
}

// BenchmarkInlineCallbacks is a sequential dispatch floor: every callback runs
// synchronously inside the publisher goroutine. It has neither independent
// receiver goroutines nor native per-callback select. It is intentionally kept
// separate from BenchmarkBroadcast because those are material semantic changes.
func BenchmarkInlineCallbacks(b *testing.B) {
	for _, n := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			results := make([]broadcastBenchResult, n)
			expected := make([]uint64, n)
			callbacks := make([]func(uint64), n)
			for id := range callbacks {
				callbacks[id] = func(v uint64) {
					results[id].sum += v
					if v != expected[id] {
						results[id].bad = true
					}
					expected[id]++
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, callback := range callbacks {
					callback(uint64(i))
				}
			}
			b.StopTimer()
			benchmarkCheckResults(b, results)
		})
	}
}

// BenchmarkSubscriptionChurn isolates membership lifetimes with no backlog.
// FreshChannel is a deliberately optimistic lower bound for a design creating a
// channel per lifetime: it omits registry management and any helper goroutine.
// It is not a claim that all channel-based designs must allocate on each reuse.
func BenchmarkSubscriptionChurn(b *testing.B) {
	b.Run("ChanCaster", func(b *testing.B) {
		x := NewChanCaster(make(chan uint64))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x.Add(1)
			x.Add(-1)
		}
	})
	b.Run("ChanPubSub", func(b *testing.B) {
		x := NewChanPubSub(make(chan uint64))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x.Subscribe()
			x.Unsubscribe()
		}
	})
	b.Run("RingPubSub", func(b *testing.B) {
		x := NewRingPubSub[uint64](64)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := x.Subscribe()
			s.Close()
		}
	})
	for _, capacity := range []int{0, 64} {
		b.Run(fmt.Sprintf("FreshChannel/B=%d", capacity), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := make(chan uint64, capacity)
				close(c)
			}
		})
	}
}

// BenchmarkRingQueuedLifecycle includes Subscribe, filling B messages, and
// abandoning all B through Close. Thus ns/op is a complete queued lifecycle,
// not cancellation latency alone. No sleeping receiver or helper goroutine is
// needed to retire unread obligations.
func BenchmarkRingQueuedLifecycle(b *testing.B) {
	for _, capacity := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("B=%d", capacity), func(b *testing.B) {
			x := NewRingPubSub[uint64](capacity)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := x.Subscribe()
				for j := 0; j < capacity; j++ {
					if got, err := x.TrySend(uint64(j)); err != nil || got != 1 {
						b.Fatalf("sent to %d subscribers, error %v; want 1", got, err)
					}
				}
				s.Close()
			}
			b.StopTimer()
			b.ReportMetric(float64(capacity), "queued/op")
		})
	}
}

var (
	benchmarkConstructedCaster   *ChanCaster[chan uint64, uint64]
	benchmarkConstructedPubSub   *ChanPubSub[chan uint64, uint64]
	benchmarkConstructedRing     *RingPubSub[uint64]
	benchmarkConstructedChannels []chan uint64
)

// BenchmarkConstruction measures allocation at construction, separately from
// steady-state operation. Global sinks deliberately model an escaping hub.
// The originals include construction of their externally supplied channel.
// LinearChannels includes a slice and one channel per subscriber; RingPubSub
// includes only shared infrastructure, not caller-owned cursor/goroutine storage.
func BenchmarkConstruction(b *testing.B) {
	b.Run("ChanCaster", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkConstructedCaster = NewChanCaster(make(chan uint64))
		}
	})
	b.Run("ChanPubSub", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkConstructedPubSub = NewChanPubSub(make(chan uint64))
		}
	})
	for _, capacity := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("RingPubSub/B=%d", capacity), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkConstructedRing = NewRingPubSub[uint64](capacity)
			}
		})
	}
	for _, n := range []int{1, 64, 512} {
		for _, capacity := range []int{0, 64} {
			b.Run(fmt.Sprintf("LinearChannels/B=%d/N=%d", capacity, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					channels := make([]chan uint64, n)
					for j := range channels {
						channels[j] = make(chan uint64, capacity)
					}
					benchmarkConstructedChannels = channels
				}
			})
		}
	}
}
-- source/broadcast_scale_test.go --
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
	"testing"
)

// The same complete-delivery benchmark at larger subscriber counts. Keep it
// separate so ordinary benchmark runs can choose the smaller, cheaper matrix.
func BenchmarkBroadcastLarge(b *testing.B) {
	for _, n := range []int{4096, 16384} {
		b.Run(fmt.Sprintf("ChanPubSub/N=%d", n), func(b *testing.B) {
			benchmarkChanPubSub(b, n)
		})
		for _, capacity := range []int{1, 64} {
			b.Run(fmt.Sprintf("RingFast/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, false)
			})
			b.Run(fmt.Sprintf("RingSelect/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, true)
			})
		}
		b.Run(fmt.Sprintf("LinearChannels/B=64/N=%d", n), func(b *testing.B) {
			benchmarkLinearChannels(b, n, 64)
		})
	}
}
-- source/chancaster.go --
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

-- source/chanpubsub.go --
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

-- source/go.mod --
module github.com/joeycumines/go-bigbuff

go 1.23.4
-- source/ringpubsub.go --
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
	"io"
	"sync"
	"sync/atomic"
)

// ErrRingFull indicates that a RingPubSub cannot admit another value until a
// reader consumes or abandons an outstanding value. No value was published.
var ErrRingFull = errors.New("bigbuff: ringpubsub: full")

// A shared terminal channel makes inactive, including zero-value,
// subscriptions selectable without allocating a channel per subscription.
var ringPubSubInactive = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// RingPubSub is a bounded, lossless broadcast log with selectable subscriptions.
// Each accepted value belongs to every subscription active at publication.
// Readers consume in publication order, independently, with no broadcast-wide
// acknowledgement barrier. A slow reader eventually backpressures publishers.
//
// Unlike ChanPubSub.Send, Send reports admission, NOT completed delivery. Its
// result counts subscriptions at publication, including any that subsequently
// abandon that value. Closing a subscription explicitly abandons its backlog.
//
// The hub has no subscriber registry, per-subscriber channel, callback, or
// background goroutine. Construction allocates a fixed ring and capacity+1
// reusable notification channels. Subscribe returns a small caller-owned value;
// whether that value escapes to the heap depends on its caller. The methods do
// not create per-publication, per-receive, or per-subscription infrastructure.
// Escape analysis, large value temporaries, runtime parking, and caller-owned
// values can still allocate. See the allocation tests and benchmarks.
//
// Publishers and distinct subscriptions may operate concurrently. Each individual
// subscription has one owner; see RingSubscription for its select contract.
// Construct with NewRingPubSub. A hub must not be copied after construction.
type RingPubSub[V any] struct {
	mu          sync.Mutex
	slots       []ringPubSubSlot[V]
	space       chan struct{}
	head        int  // oldest unreclaimed slot; guarded by mu
	used        int  // published but not yet reclaimed slots; guarded by mu
	subscribers int  // guarded by mu
	closed      bool // guarded by mu
	// tail is a physical index, NOT a wrapping message sequence. Release via
	// Store publishes slot initialization to readers that observe this index.
	tail atomic.Uint64
}

type ringPubSubSlot[V any] struct {
	// value is immutable from publication until the last obligation is released.
	value     V
	remaining atomic.Int64
	ready     chan struct{}
}

// RingSubscription is a single-owner cursor in a RingPubSub. Its zero value is
// inactive. An active subscription MUST NOT be copied. Transfer ownership to
// another goroutine by passing its address, or move a newly returned value once
// before use and never use the original again. All methods, and receives from
// Ready, must be serialized for this individual subscription.
//
// Every successful receive from Ready MUST be followed promptly by TryRecv,
// Read, Recv, or Close. A wakeup is a shared notification, not a value or an
// acknowledgement. Keeping it without advancing or closing can delay other
// readers. If another select case wins, no repair is needed: the subscription
// may continue later or Close to abandon its backlog.
//
// Close when finished. An abandoned active cursor can retain values and block
// publishers indefinitely, just like a reader that has stopped consuming.
type RingSubscription[V any] struct {
	noCopy ringPubSubNoCopy
	hub    *RingPubSub[V]
	next   int
}

// A vet marker, deliberately not a self-pointer: self-pointer copy checks would
// force otherwise stack-resident subscriptions to escape.
type ringPubSubNoCopy struct{}

func (*ringPubSubNoCopy) Lock()   {}
func (*ringPubSubNoCopy) Unlock() {}

// NewRingPubSub creates a broadcast log retaining at most capacity values.
// Capacity must be positive and must leave room for capacity+1 physical slots.
// The extra slot is essential: a caught-up cursor must never select a channel
// that still contains a notification for an older generation of that slot.
func NewRingPubSub[V any](capacity int) *RingPubSub[V] {
	if capacity <= 0 || capacity == int(^uint(0)>>1) {
		panic("bigbuff: ringpubsub: invalid capacity")
	}
	x := &RingPubSub[V]{
		slots: make([]ringPubSubSlot[V], capacity+1),
		space: make(chan struct{}, 1),
	}
	for i := range x.slots {
		x.slots[i].ready = make(chan struct{}, 1)
	}
	return x
}

func (x *RingPubSub[V]) checkInitialized() {
	if len(x.slots) == 0 {
		panic("bigbuff: ringpubsub: must use factory function")
	}
}

// Subscribe immediately registers a reader for future publications. It does not
// replay existing values. Subscribing to a closed hub returns an inactive cursor.
// Registration takes constant work and does not wait for readers or publishers
// blocked on capacity. The returned active value must not be copied.
func (x *RingPubSub[V]) Subscribe() RingSubscription[V] {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return RingSubscription[V]{}
	}
	if x.subscribers == int(^uint(0)>>1) {
		panic("bigbuff: ringpubsub: too many subscribers")
	}
	x.subscribers++
	return RingSubscription[V]{hub: x, next: int(x.tail.Load())}
}

// TrySend publishes a value without waiting for capacity. It returns the number
// of subscriptions at publication, ErrRingFull if capacity is exhausted, or
// io.ErrClosedPipe after Close. With no subscribers it returns (0, nil) without
// retaining the value. The mutex may still contend; this is not a wait-free API.
func (x *RingPubSub[V]) TrySend(value V) (int, error) {
	x.checkInitialized()
	x.mu.Lock()
	n, err := x.trySendLocked(value)
	x.mu.Unlock()
	return n, err
}

// Send waits for capacity and publishes value, or returns a context error or
// io.ErrClosedPipe. A nil context means no cancellation. On an error no value
// was published. Cancellation concurrent with successful publication may lose
// the race; a successful result always means publication occurred.
//
// This method does not wait for recipients to consume the value. Publishers are
// ordered by admission under the mutex, not by the start times of their calls.
func (x *RingPubSub[V]) Send(ctx context.Context, value V) (int, error) {
	x.checkInitialized()
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				// We might have consumed a space hint on the previous iteration.
				// Pass it on before cancellation so another writer cannot strand.
				x.mu.Lock()
				x.wakeWriterLocked()
				x.mu.Unlock()
				return 0, err
			}
		}
		x.mu.Lock()
		n, err := x.trySendLocked(value)
		x.mu.Unlock()
		if err != ErrRingFull {
			return n, err
		}
		select {
		case <-done:
			return 0, ctx.Err()
		case <-x.space:
		}
	}
}

// trySendLocked initializes a slot before exposing it through tail. Keep the
// mutex through the initial notification: a fast reader can release the last
// obligation before that notification, but cannot reclaim the slot before it.
func (x *RingPubSub[V]) trySendLocked(value V) (int, error) {
	if x.closed {
		return 0, io.ErrClosedPipe
	}
	n := x.subscribers
	if n == 0 {
		x.wakeWriterLocked()
		return 0, nil
	}
	if x.used == len(x.slots)-1 {
		// Help a final reader that released its obligation but was descheduled
		// before reclaiming. A zero count certifies all old slot accesses are
		// over; publication need not wait for that reader to run again.
		x.reapLocked()
		if x.used == len(x.slots)-1 {
			return 0, ErrRingFull
		}
	}
	i := int(x.tail.Load())
	slot := &x.slots[i]
	slot.value = value
	slot.remaining.Store(int64(n))
	x.used++
	x.tail.Store(uint64(x.advance(i)))
	ringPubSubSignal(slot.ready)
	x.wakeWriterLocked()
	return n, nil
}

// Close stops publication, wakes blocked publishers, and allows every active
// subscription to drain its existing values before observing EOF. It returns
// true exactly once. Only the unused tail notification channel is closed: this
// is a terminal position that every draining cursor will eventually reach.
// Close never races a send on an occupied slot's notification channel.
func (x *RingPubSub[V]) Close() bool {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return false
	}
	x.closed = true
	close(x.slots[int(x.tail.Load())].ready)
	close(x.space)
	return true
}

// Ready returns the notification channel for this cursor's next value. Evaluate
// it again after every successful receive or Read: it changes as the cursor
// advances. Do not retain a channel across a cursor mutation or transfer it to
// another reader. A closed channel indicates EOF; unread values precede EOF.
// An inactive subscription returns a shared, already-closed channel.
//
// A successful receive must promptly be followed by TryRecv, Read, Recv, or
// Close. When the hub is open and the contract is respected, such a receive
// guarantees that this cursor has a value to consume.
func (s *RingSubscription[V]) Ready() <-chan struct{} {
	if s.hub == nil {
		return ringPubSubInactive
	}
	return s.hub.slots[s.next].ready
}

// TryRecv consumes the next value if available. No prior Ready receive is
// necessary. It returns false if the cursor is caught up or inactive; Recv or
// Ready distinguishes a temporarily empty stream from EOF.
//
// The common non-final reader does not acquire the hub mutex. The final release
// reclaims completed slots under that mutex; it may contend with publishers.
func (s *RingSubscription[V]) TryRecv() (value V, ok bool) {
	x := s.hub
	if x == nil || s.next == int(x.tail.Load()) {
		return
	}
	slot := &x.slots[s.next]
	value = slot.value
	// Signal BEFORE releasing our obligation. The obligation pins both the
	// immutable value and its notification channel's generation until we are
	// completely finished with them. In particular, a delayed signal can never
	// leak into a later reuse of this slot.
	ringPubSubSignal(slot.ready)
	remaining := slot.remaining.Add(-1)
	s.next = x.advance(s.next)
	if remaining == 0 {
		x.mu.Lock()
		// Do not reclaim the captured slot here: another reaper may have
		// already reclaimed it while we were acquiring the mutex. Reap only
		// the CURRENT completed prefix, without retiring an old slot twice.
		x.reapLocked()
		x.mu.Unlock()
	}
	return value, true
}

// Read fills as much of dst as is currently available, returning the number of
// values copied. It never waits for more values. The caller owns and may reuse
// dst. An empty dst performs no operation and does not satisfy the obligation
// to advance or close after consuming a Ready notification.
func (s *RingSubscription[V]) Read(dst []V) int {
	for i := range dst {
		value, ok := s.TryRecv()
		if !ok {
			return i
		}
		dst[i] = value
	}
	return len(dst)
}

// Recv consumes a value, waits for one, or returns io.EOF or a context error.
// A nil context means no cancellation. A canceled call leaves the subscription
// active; Close it when no further reads will be made. Cancellation already
// observed at the start of an attempt takes precedence over available data;
// cancellation concurrent with consuming a value may lose the race.
func (s *RingSubscription[V]) Recv(ctx context.Context) (value V, err error) {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				// Ready may have been consumed by this loop OR by the caller.
				// Keep our obligation, but pass on its notification before
				// returning. A pending obligation pins an open data lane;
				// a caught-up cursor names an empty or terminal lane instead.
				if x := s.hub; x != nil && s.next != int(x.tail.Load()) {
					ringPubSubSignal(x.slots[s.next].ready)
				}
				return value, err
			}
		}
		if v, ok := s.TryRecv(); ok {
			return v, nil
		}
		select {
		case <-done:
			return value, ctx.Err()
		case _, ok := <-s.Ready():
			if !ok {
				return value, io.EOF
			}
		}
	}
}

// Close unregisters this subscription and abandons its unread values. It is
// idempotent, including for a zero-value cursor. It never waits for other readers
// to make progress. Its work is proportional to this cursor's unread backlog,
// bounded by the hub's capacity, plus amortized completed-prefix reclamation.
// It must not run concurrently with another operation on this same cursor.
func (s *RingSubscription[V]) Close() {
	x := s.hub
	if x == nil {
		return
	}
	x.mu.Lock()
	x.subscribers--
	tail := int(x.tail.Load())
	for s.next != tail {
		slot := &x.slots[s.next]
		// Repair a notification even if Close follows a selected wakeup.
		// As in TryRecv, our obligation protects this signal from slot reuse.
		ringPubSubSignal(slot.ready)
		slot.remaining.Add(-1)
		s.next = x.advance(s.next)
	}
	x.reapLocked()
	x.mu.Unlock()
	s.hub = nil
}

func (x *RingPubSub[V]) advance(i int) int {
	i++
	if i == len(x.slots) {
		return 0
	}
	return i
}

// reapLocked only touches the current published prefix, never an old captured
// slot. A zero count proves that all value copies AND all notifications for the
// generation finished. Clear pointer-bearing payloads and stale hints before
// permitting reuse. A delayed final reader may safely reap nothing at all.
func (x *RingPubSub[V]) reapLocked() {
	for x.used != 0 {
		slot := &x.slots[x.head]
		if slot.remaining.Load() != 0 {
			break
		}
		var zero V
		slot.value = zero
		select {
		case <-slot.ready:
		default:
		}
		x.head = x.advance(x.head)
		x.used--
	}
	x.wakeWriterLocked()
}

// Multiple publishers share one coalesced space hint. Every successful writer
// relays it if more capacity exists. Full transitions drain an obsolete hint;
// canceled writers that consumed a hint explicitly repair it in Send.
func (x *RingPubSub[V]) wakeWriterLocked() {
	if x.closed {
		return
	}
	if x.used < len(x.slots)-1 {
		ringPubSubSignal(x.space)
	} else {
		select {
		case <-x.space:
		default:
		}
	}
}

func ringPubSubSignal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}
-- source/ringpubsub_example_test.go --
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

package bigbuff_test

import (
	"fmt"

	"github.com/joeycumines/go-bigbuff"
)

// One goroutine can consume multiple subscriptions: advancing one cursor never
// waits for another cursor to acknowledge the same publication.
func ExampleRingPubSub_select() {
	hub := bigbuff.NewRingPubSub[int](2)
	first, second := hub.Subscribe(), hub.Subscribe()
	defer first.Close()
	defer second.Close()

	for _, value := range []int{10, 20} {
		if _, err := hub.TrySend(value); err != nil {
			panic(err)
		}
	}
	hub.Close() // Existing subscriptions still receive both queued values.

	firstReady, secondReady := first.Ready(), second.Ready()
	var firstValues, secondValues []int
	for firstReady != nil || secondReady != nil {
		select {
		case _, open := <-firstReady:
			if !open {
				first.Close()
				firstReady = nil // Disable this case after EOF.
				continue
			}
			value, ok := first.TryRecv()
			if !ok {
				panic("ready subscription has no value")
			}
			firstValues = append(firstValues, value)
			firstReady = first.Ready() // The cursor's next lane has changed.

		case _, open := <-secondReady:
			if !open {
				second.Close()
				secondReady = nil // Close alone would leave a ready EOF case.
				continue
			}
			value, ok := second.TryRecv()
			if !ok {
				panic("ready subscription has no value")
			}
			secondValues = append(secondValues, value)
			secondReady = second.Ready()
		}
	}

	// Select order is unspecified; each subscription's value order is fixed.
	fmt.Println("first:", firstValues)
	fmt.Println("second:", secondValues)
	// Output:
	// first: [10 20]
	// second: [10 20]
}
-- source/ringpubsub_test.go --
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
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func ringTestNotReady(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
		t.Fatal("unexpected readiness for a subscription with no pending value")
	default:
	}
}

func ringTestReceive(t *testing.T, s *RingSubscription[int], want int) {
	t.Helper()
	select {
	case <-s.Ready():
		if got, ok := s.TryRecv(); !ok || got != want {
			t.Fatalf("TryRecv after Ready = (%d, %v), want (%d, true)", got, ok, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription never became ready")
	}
}

func TestRingPubSubCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1, int(^uint(0) >> 1)} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid capacity did not panic")
				}
			}()
			NewRingPubSub[int](capacity)
		})
	}
}

func TestRingPubSubNoSubscribersAndClosedSubscription(t *testing.T) {
	p := NewRingPubSub[int](1)
	for i := range 100 {
		if n, err := p.TrySend(i); n != 0 || err != nil {
			t.Fatalf("TrySend without subscribers = (%d, %v)", n, err)
		}
	}
	s := p.Subscribe()
	ringTestNotReady(t, s.Ready())
	if v, ok := s.TryRecv(); v != 0 || ok {
		t.Fatalf("new subscriber received historical value: (%d, %v)", v, ok)
	}
	s.Close()
	s.Close()
	if v, err := s.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Recv on closed subscription = (%d, %v)", v, err)
	}
	if !p.Close() || p.Close() {
		t.Fatal("Close must report exactly one transition")
	}
	if n, err := p.TrySend(1); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("TrySend on closed hub = (%d, %v)", n, err)
	}
	if n, err := p.Send(nil, 1); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Send on closed hub = (%d, %v)", n, err)
	}
	late := p.Subscribe()
	defer late.Close()
	if v, err := late.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Recv on subscription created after hub Close = (%d, %v)", v, err)
	}
}

// The extra lane is required when either an existing reader catches up or a
// reader joins a full ring. Neither may receive an older cohort's wake token.
func TestRingPubSubSpareLane(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 7} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			p := NewRingPubSub[int](capacity)
			defer p.Close()
			slow, fast := p.Subscribe(), p.Subscribe()
			defer slow.Close()
			defer fast.Close()
			for i := range capacity {
				if n, err := p.TrySend(i); n != 2 || err != nil {
					t.Fatalf("publish %d = (%d, %v)", i, n, err)
				}
			}
			for i := range capacity {
				ringTestReceive(t, &fast, i)
			}
			ringTestNotReady(t, fast.Ready())
			late := p.Subscribe()
			defer late.Close()
			ringTestNotReady(t, late.Ready())
			if n, err := p.TrySend(99); n != 0 || !errors.Is(err, ErrRingFull) {
				t.Fatalf("full ring accepted publication: (%d, %v)", n, err)
			}
			for i := range capacity {
				ringTestReceive(t, &slow, i)
			}
			for i := range 5 * (capacity + 1) {
				if n, err := p.TrySend(100 + i); n != 3 || err != nil {
					t.Fatalf("publish after wrap = (%d, %v)", n, err)
				}
				ringTestReceive(t, &late, 100+i)
				ringTestReceive(t, &fast, 100+i)
				ringTestReceive(t, &slow, 100+i)
				ringTestNotReady(t, late.Ready())
			}
		})
	}
}

func TestRingPubSubSelectedHintThenUnsubscribe(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	a, b := p.Subscribe(), p.Subscribe()
	defer b.Close()
	if n, err := p.TrySend(7); n != 2 || err != nil {
		t.Fatalf("TrySend = (%d, %v)", n, err)
	}
	select {
	case <-a.Ready():
	default:
		t.Fatal("publication did not produce a wake")
	}
	// A selected wake has not acknowledged a value. Closing this reader must
	// hand notification to B, even though A consumed the only queued wake.
	a.Close()
	ringTestReceive(t, &b, 7)
	if n, err := p.TrySend(8); n != 1 || err != nil {
		t.Fatalf("TrySend after unsubscribe = (%d, %v)", n, err)
	}
	ringTestReceive(t, &b, 8)
}

func TestRingPubSubMultipleInFlightHints(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	a, b, c := p.Subscribe(), p.Subscribe(), p.Subscribe()
	defer a.Close()
	defer b.Close()
	defer c.Close()
	if _, err := p.TrySend(11); err != nil {
		t.Fatal(err)
	}
	<-a.Ready()
	// A still owes this generation. B may poll without owning a notification.
	if got, ok := b.TryRecv(); !ok || got != 11 {
		t.Fatalf("polling B = (%d, %v)", got, ok)
	}
	select {
	case <-c.Ready():
	default:
		t.Fatal("polling reader did not relay notification")
	}
	if _, err := p.TrySend(12); !errors.Is(err, ErrRingFull) {
		t.Fatalf("in-flight readers failed to pin generation: %v", err)
	}
	if got, ok := a.TryRecv(); !ok || got != 11 {
		t.Fatalf("selected A = (%d, %v)", got, ok)
	}
	if _, err := p.TrySend(12); !errors.Is(err, ErrRingFull) {
		t.Fatalf("last in-flight reader failed to pin generation: %v", err)
	}
	c.Close()
	if n, err := p.TrySend(12); n != 2 || err != nil {
		t.Fatalf("publish after final retirement = (%d, %v)", n, err)
	}
	ringTestReceive(t, &a, 12)
	ringTestReceive(t, &b, 12)
	ringTestNotReady(t, a.Ready())
	ringTestNotReady(t, b.Ready())
}

func TestRingPubSubCloseDrainsEachCohort(t *testing.T) {
	p := NewRingPubSub[int](4)
	a := p.Subscribe()
	defer a.Close()
	p.TrySend(1)
	b := p.Subscribe()
	defer b.Close()
	p.TrySend(2)
	p.TrySend(3)
	ringTestReceive(t, &a, 1)
	if !p.Close() {
		t.Fatal("first close returned false")
	}
	for _, s := range []*RingSubscription[int]{&a, &b} {
		for _, want := range []int{2, 3} {
			ringTestReceive(t, s, want)
		}
		select {
		case <-s.Ready():
		default:
			t.Fatal("drained closed hub did not signal EOF")
		}
		if got, err := s.Recv(nil); got != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("Recv after drain = (%d, %v)", got, err)
		}
	}
}

func TestRingPubSubCloseWakesCaughtUpReaders(t *testing.T) {
	p := NewRingPubSub[int](2)
	const readers = 16
	subs := make([]RingSubscription[int], readers)
	for i := range subs {
		subs[i] = p.Subscribe()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, readers)
	for i := range subs {
		go func(s *RingSubscription[int]) {
			defer s.Close()
			_, err := s.Recv(ctx)
			results <- err
		}(&subs[i])
	}
	p.Close()
	for range readers {
		select {
		case err := <-results:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("waiting reader returned %v, want EOF", err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not wake every reader")
		}
	}
}

func TestRingPubSubCancellationPreservesSubscription(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	s := p.Subscribe()
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if v, err := s.Recv(ctx); v != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Recv = (%d, %v)", v, err)
	}
	if n, err := p.Send(nil, 4); n != 1 || err != nil {
		t.Fatalf("Send after canceled Recv = (%d, %v)", n, err)
	}
	if n, err := p.Send(ctx, 5); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled blocked Send = (%d, %v)", n, err)
	}
	ringTestReceive(t, &s, 4)
	ringTestNotReady(t, s.Ready())
	if n, err := p.TrySend(6); n != 1 || err != nil {
		t.Fatalf("subscriber lost after cancellation: (%d, %v)", n, err)
	}
	ringTestReceive(t, &s, 6)
}

func TestRingPubSubCanceledRecvPreservesBacklogAndRelaysWake(t *testing.T) {
	for _, hubClosed := range []bool{false, true} {
		for _, selected := range []bool{false, true} {
			t.Run(fmt.Sprintf("closed=%v/selected=%v", hubClosed, selected), func(t *testing.T) {
				p := NewRingPubSub[int](2)
				defer p.Close()
				a, b := p.Subscribe(), p.Subscribe()
				defer a.Close()
				defer b.Close()
				for _, v := range []int{1, 2} {
					if _, err := p.TrySend(v); err != nil {
						t.Fatal(err)
					}
				}
				if selected {
					select {
					case <-a.Ready():
					default:
						t.Fatal("publication did not signal the reader")
					}
				}
				if hubClosed {
					p.Close()
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if got, err := a.Recv(ctx); got != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled Recv with backlog = (%d, %v)", got, err)
				}
				// A keeps its obligation, but must not keep the only notification.
				// B can drain both messages while A is paused after cancellation.
				ringTestReceive(t, &b, 1)
				ringTestReceive(t, &b, 2)
				if !hubClosed {
					if _, err := p.TrySend(3); !errors.Is(err, ErrRingFull) {
						t.Fatalf("cancellation unexpectedly acknowledged backlog: %v", err)
					}
				}
				ringTestReceive(t, &a, 1)
				ringTestReceive(t, &a, 2)
				if hubClosed {
					if _, err := a.Recv(nil); !errors.Is(err, io.EOF) {
						t.Fatalf("drained reader returned %v, want EOF", err)
					}
				} else {
					ringTestNotReady(t, a.Ready())
				}
			})
		}
	}
}

func TestRingSubscriptionZeroValue(t *testing.T) {
	var s RingSubscription[int]
	select {
	case _, ok := <-s.Ready():
		if ok {
			t.Fatal("zero-value subscription notification was not closed")
		}
	default:
		t.Fatal("zero-value subscription is not immediately terminal")
	}
	if v, ok := s.TryRecv(); v != 0 || ok {
		t.Fatalf("zero-value TryRecv = (%d, %v)", v, ok)
	}
	output := []int{1, 2}
	if n := s.Read(output); n != 0 || !slices.Equal(output, []int{1, 2}) {
		t.Fatalf("zero-value Read = (%v, %d)", output, n)
	}
	if v, err := s.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("zero-value Recv = (%d, %v)", v, err)
	}
	s.Close()
	s.Close()
}

func TestRingPubSubReleasesPayloadReferences(t *testing.T) {
	type payload struct{ value int }
	p := NewRingPubSub[*payload](3)
	defer p.Close()
	a, b := p.Subscribe(), p.Subscribe()
	defer a.Close()
	defer b.Close()
	values := []*payload{% raw %}{{1}, {2}, {3}}{% endraw %}
	for _, v := range values {
		if _, err := p.TrySend(v); err != nil {
			t.Fatal(err)
		}
	}
	var output [3]*payload
	if n := a.Read(output[:]); n != len(output) || !slices.Equal(output[:], values) {
		t.Fatalf("first reader = %v (%d)", output, n)
	}
	for i, v := range values {
		if p.slots[i].value != v {
			t.Fatalf("slot %d cleared before the remaining subscriber consumed it", i)
		}
	}
	if v, ok := b.TryRecv(); !ok || v != values[0] {
		t.Fatalf("final receive = (%v, %v)", v, ok)
	}
	if p.slots[0].value != nil {
		t.Fatal("last receive retained a payload reference")
	}
	p.Close()
	b.Close() // abandon the two remaining values after hub shutdown
	for i := range p.slots {
		if p.slots[i].value != nil {
			t.Fatalf("slot %d retained an abandoned payload reference", i)
		}
	}
}

// Hold the hub mutex to pause the final reader after it has released its atomic
// obligation. A publisher must be able to help reclaim and publish before that
// reader runs its reaper. The delayed reaper must then inspect the current
// prefix, rather than retiring its captured old slot a second time.
func TestRingPubSubPublisherHelpsDelayedFinalReader(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	s := p.Subscribe()
	if _, err := p.TrySend(1); err != nil {
		t.Fatal(err)
	}
	type result struct {
		value int
		ok    bool
	}
	read := make(chan result, 1)
	p.mu.Lock()
	head := p.head
	go func() {
		value, ok := s.TryRecv()
		read <- result{value, ok}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for p.slots[head].remaining.Load() != 0 {
		if time.Now().After(deadline) {
			p.mu.Unlock()
			t.Fatal("last reader did not release its atomic obligation")
		}
		runtime.Gosched()
	}
	// Test the locked production path while deliberately excluding the
	// delayed reader's lock acquisition; no test hook is needed in the code.
	n, err := p.trySendLocked(2)
	p.mu.Unlock()
	if n != 1 || err != nil {
		t.Fatalf("publisher failed to help final reader: (%d, %v)", n, err)
	}
	select {
	case got := <-read:
		if !got.ok || got.value != 1 {
			t.Fatalf("delayed reader = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delayed reader did not finish")
	}
	defer s.Close()
	if _, err := p.TrySend(3); !errors.Is(err, ErrRingFull) {
		t.Fatalf("delayed reaper corrupted the newly occupied slot: %v", err)
	}
	ringTestReceive(t, &s, 2)
	if _, err := p.TrySend(3); err != nil {
		t.Fatal(err)
	}
	ringTestReceive(t, &s, 3)
}

// A batch retirement coalesces its space notifications. Publishers must relay
// that hint while capacity remains, or only one blocked publisher will wake.
func TestRingPubSubSpaceHintReachesAllPublishers(t *testing.T) {
	const capacity = 8
	p := NewRingPubSub[int](capacity)
	defer p.Close()
	s := p.Subscribe()
	defer s.Close()
	for i := range capacity {
		if _, err := p.TrySend(i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, capacity)
	results := make(chan error, capacity)
	for i := range capacity {
		go func(v int) {
			started <- struct{}{}
			n, err := p.Send(ctx, 100+v)
			if err == nil && n != 1 {
				err = fmt.Errorf("recipient count %d, want 1", n)
			}
			results <- err
		}(i)
	}
	for range capacity {
		<-started
	}
	// The assertion does not depend on a scheduling deadline; this just makes
	// it likely that all producers exercise the blocked path in normal runs.
	for range capacity * 4 {
		runtime.Gosched()
	}
	var out [capacity]int
	if n := s.Read(out[:]); n != capacity {
		t.Fatalf("Read freed %d values, want %d", n, capacity)
	}
	for i, v := range out {
		if v != i {
			t.Fatalf("Read[%d] = %d", i, v)
		}
	}
	for range capacity {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("free capacity stranded blocked publishers")
		}
	}
	if n := s.Read(out[:]); n != capacity {
		t.Fatalf("published %d values, want %d", n, capacity)
	}
	slices.Sort(out[:])
	for i, v := range out {
		if v != 100+i {
			t.Fatalf("publication[%d] = %d, want %d", i, v, 100+i)
		}
	}
}

func TestRingPubSubCloseWakesBlockedPublishers(t *testing.T) {
	p := NewRingPubSub[int](1)
	s := p.Subscribe()
	defer s.Close()
	p.TrySend(1)
	const publishers = 16
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, publishers)
	for range publishers {
		go func() {
			_, err := p.Send(ctx, 2)
			results <- err
		}()
	}
	p.Close()
	for range publishers {
		select {
		case err := <-results:
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("blocked Send returned %v, want ErrClosedPipe", err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not wake every publisher")
		}
	}
	ringTestReceive(t, &s, 1)
}

func TestRingPubSubCanceledPublishersDoNotStrandCapacity(t *testing.T) {
	for iteration := range 50 {
		p := NewRingPubSub[int](1)
		s := p.Subscribe()
		p.TrySend(-1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cancelCtx, cancelBlocked := context.WithCancel(ctx)
		const canceled = 4
		results := make(chan error, canceled)
		for range canceled {
			go func() {
				_, err := p.Send(cancelCtx, -2)
				results <- err
			}()
		}
		live := make(chan error, 1)
		go func() {
			_, err := p.Send(ctx, iteration)
			live <- err
		}()
		for range 16 {
			runtime.Gosched()
		}
		cancelBlocked()
		type readResult struct {
			values []int
			err    error
		}
		read := make(chan readResult, 1)
		go func() {
			defer s.Close()
			var r readResult
			for {
				v, err := s.Recv(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						r.err = err
					}
					read <- r
					return
				}
				r.values = append(r.values, v)
			}
		}()
		// A publisher can have passed its cancellation check before being
		// delayed on the hub mutex. Such an overlapping successful publication
		// is allowed, and must correspond to exactly one delivered value.
		committed := 0
		for range canceled {
			select {
			case err := <-results:
				if err == nil {
					committed++
				} else if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled blocked publisher returned %v", err)
				}
			case <-ctx.Done():
				t.Fatal("canceled publisher remained blocked")
			}
		}
		select {
		case err := <-live:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("canceled publisher consumed the sole space hint")
		}
		p.Close()
		select {
		case r := <-read:
			if r.err != nil || len(r.values) != committed+2 {
				t.Fatalf("drain after cancellation = %v, %v; successful canceled sends=%d", r.values, r.err, committed)
			}
			if r.values[0] != -1 {
				t.Fatalf("initial value = %d, want -1", r.values[0])
			}
			seenLive, seenCanceled := 0, 0
			for _, v := range r.values[1:] {
				switch v {
				case iteration:
					seenLive++
				case -2:
					seenCanceled++
				default:
					t.Fatalf("unexpected delivered value %d", v)
				}
			}
			if seenLive != 1 || seenCanceled != committed {
				t.Fatalf("delivered live=%d, canceled=%d; committed canceled=%d", seenLive, seenCanceled, committed)
			}
		case <-ctx.Done():
			t.Fatal("consumer did not drain after cancellation")
		}
		cancel()
	}
}

func TestRingPubSubConcurrentPublishersAgreeOnOrder(t *testing.T) {
	const publishers, perPublisher, subscribers = 4, 250, 8
	p := NewRingPubSub[int](8)
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	subs := make([]RingSubscription[int], subscribers)
	for i := range subs {
		subs[i] = p.Subscribe()
	}
	type result struct {
		values []int
		err    error
	}
	consumed := make(chan result, subscribers)
	for i := range subs {
		go func(s *RingSubscription[int]) {
			defer s.Close()
			r := result{values: make([]int, 0, publishers*perPublisher)}
			for {
				v, err := s.Recv(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						r.err = err
					}
					consumed <- r
					return
				}
				r.values = append(r.values, v)
			}
		}(&subs[i])
	}
	produced := make(chan error, publishers)
	for producer := range publishers {
		go func(id int) {
			for i := range perPublisher {
				n, err := p.Send(ctx, id*perPublisher+i)
				if err != nil {
					produced <- err
					return
				}
				if n != subscribers {
					produced <- fmt.Errorf("Send recipients %d, want %d", n, subscribers)
					return
				}
			}
			produced <- nil
		}(producer)
	}
	for range publishers {
		select {
		case err := <-produced:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("publishers did not complete")
		}
	}
	p.Close()
	var first []int
	for reader := range subscribers {
		select {
		case r := <-consumed:
			if r.err != nil {
				t.Fatal(r.err)
			}
			if len(r.values) != publishers*perPublisher {
				t.Fatalf("reader %d received %d values, want %d", reader, len(r.values), publishers*perPublisher)
			}
			if reader == 0 {
				first = r.values
			} else if !slices.Equal(first, r.values) {
				t.Fatal("subscribers observed different publication orders")
			}
		case <-ctx.Done():
			t.Fatal("readers did not drain")
		}
	}
	seen := make([]bool, publishers*perPublisher)
	previous := make([]int, publishers)
	for i := range previous {
		previous[i] = -1
	}
	for _, v := range first {
		if v < 0 || v >= len(seen) || seen[v] {
			t.Fatalf("out-of-range or duplicate value %d", v)
		}
		seen[v] = true
		producer, index := v/perPublisher, v%perPublisher
		if index != previous[producer]+1 {
			t.Fatalf("publisher %d order: got %d after %d", producer, index, previous[producer])
		}
		previous[producer] = index
	}
}

func TestRingPubSubConcurrentSubscriptionChurn(t *testing.T) {
	const publications, churners = 1000, 8
	p := NewRingPubSub[int](4)
	defer p.Close()
	stable := p.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan error, churners+2)
	go func() {
		defer stable.Close()
		for want := range publications {
			got, err := stable.Recv(ctx)
			if err != nil || got != want {
				results <- fmt.Errorf("stable reader got (%d, %v), want %d", got, err, want)
				return
			}
		}
		results <- nil
	}()
	var ready sync.WaitGroup
	ready.Add(churners)
	for id := range churners {
		go func(id int) {
			ready.Done()
			for iteration := range 100 {
				s := p.Subscribe()
				last := -1
				for range (iteration + id) % 5 {
					v, err := s.Recv(ctx)
					if errors.Is(err, io.EOF) {
						s.Close()
						results <- nil
						return
					}
					if err != nil || v < 0 || v >= publications || (last >= 0 && v != last+1) {
						s.Close()
						results <- fmt.Errorf("churn reader %d got (%d, %v) after %d", id, v, err, last)
						return
					}
					last = v
				}
				s.Close()
			}
			results <- nil
		}(id)
	}
	ready.Wait()
	go func() {
		for i := range publications {
			if _, err := p.Send(ctx, i); err != nil {
				results <- err
				return
			}
		}
		p.Close()
		results <- nil
	}()
	for range churners + 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("subscription churn stalled progress")
		}
	}
}

// This oracle stores one ordinary FIFO per subscription. It has no knowledge
// of ring indices, reference counts, notification lanes, or the implementation's
// state. The longest FIFO determines retention because live readers receive
// every publication made after their respective subscriptions.
func ringTestModel(t *testing.T, capacity int, operations []byte) {
	t.Helper()
	p := NewRingPubSub[int](capacity)
	defer p.Close()
	type reader struct {
		s       *RingSubscription[int]
		pending []int
	}
	var readers [8]reader
	defer func() {
		for _, r := range readers {
			if r.s != nil {
				r.s.Close()
			}
		}
	}()
	serial := 0
	for step, op := range operations {
		index := int(op>>3) % len(readers)
		r := &readers[index]
		switch op & 7 {
		case 0, 1:
			if r.s == nil {
				s := p.Subscribe()
				r.s = &s
			}
		case 2, 3:
			active, retained := 0, 0
			for _, q := range readers {
				if q.s != nil {
					active++
					retained = max(retained, len(q.pending))
				}
			}
			serial++
			n, err := p.TrySend(serial)
			if retained == capacity {
				if n != 0 || !errors.Is(err, ErrRingFull) {
					t.Fatalf("step %d: full TrySend = (%d, %v)", step, n, err)
				}
			} else {
				if n != active || err != nil {
					t.Fatalf("step %d: TrySend = (%d, %v), want (%d, nil)", step, n, err, active)
				}
				for i := range readers {
					if readers[i].s != nil {
						readers[i].pending = append(readers[i].pending, serial)
					}
				}
			}
		case 4, 5:
			if r.s == nil {
				break
			}
			if op&7 == 5 {
				select {
				case <-r.s.Ready():
					if len(r.pending) == 0 {
						t.Fatalf("step %d: empty subscriber %d became ready", step, index)
					}
				default:
					if len(r.pending) != 0 {
						t.Fatalf("step %d: subscriber %d has data without a wake", step, index)
					}
				}
			}
			got, ok := r.s.TryRecv()
			if len(r.pending) == 0 {
				if got != 0 || ok {
					t.Fatalf("step %d: empty TryRecv = (%d, %v)", step, got, ok)
				}
			} else {
				if !ok || got != r.pending[0] {
					t.Fatalf("step %d: TryRecv = (%d, %v), want %d", step, got, ok, r.pending[0])
				}
				r.pending = r.pending[1:]
			}
		case 6:
			if r.s != nil {
				var output [9]int
				for i := range output {
					output[i] = -1
				}
				size := int(op>>4) % len(output)
				wantN := min(size, len(r.pending))
				n := r.s.Read(output[:size])
				if n != wantN || !slices.Equal(output[:n], r.pending[:wantN]) {
					t.Fatalf("step %d: Read = %v (%d), want %v", step, output, n, r.pending[:wantN])
				}
				for _, v := range output[n:] {
					if v != -1 {
						t.Fatalf("step %d: Read changed output beyond returned count", step)
					}
				}
				r.pending = r.pending[n:]
			}
		case 7:
			if r.s != nil {
				r.s.Close()
				r.s.Close()
				r.s, r.pending = nil, nil
			}
		}
		for i := range readers {
			if readers[i].s != nil && len(readers[i].pending) == 0 {
				select {
				case <-readers[i].s.Ready():
					t.Fatalf("step %d: caught-up subscriber %d received another cohort's wake", step, i)
				default:
				}
			}
		}
	}
	p.Close()
	for i := range readers {
		r := &readers[i]
		if r.s == nil {
			continue
		}
		for _, want := range r.pending {
			ringTestReceive(t, r.s, want)
		}
		if _, err := r.s.Recv(nil); !errors.Is(err, io.EOF) {
			t.Fatalf("final subscriber %d drain = %v", i, err)
		}
	}
}

func TestRingPubSubModel(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 7, 16} {
		for seed := range uint64(8) {
			t.Run(fmt.Sprintf("capacity=%d/seed=%d", capacity, seed), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, seed+1))
				operations := make([]byte, 2000)
				for i := range operations {
					operations[i] = byte(rng.Uint32())
				}
				ringTestModel(t, capacity, operations)
			})
		}
	}
}

func FuzzRingPubSubModel(f *testing.F) {
	f.Add([]byte{1, 0, 8, 2, 4, 13, 7, 2, 2, 2, 14, 15})
	f.Add([]byte{3, 0, 8, 16, 2, 2, 2, 5, 13, 21, 7, 15, 23})
	f.Add([]byte{7})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		if len(input) > 4097 {
			input = input[:4097]
		}
		ringTestModel(t, int(input[0]%16)+1, input[1:])
	})
}

func TestRingPubSubZeroSteadyStateAllocations(t *testing.T) {
	t.Run("subscription churn", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		if n := testing.AllocsPerRun(1000, func() {
			s := p.Subscribe()
			s.Close()
		}); n != 0 {
			t.Fatalf("Subscribe/Close allocated %.3f objects per cycle", n)
		}
	})
	t.Run("publication and selected receive", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		a, b := p.Subscribe(), p.Subscribe()
		defer a.Close()
		defer b.Close()
		var failed bool
		allocs := testing.AllocsPerRun(1000, func() {
			if n, err := p.Send(nil, 42); n != 2 || err != nil {
				failed = true
			}
			<-a.Ready()
			if got, ok := a.TryRecv(); !ok || got != 42 {
				failed = true
			}
			<-b.Ready()
			if got, ok := b.TryRecv(); !ok || got != 42 {
				failed = true
			}
		})
		if failed || allocs != 0 {
			t.Fatalf("Send/select/TryRecv: failure=%v, allocations=%.3f", failed, allocs)
		}
	})
	t.Run("queued unsubscribe", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		var failed bool
		allocs := testing.AllocsPerRun(1000, func() {
			s := p.Subscribe()
			for i := range 4 {
				if n, err := p.TrySend(i); n != 1 || err != nil {
					failed = true
				}
			}
			s.Close()
		})
		if failed || allocs != 0 {
			t.Fatalf("Subscribe/publish/Close: failure=%v, allocations=%.3f", failed, allocs)
		}
	})
	t.Run("batch read", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		s := p.Subscribe()
		defer s.Close()
		var output [4]int
		var failed bool
		allocs := testing.AllocsPerRun(1000, func() {
			for i := range output {
				if _, err := p.TrySend(i); err != nil {
					failed = true
				}
			}
			if n := s.Read(output[:]); n != len(output) {
				failed = true
			}
		})
		if failed || allocs != 0 {
			t.Fatalf("publish/Read: failure=%v, allocations=%.3f", failed, allocs)
		}
	})
}

````
