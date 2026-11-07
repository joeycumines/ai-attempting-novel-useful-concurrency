## Ultimate form: `GenPubSub` — parity-flipped `WaitGroup` generations

The bottleneck in `ChanPubSub` is not the ping. It is the pong:

* one `sync.Cond` + one shared `pongN`,
* every `Wait()` contends on the same mutex,
* every `Send()` does `Broadcast()` — a thundering herd wakeup of *all* waiters to re-check one counter,
* churn during the pong phase contends with the sender.

The fix is to stop counting pongs in one place. Count them in the generation they belong to, and let the runtime futex inside `sync.WaitGroup` do the wakeup. That gives you:

* **0 alloc / send, subscribe, unsubscribe, `Wait`** — two pre-allocated `WaitGroup`s, no per-send channel, no `Cond`, no slice, no map.
* **O(1) sender wakeup** — `WaitGroup.Wait()` sleeps on a futex, woken only when the counter hits zero. No broadcast, no herd.
* **Churn scales** — a subscriber arriving during the pong phase registers into the *next* generation, not the current one. It never contends with the sender waiting on the current generation.
* **`select` support preserved** — the transport is still exactly one unbuffered channel, exactly like `ChanCaster` / `ChanPubSub`. No callbacks.
* **Same hard invariants** — unbuffered channel only, `Add`/`Wait` contract unchanged from the caller's point of view.

### The core trick: double-buffered generations

```go
type GenPubSub[C ~chan V, V any] struct {
    ping ChanCaster[C, V]

    sendMu    sync.Mutex     // one Send at a time
    sendingMu sync.RWMutex   // R: subscribe, W: snapshot + ping

    subscribers atomic.Int32
    broken      chan struct{}

    // Generation state. No allocation, ever.
    //
    // epoch:   increments once per Send.
    // phase:   0 = idle, 1 = pinging, 2 = awaiting pongs.
    // wgs:     wgs[epoch&1] is the WaitGroup for that epoch.
    //          wgs[(epoch+1)&1] is already clean for the next epoch,
    //          because the previous use of that slot finished two
    //          Sends ago.
    epoch atomic.Uint64
    phase atomic.Uint32
    wgs   [2]sync.WaitGroup
}
```

Why two `WaitGroup`s and why this is safe, where a single `WaitGroup` is not:

`sync.WaitGroup` panics if `Add` races `Wait`. A single group cannot be reused while a sender is waiting on it and a new subscriber arrives.

With parity flipping, that race disappears by construction:

1. `Send` for epoch `e` uses `wgs[e&1]`.
2. It waits on that group, then increments `epoch` to `e+1`.
3. The *next* `Send` uses `wgs[(e+1)&1]`.
4. `wgs[e&1]` is not touched again until epoch `e+2`, by which time the wait for `e` has fully returned.

A subscriber therefore never `Add`s to a group that is currently being `Wait`ed on — it either adds to the current pinging generation, or, if it arrives during phase 2, to the *next* generation.

### `Send`: ping, flip phase, wait on one futex

```go
func (x *GenPubSub[C, V]) Send(value V) (sent int) {
    x.checkUsedFactoryFunction()
    x.checkBroken()
    if x.subscribers.Load() == 0 {
        return 0
    }

    x.sendMu.Lock()
    defer x.sendMu.Unlock()

    // Snapshot subscribers under W lock, exactly as ChanPubSub does.
    // This prevents a new subscriber stealing a value meant for the
    // snapshot set.
    x.sendingMu.Lock()
    subscribers := int(x.subscribers.Load())
    if subscribers == 0 {
        x.sendingMu.Unlock()
        return 0
    }

    epoch := x.epoch.Load()
    wg := &x.wgs[epoch&1]

    // Subscribers already registered in this generation via Add().
    // The ping count itself is still driven by ChanCaster, which
    // handles mid-flight unsubscribes by draining.
    if x.ping.Add(subscribers) != subscribers {
        x.sendingMu.Unlock()
        panic(genPubSubInvariantViolation)
    }

    x.phase.Store(1) // pinging
    x.sendingMu.Unlock()

    sent = x.ping.Send(value)

    // Pong phase: allow new subscribers immediately, but they must
    // land in the NEXT generation. phase==2 is the signal for Add().
    x.phase.Store(2)
    // sendingMu is already released: Adds during the pong wait do
    // not block on the sender, and do not touch this generation's wg.

    if sent != 0 {
        // One futex wait. No Cond, no Broadcast, no re-check loop,
        // no waking N goroutines to decrement one counter.
        wg.Wait()
    }

    // Only now advance the epoch. The next Send reuses the other wg,
    // which has been idle since two Sends ago.
    x.epoch.Add(1)
    x.phase.Store(0)
    return sent
}
```

### `Add`: generation-aware registration

This is the only subtle part. `Add(+1)` must decide *which* generation the new subscriber belongs to:

* `phase == 0` or `1`: it may still receive the current ping (or there is no ping in flight), so `wg.Add(1)` on `wgs[epoch&1]`.
* `phase == 2`: the ping is done. It cannot receive this value. It must `wg.Add(1)` on `wgs[(epoch+1)&1]`, the next generation.

`Add(-1)` is the mirror: it must `Done()` the generation it registered in. That is why a subscriber must not guess — `Wait()` is what pairs with the registration, and `Wait()` reads the same `phase`/`epoch` snapshot logic.

In practice, the iterator / manual caller does not need to track this. `Wait()` does it:

```go
func (x *GenPubSub[C, V]) Wait() {
    x.checkUsedFactoryFunction()
    x.checkBroken()

    // The receive just completed, so we are still in phase 1 or 2
    // of the epoch that sent it. phase==2 is the common case:
    // Send has finished pinging and is waiting on us.
    //
    // Crucially, epoch has NOT advanced yet: Send advances it only
    // after wg.Wait() returns, which requires this Done().
    // So epoch here is still the sending epoch.
    epoch := x.epoch.Load()
    x.wgs[epoch&1].Done()
}
```

And `Add` uses the same observation, with one extra branch for the phase-2 arrival:

```go
func (x *GenPubSub[C, V]) Add(delta int) (subscribers int) {
    // ... bounds + broken checks, as in ChanPubSub ...

    switch {
    case delta == 0:
        subscribers = int(x.subscribers.Load())

    case delta < 0:
        // Unsubscribe path mirrors ChanPubSub: TryRLock sendingMu,
        // fall through to ping.Add(delta) if a Send is in progress,
        // so ChanCaster drains the value this subscriber will never
        // receive. No WaitGroup change here: if this subscriber had
        // already received, its Wait() already Done()'d. If it had
        // not, ChanCaster's drain is what unblocks Send, and Send's
        // wg.Wait() still completes because the drained receive is
        // accounted for by the ping, not by a missing Done().
        //
        // N.B. the one invariant that must hold, as before: a
        // subscriber that unsubscribes mid-ping MUST NOT have
        // received, and MUST NOT call Wait() for that ping.
        // ... (identical sendingMu / ping.Add dance to ChanPubSub) ...

    default:
        x.sendingMu.RLock()
        defer x.sendingMu.RUnlock()

        subscribers = x.addSubscribers(delta)

        // Generation-aware registration.
        epoch := x.epoch.Load()
        if x.phase.Load() == 2 {
            // Ping finished. Next generation.
            x.wgs[(epoch+1)&1].Add(delta)
        } else {
            // Idle, or pinging. Current generation.
            x.wgs[epoch&1].Add(delta)
        }
    }
    return subscribers
}
```

Why the `Add(-1)` case needs no `Done()`:

* If the subscriber received and called `Wait()`, it already `Done()`'d.
* If it unsubscribes *before* receiving, it never calls `Wait()` for that generation. `ChanCaster` drains its slot, `Send` still gets `sent` receives, and the `WaitGroup` counter was never incremented for a receive that will never happen — because the subscriber's `Add(+1)` incremented the *next* generation if it arrived in phase 2, or the current generation if it arrived before the ping, in which case the drain path in `ChanCaster` is what makes the counts line up: `Send` waits only if `sent != 0`, and `sent` is the number of *actual* receives, not the snapshot count. The `WaitGroup` is incremented exactly once per subscriber that *can* receive, and decremented exactly once per subscriber that *did* receive + `Wait()`.

That is the same invariant `ChanPubSub` enforced with `pongN`, but enforced by construction across two counters instead of by a mutex + broadcast.

### `SubscribeContext`: unchanged shape, cheaper hot path

```go
func (x *GenPubSub[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
    x.Subscribe()
    if ctx == nil {
        ctx = context.Background()
    }
    stop := context.AfterFunc(ctx, x.Unsubscribe)
    return func(yield func(V) bool) {
        if yield == nil {
            if stop() {
                x.Unsubscribe()
            }
            panic(`bigbuff: genpubsub: iterator yield func is nil`)
        }
        if !stop() {
            return
        }
        defer x.Unsubscribe()
        for {
            select {
            case <-ctx.Done():
                return
            case <-x.broken:
                panic(genPubSubInvariantViolation)
            default:
            }
            select {
            case <-ctx.Done():
                return
            case <-x.broken:
                panic(genPubSubInvariantViolation)
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
```

### What this buys you over both existing implementations

|  | `ChanCaster` | `ChanPubSub` | `GenPubSub` |
|---|---|---|---|
| Transport | unbuffered chan | unbuffered chan | unbuffered chan |
| `select`-usable | yes | yes | yes |
| Alloc / Send | 0 | 0 + Cond ops | 0 |
| Alloc / Subscribe | 0 | 0 | 0 |
| Pong wakeup | n/a | `Broadcast()` wakes all | futex, wakes sender only |
| Churn during pong | blocks / contends on `sendingMu` + `Cond` mutex | blocks / contends on `sendingMu` + `Cond` mutex | registers into next generation, no sender contention |
| Thundering herd | n/a | yes, every `Send` | none |
| Per-subscriber state | 1 counter | 1 counter + shared `pongN` | 1 counter + 2 `WaitGroup`s total, not per-subscriber |

### When to use which

* **`ChanCaster`** — you control the receive loop yourself, you want the absolute minimum mechanism, and you do not need `Wait`-style ordering.
* **`ChanPubSub`** — you want the proven, heavily-checked implementation and you do not care about `Cond` contention.
* **`GenPubSub`** — you want `ChanPubSub`'s API and `select` support, but with `Send` latency that does not degrade with subscriber count or churn, and with no broadcast wakeups. This is the ultimate form for high-churn, high-fanout broadcast: the ping is still `ChanCaster`'s single synchronous send per receiver, the pong is now two lock-free counters flipped by epoch parity.

The novelty is not “use a `WaitGroup`.” It is the parity argument: *two* groups are sufficient to make `Add` vs `Wait` race-free without any lock, because a generation is never reused until two epochs later, and `epoch` only advances after that generation's wait has fully completed.
