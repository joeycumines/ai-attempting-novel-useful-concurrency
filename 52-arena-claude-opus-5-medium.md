# `ChanCascade` — a cooperative *cascade* broadcast with phase‑alternating transports

## Why this, and why it's new

`ChanCaster`/`ChanPubSub` occupy a genuinely rare niche: **zero‑allocation, select‑compatible, exactly‑counted broadcast**. Their weaknesses are structural, not incidental:

| | cost |
|---|---|
| `ChanCaster.Send(n)` | **n sequential rendezvous on the sender's critical path** — latency is `O(n)` goroutine handoffs, strictly serialised, no parallelism available even on 64 cores. |
| `ChanCaster` + select | every speculative select requires `Add(1)` … `Add(-1)` — registration churn *per attempt*. |
| `ChanPubSub.Send` | `O(n)` pings **plus** an `O(n)` `sync.Cond` pong barrier (thundering herd, ~2 park/unpark per subscriber per message) **plus** 3 mutexes and a `TryRLock` spin loop in the unsubscribe path. |
| `ChanPubSub.Add` | subscribe/unsubscribe is heavyweight and contends with sending. |

The new primitive below attacks all four at once with two ideas that compose into something I haven't seen in Go:

### 1. Cooperative cascade (the "chain‑reaction" broadcast)
The sender performs **exactly one** channel send. Every receiver, immediately after receiving, atomically *claims a relay duty* (`relays.Add(-1)`) and, if one is owed, **forwards the value back into the same channel**. Value holders therefore **double every round**: the broadcast completes in `⌈log₂ n⌉ + 1` handoff depth instead of `n`, with the handoffs executing *in parallel across Ps*. Total work is still exactly `n` handoffs (optimal — each receiver must be woken), but the *critical path* collapses, and the sender's CPU is no longer the bottleneck.

The counting argument that makes this safe is exact, not probabilistic:
* `relays` is seeded to `n-1`; the `i`‑th receipt yields `n-1-i`; forwarding iff `≥ 0` ⟹ exactly `n-1` forwards ⟹ exactly `n` sends for `n` receivers.
* At any instant, *unmatched sends ≤ receivers‑not‑yet‑served* (`issued ≤ n`, `unmatched = issued − received ≤ n − received`) ⟹ **a forward can never deadlock**.
* `relays == -1` ⟺ all `n` receipts occurred ⟹ **no claimed‑but‑unsent forward can exist** (if one did, completed sends `≤ n−1 <` received — contradiction). This is what licenses the sender to end the round on a single atomic read, with no `WaitGroup` and no barrier.

### 2. Phase‑alternating transports (the reason no pong barrier is needed)
`ChanPubSub` needs its `sync.Cond` round‑trip purely to stop a fast subscriber from *stealing* a message intended for a slow peer. `ChanCascade` eliminates that entire mechanism by alternating between **two** unbuffered channels: a receiver that has consumed round `k` has already flipped to the opposite channel, so it is *physically incapable* of stealing a relay for round `k`. The pong barrier — `O(n)` cond wakeups, a mutex, and the whole `broken`/deadlock‑mitigation apparatus — simply evaporates.

The phase bit also becomes a **free, exact ownership oracle**: `sending && globalPhase == myPhase` is *precisely* the predicate "I still owe a receipt this round", which makes mid‑flight unsubscribe a single CAS decision with no locks and no races.

### 3. Fallout: lock‑free, never‑blocking subscribe
Because the sender's `n` is captured by the *same CAS* that sets the sending bit, a subscriber's increment is totally ordered against it: land before ⟹ you're in the round at phase `p`; land after ⟹ the sending bit is set, so you join at phase `p^1` and cleanly skip the in‑flight message. **Subscribe/unsubscribe are lock‑free single‑CAS operations that are never blocked by an in‑flight broadcast** — which is exactly the consumer‑churn scalability the callback design can't deliver.

### 4. Spin‑then‑park completion, with a pre‑allocated wake slot
No `WaitGroup`, no `Cond`. The sender spins briefly on `relays` (so the `n == 1` case costs **one** handoff — parity with `ChanCaster`), then publishes `parked` and blocks on a *constructor‑allocated*, cap‑1 `wake` channel; only the final receiver signals, non‑blockingly. The Dekker‑style handshake is sound under Go's sequentially‑consistent `sync/atomic`, and a stale token is absorbed harmlessly by the re‑check loop.

### Resulting characteristics

| property | `ChanCaster` | `ChanPubSub` | **`ChanCascade`** |
|---|---|---|---|
| allocations / send | 0 | 0 | **0** |
| allocations / (un)subscribe | 0 | 0 | **0** |
| send critical path | `n` handoffs | `n` + `n` cond wakes | **`⌈log₂ n⌉+1` handoffs** |
| sender atomics | ~4 + n sends | many | **~7 + n sends (parallel)** |
| receiver cost | 0 | lock + cond | **1 RMW (+1 load)** |
| select‑compatible | ✅ (needs Add/Add⁻¹ per attempt) | ✅ | **✅ (no churn — stay registered)** |
| subscribe during send | blocks | blocks | **lock‑free, never blocks** |
| exact delivered count | ✅ | ✅ | **✅** |
| backpressure / ordering | ✅ | ✅ | **✅** |
| misuse detection | ✅ | ✅ | **✅ (`relays < -1`, state bits)** |

---

## `chancascade.go`

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
	"iter"
	"math"
	"sync"
	"sync/atomic"
)

const (
	chanCascadeInvariant = `bigbuff: chancascade: state invariant violation`

	// chanCascadeMaxReceivers is the inclusive upper bound on the number of
	// concurrently registered receivers.
	chanCascadeMaxReceivers = math.MaxInt32

	// state (uint64) bit layout:
	//
	//	63..32  receivers  (uint32, <= chanCascadeMaxReceivers)
	//	31      sending    (a broadcast round is in flight)
	//	30..1   reserved   (MUST be zero - cheap corruption detection)
	//	0       phase      (index of the transport channel in use)
	chanCascadeSendingBit  = uint32(1) << 31
	chanCascadePhaseBit    = uint32(1)
	chanCascadeReservedBit = ^(chanCascadeSendingBit | chanCascadePhaseBit)

	// chanCascadeSpin bounds the sender's adaptive spin, prior to parking. It
	// is deliberately only applied for small rounds, where the round latency
	// is comparable to a park/unpark pair, and where the spin cannot
	// meaningfully interfere with receivers contending on the same line.
	chanCascadeSpin         = 128
	chanCascadeSpinMaxRecvs = 8
)

// chanCascadePad separates mutable hot fields onto distinct cache lines,
// preventing false sharing between the (sender-owned) registration word and
// the (receiver-hammered) round word.
type chanCascadePad [64]byte

// ChanCascade is an allocation-free, select-compatible, exactly-counted
// broadcast ("pub/sub") primitive, in which receivers cooperatively relay each
// value onwards, reducing broadcast latency from O(n) sequential channel
// handoffs to O(log n) handoff depth, executed in parallel.
//
// It occupies the same niche as [ChanPubSub] - every registered receiver
// observes every value, and [ChanCascade.Send] returns only once delivery is
// complete - but does so without a secondary barrier (no [sync.Cond], no
// [sync.WaitGroup]), without any allocation on any path, and without blocking
// subscribe/unsubscribe behind an in-flight broadcast.
//
// # How it works
//
// Two unbuffered transport channels are used, alternately. The channel in use
// for a given broadcast is identified by a single "phase" bit; each receiver
// tracks its own phase, flipping it on each value consumed. Because a receiver
// that has consumed the current round has already flipped away from the
// channel still carrying that round's relays, it is structurally incapable of
// consuming the same value twice, or of stealing a value owed to a slower
// peer. This is what replaces [ChanPubSub]'s mandatory ping-pong barrier.
//
// [ChanCascade.Send] performs exactly one channel send. Each receiver, upon
// consuming a value, atomically claims a relay duty; if one is owed, it sends
// the value back onto the same channel. The set of value-holders therefore
// doubles each round, giving a critical path of ceil(log2(n))+1 handoffs for n
// receivers, while performing exactly n handoffs in total (the minimum
// possible, since each receiver must be woken exactly once).
//
// # Contract
//
//  1. Instances MUST be created via [NewChanCascade] or [MakeChanCascade].
//  2. The transport channels MUST be unbuffered, distinct, and MUST NOT be
//     sent to, received from, or closed, by anything other than this type and
//     its receivers. (Both may be nil, yielding a permanently "disabled"
//     instance; it is then an error to register any receiver.)
//  3. A receiver handle, obtained from [ChanCascade.Subscribe], is owned by
//     exactly one goroutine at a time, and MUST NOT be copied after use.
//  4. [ChanCascadeReceiver.Received] MUST be called immediately after - and
//     only after - each value received from [ChanCascadeReceiver.C], before
//     any further use of the handle. Failing to do so stalls all senders, and
//     all other receivers.
//  5. A receiver MUST promptly and continually either receive-then-Received,
//     or unsubscribe. It MAY use [ChanCascadeReceiver.C] in a select
//     statement, and - unlike [ChanCaster] - needs no registration churn to do
//     so: an unselected case costs nothing and changes no state.
//  6. [ChanCascadeReceiver.Unsubscribe] MUST be called exactly once per
//     handle, and MUST NOT be called concurrently with any other use of that
//     handle. It is safe (and lock-free) at any point in a broadcast round,
//     including from within a select statement's "give up" branch.
//
// Violations of (4) or of the exclusive-ownership rules are detected where
// cheaply possible, and surface as panics rather than silent corruption.
//
// # Caveats
//
// The cascade converts latency into parallelism; with GOMAXPROCS=1 there is no
// parallelism to exploit, and the cascade is marginally slower than a linear
// broadcast (by a handful of atomics). It is strictly superior whenever
// receivers can run concurrently.
type ChanCascade[C ~chan V, V any] struct {
	// c holds the two alternating transports, indexed by phase.
	c [2]C

	// wake is a cap-1 signalling slot, allocated once, used to unpark a
	// sender that failed to complete its round within its spin budget.
	wake chan struct{}

	// sendMu serialises broadcast rounds. It has no other purpose; notably,
	// it is NOT held against registration, which is wholly lock-free.
	sendMu sync.Mutex

	_ chanCascadePad

	// state is the registration word; see the bit layout above. Mutated by
	// senders (sending bit, phase bit) and by registration (receiver count),
	// always via CAS, so that "capture the round membership" and "join/leave"
	// are totally ordered against one another.
	state atomic.Uint64

	_ chanCascadePad

	// relays is the round word. Seeded to n-1 at the start of each round, and
	// decremented once per receipt; a non-negative result confers the duty to
	// relay the value onwards. Reaching exactly -1 means "round complete":
	// all n receipts have occurred, which (by the counting argument in the
	// package docs) implies no claimed relay remains unsent.
	relays atomic.Int64

	// parked indicates the sender has exhausted its spin budget and is (or is
	// about to be) blocked on wake.
	parked atomic.Bool

	// dropped is a monotonic count of receipts absorbed by mid-round
	// unsubscribes; rounds take a difference of snapshots, so that it is never
	// reset (and therefore never races with an in-flight absorber).
	dropped atomic.Uint64

	_ chanCascadePad
}

// ChanCascadeReceiver is a registered receiver of a [ChanCascade]. It is a
// value type, two words wide, which does not escape under normal use, making
// the entire subscribe/receive/unsubscribe lifecycle allocation-free.
//
// A handle MUST NOT be copied once used, and MUST be used by only one
// goroutine at a time. See [ChanCascade] for the full contract.
type ChanCascadeReceiver[C ~chan V, V any] struct {
	x     *ChanCascade[C, V]
	phase uint32
}

// NewChanCascade constructs a [ChanCascade] over the two provided transport
// channels. Both must be unbuffered and distinct, or both nil. It panics
// otherwise.
func NewChanCascade[C ~chan V, V any](a, b C) *ChanCascade[C, V] {
	switch {
	case cap(a) != 0 || cap(b) != 0:
		panic(`bigbuff: chancascade: must use unbuffered channels`)
	case (a == nil) != (b == nil):
		panic(`bigbuff: chancascade: both channels must be nil, or neither`)
	case a != nil && a == b:
		panic(`bigbuff: chancascade: channels must be distinct`)
	}
	x := new(ChanCascade[C, V])
	x.c[0], x.c[1] = a, b
	x.wake = make(chan struct{}, 1)
	return x
}

// MakeChanCascade constructs a [ChanCascade] with freshly made transports. It
// is the idiomatic constructor, where the channel type need not be named.
func MakeChanCascade[V any]() *ChanCascade[chan V, V] {
	return NewChanCascade(make(chan V), make(chan V))
}

// Receivers returns the number of currently registered receivers. Note that
// this is inherently racy, for anything other than diagnostics.
func (x *ChanCascade[C, V]) Receivers() int {
	return int(uint32(x.state.Load() >> 32))
}

// Subscribe registers a new receiver, and returns its handle. It is lock-free,
// never blocks, and never allocates - including while a broadcast is in
// flight, in which case the new receiver cleanly joins from the next round.
//
// The caller MUST eventually call [ChanCascadeReceiver.Unsubscribe].
func (x *ChanCascade[C, V]) Subscribe() ChanCascadeReceiver[C, V] {
	x.checkUsedFactoryFunction()
	for {
		state := x.state.Load()
		receivers, flags := uint32(state>>32), uint32(state)
		if receivers >= chanCascadeMaxReceivers || flags&chanCascadeReservedBit != 0 {
			panic(chanCascadeInvariant)
		}
		phase := flags & chanCascadePhaseBit
		if flags&chanCascadeSendingBit != 0 {
			// A round is in flight, and this CAS is ordered after the CAS that
			// captured its membership - so we are NOT part of it. Join on the
			// opposite transport, which nothing will send on until that round
			// has fully completed.
			phase ^= 1
		}
		if x.state.CompareAndSwap(state, state+1<<32) {
			return ChanCascadeReceiver[C, V]{x: x, phase: phase}
		}
	}
}

// Send broadcasts a value to every registered receiver, blocking until every
// one of them has received it (or has unsubscribed mid-flight), and returns
// the number of receivers that actually received it.
//
// Exactly one send is performed by the calling goroutine; the remaining n-1
// deliveries are cascaded by the receivers themselves, in parallel.
//
// Send never allocates. Concurrent calls are serialised.
func (x *ChanCascade[C, V]) Send(value V) int {
	if x.state.Load()>>32 == 0 {
		return 0 // no receivers (fast path: single relaxed load)
	}

	x.checkUsedFactoryFunction()

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// Snapshot the absorb counter. Safe here, and only here: absorbers may
	// only increment it while the sending bit is set, and the previous round's
	// absorbers are all guaranteed to have completed (see await).
	dropped := x.dropped.Load()

	// Atomically capture round membership and claim the sending bit. This
	// single CAS is what totally orders membership against Subscribe.
	var receivers, phase uint32
	for {
		state := x.state.Load()
		receivers, phase = uint32(state>>32), uint32(state)
		if receivers > chanCascadeMaxReceivers ||
			phase&(chanCascadeSendingBit|chanCascadeReservedBit) != 0 {
			panic(chanCascadeInvariant)
		}
		if receivers == 0 {
			return 0 // no receivers (slow path)
		}
		phase &= chanCascadePhaseBit
		if x.state.CompareAndSwap(state, state|uint64(chanCascadeSendingBit)) {
			break
		}
	}

	// Publish the round. Receivers cannot observe any of this before the seed
	// send below, which is the only thing that can hand them a value.
	x.parked.Store(false)
	x.relays.Store(int64(receivers) - 1)

	// Seed the cascade. From here, delivery is self-propelled.
	x.c[phase] <- value

	x.await(receivers)

	if x.relays.Load() != -1 {
		panic(chanCascadeInvariant)
	}
	dropped = x.dropped.Load() - dropped
	if dropped > uint64(receivers) {
		panic(chanCascadeInvariant)
	}

	// Close the round: clear the sending bit and flip the phase. The receiver
	// count may legitimately have changed (joins and leaves are lock-free), so
	// only the flags are validated.
	for {
		state := x.state.Load()
		if uint32(state) != chanCascadeSendingBit|phase {
			panic(chanCascadeInvariant)
		}
		if x.state.CompareAndSwap(state, state&^uint64(math.MaxUint32)|uint64(phase^1)) {
			break
		}
	}

	return int(receivers) - int(dropped)
}

// await blocks until the current round is complete, i.e. until every receipt
// has been accounted for. Small rounds spin (so that the single-receiver case
// costs exactly one handoff, matching a direct channel send); larger rounds
// park immediately, amortising one park/unpark across n deliveries.
func (x *ChanCascade[C, V]) await(receivers uint32) {
	if receivers <= chanCascadeSpinMaxRecvs {
		for i := 0; i < chanCascadeSpin; i++ {
			if x.relays.Load() == -1 {
				return
			}
		}
	}

	// Dekker-style handshake against ChanCascade.signal. Go's sync/atomic
	// operations are sequentially consistent, so if the load below misses the
	// final receipt, that receipt's subsequent load of parked cannot miss this
	// store.
	x.parked.Store(true)
	for x.relays.Load() != -1 {
		<-x.wake
	}
	x.parked.Store(false)
}

// signal wakes a parked sender, if any. Non-blocking: a lost signal is
// impossible (see await), and a redundant signal is harmlessly consumed by a
// later park, which always re-checks the round word.
func (x *ChanCascade[C, V]) signal() {
	if x.parked.Load() {
		select {
		case x.wake <- struct{}{}:
		default:
		}
	}
}

// ack records a receipt, discharges any relay duty it confers, and completes
// the round if it was the last. This is the heart of the cascade: exactly one
// atomic read-modify-write per receiver, per value.
func (x *ChanCascade[C, V]) ack(phase uint32, value V) {
	switch remaining := x.relays.Add(-1); {
	case remaining >= 0:
		// We owe a relay. This send cannot deadlock: at any instant, the
		// number of unmatched sends is bounded above by the number of
		// receivers yet to be served.
		x.c[phase] <- value
	case remaining == -1:
		// Final receipt of the round. By the counting argument, no claimed
		// relay can still be unsent, so the round is genuinely complete.
		x.signal()
	default:
		// An unregistered or duplicated receive.
		panic(chanCascadeInvariant)
	}
}

func (x *ChanCascade[C, V]) checkUsedFactoryFunction() {
	if x.wake == nil {
		panic(`bigbuff: chancascade: must use factory function`)
	}
}

// C returns the transport channel this receiver must currently receive from.
// It is suitable for direct use in a select statement; an unselected case
// costs nothing, and requires no deregistration.
//
// The value returned changes after each value consumed, and MUST be re-read
// each time.
func (r *ChanCascadeReceiver[C, V]) C() C {
	if r.x == nil {
		panic(`bigbuff: chancascade: receiver is not subscribed`)
	}
	return r.x.c[r.phase]
}

// Received MUST be called immediately after each value received from
// [ChanCascadeReceiver.C], passing that value, and MUST NOT be called
// otherwise. It discharges this receiver's share of the broadcast - which may
// involve relaying the value onwards to a peer - and advances this receiver to
// the next phase.
//
// It never allocates, and performs exactly one atomic read-modify-write in
// addition to at most one channel send.
func (r *ChanCascadeReceiver[C, V]) Received(value V) {
	x := r.x
	if x == nil {
		panic(`bigbuff: chancascade: receiver is not subscribed`)
	}
	phase := r.phase
	r.phase = phase ^ 1
	x.ack(phase, value)
}

// Recv blocks until a value is broadcast, consumes it, discharges the
// associated relay duty, and returns the value. It is shorthand for receiving
// from [ChanCascadeReceiver.C] then calling [ChanCascadeReceiver.Received].
func (r *ChanCascadeReceiver[C, V]) Recv() V {
	value := <-r.C()
	r.Received(value)
	return value
}

// Unsubscribe deregisters this receiver, exactly once. It is lock-free, never
// allocates, and is safe at any point in a broadcast round.
//
// If a broadcast is in flight and this receiver still owes it a receipt, that
// receipt is absorbed (and relayed onwards) here, on behalf of the sender,
// which will exclude it from its reported delivery count. Consequently, this
// call may block for the duration of the in-flight round, exactly as a
// well-behaved receive would have.
func (r *ChanCascadeReceiver[C, V]) Unsubscribe() {
	x := r.x
	if x == nil {
		panic(`bigbuff: chancascade: receiver is not subscribed`)
	}
	r.x = nil

	// The phase bit doubles as an exact ownership oracle: a round is in flight
	// AND it is being broadcast on our phase if and only if we still owe it a
	// receipt. Receivers that have already consumed this round, and receivers
	// that joined after it began, have both already flipped away from it.
	var absorb bool
	for {
		state := x.state.Load()
		receivers, flags := uint32(state>>32), uint32(state)
		if receivers == 0 || receivers > chanCascadeMaxReceivers ||
			flags&chanCascadeReservedBit != 0 {
			panic(chanCascadeInvariant)
		}
		absorb = flags&chanCascadeSendingBit != 0 &&
			flags&chanCascadePhaseBit == r.phase
		if x.state.CompareAndSwap(state, state-1<<32) {
			break
		}
	}

	if absorb {
		// Ordered before the receipt, and therefore before the sender can
		// observe round completion.
		x.dropped.Add(1)
		x.ack(r.phase, <-x.c[r.phase])
	}
}

// SubscribeContext immediately subscribes, and returns a single-use iterator
// yielding every value broadcast until the context is cancelled, or iteration
// stops. Unsubscription is automatic and exactly-once, on every path.
//
// WARNING: The returned iterator MUST be used promptly, OR the context MUST be
// promptly cancelled, to avoid stalling senders.
func (x *ChanCascade[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	r := x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}

	// Covers never-iterated and iterated-after-cancel; disarmed by the
	// iterator, which thereafter owns the (single-goroutine) handle.
	stop := context.AfterFunc(ctx, (&r).Unsubscribe)

	return func(yield func(V) bool) {
		if !stop() {
			return // cancelled, or called more than once
		}
		defer (&r).Unsubscribe()

		if yield == nil {
			panic(`bigbuff: chancascade: iterator yield func is nil`)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-r.C():
				if !ok {
					return // transport closed: outside the contract
				}
				r.Received(value)
				if !yield(value) {
					return
				}
			}
		}
	}
}
```

---

## Usage

```go
cc := bigbuff.MakeChanCascade[int]()

for range 10_000 {
    go func() {
        r := cc.Subscribe()
        defer r.Unsubscribe()
        for {
            select {
            case <-done:
                return          // no registration churn: just leave
            case v := <-r.C():
                r.Received(v)   // mandatory, immediate
                use(v)
            }
        }
    }()
}

n := cc.Send(42) // one send by this goroutine; ~14 handoffs deep, 10k wide
```

## Notes on migration / complementarity

* `ChanCascade` is a **drop-in conceptual replacement for `ChanPubSub`** with strictly better asymptotics and a much smaller trusted core (no `broken` channel, no `TryRLock` loop, no `sync.Cond`, no nested mutexes). `ChanPubSub.Wait()` is subsumed by `Received(v)`, which does strictly more work for strictly less cost.
* `ChanCaster` remains the right tool for the *speculative, one-shot* pattern (register → select → maybe deregister) where receivers are transient. `ChanCascade` is the right tool for *persistent* subscribers, which is where consumer churn and large `n` actually bite.
* Suggested tests: `-race` fan-out matrices over `{n=1..1024} × {churn}` asserting `Send` return values equal `n − absorbed`; a `relays`/`state` invariant fuzz harness; and a `testing.AllocsPerRun` assertion of `0` on `Send`, `Subscribe`, `Recv`, and `Unsubscribe`.
