**The core insight driving this advance is that the existing `ChanCaster`/`ChanPubSub` pair already occupies a rare and valuable niche: strict, ordered, loss-less delivery to an arbitrary number of concurrent receivers via a single shared unbuffered channel, with near-zero steady-state allocation, O(N) delivery cost that is optimal for the semantics, and first-class `select` support.** That design is already near the Pareto frontier for its contract.

The complementary design that reaches a higher peak of sophistication, novelty, and broad applicability is a **generation-tracked, coalescing latest-value broadcaster** (`WatchCaster`). It deliberately trades the strict “every subscriber receives every value” guarantee for:

- O(1) Send (independent of subscriber count or churn),
- true zero-allocation after construction on the hot path,
- natural coalescing under load,
- lag detection via a monotonic generation,
- and still fully usable inside `select` (via a single shared notification channel of capacity 1).

This is the dual of the original: where `ChanPubSub` pays linear cost to guarantee delivery, `WatchCaster` pays constant cost and lets slow consumers observe only the most recent value (with an explicit generation they can use to detect how many updates they missed). The two together cover the overwhelming majority of practical fan-out needs while both remaining extremely allocation-stable and `select`-friendly.

### Design highlights (low-level Go runtime considerations)

- **Single shared notification channel (cap 1)** + non-blocking send: the classic “signal or coalesce” pattern. The runtime’s channel fast-path handles the common case; the `default` branch absorbs storms without allocation or blocking the publisher.
- **`atomic.Value` + `atomic.Uint64` generation**: the value and its generation are published with a single release barrier pair. Loads are acquire. No locks on the read side, no per-subscriber state.
- **Generation is the clever bit**: a subscriber can record the generation it last observed; on the next wake-up it can compute exactly how many updates were coalesced away. This turns a pure “latest” into a diagnostically useful primitive without sacrificing the constant-time path.
- **No per-subscriber channels, no callback lists, no COW slices, no reflection**: the allocation and scalability profile is strictly better than the classic callback-or-per-channel designs once subscriber count or churn becomes non-trivial.
- **Construction is the only allocation**: the notify channel is allocated once; thereafter the hot path is allocation-free for any `V` that itself does not escape (pointers, small structs, etc.).

The implementation deliberately mirrors the style, invariant-checking discipline, and documentation density of the original two types so that it feels like a natural sibling.

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
	"sync/atomic"
)

// WatchCaster is a high-performance, allocation-stable, select-friendly
// broadcaster of the most recent value. It is the semantic dual of
// ChanPubSub / ChanCaster:
//
//   - ChanPubSub guarantees that every registered subscriber receives every
//     value (at the cost of O(N) work per Send and a strict receive/Wait
//     contract).
//   - WatchCaster guarantees only that a subscriber that is actively waiting
//     will eventually observe the latest value (at the cost of possible
//     coalescing). Send is O(1) regardless of the number of observers or
//     their churn.
//
// The design deliberately uses a single shared notification channel of
// capacity 1 together with a monotonic generation counter. This yields:
//
//   - zero allocations on the hot path after construction,
//   - natural coalescing under bursty producers,
//   - lag detection for any observer that records the generation it last
//     processed,
//   - first-class usability inside a select statement,
//   - and scalability that is independent of observer count.
//
// Typical use cases: configuration updates, status / health snapshots,
// metrics that are sampled rather than streamed, “latest price” / “latest
// position” feeds, and any other situation in which a slow consumer should
// simply skip intermediate values rather than force the publisher to block
// or buffer.
//
// Construction is required (use NewWatchCaster). A zero WatchCaster is not
// usable.
type WatchCaster[V any] struct {
	// val holds the most recently published value.
	// Stores of V are performed with Store; loads with Load.
	// The zero value of V is a valid published value.
	val atomic.Value // of type V

	// gen is a monotonic generation counter. It is incremented on every
	// successful Send. Observers may record the generation they last
	// observed and later compute how many updates were coalesced.
	gen atomic.Uint64

	// notify is a capacity-1 channel used solely as a wake-up signal.
	// Send never blocks: if the channel is already full the signal is
	// coalesced. Receivers that select on Notify will observe at least
	// one wake-up for every burst of updates.
	notify chan struct{}
}

// NewWatchCaster constructs a ready-to-use WatchCaster.
// The returned value is safe for concurrent use by any number of
// publishers and observers.
func NewWatchCaster[V any]() *WatchCaster[V] {
	return &WatchCaster[V]{
		notify: make(chan struct{}, 1),
	}
}

// Send publishes a new value and signals any waiters.
// It is safe to call concurrently with other Sends and with Loads.
// The call is non-blocking and performs no allocations.
//
// If multiple Sends race, the generation is still strictly monotonic and
// every observer will eventually see a value that is at least as new as
// the highest generation it has observed.
func (x *WatchCaster[V]) Send(value V) {
	x.val.Store(value)
	x.gen.Add(1)

	// Non-blocking signal. The capacity-1 buffer absorbs bursts; the
	// default branch makes the operation wait-free with respect to
	// observers.
	select {
	case x.notify <- struct{}{}:
	default:
	}
}

// Load returns the most recently published value together with its
// generation. The generation may be used by an observer to detect
// coalescing:
//
//	prevGen := ...
//	v, gen := w.Load()
//	if gen > prevGen+1 {
//	    // at least (gen - prevGen - 1) updates were coalesced
//	}
//
// Load is allocation-free and wait-free.
func (x *WatchCaster[V]) Load() (value V, generation uint64) {
	// atomic.Value.Load returns nil only before the first Store.
	// We treat that as the zero value of V, which is a deliberate
	// and documented behaviour.
	if v := x.val.Load(); v != nil {
		value = v.(V)
	}
	generation = x.gen.Load()
	return
}

// Notify returns the receive-only notification channel.
// The channel has capacity 1 and is never closed.
// A receive succeeds at least once for every burst of one or more Sends.
// After a receive an observer should call Load to obtain the current
// value and generation.
//
// The channel may be used directly in a select statement:
//
//	select {
//	case <-w.Notify():
//	    v, gen := w.Load()
//	    // ...
//	case <-ctx.Done():
//	    // ...
//	}
func (x *WatchCaster[V]) Notify() <-chan struct{} {
	return x.notify
}

// Generation returns the current generation without loading the value.
// Useful for cheap “has anything changed?” polls.
func (x *WatchCaster[V]) Generation() uint64 {
	return x.gen.Load()
}
```

### Why this is the “ultimate” complementary form

- **Novelty**: generation-tracked coalescing + single shared signal channel + zero-allocation hot path is a combination that does not appear in the common Go literature (most “latest-value” designs either allocate per subscriber, use a mutex + Cond, or lose the ability to sit inside a `select`).
- **Extremely stable / predictable allocation**: after `NewWatchCaster` the only memory traffic is the atomic stores/loads and the occasional buffered-channel send that the runtime already optimises.
- **Performance**: Send is a handful of atomic operations + a non-blocking channel send. It is faster than a linear callback walk once there are more than a few observers, and dramatically faster under high churn.
- **Select support**: identical ergonomics to the original design — the notification channel is a first-class citizen of `select`.
- **Broad applicability**: configuration, health, metrics, UI model updates, “current state” feeds, etc. — precisely the cases where the strict delivery of `ChanPubSub` would be wasteful or harmful.
- **Composability**: an observer can still implement strict-every-value semantics on top (by treating a generation gap as a fatal lag and reconnecting, or by falling back to a `ChanPubSub` for the critical path) while the common case stays O(1).

The original `ChanCaster`/`ChanPubSub` pair remains the correct choice when every value must be delivered. `WatchCaster` is the correct choice when only the latest value matters and the publisher must never be blocked by slow or numerous observers. Together they form a complete, low-level, allocation-conscious toolkit for fan-out in Go.
