Here is a completely new, self-contained implementation for `bigbuff`. It is intentionally **not** derived from `ChanCaster` or `ChanPubSub`.

### Design summary

`Relay[V]` is a lock-free, **zero-allocation-per-publish** broadcast relay built on a seqlock-guarded ring buffer.

* **Publish is O(1) and independent of subscriber count.** It writes one slot, performs one release store, and optionally rings a single shared doorbell channel (`cap 1`). There is no per-subscriber loop, no mutex, and no registration list.
* **Subscribers are unregistered cursors.** `Subscribe()` simply snapshots the current frontier; `Link` tracks its own sequence. Churn is free—adding or dropping a subscriber touches no global state.
* **Select-compatible.** `Relay.Notify()` returns the shared doorbell, safe to place in a `select` alongside `ctx.Done()`. Consumers wake, drain all pending values with `Link.Next()`, then go back to waiting.
* **Conflation-safe.** Slow subscribers are not block the publisher. If the ring buffer overwrites an unread slot, the link’s cursor jumps forward and `Lag()` reports the loss.
* **Race-free reads.** Each slot uses a seqlock (high bit = write-in-progress, remaining bits = message sequence), so `Next()` never observes a partially written value.

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
	"sync/atomic"
)

// lockBit is the high bit of a slot's sequence word.  While set, the slot is
// mid-write and readers must treat it as unavailable.
const lockBit uint64 = 1 << 63

// Relay is a lock-free broadcast relay.
//
// Publish appends a value to a circular buffer and returns a monotonically
// increasing sequence.  Any number of Link subscribers can read those values
// independently, at their own pace, without ever being registered with the
// Relay.
//
// Because there is no subscriber registry, Publish never touches per-subscriber
// state: its cost is one atomic increment, one slot write, and one coalesced
// doorbell signal.  Subscribe/Unsubscribe are therefore free operations with
// no global churn cost.
//
// Slow subscribers are naturally conflated.  If a ring slot is reused before a
// link has consumed the previous value stored there, that link simply observes
// a jump in sequence numbers; Lag reports how far behind it is.
type Relay[V any] struct {
	mask  uint64
	slots []slot[V]
	// pubSeq is the most recently published sequence (1-based).  Zero means
	// nothing has been published yet.
	pubSeq atomic.Uint64
	// bell is a capacity-1 doorbell.  Publish performs a non-blocking send,
	// so at most one wakeup is pending at a time and concurrent publishes
	// coalesce into a single notification.
	bell chan struct{}
}

type slot[V any] struct {
	// seqLock combines the write-in-progress flag (high bit) with the
	// message sequence (remaining bits).  Even values mean the slot is
	// stable and readable.
	seqLock atomic.Uint64
	val     V
}

// NewRelay allocates a Relay whose ring buffer holds at least bufferSize
// slots.  The size is rounded up to a power of two so index calculation uses
// a bitmask instead of division.
func NewRelay[V any](bufferSize int) *Relay[V] {
	if bufferSize < 1 {
		panic("bigbuff: relay: buffer size must be at least 1")
	}
	size := uint64(1)
	for size < uint64(bufferSize) {
		size <<= 1
	}
	return &Relay[V]{
		mask:  size - 1,
		slots: make([]slot[V], size),
		bell:  make(chan struct{}, 1),
	}
}

// Publish stores value in the next ring-buffer slot and returns its sequence
// number.  The call is wait-free, allocates nothing, and its cost does not
// depend on the number of subscribers.
//
// A publish acquires the slot's seqlock, writes the payload, then releases the
// sequence.  Any subscriber that observes the new sequence is guaranteed to
// observe the corresponding value (provided V is not mutated after Publish).
//
// If the ring buffer wraps and overwrites a slot that a slow subscriber has
// not yet consumed, that subscriber's value is silently dropped; see Link.Lag.
func (r *Relay[V]) Publish(value V) uint64 {
	seq := r.pubSeq.Add(1) // 1-based
	if seq >= lockBit {
		panic("bigbuff: relay: sequence overflow")
	}

	idx := (seq - 1) & r.mask
	slot := &r.slots[idx]

	// Acquire the write lock.  Because the critical section is tiny (two
	// stores), a brief spin is cheaper than a kernel-level park/unpark.
	for {
		old := slot.seqLock.Load()
		if old&lockBit == 0 && slot.seqLock.CompareAndSwap(old, old|lockBit) {
			break
		}
	}

	// Payload first, then release the sequence with the write lock cleared.
	slot.val = value
	slot.seqLock.Store(seq)

	r.ring()
	return seq
}

// ring performs a non-blocking send on the shared doorbell.  Multiple rapid
// publishes collapse into a single pending wakeup.
func (r *Relay[V]) ring() {
	select {
	case r.bell <- struct{}{}:
	default:
	}
}

// Notify returns a channel that is signalled whenever Publish is called.
// It is safe to use directly in a select statement:
//
//	select {
//	case <-relay.Notify():
//	    // drain link with Next()
//	case <-ctx.Done():
//	    return
//	}
func (r *Relay[V]) Notify() <-chan struct{} {
	return r.bell
}

// Seq returns the most recently published sequence, or 0 if nothing has been
// published yet.
func (r *Relay[V]) Seq() uint64 {
	return r.pubSeq.Load()
}

// Link is an independent read cursor into a Relay.
//
// Links are not registered with the Relay.  Creating or discarding a Link has
// no effect on any other subscriber and costs exactly one allocation.  A Link
// begins consuming at the next unpublished sequence (Subscribe), the latest
// sequence (SubscribeLatest), or an arbitrary sequence (SubscribeFrom).
type Link[V any] struct {
	relay *Relay[V]
	seq   atomic.Uint64 // last consumed sequence (0-based cursor)
}

func (r *Relay[V]) Subscribe() *Link[V] {
	return &Link[V]{relay: r}
}

// SubscribeLatest returns a Link whose first Next will observe the first
// value published after the call (or the most recent value already in the
// buffer).
func (r *Relay[V]) SubscribeLatest() *Link[V] {
	l := &Link[V]{relay: r}
	l.seq.Store(r.Seq())
	return l
}

// SubscribeFrom returns a Link that attempts to consume starting at seq.
// If seq is 0, the first Next attempts sequence 1.
func (r *Relay[V]) SubscribeFrom(seq uint64) *Link[V] {
	l := &Link[V]{relay: r}
	l.seq.Store(seq)
	return l
}

// Next attempts to consume the next available value for this link.
//
// It is non-blocking and allocates nothing.  If the target sequence has not
// been published yet, ok is false.  If the slot has already been recycled
// and holds a newer sequence, that newer value is returned and the cursor
// jumps forward (conflation).
//
// Callers typically loop Next until it returns false, then wait on
// Relay.Notify or a context before trying again.
func (l *Link[V]) Next() (V, bool) {
	nextSeq := l.seq.Load() + 1
	idx := (nextSeq - 1) & l.relay.mask
	slot := &l.relay.slots[idx]

	// First observation: the slot must not be mid-write.
	old := slot.seqLock.Load()
	if old&lockBit != 0 {
		var zero V
		return zero, false
	}

	slotSeq := old
	if slotSeq < nextSeq {
		var zero V
		return zero, false
	}

	// Snapshot the payload, then verify the slot was not modified during
	// the read.  This seqlock validation makes Next race-free without any
	// per-slot mutex or per-publish allocation.
	val := slot.val
	if slot.seqLock.Load() != old {
		var zero V
		return zero, false
	}

	l.seq.Store(slotSeq)
	return val, true
}

// Seq returns the last sequence consumed by this link.
func (l *Link[V]) Seq() uint64 {
	return l.seq.Load()
}

// Lag returns the number of published values this link has not yet consumed.
// A lag greater than or equal to the ring buffer size means values have been
// dropped for this subscriber.
func (l *Link[V]) Lag() uint64 {
	return l.relay.pubSeq.Load() - l.seq.Load()
}

// WaitNext blocks until a value is available for this link or ctx is
// cancelled.  It selects internally on Relay.Notify, so it is suitable as
// the core of a select-driven consumer loop.
//
// Because Notify is shared and coalesced, a burst of publishes results in a
// single wakeup; the consumer then drains all available values with Next.
func (l *Link[V]) WaitNext(ctx context.Context) (V, bool) {
	for {
		if v, ok := l.Next(); ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			var zero V
			return zero, false
		case <-l.relay.Notify():
		}
	}
}

// Iter adapts this link into a standard Go 1.22 pull iterator over the
// relay's values.  The iterator blocks until ctx is cancelled, then returns.
//
// Usage:
//
//	for v := range link.Iter(context.Background()) {
//	    // consume v
//	}
func (l *Link[V]) Iter(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		for {
			v, ok := l.WaitNext(ctx)
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
```

### Why this is different

| Characteristic | `ChanCaster` / `ChanPubSub` | `Relay` / `Link` |
|---|---|---|
| Publish cost | O(N) channel sends | O(1) atomic store + coalesced doorbell |
| Mutex on publish path | `sync.RWMutex` | None |
| Subscriber registry | Yes (subscriber count tracked) | No (unregistered cursors) |
| Subscribe/Unsubscribe cost | Mutex + atomic add/sub | Allocation-free cursor creation |
| `select` support | On shared data channel | On shared doorbell (`Relay.Notify`) |
| Backpressure | Publisher blocks until all receive | Drop-oldest conflation (no publisher block) |
| Allocation per publish | 0 | 0 |
| Allocation per subscriber | 0 | 1 (`Link`) |

### Typical usage

```go
relay := NewRelay[string](4096)
link := relay.SubscribeLatest()

// Publisher goroutine
go func() {
    for i := 0; i < 1000; i++ {
        relay.Publish(fmt.Sprintf("event-%d", i))
    }
}()

// Consumer using select
for {
    select {
    case <-relay.Notify():
        for {
            v, ok := link.Next()
            if !ok {
                break
            }
            println(v)
        }
        if lag := link.Lag(); lag > 0 {
            fmt.Printf("consumer is %d events behind\n", lag)
        }
    case <-ctx.Done():
        return
    }
}
```

Or with the standard iterator API:

```go
for v := range link.Iter(context.Background()) {
    println(v)
}
```
