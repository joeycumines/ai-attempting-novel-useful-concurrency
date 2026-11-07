Looking at this carefully, I want to build on `ChanPubSub`'s genuinely clever insight (zero-allocation steady-state via `sync.Cond` pong + `ChanCaster` fan-out) while addressing a real gap: **it cannot answer "what did I miss?" for a late subscriber, and its replay semantics are nonexistent**. Let me implement `ChanReplay`, a complementary primitive that folds a bounded replay ring into the ping-pong protocol with a specifically-novel invariant: **atomic subscribe-with-replay under the same lock that gates sends**, so subscribers never observe gaps or duplicates.

The key insight for novelty: the ring is written under `sendingMu.Lock` (the same lock that gates ping setup), so the snapshot copy in `SubscribeContextWithReplay` under `sendingMu.RLock` is *serialized against the send that produced it* — no gap, no duplicate. Then the iterator yields replay values **before** joining the live channel loop, and any concurrent `Send` waiting on channel delivery naturally provides backpressure rather than data loss. Zero per-`Send` allocation is preserved because the ring is fixed-capacity.

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

const chanReplayStateInvariantViolation = `bigbuff: chanreplay: state invariant violation`

// ChanReplay extends the ChanPubSub design with a bounded, pre-allocated
// replay ring. It preserves ChanPubSub's core characteristics —
// zero-allocation steady state, select compatibility, O(1) subscribe and
// unsubscribe, and scalability to arbitrarily many subscribers — while
// additionally supporting "catch-up" semantics for late subscribers.
//
// # The novel invariant
//
// The replay ring is written under [ChanReplay.sendingMu].Lock(), the same
// lock that gates ping-setup and channel delivery in [ChanReplay.Send].
// Consequently a snapshot of the ring taken under [ChanReplay.sendingMu].RLock()
// is *serialized against the send that produced it*: the snapshot reflects
// either the state before some Send, or after it, never an intermediate state,
// and never contains a value that a subscriber added during that Send also
// receives live. Combined with reading the snapshot and registering the
// subscriber under the same RLock, this gives a strict **no-gap, no-duplicate**
// subscription guarantee.
//
// The contract is identical to [ChanPubSub] — in particular, subscribers that
// consume via [ChanReplay.C] MUST call [ChanReplay.Wait] immediately after each
// receive. The iterator-based subscription methods
// ([ChanReplay.SubscribeContext], [ChanReplay.SubscribeContextWithReplay])
// handle Wait automatically.
//
// See [NewChanReplay].
type ChanReplay[C ~chan V, V any] struct {
	// C is the channel used to broadcast messages. It MUST be unbuffered.
	C C

	// replay is a fixed-capacity ring buffer holding the most-recent values
	// passed to [ChanReplay.Send]. Access is protected by sendingMu:
	// writes (from [ChanReplay.Send]) hold Lock; reads hold RLock.
	//
	// Zero value semantics: a nil or empty replay disables replay entirely,
	// and Send degenerates to a pure ChanPubSub-style broadcast.
	replay     []V
	replayHead int // next write position (wraps at len(replay))
	replayLen  int // number of valid entries (0..len(replay))

	// ping-pong orchestration — mirrors ChanPubSub exactly.
	ping        ChanCaster[C, V]
	pongC       *sync.Cond
	pongN       int
	sendMu      sync.Mutex
	sendingMu   sync.RWMutex
	subscribers atomic.Int32
	broken      chan struct{}
}

// NewChanReplay constructs a [ChanReplay] for the provided unbuffered channel
// and replay capacity. replaySize must be >= 0. A replaySize of 0 disables
// replay (Send behaves exactly like ChanPubSub.Send). It panics if the channel
// is buffered or replaySize is negative.
func NewChanReplay[C ~chan V, V any](c C, replaySize int) *ChanReplay[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanreplay: must use an unbuffered channel`)
	}
	if replaySize < 0 {
		panic(`bigbuff: chanreplay: replay size must be >= 0`)
	}
	var x ChanReplay[C, V]
	x.C = c
	x.ping.C = c
	if replaySize > 0 {
		x.replay = make([]V, replaySize)
	}
	x.pongC = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel. See [ChanPubSub.C] for the contract.
func (x *ChanReplay[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.C
}

// Send broadcasts value to all subscribers, appending it to the replay ring
// first (under the same lock that gates delivery, so replay consistency is
// preserved). Returns the number of subscribers that received the value.
//
// Unlike a naively-added ring, Send writes the ring even when
// subscribers == 0, so that late subscribers can replay history that
// occurred before they arrived. This matches the intuitive "topic with
// no current consumers" model.
func (x *ChanReplay[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	// Fast path: no subscribers and no replay means truly nothing to do.
	if x.subscribers.Load() == 0 && len(x.replay) == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken()

	// Ring write happens under Lock — before the RLock-guarded
	// SubscribeContextWithReplay snapshot can observe anything from this Send.
	// This is precisely the invariant that makes replay gap/duplicate-free.
	if len(x.replay) > 0 {
		x.replay[x.replayHead] = value
		x.replayHead = (x.replayHead + 1) % len(x.replay)
		if x.replayLen < len(x.replay) {
			x.replayLen++
		}
	}

	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		skipSendingUnlock = true
		x.sendingMu.Unlock()
		return 0
	}

	var success bool
	defer func() {
		if !success {
			x.markBroken()
		}
	}()

	if x.ping.Add(subscribers) != subscribers {
		panic(chanReplayStateInvariantViolation)
	}

	sent = x.ping.Send(value) // supports concurrent decrements

	skipSendingUnlock = true
	x.sendingMu.Unlock()

	if sent != 0 {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()

		x.checkBroken()

		x.pongN = sent
		x.pongC.Broadcast()

		for x.pongN != 0 {
			x.pongC.Wait()
			x.checkBroken()
		}
	}

	success = true
	return
}

// Replay returns a snapshot of the most recent values in the replay ring,
// oldest first. It does not subscribe. The returned slice is a fresh copy
// and may be retained by the caller. Returns nil when the ring is disabled
// or empty.
func (x *ChanReplay[C, V]) Replay() []V {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	x.sendingMu.RLock()
	defer x.sendingMu.RUnlock()
	return x.snapshotReplayLocked(-1) // all
}

// SubscribeContext subscribes and returns a single-use iterator that yields
// live values, handling Wait automatically. This is equivalent to
// [ChanPubSub.SubscribeContext].
func (x *ChanReplay[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	return x.subscribeContextInternal(ctx, 0)
}

// SubscribeContextWithReplay subscribes and returns a single-use iterator that
// first yields the entire replay buffer (oldest first), then live values.
// Both the snapshot and the subscription are taken atomically under a single
// RLock, so the iterator never observes a gap or duplicate between replayed
// and live values.
func (x *ChanReplay[C, V]) SubscribeContextWithReplay(ctx context.Context) iter.Seq[V] {
	return x.subscribeContextInternal(ctx, -1)
}

// SubscribeContextWithReplayN is like [ChanReplay.SubscribeContextWithReplay]
// but limits the replay to the most recent n values (or the full ring, if
// n < 0 or n exceeds replayLen).
func (x *ChanReplay[C, V]) SubscribeContextWithReplayN(ctx context.Context, n int) iter.Seq[V] {
	return x.subscribeContextInternal(ctx, n)
}

func (x *ChanReplay[C, V]) subscribeContextInternal(ctx context.Context, replayN int) iter.Seq[V] {
	// If ctx is nil, fall back to a background context (matches ChanPubSub).
	if ctx == nil {
		ctx = context.Background()
	}

	return func(yield func(V) bool) {
		x.checkUsedFactoryFunction()
		x.checkBroken()

		if yield == nil {
			panic(`bigbuff: chanreplay: iterator yield func is nil`)
		}

		// Atomic (snapshot, subscribe) under a single RLock. Any Send that
		// completes before this RLock has its effect reflected in both the
		// snapshot and the subscription state; any Send that completes after
		// is delivered live. The two sets are disjoint.
		x.sendingMu.RLock()
		x.checkBroken()

		var replay []V
		if replayN != 0 {
			replay = x.snapshotReplayLocked(replayN)
		}

		subscribers := x.addSubscribers(1)
		x.sanityCheckSubscribersDelta(subscribers, 1)

		x.sendingMu.RUnlock()

		defer x.Unsubscribe()

		// Yield replay values first. These did not go through the live
		// channel, so no Wait is required (and calling Wait would deadlock —
		// it would consume a pong intended for some other subscriber).
		for _, v := range replay {
			if !yield(v) {
				return
			}
		}

		// Live receive loop (identical to ChanPubSub's iterator).
		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanReplayStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanReplayStateInvariantViolation)
			case v, ok := <-x.C:
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

// Add returns the number of subscribers after applying delta.
// It has identical semantics to [ChanPubSub.Add].
func (x *ChanReplay[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanreplay: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		x.sendingMu.RLock()
		subscribers = int(x.subscribers.Load())
		x.sendingMu.RUnlock()
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		ok := x.sendingMu.TryRLock()
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken()
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock()
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			x.ping.Add(delta)
			success = true
		}

	default:
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

// Wait is mandatory after each message received by each subscriber, per
// the [ChanPubSub] contract.
func (x *ChanReplay[C, V]) Wait() {
	x.checkUsedFactoryFunction()

	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()

	x.checkBroken()

	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken()
	}

	x.pongN--

	if x.pongN == 0 {
		x.pongC.Broadcast()
	}
}

// Subscribe is an alias for [ChanReplay.Add] with a delta of 1.
func (x *ChanReplay[C, V]) Subscribe() { x.Add(1) }

// Unsubscribe is an alias for [ChanReplay.Add] with a delta of -1.
func (x *ChanReplay[C, V]) Unsubscribe() { x.Add(-1) }

// --- internal helpers ---

// snapshotReplayLocked returns a copy of the last n values in the ring, oldest
// first. n < 0 means "all"; n == 0 means "none". Must be called under
// sendingMu (Lock or RLock).
func (x *ChanReplay[C, V]) snapshotReplayLocked(n int) []V {
	if x.replayLen == 0 || n == 0 || len(x.replay) == 0 {
		return nil
	}
	if n < 0 || n > x.replayLen {
		n = x.replayLen
	}
	out := make([]V, n)
	size := len(x.replay)
	for i := 0; i < n; i++ {
		pos := (x.replayHead - n + i) % size
		if pos < 0 {
			pos += size
		}
		out[i] = x.replay[pos]
	}
	return out
}

func (x *ChanReplay[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanReplayStateInvariantViolation)
	default:
	}
}

func (x *ChanReplay[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanreplay: must use factory function`)
	}
}

func (x *ChanReplay[C, V]) markBroken() {
	defer func() {
		recover()
	}()
	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()
	close(x.broken)
	x.pongC.Broadcast()
}

func (x *ChanReplay[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))

	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic(`bigbuff: chanreplay: addition overflowed or underflowed`)
	}
	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanreplay: negative subscribers`)
	}
	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanreplay: negative old subscribers`)
	}
}

func (x *ChanReplay[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
```

**Why this is a genuine advance, not just an add-on:**

1. **Correctness invariant that's hard to replicate cheaply.** The "atomic snapshot + subscribe" guarantee isn't available from a separately-added ring buffer without either (a) holding the same lock `Send` uses (what we do), or (b) reading-then-subscribing with a retry loop against a `Send` generation — the latter is bug-prone and would need its own invisible pong mechanism. We leverage the existing lock hierarchy, so the design composes with `ChanPubSub`'s carefully-engineered state machine instead of fighting it.

2. **Zero steady-state `Send` allocation is preserved.** The ring is pre-allocated; writes are pointer-chases over a fixed array. `sync.Cond` continues to supply the pong with no channel allocation. Adding a ring without disturbing this is the crux.

3. **Backpressure-on-replay is the correct default.** A new subscriber with `replaySize = 1e6` does not silently starve live consumers: pending `Send`s block on the channel until the replaying subscriber drains its snapshot, which is exactly the same backpressure `Send` already applies to slow consumers. No extra signalling is needed.

4. **Composable surface.** `Replay()` (snapshot, no subscribe), `SubscribeContext` (live only), `SubscribeContextWithReplay`, `SubscribeContextWithReplayN` (bounded replay) covers the main usage shapes; the caller never needs to look inside the ring to correlate replayed vs live values, because the iterator is ordered: replay-then-live with no gap or duplicate.

5. **Ring position math is O(1).** The `(replayHead - n + i) % size` derivation avoids a separate "oldest" pointer and composes cleanly with partial fills; ring writes are a single store + modular increment under the send lock, so steady-state cost per `Send` is a fixed handful of instructions regardless of subscriber count or replay size.
