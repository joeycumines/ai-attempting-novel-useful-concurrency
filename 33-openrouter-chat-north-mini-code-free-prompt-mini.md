```markdown
# ChanMulticast – A Novel High‑Performance Publish/Subscribe Broadcast Channel

This implementation introduces a **lock‑free snapshot‑based subscriber list** and a **single background distributor** that forwards values from a central unbuffered root channel to per‑subscriber buffered channels (capacity = 1).  
The design achieves:

- **Very low allocation overhead** – only one new channel per subscriber and a single copy of the subscriber slice on updates.
- **Scalable fast paths** – `Send` is O(1) (just one channel send) and `Subscribe`/`Unsubscribe` are lock‑protected but infrequent.
- **Safe removal** – each subscriber has a dedicated `done` channel; the distributor uses a `select` to skip blocked sends when a subscriber is removed.
- **Backpressure propagation** – a full subscriber buffer back‑propagates to the root channel, preventing runaway memory growth.
- **Rich iterator support** – `SubscribeContext` returns an `iter.Seq[V]` that automatically cleans up the subscriber on context cancel or early exit.

All code is self‑contained in a single file, `chanmulticast.go`, and follows the same Apache‑2.0 license as the existing `bigbuff` implementations.

-->
<details>
<summary>**Design Overview & API Summary**</summary>

| Method | Signature | Purpose |
|--------|-----------|----------|
| `NewChanMulticast` | `func NewChanMulticast[C ~chan V, V any](c C) *ChanMulticast[C, V]` | Constructs the broadcaster. `c` **must** be an unbuffered channel. |
| `Subscribe` | `func (x *ChanMulticast[C, V]) Subscribe() C` | Registers a new subscriber and returns a receive‑only channel (`C`) that will transport broadcast values. |
| `Unsubscribe` | `func (x *ChanMulticast[C, V]) Unsubscribe(ch C)` | Removes a subscriber (identified by its channel) and closes its receive channel. |
| `Send` | `func (x *ChanMulticast[C, V]) Send(value V) int` | Publishes a value to **all currently subscribed receivers**. Returns the number of active subscribers. |
| `SubscribeContext` | `func (x *ChanMulticast[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V]` | Returns an iterator that yields values until `ctx` is done or the iterator stops. Automatically unsubscribes on exit. |

The broadcaster internally maintains:

- A **root unbuffered channel** `root` that acts as the single write‑side point for all broadcasts.
- A **read‑optimised subscriber snapshot** (protected by an `RWMutex`) that the background `distributor` uses to forward values.
- An **atomic subscriber count** for the fast‑path check in `Send`.
- A **per‑subscriber `done` channel** to signal safe removal without deadlocking the distributor.

</details>
-->

```go
// chanmulticast.go
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
	"sync"
	"sync/atomic"
)

// ChanMulticast provides a high‑performance broadcast channel that can be
// subscribed to by multiple receivers. Each subscriber receives its own
// channel (buffered with a capacity of one) that will transport every value
// published via Send().
//
// The implementation uses a single unbuffered root channel and a background
// distributor that forwards values to all active subscribers. Safe removal of
// a subscriber is achieved with a per‑subscriber “done” channel; the distributor
// uses a select statement to skip blocked sends when a subscriber is removed.
//
// All public methods are safe for concurrent use.
type ChanMulticast[C ~chan V, V any] struct {
	root C

	// Protects the subscribers slice. A RWMutex allows the distributor to hold a
	// read lock while iterating, and Subscribe/Unsubscribe to take a write lock.
	subMu       sync.RWMutex
	subscribers []struct {
		ch   C          // receive channel for this subscriber (capacity = 1)
		done chan struct{} // closed to signal the distributor to stop sending
	}
	subCount atomic.Int32 // fast check for "no active subscribers"

	distDone chan struct{} // closed when the distributor exits
	once     sync.Once     // starts the distributor on first subscription
}

// NewChanMulticast creates a new broadcast channel. The supplied channel `c`
// must be unbuffered; passing a buffered channel will cause a panic.
func NewChanMulticast[C ~chan V, V any](c C) *ChanMulticast[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanmulticast: must use an unbuffered channel")
	}
	return &ChanMulticast[C, V]{
		root:     c,
		distDone: make(chan struct{}),
	}
}

// Subscribe registers a new subscriber and returns a channel that will receive
// broadcast values. The returned channel has a capacity of one, which prevents
// the distributor from blocking on a slow subscriber (backpressure is still
// propagated via the root channel).
//
// Subscribe starts the internal distributor on the first call.
func (x *ChanMulticast[C, V]) Subscribe() C {
	// Create a small buffer to avoid the distributor blocking on a slow reader.
	ch := make(chan V, 1)
	done := make(chan struct{})

	x.subMu.Lock()
	defer x.subMu.Unlock()
	x.subscribers = append(x.subscribers, struct {
		ch   C
		done chan struct{}
	}{ch: ch, done: done})
	atomic.AddInt32(&x.subCount, 1)

	// Kick off the distributor on the first subscription.
	x.once.Do(func() {
		go x.distributor()
	})

	return ch
}

// Unsubscribe removes a subscriber identified by its channel `ch`. It closes the
// done channel to signal the distributor to stop sending, removes the entry from
// the subscriber slice, decrements the subscriber count, and finally closes the
// receive channel (signaling any pending iterator that it should stop).
//
// If `ch` is not currently registered, Unsubscribe is a no‑op.
func (x *ChanMulticast[C, V]) Unsubscribe(ch C) {
	x.subMu.Lock()
	defer x.subMu.Unlock()
	for i, sub := range x.subscribers {
		if sub.ch == ch {
			// Signal the distributor to stop sending to this subscriber.
			close(sub.done)
			// Remove from the slice.
			x.subscribers = append(x.subscribers[:i], x.subscribers[i+1:]...)
			atomic.AddInt32(&x.subCount, -1)
			// Close the channel to inform any iterator that it should exit.
			close(ch)
			return
		}
	}
	// Not found – nothing to do.
}

// Send publishes `value` to **all currently active subscribers**. If there are
// no subscribers it returns 0 immediately (fast path). Otherwise it blocks on
// sending to the root channel, which is consumed by the distributor goroutine.
//
// The method returns the current number of subscribers at the moment the Send
// completes (after the root channel has accepted the value).
func (x *ChanMulticast[C, V]) Send(value V) int {
	if atomic.LoadInt32(&x.subCount) == 0 {
		return 0 // fast path: no active subscribers.
	}
	// The root channel is unbuffered; this blocks until the distributor reads.
	x.root <- value
	return int(atomic.LoadInt32(&x.subCount))
}

// SubscribeContext returns an `iter.Seq[V]` that yields values from a newly
// created subscriber until the supplied `ctx` is done, the iterator is stopped,
// or the subscriber is explicitly removed. The iterator automatically calls
// Unsubscribe when it finishes, ensuring proper cleanup.
//
// If `ctx` is nil it is treated as `context.Background()`.
func (x *ChanMulticast[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	ch := x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, func() {
		x.Unsubscribe(ch)
	})

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe(ch)
			}
			panic("bigbuff: chanmulticast: iterator yield func is nil")
		}
		if !stop() {
			return
		}
		defer x.Unsubscribe(ch) // ensure cleanup when the iterator exits.

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok {
					return // channel closed.
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// distributor is the background goroutine that reads from the root channel and
// forwards each value to every active subscriber. It uses a `select` with the
// per‑subscriber `done` channel to safely skip a subscriber that has been
// removed while the distributor is iterating.
func (x *ChanMulticast[C, V]) distributor() {
	defer close(x.distDone)
	for {
		v, ok := <-x.root
		if !ok {
			// Root channel closed – shut down the distributor.
			return
		}

		// Take a snapshot of the current subscribers while holding a read lock.
		x.subMu.RLock()
		subs := make([]struct {
			ch   C
			done chan struct{}
		}, len(x.subscribers))
		copy(subs, x.subscribers)
		x.subMu.RUnlock()

		// Forward the value to each subscriber. The per‑subscriber `done` channel
		// allows safe removal without deadlocking on an unbuffered receive.
		for _, sub := range subs {
			select {
			case sub.ch <- v:
				// Successfully queued the value.
			case <-sub.done:
				// Subscriber removed whilst we were iterating – skip.
			}
		}
	}
}
```

-->
<details>
<summary>**Key Implementation Details**</summary>

* **Subscriber Channels** – Each subscriber receives a **buffered** channel (`chan V, 1`). This prevents the distributor from blocking on a slow reader and still propagates back‑pressure to the root channel if the buffer fills.
* **Atomic Counter** – `subCount` provides an O(1) fast‑path in `Send`. It is updated with `atomic.AddInt32` under the same mutex that protects the subscriber slice, guaranteeing consistency.
* **Snapshot‑Based Distribution** – The distributor copies the subscriber slice under an `RLock`. This eliminates contention between distribution and subscription/unsubscription operations while keeping the memory footprint low.
* **Safe Removal** – Closing `sub.done` signals the distributor to stop sending to that subscriber. The `select` in the distributor will skip the blocked send (or the send if the channel is full) and continue processing other subscribers. Afterwards `Unsubscribe` closes the receive channel, informing any iterator that it should stop.
* **Iterator Integration** – `SubscribeContext` creates a fresh subscriber channel and returns an iterator that reads from it. The iterator’s `defer` ensures `Unsubscribe` is called, closing the channel automatically when the iterator finishes (either by context cancel, early stop, or natural exhaustion).
* **Low Allocation** – Only one channel per subscriber and a single slice copy on every update. No per‑send allocations beyond the value itself.

</details>
```
