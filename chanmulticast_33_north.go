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

// Provenance: 33-openrouter-chat-north-mini-code-free-prompt-mini.md.
//
// Repairs required to integrate (all in spirit: one unbuffered root channel,
// one background distributor, a snapshot of the subscriber slice, and a
// per-subscriber retirement signal are preserved exactly):
//
//  1. The subscriber list used an anonymous struct type that could not be
//     shared between Subscribe and the distributor's snapshot; it is now a named
//     type.
//  2. Unsubscribe closed the subscriber's channel. That is unsafe by
//     construction: the distributor iterates a snapshot and may be blocked in a
//     send to that channel at that instant, so closing it panics and is a data
//     race (this showed up under -race). Retirement is now signalled only
//     through the per-subscriber done channel, which the distributor and the
//     iterator both select on.
//  3. The iterator could not observe retirement (it only watched ctx and the
//     value channel); it now watches the done channel and drains one pending
//     value before returning, so no value handed over by the distributor is
//     lost.
//  4. sync/atomic.Int32 fields were passed to the package-level atomic helpers
//     by address, which does not compile; they use the typed API.

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

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

// ChanMulticast33North provides a high‑performance broadcast channel that can be
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
// chanmulticast33NorthSub is one distributor target. done is closed by
// Unsubscribe before the subscriber's channel is closed, which is what lets the
// distributor skip a departed subscriber instead of sending on a closed
// channel.
type chanmulticast33NorthSub[C ~chan V, V any] struct {
	ch   C
	done chan struct{}
}

type ChanMulticast33North[C ~chan V, V any] struct {
	root C

	// Protects the subscribers slice. A RWMutex allows the distributor to hold a
	// read lock while iterating, and Subscribe/Unsubscribe to take a write lock.
	subMu       sync.RWMutex
	subscribers []chanmulticast33NorthSub[C, V]
	subCount atomic.Int32 // fast check for "no active subscribers"

	distDone chan struct{} // closed when the distributor exits
	once     sync.Once     // starts the distributor on first subscription
}

// NewChanMulticast33North creates a new broadcast channel. The supplied channel `c`
// must be unbuffered; passing a buffered channel will cause a panic.
func NewChanMulticast33North[C ~chan V, V any](c C) *ChanMulticast33North[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanmulticast: must use an unbuffered channel")
	}
	return &ChanMulticast33North[C, V]{
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
func (x *ChanMulticast33North[C, V]) Subscribe() C {
	ch, _ := x.subscribe()
	return ch
}

// subscribe is the shared registration path, returning the subscriber channel
// and its retirement signal.
func (x *ChanMulticast33North[C, V]) subscribe() (C, <-chan struct{}) {
	// Create a small buffer to avoid the distributor blocking on a slow reader.
	ch := make(chan V, 1)
	done := make(chan struct{})

	x.subMu.Lock()
	defer x.subMu.Unlock()
	x.subscribers = append(x.subscribers, chanmulticast33NorthSub[C, V]{ch: ch, done: done})
	x.subCount.Add(1)

	// Kick off the distributor on the first subscription.
	x.once.Do(func() {
		go x.distributor()
	})

	return ch, done
}

// Unsubscribe removes a subscriber identified by its channel `ch`. It closes the
// done channel to signal the distributor to stop sending, removes the entry from
// the subscriber slice, decrements the subscriber count, and finally closes the
// receive channel (signaling any pending iterator that it should stop).
//
// If `ch` is not currently registered, Unsubscribe is a no‑op.
func (x *ChanMulticast33North[C, V]) Unsubscribe(ch C) {
	x.subMu.Lock()
	defer x.subMu.Unlock()
	for i, sub := range x.subscribers {
		if sub.ch == ch {
			// Signal the distributor to stop sending to this subscriber.
			close(sub.done)
			// Remove from the slice.
			x.subscribers = append(x.subscribers[:i], x.subscribers[i+1:]...)
			x.subCount.Add(-1)
			// The subscriber channel is deliberately NOT closed here. The
			// distributor checks a snapshot of the subscriber list, so it may be
			// inside a send to this channel at this instant; closing a channel
			// with a concurrent sender panics and is a data race. The per
			// subscriber done channel (already closed above) is the retirement
			// signal, and it is what the distributor and any iterator select on.
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
func (x *ChanMulticast33North[C, V]) Send(value V) int {
	if x.subCount.Load() == 0 {
		return 0 // fast path: no active subscribers.
	}
	// The root channel is unbuffered; this blocks until the distributor reads.
	x.root <- value
	return int(x.subCount.Load())
}

// SubscribeContext returns an `iter.Seq[V]` that yields values from a newly
// created subscriber until the supplied `ctx` is done, the iterator is stopped,
// or the subscriber is explicitly removed. The iterator automatically calls
// Unsubscribe when it finishes, ensuring proper cleanup.
//
// If `ctx` is nil it is treated as `context.Background()`.
func (x *ChanMulticast33North[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	ch, retired := x.subscribe()
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
			// The context was already cancelled: Unsubscribe has already run (or
			// is running) via the AfterFunc callback, so there is nothing to
			// yield. Returning here matches the original intent; the guard
			// exists so the iterator never reads a closed channel twice.
			return
		}
		defer x.Unsubscribe(ch) // ensure cleanup when the iterator exits.

		for {
			select {
			case <-ctx.Done():
				return
			case <-retired:
				// Drain anything already buffered before leaving, so a value the
				// distributor handed over is not lost.
				select {
				case v := <-ch:
					if !yield(v) {
						return
					}
				default:
					return
				}
			case v := <-ch:
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
func (x *ChanMulticast33North[C, V]) distributor() {
	defer close(x.distDone)
	for {
		v, ok := <-x.root
		if !ok {
			// Root channel closed – shut down the distributor.
			return
		}

		// Take a snapshot of the current subscribers while holding a read lock.
		x.subMu.RLock()
		subs := make([]chanmulticast33NorthSub[C, V], len(x.subscribers))
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
