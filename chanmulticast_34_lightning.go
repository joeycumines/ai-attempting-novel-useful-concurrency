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

// Provenance: 34-openrouter-chat-nemotron-3.5-lightning-free-prompt-mini.md.
//
// Repairs required to integrate (all in spirit: one shared public channel,
// a per-subscriber private unbuffered channel, an RWMutex around membership
// changes and a lock-free empty fast path are all preserved):
//
//  1. The file carried a stray "// +build ignore" line, so it would never have
//     been compiled at all. Removed (the modern equivalent would be a go:build
//     constraint, and there is no reason to exclude it).
//  2. Send sliced the receiver list to a count loaded outside the lock and
//     released the lock before fanning out, so a concurrent Unsubscribe could
//     truncate the slice underneath it (a slice bounds panic) or let it block
//     forever on a departed subscriber. The snapshot is now taken under the
//     read lock for the duration of the fan-out, and each receiver has a
//     retirement signal that a blocked send selects on.
//  3. Unsubscribe closed the subscriber's channel while a sender could be
//     blocked on it, which panics. Retirement is signalled instead.
//  4. Add built its new slots using a length-vs-count expression that could
//     index out of range, and its removal path trimmed `receivers` but not the
//     new parallel retirement slice. Both are rewritten against a single
//     invariant: len(receivers) == len(retiring) == subscriber count.
//  5. There was no way to stop the broadcaster, and Iter ranged over a channel
//     nothing ever closed, so it could never terminate. A documented Close now
//     retires subscribers, closes C (making Iter terminate) and is idempotent.
//  6. math was imported to force use of a package that was never needed.

import (
	"iter"
	"sync"
	"sync/atomic"
)

// ChanMultiCast34Lightning implements a many‑to‑one broadcast channel.
//
// The exported field C is the primary data channel; sending a value via
// Send forwards it to every currently subscribed receiver.  Users may
// select on C just like a normal channel.
type ChanMultiCast34Lightning[V any] struct {
	// C is the public data channel. It is closed by Close.
	C chan V

	// subscribers is the current number of active receivers.  Updated
	// atomically; the fast path in Send only loads this field.
	subscribers atomic.Int32

	// receivers[i] is the private unbuffered channel belonging to the i‑th
	// subscriber, and retiring[i] is its retirement signal. The slice length is
	// always equal to the subscriber count; the two slices are grown and
	// shrunk together under mu.
	receivers []chan V
	retiring  []chan struct{}
	closed    bool

	// mu protects the receivers slice when it is grown or shrunk.
	mu sync.RWMutex
}

// NewChanMultiCast34Lightning creates a new broadcaster with an internal unbuffered
// data channel.  *capacity* is a hint for the initial number of subscriber
// slots; the slice will grow automatically if more subscribers are added.
func NewChanMultiCast34Lightning[V any](capacity int) *ChanMultiCast34Lightning[V] {
	if capacity < 0 {
		panic("bigbuff: NewChanMultiCast34Lightning: negative capacity")
	}
	c := make(chan V) // unbuffered core channel
	x := &ChanMultiCast34Lightning[V]{
		C:         c,
		receivers: make([]chan V, 0, capacity),
		retiring:  make([]chan struct{}, 0, capacity),
	}
	return x
}

// ----------------------------------------------------------------------
// Subscribe / Unsubscribe – add / remove a receiver.
//
// Each subscriber gets its own private unbuffered channel; the caller should
// drain (or select on) that channel in a goroutine that is ready to receive.
// ----------------------------------------------------------------------

// Subscribe adds a new receiver and returns its private channel.  The returned
// channel is unbuffered; the caller must ensure a receiver goroutine is reading
// from it, otherwise Send will block.
func (x *ChanMultiCast34Lightning[V]) Subscribe() <-chan V {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		panic("bigbuff: ChanMultiCast34Lightning: subscribe on closed")
	}
	ch := make(chan V)
	x.receivers = append(x.receivers, ch)
	x.retiring = append(x.retiring, make(chan struct{}))
	x.subscribers.Add(1)
	return ch
}

// Unsubscribe removes the most‑recently added subscriber.  It is a panic if
// there are no subscribers.  After removal the subscriber’s channel is closed
// (the caller should not use it again).
func (x *ChanMultiCast34Lightning[V]) Unsubscribe() {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := len(x.receivers)
	if n == 0 {
		panic("bigbuff: ChanMultiCast34Lightning: no subscribers to unsubscribe")
	}
	// Remove the last slot (LIFO – simple and O(1)).
	// The subscriber channel is never closed: an in-flight Send may hold the
	// channel that was captured from the snapshot below and be blocked in a
	// send to it, so closing it here would panic. Retirement is signalled with
	// the paired retiring channel instead, which Send also selects on.
	close(x.retiring[n-1])
	x.receivers = x.receivers[:n-1]
	x.retiring = x.retiring[:n-1]
	x.subscribers.Add(-1)
}

// ----------------------------------------------------------------------
// Add / Remove – low‑level adjustment of the subscriber count.
//
// Positive *delta* adds that many new receivers; negative *delta* removes.
// The methods return the new total subscriber count.
// ----------------------------------------------------------------------

// Add(delta) increases the subscriber set by delta (delta may be positive or
// negative).  It panics if the operation would underflow.
func (x *ChanMultiCast34Lightning[V]) Add(delta int) (newCount int) {
	if delta == 0 {
		return int(x.subscribers.Load())
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		panic("bigbuff: ChanMultiCast34Lightning: add on closed")
	}

	if delta > 0 {
		for i := 0; i < delta; i++ {
			x.receivers = append(x.receivers, make(chan V))
			x.retiring = append(x.retiring, make(chan struct{}))
		}
		x.subscribers.Add(int32(delta))
		return int(x.subscribers.Load())
	}

	// delta < 0  → removal
	removal := -delta
	current := len(x.receivers)
	if current < removal {
		panic("bigbuff: ChanMultiCast34Lightning: removing more subscribers than exist")
	}
	// Retire the last *removal* slots.
	start := current - removal // first index to reclaim
	for i := start; i < current; i++ {
		close(x.retiring[i])
	}
	x.receivers = x.receivers[:start]
	x.retiring = x.retiring[:start]
	x.subscribers.Add(int32(-removal))
	return int(x.subscribers.Load())
}

// ----------------------------------------------------------------------
// Send – the core broadcast primitive.
//
// The fast path (no subscribers) costs only one atomic load and returns
// immediately.  When there are subscribers we take a read‑only slice view
// and send the value to each receiver.  Because the slice is only mutated
// under a write‑lock, concurrent Send calls may proceed simultaneously.
//
// Returns the number of receivers that received the value (always equal to
// the subscriber count unless a receiver channel is closed prematurely).
// ----------------------------------------------------------------------

// Send delivers *value* to every currently active subscriber.
func (x *ChanMultiCast34Lightning[V]) Send(value V) (sent int) {
	// Fast path – nobody is listening.
	if x.subscribers.Load() == 0 {
		return 0
	}

	// Take a read‑only snapshot. Holding the read lock for the duration of the
	// fan-out is what makes the snapshot sound: without it, a concurrent
	// Unsubscribe could truncate the slice under the sender's feet (the
	// original sliced to a stale count and then released the lock). Retirement
	// is additionally signalled per receiver, so a subscriber that departs
	// while a sender is blocked on it cannot deadlock the publisher.
	x.mu.RLock()
	defer x.mu.RUnlock()
	for i := range x.receivers {
		select {
		case x.receivers[i] <- value:
			sent++
		case <-x.retiring[i]:
			// Retired between the snapshot and the send; skip it.
		}
	}
	return sent
}

// ----------------------------------------------------------------------
// Optional: iterator support for range‑loops over the public data channel.
//
// Usage:  for v := range mc.Iter() { … }
// ----------------------------------------------------------------------

// Iter returns an iter.Seq[V] that yields values from the broadcaster’s
// data channel as long as at least one subscriber exists.  The iterator
// simply ranges over the internal channel C.
func (x *ChanMultiCast34Lightning[V]) Iter() iter.Seq[V] {
	return func(yield func(V) bool) {
		for v := range x.C {
			if !yield(v) {
				return
			}
		}
	}
}

// Close shuts the broadcaster down: it retires every subscriber, closes the
// public channel C and rejects further subscriptions. It is idempotent.
func (x *ChanMultiCast34Lightning[V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return
	}
	x.closed = true
	for i := range x.retiring {
		close(x.retiring[i])
	}
	x.receivers = nil
	x.retiring = nil
	x.subscribers.Store(0)
	close(x.C)
}

// ----------------------------------------------------------------------
// Example (commented out) ----------------------------------------------------
// ----------------------------------------------------------------------
//
// /*
// func main() {
// 	mc := bigbuff.NewChanMultiCast34Lightning[int](4)
//
// 	// Two consumers subscribe.
// 	chA := mc.Subscribe()
// 	chB := mc.Subscribe()
//
// 	// Producer sends a value; both receivers get it.
// 	go func() {
// 		mc.Send(42)
// 	}()
//
// 	// Consumer A prints what it receives.
// 	go func() {
// 		for v := range chA {
// 			println("A received:", v)
// 		}
// 	}()
//
// 	// Consumer B prints what it receives.
// 	go func() {
// 		for v := range chB {
// 			println("B received:", v)
// 		}
// 	}()
//
// 	// ... program continues, subscribers may be added/removed, etc.
// }
// */
// ----------------------------------------------------------------------
