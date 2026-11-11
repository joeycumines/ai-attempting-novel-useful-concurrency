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
//  7. Repair 2 held the read lock for the whole fan-out, and membership removal
//     needs the write lock. A subscriber that stopped receiving and then tried
//     to leave therefore waited for a Send that was waiting for it: neither
//     ever returned, and Close hung the same way
//     (TestChanMultiCast34Lightning_LeaveWhileSendIsBlockedOnYou). Send now
//     takes the two slice headers under the read lock and releases it before
//     the fan-out. That is sound because removal no longer shrinks the slices
//     in place: it copies the survivors into fresh arrays, so an element a
//     sender can see is never overwritten. Growth still appends in place (it
//     only writes past every snapshot's length), so the capacity hint keeps
//     its meaning and Subscribe below the hint still allocates only the
//     subscriber's two channels.
//  8. Unsubscribe could only remove the most recently added subscriber, so a
//     subscriber could not remove itself unless it happened to be the last
//     one: with A then B subscribed, A calling Unsubscribe retired B.
//     UnsubscribeChan removes the subscriber that owns a given channel.
//     Unsubscribe keeps its delivered meaning (remove the newest).

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
	//
	// Senders read the slice headers under the read lock and the elements
	// after releasing it, so an element below the current length is never
	// written again in the same backing array: growth appends past the
	// length, removal copies into new arrays (see keepLocked).
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
// there are no subscribers.  The subscriber's channel is not closed: it simply
// stops receiving values, and a Send blocked on it is released.
//
// A subscriber that wants to remove itself should use UnsubscribeChan, since
// it is the newest subscriber only if nobody subscribed after it.
func (x *ChanMultiCast34Lightning[V]) Unsubscribe() {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := len(x.receivers)
	if n == 0 {
		panic("bigbuff: ChanMultiCast34Lightning: no subscribers to unsubscribe")
	}
	// The subscriber channel is never closed: an in-flight Send may hold the
	// channel from its snapshot and be blocked in a send to it, so closing it
	// here would panic. Retirement is signalled with the paired retiring
	// channel instead, which Send also selects on.
	close(x.retiring[n-1])
	x.keepLocked(n-1, n)
}

// UnsubscribeChan removes the subscriber whose channel is ch, wherever it is
// in the subscriber list, and reports whether it was subscribed. It is safe to
// call while a Send is blocked on ch: that Send skips the subscriber and
// carries on. Calling it again, or after Close, returns false.
func (x *ChanMultiCast34Lightning[V]) UnsubscribeChan(ch <-chan V) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	for i := range x.receivers {
		if (<-chan V)(x.receivers[i]) == ch {
			close(x.retiring[i])
			x.keepLocked(i, i+1)
			return true
		}
	}
	return false
}

// keepLocked drops subscribers [from, to) by copying the others into fresh
// arrays of the same capacity. The arrays being replaced are left untouched
// for any Send that is still walking them. The caller holds the write lock and
// has already closed the retirement channels of the dropped subscribers.
func (x *ChanMultiCast34Lightning[V]) keepLocked(from, to int) {
	receivers := make([]chan V, 0, cap(x.receivers))
	receivers = append(receivers, x.receivers[:from]...)
	receivers = append(receivers, x.receivers[to:]...)
	retiring := make([]chan struct{}, 0, cap(x.retiring))
	retiring = append(retiring, x.retiring[:from]...)
	retiring = append(retiring, x.retiring[to:]...)
	x.receivers = receivers
	x.retiring = retiring
	x.subscribers.Add(int32(from - to))
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
	x.keepLocked(start, current)
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
// Returns the number of receivers that received the value. A subscriber that
// leaves while the Send is in flight is skipped and not counted, unless it
// had already taken the value.
// ----------------------------------------------------------------------

// Send delivers *value* to every currently active subscriber.
func (x *ChanMultiCast34Lightning[V]) Send(value V) (sent int) {
	// Fast path – nobody is listening.
	if x.subscribers.Load() == 0 {
		return 0
	}

	// Take a read‑only snapshot: the two slice headers, read together under
	// the read lock. The lock is released before the fan-out, because a
	// subscriber must be able to leave (which needs the write lock) while a
	// sender is blocked on it. The snapshot stays valid afterwards because
	// its elements are never overwritten (see the receivers field), and each
	// receiver has a retirement signal, so a subscriber that departs while a
	// sender is blocked on it releases that sender.
	x.mu.RLock()
	receivers, retiring := x.receivers, x.retiring
	x.mu.RUnlock()
	for i := range receivers {
		select {
		case receivers[i] <- value:
			sent++
		case <-retiring[i]:
			// Retired after the snapshot; skip it.
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
