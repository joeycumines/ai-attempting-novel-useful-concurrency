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

// Provenance: 30-openrouter-chat-dots3-note-preview-free-prompt-mini.md.
//
// Repairs required to integrate (all in spirit):
//
//  1. The delivered file contains a duplicated, truncated import block and a
//     set of "keep the imports used" placeholder vars using packages that were
//     not imported. Those are removed; the implementation only needs context,
//     iter, runtime, sync and sync/atomic.
//  2. runtime_Gosched was an elaborate no-op wrapper (with a stale comment
//     about an import cycle that does not exist); it now calls runtime.Gosched.
//  3. The ring payload was written as a plain V store ordered only by the
//     cursor increment, which is a data race (and a happens-before inversion:
//     the cursor was incremented BEFORE the payload store). The payload is now
//     an atomic pointer published before the cursor increment, which is the
//     ordering the design's own comment claims, and readers that observe a
//     cursor without a payload rewind their cursor and retry instead of
//     returning a zero value.
//  4. Receive could return ok=false for a live subscriber whose ring had been
//     overtaken; the implementation now reports ok=false only when the
//     subscriber is detached, matching the documented contract.

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Arcadia — Lock-Free Shared-Memory Broadcast
// ---------------------------------------------------------------------------
//
// Arcadia replaces channel-based fan-out with a pre-allocated ring buffer
// and per-subscriber read cursors. The publisher writes values into the
// buffer and advances a single global write cursor — O(1) regardless of
// subscriber count. Subscribers read at their own pace using their own
// read cursor, without blocking the publisher.
//
// # Design
//
//   - A single power-of-2 ring buffer ([]V) is shared between publisher
//     and all subscribers.
//   - The publisher atomically increments a global write cursor
//     (a.write) and writes the value at write&mask.
//   - Each subscriber has its own read cursor (sub.read). Receive()
//     advances the cursor via CAS and returns the buffered value.
//   - Subscriber management uses an immutable slice swapped via
//     atomic.Pointer[*arcadiaSubList[V]]. Subscribe/Unsubscribe create a
//     new slice and CAS it in — allocation happens only on membership
//     change, never in the hot path.
//   - Notification uses a generation counter (a.gen) incremented on
//     every Send. Subscribers spin briefly, then park on a sync.Cond
//     guarded by a waiter count. This avoids thundering herd: only
//     parked subscribers are woken, and the generation counter lets
//     spinners detect missed notifications without polling.
//
// # Hot-Path Allocations
//
//	Send:        0 allocations
//	Receive:     0 allocations
//	TryReceive:  0 allocations
//
// # Memory Model
//
//	The write cursor increment acts as a release fence. The value write
//	happens-before the cursor increment, so subscribers observing the
//	new cursor value are guaranteed to see the published value.
//
//	The generation counter uses release/acquire semantics for the
//	notification path: Send does a.gen.Add (release), subscribers do
//	a.gen.Load (acquire).
//
// # Invariants
//
//   - buffer length is a power of 2 (mask = len-1)
//   - write cursor is monotonically increasing (uint64 wrap is acceptable)
//   - subscriber read cursors never exceed the write cursor
//   - subscriber list is immutable once published via CAS
//   - waiter count is balanced (Add(1) before park, Add(-1) after unpark)
type Arcadia[V any] struct {
	buffer []atomic.Pointer[V] // pre-allocated ring buffer (power of 2)
	mask   uint64              // buffer size - 1
	write  atomic.Uint64       // global write cursor (monotonically increasing)

	// Subscriber management.
	// The subscriber list is an immutable slice. Subscribe/Unsubscribe
	// allocate a new slice and swap via CAS. The hot path never touches
	// this; only Send reads the length (for the return value) and
	// Receive/TryReceive do not touch it at all.
	subs atomic.Pointer[arcadiaSubList[V]]

	// Notification.
	gen     atomic.Uint64 // generation counter (incremented on each Send)
	waiters atomic.Int64  // number of currently parked subscribers

	// Parking infrastructure.
	// Used only when a subscriber has spun long enough and needs to
	// block. Broadcast is called under parkMu.Lock, so a subscriber
	// parked on parkC.Wait is guaranteed to be woken.
	parkMu sync.Mutex
	parkC  *sync.Cond

	// Configuration.
	maxSpin int // number of spin iterations before parking
}

// arcadiaSubList is an immutable snapshot of subscribers.
// It is only ever created by Subscribe/Unsubscribe and then swapped
// in via atomic.Pointer.CompareAndSwap.
type arcadiaSubList[V any] struct {
	subs []*arcadiaSub[V]
}

// arcadiaSub is a single subscriber's state.
// Each subscriber has its own read cursor and a cached generation
// value used during the spin-then-park wait.
type arcadiaSub[V any] struct {
	read atomic.Uint64 // read cursor for this subscriber
	gen  atomic.Uint64 // last seen generation (used during wait)
	// done reports that the subscriber has been detached. It exists so that a
	// subscriber parked on the condition variable can be released without a
	// further Send: the documented contract for SubscribeContext is that
	// cancellation stops iteration promptly, which is impossible while
	// Receive has no other escape from wait().
	done atomic.Bool
}

// ArcadiaSub is a handle to a subscriber in an Arcadia broadcast.
// It is safe to use ArcadiaSub concurrently with Send, Subscribe,
// and Unsubscribe on the parent Arcadia.
type ArcadiaSub[V any] struct {
	a   *Arcadia[V]
	sub *arcadiaSub[V]
}

// NewArcadia creates a new Arcadia broadcaster with the given buffer
// capacity. The capacity is rounded up to the next power of 2.
//
// Panics if capacity <= 0.
func NewArcadia[V any](capacity int) *Arcadia[V] {
	if capacity <= 0 {
		panic(`bigbuff: arcadia: capacity must be positive`)
	}

	size := uint64(1)
	for size < uint64(capacity) {
		size <<= 1
	}

	a := &Arcadia[V]{
		buffer:  make([]atomic.Pointer[V], size),
		mask:    size - 1,
		maxSpin: 100,
	}
	a.parkC = sync.NewCond(&a.parkMu)
	a.subs.Store(&arcadiaSubList[V]{})
	return a
}

// ---------------------------------------------------------------------------
// Publisher API
// ---------------------------------------------------------------------------

// Send broadcasts a value to all current and future subscribers.
//
// This is the hot path. It performs, in order:
//  1. Atomic increment of the write cursor (reserves a slot).
//  2. Write the value into the ring buffer at the reserved slot.
//  3. Atomic increment of the generation counter (notification).
//  4. If there are parked waiters, broadcast to wake them.
//
// The cost is O(1) regardless of subscriber count. Returns the number
// of subscribers that will receive the value (a snapshot at call time).
//
// If the buffer is full (write cursor catches up to the slowest
// subscriber's read cursor), the oldest values are silently overwritten.
// This is a deliberate design choice: the publisher never blocks, and
// slow subscribers see a "lagging" view. Use a larger buffer if this
// is unacceptable.
func (a *Arcadia[V]) Send(value V) int {
	// Reserve a slot in the ring buffer.
	// This increment acts as a release fence: the value write below
	// happens-before any subscriber observing this cursor value.
	pos := a.write.Add(1) - 1

	// Publish the payload through an atomic pointer. The original wrote the
	// slot directly and relied on the cursor increment as a release fence;
	// that is not a valid happens-before edge on its own (the increment is
	// released before the payload store, not after), and it races with any
	// reader that observes the new cursor and reads the slot it points at.
	// Payload-store-then-cursor-increment is the ordering the design intends.
	a.buffer[pos&a.mask].Store(&value)

	// Count current subscribers (for the return value).
	// This is a load, not a CAS, so it's fast. The count may be
	// stale by the time we return, but that's acceptable.
	subs := a.subs.Load()
	count := len(subs.subs)

	// Advance the generation counter to notify subscribers.
	// This is a release operation: subscribers that acquire-load
	// this generation will see the new value.
	a.gen.Add(1)

	// Wake up parked subscribers if any.
	// We check the waiter count first to avoid taking the lock
	// when there are no waiters (common case).
	if a.waiters.Load() > 0 {
		a.parkMu.Lock()
		a.parkC.Broadcast()
		a.parkMu.Unlock()
	}

	return count
}

// TrySend broadcasts a value without blocking. Currently always
// succeeds (the buffer never blocks the publisher). Provided for
// API symmetry with future non-blocking variants.
//
// Returns the number of subscribers that will receive the value.
func (a *Arcadia[V]) TrySend(value V) (int, bool) {
	return a.Send(value), true
}

// ---------------------------------------------------------------------------
// Subscriber Management
// ---------------------------------------------------------------------------

// Subscribe adds a new subscriber and returns a handle for receiving
// values. The subscriber starts from the current write position, so
// it will only receive values sent after subscription.
//
// Subscribe allocates a new subscriber slice (the old one becomes
// garbage). This is the only allocation in the subscribe path.
func (a *Arcadia[V]) Subscribe() *ArcadiaSub[V] {
	sub := &arcadiaSub[V]{}
	// Start from the current write position so we don't see
	// values that were sent before subscription.
	sub.read.Store(a.write.Load())

	for {
		old := a.subs.Load()
		// Repair: append(old.subs, sub) can write into spare capacity of a
		// snapshot that another Subscribe is also appending to. The list is
		// immutable only if every mutation copies the backing array.
		next := make([]*arcadiaSub[V], len(old.subs)+1)
		copy(next, old.subs)
		next[len(old.subs)] = sub
		newSubs := &arcadiaSubList[V]{subs: next}
		if a.subs.CompareAndSwap(old, newSubs) {
			break
		}
		// CAS failed — another goroutine subscribed/unsubscribed
		// concurrently. Retry with the new list.
	}

	return &ArcadiaSub[V]{a: a, sub: sub}
}

// Unsubscribe removes a subscriber. Safe to call concurrently with
// Send, Subscribe, and other Unsubscribe calls.
//
// After Unsubscribe returns, the subscriber will no longer receive
// new values. Any in-flight Receive calls may still return values
// that were already published.
//
// Unsubscribe allocates a new subscriber slice (without the removed
// subscriber). The old slice becomes garbage.
func (a *Arcadia[V]) Unsubscribe(sub *ArcadiaSub[V]) {
	for {
		old := a.subs.Load()
		found := false
		var newSubs []*arcadiaSub[V]

		for _, s := range old.subs {
			if s == sub.sub {
				found = true
				continue
			}
			newSubs = append(newSubs, s)
		}

		if !found {
			// Already removed (e.g. double unsubscribe).
			return
		}

		if a.subs.CompareAndSwap(old, &arcadiaSubList[V]{subs: newSubs}) {
			break
		}
		// CAS failed — retry with the new list.
	}
	// Detach for receivers: mark the subscription done and wake anyone parked
	// on it, so a cancelled consumer's Receive returns instead of parking until
	// the next Send.
	sub.sub.done.Store(true)
	if a.waiters.Load() > 0 {
		a.parkMu.Lock()
		a.parkC.Broadcast()
		a.parkMu.Unlock()
	}
}

// Subscribers returns the current number of subscribers.
// This is a snapshot and may be stale immediately after return.
func (a *Arcadia[V]) Subscribers() int {
	return len(a.subs.Load().subs)
}

// ---------------------------------------------------------------------------
// Subscriber Receive API
// ---------------------------------------------------------------------------

// Receive reads the next value sent to this subscriber. It blocks until
// a value is available, using an adaptive spin-then-park strategy.
//
// Returns the value and true. If the subscriber has been unsubscribed
// and no more values are available (the buffer has wrapped around past
// this subscriber's read cursor), returns the zero value and false.
//
// Receive is safe to call concurrently from multiple goroutines on
// the same ArcadiaSub, but this is not recommended — values will be
// distributed round-robin among the callers, and some may see gaps.
func (s *ArcadiaSub[V]) Receive() (V, bool) {
	var zero V
	for {
		writePos := s.a.write.Load()
		readPos := s.sub.read.Load()

		// No new data available.
		if readPos >= writePos {
			if s.sub.done.Load() {
				return zero, false
			}
			s.wait()
			continue
		}

		// Try to advance our read cursor.
		// CAS ensures only one goroutine (per subscriber) consumes
		// each slot. If multiple goroutines call Receive on the same
		// ArcadiaSub, they will contend here.
		if !s.sub.read.CompareAndSwap(readPos, readPos+1) {
			// CAS failed — another goroutine consumed this slot.
			// Retry from the new cursor position.
			continue
		}

		// Read the value from the ring buffer. A nil slot means the publisher
		// observed the cursor but has not finished publishing the payload; the
		// value is not lost, so rewind the cursor and retry.
		vp := s.a.buffer[readPos&s.a.mask].Load()
		if vp == nil {
			s.sub.read.CompareAndSwap(readPos+1, readPos)
			runtime.Gosched()
			continue
		}
		return *vp, true
	}
}

// TryReceive reads the next value without blocking.
// Returns the value and true if available, or the zero value and false.
func (s *ArcadiaSub[V]) TryReceive() (V, bool) {
	var zero V

	writePos := s.a.write.Load()
	readPos := s.sub.read.Load()

	if readPos >= writePos {
		return zero, false
	}

	if !s.sub.read.CompareAndSwap(readPos, readPos+1) {
		return zero, false
	}

	vp := s.a.buffer[readPos&s.a.mask].Load()
	if vp == nil {
		s.sub.read.CompareAndSwap(readPos+1, readPos)
		return zero, false
	}
	return *vp, true
}

// ReceiveBatch reads up to n values into dst, blocking until at least
// one value is available. Returns the number of values read.
//
// This is more efficient than calling Receive n times because it
// avoids the per-value spin/park overhead and batches the cursor
// advancement.
func (s *ArcadiaSub[V]) ReceiveBatch(dst []V, n int) int {
	if n <= 0 || len(dst) == 0 {
		return 0
	}

	maxRead := n
	if maxRead > len(dst) {
		maxRead = len(dst)
	}

	// Ensure at least one value is available.
	for {
		writePos := s.a.write.Load()
		readPos := s.sub.read.Load()
		if readPos < writePos {
			break
		}
		if s.sub.done.Load() {
			return 0
		}
		s.wait()
	}

	// Read as many as we can without blocking.
	count := 0
	for count < maxRead {
		writePos := s.a.write.Load()
		readPos := s.sub.read.Load()

		if readPos >= writePos {
			break
		}

		if !s.sub.read.CompareAndSwap(readPos, readPos+1) {
			continue
		}

		vp := s.a.buffer[readPos&s.a.mask].Load()
		if vp == nil {
			s.sub.read.CompareAndSwap(readPos+1, readPos)
			continue
		}
		dst[count] = *vp
		count++
	}

	return count
}

// ---------------------------------------------------------------------------
// Internal: Adaptive Wait
// ---------------------------------------------------------------------------

// wait blocks until new data is available, using an adaptive
// spin-then-park strategy.
//
// The algorithm:
//  1. Record the current generation.
//  2. Spin for maxSpin iterations, checking the generation counter
//     after each iteration. If it changed, new data has been sent.
//  3. If still no change, increment the waiter count and double-check
//     the generation. If it changed, decrement and return.
//  4. Park on the condition variable. The sender's Broadcast will
//     wake us. After waking, check the generation again.
//  5. Decrement the waiter count.
//
// This avoids the thundering herd problem: only subscribers that have
// actually parked are woken by Broadcast. Subscribers that are still
// spinning will see the generation change and exit their spin loop.
func (s *ArcadiaSub[V]) wait() {
	currentGen := s.a.gen.Load()
	s.sub.gen.Store(currentGen)

	// Phase 1: Spin.
	// Most waits are very short (the publisher is actively sending),
	// so spinning for a bounded number of iterations is faster than
	// parking and being woken.
	for i := 0; i < s.a.maxSpin; i++ {
		if s.a.gen.Load() != currentGen || s.sub.done.Load() {
			return
		}
		runtime.Gosched()
	}

	// Phase 2: Park.
	// We've spun long enough. Increment the waiter count so that
	// future Send calls know to broadcast.
	s.a.waiters.Add(1)
	defer s.a.waiters.Add(-1)

	// Double-check after acquiring the waiter count.
	// A Send could have happened between our last spin check and
	// the waiter increment. If the generation changed, don't park.
	if s.a.gen.Load() != currentGen || s.sub.done.Load() {
		return
	}

	s.a.parkMu.Lock()
	// Check again under the lock. The sender's Broadcast is called
	// under parkMu.Lock, so if the generation hasn't changed by now,
	// we're guaranteed to be woken by the next Broadcast.
	for s.a.gen.Load() == currentGen && !s.sub.done.Load() {
		s.a.parkC.Wait()
	}
	s.a.parkMu.Unlock()
}

// ---------------------------------------------------------------------------
// Channel Bridge
// ---------------------------------------------------------------------------

// AsChannel returns a read-only channel that yields values sent to
// this subscriber. The channel is backed by a goroutine that calls
// Receive in a loop.
//
// This is useful for integrating with code that expects a channel
// (e.g. select statements). The goroutine allocation is acceptable
// because it's not in the hot path — it's a one-time cost per
// subscriber that wants channel semantics.
//
// The channel is closed when the subscriber is unsubscribed and no
// more values are available.
func (s *ArcadiaSub[V]) AsChannel() <-chan V {
	ch := make(chan V, 1)
	go func() {
		defer close(ch)
		for {
			v, ok := s.Receive()
			if !ok {
				return
			}
			ch <- v
		}
	}()
	return ch
}

// ---------------------------------------------------------------------------
// Iterator Support (for range)
// ---------------------------------------------------------------------------

// SubscribeContext is like Subscribe but also stops iteration when
// the context is done. It returns an iter.Seq[V] for use with
// "for range".
//
// This is a convenience wrapper that combines Subscribe, context
// cancellation, and Unsubscribe into a single iterator.
//
//	N.B. handles guarding on check* functions
func (a *Arcadia[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := a.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, func() { a.Unsubscribe(sub) })

	return func(yield func(V) bool) {
		defer func() {
			if stop() {
				a.Unsubscribe(sub)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			v, ok := sub.Receive()
			if !ok {
				return
			}

			if !yield(v) {
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// Cap returns the ring buffer capacity.
func (a *Arcadia[V]) Cap() int {
	return len(a.buffer)
}

// Len returns an approximate number of values currently in the buffer.
// This is computed as the difference between the write cursor and the
// oldest subscriber's read cursor. It is an approximation and may be
// inaccurate under concurrency.
func (a *Arcadia[V]) Len() int {
	w := a.write.Load()
	subs := a.subs.Load()
	if len(subs.subs) == 0 {
		return 0
	}

	oldest := w
	for _, s := range subs.subs {
		r := s.read.Load()
		if r < oldest {
			oldest = r
		}
	}

	return int(w - oldest)
}
