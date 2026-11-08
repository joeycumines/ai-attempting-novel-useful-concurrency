// Provenance: 47-arena-gemini-3.8-flash.md (Gemini, response #47). Namespaced to preserve the existing implementations.
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
	"math/bits"
	"sync"
	"sync/atomic"
)

// closedRingNotify47Gemini is a pre-closed singleton channel returned by
// [RingReader47Gemini.Ready] whenever one or more unread messages are already buffered
// in the ring or the ring has been closed, allowing select statements to fire
// immediately with zero allocations and zero scheduler parking.
var closedRingNotify47Gemini = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type ringNotifyNode47Gemini struct {
	seq uint64
	ch  chan struct{}
}

type ringSlot47Gemini[V any] struct {
	// seq stores the 1-based sequence number currently occupying this slot.
	// Written with Release semantics after value and notify are prepared;
	// read with Acquire semantics by readers.
	seq atomic.Uint64

	// readers tracks how many active subscribers have not yet consumed this slot.
	// When it transitions 1 -> 0, any publisher waiting on drained is woken.
	// The payload remains until slot reuse, keeping retention bounded by Cap().
	readers atomic.Int32

	// waitingDrained indicates a publisher is blocked waiting for readers -> 0.
	waitingDrained atomic.Bool

	// notify points to the notification channel for the next sequence that
	// will occupy this slot index.
	notify atomic.Pointer[ringNotifyNode47Gemini]

	// drained is a 1-buffered channel signaled when readers reaches 0 while
	// waitingDrained is true.
	drained chan struct{}

	// value holds the broadcast payload stored once for all subscribers.
	value V
}

// ChanRing47Gemini is a bounded, multi-consumer, pipelined broadcast ring that
// integrates natively with Go select statements without allocating per-consumer
// channels.
//
// Unlike [ChanCaster47Gemini] and [ChanPubSub47Gemini], which perform unbuffered rendezvous
// broadcasts (1 message in flight, stalling the publisher until every
// subscriber finishes each message), ChanRing47Gemini allows up to Cap() messages to
// be pipelined in flight concurrently while preserving:
//
//   - Zero heap allocations on [ChanRing47Gemini.Subscribe], [RingReader47Gemini.Close],
//     [RingReader47Gemini.Ready], and [RingReader47Gemini.TryRecv], enabling extreme consumer
//     churn with zero GC pressure.
//   - Single-copy payload storage: each published value V is written once into
//     a pre-allocated ring slot regardless of the number of consumers. Values
//     remain bounded by ring capacity and are replaced when their slot is reused.
//   - Native select composability via [RingReader47Gemini.Ready] (<-chan struct{}),
//     paired with lock-free batch draining via [RingReader47Gemini.TryRecv] to amortize
//     Go runtime scheduler wakeups across message bursts.
//   - Lossless backpressure ([ChanRing47Gemini.Send]) or non-blocking overrun
//     detection ([ChanRing47Gemini.TrySend]).
type ChanRing47Gemini[V any] struct {
	slots []ringSlot47Gemini[V]
	mask  uint64

	// writeSeq holds the 1-based sequence number of the most recently published
	// message (0 when nothing has been published yet).
	writeSeq atomic.Uint64

	// subscribers tracks the number of currently active RingReader47Gemini instances.
	subscribers atomic.Int32

	closed atomic.Bool

	// mu serialises Send, TrySend, Subscribe, RingReader47Gemini.Close, and Close.
	// Hot-path consumer operations (Ready and TryRecv) are 100% lock-free.
	mu sync.Mutex
}

// NewChanRing47Gemini constructs a [ChanRing47Gemini] with capacity rounded up to the next
// power of two (minimum 2). It panics if capacity < 1.
func NewChanRing47Gemini[V any](capacity int) *ChanRing47Gemini[V] {
	if capacity < 1 {
		panic("bigbuff: chanring: capacity must be >= 1")
	}
	size := nextPowerOfTwo47Gemini(capacity)
	if size < 2 {
		size = 2
	}

	r := &ChanRing47Gemini[V]{
		slots: make([]ringSlot47Gemini[V], size),
		mask:  uint64(size - 1),
	}
	for i := range size {
		// Slot i first serves 1-based sequence i (with slot 0 serving sequence size).
		firstSeq := uint64(i)
		if firstSeq == 0 {
			firstSeq = uint64(size)
		}
		r.slots[i].drained = make(chan struct{}, 1)
		r.slots[i].notify.Store(&ringNotifyNode47Gemini{
			seq: firstSeq,
			ch:  make(chan struct{}),
		})
	}
	return r
}

func nextPowerOfTwo47Gemini(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// Cap returns the ring buffer capacity (always a power of two >= 2).
func (r *ChanRing47Gemini[V]) Cap() int {
	return len(r.slots)
}

// Subscribers returns the current number of active subscribers.
func (r *ChanRing47Gemini[V]) Subscribers() int {
	return int(r.subscribers.Load())
}

// Send broadcasts value to all active subscribers, returning the number of
// subscribers that will receive the value.
//
// If the slowest active subscriber is a full ring ([ChanRing47Gemini.Cap] messages)
// behind, Send blocks until that subscriber advances or unsubscribes, or until
// the ring is closed (in which case Send returns 0).
//
// If there are no active subscribers, Send returns 0 immediately without
// consuming a ring slot or allocating.
func (r *ChanRing47Gemini[V]) Send(value V) int {
	sent, _ := r.publish(value, true)
	return sent
}

// TrySend attempts to broadcast value to all active subscribers without
// blocking. It returns (sent, true) if the value was published (including when
// there are 0 subscribers, returning (0, true)), or (0, false) if the ring is
// full or closed.
func (r *ChanRing47Gemini[V]) TrySend(value V) (sent int, ok bool) {
	return r.publish(value, false)
}

func (r *ChanRing47Gemini[V]) publish(value V, block bool) (int, bool) {
	if r.closed.Load() {
		return 0, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return 0, false
	}

	subs := r.subscribers.Load()
	if subs == 0 {
		return 0, true // fast path: nobody listening
	}

	nextSeq := r.writeSeq.Load() + 1
	slot := &r.slots[nextSeq&r.mask]

	// Ensure the slot from generation (nextSeq - Cap) has been completely
	// drained by all readers before reusing it.
	if slot.readers.Load() > 0 {
		if !block {
			return 0, false
		}
		// Drain any stale token before arming waitingDrained.
		select {
		case <-slot.drained:
		default:
		}
		slot.waitingDrained.Store(true)
		for slot.readers.Load() > 0 && !r.closed.Load() {
			// Release mu while waiting so slow readers can call Close() to
			// unsubscribe and unblock Send without deadlocking!
			r.mu.Unlock()
			<-slot.drained
			r.mu.Lock()
		}
		slot.waitingDrained.Store(false)
		if r.closed.Load() {
			return 0, false
		}
		// Re-check subscribers in case membership changed while parked.
		subs = r.subscribers.Load()
		if subs == 0 {
			return 0, true
		}
	}

	curNode := slot.notify.Load()
	nextNode := &ringNotifyNode47Gemini{
		seq: nextSeq + uint64(len(r.slots)),
		ch:  make(chan struct{}),
	}

	// Publish value with Release semantics on slot.seq and writeSeq, then close
	// the previous generation's notification channel to wake all caught-up
	// readers in a single runtime closechan operation.
	slot.value = value
	slot.readers.Store(subs)
	slot.notify.Store(nextNode)
	r.writeSeq.Store(nextSeq)
	slot.seq.Store(nextSeq)
	close(curNode.ch)

	return int(subs), true
}

// Close closes the ring, waking any blocked [ChanRing47Gemini.Send] calls and all
// readers parked on [RingReader47Gemini.Ready]. Subsequent sends return 0 / false,
// while readers may continue draining any already-published messages via
// [RingReader47Gemini.TryRecv].
func (r *ChanRing47Gemini[V]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.closed.CompareAndSwap(false, true) {
		return
	}

	// Wake any publisher blocked on a full slot and close all pending slot
	// notification channels so parked readers wake immediately.
	for i := range r.slots {
		slot := &r.slots[i]
		if slot.waitingDrained.Load() {
			select {
			case slot.drained <- struct{}{}:
			default:
			}
		}
		if node := slot.notify.Load(); node != nil {
			// Replace with an already-closed node before closing node.ch so
			// repeated inspection never observes an unclosed channel.
			slot.notify.Store(&ringNotifyNode47Gemini{
				seq: node.seq,
				ch:  closedRingNotify47Gemini,
			})
			if node.ch != closedRingNotify47Gemini {
				close(node.ch)
			}
		}
	}
}

// Subscribe creates a new [RingReader47Gemini] positioned at the current head of the
// ring, receiving all messages published after Subscribe returns.
//
// Subscribe performs zero heap allocations; callers should call
// [RingReader47Gemini.Close] when finished consuming so unread slots can be reclaimed.
func (r *ChanRing47Gemini[V]) Subscribe() RingReader47Gemini[V] {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return RingReader47Gemini[V]{ring: r, closed: true}
	}

	head := r.writeSeq.Load()
	r.subscribers.Add(1)
	return RingReader47Gemini[V]{
		ring: r,
		seq:  head,
	}
}

// SubscribeContext subscribes to the ring and returns a single-use [iter.Seq]
// that yields broadcast messages until ctx is cancelled, the iterator stops
// early, or the ring is closed.
func (r *ChanRing47Gemini[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := r.Subscribe()

	if ctx == nil {
		ctx = context.Background()
	}

	// Ensure subscription is cleaned up even if the iterator is never invoked.
	var closeOnce sync.Once
	cleanup := func() {
		closeOnce.Do(sub.Close)
	}
	stop := context.AfterFunc(ctx, cleanup)

	return func(yield func(V) bool) {
		if yield == nil {
			stop()
			cleanup()
			panic("bigbuff: chanring: iterator yield func is nil")
		}

		// Once iteration starts, let only this goroutine touch RingReader's
		// value-held cursor. If cancellation already began cleanup, join it.
		if !stop() {
			cleanup()
			return
		}
		defer cleanup()

		for {
			// Drain all buffered messages without touching select or runtime channels.
			for {
				v, ok := sub.TryRecv()
				if !ok {
					break
				}
				if !yield(v) {
					return
				}
			}

			if sub.Closed() {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-sub.Ready():
			}
		}
	}
}

// RingReader47Gemini is a single-goroutine consumer cursor into a [ChanRing47Gemini].
//
// RingReader47Gemini is designed to be held by value (zero heap allocations) and used
// by a single consumer goroutine. Its [RingReader47Gemini.Ready] method returns a
// standard receive-only channel suitable for use in Go select statements.
type RingReader47Gemini[V any] struct {
	ring   *ChanRing47Gemini[V]
	seq    uint64
	closed bool
}

// Ready returns a channel that becomes readable (closed) as soon as at least
// one unread message is available in the ring or the ring/reader is closed.
//
// When messages are already buffered, Ready returns a pre-closed channel with
// zero allocations, causing select statements to proceed immediately without
// parking the goroutine.
func (r *RingReader47Gemini[V]) Ready() <-chan struct{} {
	if r.closed || r.ring == nil || r.ring.closed.Load() {
		return closedRingNotify47Gemini
	}

	target := r.seq + 1
	if r.ring.writeSeq.Load() >= target {
		return closedRingNotify47Gemini
	}

	slot := &r.ring.slots[target&r.ring.mask]
	node := slot.notify.Load()
	if node == nil || node.seq != target || slot.seq.Load() >= target || r.ring.closed.Load() {
		return closedRingNotify47Gemini
	}
	return node.ch
}

// TryRecv consumes and returns the next unread message if available, without
// blocking. Callers woken by [RingReader47Gemini.Ready] should drain all available
// messages in a loop until TryRecv returns ok == false.
func (r *RingReader47Gemini[V]) TryRecv() (value V, ok bool) {
	if r.closed || r.ring == nil {
		return value, false
	}

	target := r.seq + 1
	slot := &r.ring.slots[target&r.ring.mask]
	if slot.seq.Load() < target {
		return value, false
	}

	value = slot.value
	r.seq = target
	releaseRingSlot47Gemini(slot)
	return value, true
}

// Pending returns the number of published messages currently buffered ahead of
// this reader.
func (r *RingReader47Gemini[V]) Pending() int {
	if r.closed || r.ring == nil {
		return 0
	}
	head := r.ring.writeSeq.Load()
	if head <= r.seq {
		return 0
	}
	return int(head - r.seq)
}

// Closed reports whether either this reader or its underlying [ChanRing47Gemini] has
// been closed.
func (r *RingReader47Gemini[V]) Closed() bool {
	return r.closed || r.ring == nil || r.ring.closed.Load()
}

// Close unsubscribes this reader from the underlying [ChanRing47Gemini], releasing any
// unread slots so they cannot block future [ChanRing47Gemini.Send] calls. Close is
// idempotent.
func (r *RingReader47Gemini[V]) Close() {
	if r.closed || r.ring == nil {
		return
	}

	r.ring.mu.Lock()
	defer r.ring.mu.Unlock()

	if r.closed {
		return
	}
	r.closed = true

	if r.ring.closed.Load() {
		// Release any unread slots even if the ring was closed first.
		head := r.ring.writeSeq.Load()
		for r.seq < head {
			r.seq++
			releaseRingSlot47Gemini(&r.ring.slots[r.seq&r.ring.mask])
		}
		return
	}

	r.ring.subscribers.Add(-1)
	head := r.ring.writeSeq.Load()
	for r.seq < head {
		r.seq++
		releaseRingSlot47Gemini(&r.ring.slots[r.seq&r.ring.mask])
	}
}

func releaseRingSlot47Gemini[V any](slot *ringSlot47Gemini[V]) {
	if slot.readers.Add(-1) == 0 {
		if slot.waitingDrained.Load() {
			select {
			case slot.drained <- struct{}{}:
			default:
			}
		}
	}
}
