// Provenance: 49-qwen-chat-qwen-3.7-max.md (Qwen, response #49). Namespaced to preserve the existing broadcast implementations.
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

// SelectBroadcaster49Qwen is a highly sophisticated, O(1) broadcast mechanism that
// supports native Go `select` statements without requiring explicit
// synchronization methods like `Wait()` (unlike ChanPubSub) and without the
// O(N) send time or callback allocation overhead of traditional pub/sub.
//
// It achieves this by utilizing a "read-copy-update" (RCU) inspired state
// publication model coupled with a fixed-size ring buffer for message history.
// The sender publishes a new message by atomically swapping the current
// notification epoch, writing the payload to the ring buffer, and closing
// the previous notification channel. Closing a channel is a native Go
// operation that immediately unblocks ALL receivers waiting on it in a
// `select` statement.
//
// # Characteristics
//
//   - O(1) Send Time: The sender performs a constant number of operations,
//     regardless of the number of subscribers.
//   - Native `select` Support: Subscribers wait on a channel, allowing seamless
//     integration with `context`, timeouts, and other channels.
//   - O(1) publication work: Send never scans subscribers, though it allocates
//     a new immutable epoch/channel and sequence-stamped ring snapshot.
//   - Lossy for Slow Consumers: If a consumer falls behind by more than the
//     ring buffer capacity, it skips overwritten values. This API does not
//     expose an exact missed-value counter.
//
// See also [ChanCaster] and [ChanPubSub].
type SelectBroadcaster49Qwen[T any] struct {
	// mu serializes Send calls to ensure strict ordering and consistent
	// epoch transitions.
	mu sync.Mutex

	// current points to the active epoch. Subscribers read this to get the
	// current notification channel and the latest sequence number.
	current atomic.Pointer[selectBroadcaster49Epoch[T]]

	// ring contains immutable sequence-stamped snapshots. A reader verifies the
	// expected sequence after loading so slot reuse cannot return a newer value
	// as if it were an older message.
	ring []atomic.Value

	cap  uint64
	mask uint64
}

type selectBroadcaster49Epoch[T any] struct {
	// Immutable after publication: a sender closes ch but never mutates or
	// recycles this epoch object while readers may still hold its pointer.
	ch  chan struct{}
	seq uint64
}

type selectBroadcaster49Slot[T any] struct {
	seq   uint64
	value T
}

// NewSelectBroadcaster49Qwen constructs a new [SelectBroadcaster49Qwen] with the specified
// ring buffer capacity. The capacity must be a power of 2 and greater than 0.
//
// The initial state is seeded with an open notification channel and a
// sequence number of 0, ensuring that subscribers can immediately block on
// `select` even before any messages are sent.
func NewSelectBroadcaster49Qwen[T any](capacity uint64) *SelectBroadcaster49Qwen[T] {
	if capacity == 0 || capacity > uint64(int(^uint(0)>>1)) || (capacity&(capacity-1)) != 0 {
		panic("bigbuff: selectbroadcaster: capacity must be a power of 2 and greater than 0")
	}

	b := &SelectBroadcaster49Qwen[T]{
		ring: make([]atomic.Value, int(capacity)),
		cap:  capacity,
		mask: capacity - 1,
	}
	b.current.Store(&selectBroadcaster49Epoch[T]{ch: make(chan struct{})})

	return b
}

// Send broadcasts a message to all current and future subscribers in O(1) time.
//
// It writes the message to the ring buffer, creates a new notification epoch,
// atomically swaps it as the current epoch, and closes the previous epoch's
// notification channel to unblock all waiting receivers.
//
// This method is thread-safe and serializes concurrent calls.
func (x *SelectBroadcaster49Qwen[T]) Send(value T) {
	x.mu.Lock()
	old := x.current.Load()
	if old.seq == ^uint64(0) {
		x.mu.Unlock()
		panic("bigbuff: selectbroadcaster: sequence overflow")
	}
	newSeq := old.seq + 1
	x.ring[newSeq&x.mask].Store(selectBroadcaster49Slot[T]{seq: newSeq, value: value})
	next := &selectBroadcaster49Epoch[T]{ch: make(chan struct{}), seq: newSeq}
	x.current.Store(next)
	x.mu.Unlock()

	// The old epoch is immutable and is never returned to a pool: a reader
	// holding it may still be preparing to select on old.ch.
	close(old.ch)
}

// SubscribeContext immediately subscribes to the receiver, and returns a
// single-use iterator, which will yield all values sent to the receiver.
//
// The iterator will only yield messages sent AFTER the iterator is created.
// If the consumer falls behind by more than the ring buffer capacity, it
// will automatically skip to the oldest available message to avoid blocking
// the sender or corrupting the read stream.
//
// Behavior is undefined if the iterator is used more than once.
func (x *SelectBroadcaster49Qwen[T]) SubscribeContext(ctx context.Context) iter.Seq[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	// Capture the subscription point eagerly so a publish between this call
	// and first ranging is not lost.
	startSeq := x.current.Load().seq

	return func(yield func(T) bool) {
		if yield == nil {
			panic("bigbuff: selectbroadcaster: iterator yield func is nil")
		}
		mySeq := startSeq
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			e := x.current.Load()
			if e.seq > mySeq {
				if e.seq-mySeq > x.cap {
					mySeq = e.seq - x.cap
				}
				for mySeq < e.seq {
					target := mySeq + 1
					item := x.ring[target&x.mask].Load()
					if item == nil {
						break
					}
					slot := item.(selectBroadcaster49Slot[T])
					if slot.seq != target {
						// A publisher overran this cursor while yield was running.
						latest := x.current.Load().seq
						if latest > mySeq+x.cap {
							mySeq = latest - x.cap
						}
						break
					}
					mySeq = target
					if !yield(slot.value) {
						return
					}
				}
				continue
			}

			select {
			case <-ctx.Done():
				return
			case <-e.ch:
			}
		}
	}
}

// SelectBroadcaster49State is a stable notification epoch snapshot for manual
// select integration. Its channel remains valid after later sends close it.
type SelectBroadcaster49State[T any] struct {
	b   *SelectBroadcaster49Qwen[T]
	ch  <-chan struct{}
	seq uint64
}

// Epoch returns the current immutable notification epoch.
func (x *SelectBroadcaster49Qwen[T]) Epoch() SelectBroadcaster49State[T] {
	e := x.current.Load()
	return SelectBroadcaster49State[T]{b: x, ch: e.ch, seq: e.seq}
}

func (s SelectBroadcaster49State[T]) Notify() <-chan struct{} { return s.ch }
func (s SelectBroadcaster49State[T]) Seq() uint64             { return s.seq }

// Load returns exactly seq if that ring slot still contains it. It panics for
// sequence zero, a future sequence, or an overwritten slot.
func (s SelectBroadcaster49State[T]) Load(seq uint64) T {
	if s.b == nil || seq == 0 {
		panic("bigbuff: selectbroadcaster: invalid state or sequence")
	}
	current := s.b.current.Load().seq
	if seq > current || current-seq >= s.b.cap {
		panic("bigbuff: selectbroadcaster: sequence number out of bounds (overwritten or future)")
	}
	item := s.b.ring[seq&s.b.mask].Load()
	if item == nil {
		panic("bigbuff: selectbroadcaster: sequence number not published")
	}
	slot := item.(selectBroadcaster49Slot[T])
	if slot.seq != seq {
		panic("bigbuff: selectbroadcaster: sequence number overwritten")
	}
	return slot.value
}
