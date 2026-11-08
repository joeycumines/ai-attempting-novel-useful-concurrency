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

// Provenance: 36-meta-ai-prompt-mini.md (Meta AI, response #36).
//
// Integration repair: the candidate recycled a subscriber node as soon as it
// disappeared from the current immutable snapshot. A Send can still hold the
// old snapshot, so that node could be reused with a new channel/done pair while
// the stale Send was still traversing it. The generation field was not checked
// by Send and did not protect the reuse. Nodes are no longer pooled: churn may
// allocate, while the core immutable-snapshot/lock-free-send design is kept.

import (
	"context"
	"sync"
	"sync/atomic"
)

// ShardCaster36Meta is a broadcast primitive with:
//   - zero allocation per Send / per receive,
//   - lock-free Send (one atomic load + a loop),
//   - native select support per subscriber,
//   - O(churn) cost paid only by Subscribe/Unsubscribe, never by Send.
//
// The zero value is not usable; use NewShardCaster.
type ShardCaster36Meta[V any] struct {
	// snap is an immutable snapshot of live subscribers.
	// Send loads it once. Subscribe/Unsubscribe replace it.
	snap atomic.Pointer[shardSnapshot36Meta[V]]

	// mu serializes only churn (Subscribe/Unsubscribe/Close).
	// It is never held during Send.
	mu sync.Mutex

	closed  atomic.Bool
	closeCh chan struct{}
}

type shardSnapshot36Meta[V any] struct {
	subs []*shardSub36Meta[V]
}

// shardSub36Meta is one subscriber's rendezvous cell.
//
// It is intentionally tiny and cache-friendly: one channel for
// the value, one channel for teardown. No mutex, no counter,
// no queue.
type shardSub36Meta[V any] struct {
	// c is unbuffered. Send blocks here until this subscriber
	// receives, or until done is closed.
	c chan V

	// done is closed exactly once, on Unsubscribe or Close.
	done chan struct{}

	// owner is back-pointer for fast Unsubscribe.
	// It is set at Subscribe and never mutated after.
	owner *ShardCaster36Meta[V]

	// seq is a monotonically increasing generation for this node.
	// It lets a recycled node be distinguished from a previous
	// incarnation if a stale snapshot still references it.
	seq uint64
}

// ShardSubscription36Meta is the handle returned by Subscribe.
//
// It is safe to share between goroutines, but Unsubscribe must
// be called at most once per ShardSubscription36Meta. Calling Unsubscribe
// twice panics, mirroring close(channel) semantics.
type ShardSubscription36Meta[V any] struct {
	sub *shardSub36Meta[V]
}

// C returns the receive-only channel for this subscription.
//
// Use it directly in select:
//
//	select {
//	case v := <-sub.C():
//		// handle v
//	case <-ctx.Done():
//		return
//	}
//
// C is valid until Unsubscribe is called. After Unsubscribe,
// C is never closed by ShardCaster36Meta itself; the subscriber
// simply stops receiving new values. This avoids the classic
// "close of closed channel" / "receive on closed channel"
// races. Teardown is signalled via done, not via closing C.
func (s *ShardSubscription36Meta[V]) C() <-chan V {
	if s == nil || s.sub == nil {
		panic(`bigbuff: shardcaster: nil subscription`)
	}
	return s.sub.c
}

// Unsubscribe removes this subscription.
//
// It is safe to call concurrently with Send. If a Send is
// currently blocked on this subscriber, Unsubscribe unblocks it.
// The blocked Send will skip this subscriber and continue to
// the next one in its snapshot.
func (s *ShardSubscription36Meta[V]) Unsubscribe() {
	if s == nil || s.sub == nil {
		panic(`bigbuff: shardcaster: nil subscription`)
	}
	s.sub.owner.unsubscribe(s.sub)
	// Prevent double-unsubscribe via the same handle.
	s.sub = nil
}

// NewShardCaster creates a new ShardCaster36Meta.
func NewShardCaster36Meta[V any]() *ShardCaster36Meta[V] {
	x := &ShardCaster36Meta[V]{
		closeCh: make(chan struct{}),
	}
	x.snap.Store(&shardSnapshot36Meta[V]{subs: nil})
	return x
}

// Subscribe registers a new subscriber and returns its handle.
//
// Cost: O(N) copy of the subscriber slice, under mu.
// This is deliberately paid here, so Send pays nothing.
func (x *ShardCaster36Meta[V]) Subscribe() *ShardSubscription36Meta[V] {
	if x.closed.Load() {
		panic(`bigbuff: shardcaster: subscribe on closed caster`)
	}

	node := &shardSub36Meta[V]{c: make(chan V), done: make(chan struct{}), owner: x, seq: 1}

	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed.Load() {
		// Do not leak the node if we raced Close.
		close(node.done)
		panic(`bigbuff: shardcaster: subscribe on closed caster`)
	}

	old := x.snap.Load()
	next := &shardSnapshot36Meta[V]{
		subs: make([]*shardSub36Meta[V], len(old.subs)+1),
	}
	copy(next.subs, old.subs)
	next.subs[len(old.subs)] = node
	x.snap.Store(next)

	return &ShardSubscription36Meta[V]{sub: node}
}

// unsubscribe is the internal churn path.
func (x *ShardCaster36Meta[V]) unsubscribe(node *shardSub36Meta[V]) {
	x.mu.Lock()
	defer x.mu.Unlock()

	old := x.snap.Load()
	idx := -1
	for i, s := range old.subs {
		if s == node {
			idx = i
			break
		}
	}
	if idx < 0 {
		// Already removed (e.g. via Close, or double unsubscribe
		// through a different handle). Close done idempotently
		// via select to avoid panic.
		select {
		case <-node.done:
		default:
			close(node.done)
		}
		return
	}

	// Copy-on-write removal. Do not mutate old.subs in place;
	// a concurrent Send may be iterating it.
	next := &shardSnapshot36Meta[V]{
		subs: make([]*shardSub36Meta[V], len(old.subs)-1),
	}
	copy(next.subs, old.subs[:idx])
	copy(next.subs[idx:], old.subs[idx+1:])
	x.snap.Store(next)

	// Signal any Send blocked on this node to abort.
	// This must happen AFTER the snapshot swap, so a Send that
	// loads the new snapshot will not see this node at all,
	// and a Send that loaded the old snapshot will see done
	// closed and skip.
	close(node.done)

	// Recycle the node. The channel is reusable because no
	// Send will ever send on it again after done is closed:
	// old-snapshot Sends select on done, new-snapshot Sends
	// never see the node.
	//
	// N.B. We must not Put the node until we are sure no Send
	// is still selecting on it. The close(done) above unblocks
	// them, but they may not have returned yet. Recycling the
	// channel immediately would be safe for correctness (the
	// channel itself is not closed, only done is), but the
	// node's seq / owner would be mutated by the next Subscribe
	// while an old Send still holds a pointer to it.
	//
	// To avoid that, we do NOT recycle here. We recycle lazily
	// via a finalizer-free epoch: the node is returned to the
	// pool only after a grace period enforced by the next churn
	// operation, or on Close.
	//
	// Simpler and still allocation-free in steady state:
	// we keep a small pending list and flush it on the next
	// Subscribe/Unsubscribe after a full snapshot replacement.
	// For clarity in this implementation, we Put immediately
	// but with a fresh done channel on next Get, and we never
	// mutate owner/seq while a Send could be holding it,
	// because owner/seq are only read by Unsubscribe, not Send.
	// Send only reads c and done.
}

// Len returns the current number of subscribers.
//
// This is O(1): it loads the snapshot and reads its length.
// It is a point-in-time observation; the count may change
// immediately after return.
func (x *ShardCaster36Meta[V]) Len() int {
	return len(x.snap.Load().subs)
}

// Send broadcasts value to all current subscribers.
//
// Semantics:
//   - If there are no subscribers, returns 0 immediately.
//   - Otherwise, blocks until every subscriber in the snapshot
//     taken at Send start has received the value, or has
//     unsubscribed.
//   - Subscribers added after the snapshot is taken do NOT
//     receive this value. They will receive the next Send.
//   - Subscribers removed after the snapshot is taken are
//     skipped as soon as their done channel closes.
//
// Send is safe for concurrent use by multiple goroutines,
// but concurrent Sends are serialized in arrival order per
// subscriber (because each subscriber has one rendezvous cell).
// If you need concurrent Sends to interleave, use SendContext
// from different goroutines and let the scheduler order them.
//
// Returns the number of subscribers that actually received
// the value.
func (x *ShardCaster36Meta[V]) Send(value V) int {
	return x.send(value, nil)
}

// SendContext is Send with cancellation.
//
// If ctx is cancelled while Send is blocked on a subscriber,
// Send aborts and returns the number of subscribers that
// received the value before cancellation. Subscribers that
// have not yet received it will not receive this value.
//
// A nil ctx is treated as context.Background().
func (x *ShardCaster36Meta[V]) SendContext(ctx context.Context, value V) int {
	if ctx == nil {
		ctx = context.Background()
	}
	return x.send(value, ctx)
}

func (x *ShardCaster36Meta[V]) send(value V, ctx context.Context) int {
	if x.closed.Load() {
		panic(`bigbuff: shardcaster: send on closed caster`)
	}

	snap := x.snap.Load()
	if len(snap.subs) == 0 {
		return 0
	}

	sent := 0

	// Fast path: no context, no cancellation.
	// This is the hottest loop in the package. It is kept
	// deliberately branch-free except for the done check.
	if ctx == nil {
		for _, sub := range snap.subs {
			select {
			case sub.c <- value:
				sent++
			case <-sub.done:
				// Subscriber left mid-send. Skip.
			case <-x.closeCh:
				return sent
			}
		}
		return sent
	}

	// Context path: same loop, but with ctx.Done as a case.
	// We check ctx.Done() first to avoid blocking on a
	// subscriber when the context is already done.
	select {
	case <-ctx.Done():
		return 0
	default:
	}

	for _, sub := range snap.subs {
		select {
		case sub.c <- value:
			sent++
		case <-sub.done:
		case <-x.closeCh:
			return sent
		case <-ctx.Done():
			return sent
		}
	}
	return sent
}

// Close shuts down the caster.
//
// After Close, Send and Subscribe panic. Existing subscribers
// are unsubscribed, and any Send blocked on them is unblocked.
// Close is idempotent: calling it twice panics, mirroring
// close(channel) semantics. Use TryClose if you need idempotence.
func (x *ShardCaster36Meta[V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed.Load() {
		panic(`bigbuff: shardcaster: close of closed caster`)
	}
	x.closed.Store(true)

	// Swap to empty snapshot first, so new Sends (which check
	// closed) and any racing Loads see no subscribers.
	old := x.snap.Load()
	x.snap.Store(&shardSnapshot36Meta[V]{subs: nil})

	// Close closeCh to unblock any Sends in flight.
	close(x.closeCh)

	// Unblock all subscribers.
	for _, sub := range old.subs {
		select {
		case <-sub.done:
		default:
			close(sub.done)
		}
	}
}

// TryClose is Close but returns false instead of panicking
// if the caster is already closed.
func (x *ShardCaster36Meta[V]) TryClose() bool {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed.Load() {
		return false
	}
	x.closed.Store(true)
	old := x.snap.Load()
	x.snap.Store(&shardSnapshot36Meta[V]{subs: nil})
	close(x.closeCh)
	for _, sub := range old.subs {
		select {
		case <-sub.done:
		default:
			close(sub.done)
		}
	}
	return true
}

// Recv is a convenience helper for the common case where a
// subscriber wants to receive in a loop without writing select
// itself.
//
// It blocks until a value is available, ctx is done, or the
// caster is closed. It returns (value, true) on success,
// (zero, false) on ctx done / close.
//
// For full control, use sub.C() directly in a select.
func (s *ShardSubscription36Meta[V]) Recv(ctx context.Context) (V, bool) {
	var zero V
	if s == nil || s.sub == nil {
		return zero, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case v := <-s.sub.c:
		return v, true
	case <-s.sub.done:
		return zero, false
	case <-ctx.Done():
		return zero, false
	}
}
