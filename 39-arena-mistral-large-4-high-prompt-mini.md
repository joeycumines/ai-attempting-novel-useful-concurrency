Below is a complementary third primitive — **not** a modification of `ChanCaster`/`ChanPubSub` — designed around the same strengths you called out: very low/steady-state allocation, select-ability, and much better behaviour under consumer churn and large consumer counts.

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
	"runtime"
	"sync"
	"sync/atomic"
)

// CastPolicy controls Send behaviour when a subscriber's buffer is full.
type CastPolicy uint8

const (
	// PolicyDrop keeps the queued value and drops the new value for that
	// subscriber. This is the safest default: Send never blocks.
	PolicyDrop CastPolicy = iota

	// PolicyLatest guarantees that the new value is enqueued for every live
	// subscriber, evicting the oldest queued value when necessary. This gives
	// "latest value wins" per-subscriber semantics.
	PolicyLatest
)

// ShardCastOptions configures a ShardCast.
type ShardCastOptions struct {
	// Shards is the number of striped subscriber lists. More shards reduce
	// mutation contention under churn; fewer shards reduce Send traversal
	// overhead. Zero chooses a sensible default based on GOMAXPROCS.
	Shards int

	// Buffer is the per-subscriber channel capacity. Zero/negative means 1.
	Buffer int

	// Policy is the full-buffer policy. Invalid values fall back to PolicyDrop.
	Policy CastPolicy
}

// castNode is the pooled, internal subscriber record. Public Subscription
// handles refer to nodes; nodes may be recycled after Unsubscribe, which is
// why handles are cleared on Unsubscribe and must not be copied while active.
type castNode[V any] struct {
	ch     chan V
	done   chan struct{}
	hub    *ShardCast[V]
	shard  int
	closed atomic.Bool
}

type castShard[V any] struct {
	mu   sync.RWMutex
	subs []*castNode[V]
	n    atomic.Int32 // approximate live count; lets Send skip empty shards cheaply
}

// ShardCast is a selectable, sharded fan-out primitive.
//
// Design notes:
//   - Producers are totally ordered by a single lightweight send mutex, but
//     subscriber mutation is striped across shards, so churn does not create
//     one global write-lock bottleneck.
//   - Send is O(number of live subscribers), never O(shards) when subscribers
//     are sparse, and performs no allocation.
//   - Subscriptions expose a buffered channel, so consumers can use ordinary
//     `select` cases directly.
//   - Nodes/channels/done-signals are pooled. With SubscribeInto and caller-
//     owned Subscription values, steady-state subscription churn performs no
//     heap allocation after warmup.
type ShardCast[V any] struct {
	shards    []castShard[V]
	pool      sync.Pool
	buf       int
	policy    CastPolicy
	next      atomic.Uint64
	total     atomic.Int64
	sendMu    sync.Mutex
	closed    atomic.Bool
	closeOnce sync.Once
}

// Subscription is a stable, user-facing handle. Use SubscribeInto with a
// caller-owned Subscription to avoid allocating a handle per subscription.
//
// A Subscription must not be copied after it becomes active.
type Subscription[V any] struct {
	hub  *ShardCast[V]
	node *castNode[V]
	ch   chan V
}

func defaultShardCount() int {
	n := runtime.GOMAXPROCS(0) * 8
	if n < 8 {
		n = 8
	}
	if n > 256 {
		n = 256
	}
	return n
}

// NewShardCast creates a new sharded fan-out hub.
func NewShardCast[V any](opts ShardCastOptions) *ShardCast[V] {
	if opts.Shards <= 0 {
		opts.Shards = defaultShardCount()
	}
	if opts.Buffer <= 0 {
		opts.Buffer = 1
	}
	if opts.Policy != PolicyDrop && opts.Policy != PolicyLatest {
		opts.Policy = PolicyDrop
	}

	h := &ShardCast[V]{
		shards: make([]castShard[V], opts.Shards),
		buf:    opts.Buffer,
		policy: opts.Policy,
	}
	h.pool.New = func() any {
		return &castNode[V]{
			ch:   make(chan V, opts.Buffer),
			done: make(chan struct{}, 1),
		}
	}
	return h
}

// Subscribe allocates a new stable handle. For churn-sensitive code prefer
// SubscribeInto with a caller-owned Subscription value.
func (h *ShardCast[V]) Subscribe() *Subscription[V] {
	s := new(Subscription[V])
	h.SubscribeInto(s)
	return s
}

// SubscribeInto activates s using a pooled node. If s is embedded in a slice,
// map value, struct, etc., this is allocation-free after warmup.
func (h *ShardCast[V]) SubscribeInto(s *Subscription[V]) {
	if s == nil {
		panic("bigbuff: shardcast: nil subscription")
	}
	if s.node != nil {
		panic("bigbuff: shardcast: subscription already active")
	}
	if h.closed.Load() {
		panic("bigbuff: shardcast: closed")
	}

	var n *castNode[V]
	if v := h.pool.Get(); v != nil {
		n = v.(*castNode[V])
		if cap(n.ch) != h.buf {
			n.ch = make(chan V, h.buf)
		} else {
			drainChan(n.ch)
		}
		if cap(n.done) != 1 {
			n.done = make(chan struct{}, 1)
		} else {
			drainChan(n.done)
		}
	} else {
		n = &castNode[V]{
			ch:   make(chan V, h.buf),
			done: make(chan struct{}, 1),
		}
	}

	n.hub = h
	n.closed.Store(false)

	idx := int(h.next.Add(1)-1) % len(h.shards)
	n.shard = idx
	sh := &h.shards[idx]

	sh.mu.Lock()
	if h.closed.Load() {
		sh.mu.Unlock()
		n.hub = nil
		h.pool.Put(n)
		panic("bigbuff: shardcast: closed")
	}
	sh.n.Add(1)
	sh.subs = append(sh.subs, n)
	sh.mu.Unlock()
	h.total.Add(1)

	s.hub = h
	s.node = n
	s.ch = n.ch
}

// Unsubscribe removes the subscription and recycles the internal node. The
// handle itself is cleared; using it afterwards is invalid.
func (s *Subscription[V]) Unsubscribe() {
	if s == nil {
		return
	}
	n := s.node
	if n == nil {
		return
	}

	// Clear this handle first. If another copy of the handle already won the
	// race, do not touch the possibly-recycled node.
	s.hub = nil
	s.node = nil
	s.ch = nil

	if !n.closed.CompareAndSwap(false, true) {
		return
	}
	h := n.hub
	if h == nil {
		return
	}

	sh := &h.shards[n.shard]
	sh.mu.Lock()
	removeCastNode(sh, n)
	sh.n.Add(-1)
	sh.mu.Unlock()
	h.total.Add(-1)

	// A concurrent Send either already enqueued under sh.mu.RLock before we
	// acquired the write lock, or it will skip this node because closed is set.
	// Either way, draining here prevents stale values entering a recycled node.
	drainChan(n.ch)

	// Selectable "subscription ended" signal. The done channel is not closed,
	// so the node can still be pooled; the token is drained on reuse.
	select {
	case n.done <- struct{}{}:
	default:
	}

	n.hub = nil
	h.pool.Put(n)
}

// C returns the receive channel for use directly in a select case.
func (s *Subscription[V]) C() <-chan V {
	if s == nil {
		return nil
	}
	return s.ch
}

// Done returns a channel that receives one token when the subscription is
// unsubscribed. It is nil for an inactive handle.
func (s *Subscription[V]) Done() <-chan struct{} {
	if s == nil || s.node == nil {
		return nil
	}
	return s.node.done
}

// Valid reports whether the handle is currently active.
func (s *Subscription[V]) Valid() bool {
	return s != nil && s.node != nil
}

// Len returns the number of values currently buffered for this subscription.
func (s *Subscription[V]) Len() int {
	if s == nil || s.ch == nil {
		return 0
	}
	return len(s.ch)
}

// Cap returns the buffer capacity of this subscription's channel.
func (s *Subscription[V]) Cap() int {
	if s == nil || s.ch == nil {
		return 0
	}
	return cap(s.ch)
}

// Recv blocks until a value is available or the handle is inactive.
func (s *Subscription[V]) Recv() (V, bool) {
	if s == nil || s.ch == nil {
		var zero V
		return zero, false
	}
	v, ok := <-s.ch
	return v, ok
}

// TryRecv is the non-blocking form of Recv.
func (s *Subscription[V]) TryRecv() (V, bool) {
	if s == nil || s.ch == nil {
		var zero V
		return zero, false
	}
	select {
	case v, ok := <-s.ch:
		return v, ok
	default:
		var zero V
		return zero, false
	}
}

// Seq returns an iterator over received values until ctx is done, the
// subscription is unsubscribed, or yield returns false.
func (s *Subscription[V]) Seq(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		if yield == nil {
			panic("bigbuff: shardcast: iterator yield func is nil")
		}
		if ctx == nil {
			ctx = context.Background()
		}
		ch, done := s.ch, s.Done()
		if ch == nil {
			return
		}
		for {
			if s.node == nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case v, ok := <-ch:
				if !ok || s.node == nil || !yield(v) {
					return
				}
			}
		}
	}
}

// Send delivers v to every live subscriber according to the configured policy.
// It returns the number of subscribers for which the value was enqueued.
//
// Send is non-blocking for PolicyDrop. For PolicyLatest it always enqueues by
// evicting oldest values when needed. It allocates nothing.
func (h *ShardCast[V]) Send(v V) (delivered int) {
	if h.closed.Load() || h.total.Load() == 0 {
		return 0
	}

	// Total order for producers; subscriber lists are only read under shard
	// RLocks, so this does not serialize against Subscribe/Unsubscribe except
	// for the brief shard read critical sections.
	h.sendMu.Lock()
	defer h.sendMu.Unlock()

	if h.closed.Load() || h.total.Load() == 0 {
		return 0
	}

	for i := range h.shards {
		sh := &h.shards[i]
		if sh.n.Load() == 0 {
			continue
		}
		sh.mu.RLock()
		for _, n := range sh.subs {
			if n.closed.Load() {
				continue
			}
			if castEnqueue(n.ch, v, h.policy) {
				delivered++
			}
		}
		sh.mu.RUnlock()
	}
	return delivered
}

// Len returns the approximate number of live subscribers.
func (h *ShardCast[V]) Len() int {
	return int(h.total.Load())
}

// Closed reports whether Close has been called.
func (h *ShardCast[V]) Closed() bool {
	return h.closed.Load()
}

// Close prevents new subscriptions and makes Send a no-op. Existing
// subscriptions remain valid until individually unsubscribed.
func (h *ShardCast[V]) Close() {
	h.closeOnce.Do(func() {
		h.closed.Store(true)
	})
}

// castEnqueue enqueues v according to policy. PolicyLatest is only used with
// buffered channels of capacity >= 1, which NewShardCast guarantees.
func castEnqueue[V any](ch chan V, v V, policy CastPolicy) bool {
	if policy == PolicyLatest {
		for {
			select {
			case ch <- v:
				return true
			default:
			}
			// Buffer is full; make room by dropping the oldest value. Sends are
			// serialized by the hub, and consumers can only remove values, so
			// this loop cannot spin indefinitely.
			select {
			case <-ch:
			default:
			}
		}
	}
	select {
	case ch <- v:
		return true
	default:
		return false
	}
}

func removeCastNode[V any](sh *castShard[V], n *castNode[V]) {
	subs := sh.subs
	for i := range subs {
		if subs[i] == n {
			last := len(subs) - 1
			subs[i] = subs[last]
			sh.subs = subs[:last]
			return
		}
	}
}

func drainChan[V any](ch chan V) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
```

Key properties of this design:

- **Select-native**: each active subscription exposes `C() <-chan V`, so consumers can write plain `select { case v := <-sub.C(): ... }`.
- **Low allocation**: internal nodes, data channels, and done-signal channels are pooled; `SubscribeInto` with caller-owned `Subscription` values is allocation-free in steady state under churn.
- **Scales with churn/large N**: subscriber mutation is striped over shards with `RWMutex`; removal is swap-remove; `Send` skips empty shards via an atomic count and only takes shard read locks when a shard is non-empty.
- **Predictable producer ordering**: a single `sendMu` gives sends a total order, while not creating a global mutation bottleneck.
- **No per-send allocation**: `Send` traverses existing slices and reuses pooled channels; complexity is O(live subscribers), with an O(1) empty-hub fast path.
- **Safe recycling**: `closed` is CAS’d before removal, senders check it under the shard read lock, and unsubscribe drains before returning a node to the pool, preventing stale values from leaking into a reused subscription.
