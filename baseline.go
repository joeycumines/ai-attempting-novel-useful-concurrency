package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// This file holds the standard-practice BASELINE implementations, one per
// major behavioral variant in this repository. Each baseline is the
// simplest sound design that is ordinary "standard practice" - deliberately
// NOT optimised and NOT novel - so the novel implementations can be compared
// apples-to-apples:
//
//   - Lock-step pub-sub (lossless broadcast, Send waits for all receivers):
//     baseline is ChanPubSub (chanpubsub.go) - a mutex/cond barrier plus one
//     unbuffered rendezvous channel shared by all subscribers.
//   - Rendezvous caster (Send hands the value to exactly N receivers):
//     baseline is ChanCaster (chancaster.go) - the reference implementation.
//   - Latest-value coalescing broadcast: baseline is ChanLatest
//     (chanlatest.go) - an atomic slot plus a swapped signal channel.
//   - Async fan-out (Send never blocks on slow subscribers):
//     baseline is BaselineFanout (below) - a mutex-guarded subscriber set of
//     buffered channels with non-blocking drop-oldest delivery.

// BaselineFanout is the standard-practice async broadcast: a mutex-guarded
// set of subscribers, each with its own buffered channel; Send performs a
// non-blocking send to every subscriber, dropping the OLDEST buffered value
// when a subscriber's buffer is full (so the newest value is always kept).
// Sound, simple, and lossy under subscriber lag.
type BaselineFanout[V any] struct {
	mu     sync.RWMutex
	subs   map[uint64]chan V
	nextID atomic.Uint64
	cap    int
	closed atomic.Bool
}

func NewBaselineFanout[V any](capacity int) *BaselineFanout[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &BaselineFanout[V]{subs: make(map[uint64]chan V), cap: capacity}
}

// Send delivers v to every current subscriber without ever blocking on a
// slow one: if a subscriber's buffer is full, its oldest buffered value is
// dropped to make room.
func (b *BaselineFanout[V]) Send(v V) int {
	if b.closed.Load() {
		return 0
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, ch := range b.subs {
		select {
		case ch <- v:
		default:
			select {
			case <-ch: // drop oldest
			default:
			}
			select {
			case ch <- v:
			default:
			}
		}
		n++
	}
	return n
}

// Subscribe registers a new buffered channel and returns its id.
func (b *BaselineFanout[V]) Subscribe() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextID.Add(1)
	b.subs[id] = make(chan V, b.cap)
	return id
}

// Unsubscribe removes a subscription and closes its channel.
func (b *BaselineFanout[V]) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

// C returns the channel for a subscription id.
func (b *BaselineFanout[V]) C(id uint64) <-chan V {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.subs[id]
}

// Close shuts down the broadcaster, closing all subscriber channels.
func (b *BaselineFanout[V]) Close() {
	if b.closed.CompareAndSwap(false, true) {
		b.mu.Lock()
		defer b.mu.Unlock()
		for id, ch := range b.subs {
			delete(b.subs, id)
			close(ch)
		}
	}
}

// SubscribeContext yields values delivered to a fresh subscription until ctx
// is cancelled or the broadcaster is closed.
func (b *BaselineFanout[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	id := b.Subscribe()
	return func(yield func(V) bool) {
		defer b.Unsubscribe(id)
		ch := b.C(id)
		if ch == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok || !yield(v) {
					return
				}
			}
		}
	}
}
