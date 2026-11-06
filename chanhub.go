package bigbuff

import (
	"sync"
	"sync/atomic"
)

// ChanHub is a lock-free, RCU-protected fan-out hub.
//
// Subscribers register by providing their own channel via [Subscribe] and
// receive a stable identity. [Send] publishes a value to every current
// subscriber; [TrySend] performs a non-blocking fan-out.
//
// The subscriber list is maintained as a copy-on-write snapshot stored in
// an atomic pointer. Broadcasts traverse the snapshot without any mutex,
// so subscribe/unsubscribe churn never blocks a sender.
//
// All broadcast methods allocate zero memory on the hot path.
type ChanHub[V any] struct {
	mu   sync.Mutex
	snap atomic.Pointer[hubSnapshot[V]]
	next atomic.Uint64
}

// hubSnapshot is an immutable slice of subscriptions published atomically.
type hubSnapshot[V any] struct {
	gen  uint64
	subs []hubSubscription[V]
}

// hubSubscription binds a subscriber identity to its channel. done is closed
// by [ChanHub.Unsubscribe], allowing an in-progress [ChanHub.Send] to abandon
// a send to a departed subscriber instead of blocking forever.
type hubSubscription[V any] struct {
	id   uint64
	ch   chan V
	done chan struct{}
}

// NewChanHub constructs an empty [ChanHub].
func NewChanHub[V any]() *ChanHub[V] {
	h := &ChanHub[V]{}
	h.snap.Store(&hubSnapshot[V]{})
	return h
}

// Subscribe registers ch to receive broadcasts and returns a stable
// subscription ID. The ID must be passed to [Unsubscribe] to remove it.
//
// Panics if ch is nil.
func (h *ChanHub[V]) Subscribe(ch chan V) uint64 {
	if ch == nil {
		panic("bigbuff: chanhub: subscribe: nil channel")
	}

	id := h.next.Add(1)

	h.mu.Lock()
	old := h.snap.Load()
	subs := make([]hubSubscription[V], len(old.subs)+1)
	copy(subs, old.subs)
	subs[len(subs)-1] = hubSubscription[V]{id: id, ch: ch, done: make(chan struct{})}
	h.snap.Store(&hubSnapshot[V]{gen: old.gen + 1, subs: subs})
	h.mu.Unlock()

	return id
}

// Unsubscribe removes the subscription identified by id. It returns false
// if no such subscription exists.
func (h *ChanHub[V]) Unsubscribe(id uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	old := h.snap.Load()
	idx := -1
	for i, sub := range old.subs {
		if sub.id == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}

	// Signal departure BEFORE publishing the new snapshot, so a concurrent
	// Send holding the old snapshot observes the closed done channel and
	// abandons the send to this subscriber instead of blocking forever.
	close(old.subs[idx].done)

	subs := make([]hubSubscription[V], len(old.subs)-1)
	copy(subs, old.subs[:idx])
	copy(subs[idx:], old.subs[idx+1:])
	h.snap.Store(&hubSnapshot[V]{gen: old.gen + 1, subs: subs})
	return true
}

// Send broadcasts value to every current subscriber, blocking until each
// subscriber's channel accepts the value. It returns the number of
// subscribers that received the value. A subscriber that unsubscribes while
// the send is in flight is skipped (its done channel is closed), so Send
// never blocks forever on a departed subscriber.
//
// Because each subscriber owns its channel, callers may select on that
// channel directly; this is the natural companion to [select] statements.
func (h *ChanHub[V]) Send(value V) int {
	snap := h.snap.Load()
	sent := 0
	for _, sub := range snap.subs {
		// Select on the per-subscription done channel: a subscriber that
		// unsubscribes while this send is in flight must not block the
		// sender forever on a channel nobody will receive from anymore.
		select {
		case sub.ch <- value:
			sent++
		case <-sub.done:
		}
	}
	return sent
}

// TrySend broadcasts value to every current subscriber without blocking.
// If a subscriber's channel is not ready to accept, that subscriber is
// skipped for this broadcast. It returns the number of subscribers that
// received the value.
func (h *ChanHub[V]) TrySend(value V) int {
	snap := h.snap.Load()
	sent := 0
	for _, sub := range snap.subs {
		select {
		case sub.ch <- value:
			sent++
		case <-sub.done:
		default:
		}
	}
	return sent
}

// Len returns the current number of subscribers.
func (h *ChanHub[V]) Len() int {
	return len(h.snap.Load().subs)
}
