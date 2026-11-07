I would add a **bounded replay broadcaster** rather than make `ChanPubSub`’s handshake more intricate. It offers a different, useful contract: subscribers can use `select`, publishers never wait for a subscriber, subscriber churn does not add work to publication, and a slow subscriber gets an *exact loss count* instead of holding up the system.

The key trick is that subscriptions share a **generation notification channel**, not a value channel. A publication replaces and closes that channel once, waking any selectors. Values live in a bounded sequence-numbered ring; each subscription keeps only its own cursor. Thus publication and subscription are O(1) with respect to subscriber count, with roughly one notification-channel allocation per publication while subscribers exist. Waking subscribers still entails scheduler work proportional to the number that actually wake—no implementation can avoid that.

Add `replaycast.go`:

```go
package bigbuff

import (
	"errors"
	"sync"
)

var (
	ErrReplayClosed       = errors.New("bigbuff: replaycast closed")
	ErrReplayEmpty        = errors.New("bigbuff: replaycast has no unread value")
	ErrReplayUnsubscribed = errors.New("bigbuff: replaycast subscription closed")
	ErrReplayExhausted    = errors.New("bigbuff: replaycast sequence exhausted")
)

var replayReady = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// ReplayCast broadcasts to independently paced subscribers.
//
// It retains at most capacity values. A subscriber that falls behind skips
// overwritten values; Next reports exactly how many it skipped. In contrast
// to ChanPubSub, delivery of every value is not guaranteed.
//
// The zero value is not usable; construct one with NewReplayCast.
type ReplayCast[V any] struct {
	mu     sync.Mutex
	values []V

	// seq is the sequence number of the last publication. Sequence zero
	// denotes the state before the first publication.
	seq uint64

	// signal is closed and replaced on publication while subscribers exist.
	signal chan struct{}
	active int
	closed bool
}

// ReplaySubscription is a single-consumer cursor into a ReplayCast.
// Its methods are safe to call concurrently, although consuming a
// subscription from multiple goroutines rarely has useful semantics.
type ReplaySubscription[V any] struct {
	cast *ReplayCast[V]
	next uint64 // sequence number of the next desired value; protected by cast.mu
	done chan struct{}
	dead bool // protected by cast.mu
}

// NewReplayCast constructs a broadcaster with strictly bounded value storage.
// capacity must be positive.
func NewReplayCast[V any](capacity int) *ReplayCast[V] {
	if capacity <= 0 {
		panic("bigbuff: replaycast capacity must be positive")
	}
	return &ReplayCast[V]{
		values: make([]V, capacity),
		signal: make(chan struct{}),
	}
}

// Subscribe starts with the NEXT publication, not the most recent one.
func (x *ReplayCast[V]) Subscribe() (*ReplaySubscription[V], error) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		return nil, ErrReplayClosed
	}
	x.active++
	return &ReplaySubscription[V]{
		cast: x,
		next: x.seq + 1,
		done: make(chan struct{}),
	}, nil
}

// Publish publishes one value.
func (x *ReplayCast[V]) Publish(value V) error {
	return x.PublishBatch([]V{value})
}

// PublishBatch publishes values in order, as one notification generation.
// If the batch exceeds capacity, only its last capacity values are retained.
// An empty batch has no effect.
func (x *ReplayCast[V]) PublishBatch(values []V) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		return ErrReplayClosed
	}
	if len(values) == 0 {
		return nil
	}

	// Reserve the maximum uint64 value so that every subscription's
	// next = seq + 1 remains representable.
	if uint64(len(values)) > ^uint64(0)-1-x.seq {
		return ErrReplayExhausted
	}

	old := x.seq
	x.seq += uint64(len(values))

	if x.active == 0 {
		// No cursor can observe these values. Avoid copying a potentially
		// huge batch or allocating a notification channel.
		return nil
	}

	start := 0
	if len(values) > len(x.values) {
		start = len(values) - len(x.values)
	}
	for i := start; i < len(values); i++ {
		seq := old + uint64(i) + 1
		x.values[(seq-1)%uint64(len(x.values))] = values[i]
	}

	previous := x.signal
	x.signal = make(chan struct{})
	close(previous)
	return nil
}

// Ready returns a channel suitable for a select receive case.
//
// A notification means "call Next", NOT "exactly one value is available".
// After handling it, call Ready again: the previously returned channel
// remains closed. If unread values already exist, Ready returns an already
// closed channel. A closed broadcaster likewise wakes selectors so they can
// observe ErrReplayClosed from Next.
//
// To also react to an independent Unsubscribe call, select on Done().
func (s *ReplaySubscription[V]) Ready() <-chan struct{} {
	x := s.cast
	x.mu.Lock()
	defer x.mu.Unlock()

	if s.dead || s.next <= x.seq || x.closed {
		return replayReady
	}
	return x.signal
}

// Next attempts to consume one value without blocking.
//
// missed is the number of publications overwritten since this subscription's
// previous successful Next (or since Subscribe). On ErrReplayEmpty, call
// Ready and wait. After Close, retained unread values remain consumable;
// ErrReplayClosed follows once they have been drained.
func (s *ReplaySubscription[V]) Next() (value V, missed uint64, err error) {
	x := s.cast
	x.mu.Lock()
	defer x.mu.Unlock()

	if s.dead {
		return value, 0, ErrReplayUnsubscribed
	}
	if s.next > x.seq {
		if x.closed {
			return value, 0, ErrReplayClosed
		}
		return value, 0, ErrReplayEmpty
	}

	oldest := uint64(1)
	if x.seq >= uint64(len(x.values)) {
		oldest = x.seq - uint64(len(x.values)) + 1
	}
	if s.next < oldest {
		missed = oldest - s.next
		s.next = oldest
	}

	value = x.values[(s.next-1)%uint64(len(x.values))]
	s.next++
	return value, missed, nil
}

// Done closes when this subscription is unsubscribed, including when another
// goroutine calls Unsubscribe. Closing the broadcaster does not close Done:
// retained values can still be read.
func (s *ReplaySubscription[V]) Done() <-chan struct{} { return s.done }

// Unsubscribe releases the cursor and wakes anyone selecting on Done.
// It is idempotent.
func (s *ReplaySubscription[V]) Unsubscribe() {
	x := s.cast
	x.mu.Lock()
	defer x.mu.Unlock()

	if !s.dead {
		s.dead = true
		x.active--
		close(s.done)
	}
}

// Close prevents further publication and wakes subscribers. It is
// idempotent; subscribers can still drain retained values.
func (x *ReplayCast[V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.closed {
		x.closed = true
		close(x.signal)
	}
}
```

Typical selective consumption:

```go
sub, err := cast.Subscribe()
if err != nil {
	return err
}
defer sub.Unsubscribe()

for {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sub.Done():
		return nil
	case <-sub.Ready():
		value, missed, err := sub.Next()
		switch {
		case errors.Is(err, bigbuff.ErrReplayEmpty):
			continue // Re-arm Ready; another consumer may have advanced this cursor.
		case errors.Is(err, bigbuff.ErrReplayClosed):
			return nil
		case err != nil:
			return err
		}
		_ = missed // Record or otherwise handle an overrun.
		_ = value
	}
}
```

This deliberately complements, rather than replaces, the existing implementations: **strict every-value delivery requires backpressure or subscriber-proportional storage/work**. Here bounded storage, nonblocking publication, select compatibility, and explicit overruns make the trade-off predictable—even with very large subscriber populations or heavy subscription churn.
