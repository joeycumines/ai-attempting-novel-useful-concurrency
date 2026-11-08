// Provenance: 53-arena-gpt-6-sol.md (GPT-6 Sol, response #53). Namespaced to preserve existing APIs.
package bigbuff

import (
	"errors"
	"sync"
)

var (
	ErrReplayClosed53Gpt6Sol       = errors.New("bigbuff: replaycast closed")
	ErrReplayEmpty53Gpt6Sol        = errors.New("bigbuff: replaycast has no unread value")
	ErrReplayUnsubscribed53Gpt6Sol = errors.New("bigbuff: replaycast subscription closed")
	ErrReplayExhausted53Gpt6Sol    = errors.New("bigbuff: replaycast sequence exhausted")
)

var replayReady53Gpt6Sol = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// ReplayCast53Gpt6Sol broadcasts to independently paced subscribers.
//
// It retains at most capacity values. A subscriber that falls behind skips
// overwritten values; Next reports exactly how many it skipped. In contrast
// to ChanPubSub, delivery of every value is not guaranteed.
//
// The zero value is not usable; construct one with NewReplayCast53Gpt6Sol.
type ReplayCast53Gpt6Sol[V any] struct {
	mu     sync.Mutex
	values []V

	// seq is the sequence number of the last publication. Sequence zero
	// denotes the state before the first publication.
	seq uint64

	// signal is closed and replaced on publication while subscribers exist.
	signal      chan struct{}
	active      int
	closed      bool
	initialized bool // immutable after construction
}

// ReplaySubscription53Gpt6Sol is a single-consumer cursor into a ReplayCast53Gpt6Sol.
// Its methods are safe to call concurrently, although consuming a
// subscription from multiple goroutines rarely has useful semantics.
type ReplaySubscription53Gpt6Sol[V any] struct {
	cast *ReplayCast53Gpt6Sol[V]
	next uint64 // sequence number of the next desired value; protected by cast.mu
	done chan struct{}
	dead bool // protected by cast.mu
}

func (x *ReplayCast53Gpt6Sol[V]) checkInitialized() {
	if x == nil || !x.initialized {
		panic("bigbuff: replaycast must be constructed with NewReplayCast53Gpt6Sol")
	}
}

func (s *ReplaySubscription53Gpt6Sol[V]) checkInitialized() {
	if s == nil || s.cast == nil || s.done == nil {
		panic("bigbuff: replaycast subscription is not initialized")
	}
	s.cast.checkInitialized()
}

// NewReplayCast53Gpt6Sol constructs a broadcaster with strictly bounded value storage.
// capacity must be positive.
func NewReplayCast53Gpt6Sol[V any](capacity int) *ReplayCast53Gpt6Sol[V] {
	if capacity <= 0 {
		panic("bigbuff: replaycast capacity must be positive")
	}
	return &ReplayCast53Gpt6Sol[V]{
		values:      make([]V, capacity),
		signal:      make(chan struct{}),
		initialized: true,
	}
}

// Subscribe starts with the NEXT publication, not the most recent one.
func (x *ReplayCast53Gpt6Sol[V]) Subscribe() (*ReplaySubscription53Gpt6Sol[V], error) {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		return nil, ErrReplayClosed53Gpt6Sol
	}
	x.active++
	return &ReplaySubscription53Gpt6Sol[V]{
		cast: x,
		next: x.seq + 1,
		done: make(chan struct{}),
	}, nil
}

// Publish publishes one value.
func (x *ReplayCast53Gpt6Sol[V]) Publish(value V) error {
	return x.PublishBatch([]V{value})
}

// PublishBatch publishes values in order, as one notification generation.
// If the batch exceeds capacity, only its last capacity values are retained.
// An empty batch has no effect.
func (x *ReplayCast53Gpt6Sol[V]) PublishBatch(values []V) error {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.closed {
		return ErrReplayClosed53Gpt6Sol
	}
	if len(values) == 0 {
		return nil
	}

	// Reserve the maximum uint64 value so that every subscription's
	// next = seq + 1 remains representable.
	if uint64(len(values)) > ^uint64(0)-1-x.seq {
		return ErrReplayExhausted53Gpt6Sol
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
// observe ErrReplayClosed53Gpt6Sol from Next.
//
// To also react to an independent Unsubscribe call, select on Done().
func (s *ReplaySubscription53Gpt6Sol[V]) Ready() <-chan struct{} {
	s.checkInitialized()
	x := s.cast
	x.mu.Lock()
	defer x.mu.Unlock()

	if s.dead || s.next <= x.seq || x.closed {
		return replayReady53Gpt6Sol
	}
	return x.signal
}

// Next attempts to consume one value without blocking.
//
// missed is the number of publications overwritten since this subscription's
// previous successful Next (or since Subscribe). On ErrReplayEmpty53Gpt6Sol, call
// Ready and wait. After Close, retained unread values remain consumable;
// ErrReplayClosed53Gpt6Sol follows once they have been drained.
func (s *ReplaySubscription53Gpt6Sol[V]) Next() (value V, missed uint64, err error) {
	s.checkInitialized()
	x := s.cast
	x.mu.Lock()
	defer x.mu.Unlock()

	if s.dead {
		return value, 0, ErrReplayUnsubscribed53Gpt6Sol
	}
	if s.next > x.seq {
		if x.closed {
			return value, 0, ErrReplayClosed53Gpt6Sol
		}
		return value, 0, ErrReplayEmpty53Gpt6Sol
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
func (s *ReplaySubscription53Gpt6Sol[V]) Done() <-chan struct{} {
	s.checkInitialized()
	return s.done
}

// Unsubscribe releases the cursor and wakes anyone selecting on Done.
// It is idempotent.
func (s *ReplaySubscription53Gpt6Sol[V]) Unsubscribe() {
	s.checkInitialized()
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
func (x *ReplayCast53Gpt6Sol[V]) Close() {
	x.checkInitialized()
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.closed {
		x.closed = true
		close(x.signal)
	}
}
