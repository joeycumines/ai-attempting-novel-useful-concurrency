// Provenance: 54-arena-gpt-6.1-terra-medium-prompt-safe.md (GPT-6.1 Terra, response #54). Namespaced to preserve existing APIs.
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
	"math"
	"sync"
	"sync/atomic"
)

// EpochBroadcast54Terra broadcasts values using a shared notification channel for
// each publication. Construct it with NewEpochBroadcast54Terra; do not copy it.
//
// Send serializes publications and waits until every subscriber in its
// publication snapshot has either received the value or unsubscribed.
// Subscriptions created after publication begins do not receive that value.
//
// There is no subscriber registry: subscription and unsubscription do not
// require traversing other subscribers. A broadcast with subscribers creates
// one new epoch and one channel, regardless of subscriber count.
//
// A subscriber that neither receives nor unsubscribes can block Send
// indefinitely. In particular, a subscriber must not synchronously call Send
// while remaining subscribed unless some other goroutine can operate that
// subscription.
type EpochBroadcast54Terra[V any] struct {
	sendMu sync.Mutex

	mu          sync.Mutex
	cond        sync.Cond
	subscribers int32
	next        *broadcastEpoch54Terra[V]
}

// broadcastEpoch54Terra is initially unpublished. Publication initializes value,
// next, and state, then closes ready. The close synchronizes those writes with
// every subscriber observing ready.
//
// state packs two uint32s:
//   - high: number of subscribers that received the value
//   - low:  number of subscribers still required to receive or unsubscribe
//
// The subscriber limit is MaxInt32, so neither half can overflow in valid use.
type broadcastEpoch54Terra[V any] struct {
	ready chan struct{}
	value V
	next  *broadcastEpoch54Terra[V]
	state atomic.Uint64
}

func newBroadcastEpoch54Terra[V any]() *broadcastEpoch54Terra[V] {
	return &broadcastEpoch54Terra[V]{ready: make(chan struct{})}
}

// NewEpochBroadcast54Terra constructs an EpochBroadcast54Terra.
func NewEpochBroadcast54Terra[V any]() *EpochBroadcast54Terra[V] {
	x := &EpochBroadcast54Terra[V]{}
	x.cond.L = &x.mu
	x.next = newBroadcastEpoch54Terra[V]()
	return x
}

// EpochSubscription54Terra is a single-consumer subscription.
//
// Its methods must not be called concurrently with one another, and the
// subscription must not be copied. Different subscriptions may be operated
// concurrently.
//
// A subscription remains registered until Close is called. A caller can
// select on Ready, call Next when it fires, and call Close when no longer
// interested.
type EpochSubscription54Terra[V any] struct {
	owner  *EpochBroadcast54Terra[V]
	epoch  *broadcastEpoch54Terra[V]
	closed bool
}

// Subscribe creates a subscription. It starts with the next publication after
// Subscribe acquires the broadcaster's membership lock. It does not receive a
// publication already in progress.
func (x *EpochBroadcast54Terra[V]) Subscribe() *EpochSubscription54Terra[V] {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.next == nil {
		panic("bigbuff: epochbroadcast: must use constructor")
	}
	if x.subscribers == math.MaxInt32 {
		panic("bigbuff: epochbroadcast: too many subscribers")
	}

	s := &EpochSubscription54Terra[V]{owner: x, epoch: x.next}
	x.subscribers++
	return s
}

// Ready returns the notification channel for the subscription's next value.
// Once it is closed, Next can be called without waiting for publication.
//
// Fetch Ready again after each Next. A previously returned channel remains
// closed and must not be used as the notification for a later value.
//
// Ready panics after Close.
func (s *EpochSubscription54Terra[V]) Ready() <-chan struct{} {
	if s.closed {
		panic("bigbuff: epochbroadcast: subscription closed")
	}
	return s.epoch.ready
}

// Next waits for and receives the subscription's next value, then acknowledges
// it to the sender before returning.
//
// Next panics after Close. To cancel a wait, select on Ready and a cancellation
// channel instead of calling Next before Ready fires.
func (s *EpochSubscription54Terra[V]) Next() V {
	if s.closed {
		panic("bigbuff: epochbroadcast: subscription closed")
	}

	e := s.epoch
	<-e.ready

	// The channel close publishes value and next.
	value := e.value
	s.epoch = e.next
	s.owner.ack(e, true)
	return value
}

// Close unsubscribes. It is safe to call Close more than once, but, like all
// subscription methods, it must not run concurrently with another method on
// the same subscription.
//
// If a publication is outstanding for this subscription, Close releases its
// contribution to that publication without receiving the value.
func (s *EpochSubscription54Terra[V]) Close() {
	if s.closed {
		return
	}

	x := s.owner
	x.mu.Lock()
	s.closed = true
	x.subscribers--

	// At most one publication can be outstanding for a subscriber: Send
	// cannot publish another until this subscriber acknowledges the current
	// one. If the cursor differs from next, it is that outstanding epoch.
	e := s.epoch
	outstanding := e != x.next
	s.epoch = nil
	x.mu.Unlock()

	if outstanding {
		x.ack(e, false)
	}
}

// Send publishes value to the current subscriber snapshot and waits for each
// member of that snapshot to receive or unsubscribe. It returns the number
// that received the value.
//
// Concurrent Send calls are serialized. No ordering is promised between
// concurrent callers before they acquire the send lock.
//
// As with ordinary channel sends, the caller must synchronize any subsequent
// mutation of data reachable through value.
func (x *EpochBroadcast54Terra[V]) Send(value V) int {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	if x.next == nil {
		x.mu.Unlock()
		panic("bigbuff: epochbroadcast: must use constructor")
	}
	if x.subscribers == 0 {
		x.mu.Unlock()
		return 0
	}

	e := x.next
	next := newBroadcastEpoch54Terra[V]()
	e.value = value
	e.next = next
	e.state.Store(uint64(x.subscribers))
	x.next = next

	// Closing under mu makes publication and membership changes linearizable:
	// a subscriber is either in this snapshot or starts at next.
	close(e.ready)

	// The final acknowledgement signals under mu. Checking the predicate
	// while holding mu prevents a signal from being lost before Wait.
	for uint32(e.state.Load()) != 0 {
		x.cond.Wait()
	}
	delivered := uint32(e.state.Load() >> 32)
	x.mu.Unlock()

	return int(delivered)
}

// ack removes one outstanding obligation. For a receive, adding 0xffffffff
// simultaneously decrements the low half and, because the low half was
// nonzero, carries one into the high half. For an unsubscribe, subtracting
// one only decrements the low half.
func (x *EpochBroadcast54Terra[V]) ack(e *broadcastEpoch54Terra[V], received bool) {
	var state uint64
	if received {
		state = e.state.Add(uint64(math.MaxUint32))
	} else {
		state = e.state.Add(^uint64(0))
	}

	if uint32(state) == 0 {
		x.mu.Lock()
		x.cond.Signal()
		x.mu.Unlock()
	}
}
