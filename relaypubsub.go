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

// RelayPubSub broadcasts a value using a bounded number of circulating wake
// credits. It uses two permanent notification channels, one shared value slot,
// and caller-owned subscriptions. It retains no subscriber list and starts no
// goroutines. Use NewRelayPubSub to construct one; do not copy it after use.
//
// Each publication snapshots its subscribers. A subscriber added during a
// publication starts with the next publication. Send waits until each member of
// its snapshot has copied the value or unsubscribed. Delivery is ordered and
// lossless for subscribers that remain subscribed. One slow subscriber can
// therefore delay every sender. Send does not wait for application processing.
//
// The width passed to NewRelayPubSub controls concurrent notification credits,
// not buffering of publications. The publisher seeds min(width, subscribers)
// credits. Readers forward or retire them. All readers together still perform
// O(subscribers) work, but the publisher performs only O(min(width, subscribers))
// channel sends and receives one completion notification.
//
// Send, Subscribe, Read, C, and Unsubscribe contain no explicit allocation.
// Compiler escape decisions (including for very large V), caller allocation,
// and Go's scheduler/stack allocations are separate from this property. Storage
// owned by the broadcaster is independent of subscriber count and churn. Each
// caller owns a small subscription value.
type RelayPubSub[V any] struct {
	// remaining counts readers whose copy or cancellation has not completed.
	// Its atomic modification order also joins all value reads before reuse.
	remaining atomic.Int32

	sendMu sync.Mutex
	mu     sync.Mutex

	// Membership changes hold mu. The atomic permits the empty Send fast path.
	members atomic.Int32
	width   int32

	// phase, active and canceled are protected by mu. phase names the last
	// started publication, even when inactive. New members use phase ^ 1.
	phase    uint8
	active   bool
	canceled int32

	ready [2]chan struct{}
	done  chan struct{}
	value V
}

// RelayPubSubSubscription is a single caller's participation in a RelayPubSub.
// Obtain it from Subscribe. Do not copy an active subscription, use it from
// multiple goroutines concurrently, or receive from its C more than once
// without the corresponding Read. A zero or unsubscribed value has a nil C.
//
// A subscription must keep receiving or promptly Unsubscribe. In particular,
// call Read immediately after its channel is selected, before invoking user
// code or Unsubscribe. The raw channel receive claims a credit; Read completes
// that claim. Unsubscribe is for the branch that did not receive a credit.
//
//	sub := bus.Subscribe()
//	defer sub.Unsubscribe()
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case <-sub.C():
//			value := sub.Read()
//			use(value)
//		}
//	}
//
// Evaluate C again for every receive: Read changes which of the broadcaster's
// two channels this subscriber must use. Neither notification channel closes;
// use a context or another select case for termination.
type RelayPubSubSubscription[V any] struct {
	owner *RelayPubSub[V]
	phase uint8
}

// NewRelayPubSub constructs a broadcaster with the specified maximum number of
// circulating credits. width must be in [1, math.MaxInt32]. A small width limits
// the wakeup burst; larger widths expose more concurrent readers. There is no
// universally optimal width. It does not limit the number of subscribers.
// Width is not a bound on concurrently running application handlers.
func NewRelayPubSub[V any](width int) *RelayPubSub[V] {
	if width < 1 || uint64(width) > math.MaxInt32 {
		panic("bigbuff: relaypubsub: width out of bounds")
	}
	return &RelayPubSub[V]{
		width: int32(width),
		ready: [2]chan struct{}{make(chan struct{}, width), make(chan struct{}, width)},
		done:  make(chan struct{}, 1),
	}
}

// Subscribe immediately registers a subscriber and returns its caller-owned
// handle. Registration does not wait for an in-progress publication, which this
// new subscriber will not receive. Up to math.MaxInt32 subscribers are allowed.
func (x *RelayPubSub[V]) Subscribe() RelayPubSubSubscription[V] {
	x.checkInitialized()
	x.mu.Lock()
	n := x.members.Load()
	if n == math.MaxInt32 {
		x.mu.Unlock()
		panic("bigbuff: relaypubsub: too many subscribers")
	}
	x.members.Store(n + 1)
	s := RelayPubSubSubscription[V]{owner: x, phase: x.phase ^ 1}
	x.mu.Unlock()
	return s
}

// Send broadcasts value and returns the number of subscribers that copied it.
// Concurrent Send calls are serialized, except that an empty fast-path call
// can return immediately. A concurrent unsubscribe may abandon this value and
// is excluded from the returned count. A concurrent subscribe starts with the
// next publication if this publication has already taken its snapshot.
//
// Like a channel send, this copies V, not the objects reachable through V.
// Callers must synchronize mutation of referenced slices, maps, or objects.
func (x *RelayPubSub[V]) Send(value V) int {
	x.checkInitialized()
	if x.members.Load() == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	n := x.members.Load()
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	x.phase ^= 1
	c := x.ready[x.phase]
	x.value = value
	x.remaining.Store(n)
	x.canceled = 0
	x.active = true
	x.mu.Unlock()

	// Credit conservation makes these sends nonblocking: at most k credits
	// exist, including credits in readers and the seeds still being issued.
	k := min(n, x.width)
	for range k {
		c <- struct{}{}
	}
	<-x.done

	x.mu.Lock()
	sent := n - x.canceled
	x.active = false
	// Do not retain a pointer-bearing payload while idle. All reads precede
	// their decrements; the last decrement and done receive join those reads.
	var zero V
	x.value = zero
	x.mu.Unlock()
	return int(sent)
}

// C returns the notification channel for this subscriber's next publication,
// or nil for a zero/unsubscribed handle. It must be reevaluated after every Read.
func (s *RelayPubSubSubscription[V]) C() <-chan struct{} {
	if s.owner == nil {
		return nil
	}
	return s.owner.ready[s.phase]
}

// Read must be called exactly once, immediately after receiving from C. It
// copies the published value, advances this subscriber to the opposite phase,
// and forwards or retires its credit. It does not wait for other subscribers.
// Calling Read without first receiving from C violates the protocol.
func (s *RelayPubSubSubscription[V]) Read() V {
	x := s.owner
	if x == nil {
		panic("bigbuff: relaypubsub: read on inactive subscription")
	}
	c := x.ready[s.phase]
	value := x.value
	// Advance before the decrement: the last decrement permits the next Send.
	s.phase ^= 1
	x.release(c)
	return value
}

// Unsubscribe deregisters this handle. It is idempotent. If this subscriber
// owes an in-progress publication, it consumes one credit without reading the
// value. It can block until that credit arrives, just as cancellation of an
// in-progress ChanCaster receive can block. It must not race a receive or Read
// on the same handle, and must not be called between a receive and its Read.
func (s *RelayPubSubSubscription[V]) Unsubscribe() {
	x := s.owner
	if x == nil {
		return
	}
	x.mu.Lock()
	n := x.members.Load()
	if n <= 0 {
		x.mu.Unlock()
		panic("bigbuff: relaypubsub: membership invariant violation")
	}
	x.members.Store(n - 1)
	owes := x.active && s.phase == x.phase
	if owes {
		x.canceled++
	}
	x.mu.Unlock()
	c := x.ready[s.phase]
	s.owner = nil
	if owes {
		<-c
		x.release(c)
	}
}

func (x *RelayPubSub[V]) release(c chan struct{}) {
	r := x.remaining.Add(-1)
	switch {
	case r >= x.width:
		c <- struct{}{}
	case r == 0:
		x.done <- struct{}{}
	case r < 0:
		panic("bigbuff: relaypubsub: credit invariant violation")
	}
	// If 0 < r < width, retire this credit. Exactly min(width,n) credits
	// retire, so no old-phase credit survives publication completion.
}

func (x *RelayPubSub[V]) checkInitialized() {
	if x.width == 0 {
		panic("bigbuff: relaypubsub: must use factory function")
	}
}
