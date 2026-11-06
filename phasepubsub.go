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

// PhasePubSub broadcasts values using two permanently allocated notification
// channels and two shared value slots. It has no subscriber registry, no
// per-subscriber channel, and no post-receive barrier. A subscriber's local
// publication number determines which channel and value slot it uses next.
//
// Construct instances with NewPhasePubSub. A PhasePubSub must not be copied.
// Each subscription belongs to one goroutine at a time. After receiving one
// notification from its C method, that goroutine MUST call Read exactly once,
// before receiving again, unsubscribing, or handing the subscription to another
// goroutine. Read copies the value and advances the local publication number.
//
// Send applies backpressure: each subscriber present at the start of a
// publication must receive its notification or unsubscribe before Send returns.
// A subscriber joining during a publication starts with the next publication.
// Subscribers must therefore keep receiving or promptly unsubscribe. Send waits
// for notification handoffs, not for Read calls or application processing.
//
// The alternating channels also protect the shared slots. Before a slot can be
// overwritten two publications later, every remaining reader of its old value
// must receive the intervening publication. Its earlier Read must finish before
// that receive. For an unbuffered channel, the receive happens before completion
// of the corresponding send, providing the required reclamation ordering. A
// departing reader supplies that ordering through Unsubscribe instead.
//
// The channels carry only struct{}, so each value is stored once by Send and
// copied directly by readers. As with ordinary channels, reference-containing
// values do not imply a deep copy or permit concurrent mutation of their data.
// There are no explicit allocations after construction, including subscription
// churn; compiler/runtime allocations and allocation by V itself are separate.
type PhasePubSub[V any] struct {
	sendMu sync.Mutex
	mu     sync.Mutex

	// All writes to subscribers, epoch, and active hold mu. The atomic count
	// also supplies the empty-publisher fast path without acquiring either lock.
	subscribers atomic.Int32
	epoch       uint64
	active      bool
	canceled    int

	notify [2]chan struct{}
	value  [2]V
}

// PhaseSubscription is a caller-owned subscription to a PhasePubSub. Its zero
// value is unsubscribed. It must not be copied after first use or used
// concurrently. Moving an unused value into its eventual owner is permitted.
//
// There is no registry entry or channel associated with this handle. C and Read
// only access the handle and the current shared slot; they acquire no mutex and
// perform no atomic read-modify-write operation.
type PhaseSubscription[V any] struct {
	pub   *PhasePubSub[V]
	epoch uint64
}

// NewPhasePubSub constructs a broadcaster with two unbuffered notification
// channels. The channels remain open for the lifetime of the broadcaster.
// Consumers select on their own cancellation signal and then Unsubscribe.
func NewPhasePubSub[V any]() *PhasePubSub[V] {
	x := new(PhasePubSub[V])
	x.notify[0] = make(chan struct{})
	x.notify[1] = make(chan struct{})
	return x
}

// Subscribe immediately registers a subscriber. If a publication is already
// in progress, the subscriber joins the next publication without waiting for
// the current one to finish. The returned handle need not be heap allocated.
func (x *PhasePubSub[V]) Subscribe() PhaseSubscription[V] {
	x.checkInitialized()
	x.mu.Lock()
	n := x.subscribers.Load()
	if n == math.MaxInt32 {
		x.mu.Unlock()
		panic("bigbuff: phasepubsub: too many subscribers")
	}
	x.subscribers.Store(n + 1)
	epoch := x.epoch
	if x.active {
		epoch++
	}
	x.mu.Unlock()
	return PhaseSubscription[V]{pub: x, epoch: epoch}
}

// Send publishes value and returns the number of subscribers that received its
// notification, excluding notifications drained by Unsubscribe. It does not
// wait for those subscribers to call Read. Publications are serialized.
// With no subscribers, Send returns zero without acquiring a lock.
func (x *PhasePubSub[V]) Send(value V) int {
	x.checkInitialized()
	if x.subscribers.Load() == 0 {
		return 0
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.mu.Lock()
	n := int(x.subscribers.Load())
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	i := x.epoch & 1
	x.active = true
	x.canceled = 0
	x.mu.Unlock()

	// The preceding publication's handoffs/unsubscriptions protect this slot
	// from old readers. This write precedes all current notification handoffs.
	x.value[i] = value
	for range n {
		x.notify[i] <- struct{}{}
	}

	x.mu.Lock()
	sent := n - x.canceled
	x.active = false
	x.epoch++
	if x.subscribers.Load() == 0 {
		// All readers copied before leaving; release references in both slots.
		x.value = [2]V{}
	}
	x.mu.Unlock()
	return sent
}

// C returns the channel for this subscription's next notification. It returns
// nil for an unsubscribed handle, disabling its case in a select statement.
// Do not cache the channel across Read calls: it alternates on every Read.
func (s *PhaseSubscription[V]) C() <-chan struct{} {
	if s.pub == nil {
		return nil
	}
	return s.pub.notify[s.epoch&1]
}

// Read copies the current value and advances to the next notification channel.
// It MUST be called exactly once after each receive from C and before any
// further use of the subscription. Calling it without receiving first, or
// receiving again/unsubscribing before Read, violates the protocol.
func (s *PhaseSubscription[V]) Read() V {
	if s.pub == nil {
		panic("bigbuff: phasepubsub: read of unsubscribed handle")
	}
	// Copy before advancing: a subsequent receive or Unsubscribe permits this
	// slot to be reused. No reference to the slot is exposed to the caller.
	value := s.pub.value[s.epoch&1]
	s.epoch++
	return value
}

// Unsubscribe removes this subscription. Repeated calls are harmless, but
// concurrent calls or concurrent receives/Read calls on the same handle are
// not permitted. A notification already received MUST be followed by Read
// before calling Unsubscribe.
//
// If an unreceived notification is already owed, Unsubscribe receives and
// discards it. It can therefore wait for the active publisher
// to progress, but never waits for another publication to begin.
func (s *PhaseSubscription[V]) Unsubscribe() {
	x := s.pub
	if x == nil {
		return
	}

	x.mu.Lock()
	epoch := x.epoch
	n := x.subscribers.Load()
	if n <= 0 || (s.epoch != epoch && (!x.active || s.epoch != epoch+1)) {
		x.mu.Unlock()
		panic("bigbuff: phasepubsub: subscription invariant violation")
	}
	owed := x.active && s.epoch == epoch
	x.subscribers.Store(n - 1)
	if owed {
		x.canceled++
	}
	s.pub = nil
	if n == 1 && !x.active {
		x.value = [2]V{}
	}
	x.mu.Unlock()

	if owed {
		// This publication cannot finish while our receipt is still owed.
		// Therefore it cannot recycle this phase before this operation has
		// completed the cancelling receive. Record cancellation before the
		// receive so Send's final count cannot race ahead of that accounting.
		<-x.notify[epoch&1]
	}
}

func (x *PhasePubSub[V]) checkInitialized() {
	if x == nil || x.notify[0] == nil || x.notify[1] == nil {
		panic("bigbuff: phasepubsub: must use factory function")
	}
}

