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

// Provenance: response 28.
//
// Repair made in the completion phase. Send restarted its walk from the head
// whenever it met a retired subscriber that was not the head, and never
// unlinked that subscriber: the subscribers in front of it were handed the
// value again on every restart and Send did not return. Send now steps past
// a retired subscriber and unlinks it with a compare-and-swap on its
// predecessor. Regression tests: broadcast_28_flint_test.go.

import (
	"context"
	"sync/atomic"
)

// Broadcast provides context-aware publish/subscribe semantics over
// unbuffered channels. It uses a lock-free subscription stack with lazy
// head cleanup, and an atomic fast path for the no-subscriber case.
type Broadcast28Flint[V any] struct {
	subscribers atomic.Int32
	head        atomic.Pointer[subscription28Flint[V]]
}

type subscription28Flint[V any] struct {
	ch    chan V
	quit  chan struct{} // closed exactly once when the subscription is retired
	next  atomic.Pointer[subscription28Flint[V]]
	state atomic.Int32 // 1 = active, 0 = inactive
}

type Subscription28Flint[V any] struct {
	b    *Broadcast28Flint[V]
	sub  *subscription28Flint[V]
	stop func() bool
}

func NewBroadcast28Flint[V any]() *Broadcast28Flint[V] {
	return &Broadcast28Flint[V]{}
}

func (b *Broadcast28Flint[V]) Subscribe(ctx context.Context) (*Subscription28Flint[V], error) {
	if ctx == nil {
		ctx = context.Background()
	}

	sub := &subscription28Flint[V]{
		ch:   make(chan V),
		quit: make(chan struct{}),
	}
	sub.state.Store(1)
	sub.next.Store(b.head.Load())

	old := b.head.Load()
	for !b.head.CompareAndSwap(old, sub) {
		old = b.head.Load()
		sub.next.Store(old)
	}

	b.subscribers.Add(1)

	s := &Subscription28Flint[V]{
		b:   b,
		sub: sub,
	}

	// context.AfterFunc returns a stop function that reports whether it
	// successfully pre-empted the callback (Go 1.21+ semantics); the original
	// snippet treated it as (bool, error), which does not exist.
	s.stop = context.AfterFunc(ctx, func() {
		_ = b.Unsubscribe(s)
	})

	return s, nil
}

func (b *Broadcast28Flint[V]) Unsubscribe(s *Subscription28Flint[V]) error {
	if s == nil || s.sub == nil {
		return nil
	}

	if s.sub.state.CompareAndSwap(1, 0) {
		// Closing quit is what lets an in-flight Send abandon this
		// subscriber instead of blocking forever on an unbuffered channel that
		// nobody will ever receive from. Closing happens exactly once because
		// only the CAS winner reaches it.
		close(s.sub.quit)
		b.subscribers.Add(-1)
	}

	return nil
}

func (s *Subscription28Flint[V]) Stop() error {
	if s.stop != nil {
		s.stop()
		s.stop = nil
		_ = s.b.Unsubscribe(s)
	}
	return nil
}

func (s *Subscription28Flint[V]) Ch() <-chan V {
	return s.sub.ch
}

func (b *Broadcast28Flint[V]) Send(value V) int {
	if b.subscribers.Load() == 0 {
		return 0
	}

	sent := 0
	var prev *subscription28Flint[V] // last node delivered to, if any
	sub := b.head.Load()

	for sub != nil {
		next := sub.next.Load()
		if sub.state.Load() == 0 {
			// Integration repair. As delivered, a retired node that was not
			// the head made the walk restart from the head: the node was
			// never unlinked, so every Send looped forever, delivering the
			// same value again and again to the subscribers in front of it.
			// The walk now steps past a retired node and unlinks it from
			// whichever pointer led to it. Either CAS may lose to a
			// concurrent Subscribe (head) or Send (prev.next); the node is
			// then simply left for a later Send. Only retired nodes are ever
			// unlinked and subscribers are only ever added at the head, so
			// no active subscriber can be dropped from the list.
			if prev == nil {
				b.head.CompareAndSwap(sub, next)
			} else {
				prev.next.CompareAndSwap(sub, next)
			}
			sub = next
			continue
		}

		select {
		case sub.ch <- value:
			sent++
		case <-sub.quit:
			// Retired between the state check and the send; abandon this
			// subscriber and keep walking (lazy cleanup happens on the next
			// visit to this node).
		}

		prev = sub
		sub = next
	}

	return sent
}
