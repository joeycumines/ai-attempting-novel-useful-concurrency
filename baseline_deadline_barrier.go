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

// BaselineDeadlineBarrier is the staff-engineer-in-a-vacuum answer to the
// deadline-bounded barrier broadcast use case (a publisher hands each value
// to every current member and waits for every acknowledgement, but must be
// able to give up at a deadline, and must never be stalled by a member that
// leaves mid-broadcast): one unbuffered channel per member, one fresh
// acknowledgement channel per member per send, and a select on the sender's
// context at every blocking point.
//
// It is deliberately ordinary. Sends are serialised by a token channel, the
// member set is copied under a mutex on every send, and every send allocates
// one small acknowledgement channel per member. Because each send has its
// own acknowledgement channels, an acknowledgement that arrives after the
// sender gave up lands in a channel nobody reads and is simply collected:
// late acknowledgements are harmless by construction, not by protocol.

import (
	"context"
	"errors"
	"sync"
)

// ErrBaselineDeadlineBarrierClosed is returned by Send after Close.
var ErrBaselineDeadlineBarrierClosed = errors.New("bigbuff: baseline deadline barrier closed")

type baselineDeadlineMsg[V any] struct {
	v   V
	ack chan struct{} // buffered 1: Ack never blocks
}

// BaselineDeadlineBarrier broadcasts to a dynamic member set with a
// per-value acknowledgement barrier bounded by the sender's context. The
// zero value is not ready for use; construct with NewBaselineDeadlineBarrier.
type BaselineDeadlineBarrier[V any] struct {
	token   chan struct{} // capacity 1: held by the sender in progress
	mu      sync.Mutex
	members map[*BaselineDeadlineMember[V]]struct{}
	closed  bool
	done    chan struct{}
}

// BaselineDeadlineMember is one member's handle. Recv and Ack must be called
// from a single goroutine; Leave may be called from any goroutine.
type BaselineDeadlineMember[V any] struct {
	b        *BaselineDeadlineBarrier[V]
	in       chan baselineDeadlineMsg[V]
	gone     chan struct{}
	leave    sync.Once
	lastAck  chan struct{}
	hasValue bool
}

// NewBaselineDeadlineBarrier returns a barrier with no members.
func NewBaselineDeadlineBarrier[V any]() *BaselineDeadlineBarrier[V] {
	b := &BaselineDeadlineBarrier[V]{
		token:   make(chan struct{}, 1),
		members: make(map[*BaselineDeadlineMember[V]]struct{}),
		done:    make(chan struct{}),
	}
	b.token <- struct{}{}
	return b
}

// Join adds a member. It reports false after Close.
func (b *BaselineDeadlineBarrier[V]) Join() (*BaselineDeadlineMember[V], bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, false
	}
	m := &BaselineDeadlineMember[V]{b: b, in: make(chan baselineDeadlineMsg[V]), gone: make(chan struct{})}
	b.members[m] = struct{}{}
	return m, true
}

// Send hands v to every member joined when it starts and waits until each
// has acknowledged or left. It returns the number of acknowledgements. If
// ctx is done first it returns the count so far and ctx.Err(); members that
// had not yet received v never will, and members that had may acknowledge
// late without effect. After Close it returns ErrBaselineDeadlineBarrierClosed.
func (b *BaselineDeadlineBarrier[V]) Send(ctx context.Context, v V) (int, error) {
	select {
	case <-b.token:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.done:
		return 0, ErrBaselineDeadlineBarrierClosed
	}
	defer func() { b.token <- struct{}{} }()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, ErrBaselineDeadlineBarrierClosed
	}
	targets := make([]*BaselineDeadlineMember[V], 0, len(b.members))
	for m := range b.members {
		targets = append(targets, m)
	}
	b.mu.Unlock()

	acks := make([]chan struct{}, 0, len(targets))
	owners := make([]*BaselineDeadlineMember[V], 0, len(targets))
	for _, m := range targets {
		msg := baselineDeadlineMsg[V]{v: v, ack: make(chan struct{}, 1)}
		select {
		case m.in <- msg:
			acks = append(acks, msg.ack)
			owners = append(owners, m)
		case <-m.gone:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-b.done:
			return 0, ErrBaselineDeadlineBarrierClosed
		}
	}
	acked := 0
	for i, ack := range acks {
		select {
		case <-ack:
			acked++
		case <-owners[i].gone:
			// A member that acknowledged and then left has both channels
			// ready, and select picks either. The acknowledgement happened,
			// so count it.
			select {
			case <-ack:
				acked++
			default:
			}
		case <-ctx.Done():
			return acked, ctx.Err()
		case <-b.done:
			return acked, ErrBaselineDeadlineBarrierClosed
		}
	}
	return acked, nil
}

// Close releases an in-flight Send and every blocked Recv. Idempotent.
func (b *BaselineDeadlineBarrier[V]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		close(b.done)
	}
}

// Recv blocks for the next value. It reports false when ctx is done, the
// member has left or the barrier is closed.
func (m *BaselineDeadlineMember[V]) Recv(ctx context.Context) (V, bool) {
	select {
	case msg := <-m.in:
		m.lastAck, m.hasValue = msg.ack, true
		return msg.v, true
	case <-ctx.Done():
	case <-m.gone:
	case <-m.b.done:
	}
	var zero V
	return zero, false
}

// Ack acknowledges the value most recently returned by Recv. It never
// blocks, and is a no-op when there is nothing to acknowledge.
func (m *BaselineDeadlineMember[V]) Ack() {
	if !m.hasValue {
		return
	}
	m.hasValue = false
	select {
	case m.lastAck <- struct{}{}:
	default:
	}
}

// Leave removes the member, releasing a sender that is waiting to hand it a
// value or waiting for its acknowledgement. Idempotent.
func (m *BaselineDeadlineMember[V]) Leave() {
	m.leave.Do(func() {
		m.b.mu.Lock()
		delete(m.b.members, m)
		m.b.mu.Unlock()
		close(m.gone)
	})
}
