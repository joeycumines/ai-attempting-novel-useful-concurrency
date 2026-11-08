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
	"errors"
	"sync"
	"sync/atomic"
)

var (
	Err45FanoutClosed         = errors.New("bigbuff: fanout: closed")
	Err45FanoutNegativeBuffer = errors.New("bigbuff: fanout: negative buffer capacity")
)

const (
	fanoutActiveBit uint32 = 1 << 0
	fanoutClosedBit uint32 = 1 << 1
)

// Fanout broadcasts independently to each subscription's channel.
//
// Its zero value is usable. Do not copy a Fanout after first use. Each
// subscription gets its own channel and can therefore receive directly in an
// ordinary select. Publishers are serialized, so every subscriber observes
// publications in the same order. The steady-state Broadcast path allocates
// nothing and takes no membership mutex.
//
// The intrusive list is edited under mu. Publishers traverse atomic forward
// links without copying a subscriber slice. A two-bit node lease prevents a
// channel from being closed while a send is active. Because sendMu serializes
// publishers, one active bit (rather than a reference counter) suffices.
//
// Exact delivery has an unavoidable cost: an unbuffered or full subscriber
// can hold up the publisher. SubscribeBuffered gives each subscriber an
// independent bounded queue when that trade-off is undesirable.
type Fanout[T any] struct {
	mu     sync.Mutex // protects closed, node.prev, and node.linked
	sendMu sync.Mutex // serializes publications

	head  atomic.Pointer[fanoutNode[T]]
	count atomic.Int64

	closed    bool // protected by mu
	closeOnce sync.Once
}

type fanoutNode[T any] struct {
	hub  *Fanout[T]
	ch   chan T
	stop chan struct{}

	next atomic.Pointer[fanoutNode[T]]
	prev *fanoutNode[T] // protected by hub.mu

	// active protects ch from closure during a send; closed bars new leases.
	state atomic.Uint32

	doneMu   sync.Mutex
	done     chan struct{} // allocated only if a closer must wait
	finished bool          // protected by doneMu
	linked   bool          // protected by hub.mu
}

// FanoutSubscription owns one receive channel in a Fanout.
type FanoutSubscription[T any] struct {
	node      atomic.Pointer[fanoutNode[T]]
	ch        <-chan T
	closeOnce sync.Once
}

func NewFanout[T any]() *Fanout[T] { return new(Fanout[T]) }

// Subscribe adds an unbuffered subscription.
func (f *Fanout[T]) Subscribe() (*FanoutSubscription[T], error) {
	return f.SubscribeBuffered(0)
}

// SubscribeBuffered adds a subscription with a bounded queue. A capacity of
// zero has rendezvous semantics. Negative capacities are rejected.
func (f *Fanout[T]) SubscribeBuffered(capacity int) (*FanoutSubscription[T], error) {
	if capacity < 0 {
		return nil, Err45FanoutNegativeBuffer
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, Err45FanoutClosed
	}

	n := &fanoutNode[T]{
		hub: f, ch: make(chan T, capacity), stop: make(chan struct{}), linked: true,
	}
	s := &FanoutSubscription[T]{ch: n.ch}
	s.node.Store(n)

	oldHead := f.head.Load()
	n.next.Store(oldHead)
	if oldHead != nil {
		oldHead.prev = n
	}
	f.head.Store(n)
	f.count.Add(1)
	return s, nil
}

// C returns the receive-only channel. A nil Subscription returns nil, which
// is disabled in a select statement.
func (s *FanoutSubscription[T]) C() <-chan T {
	if s == nil {
		return nil
	}
	return s.ch
}

// Close prevents new sends, asks a blocked in-flight send to abort, unlinks
// the node in O(1), and waits until C is closed. A send whose channel case is
// already ready may still complete before Close returns. Values already
// queued in a buffered channel remain readable after closure. Concurrent and
// repeated calls wait for the same close operation. A nil receiver is a no-op.
func (s *FanoutSubscription[T]) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		n := s.node.Swap(nil)
		if n == nil {
			return
		}
		n.deactivate()
		n.hub.unlink(n)
		n.waitFinished()
	})
}

func (s *FanoutSubscription[T]) Unsubscribe() { s.Close() }

// Broadcast sends value once to every subscription it can acquire and returns
// the number of successful hand-offs or buffered enqueues. It visits
// subscribers newest-first. A subscription added after the traversal's head
// snapshot is excluded. A concurrent Close either prevents acquisition or
// causes a blocked send to abort. No per-publication snapshot or allocation is
// used.
func (f *Fanout[T]) Broadcast(value T) (delivered int) {
	if f.head.Load() == nil {
		return 0
	}
	f.sendMu.Lock()
	defer f.sendMu.Unlock()

	for n := f.head.Load(); n != nil; {
		next := n.next.Load() // capture before a potentially blocking send
		if n.acquire() {
			// The first select keeps the immediately-ready path small. Only pay
			// for the stop-aware select when the receiver/queue is not ready.
			select {
			case n.ch <- value:
				delivered++
			default:
				select {
				case n.ch <- value:
					delivered++
				case <-n.stop:
				}
			}
			n.release()
		}
		n = next
	}
	return delivered
}

// BroadcastContext is Broadcast with context cancellation while waiting for an
// individual subscriber. It returns the number already delivered; delivery is
// not rolled back. A nil context panics. As with Broadcast, an immediately
// ready send may win a race with cancellation.
func (f *Fanout[T]) BroadcastContext(ctx context.Context, value T) (delivered int, err error) {
	if ctx == nil {
		panic("bigbuff: fanout: nil context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.head.Load() == nil {
		return 0, nil
	}
	done := ctx.Done()
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	for n := f.head.Load(); n != nil; {
		next := n.next.Load()
		if n.acquire() {
			select {
			case n.ch <- value:
				delivered++
			default:
				select {
				case n.ch <- value:
					delivered++
				case <-n.stop:
				case <-done:
					n.release()
					return delivered, ctx.Err()
				}
			}
			n.release()
		}
		n = next
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
	}
	return delivered, ctx.Err()
}

// SubscriberCount returns the point-in-time number of linked subscriptions.
// It can include a subscription whose Close has started but not unlinked yet.
func (f *Fanout[T]) SubscriberCount() int { return int(f.count.Load()) }

// Close stops new subscriptions, cancels blocked sends, closes all receive
// channels, and waits for the active publication to quiesce. It is idempotent.
func (f *Fanout[T]) Close() {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		head := f.head.Load()
		f.head.Store(nil)
		f.count.Store(0)
		for n := head; n != nil; n = n.next.Load() {
			n.prev = nil
			n.linked = false
			n.deactivate()
		}
		f.mu.Unlock()

		// stop wakes any publisher blocked on a subscriber. Taking sendMu is
		// then a quiescence barrier; only after it can links be reclaimed.
		f.sendMu.Lock()
		for n := head; n != nil; {
			n.waitFinished()
			next := n.next.Load()
			n.next.Store(nil)
			n = next
		}
		f.sendMu.Unlock()
	})
}

func (f *Fanout[T]) unlink(n *fanoutNode[T]) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !n.linked {
		return
	}
	prev, next := n.prev, n.next.Load()
	if prev == nil {
		f.head.Store(next)
	} else {
		prev.next.Store(next)
	}
	if next != nil {
		next.prev = prev
	}
	n.prev = nil
	n.linked = false
	f.count.Add(-1)
}

func (n *fanoutNode[T]) acquire() bool {
	for {
		state := n.state.Load()
		if state&fanoutClosedBit != 0 {
			return false
		}
		if state&fanoutActiveBit != 0 {
			panic("bigbuff: fanout: duplicate delivery lease")
		}
		if n.state.CompareAndSwap(state, state|fanoutActiveBit) {
			return true
		}
	}
}

func (n *fanoutNode[T]) release() {
	if state := n.state.Add(^uint32(0)); state == fanoutClosedBit {
		n.finish()
	}
}

func (n *fanoutNode[T]) deactivate() {
	for {
		state := n.state.Load()
		if state&fanoutClosedBit != 0 {
			return
		}
		if n.state.CompareAndSwap(state, state|fanoutClosedBit) {
			close(n.stop)
			if state&fanoutActiveBit == 0 {
				n.finish()
			}
			return
		}
	}
}

func (n *fanoutNode[T]) finish() {
	close(n.ch)
	n.doneMu.Lock()
	n.finished = true
	if n.done != nil {
		close(n.done)
	}
	n.doneMu.Unlock()
}

func (n *fanoutNode[T]) waitFinished() {
	n.doneMu.Lock()
	if n.finished {
		n.doneMu.Unlock()
		return
	}
	done := n.done
	if done == nil {
		done = make(chan struct{})
		n.done = done
	}
	n.doneMu.Unlock()
	<-done
}
