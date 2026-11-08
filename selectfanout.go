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
	"math"
	"sync"
	"sync/atomic"
)

var (
	// ErrSelectFanoutClosed is returned when a send or subscription is attempted
	// after the fan-out has been closed.
	ErrSelectFanoutClosed = errors.New("bigbuff: selectfanout: closed")

	// ErrSelectFanoutCapacity is returned only if the uint64 subscription index
	// space is exhausted. Reaching this limit is not practically possible.
	ErrSelectFanoutCapacity = errors.New("bigbuff: selectfanout: subscription capacity exhausted")
)

// SelectFanout synchronously sends each value to every subscription present in
// the membership snapshot taken at the beginning of a send.
//
// Each subscription owns an unbuffered channel, so it can be used directly in
// an ordinary select. A send completes for a subscription only when that
// subscription receives the value. Consequently, the slowest active
// subscription applies back-pressure to the sender.
//
// The publish path reads one immutable snapshot and walks it without taking the
// membership mutex or allocating a subscriber list. Membership snapshots are
// persistent AVL trees: subscribe and unsubscribe clone only an O(log N) path,
// rather than copying an O(N) slice. Concurrent sends are serialized, giving
// every subscriber the same send order.
//
// Unsubscribe and Close cancel pending deliveries and wait for already-started
// delivery leases to drain. A data channel is closed only after its final lease
// has drained, so it is safe to range over C and cannot race a sender.
//
// Subscriber visitation order is unspecified and may change after churn.
//
// The zero value is not usable. Values must not be copied after first use.
type SelectFanout[T any] struct {
	mu        sync.Mutex
	closeOnce sync.Once

	snapshot atomic.Pointer[selectFanoutSnapshot[T]]
	closed   atomic.Bool

	// A one-token semaphore, rather than a Mutex, makes waiting to publish
	// cancellable through the caller's context.
	sendGate chan struct{}

	// Writer-lock protected. The index is the member's key in the persistent
	// AVL tree and is kept dense in [0, snapshot.count).
	members map[*selectFanoutMember[T]]uint64
	// all retains members removed from the membership tree until their
	// in-flight leases have drained, so Close can wait for them too.
	all map[*selectFanoutMember[T]]struct{}
}

type selectFanoutSnapshot[T any] struct {
	root  *selectFanoutNode[T]
	count uint64
}

type selectFanoutNode[T any] struct {
	key    uint64
	height int8
	member *selectFanoutMember[T]
	left   *selectFanoutNode[T]
	right  *selectFanoutNode[T]
}

const selectFanoutActiveBit uint64 = uint64(1) << 63
const selectFanoutLeaseMask uint64 = selectFanoutActiveBit - 1

type selectFanoutMember[T any] struct {
	ch     chan T
	cancel chan struct{}
	done   chan struct{}

	// The high bit means active; the remaining bits count delivery leases.
	// Acquiring/releasing a lease and retiring the member are CAS operations.
	// The data and done channels close exactly once when inactive && leases == 0.
	state atomic.Uint64
}

// SelectSubscription is the receive handle returned by SelectFanout.Subscribe.
// It must not be copied after first use.
type SelectSubscription[T any] struct {
	fanout *SelectFanout[T]
	member *selectFanoutMember[T]
}

// NewSelectFanout constructs a SelectFanout.
func NewSelectFanout[T any]() *SelectFanout[T] {
	f := &SelectFanout[T]{
		sendGate: make(chan struct{}, 1),
		members:  make(map[*selectFanoutMember[T]]uint64),
		all:      make(map[*selectFanoutMember[T]]struct{}),
	}
	f.snapshot.Store(new(selectFanoutSnapshot[T]))
	return f
}

// Subscribe registers a new receiver. The returned subscription is included in
// sends whose membership snapshot is taken after Subscribe publishes its new
// snapshot. A subscription created after a send has taken its snapshot will
// first receive the next send.
func (f *SelectFanout[T]) Subscribe() (*SelectSubscription[T], error) {
	f.checkInitialized()

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return nil, ErrSelectFanoutClosed
	}

	snapshot := f.snapshot.Load()
	if snapshot.count == math.MaxUint64 {
		return nil, ErrSelectFanoutCapacity
	}

	member := &selectFanoutMember[T]{
		ch:     make(chan T),
		cancel: make(chan struct{}),
		done:   make(chan struct{}),
	}

	member.state.Store(selectFanoutActiveBit)

	key := snapshot.count
	root := selectFanoutInsert(snapshot.root, key, member)
	f.members[member] = key
	f.all[member] = struct{}{}
	f.snapshot.Store(&selectFanoutSnapshot[T]{root: root, count: key + 1})

	return &SelectSubscription[T]{fanout: f, member: member}, nil
}

// C returns the subscription's receive-only data channel. It is closed after
// Unsubscribe or parent Close has drained every delivery lease. It is safe to
// range over C; Done is also available as a select case:
//
//	select {
//	case value := <-sub.C():
//	    use(value)
//	case <-sub.Done():
//	    return
//	}
func (s *SelectSubscription[T]) C() <-chan T {
	if s == nil || s.member == nil {
		return nil
	}
	return s.member.ch
}

// Done is closed after the subscription has been retired and every delivery
// lease has drained. It closes together with C's terminal state and can be used
// as a case in the same select as C.
func (s *SelectSubscription[T]) Done() <-chan struct{} {
	if s == nil || s.member == nil {
		return nil
	}
	return s.member.done
}

// Unsubscribe removes the subscription from future membership snapshots,
// cancels a delivery already waiting for this receiver, and waits until any
// delivery that won a simultaneous send/cancel race has drained. After
// Unsubscribe returns, no later value can be delivered to this subscription.
// It is safe to call more than once and concurrently with SelectFanout.Close.
func (s *SelectSubscription[T]) Unsubscribe() {
	if s == nil || s.fanout == nil || s.member == nil {
		return
	}
	s.fanout.unsubscribe(s.member)
}

// Send broadcasts value to the subscriptions in the membership snapshot taken
// for this send. It returns the number of subscriptions that received value.
// It blocks until every included subscription receives, or until a concurrent
// unsubscribe/close cancels a delivery. Use SendContext to bound back-pressure.
func (f *SelectFanout[T]) Send(value T) (int, error) {
	f.checkInitialized()
	return f.send(nil, nil, value)
}

// SendContext is Send with cancellation. If ctx is cancelled after some
// subscriptions have received the value, the returned count reports those
// completed deliveries and the error is ctx.Err(); a partial broadcast is not
// rolled back. A nil context panics, following the standard context API
// convention.
func (f *SelectFanout[T]) SendContext(ctx context.Context, value T) (int, error) {
	f.checkInitialized()
	if ctx == nil {
		panic("bigbuff: selectfanout: nil context")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return f.send(ctx, ctx.Done(), value)
}

func (f *SelectFanout[T]) send(ctx context.Context, done <-chan struct{}, value T) (int, error) {
	if f.closed.Load() {
		return 0, ErrSelectFanoutClosed
	}

	if done == nil {
		f.sendGate <- struct{}{}
	} else {
		select {
		case f.sendGate <- struct{}{}:
		case <-done:
			return 0, ctx.Err()
		}
	}
	defer func() { <-f.sendGate }()

	if done != nil {
		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}
	}
	if f.closed.Load() {
		return 0, ErrSelectFanoutClosed
	}

	snapshot := f.snapshot.Load()
	// Close stores closed before replacing the snapshot. This second check
	// distinguishes a genuinely empty live fan-out from a concurrent Close.
	if f.closed.Load() {
		return 0, ErrSelectFanoutClosed
	}
	if snapshot.count == 0 {
		return 0, nil
	}

	return f.sendTree(ctx, done, snapshot.root, value)
}

// SubscriberCount returns the number of currently subscribed receivers.
func (f *SelectFanout[T]) SubscriberCount() uint64 {
	f.checkInitialized()
	return f.snapshot.Load().count
}

// Close prevents new subscriptions and sends, removes every current
// subscription, cancels any in-flight delivery, and waits for those deliveries
// to finish. It is idempotent and safe to call concurrently.
func (f *SelectFanout[T]) Close() {
	f.checkInitialized()

	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed.Store(true)
		f.snapshot.Store(new(selectFanoutSnapshot[T]))

		members := make([]*selectFanoutMember[T], 0, len(f.all))
		for member := range f.all {
			members = append(members, member)
			member.retire()
		}
		clear(f.members)
		clear(f.all)
		f.mu.Unlock()

		for _, member := range members {
			member.waitDrained()
		}
	})
}

func (f *SelectFanout[T]) checkInitialized() {
	if f == nil || f.sendGate == nil || f.members == nil || f.all == nil || f.snapshot.Load() == nil {
		panic("bigbuff: selectfanout: must use NewSelectFanout")
	}
}

func (f *SelectFanout[T]) unsubscribe(member *selectFanoutMember[T]) {
	f.mu.Lock()

	if key, ok := f.members[member]; ok {
		snapshot := f.snapshot.Load()
		if snapshot.count == 0 {
			f.mu.Unlock()
			panic("bigbuff: selectfanout: membership invariant violation")
		}

		lastKey := snapshot.count - 1
		root := snapshot.root
		if key != lastKey {
			last := selectFanoutFind(root, lastKey)
			if last == nil || last.member == member {
				f.mu.Unlock()
				panic("bigbuff: selectfanout: index invariant violation")
			}
			root = selectFanoutSetMember(root, key, last.member)
			f.members[last.member] = key
		}
		root = selectFanoutDelete(root, lastKey)
		delete(f.members, member)
		f.snapshot.Store(&selectFanoutSnapshot[T]{root: root, count: lastKey})
	}

	// Deactivate even if Close already removed the member from the map. This
	// operation is idempotent and prevents a stale snapshot from acquiring a
	// new delivery lease.
	member.retire()
	f.mu.Unlock()

	member.waitDrained()
	f.mu.Lock()
	delete(f.all, member)
	f.mu.Unlock()
}

func (f *SelectFanout[T]) sendTree(ctx context.Context, done <-chan struct{}, root *selectFanoutNode[T], value T) (int, error) {
	// An AVL tree indexed by uint64 cannot approach this depth. Keeping the
	// traversal stack inline avoids per-publish allocation and recursive calls.
	var stack [128]*selectFanoutNode[T]
	depth := 0
	node := root
	sent := 0

	for node != nil || depth != 0 {
		for node != nil {
			if depth == len(stack) {
				panic("bigbuff: selectfanout: AVL height invariant violation")
			}
			stack[depth] = node
			depth++
			node = node.left
		}

		depth--
		node = stack[depth]
		stack[depth] = nil

		if done != nil {
			select {
			case <-done:
				return sent, ctx.Err()
			default:
			}
		}

		delivered, cancelled := node.member.deliver(done, value)
		if cancelled {
			return sent, ctx.Err()
		}
		if !delivered {
			if done != nil {
				select {
				case <-done:
					return sent, ctx.Err()
				default:
				}
			}
			if f.closed.Load() {
				return sent, ErrSelectFanoutClosed
			}
		} else {
			sent++
		}

		node = node.right
	}

	return sent, nil
}

func (m *selectFanoutMember[T]) deliver(done <-chan struct{}, value T) (delivered, cancelled bool) {
	if !m.begin() {
		return false, false
	}
	defer m.end()
	select {
	case m.ch <- value:
		return true, false
	case <-m.cancel:
		return false, false
	case <-done:
		return false, true
	}
}

func (m *selectFanoutMember[T]) begin() bool {
	for {
		state := m.state.Load()
		if state&selectFanoutActiveBit == 0 {
			return false
		}
		if state&selectFanoutLeaseMask == selectFanoutLeaseMask {
			panic("bigbuff: selectfanout: delivery lease overflow")
		}
		if m.state.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (m *selectFanoutMember[T]) end() {
	for {
		state := m.state.Load()
		leases := state & selectFanoutLeaseMask
		if leases == 0 {
			panic("bigbuff: selectfanout: delivery lease underflow")
		}
		next := state - 1
		if m.state.CompareAndSwap(state, next) {
			if next == 0 {
				close(m.ch)
				close(m.done)
			}
			return
		}
	}
}

// retire prevents new leases and wakes any sender currently waiting on ch.
// The final lease closes ch and done. Exactly one transition can produce
// inactive && zero leases, so both channels are closed exactly once.
func (m *selectFanoutMember[T]) retire() {
	for {
		state := m.state.Load()
		if state&selectFanoutActiveBit == 0 {
			return
		}
		next := state &^ selectFanoutActiveBit
		if m.state.CompareAndSwap(state, next) {
			close(m.cancel)
			if next == 0 {
				close(m.ch)
				close(m.done)
			}
			return
		}
	}
}

func (m *selectFanoutMember[T]) waitDrained() {
	<-m.done
}

func selectFanoutHeight[T any](node *selectFanoutNode[T]) int8 {
	if node == nil {
		return 0
	}
	return node.height
}

func selectFanoutMakeNode[T any](key uint64, member *selectFanoutMember[T], left, right *selectFanoutNode[T]) *selectFanoutNode[T] {
	height := selectFanoutHeight(left)
	if rightHeight := selectFanoutHeight(right); rightHeight > height {
		height = rightHeight
	}
	return &selectFanoutNode[T]{
		key:    key,
		height: height + 1,
		member: member,
		left:   left,
		right:  right,
	}
}

func selectFanoutRotateLeft[T any](node *selectFanoutNode[T]) *selectFanoutNode[T] {
	right := node.right
	newLeft := selectFanoutMakeNode(node.key, node.member, node.left, right.left)
	return selectFanoutMakeNode(right.key, right.member, newLeft, right.right)
}

func selectFanoutRotateRight[T any](node *selectFanoutNode[T]) *selectFanoutNode[T] {
	left := node.left
	newRight := selectFanoutMakeNode(node.key, node.member, left.right, node.right)
	return selectFanoutMakeNode(left.key, left.member, left.left, newRight)
}

func selectFanoutBalance[T any](node *selectFanoutNode[T]) *selectFanoutNode[T] {
	balance := int(selectFanoutHeight(node.left)) - int(selectFanoutHeight(node.right))
	if balance > 1 {
		left := node.left
		if selectFanoutHeight(left.left) < selectFanoutHeight(left.right) {
			left = selectFanoutRotateLeft(left)
			node = selectFanoutMakeNode(node.key, node.member, left, node.right)
		}
		return selectFanoutRotateRight(node)
	}
	if balance < -1 {
		right := node.right
		if selectFanoutHeight(right.right) < selectFanoutHeight(right.left) {
			right = selectFanoutRotateRight(right)
			node = selectFanoutMakeNode(node.key, node.member, node.left, right)
		}
		return selectFanoutRotateLeft(node)
	}
	return node
}

func selectFanoutInsert[T any](node *selectFanoutNode[T], key uint64, member *selectFanoutMember[T]) *selectFanoutNode[T] {
	if node == nil {
		return selectFanoutMakeNode(key, member, nil, nil)
	}
	if key < node.key {
		return selectFanoutBalance(selectFanoutMakeNode(node.key, node.member,
			selectFanoutInsert(node.left, key, member), node.right))
	}
	if key > node.key {
		return selectFanoutBalance(selectFanoutMakeNode(node.key, node.member,
			node.left, selectFanoutInsert(node.right, key, member)))
	}
	panic("bigbuff: selectfanout: duplicate index")
}

func selectFanoutSetMember[T any](node *selectFanoutNode[T], key uint64, member *selectFanoutMember[T]) *selectFanoutNode[T] {
	if node == nil {
		panic("bigbuff: selectfanout: missing index")
	}
	if key == node.key {
		return selectFanoutMakeNode(node.key, member, node.left, node.right)
	}
	if key < node.key {
		return selectFanoutMakeNode(node.key, node.member,
			selectFanoutSetMember(node.left, key, member), node.right)
	}
	return selectFanoutMakeNode(node.key, node.member,
		node.left, selectFanoutSetMember(node.right, key, member))
}

func selectFanoutFind[T any](node *selectFanoutNode[T], key uint64) *selectFanoutNode[T] {
	for node != nil {
		if key < node.key {
			node = node.left
		} else if key > node.key {
			node = node.right
		} else {
			return node
		}
	}
	return nil
}

func selectFanoutPopMin[T any](node *selectFanoutNode[T]) (root, min *selectFanoutNode[T]) {
	if node.left == nil {
		return node.right, node
	}
	left, min := selectFanoutPopMin(node.left)
	return selectFanoutBalance(selectFanoutMakeNode(node.key, node.member, left, node.right)), min
}

func selectFanoutDelete[T any](node *selectFanoutNode[T], key uint64) *selectFanoutNode[T] {
	if node == nil {
		panic("bigbuff: selectfanout: missing index")
	}
	if key < node.key {
		return selectFanoutBalance(selectFanoutMakeNode(node.key, node.member,
			selectFanoutDelete(node.left, key), node.right))
	}
	if key > node.key {
		return selectFanoutBalance(selectFanoutMakeNode(node.key, node.member,
			node.left, selectFanoutDelete(node.right, key)))
	}
	if node.left == nil {
		return node.right
	}
	if node.right == nil {
		return node.left
	}
	right, successor := selectFanoutPopMin(node.right)
	return selectFanoutBalance(selectFanoutMakeNode(successor.key, successor.member, node.left, right))
}
