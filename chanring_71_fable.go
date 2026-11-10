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

// Ported from 71-claude-fable-5.2-medium.md (ChanRing; chanring.go). Renamed
// with the 71Fable suffix for the flat package; logic is as generated.
//
// Integration review notes (as generated: compiles, vets, and its own tests
// pass under -race at GOMAXPROCS 1-8 on Go 1.27.3; no repairs were needed):
//
//   - Send is O(1) in subscriber count only when no wake round has to be
//     started. Starting a round is one unbuffered channel send, which waits
//     for one armed subscriber to reach its select; and a concurrent sender
//     (or Close) that observed the same armed waiters but lost the race to
//     claim them waits on the cascade's write lock until that round's last
//     token has been received. "Never waits on a slow subscriber" is true of
//     slow consumers of values, not of subscribers that arm and then stall
//     before selecting (the documented Ack obligation).
//   - Ack/Disarm forward the wake with up to two blocking channel sends on
//     the subscriber's goroutine: a subscriber's wake-up latency includes
//     waiting for its subtree's first receivers to reach their selects.
//   - Send on a closed ring panics (channel semantics, by design). The
//     harness adapter checks Closed() first; it never closes concurrently
//     with a publish.
//   - A cursor that is exactly one lap behind and mid-copy makes the sender
//     spin (yielding after 32 iterations) until the copy completes; a reader
//     that meets the writer bit counts the slot as lost without waiting.

package bigbuff

import (
	"context"
	"errors"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrChanRingClosed71Fable is returned by [ChanRingCursor71Fable.Recv], after the ring was
// closed, and the cursor has consumed every retained value.
var ErrChanRingClosed71Fable = errors.New(`bigbuff: chanring: closed`)

const (
	chanRingStateInvariantViolation71Fable = `bigbuff: chanring: state invariant violation`

	// chanRingWriter71Fable is the writer bit, of chanRingSlot71Fable.lock. It is the top
	// bit of a uint32, meaning that adding it a second time clears it, and
	// that any value >= chanRingWriter71Fable has the bit set.
	chanRingWriter71Fable = 1 << 31
)

type (
	// ChanRing71Fable is a buffered, lag-tolerant broadcast log, that is the
	// asynchronous complement to [ChanPubSub], and is built from the same
	// idea as [ChanCaster] (counted, anonymous tokens, on one shared,
	// unbuffered channel), with select support, and without callbacks.
	//
	// # Characteristics
	//
	// Where [ChanPubSub] is a synchronous rendezvous (every send hands a
	// value to every subscriber, and waits for all of them), ChanRing71Fable
	// decouples the two sides, entirely:
	//
	//  1. Send is O(1), regardless of the number of subscribers, parked or
	//     otherwise, and never waits on a slow subscriber. It writes one slot
	//     of a fixed ring, publishes the new tail, then performs AT MOST one
	//     channel send (see "wake cascade", below), and only if at least one
	//     subscriber is actually parked. If all subscribers are busy, a send
	//     performs no channel operations at all.
	//  2. A subscriber is a [ChanRingCursor71Fable], which is nothing more than a
	//     position. There is no subscriber registry. Subscribing and
	//     unsubscribing are free (no shared writes, no allocation), so
	//     arbitrary subscriber churn costs nothing, and is invisible to both
	//     senders and other subscribers.
	//  3. Steady state allocation is zero, for all operations. The only
	//     allocations are the ring and the channel, at construction.
	//  4. Receiving is wait-free. A subscriber which is behind drains the ring
	//     in a tight loop, at the cost of two uncontended-by-the-sender atomic
	//     adds per value, never parking, and never touching the channel.
	//  5. A subscriber which falls more than the ring's capacity behind does
	//     not stall anyone. It skips forward, to the oldest retained value,
	//     and the loss is observable, both via the (gapped) sequence numbers,
	//     and [ChanRingCursor71Fable.Lost].
	//  6. Blocking is select-compatible, via [ChanRing71Fable.C], following an
	//     arm/ack/disarm protocol, analogous to the contract of
	//     [ChanCaster.Add].
	//
	// # Wake cascade
	//
	// Parking uses a single unbuffered channel, shared by all subscribers,
	// carrying counted tokens, per [ChanCaster]. Unlike [ChanCaster], where
	// the sender performs one send per registered receiver, a ChanRing71Fable sender
	// sends exactly one token, carrying a budget, of the remaining receivers.
	// Each receiver of a token splits that budget in half, and forwards it,
	// via (at most) two further sends, on the same channel. Wake-ups therefore
	// fan out as a binary tree, across however many Ps are available, reaching
	// N parked subscribers in O(log N) hand-offs of latency, with O(1) work
	// on the part of the sender, which doesn't wait for the cascade to finish.
	// The token count is exact: N registered receivers result in exactly N
	// sends, and no receiver can be skipped, or woken twice.
	//
	// # Usage
	//
	// Instances MUST be constructed via [NewChanRing71Fable]. For simple cases, use
	// [ChanRing71Fable.SubscribeContext], or [ChanRingCursor71Fable.Recv]. For use within a
	// select statement, the contract is:
	//
	//	cur := ring.Cursor()
	//	for !cur.Closed() {
	//		for v, seq, ok := cur.TryRecv(); ok; v, seq, ok = cur.TryRecv() {
	//			// handle v
	//		}
	//		if !cur.Arm() {
	//			continue // a value arrived, or the ring was closed
	//		}
	//		select {
	//		case w := <-ring.C():
	//			cur.Ack(w) // MUST be called, promptly, exactly once
	//		case <-other:
	//			cur.Disarm() // MUST be called, promptly, exactly once
	//		}
	//	}
	//
	// That is, values MUST NOT be received from [ChanRing71Fable.C], unless armed,
	// and each successful [ChanRingCursor71Fable.Arm] MUST be followed by EITHER
	// exactly one receive, from [ChanRing71Fable.C], then [ChanRingCursor71Fable.Ack], OR
	// [ChanRingCursor71Fable.Disarm]. A receive from [ChanRing71Fable.C] indicates that
	// state MAY have changed. It is a hint, and may be spurious.
	//
	// # Caveats
	//
	// Values remain referenced, by the ring, until overwritten. Sends are
	// serialised, and totally ordered. A send will wait (briefly, without
	// parking) if, and only if, a subscriber is mid-copy, of the exact slot
	// being overwritten, i.e. that subscriber is precisely one full lap behind.
	ChanRing71Fable[V any] struct {
		slots []chanRingSlot71Fable[V]
		mask  uint64
		_     [64]byte
		// tail is the sequence number of the next value to be sent, and is
		// the only state shared between senders and (non-parking) receivers,
		// aside from the slot being read
		tail atomic.Uint64
		_    [64]byte
		// sendMu serialises senders (and Close)
		sendMu sync.Mutex
		closed atomic.Bool
		_      [64]byte
		wake   chanCascade71Fable
	}

	// ChanRingCursor71Fable is a subscriber's position, within a [ChanRing71Fable]. It is a
	// plain value, that may live on the stack, and requires no cleanup,
	// provided it isn't armed. It MUST NOT be used concurrently, and MUST NOT
	// be copied while armed.
	//
	// See [ChanRing71Fable.Cursor], and the docs for [ChanRing71Fable].
	ChanRingCursor71Fable[V any] struct {
		ring  *ChanRing71Fable[V]
		next  uint64
		lost  uint64
		armed bool
	}

	// ChanRingWake71Fable is an opaque wake token, received from [ChanRing71Fable.C], which
	// MUST be passed to [ChanRingCursor71Fable.Ack], immediately after being
	// received.
	ChanRingWake71Fable struct {
		// budget is the number of FURTHER tokens the receiver must cause to
		// be received, i.e. the size of the receiver's subtree, less itself
		budget uint32
	}

	chanRingSlot71Fable[V any] struct {
		// lock is a tiny reader-writer lock: The low 31 bits count readers
		// that have pinned the slot, and the top bit is set by the (single)
		// writer. It is writer-preferring, and neither side ever parks:
		// Readers that observe the writer bit back off, and treat the slot
		// as lost (which it is, as it's being overwritten), while the writer
		// spins, only for as long as it takes pinned readers to copy a value.
		lock atomic.Uint32
		// seq and val are guarded by lock
		seq uint64
		val V
	}

	// chanCascade71Fable is a select-compatible futex, with a self-propagating
	// (tree-structured) wake-all. It is, effectively, a [ChanCaster], where
	// the work of sending is delegated to the receivers.
	chanCascade71Fable struct {
		c chan ChanRingWake71Fable
		// mu serves the same purpose as ChanCaster.mutex: Waiters may only be
		// added (read lock) while no wake round is in flight (write lock). The
		// write lock is acquired by the waker, and is released by whichever
		// goroutine receives the final token of the round - NOT the waker.
		mu sync.RWMutex
		// state is two counters. The high 32 bits are the number of armed
		// waiters, not yet claimed by a wake round. The low 32 bits are the
		// number of tokens, of the in-flight wake round (if any), which are
		// yet to be received. At most one of the two is non-zero.
		state atomic.Uint64
	}
)

// NewChanRing71Fable constructs a [ChanRing71Fable], retaining (at least) the most recent
// capacity values, which is rounded up to a power of two. A panic will occur
// if capacity is not positive.
func NewChanRing71Fable[V any](capacity int) *ChanRing71Fable[V] {
	if capacity <= 0 || capacity > 1<<30 {
		panic(`bigbuff: chanring: capacity out of bounds`)
	}
	n := uint64(1)
	for n < uint64(capacity) {
		n <<= 1
	}
	x := ChanRing71Fable[V]{
		slots: make([]chanRingSlot71Fable[V], n),
		mask:  n - 1,
	}
	x.wake.c = make(chan ChanRingWake71Fable)
	return &x
}

// Cap returns the number of values retained by the ring.
func (x *ChanRing71Fable[V]) Cap() int { return len(x.slots) }

// Tail returns the sequence number that will be assigned to the next value
// sent, which is also the total number of values sent, thus far.
func (x *ChanRing71Fable[V]) Tail() uint64 { return x.tail.Load() }

// Closed returns true if [ChanRing71Fable.Close] has been called. See also
// [ChanRingCursor71Fable.Closed], which additionally accounts for unread values.
func (x *ChanRing71Fable[V]) Closed() bool { return x.closed.Load() }

// C returns the wake channel, for use in select statements. It MUST only be
// received from while armed, per the contract documented on [ChanRing71Fable]. It
// is never closed.
func (x *ChanRing71Fable[V]) C() <-chan ChanRingWake71Fable {
	x.check()
	return x.wake.c
}

// Cursor returns a new subscriber, positioned at the tail, such that it will
// observe only values sent after this call.
func (x *ChanRing71Fable[V]) Cursor() ChanRingCursor71Fable[V] {
	x.check()
	return ChanRingCursor71Fable[V]{ring: x, next: x.tail.Load()}
}

// Send broadcasts value, to all current and future cursors, returning the
// sequence number assigned to it. It will panic if the ring is closed.
//
// This method never waits on subscribers. See [ChanRing71Fable], for details.
func (x *ChanRing71Fable[V]) Send(value V) (seq uint64) {
	x.check()

	x.sendMu.Lock()

	if x.closed.Load() {
		x.sendMu.Unlock()
		panic(`bigbuff: chanring: send on closed ring`)
	}

	seq = x.tail.Load() // N.B. only modified while holding sendMu
	slot := &x.slots[seq&x.mask]

	// acquire the slot, for writing: set the writer bit (turning away new
	// readers, which will correctly consider the slot's old value lost), then
	// wait out any reader which had already pinned it - an event which
	// requires said reader to be exactly one lap behind, and mid-copy
	slot.lock.Add(chanRingWriter71Fable)
	for i := 0; slot.lock.Load() != chanRingWriter71Fable; i++ {
		if i >= 32 {
			runtime.Gosched()
		}
	}

	slot.seq = seq
	slot.val = value

	slot.lock.Add(chanRingWriter71Fable) // clears the writer bit (overflow)

	// publish - must be AFTER releasing the slot (readers treat the writer
	// bit as proof of having been lapped)
	x.tail.Store(seq + 1)

	x.sendMu.Unlock()

	// must be AFTER publishing (pairs with the re-check of ChanRingCursor71Fable.Arm)
	x.wake.wake()

	return seq
}

// Close marks the ring as closed, and wakes all parked subscribers. Values
// already sent remain available. Subsequent calls are no-ops, and subsequent
// calls to [ChanRing71Fable.Send] will panic.
func (x *ChanRing71Fable[V]) Close() {
	x.check()
	x.sendMu.Lock()
	x.closed.Store(true)
	x.sendMu.Unlock()
	x.wake.wake() // N.B. unconditional, to be safe vs concurrent Close calls
}

// SubscribeContext immediately subscribes (per [ChanRing71Fable.Cursor]), returning
// a single-use iterator, yielding the sequence number and value, of each
// value subsequently sent, until the context is cancelled, the iterator is
// stopped, or the ring is closed and drained. Gaps in the yielded sequence
// numbers indicate values lost, due to the consumer falling too far behind.
//
// Unlike [ChanPubSub.SubscribeContext], there is nothing to leak, should the
// iterator never be used.
func (x *ChanRing71Fable[V]) SubscribeContext(ctx context.Context) iter.Seq2[uint64, V] {
	cur := x.Cursor()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(uint64, V) bool) {
		for {
			v, seq, err := cur.Recv(ctx)
			if err != nil || !yield(seq, v) {
				return
			}
		}
	}
}

func (x *ChanRing71Fable[V]) check() {
	if x.slots == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
}

// Lost returns the total number of values skipped, by this cursor, due to
// falling more than the ring's capacity behind.
func (c *ChanRingCursor71Fable[V]) Lost() uint64 { return c.lost }

// Seq returns the sequence number of the next value this cursor will attempt
// to receive.
func (c *ChanRingCursor71Fable[V]) Seq() uint64 { return c.next }

// Pending returns the number of values sent, but not yet received (or lost),
// by this cursor. It may exceed the ring's capacity, if values have been lost.
func (c *ChanRingCursor71Fable[V]) Pending() uint64 { return c.ring.tail.Load() - c.next }

// Closed returns true if the ring was closed, AND there are no further values
// for this cursor to receive.
func (c *ChanRingCursor71Fable[V]) Closed() bool {
	// N.B. order matters - closed is set after the final send
	return c.ring.closed.Load() && c.ring.tail.Load() == c.next
}

// Rewind repositions the cursor, to the oldest value still retained, allowing
// a new subscriber to replay recent history. It panics if the cursor is armed.
func (c *ChanRingCursor71Fable[V]) Rewind() {
	if c.armed {
		panic(`bigbuff: chanring: rewind while armed`)
	}
	tail, n := c.ring.tail.Load(), uint64(len(c.ring.slots))
	c.next = 0
	if tail > n {
		c.next = tail - n
	}
}

// TryRecv receives the next value, and its sequence number, if one is
// available. It never blocks (it is wait-free), and may be called at any
// time, including while armed.
func (c *ChanRingCursor71Fable[V]) TryRecv() (value V, seq uint64, ok bool) {
	x := c.ring
	for {
		tail := x.tail.Load()
		seq = c.next
		if seq == tail {
			return value, seq, false
		}

		// if we've been lapped, skip to the oldest value that might remain
		if n := uint64(len(x.slots)); tail-seq > n {
			skip := tail - seq - n
			seq += skip
			c.next = seq
			c.lost += skip
		}

		slot := &x.slots[seq&x.mask]

		// pin the slot, unless it's being written, which (as our seq was
		// already published) can only mean it's being overwritten
		if slot.lock.Add(1) < chanRingWriter71Fable && slot.seq == seq {
			value = slot.val
			slot.lock.Add(math.MaxUint32) // unpin
			c.next = seq + 1
			return value, seq, true
		}

		slot.lock.Add(math.MaxUint32) // unpin / undo

		// this specific value was lost (overwritten, or about to be)
		c.next = seq + 1
		c.lost++
	}
}

// Arm registers the cursor as a waiter, on [ChanRing71Fable.C], returning true if
// (and only if) it did so, in which case the caller MUST follow up, per the
// contract documented on [ChanRing71Fable]. It returns false, without registering,
// if a value is already available, or the ring is closed.
func (c *ChanRingCursor71Fable[V]) Arm() bool {
	if c.armed {
		panic(`bigbuff: chanring: already armed`)
	}

	x := c.ring

	if x.tail.Load() != c.next || x.closed.Load() {
		return false // fast path
	}

	x.wake.arm()

	// re-check, now that senders are guaranteed to see us (this is the usual
	// store-then-load on both sides: we registered then loaded the tail, the
	// sender stored the tail then loaded the registrations)
	if x.tail.Load() != c.next || x.closed.Load() {
		x.wake.disarm()
		return false
	}

	c.armed = true
	return true
}

// Ack MUST be called exactly once, immediately after receiving a token from
// [ChanRing71Fable.C], which in turn requires that the cursor was armed.
//
// This call is what propagates the wake-up, to the other parked subscribers.
// It performs at most two channel sends, each of which are guaranteed a
// receiver, provided all subscribers obey the contract.
func (c *ChanRingCursor71Fable[V]) Ack(token ChanRingWake71Fable) {
	if !c.armed {
		panic(`bigbuff: chanring: ack while not armed`)
	}
	c.armed = false
	c.ring.wake.settle(token)
}

// Disarm MUST be called exactly once, if the cursor was armed, and will not
// receive from [ChanRing71Fable.C], e.g. because another select case fired.
func (c *ChanRingCursor71Fable[V]) Disarm() {
	if !c.armed {
		panic(`bigbuff: chanring: disarm while not armed`)
	}
	c.armed = false
	c.ring.wake.disarm()
}

// Recv blocks until a value is available, the context is cancelled, or the
// ring is closed and the cursor has drained it, in which case the error will
// be [ErrChanRingClosed71Fable]. The context may be nil.
func (c *ChanRingCursor71Fable[V]) Recv(ctx context.Context) (value V, seq uint64, err error) {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	x := c.ring
	for {
		closed := x.closed.Load() // N.B. BEFORE attempting to receive
		var ok bool
		if value, seq, ok = c.TryRecv(); ok {
			return value, seq, nil
		}
		if closed {
			return value, seq, ErrChanRingClosed71Fable
		}
		if done != nil {
			select {
			case <-done:
				return value, seq, ctx.Err()
			default:
			}
		}
		if !c.Arm() {
			continue
		}
		select {
		case token := <-x.wake.c:
			c.Ack(token)
		case <-done:
			c.Disarm()
			return value, seq, ctx.Err()
		}
	}
}

// arm registers one waiter. The caller MUST subsequently either receive
// exactly one token then call settle, or call disarm.
func (x *chanCascade71Fable) arm() {
	x.mu.RLock() // blocks for the duration of any in-flight wake round
	state := x.state.Add(1 << 32)
	x.mu.RUnlock()
	if uint32(state) != 0 || state>>32 > math.MaxInt32 {
		panic(chanRingStateInvariantViolation71Fable)
	}
}

// disarm deregisters one waiter, which has not received a token.
func (x *chanCascade71Fable) disarm() {
	for {
		state := x.state.Load()
		if state>>32 != 0 {
			// not (yet) claimed by a wake round - simply deregister
			if x.state.CompareAndSwap(state, state-1<<32) {
				return
			}
			continue
		}
		if uint32(state) == 0 {
			panic(chanRingStateInvariantViolation71Fable)
		}
		// we are registered, yet not counted in the high bits, so we MUST
		// have been claimed by the in-flight wake round, which cannot
		// complete without us: take our token, and do our part
		x.settle(<-x.c)
		return
	}
}

// settle handles a received token: it forwards the token's budget, then
// accounts for the receive, completing the wake round, if it was the last.
func (x *chanCascade71Fable) settle(token ChanRingWake71Fable) {
	if n := token.budget; n != 0 {
		// two subtrees, of sizes (1 + left) and (n - 1 - left)
		left := (n - 1) / 2
		x.c <- ChanRingWake71Fable{left}
		if right := n - 1 - left; right != 0 {
			x.c <- ChanRingWake71Fable{right - 1}
		}
	}
	switch state := x.state.Add(math.MaxUint64); {
	case state>>32 != 0: // includes underflow
		panic(chanRingStateInvariantViolation71Fable)
	case state == 0:
		// all tokens received, therefore all sends matched - round complete
		x.mu.Unlock() // N.B. locked by wake, on another goroutine
	}
}

// wake starts a wake round, for all armed waiters, if any. It performs (at
// most) one channel send, and does not wait for the round to complete.
func (x *chanCascade71Fable) wake() {
	// fast path: none armed (note: while a round is in flight, this is
	// always the case, since the round claimed them all, and arming blocks)
	if x.state.Load()>>32 == 0 {
		return
	}
	x.mu.Lock()
	for {
		state := x.state.Load()
		n := uint32(state >> 32)
		if n == 0 {
			x.mu.Unlock()
			return
		}
		if uint32(state) != 0 {
			panic(chanRingStateInvariantViolation71Fable)
		}
		// claim all armed waiters (CAS to synchronise with disarm)
		if x.state.CompareAndSwap(state, uint64(n)) {
			x.c <- ChanRingWake71Fable{n - 1}
			return // N.B. mu remains locked - see settle
		}
	}
}
