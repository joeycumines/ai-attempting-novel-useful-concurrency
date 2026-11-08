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
	"iter"
	"math"
	"runtime"
	"sync/atomic"
)

// subscription lifecycle states, kept in Subscription.state.
const (
	multicastSubFresh  uint32 = iota // zero value; never registered
	multicastSubActive               // registered; mailbox delivering
	multicastSubDead                 // unsubscribed; quit channel closed
	multicastSubUnlinked             // tombstone physically unlinked; grave released
)

// Multicast.state is a single atomic word packing two 32-bit counters:
//
//	hi: live subscription count (incremented before the node becomes reachable)
//	lo: "graves": tombstoned subscriptions still reachable from head
//
// The packed deltas below are borrow-free by invariant: count is only
// decremented after winning the active->dead CAS (so count >= 1), and graves
// is only decremented after observing a tombstone (so graves >= 1, because
// every tombstone is preceded, in the atomic total order, by its grave
// increment). This is the same single-word discipline as ChanCaster's
// tracker word.
const (
	multicastCountInc = uint64(1) << 32          // count + 1
	multicastCountDec = (^uint64(0) >> 32) << 32 // count - 1, in the high word
	multicastGraveInc = uint64(1)                // graves + 1
	multicastGraveDec = ^uint64(0)               // graves - 1: a whole-word decrement cannot carry into the count
)

// delivery and reaping tuning.
const (
	multicastDeliverySpins = 16 // attempts before parking on a full mailbox
	multicastDeliveryYield = 4  // pure-spin attempts before Gosched between attempts
	multicastReapLimit     = 64 // bounded opportunistic unlink walk per Unsubscribe
)

// Multicast is a select-capable broadcast hub: every subscription owns a
// private channel that can be used directly in select statements, while
// publishing costs, allocation, and churn behaviour remain in the same class
// as the package's shared-channel designs (ChanCaster, ChanPubSub) and the
// linear "callback channel" design.
//
// Where ChanPubSub multiplexes all subscribers over a single ping/pong
// channel pair (which is why its subscribers cannot safely place that channel
// in a select with other cases), Multicast inverts the topology: the hub is a
// lock-free registry of subscriptions, each with its own bounded mailbox.
// The value of that inversion is precisely the characteristic the callback
// design buys at the cost of a subscriber slice and a lock: first-class select
// integration per consumer. Unlike the callback design, subscribing and
// unsubscribing are lock-free and (amortized) O(1), so both consumer churn and
// large consumer counts scale.
//
// The implementation contains no mutexes, condition variables, pools, or
// background goroutines. Its moving parts:
//
//   - Subscriptions form an intrusive, push-only, singly-linked list (a
//     lock-free stack) with lazy tombstone unlinking. Subscribe is a CAS.
//     Unsubscribe is a CAS plus a bounded opportunistic unlink walk
//     (multicastReapLimit nodes). There is no ABA problem because
//     subscription nodes are never reused; reclamation is left to the garbage
//     collector, which also makes in-flight deliveries to just-unlinked nodes
//     harmless.
//
//   - Publishing walks the list newest-first, handing the value to every
//     live subscription with a channel send into its mailbox. For the
//     lossless Send, completion of that send is the acknowledgement: no pong
//     counter, no sync.Cond, and (with mailbox capacity >= 1) consumers drain
//     while the publisher walks on, pipelining where ChanPubSub barriers.
//     Backpressure is therefore per-subscriber and identical in kind to a
//     per-subscriber buffered channel pipeline.
//
//   - All hub bookkeeping lives in one atomic word (state): the high 32 bits
//     count live subscriptions, the low 32 bits count graves (tombstoned
//     nodes still reachable from head). A publish against an idle hub is a
//     single atomic load, the same zero-state fast path as ChanCaster. Graves
//     make unlinking amortized: every tombstone guarantees its node is
//     unlinked by the next traversal (Send, TrySend, Close, or the
//     subscriber's own bounded reap), and the packed arithmetic is
//     borrow-free by construction (see the state constants).
//
//   - Delivery spins briefly (a few attempts, then Gosched between attempts)
//     before parking, converting short consumer latencies into cheap
//     handoffs and paying the runtime parking cost only for genuinely slow
//     consumers.
//
// Guarantees, and the trade-offs that buy them:
//
//   - Lossless: every Send hands its value to every subscription live for
//     the duration of the call; the return value counts the deliveries (a
//     best-effort figure under concurrent unsubscribe). TrySend is the lossy
//     sibling: it never blocks and reports per-subscriber drops instead.
//   - Send may block while any live subscriber's mailbox is full
//     (backpressure, like ChanPubSub). A subscriber that unsubscribes, or
//     Close, always unblocks it. Send panics only if the hub was closed
//     before the call.
//   - Concurrent Send calls are supported and are not serialized;
//     per-subscriber relative order of two concurrent sends is undefined.
//   - Subscribe linearizes at the head push: a Send racing a Subscribe may
//     miss the new subscriber. (ChanPubSub closes this window with its
//     sending lock; Multicast spends that lock to make churn lock-free.)
//   - Unsubscribe is idempotent and never blocks, including when called
//     from inside an iterator yield or a select case.
//   - Zero allocations per Send/TrySend. Per subscription: two small runtime
//     channel allocations (mailbox and quit), plus the handle for Subscribe -
//     or none, by Registering a Subscription embedded in caller-owned memory.
//   - Close is idempotent, unblocks everything, guarantees reclamation of
//     all registered nodes, and ends iterators gracefully.
//
// The zero value is ready to use (with unbuffered mailboxes). Neither
// Multicast nor Subscription may be copied after first use (both contain
// atomics; go vet's copylocks check enforces it).
//
// Reentrancy limit: a Send issued from inside a subscription's iterator yield
// re-enters that subscription's mailbox; as with any bounded mailbox this
// deadlocks only once the re-entrant sends exceed the mailbox capacity.
type Multicast27Mistral[C ~chan V, V any] struct {
	head       atomic.Pointer[Multicast27Subscription[C, V]]
	state      atomic.Uint64
	closed     atomic.Bool
	mailboxCap int
}

// Subscription is a handle to one Multicast consumer. It is its own list node
// (intrusive), so subscribing with Register costs no node allocation: embed
// one in a longer-lived struct, or take the heap allocation from Subscribe.
//
// The intended consumption pattern is a select over C() (the private mailbox,
// with the capacity configured by NewMulticast) and Done():
//
//	s := m.Subscribe()
//	defer s.Unsubscribe()
//	for {
//		select {
//		case v := <-s.C():
//			handle(v)
//		case <-s.Done():
//			return
//		case <-ctx.Done():
//			return
//		}
//	}
//
// After Unsubscribe (or Close), Done is closed and C() must not be used to
// receive further values; anything already in flight into the mailbox is
// abandoned and never observed by a well-behaved subscriber. A Subscription
// is single-use: Register accepts only fresh (never-registered) values, and
// Unsubscribe on a fresh one panics. Must not be copied after first use.
type Multicast27Subscription[C ~chan V, V any] struct {
	m     *Multicast27Mistral[C, V]
	ch    C
	quit  chan struct{}
	next  atomic.Pointer[Multicast27Subscription[C, V]]
	state atomic.Uint32
}

// NewMulticast returns a Multicast whose subscription mailboxes have the
// capacity of the exemplar channel c. The exemplar itself is neither used nor
// retained; only its type and cap are read.
//
// The mailbox capacity is the hub's latency/throughput policy knob:
//
//	0    per-subscriber rendezvous; Send blocks until each subscriber
//	     receives (the closest analogue of ChanPubSub's synchronous rounds).
//	1    (recommended) one-deep mailbox; subscribers drain concurrently with
//	     the publisher's walk (pipelining), and the slowest consumer sets
//	     the pace.
//	n>1  jitter absorption for bursty consumers, at n*sizeof(V) per
//	     subscription.
//
// A nil exemplar is valid and selects capacity 0.
func NewMulticast27Mistral[C ~chan V, V any](c C) *Multicast27Mistral[C, V] {
	x := new(Multicast27Mistral[C, V])
	x.mailboxCap = cap(c)
	return x
}

// Subscribe registers and returns a fresh subscription. It panics if the
// multicast is closed (including a close racing the call).
func (m *Multicast27Mistral[C, V]) Subscribe() *Multicast27Subscription[C, V] {
	s := new(Multicast27Subscription[C, V])
	m.Register(s)
	return s
}

// Register subscribes with caller-owned storage (intrusive subscribe: no
// node allocation). s must be fresh (its zero value, or never-registered
// memory); it panics on a reused or nil subscription, or if the multicast is
// closed, including a close racing the call.
func (m *Multicast27Mistral[C, V]) Register(s *Multicast27Subscription[C, V]) {
	if s == nil {
		panic(`bigbuff: multicast: register: nil subscription`)
	}
	if m.closed.Load() {
		panic(`bigbuff: multicast: register on closed multicast`)
	}
	if !m.register(s) {
		panic(`bigbuff: multicast: register on closed multicast`)
	}
}

// register is the non-panicking core of Register. It returns false iff the
// multicast became closed while registering, in which case the racing
// subscription has been retired and reaped and must not be used.
func (m *Multicast27Mistral[C, V]) register(s *Multicast27Subscription[C, V]) (ok bool) {
	if !s.state.CompareAndSwap(multicastSubFresh, multicastSubActive) {
		panic(`bigbuff: multicast: register: subscription is not fresh`)
	}
	s.m = m
	s.ch = make(C, m.mailboxCap)
	s.quit = make(chan struct{})
	s.next.Store(nil)
	if n := m.state.Add(multicastCountInc); uint32(n>>32) > math.MaxInt32 {
		panic(`bigbuff: multicast: register: too many subscriptions`)
	}
	// count is incremented BEFORE the push, so state == 0 implies no live
	// node is reachable and Send's fast path is sound.
	for {
		h := m.head.Load()
		s.next.Store(h)
		if m.head.CompareAndSwap(h, s) {
			break
		}
	}
	if m.closed.Load() {
		// Lost a race with Close: retire and reap ourselves so no live node
		// is orphaned behind the closing pass (rare, correctness-first).
		s.retire()
		m.reap(s, math.MaxInt)
		return false
	}
	return true
}

// Send hands value to every live subscription, blocking (backpressure) while
// any live subscriber's mailbox is full, and returns the number of deliveries.
// It never allocates. It panics only if the multicast was closed before the
// call; a close racing an in-flight Send unblocks it and it returns normally.
func (m *Multicast27Mistral[C, V]) Send(value V) (delivered int) {
	if m.closed.Load() {
		panic(`bigbuff: multicast: send on closed multicast`)
	}
	if m.state.Load() == 0 {
		return 0 // fast path: no live subscriptions and no graves
	}
	anchor := &m.head
	for {
		s := anchor.Load()
		if s == nil {
			return delivered
		}
		if s.state.Load() == multicastSubDead {
			if anchor.CompareAndSwap(s, s.next.Load()) && s.state.CompareAndSwap(multicastSubDead, multicastSubUnlinked) {
				m.state.Add(multicastGraveDec)
			}
			// Re-read from the same anchor whether or not the unlink landed:
			// a failed CAS means a concurrent reaper already replaced it (so
			// the successor must be re-read), and advancing would skip a live
			// node. Not advancing on success is required too: the successor may
			// itself be a tombstone that only this traversal will unlink.
			continue
		}
		if !s.deliver(value) {
			// Unsubscribed mid-delivery. Retire left the node tombstoned but
			// still linked; unlink it here (rather than advancing over it) so
			// its grave is released exactly once. The node stays tombstoned,
			// so a failed CAS simply re-reads the same anchor.
			if anchor.CompareAndSwap(s, s.next.Load()) && s.state.CompareAndSwap(multicastSubDead, multicastSubUnlinked) {
				m.state.Add(multicastGraveDec)
			}
			continue
		}
		delivered++
		anchor = &s.next
	}
}

// TrySend is the lossy, non-blocking sibling of Send: every live subscription
// either receives the value or is counted as dropped, and nothing ever parks.
// It never allocates. It panics if the multicast was closed before the call.
func (m *Multicast27Mistral[C, V]) TrySend(value V) (delivered, dropped int) {
	if m.closed.Load() {
		panic(`bigbuff: multicast: try-send on closed multicast`)
	}
	if m.state.Load() == 0 {
		return 0, 0 // fast path: no live subscriptions and no graves
	}
	anchor := &m.head
	for {
		s := anchor.Load()
		if s == nil {
			return delivered, dropped
		}
		if s.state.Load() == multicastSubDead {
			if anchor.CompareAndSwap(s, s.next.Load()) && s.state.CompareAndSwap(multicastSubDead, multicastSubUnlinked) {
				m.state.Add(multicastGraveDec)
			}
			continue
		}
		select {
		case s.ch <- value:
			delivered++
		case <-s.quit:
			dropped++
		default:
			dropped++
		}
		anchor = &s.next
	}
}

// Close unsubscribes every subscription (their Done channels fire), unblocks
// any blocked Send, guarantees reclamation of all registered nodes, and ends
// iterators gracefully. It is idempotent and safe to call concurrently.
func (m *Multicast27Mistral[C, V]) Close() {
	if !m.closed.CompareAndSwap(false, true) {
		return // idempotent
	}
	for {
		s := m.head.Load()
		if s == nil {
			return
		}
		s.retire()
		if m.head.CompareAndSwap(s, s.next.Load()) && s.state.CompareAndSwap(multicastSubDead, multicastSubUnlinked) {
			m.state.Add(multicastGraveDec)
		}
	}
}

// Len returns the number of live subscriptions; approximate under concurrent
// use.
func (m *Multicast27Mistral[C, V]) Len() int {
	return int(int32(uint32(m.state.Load() >> 32)))
}

// SubscribeContext returns an iterator over the values delivered to a fresh
// subscription, lazily created on the first range and always unsubscribed.
// Iteration ends when the context is done, the subscription is unsubscribed,
// or the multicast is closed; unlike ChanPubSub, a close ends the iteration
// gracefully rather than panicking.
func (m *Multicast27Mistral[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		if yield == nil {
			panic(`bigbuff: multicast: iterator yield func is nil`)
		}
		if m.closed.Load() {
			return
		}
		s := new(Multicast27Subscription[C, V])
		if !m.register(s) {
			return // raced with Close; nothing was ever delivered
		}
		defer s.Unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.quit:
				return // unsubscribed, or the multicast was closed
			case v, ok := <-s.ch:
				if !ok {
					return // defensive: subscription channels are never closed
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// C returns the subscription's private mailbox channel, for use in selects.
// It panics on a fresh (never-registered) subscription, and must not be
// received from after Done fires.
func (s *Multicast27Subscription[C, V]) C() C {
	if s.state.Load() == multicastSubFresh {
		panic(`bigbuff: multicast: subscription is not registered`)
	}
	return s.ch
}

// Done returns a channel closed exactly once, when the subscription is
// unsubscribed or the multicast is closed. Always include it in selects that
// include C(). It panics on a fresh (never-registered) subscription.
func (s *Multicast27Subscription[C, V]) Done() <-chan struct{} {
	if s.state.Load() == multicastSubFresh {
		panic(`bigbuff: multicast: subscription is not registered`)
	}
	return s.quit
}

// Unsubscribe removes the subscription from its multicast. It is idempotent,
// never blocks, and unblocks any Send currently parked on this subscription's
// mailbox. It panics on a fresh (never-registered) subscription.
func (s *Multicast27Subscription[C, V]) Unsubscribe() {
	switch s.state.Load() {
	case multicastSubDead:
		return // idempotent
	case multicastSubFresh:
		panic(`bigbuff: multicast: unsubscribe: subscription is not registered`)
	}
	// Note: Multicast27Subscription lives in package bigbuff, so the constants
	// multicastSubFresh/Active/Dead are shared with the other implementations in
	// this flat package; they are identical in meaning.
	s.retire()
	// Guarantee reclamation: the bounded opportunistic walk first (cheap when
	// the node is near the head), then an unbounded walk so the tombstone is
	// always physically removed and its grave released. Without this the live
	// count never returns to zero under churn, which defeats the single-word
	// accounting the design is built on.
	if !s.m.reap(s, multicastReapLimit) {
		s.m.reap(s, math.MaxInt)
	}
}

// retire performs the subscription's tombstone exactly once across all
// racing callers (Unsubscribe, Close, and Register's race with Close). The
// grave is counted BEFORE the tombstone CAS becomes visible: within the
// atomic total order that makes every later graves-decrement borrow-free.
func (s *Multicast27Subscription[C, V]) retire() {
	m := s.m
	m.state.Add(multicastGraveInc)
	if s.state.CompareAndSwap(multicastSubActive, multicastSubDead) {
		// The subscription becomes a tombstone: the live count drops by one
		// and a grave is left behind by exactly this subscription. The grave
		// is released when the node is physically unlinked (Unsubscribe's
		// bounded reap, Send's opportunistic unlink, or Close's full pass).
		m.state.Add(multicastCountDec)
		close(s.quit)
	} else {
		m.state.Add(multicastGraveDec) // lost the race; un-count the grave
	}
}

// deliver hands value to one subscription's mailbox, returning false iff the
// subscription was unsubscribed before the value was accepted. It spins
// briefly (attempts plus Gosched) to convert short consumer latencies into
// cheap handoffs, then parks on the full mailbox or the subscription's quit
// channel, whichever resolves first.
func (s *Multicast27Subscription[C, V]) deliver(value V) bool {
	for i := 0; i < multicastDeliverySpins; i++ {
		select {
		case s.ch <- value:
			return true
		default:
		}
		if s.state.Load() == multicastSubDead {
			return false
		}
		if i >= multicastDeliveryYield {
			runtime.Gosched()
		}
	}
	select {
	case s.ch <- value:
		return true
	case <-s.quit:
		return false
	}
}

// reap opportunistically unlinks dead subscriptions during a bounded walk
// from the head of the list, stopping early once target is unlinked. It
// returns true iff target was unlinked. Nodes not unlinked within the limit
// are guaranteed reclamation by the next full traversal (their graves keep
// state non-zero, so the Send fast path never skips cleanup).
func (m *Multicast27Mistral[C, V]) reap(target *Multicast27Subscription[C, V], limit int) (unlinked bool) {
	anchor := &m.head
	for steps := 0; steps < limit; {
		s := anchor.Load()
		if s == nil {
			return false
		}
		if s.state.Load() == multicastSubDead {
			if anchor.CompareAndSwap(s, s.next.Load()) && s.state.CompareAndSwap(multicastSubDead, multicastSubUnlinked) {
				m.state.Add(multicastGraveDec)
				if s == target {
					return true
				}
			}
			// Do not advance on failure: the head of this position may have
			// been unlinked by a concurrent reaper, so re-read it against the
			// same anchor. Advancing here would skip a live node and leave the
			// target permanently linked.
			steps++
			continue
		}
		anchor = &s.next
		steps++
	}
	return false
}
