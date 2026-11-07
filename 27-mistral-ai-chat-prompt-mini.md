No Go toolchain exists in this sandbox, so this is delivered desk-checked rather than test-verified — run `go test -race ./...` and `go vet .` on your side to confirm (race-safety is by construction: every shared field is an atomic or a channel).

## What I built, and why

The complementary gap you identified is exactly right: `ChanPubSub` multiplexes everyone over one ping/pong channel pair, so its "channel" can't be safely placed in a `select` with other cases; the callback-channel design gets select support but pays a locked subscriber slice that degrades under churn and large N. The implementation below — **`Multicast`** — inverts the topology. The hub is a lock-free registry of *subscriptions*, each owning a private, capacity-configured mailbox channel that is first-class in a `select`. It contains **no mutexes, no condition variables, no pools, no background goroutines** — the whole thing is atomics plus channel sends.

The novel mechanics, in order of what I think makes them interesting:

1. **A single packed `state` word (`[count:hi32 | graves:lo32]`)** — the same single-word discipline as `ChanCaster`'s tracker, but carrying a second meaning: a publish against an idle hub is *one atomic load* (`state == 0` means no live subscribers **and** no graves), and graves are the amortization device that guarantees every tombstone is unlinked by the next traversal, borrow-free by construction (the grave is counted *before* the tombstone becomes visible, so the atomic total order forbids a borrow on the decrement — the same flavour of invariant as your `tracker` arithmetic).
2. **Mailbox-send-as-ack.** For the lossless `Send`, completion of the per-subscriber channel send *is* the acknowledgement — the pong counter, the `sync.Cond`, and their deadlock surface are deleted entirely. Slow subscribers apply per-subscriber backpressure, and (mailbox cap ≥ 1) consumers drain *concurrently with the publisher's walk*, pipelining where `ChanPubSub` barriers on `pongN == 0`.
3. **An intrusive, push-only lock-free list with lazy tombstone unlinking, reclaimed by the GC.** Nodes are never reused, so there is no ABA problem, no epoch reclamation, no hazard pointers — in-flight deliveries to a just-unlinked node are harmless. Subscribe is a CAS; Unsubscribe is a CAS plus a *bounded* opportunistic unlink walk (head-fast-path common under LIFO churn). This is what makes churn and large N scale where the callback design's lock does not.
4. **Spin-then-`Gosched`-then-park delivery**, converting short consumer latencies into cheap handoffs and paying runtime parking only for genuinely slow consumers.
5. **Dual modes**: `Send` (lossless, backpressure) and `TrySend` (lossy, non-blocking, per-subscriber drop accounting).

| | ChanCaster | ChanPubSub | callback-channel | **Multicast** |
|---|---|---|---|---|
| alloc per publish | 0 | 0 | 0 | **0** |
| alloc per subscribe | 0 | 0 | chan + node, under lock | 2–3 tiny chans (+handle), or node-free via intrusive `Register` |
| per-subscriber `select` | ✗ (shared chan) | ✗ (unsafe) | ✔ | **✔** |
| churn (sub/unsub) | atomic add | lock-adjacent, cond-coupled | mutex + slice/node churn | **lock-free CAS, amortized O(1)** |
| concurrent publishers | serialized (mutex) | serialized (`sendMu`) | usually locked | **✔, not serialized** |
| idle publish cost | 1 atomic load | 1 atomic load | O(N) loop | **1 atomic load** |
| slow-consumer parking | chan rendezvous | global pong barrier | chan send | spin-then-park, per-subscriber |

`-- multicast.go --`

```go
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
	multicastCountInc = uint64(1) << 32    // count + 1
	multicastCountDec = ^uint64(0) << 32   // count - 1 (count >= 1)
	multicastGraveInc = uint64(1)          // graves + 1
	multicastGraveDec = uint64(^uint32(0)) // graves - 1 (graves >= 1)
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
type Multicast[C ~chan V, V any] struct {
	head       atomic.Pointer[Subscription[C, V]]
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
type Subscription[C ~chan V, V any] struct {
	m     *Multicast[C, V]
	ch    C
	quit  chan struct{}
	next  atomic.Pointer[Subscription[C, V]]
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
func NewMulticastC ~chan V, V any *Multicast[C, V] {
	x := new(Multicast[C, V])
	x.mailboxCap = cap(c)
	return x
}

// Subscribe registers and returns a fresh subscription. It panics if the
// multicast is closed (including a close racing the call).
func (m *Multicast[C, V]) Subscribe() *Subscription[C, V] {
	s := new(Subscription[C, V])
	m.Register(s)
	return s
}

// Register subscribes with caller-owned storage (intrusive subscribe: no
// node allocation). s must be fresh (its zero value, or never-registered
// memory); it panics on a reused or nil subscription, or if the multicast is
// closed, including a close racing the call.
func (m *Multicast[C, V]) Register(s *Subscription[C, V]) {
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
func (m *Multicast[C, V]) register(s *Subscription[C, V]) (ok bool) {
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
func (m *Multicast[C, V]) Send(value V) (delivered int) {
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
			if anchor.CompareAndSwap(s, s.next.Load()) {
				m.state.Add(multicastGraveDec)
			}
			continue
		}
		if !s.deliver(value) {
			continue // unsubscribed mid-delivery; unlink before advancing
		}
		delivered++
		anchor = &s.next
	}
}

// TrySend is the lossy, non-blocking sibling of Send: every live subscription
// either receives the value or is counted as dropped, and nothing ever parks.
// It never allocates. It panics if the multicast was closed before the call.
func (m *Multicast[C, V]) TrySend(value V) (delivered, dropped int) {
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
			if anchor.CompareAndSwap(s, s.next.Load()) {
				m.state.Add(multicastGraveDec)
			}
			continue
		}
		select {
		case s.ch <- value:
			delivered++
		default:
			dropped++
		}
		anchor = &s.next
	}
}

// Close unsubscribes every subscription (their Done channels fire), unblocks
// any blocked Send, guarantees reclamation of all registered nodes, and ends
// iterators gracefully. It is idempotent and safe to call concurrently.
func (m *Multicast[C, V]) Close() {
	if !m.closed.CompareAndSwap(false, true) {
		return // idempotent
	}
	for {
		s := m.head.Load()
		if s == nil {
			return
		}
		s.retire()
		if m.head.CompareAndSwap(s, s.next.Load()) {
			m.state.Add(multicastGraveDec)
		}
	}
}

// Len returns the number of live subscriptions; approximate under concurrent
// use.
func (m *Multicast[C, V]) Len() int {
	return int(int32(uint32(m.state.Load() >> 32)))
}

// SubscribeContext returns an iterator over the values delivered to a fresh
// subscription, lazily created on the first range and always unsubscribed.
// Iteration ends when the context is done, the subscription is unsubscribed,
// or the multicast is closed; unlike ChanPubSub, a close ends the iteration
// gracefully rather than panicking.
func (m *Multicast[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
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
		s := new(Subscription[C, V])
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
func (s *Subscription[C, V]) C() C {
	if s.state.Load() == multicastSubFresh {
		panic(`bigbuff: multicast: subscription is not registered`)
	}
	return s.ch
}

// Done returns a channel closed exactly once, when the subscription is
// unsubscribed or the multicast is closed. Always include it in selects that
// include C(). It panics on a fresh (never-registered) subscription.
func (s *Subscription[C, V]) Done() <-chan struct{} {
	if s.state.Load() == multicastSubFresh {
		panic(`bigbuff: multicast: subscription is not registered`)
	}
	return s.quit
}

// Unsubscribe removes the subscription from its multicast. It is idempotent,
// never blocks, and unblocks any Send currently parked on this subscription's
// mailbox. It panics on a fresh (never-registered) subscription.
func (s *Subscription[C, V]) Unsubscribe() {
	switch s.state.Load() {
	case multicastSubDead:
		return // idempotent
	case multicastSubFresh:
		panic(`bigbuff: multicast: unsubscribe: subscription is not registered`)
	}
	s.retire()
	s.m.reap(s, multicastReapLimit)
}

// retire performs the subscription's tombstone exactly once across all
// racing callers (Unsubscribe, Close, and Register's race with Close). The
// grave is counted BEFORE the tombstone CAS becomes visible: within the
// atomic total order that makes every later graves-decrement borrow-free.
func (s *Subscription[C, V]) retire() {
	m := s.m
	m.state.Add(multicastGraveInc)
	if s.state.CompareAndSwap(multicastSubActive, multicastSubDead) {
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
func (s *Subscription[C, V]) deliver(value V) bool {
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
func (m *Multicast[C, V]) reap(target *Subscription[C, V], limit int) (unlinked bool) {
	anchor := &m.head
	for i := 0; i < limit; i++ {
		s := anchor.Load()
		if s == nil {
			return false
		}
		if s.state.Load() == multicastSubDead {
			if anchor.CompareAndSwap(s, s.next.Load()) {
				m.state.Add(multicastGraveDec)
				if s == target {
					return true
				}
			}
			continue
		}
		anchor = &s.next
	}
	return false
}
```

## Tests and benchmarks

`-- multicast_test.go --`

```go
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
	"sync"
	"testing"
	"time"
)

func multicastMustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}()
	f()
}

func TestMulticastSendReceive(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	const subs, values = 3, 100
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := m.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			for n := 0; n < values; n++ {
				select {
				case v := <-s.C():
					if v != n {
						t.Errorf("expected %d, got %d", n, v)
						return
					}
				case <-s.Done():
					t.Error("done prematurely")
					return
				}
			}
		}()
	}
	for n := 0; n < values; n++ {
		if d := m.Send(n); d != subs {
			t.Fatalf("Send(%d) delivered %d, want %d", n, d, subs)
		}
	}
	wg.Wait()
}

func TestMulticastSelect(t *testing.T) {
	m := NewMulticast(make(chan string, 1))
	s := m.Subscribe()
	defer s.Unsubscribe()
	go func() { m.Send("hello") }()
	select {
	case v := <-s.C():
		if v != "hello" {
			t.Fatalf("got %q", v)
		}
	case <-s.Done():
		t.Fatal("done prematurely")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for value")
	}
}

func TestMulticastNoSubscribers(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	if m.Len() != 0 {
		t.Fatal("Len != 0")
	}
	if n := m.Send(1); n != 0 {
		t.Fatalf("Send returned %d", n)
	}
	if d, dr := m.TrySend(1); d != 0 || dr != 0 {
		t.Fatalf("TrySend returned %d, %d", d, dr)
	}
	var s Subscription[chan int, int]
	m.Register(&s)
	if m.Len() != 1 {
		t.Fatal("Len != 1")
	}
	s.Unsubscribe()
	if m.Len() != 0 {
		t.Fatal("Len != 0 after unsubscribe")
	}
	if n := m.Send(2); n != 0 {
		t.Fatalf("Send returned %d", n)
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0 (graves must be cleaned)", st)
	}
}

func TestMulticastUnsubscribeUnblocksSend(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	s := m.Subscribe()
	if m.Send(1) != 1 {
		t.Fatal("first send should fill the mailbox")
	}
	done := make(chan int, 1)
	go func() { done <- m.Send(2) }()
	select {
	case n := <-done:
		t.Fatalf("Send returned %d while mailbox full", n)
	case <-time.After(50 * time.Millisecond):
	}
	s.Unsubscribe()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("Send returned %d, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not unblock after Unsubscribe")
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0", st)
	}
}

func TestMulticastClose(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	s1 := m.Subscribe()
	s2 := m.Subscribe()
	if m.Send(1) != 2 {
		t.Fatal("expected 2 deliveries")
	}
	done := make(chan int, 1)
	go func() { done <- m.Send(2) }() // blocks: mailboxes full
	time.Sleep(50 * time.Millisecond)
	m.Close()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("Send returned %d after Close, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not unblock after Close")
	}
	select {
	case <-s1.Done():
	default:
		t.Fatal("s1.Done not closed")
	}
	select {
	case <-s2.Done():
	default:
		t.Fatal("s2.Done not closed")
	}
	multicastMustPanic(t, func() { m.Send(3) })
	multicastMustPanic(t, func() { m.TrySend(3) })
	multicastMustPanic(t, func() { m.Subscribe() })
	m.Close() // idempotent
	m.Close()
	if m.Len() != 0 {
		t.Fatal("Len != 0 after Close")
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0 after Close", st)
	}
}

func TestMulticastTrySend(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	slow := m.Subscribe() // never drains
	fast := m.Subscribe()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-fast.C():
			}
		}
	}()
	const attempts = 100
	var delivered, dropped int
	for i := 0; i < attempts; i++ {
		d, dr := m.TrySend(i)
		delivered += d
		dropped += dr
	}
	if delivered+dropped != 2*attempts {
		t.Fatalf("delivered(%d)+dropped(%d) != %d", delivered, dropped, 2*attempts)
	}
	if dropped == 0 {
		t.Fatal("expected drops from the slow subscriber")
	}
	close(stop)
	wg.Wait()
	slow.Unsubscribe()
	fast.Unsubscribe()
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0", st)
	}
}

func TestMulticastConcurrentSends(t *testing.T) {
	const senders, each, subs = 2, 100, 3
	m := NewMulticast(make(chan int, senders*each+8)) // ample: deterministic totals
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := m.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			var sum int
			for n := 0; n < senders*each; n++ {
				select {
				case v := <-s.C():
					sum += v
				case <-s.Done():
					t.Error("done prematurely")
					return
				}
			}
			if want := senders * each * (each - 1) / 2; sum != want {
				t.Errorf("sum = %d, want %d", sum, want)
			}
		}()
	}
	var swg sync.WaitGroup
	for i := 0; i < senders; i++ {
		swg.Add(1)
		go func() {
			defer swg.Done()
			for n := 0; n < each; n++ {
				m.Send(n)
			}
		}()
	}
	swg.Wait()
	wg.Wait()
}

func TestMulticastChurn(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	const churners, iterations = 16, 100
	var churnWG sync.WaitGroup
	churnDone := make(chan struct{})
	for i := 0; i < churners; i++ {
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			for j := 0; j < iterations; j++ {
				s := m.Subscribe()
				for n := 0; n < 3; n++ {
					select {
					case <-s.C():
					case <-s.Done():
						return
					}
				}
				s.Unsubscribe()
			}
		}()
	}
	go func() { churnWG.Wait(); close(churnDone) }()
	var sendWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		sendWG.Add(1)
		go func() {
			defer sendWG.Done()
			for {
				select {
				case <-churnDone:
					return
				default:
					m.Send(0)
				}
			}
		}()
	}
	sendWG.Wait()
	m.Close()
	if m.Len() != 0 {
		t.Fatalf("Len = %d after churn", m.Len())
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x after churn + Close, want 0", st)
	}
}

func TestMulticastSubscribeContext(t *testing.T) {
	m := NewMulticast(make(chan int, 16))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for n := 0; n < 5; n++ {
			m.Send(n)
		}
	}()
	var got []int
	for v := range m.SubscribeContext(ctx) {
		got = append(got, v)
		if len(got) == 5 {
			break
		}
	}
	if len(got) != 5 {
		t.Fatalf("got %d values, want 5", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("got[%d] = %d, want %d", i, v, i)
		}
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d after break", m.Len())
	}
	ended := make(chan int, 1)
	iterating := make(chan struct{})
	go func() {
		close(iterating)
		n := 0
		for range m.SubscribeContext(ctx) {
			n++
		}
		ended <- n
	}()
	<-iterating
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case n := <-ended:
		if n != 0 {
			t.Fatalf("iterator received %d values after cancel", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("iterator did not end on ctx cancel")
	}
}

func TestMulticastSubscriptionPanics(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	var s Subscription[chan int, int]
	multicastMustPanic(t, func() { s.Unsubscribe() })
	multicastMustPanic(t, func() { _ = s.C() })
	multicastMustPanic(t, func() { _ = s.Done() })
	m.Register(&s)
	if s.C() == nil || s.Done() == nil {
		t.Fatal("nil channels after register")
	}
	multicastMustPanic(t, func() { m.Register(&s) }) // not fresh
	s.Unsubscribe()
	s.Unsubscribe() // idempotent
	if s.C() == nil {
		t.Fatal("mailbox still accessible after unsubscribe")
	}
}

func TestMulticastMailboxCapacity(t *testing.T) {
	m := NewMulticast(make(chan int, 3))
	s := m.Subscribe()
	for i := 0; i < 3; i++ {
		if m.Send(i) != 1 {
			t.Fatalf("Send(%d) should not block", i)
		}
	}
	done := make(chan int, 1)
	go func() { done <- m.Send(3) }() // blocks: fourth value
	time.Sleep(50 * time.Millisecond)
	select {
	case n := <-done:
		t.Fatalf("Send returned %d with a full mailbox", n)
	default:
	}
	select {
	case v := <-s.C():
		if v != 0 {
			t.Fatalf("drained %d, want 0", v)
		}
	default:
		t.Fatal("mailbox should not be empty")
	}
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("Send returned %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not unblock after drain")
	}
	s.Unsubscribe()
}

func TestMulticastUnbufferedMailbox(t *testing.T) {
	m := NewMulticast((chan int)(nil)) // rendezvous mode
	s := m.Subscribe()
	received := make(chan int, 1)
	go func() { received <- (<-s.C()) }()
	if m.Send(42) != 1 {
		t.Fatal("expected 1 delivery")
	}
	select {
	case v := <-received:
		if v != 42 {
			t.Fatalf("got %d, want 42", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rendezvous did not complete")
	}
	s.Unsubscribe()
}

func TestMulticastZeroValue(t *testing.T) {
	var m Multicast[chan int, int]
	s := m.Subscribe()
	go func() { m.Send(1) }()
	if v := <-s.C(); v != 1 {
		t.Fatalf("got %d, want 1", v)
	}
	s.Unsubscribe()
	m.Close()
	multicastMustPanic(t, func() { m.Send(2) })
}

func TestMulticastGraveAccounting(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	const n = 100
	subs := make([]*Subscription[chan int, int], 0, n)
	for i := 0; i < n; i++ {
		subs = append(subs, m.Subscribe())
	}
	if m.Len() != n {
		t.Fatalf("Len = %d, want %d", m.Len(), n)
	}
	for _, s := range subs {
		s.Unsubscribe()
	}
	if m.Len() != 0 {
		t.Fatal("Len != 0 after unsubscribing all")
	}
	if d := m.Send(0); d != 0 {
		t.Fatalf("Send returned %d", d)
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0 (graves must be fully cleaned)", st)
	}
}

func TestMulticastConcurrentClose(t *testing.T) {
	m := NewMulticast(make(chan int, 1))
	subs := make([]*Subscription[chan int, int], 64)
	for i := range subs {
		subs[i] = m.Subscribe()
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.Close() }()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, s := range subs {
				s.Unsubscribe()
			}
		}()
	}
	wg.Wait()
	m.Close()
	if m.Len() != 0 {
		t.Fatal("Len != 0")
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0", st)
	}
	for _, s := range subs {
		select {
		case <-s.Done():
		default:
			t.Fatal("Done not closed")
		}
	}
}

func benchmarkMulticastSend(b *testing.B, subs int) {
	m := NewMulticast(make(chan int, 1))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := m.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-s.C():
				}
			}
		}()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Send(i)
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
	m.Close()
}

func BenchmarkMulticastSend1(b *testing.B)  { benchmarkMulticastSend(b, 1) }
func BenchmarkMulticastSend4(b *testing.B)  { benchmarkMulticastSend(b, 4) }
func BenchmarkMulticastSend16(b *testing.B) { benchmarkMulticastSend(b, 16) }
func BenchmarkMulticastSend64(b *testing.B) { benchmarkMulticastSend(b, 64) }

func benchmarkCallbackChannelSend(b *testing.B, subs int) {
	chs := make([]chan int, subs)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range chs {
		chs[i] = make(chan int, 1)
		wg.Add(1)
		go func(ch chan int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-ch:
				}
			}
		}(chs[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ch := range chs {
			ch <- i
		}
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}

func BenchmarkCallbackChannelSend1(b *testing.B)  { benchmarkCallbackChannelSend(b, 1) }
func BenchmarkCallbackChannelSend4(b *testing.B)  { benchmarkCallbackChannelSend(b, 4) }
func BenchmarkCallbackChannelSend16(b *testing.B) { benchmarkCallbackChannelSend(b, 16) }
func BenchmarkCallbackChannelSend64(b *testing.B) { benchmarkCallbackChannelSend(b, 64) }

func BenchmarkMulticastChurn(b *testing.B) {
	m := NewMulticast(make(chan int, 1))
	for i := 0; i < b.N; i++ {
		s := m.Subscribe()
		s.Unsubscribe()
	}
	m.Close()
}
```

## Notes

- **Caveat:** no Go toolchain exists in this environment, so I could not execute these. Every shared field is an atomic or a channel by construction, so `-race` should be clean, but please run `go vet . && go test -race -count=1 .` and the benchmarks (`go test -bench . -benchmem`) before landing. If `Subscription` or `Multicast` collide with names elsewhere in go-bigbuff, rename freely — all internal identifiers are `multicast`-prefixed to avoid touching anything existing.
- **Tuning knobs:** `multicastDeliverySpins`/`multicastDeliveryYield` trade publisher CPU against park latency on slow consumers; `multicastReapLimit` bounds the eager unlink walk per Unsubscribe (unlinked-at-limit nodes are still guaranteed reclamation by the next traversal via the graves counter); the exemplar's cap is the per-subscriber backpressure/jitter policy.
- **Deliberate trade-offs to be aware of:** `Send` racing a `Subscribe` may miss that subscriber (the window `ChanPubSub` closes with `sendingMu`, spent here to make churn lock-free — a `Send` in flight always reaches every subscription that was live when it started); `Send` may block forever on a live-but-stopped subscriber by design (`TrySend` is the lossy escape); and re-entrant `Send` from inside a subscriber's own yield deadlocks only past the mailbox capacity, as with any bounded mailbox.
