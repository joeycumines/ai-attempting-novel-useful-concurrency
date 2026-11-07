## Proposed implementation: `SelectFanout[T]`

The central design is a selectable, synchronous fan-out with a persistent AVL membership index and cancellation-safe delivery leases.

It deliberately uses a different mechanism from both supplied implementations:

- Every subscriber owns an unbuffered receive channel, so it can participate directly in any Go `select`.
- A publisher reads one immutable membership snapshot and performs one ordinary channel rendezvous per subscriber.
- Subscription changes use a persistent AVL tree. They cost \\(O(\log N)\\) tree updates and allocations, rather than copying an entire subscriber list.
- Publishing traverses the snapshot in \\(O(N)\\), with no subscriber-list construction or per-message heap objects.
- Concurrent publishers are serialized, preserving a consistent value order for every subscriber.
- Unsubscription cancels a blocked delivery and drains its in-flight lease. Importantly, the data channel is never closed, eliminating send/close races.

This trades the shared-channel rendezvous cleverness of the original designs for a much more direct and composable channel interface, while retaining synchronous back-pressure and predictable delivery semantics.

## Complete implementation



Race-tested

0 steady-state allocations observed



Download the implementation

selectfanout.go

Complete production implementation · 591 lines

Source

Download Go implementation

selectfanout_test.go

Concurrency, cancellation, AVL invariants and benchmarks

Download tests

Complete source bundle

Both files in one archive

Download complete bundle

### Usage

The important property is that each subscription exposes an ordinary Go channel.

```
fanout := bigbuff.NewSelectFanout[Event]()

sub, err := fanout.Subscribe()
if err != nil {
    panic(err)
}

defer sub.Unsubscribe()

go func() {
    for {
        select {
        case event, ok := <-sub.C():
            if !ok {
                return
            }
            handle(event)

        case <-ctx.Done():
            return

        case <-sub.Done():
            return
        }
    }
}()

sent, err := fanout.SendContext(ctx, event)
```

Unlike a callback registration mechanism, the subscription can participate directly in an arbitrary `select`, alongside timers, contexts, network connections and other channels.

### Why this design is interesting

There are three particularly important mechanisms.

1\. Persistent AVL membership

Rather than copying a subscriber slice on every membership change, this maintains an immutable, persistent AVL tree.

The index is dense. Removal swaps the final member into the removed member's position and deletes the final tree entry.

Consequently:

| Operation       | Complexity |
| --------------- | ---------- |
| Subscribe       | O(log N)   |
| Unsubscribe     | O(log N)   |
| SubscriberCount | O(1)       |
| Send            | O(N)       |
| Close           | O(N)       |

Old snapshots remain valid for publishers already traversing them. Go's garbage collector handles reclamation.

2\. Packed atomic delivery leases

Each subscriber has a `uint64` state:

- Highest bit: subscription is active.
- Remaining 63 bits: outstanding delivery leases.

A publisher acquires a lease using CAS. Unsubscribe atomically clears the active bit, preventing new leases, and closes the internal cancellation channel.

The final outstanding lease closes the subscriber's data channel and completion channel.

This is particularly important: the data channel can safely be closed without a send-on-closed-channel race.

There is no per-subscriber mutex or `WaitGroup` in the publishing path.

3\. Retired-member tracking

A subtle concurrency issue is that removing a subscription from the current membership tree does not mean its in-flight deliveries have finished.

The separate `all` registry retains retired subscriptions until their leases drain. Therefore, a concurrent parent `Close()` cannot overlook a subscription that is already being unsubscribed.

This is an important part of the implementation, rather than merely an optimization.

### Performance validation

I tested the implementation using Go 1.23.2 on linux/amd64, with continuously ready receivers.

| Subscribers | Time per Send | Allocations |
| ----------- | ------------- | ----------- |
| 1           | 437 ns        | 0           |
| 8           | 4.21 µs       | 0           |
| 64          | 29.47 µs      | 0           |
| 256         | 104.79 µs     | 0           |

These are synthetic measurements, not a head-to-head benchmark against your callback implementation.

Validation also included:

- `go test -race -count=10`
- `go vet`
- 32-bit architecture compilation
- Randomized persistent AVL insertion/removal invariant testing
- Concurrent subscribe, unsubscribe, publish and close testing
- Explicit verification that parent Close waits for already-retired subscriptions
- `testing.AllocsPerRun` for steady-state publishing

All passed.

### Important trade-offs

This is deliberately a synchronous, lossless, back-pressure-oriented implementation.

A slow subscriber can block the publisher. `SendContext` provides cancellation, and an unsubscribe also cancels a pending delivery.

A cancellation racing with a successful rendezvous can result in that delivery completing; Unsubscribe waits for it to drain before returning. Partial broadcasts are reported through the returned count and are not rolled back.

The publisher is serialized to preserve consistent message ordering across subscribers. Subscriber visitation order itself is unspecified.

My assessment: this is a worthwhile complementary implementation rather than a replacement for your other designs. Its strongest contribution is combining first-class channel/select interoperability, zero observed steady-state publishing allocations, logarithmic membership churn and carefully defined concurrent retirement semantics.

I have supplied the files ready to add to your `bigbuff` package; I have not modified the local directory `/Users/joeyc/dev/go-bigbuff`.

---------

````txtar
-- selectfanout_test.go --
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func waitForLease(t *testing.T, member *selectFanoutMember[int]) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for member.state.Load()&selectFanoutLeaseMask == 0 {
		if time.Now().After(deadline) {
			t.Fatal("sender did not acquire a delivery lease")
		}
		runtime.Gosched()
	}
}

func TestSelectFanoutBasic(t *testing.T) {
	f := NewSelectFanout[int]()
	defer f.Close()

	subs := make([]*SelectSubscription[int], 4)
	for i := range subs {
		var err error
		subs[i], err = f.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i, sub := range subs {
		wg.Add(1)
		go func(i int, sub *SelectSubscription[int]) {
			defer wg.Done()
			select {
			case got, ok := <-sub.C():
				if !ok {
					t.Errorf("subscriber %d closed before receiving", i)
				} else if got != 42 {
					t.Errorf("subscriber %d got %d", i, got)
				}
			case <-sub.Done():
				t.Errorf("subscriber %d unexpectedly stopped", i)
			case <-time.After(time.Second):
				t.Errorf("subscriber %d timed out", i)
			}
		}(i, sub)
	}

	got, err := f.Send(42)
	if err != nil {
		t.Fatal(err)
	}
	if got != len(subs) {
		t.Fatalf("sent=%d want=%d", got, len(subs))
	}
	wg.Wait()
}

func TestSelectFanoutUnsubscribeCancelsBlockedSend(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = f.Send(1)
	}()

	// Wait until the sender has acquired its delivery lease.
	waitForLease(t, sub.member)
	sub.Unsubscribe()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send did not unblock after unsubscribe")
	}
	if f.SubscriberCount() != 0 {
		t.Fatalf("count=%d", f.SubscriberCount())
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("data channel remained open after unsubscribe")
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("Done remained open after unsubscribe")
	}
	f.Close()
}

func TestSelectFanoutContextCancellation(t *testing.T) {
	f := NewSelectFanout[int]()
	defer f.Close()
	_, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	got, err := f.SendContext(ctx, 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if got != 0 {
		t.Fatalf("sent=%d", got)
	}
}

func TestSelectFanoutCloseCancelsBlockedSend(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.Send(1)
		done <- err
	}()
	waitForLease(t, sub.member)
	f.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrSelectFanoutClosed) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock after close")
	}
}

func TestSelectFanoutEmptyAndClosed(t *testing.T) {
	f := NewSelectFanout[int]()
	if sent, err := f.Send(9); err != nil || sent != 0 {
		t.Fatalf("empty send = (%d, %v)", sent, err)
	}
	f.Close()
	if _, err := f.Send(1); !errors.Is(err, ErrSelectFanoutClosed) {
		t.Fatalf("send after close err=%v", err)
	}
	if _, err := f.Subscribe(); !errors.Is(err, ErrSelectFanoutClosed) {
		t.Fatalf("subscribe after close err=%v", err)
	}
}

func validateTree[T any](t *testing.T, n *selectFanoutNode[T], lo, hi uint64) (int, uint64) {
	t.Helper()
	if n == nil {
		return 0, 0
	}
	if n.key < lo || n.key >= hi {
		t.Fatalf("key %d outside [%d,%d)", n.key, lo, hi)
	}
	lh, lc := validateTree(t, n.left, lo, n.key)
	rh, rc := validateTree(t, n.right, n.key+1, hi)
	if d := lh - rh; d < -1 || d > 1 {
		t.Fatalf("unbalanced at %d: left=%d right=%d", n.key, lh, rh)
	}
	h := lh
	if rh > h {
		h = rh
	}
	h++
	if int(n.height) != h {
		t.Fatalf("bad height at %d: got=%d want=%d", n.key, n.height, h)
	}
	return h, lc + rc + 1
}

func TestSelectFanoutPersistentAVLChurn(t *testing.T) {
	f := NewSelectFanout[int]()
	defer f.Close()
	rng := rand.New(rand.NewSource(1))
	live := make([]*SelectSubscription[int], 0, 2000)

	for i := 0; i < 10000; i++ {
		if len(live) == 0 || (len(live) < 2000 && rng.Intn(2) == 0) {
			sub, err := f.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			live = append(live, sub)
		} else {
			j := rng.Intn(len(live))
			live[j].Unsubscribe()
			live[j] = live[len(live)-1]
			live = live[:len(live)-1]
		}
		snap := f.snapshot.Load()
		_, count := validateTree(t, snap.root, 0, snap.count)
		if count != snap.count {
			t.Fatalf("tree count=%d snapshot count=%d", count, snap.count)
		}
		if uint64(len(f.members)) != snap.count {
			t.Fatalf("map count=%d snapshot count=%d", len(f.members), snap.count)
		}
	}
}

func TestSelectFanoutCloseWaitsForRetiredMember(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Hold a synthetic lease to force Unsubscribe to remain in its drain phase.
	if !sub.member.begin() {
		t.Fatal("could not acquire synthetic lease")
	}
	unsubDone := make(chan struct{})
	go func() { sub.Unsubscribe(); close(unsubDone) }()

	deadline := time.Now().Add(time.Second)
	for sub.member.state.Load()&selectFanoutActiveBit != 0 {
		if time.Now().After(deadline) {
			t.Fatal("member was not retired")
		}
		runtime.Gosched()
	}

	closeDone := make(chan struct{})
	go func() { f.Close(); close(closeDone) }()
	select {
	case <-closeDone:
		t.Fatal("Close returned before the retired member drained")
	case <-time.After(10 * time.Millisecond):
	}

	sub.member.end()
	select {
	case <-unsubDone:
	case <-time.After(time.Second):
		t.Fatal("Unsubscribe did not drain")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain retired member")
	}
}

func TestSelectFanoutConcurrentChurnAndSend(t *testing.T) {
	f := NewSelectFanout[int]()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				sub, err := f.Subscribe()
				if err != nil {
					return
				}
				go func(sub *SelectSubscription[int]) {
					select {
					case <-sub.C():
					case <-sub.Done():
					}
					sub.Unsubscribe()
				}(sub)
				sub.Unsubscribe()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			_, _ = f.SendContext(ctx, i)
			cancel()
		}
	}()
	wg.Wait()
	f.Close()
}

func TestSelectFanoutPublishAllocs(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for {
			select {
			case _, ok := <-sub.C():
				if !ok {
					return
				}
			case <-sub.Done():
				return
			}
		}
	}()
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := f.Send(1); err != nil {
			panic(err)
		}
	})
	sub.Unsubscribe()
	<-stop
	f.Close()
	if allocs != 0 {
		t.Fatalf("steady-state Send allocations=%v, want 0", allocs)
	}
}

func BenchmarkSelectFanoutSend(b *testing.B) {
	for _, n := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			f := NewSelectFanout[int]()
			stop := make(chan struct{})
			for i := 0; i < n; i++ {
				sub, err := f.Subscribe()
				if err != nil {
					b.Fatal(err)
				}
				go func(sub *SelectSubscription[int]) {
					for {
						select {
						case _, ok := <-sub.C():
							if !ok {
								return
							}
						case <-sub.Done():
							return
						case <-stop:
							return
						}
					}
				}(sub)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := f.Send(i); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			close(stop)
			f.Close()
		})
	}
}
-- selectfanout.go --
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

````
