Evolving ChanCaster / ChanPubSub beyond linear — designing a novel low-alloc, select-friendly broadcast that scales with churn.

# New complementary implementation: `EpochCaster`

Leave `ChanCaster` and `ChanPubSub` untouched. Add `epochcaster.go`: a publisher writes the payload **once** into a power-of-two epoch ring, publishes the epoch with one atomic store, and *hints* subscribers via per-subscriber buffered mailboxes. Wakeups are idempotent hints, not values; correctness comes from checking the epoch, never from counting wakeups.

## Why the old designs hit a ceiling

- **Anonymous rendezvous**: on one unbuffered channel any registered goroutine can take any copy. Correctness requires the global `Wait` barrier so nobody laps.
- **Barrier cost**: `ChanPubSub.Send` waits for the slowest subscriber every message; `sync.Cond.Broadcast` wakes $N$ goroutines under a global mutex.
- **Copies**: `ChanCaster.Send` copies $V$ $N$ times.
- **Churn**: unsubscribing mid-send needs the delicate negative-`Add` receive dance.
- **No backpressure isolation**: one slow subscriber stalls every `Send`, forever.

## The novel core

1. **Epoch ring (payload written once).** Slot $e \bmod R$ holds value $e$. Publisher writes $V$, stores `remaining = subscriberCount`, then stores `published = e`. Subscribers read slot, then decrement `remaining` exactly once. Slot $e$ is overwritten only after `remaining == 0` for $e-R$ — backpressure only on ring wrap, after $R$ epochs of slack, not every message.
2. **Hint, not channel.** Each subscription owns `wake chan struct{}` (cap 1). A token means "re-check the epoch", possibly for several epochs (tokens coalesce). Subscribers must **drain all available epochs per wake** — this is the one hard contract, and it's what makes missed/duplicate wakes harmless.
3. **Two-level dissemination.** Subscriptions pack first-fit into blocks of 64 (fixed atomic slots, zero alloc on churn). Blocks with $\le 7$ members: publisher wakes members directly. At 8 members a relay goroutine starts (hysteresis: stops only when block empties); publisher then sends **one token per block**, relays fan out in parallel. Publisher cost drops from $O(N)$ to $O(N/64)$ for large $N$.
4. **Short join gate.** `joinMu` is held only to snapshot the subscriber count and store the epoch — never during the slot-wait, the write, or waking. So unsubscribe can always free a slot the publisher is waiting on: no deadlock.
5. **Adaptive ring size** from `unsafe.Sizeof(V)`, targeting ~64 KiB of payload, clamped $[16, 4096]$, power-of-two masked (no modulo).

## Contract

- One goroutine owns a subscription; call `Unsubscribe` exactly once (idempotent).
- After `<-sub.Wake()`, drain with `TryRecv` until `ok == false`.
- A subscriber that never unsubscribes and never receives stalls the publisher after one ring wrap (same exceptional-broken-state class as before).
- `Send` is serialized across publishers; it returns the epoch (starting at 1), or 0 if there were no subscribers. `Send` after `Close` panics, like sending on a closed channel.

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
	"errors"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrEpochCasterClosed is returned by receive operations after the caster
// has been closed and all published values have been drained.
var ErrEpochCasterClosed = errors.New("bigbuff: epochcaster: closed")

const (
	// epochBlockSize is the maximum number of subscriptions in one block.
	epochBlockSize = 64
	// epochRelayThreshold is the block population at which a relay
	// goroutine is started. Relays stop only when the block empties
	// (hysteresis), avoiding start/stop flapping under churn.
	epochRelayThreshold = 8
	epochMinRingSize    = 16
	epochMaxRingSize    = 4096
	// epochDefaultRingTargetBytes bounds the default ring payload memory.
	epochDefaultRingTargetBytes = 64 << 10
)

// EpochCaster broadcasts each value to all subscribers registered at the
// time of publication, without callbacks and without a global barrier.
//
// It is select-friendly: a subscriber exposes a per-subscriber wake
// channel suitable for use in a select statement (see EpochSub.Wake).
// Unlike a shared channel, a wake token is a hint, not a value: one token
// may stand for several epochs, so a subscriber MUST drain all available
// values after each wake (see EpochSub.TryRecv).
//
// The payload is written once into a ring of epoch slots and read by each
// subscriber; the publisher never copies the value per subscriber and
// never waits for subscribers except when the ring wraps. Delivery to a
// subscriber is guaranteed for every epoch published while it is
// registered, provided it keeps receiving and eventually unsubscribes.
type EpochCaster[V any] struct {
	slots     []epochSlot[V]
	mask      uint64
	published atomic.Uint64
	closed    atomic.Bool
	// subscribers is mutated only under joinMu (write lock).
	subscribers atomic.Int64

	// sendMu serialises publishers.
	sendMu sync.Mutex
	// joinMu makes "snapshot subscriber count + publish epoch" atomic
	// with respect to Subscribe/Unsubscribe. It is held only briefly:
	// never during slot waits, payload writes, or wakeups.
	joinMu sync.RWMutex

	blocksMu   sync.Mutex
	blocksHead atomic.Pointer[epochBlock[V]]
}

// epochSlot is one ring slot. remaining counts subscribers that have not
// yet consumed the epoch currently occupying the slot; the publisher may
// reuse the slot only once it reaches zero.
type epochSlot[V any] struct {
	val       V
	remaining atomic.Int64
}

// epochBlock packs up to epochBlockSize subscriptions. Membership slots
// are fixed atomic pointers, so joining/leaving needs no allocation and
// waking needs no lock.
type epochBlock[V any] struct {
	wake  chan struct{} // cap 1; token received by the relay goroutine
	mu    sync.Mutex    // guards count and relayDone
	slots [epochBlockSize]atomic.Pointer[EpochSub[V]]
	count int // guarded by mu
	relay atomic.Bool
	// relayDone is closed to stop the relay; guarded by mu.
	relayDone chan struct{}
	// next is immutable once the block is linked into the caster list.
	next atomic.Pointer[epochBlock[V]]
}

// EpochSub is a single subscription. It must be owned by one goroutine at
// a time, and must not be reused after Unsubscribe.
type EpochSub[V any] struct {
	caster *EpochCaster[V]
	block  *epochBlock[V]
	slot   int
	wake   chan struct{} // cap 1
	// next is the next epoch to deliver. Written under joinMu at
	// Subscribe time, then owned by the subscriber goroutine.
	next uint64
	// left is set by Unsubscribe (owner goroutine only).
	left bool
}

func epochNextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func epochPrevPow2(n int) int {
	p := 1
	for p*2 <= n {
		p <<= 1
	}
	return p
}

// epochDefaultRingSize sizes the ring from sizeof(V) so that huge values
// do not imply a huge ring, while small values get generous pipelining.
func epochDefaultRingSize[V any]() int {
	var zero V
	sz := int(unsafe.Sizeof(zero))
	if sz < 8 {
		sz = 8
	}
	n := epochDefaultRingTargetBytes / (sz + 16) // +16: slot overhead
	if n < epochMinRingSize {
		n = epochMinRingSize
	}
	if n > epochMaxRingSize {
		n = epochMaxRingSize
	}
	return epochPrevPow2(n)
}

// NewEpochCaster constructs an EpochCaster with a ring size chosen from
// sizeof(V) (see epochDefaultRingSize).
func NewEpochCaster[V any]() *EpochCaster[V] {
	return NewEpochCasterWithRingSize[V](0)
}

// NewEpochCasterWithRingSize constructs an EpochCaster with the given
// ring size (rounded up to a power of two; <= 0 selects the default).
// The ring size bounds how many epochs a subscriber may lag before the
// publisher applies backpressure, and costs ringSize * sizeof(V) bytes.
func NewEpochCasterWithRingSize[V any](ringSize int) *EpochCaster[V] {
	if ringSize <= 0 {
		ringSize = epochDefaultRingSize[V]()
	} else {
		ringSize = epochNextPow2(ringSize)
		if ringSize > 1<<20 {
			panic("bigbuff: epochcaster: ring size too large")
		}
	}
	return &EpochCaster[V]{
		slots: make([]epochSlot[V], ringSize),
		mask:  uint64(ringSize - 1),
	}
}

// Send publishes value to all currently registered subscribers and
// returns its epoch (starting at 1), or 0 if there were no subscribers.
// It panics if the caster is closed.
//
// Send does not wait for subscribers to receive, except when the ring is
// about to wrap: then it waits until the epoch occupying the target slot
// has been consumed (or accounted for by unsubscription) by everyone.
func (x *EpochCaster[V]) Send(value V) uint64 {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	if x.closed.Load() {
		panic("bigbuff: epochcaster: send on closed caster")
	}
	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}

	epoch := x.published.Load() + 1
	slot := &x.slots[epoch&x.mask]

	// Wait (outside joinMu, so lagging subscribers can unsubscribe and
	// free the slot) until the previous occupant of this slot is done.
	if epoch > uint64(len(x.slots)) {
		for slot.remaining.Load() != 0 {
			runtime.Gosched()
		}
	}

	slot.val = value

	// Snapshot + publish: atomic w.r.t. Subscribe/Unsubscribe, so every
	// counted subscriber will consume this epoch exactly once, and no
	// uncounted subscriber will.
	x.joinMu.RLock()
	n := x.subscribers.Load()
	if n == 0 {
		x.joinMu.RUnlock()
		return 0
	}
	slot.remaining.Store(n)
	x.published.Store(epoch)
	x.joinMu.RUnlock()

	x.wakeAll()
	return epoch
}

// wakeAll hints every block. Small blocks are woken directly; blocks with
// a relay get a single token that the relay fans out in parallel.
// All sends are non-blocking: tokens coalesce in cap-1 channels.
func (x *EpochCaster[V]) wakeAll() {
	for b := x.blocksHead.Load(); b != nil; b = b.next.Load() {
		if b.relay.Load() {
			select {
			case b.wake <- struct{}{}:
			default:
			}
			continue
		}
		for i := range b.slots {
			sub := b.slots[i].Load()
			if sub == nil {
				continue
			}
			select {
			case sub.wake <- struct{}{}:
			default:
			}
		}
	}
}

// Close closes the caster. Subscribers drain all published values, then
// observe closure. Close is idempotent.
func (x *EpochCaster[V]) Close() {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	if x.closed.Load() {
		return
	}
	x.closed.Store(true)
	x.wakeAll()
}

// Len returns the current number of subscribers.
func (x *EpochCaster[V]) Len() int {
	return int(x.subscribers.Load())
}

// Published returns the most recently published epoch (0 if none).
func (x *EpochCaster[V]) Published() uint64 {
	return x.published.Load()
}

// RingSize returns the number of epoch slots in the ring.
func (x *EpochCaster[V]) RingSize() int {
	return len(x.slots)
}

// Subscribe registers and returns a new subscription. The subscription's
// first delivered epoch is the next epoch published after registration.
// Subscribing to a closed caster returns a subscription that immediately
// reports closure.
func (x *EpochCaster[V]) Subscribe() *EpochSub[V] {
	sub := &EpochSub[V]{caster: x, wake: make(chan struct{}, 1)}

	x.joinMu.Lock()
	defer x.joinMu.Unlock()

	sub.next = x.published.Load() + 1
	if x.closed.Load() {
		sub.left = true // detached; receives report closure
		return sub
	}
	sub.block, sub.slot = x.addToBlockLocked(sub)
	x.subscribers.Add(1)
	return sub
}

// addToBlockLocked places sub in the first block with a free slot,
// creating a block if needed. Callers must hold joinMu; it acquires
// blocksMu then the block mutex (the global lock order).
func (x *EpochCaster[V]) addToBlockLocked(sub *EpochSub[V]) (*epochBlock[V], int) {
	x.blocksMu.Lock()
	defer x.blocksMu.Unlock()

	for b := x.blocksHead.Load(); b != nil; b = b.next.Load() {
		b.mu.Lock()
		if b.count >= epochBlockSize {
			b.mu.Unlock()
			continue
		}
		slot := 0
		for slot < epochBlockSize && b.slots[slot].Load() != nil {
			slot++
		}
		if slot == epochBlockSize {
			b.mu.Unlock()
			continue // defensive; count and slots cannot disagree
		}
		b.slots[slot].Store(sub)
		b.count++
		if b.count == epochRelayThreshold {
			done := make(chan struct{})
			b.relayDone = done
			b.relay.Store(true)
			go b.relayLoop(done)
		}
		b.mu.Unlock()
		return b, slot
	}

	b := &epochBlock[V]{wake: make(chan struct{}, 1)}
	b.slots[0].Store(sub)
	b.count = 1
	b.next.Store(x.blocksHead.Load())
	x.blocksHead.Store(b)
	return b, 0
}

// relayLoop fans block wake tokens out to member mailboxes. Duplicate or
// stale tokens are harmless: subscribers re-check the epoch.
func (b *epochBlock[V]) relayLoop(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-b.wake:
			for i := range b.slots {
				sub := b.slots[i].Load()
				if sub == nil {
					continue
				}
				select {
				case sub.wake <- struct{}{}:
				default:
				}
			}
		}
	}
}

// Wake returns the subscription's wake channel, for use in a select
// statement. Receiving from it is only a hint: after it fires, the caller
// MUST drain every available value via TryRecv before blocking again,
// because one token may represent several epochs.
func (s *EpochSub[V]) Wake() <-chan struct{} {
	return s.wake
}

// NextEpoch returns the epoch that will be delivered next.
func (s *EpochSub[V]) NextEpoch() uint64 {
	return s.next
}

// TryRecv returns the next value if one is available, without blocking.
// The results are (value, ok, closed): ok reports a delivered value;
// closed reports that the caster is closed (or the subscription has been
// unsubscribed) and no values remain.
func (s *EpochSub[V]) TryRecv() (V, bool, bool) {
	var zero V
	if s.left {
		return zero, false, true
	}
	x := s.caster
	next := s.next
	if next > x.published.Load() {
		if x.closed.Load() {
			return zero, false, true
		}
		return zero, false, false
	}
	slot := &x.slots[next&x.mask]
	v := slot.val // copy out before releasing the slot
	slot.remaining.Add(-1)
	s.next = next + 1
	return v, true, false
}

// Recv blocks until the next value is available or the caster is closed.
// The boolean result is false on closure (after draining).
func (s *EpochSub[V]) Recv() (V, bool) {
	for {
		v, ok, closed := s.TryRecv()
		if ok {
			return v, true
		}
		if closed {
			var zero V
			return zero, false
		}
		<-s.wake
	}
}

// RecvContext is Recv with context cancellation. It returns
// ErrEpochCasterClosed after draining a closed caster.
func (s *EpochSub[V]) RecvContext(ctx context.Context) (V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		v, ok, closed := s.TryRecv()
		if ok {
			return v, nil
		}
		if closed {
			var zero V
			return zero, ErrEpochCasterClosed
		}
		select {
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		case <-s.wake:
		}
	}
}

// Unsubscribe withdraws the subscription, accounting for every epoch it
// was counted for but had not consumed, so the publisher is never stuck
// waiting for it. Unsubscribe is idempotent. The subscription must not
// be used afterwards.
func (s *EpochSub[V]) Unsubscribe() {
	if s.left {
		return
	}
	s.left = true
	x := s.caster
	if s.block == nil {
		return // subscription to a closed caster; never registered
	}

	x.joinMu.Lock()
	// While joinMu is held, no publisher can publish, so the set of owed
	// epochs is stable.
	published := x.published.Load()
	for epoch := s.next; epoch <= published; epoch++ {
		x.slots[epoch&x.mask].remaining.Add(-1)
	}
	s.next = published + 1
	x.subscribers.Add(-1)

	b := s.block
	b.mu.Lock()
	b.slots[s.slot].Store(nil)
	b.count--
	if b.count == 0 {
		b.relay.Store(false)
		if b.relayDone != nil {
			close(b.relayDone)
			b.relayDone = nil
		}
	}
	b.mu.Unlock()
	x.joinMu.Unlock()
}

// SubscribeContext subscribes immediately and returns a single-use
// iterator yielding every value until the context is cancelled, the
// iterator stops early, or the caster is closed; it unsubscribes on exit.
// Behavior is undefined if the iterator is used more than once.
//
// WARNING: the iterator must be used immediately, or the context must be
// promptly cancelled, to avoid leaking the subscription.
func (x *EpochCaster[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				sub.Unsubscribe()
			}
			panic("bigbuff: epochcaster: iterator yield func is nil")
		}
		if !stop() {
			return // context cancelled, or iterator used more than once
		}
		defer sub.Unsubscribe()
		for {
			v, err := sub.RecvContext(ctx)
			if err != nil {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
```

## Tests and benchmarks (`epochcaster_test.go`)

```go
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEpochCasterBasicFanout(t *testing.T) {
	x := NewEpochCaster[int]()
	subs := []*EpochSub[int]{x.Subscribe(), x.Subscribe(), x.Subscribe()}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	if got := x.Send(42); got != 1 {
		t.Fatalf("Send epoch = %d, want 1", got)
	}
	for i, s := range subs {
		v, ok := s.Recv()
		if !ok || v != 42 {
			t.Fatalf("sub %d: got (%d,%v), want (42,true)", i, v, ok)
		}
	}
}

func TestEpochCasterNoSubscribers(t *testing.T) {
	x := NewEpochCaster[int]()
	if got := x.Send(1); got != 0 {
		t.Fatalf("Send with no subscribers = %d, want 0", got)
	}
	sub := x.Subscribe()
	defer sub.Unsubscribe()
	if got := x.Send(2); got != 1 {
		t.Fatalf("first published epoch = %d, want 1", got)
	}
	if v, ok := sub.Recv(); !ok || v != 2 {
		t.Fatalf("got (%d,%v), want (2,true)", v, ok)
	}
}

func TestEpochCasterSelectAndDrain(t *testing.T) {
	x := NewEpochCaster[int]()
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	x.Send(1)
	x.Send(2)
	x.Send(3) // wake tokens coalesce into one

	select {
	case <-sub.Wake():
		for want := 1; want <= 3; want++ {
			v, ok, closed := sub.TryRecv()
			if closed || !ok || v != want {
				t.Fatalf("drain: got (%d,%v,%v), want (%d,true,false)", v, ok, closed, want)
			}
		}
		if _, ok, _ := sub.TryRecv(); ok {
			t.Fatal("drained sub still reports a value")
		}
	case <-time.After(time.Second):
		t.Fatal("no wake received")
	}
}

func TestEpochCasterRingWrapBlocksPublisher(t *testing.T) {
	x := NewEpochCasterWithRingSize[int](4)
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	for i := 1; i <= 4; i++ {
		x.Send(i)
	}
	done := make(chan uint64, 1)
	go func() { done <- x.Send(5) }()

	select {
	case <-done:
		t.Fatal("Send returned before ring slot was freed")
	case <-time.After(50 * time.Millisecond):
	}

	if v, ok := sub.Recv(); !ok || v != 1 {
		t.Fatalf("got (%d,%v), want (1,true)", v, ok)
	}
	select {
	case got := <-done:
		if got != 5 {
			t.Fatalf("blocked Send epoch = %d, want 5", got)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Send did not complete after slot freed")
	}
	for want := 2; want <= 5; want++ {
		if v, ok := sub.Recv(); !ok || v != want {
			t.Fatalf("got (%d,%v), want (%d,true)", v, ok, want)
		}
	}
}

func TestEpochCasterUnsubscribeFreesRing(t *testing.T) {
	x := NewEpochCasterWithRingSize[int](4)
	a := x.Subscribe()
	b := x.Subscribe()
	defer b.Unsubscribe()

	x.Send(1)
	a.Unsubscribe() // a never reads epoch 1; must not stall the ring

	for want := 2; want <= 20; want++ {
		x.Send(want)
		if v, ok := b.Recv(); !ok || v != want {
			t.Fatalf("got (%d,%v), want (%d,true)", v, ok, want)
		}
	}
}

func TestEpochCasterRelayFanout(t *testing.T) {
	const n = 100 // crosses the relay threshold in both blocks
	x := NewEpochCaster[int]()
	subs := make([]*EpochSub[int], n)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	x.Send(7)
	for i, s := range subs {
		if v, ok := s.Recv(); !ok || v != 7 {
			t.Fatalf("sub %d: got (%d,%v), want (7,true)", i, v, ok)
		}
	}
}

func TestEpochCasterClose(t *testing.T) {
	x := NewEpochCaster[int]()
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	x.Send(7)
	x.Close()
	x.Close() // idempotent

	if v, ok := sub.Recv(); !ok || v != 7 {
		t.Fatalf("got (%d,%v), want (7,true)", v, ok)
	}
	if _, ok := sub.Recv(); ok {
		t.Fatal("Recv after drain of closed caster returned ok")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Send after Close did not panic")
		}
	}()
	x.Send(8)
}

func TestEpochCasterSubscribeAfterClose(t *testing.T) {
	x := NewEpochCaster[int]()
	x.Close()
	sub := x.Subscribe()
	if _, ok := sub.Recv(); ok {
		t.Fatal("subscription to closed caster returned a value")
	}
	sub.Unsubscribe()
}

func TestEpochCasterRecvContextCancelled(t *testing.T) {
	x := NewEpochCaster[int]()
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.RecvContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEpochCasterChurn(t *testing.T) {
	x := NewEpochCasterWithRingSize[int](64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				sub := x.Subscribe()
				last := 0
				func() {
					defer sub.Unsubscribe()
					for {
						v, err := sub.RecvContext(ctx)
						if err != nil {
							return
						}
						if v <= last {
							t.Errorf("out-of-order delivery: %d after %d", v, last)
							return
						}
						last = v
					}
				}()
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}

	for i := 1; i <= 2000; i++ {
		x.Send(i)
	}
	cancel()
	wg.Wait()
}

func TestEpochCasterConcurrentPublishers(t *testing.T) {
	const publishers, perPublisher = 8, 100
	x := NewEpochCaster[int]()
	subs := []*EpochSub[int]{x.Subscribe(), x.Subscribe(), x.Subscribe(), x.Subscribe()}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	var counter atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				x.Send(int(counter.Add(1)))
			}
		}()
	}
	wg.Wait()

	const total = publishers * perPublisher
	for i, s := range subs {
		seen := make(map[int]bool, total)
		for j := 0; j < total; j++ {
			v, ok := s.Recv()
			if !ok {
				t.Fatalf("sub %d: closed after %d values", i, j)
			}
			if seen[v] {
				t.Fatalf("sub %d: duplicate value %d", i, v)
			}
			seen[v] = true
		}
	}
}

// BenchmarkEpochCasterSend measures publisher cost plus an in-process
// drain of every subscriber (TryRecv does not depend on wake delivery).
func BenchmarkEpochCasterSend(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			x := NewEpochCaster[int]()
			subs := make([]*EpochSub[int], n)
			for i := range subs {
				subs[i] = x.Subscribe()
			}
			defer func() {
				for _, s := range subs {
					s.Unsubscribe()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(i)
				for _, s := range subs {
					if _, ok, _ := s.TryRecv(); !ok {
						b.Fatal("missing value")
					}
				}
			}
		})
	}
}

func BenchmarkEpochCasterSubscribeUnsubscribe(b *testing.B) {
	x := NewEpochCaster[int]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Subscribe().Unsubscribe()
	}
}
```

Run with `go test -race ./...` and `go test -run . -bench EpochCaster -benchmem`.

## Complexity comparison

| | `ChanPubSub` per broadcast | `EpochCaster` per broadcast |
|---|---|---|
| Payload copies by publisher | $N$ channel sends | 1 write |
| Receiver sync | $N$ rendezvous + `sync.Cond` broadcast + `Wait` barrier (slowest wins) | 1 buffered hint; local drain |
| Publisher work, large $N$ | $O(N)$ under mutex | $O(N/64)$ tokens; relay fanout in parallel |
| Slow consumer | blocks every `Send` | blocks only after ring wrap |
| Churn | negative `Add` may receive from channel | $O(1)$ slot + owed-epoch accounting; no channel ops |
| `Send` allocs | 0 | 0 |
| `Subscribe` allocs | 0 | sub + wake channel (amortized block) |
| Select | shared channel + mandatory `Wait` | per-sub `Wake()` + drain-all |
| Memory | $O(1)$ | $O(R \cdot \mathrm{sizeof}(V))$, adaptively bounded |

## Honest costs / when not to use it

- Ring memory scales with `sizeof(V)`; cap it with `NewEpochCasterWithRingSize` for large values.
- `Subscribe` allocates (wake channel) where `ChanPubSub.Add` is allocation-free.
- Blocks above 8 subscribers run a relay goroutine each (one per 64 subscribers).
- Per-slot `remaining` is one contended atomic per epoch — the natural next evolution is per-block sub-counters, turning the countdown into a hierarchical barrier. Multi-publisher lock-free publication (ticket-claimed epochs) is the other obvious next step, but it requires replacing the short `joinMu` snapshot with a joining-gate handshake.

That combination — single-copy epoch ring, coalesced hint mailboxes, parallel relay dissemination, and a join gate held only for a snapshot — is what gets broadcast past the anonymous-channel barrier while keeping `Send` allocation-free and genuinely `select`-friendly.
