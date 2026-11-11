package bigbuff

// Tests written in the completion phase; the response shipped none and the
// first integration relied on the shared harness, which never overran the
// ring. Repairs 5 to 8 (see the header of arcadia_30_dots3.go) each have a
// regression test here.

import (
	"context"
	"sync"
	"testing"
)

func drainArcadia(s *ArcadiaSub[int]) (values []int) {
	for {
		v, ok := s.TryReceive()
		if !ok {
			return values
		}
		values = append(values, v)
	}
}

func sameInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestArcadia_CapacityIsRoundedUpAndValidated(t *testing.T) {
	for capacity, want := range map[int]int{1: 1, 2: 2, 3: 4, 5: 8, 64: 64, 65: 128} {
		if got := NewArcadia[int](capacity).Cap(); got != want {
			t.Errorf("NewArcadia(%d).Cap() = %d, want %d", capacity, got, want)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("NewArcadia(0) did not panic")
		}
	}()
	NewArcadia[int](0)
}

func TestArcadia_OrderedWithinCapacity(t *testing.T) {
	a := NewArcadia[int](8)
	if n := a.Send(-1); n != 0 {
		t.Fatalf("Send with no subscribers = %d, want 0", n)
	}
	first, second := a.Subscribe(), a.Subscribe()
	if a.Subscribers() != 2 {
		t.Fatalf("Subscribers = %d, want 2", a.Subscribers())
	}
	for v := range 8 {
		if n, ok := a.TrySend(v); n != 2 || !ok {
			t.Fatalf("TrySend = %d, %v; want 2, true", n, ok)
		}
	}
	if a.Len() != 8 {
		t.Fatalf("Len = %d with 8 unread values, want 8", a.Len())
	}
	want := []int{0, 1, 2, 3, 4, 5, 6, 7}
	for _, s := range []*ArcadiaSub[int]{first, second} { // cursors are independent
		if got := drainArcadia(s); !sameInts(got, want) {
			// The value sent before Subscribe must not appear.
			t.Fatalf("received %v, want %v", got, want)
		}
	}
	if a.Len() != 0 {
		t.Fatalf("Len = %d after both subscribers drained, want 0", a.Len())
	}
}

// TestArcadia_OverrunSkipsToOldestRetained is the regression test for the
// first half of repair 5. As delivered, this subscriber received
// 8 9 6 7 8 9 6 7 8 9.
func TestArcadia_OverrunSkipsToOldestRetained(t *testing.T) {
	a := NewArcadia[int](4)
	s := a.Subscribe()
	for v := range 10 {
		a.Send(v)
	}
	if n := a.Len(); n != 4 {
		t.Errorf("Len = %d for an overrun subscriber of a 4-slot ring, want 4", n)
	}
	if got := drainArcadia(s); !sameInts(got, []int{6, 7, 8, 9}) {
		t.Fatalf("received %v after overrun, want the retained [6 7 8 9] once, in order", got)
	}
	a.Send(10)
	if got := drainArcadia(s); !sameInts(got, []int{10}) {
		t.Fatalf("received %v after catching up, want [10]", got)
	}
}

// TestArcadia_ReservedPositionIsNotReadFromThePreviousLap is the regression
// test for the second half of repair 5. It plays one sender's two steps
// separately: with the position reserved and the payload not yet stored, the
// slot still holds the previous lap's value, which as delivered was returned
// as the new one.
func TestArcadia_ReservedPositionIsNotReadFromThePreviousLap(t *testing.T) {
	a := NewArcadia[int](4)
	s := a.Subscribe()
	for v := range 4 {
		a.Send(v)
	}
	if got := drainArcadia(s); len(got) != 4 {
		t.Fatalf("received %v, want the first lap", got)
	}
	pos := a.write.Add(1) - 1 // a sender has reserved position 4 ...
	if v, ok := s.TryReceive(); ok {
		t.Fatalf("TryReceive returned %d for a position whose payload is not stored yet", v)
	}
	a.store(pos, 40) // ... and now stores it
	if v, ok := s.TryReceive(); !ok || v != 40 {
		t.Fatalf("TryReceive = %d, %v after the store, want 40, true", v, ok)
	}
}

// TestArcadia_SlotNeverRegresses is the regression test for repair 6: two
// concurrent senders, one lap apart on a one-slot ring, the later completing
// first.
func TestArcadia_SlotNeverRegresses(t *testing.T) {
	a := NewArcadia[int](1)
	s := a.Subscribe()
	delayed := a.write.Add(1) - 1 // first sender: position reserved
	a.Send(20)                    // second sender: next position, same slot
	a.store(delayed, 10)          // first sender resumes
	if got := drainArcadia(s); !sameInts(got, []int{20}) {
		t.Fatalf("received %v, want only the newer value [20]", got)
	}
}

// TestArcadia_WaitDoesNotSleepThroughACompletedSend is the regression test
// for repair 7. Receive records the generation, finds the ring empty, and
// waits; a Send that completes between those two steps must end the wait. As
// delivered the generation was read inside wait, after the Send.
func TestArcadia_WaitDoesNotSleepThroughACompletedSend(t *testing.T) {
	a := NewArcadia[int](4)
	s := a.Subscribe()
	gen := a.gen.Load() // Receive's snapshot, taken before it looks
	a.Send(1)           // completes before the subscriber starts waiting
	woke := make(chan struct{})
	go func() { s.wait(gen); close(woke) }()
	apexAwait(t, woke, "wait, with a Send already completed since its snapshot")
}

func TestArcadia_EveryBlockedSubscriberWakes(t *testing.T) {
	const subscribers, rounds = 3, 300
	a := NewArcadia[int](8)
	got := make(chan int, subscribers)
	var readers sync.WaitGroup
	var subs []*ArcadiaSub[int]
	for range subscribers {
		s := a.Subscribe()
		subs = append(subs, s)
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				v, ok := s.Receive()
				if !ok {
					return
				}
				got <- v
			}
		}()
	}
	for round := range rounds {
		a.Send(round)
		for i := range subscribers {
			select {
			case v := <-got:
				if v != round {
					t.Fatalf("round %d: a subscriber received %d", round, v)
				}
			case <-timeAfterGuard():
				t.Fatalf("round %d: only %d of %d blocked subscribers were woken", round, i, subscribers)
			}
		}
	}
	for _, s := range subs {
		a.Unsubscribe(s)
	}
	done := make(chan struct{})
	go func() { readers.Wait(); close(done) }()
	apexAwait(t, done, "Unsubscribe releasing blocked Receive calls")
	if a.Subscribers() != 0 {
		t.Fatalf("Subscribers = %d after all unsubscribed", a.Subscribers())
	}
}

func TestArcadia_UnsubscribeDrainsThenReportsDetached(t *testing.T) {
	a := NewArcadia[int](4)
	s, other := a.Subscribe(), a.Subscribe()
	a.Send(1)
	a.Send(2)
	a.Unsubscribe(s)
	a.Unsubscribe(s) // idempotent
	if a.Subscribers() != 1 {
		t.Fatalf("Subscribers = %d, want 1", a.Subscribers())
	}
	// Values already published are still delivered; then ok=false, without
	// blocking.
	for _, want := range []int{1, 2} {
		if v, ok := s.Receive(); !ok || v != want {
			t.Fatalf("Receive = %d, %v after Unsubscribe, want %d, true", v, ok, want)
		}
	}
	if _, ok := s.Receive(); ok {
		t.Fatal("Receive on a drained, detached subscriber returned ok=true")
	}
	if n := s.ReceiveBatch(make([]int, 4), 4); n != 0 {
		t.Fatalf("ReceiveBatch on a drained, detached subscriber = %d", n)
	}
	if got := drainArcadia(other); !sameInts(got, []int{1, 2}) {
		t.Fatalf("the remaining subscriber received %v", got)
	}
}

func TestArcadia_ReceiveBatch(t *testing.T) {
	a := NewArcadia[int](8)
	s := a.Subscribe()
	dst := make([]int, 3)
	if n := s.ReceiveBatch(dst, 0); n != 0 {
		t.Fatalf("ReceiveBatch(n=0) = %d", n)
	}
	if n := s.ReceiveBatch(nil, 3); n != 0 {
		t.Fatalf("ReceiveBatch(empty dst) = %d", n)
	}
	for v := range 5 {
		a.Send(v)
	}
	if n := s.ReceiveBatch(dst, 8); n != 3 || !sameInts(dst, []int{0, 1, 2}) {
		t.Fatalf("ReceiveBatch = %d, %v; want 3 values limited by len(dst)", n, dst)
	}
	if n := s.ReceiveBatch(dst, 1); n != 1 || dst[0] != 3 {
		t.Fatalf("ReceiveBatch(n=1) = %d, first %d", n, dst[0])
	}
	// One value left: the batch returns it rather than waiting for more.
	if n := s.ReceiveBatch(dst, 3); n != 1 || dst[0] != 4 {
		t.Fatalf("ReceiveBatch = %d, first %d; want the single remaining value", n, dst[0])
	}
	// With nothing ready it blocks for the first value.
	got := make(chan int, 1)
	go func() { n := s.ReceiveBatch(dst, 3); got <- n }()
	a.Send(5)
	select {
	case n := <-got:
		if n != 1 || dst[0] != 5 {
			t.Fatalf("blocking ReceiveBatch = %d, first %d", n, dst[0])
		}
	case <-timeAfterGuard():
		t.Fatal("blocking ReceiveBatch was not woken by Send")
	}
}

func TestArcadia_SubscribeContext(t *testing.T) {
	a := NewArcadia[int](8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	values := a.SubscribeContext(ctx) // subscribed from here
	a.Send(1)
	a.Send(2)
	var seen []int
	for v := range values {
		seen = append(seen, v)
		if len(seen) == 2 {
			break
		}
	}
	if !sameInts(seen, []int{1, 2}) {
		t.Fatalf("iterator yielded %v, want [1 2]", seen)
	}
	if a.Subscribers() != 0 {
		t.Fatalf("Subscribers = %d after the range loop ended, want 0", a.Subscribers())
	}

	// Cancellation ends an iterator that is blocked with nothing to read.
	blocked := a.SubscribeContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range blocked {
			t.Error("iterator yielded although nothing was sent")
		}
	}()
	cancel()
	apexAwait(t, done, "a blocked iterator after cancel")
	if a.Subscribers() != 0 {
		t.Fatalf("Subscribers = %d after cancel, want 0", a.Subscribers())
	}
}

// TestArcadia_IteratorHasUnsubscribedWhenItReturns is the regression test
// for repair 9. With an already cancelled context the removal is queued on
// the context's AfterFunc goroutine; the iterator used to return without it.
func TestArcadia_IteratorHasUnsubscribedWhenItReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for round := range 200 {
		a := NewArcadia[int](4)
		values := a.SubscribeContext(ctx)
		for v := range values {
			t.Fatalf("round %d: a cancelled iterator yielded %d", round, v)
		}
		if n := a.Subscribers(); n != 0 {
			t.Fatalf("round %d: Subscribers = %d after the iterator returned, want 0", round, n)
		}
	}
}

func TestArcadia_AsChannel(t *testing.T) {
	a := NewArcadia[int](8)
	s := a.Subscribe()
	ch := s.AsChannel()
	a.Send(1)
	a.Send(2)
	for _, want := range []int{1, 2} {
		select {
		case v := <-ch:
			if v != want {
				t.Fatalf("channel delivered %d, want %d", v, want)
			}
		case <-timeAfterGuard():
			t.Fatalf("channel never delivered %d", want)
		}
	}
	a.Unsubscribe(s)
	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("channel delivered %d after Unsubscribe with nothing sent", v)
		}
	case <-timeAfterGuard():
		t.Fatal("channel was not closed after Unsubscribe")
	}
}

// TestArcadia_ConcurrentSendersOverrunningSubscribers checks under real
// concurrency what the lossy-ring scenario needs from an overrun subscriber:
// no value twice, each sender's values in that sender's order, and the last
// value published is the last value received.
func TestArcadia_ConcurrentSendersOverrunningSubscribers(t *testing.T) {
	const senders, perSender, subscribers = 4, 3000, 3
	a := NewArcadia[int](8)
	var readers sync.WaitGroup
	subs := make([]*ArcadiaSub[int], subscribers)
	last := make([]int, subscribers)
	for i := range subscribers {
		subs[i] = a.Subscribe()
		last[i] = -1
		readers.Add(1)
		go func() {
			defer readers.Done()
			highest := [senders]int{-1, -1, -1, -1}
			for {
				v, ok := subs[i].Receive()
				if !ok {
					return
				}
				sender, n := v/perSender, v%perSender
				if n <= highest[sender] {
					t.Errorf("subscriber %d received sender %d's value %d after its value %d", i, sender, n, highest[sender])
					return
				}
				highest[sender] = n
				last[i] = v
			}
		}()
	}
	var writers sync.WaitGroup
	for sender := range senders {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for n := range perSender {
				a.Send(sender*perSender + n)
			}
		}()
	}
	writers.Wait()
	final := a.buffer[(a.write.Load()-1)&a.mask].Load().val
	// Unsubscribe lets each Receive loop drain what is published and stop.
	for _, s := range subs {
		a.Unsubscribe(s)
	}
	done := make(chan struct{})
	go func() { readers.Wait(); close(done) }()
	apexAwait(t, done, "subscribers draining after Unsubscribe")
	for i, v := range last {
		if v != final {
			t.Errorf("subscriber %d ended on %d, want the last value published, %d", i, v, final)
		}
	}
}

// TestArcadia_Allocations records the measured cost against the response's
// "Hot-Path Allocations" table, which claims zero for Send. Each Send
// allocates the slot entry (as delivered the slot held the value inline, and
// that was a data race).
func TestArcadia_Allocations(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	a := NewArcadia[int](64)
	s := a.Subscribe()
	if n := testing.AllocsPerRun(200, func() { a.Send(1) }); n != 1 {
		t.Errorf("Send allocates %v per call, want 1 (the slot entry)", n)
	}
	if n := testing.AllocsPerRun(200, func() { s.TryReceive() }); n != 0 {
		t.Errorf("TryReceive allocates %v per call, want 0", n)
	}
}
