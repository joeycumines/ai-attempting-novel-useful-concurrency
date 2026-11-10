package bigbuff

// Tests written during integration; the response shipped none. They cover
// the ring and cursor semantics, the two integration repairs (the wakeup of
// every parked link, and a slot never regressing to an older sequence), and
// the claims the response made about allocation.

import (
	"context"
	"sync"
	"testing"
)

func drain29(l *Relay29NemotronLink[int]) (values []int) {
	for {
		v, ok := l.Next()
		if !ok {
			return values
		}
		values = append(values, v)
	}
}

func TestRelay29Nemotron_SizeIsRoundedUpAndValidated(t *testing.T) {
	for want, sizes := range map[int][]int{1: {1}, 2: {2}, 4: {3, 4}, 8: {5, 8}, 16: {9}} {
		for _, size := range sizes {
			if got := len(NewRelay29Nemotron[int](size).slots); got != want {
				t.Errorf("NewRelay29Nemotron(%d) holds %d slots, want %d", size, got, want)
			}
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("NewRelay29Nemotron(0) did not panic")
		}
	}()
	NewRelay29Nemotron[int](0)
}

func TestRelay29Nemotron_OrderedWithinCapacity(t *testing.T) {
	r := NewRelay29Nemotron[int](8)
	a, b := r.Subscribe(), r.Subscribe()
	if _, ok := a.Next(); ok {
		t.Fatal("Next returned a value before any Publish")
	}
	for v := 1; v <= 8; v++ {
		if seq := r.Publish(v * 10); seq != uint64(v) {
			t.Fatalf("Publish returned sequence %d, want %d", seq, v)
		}
	}
	if a.Lag() != 8 || r.Seq() != 8 {
		t.Fatalf("Lag = %d, Seq = %d before reading, want 8 and 8", a.Lag(), r.Seq())
	}
	for _, l := range []*Relay29NemotronLink[int]{a, b} { // links are independent
		got := drain29(l)
		if len(got) != 8 {
			t.Fatalf("read %v, want all 8 values", got)
		}
		for i, v := range got {
			if v != (i+1)*10 {
				t.Fatalf("read %v, want 10..80 in order", got)
			}
		}
		if l.Seq() != 8 || l.Lag() != 0 {
			t.Fatalf("after draining: Seq = %d, Lag = %d", l.Seq(), l.Lag())
		}
	}
}

// TestRelay29Nemotron_OverrunJumpsForward pins the conflation rule: a link
// looks only at the slot of its next sequence, and takes whatever newer
// sequence that slot now holds. Values still held elsewhere in the ring are
// skipped by the jump; the link never goes backwards and always ends on the
// latest publication.
func TestRelay29Nemotron_OverrunJumpsForward(t *testing.T) {
	r := NewRelay29Nemotron[int](4)
	l := r.Subscribe()
	for v := 1; v <= 10; v++ {
		r.Publish(v)
	}
	if lag := l.Lag(); lag < 4 {
		t.Fatalf("Lag = %d after overrunning a 4-slot ring by 6, want >= 4 (values were dropped)", lag)
	}
	// Slot 0 was written by sequences 1, 5 and 9: the link, wanting sequence
	// 1, finds 9 there and jumps, skipping 7 and 8 which the ring still holds.
	got := drain29(l)
	if len(got) != 2 || got[0] != 9 || got[1] != 10 {
		t.Fatalf("read %v after overrun, want [9 10]", got)
	}
	if l.Lag() != 0 {
		t.Fatalf("Lag = %d after draining, want 0", l.Lag())
	}
}

func TestRelay29Nemotron_StartingPoints(t *testing.T) {
	r := NewRelay29Nemotron[int](8)
	for v := 1; v <= 3; v++ {
		r.Publish(v)
	}
	// Subscribe is SubscribeFrom(0): it reads what the ring still holds.
	if got := drain29(r.Subscribe()); len(got) != 3 || got[0] != 1 {
		t.Fatalf("Subscribe read %v, want the 3 retained values", got)
	}
	latest, from := r.SubscribeLatest(), r.SubscribeFrom(1)
	if got := drain29(latest); len(got) != 0 {
		t.Fatalf("SubscribeLatest read %v, want nothing published before the call", got)
	}
	if got := drain29(from); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("SubscribeFrom(1) read %v, want [2 3]", got)
	}
	r.Publish(4)
	if got := drain29(latest); len(got) != 1 || got[0] != 4 {
		t.Fatalf("SubscribeLatest read %v after one more Publish, want [4]", got)
	}
}

// TestRelay29Nemotron_EveryParkedLinkWakes is the regression test for the
// WaitNext repair. As delivered, all links waited on one doorbell token, so
// each Publish woke one of them. Each round here publishes one value and
// needs every link to return it; the links that return first go straight
// back into WaitNext and are parked by the time the next round publishes.
func TestRelay29Nemotron_EveryParkedLinkWakes(t *testing.T) {
	const links, rounds = 3, 300
	r := NewRelay29Nemotron[int](8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan int, links)
	var readers sync.WaitGroup
	for range links {
		l := r.SubscribeLatest()
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				v, ok := l.WaitNext(ctx)
				if !ok {
					return
				}
				got <- v
			}
		}()
	}
	for round := range rounds {
		r.Publish(round)
		for i := range links {
			select {
			case v := <-got:
				if v != round {
					t.Fatalf("round %d: a link received %d", round, v)
				}
			case <-timeAfterGuard():
				t.Fatalf("round %d: only %d of %d links blocked in WaitNext were woken", round, i, links)
			}
		}
	}
	cancel()
	done := make(chan struct{})
	go func() { readers.Wait(); close(done) }()
	apexAwait(t, done, "cancelled links leaving WaitNext")
}

func TestRelay29Nemotron_WaitNextAndIterHonourContext(t *testing.T) {
	r := NewRelay29Nemotron[int](4)
	l := r.Subscribe()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := l.WaitNext(cancelled); ok {
		t.Fatal("WaitNext returned a value from an empty relay")
	}
	// An available value wins over a cancelled context: Next is tried first.
	r.Publish(7)
	if v, ok := l.WaitNext(cancelled); !ok || v != 7 {
		t.Fatalf("WaitNext = %d, %v with a value available, want 7, true", v, ok)
	}

	r.Publish(8)
	r.Publish(9)
	var seen []int
	for v := range l.Iter(context.Background()) {
		seen = append(seen, v)
		if len(seen) == 2 {
			break // stopping the range must return without another Publish
		}
	}
	if len(seen) != 2 || seen[0] != 8 || seen[1] != 9 {
		t.Fatalf("Iter yielded %v, want [8 9]", seen)
	}
	for range l.Iter(cancelled) {
		t.Fatal("Iter yielded from a drained relay with a cancelled context")
	}
}

// TestRelay29Nemotron_NotifyIsOneCoalescedToken pins what Notify is: a single
// pending token however many publications there were, and a channel WaitNext
// no longer consumes.
func TestRelay29Nemotron_NotifyIsOneCoalescedToken(t *testing.T) {
	r := NewRelay29Nemotron[int](4)
	select {
	case <-r.Notify():
		t.Fatal("Notify signalled before any Publish")
	default:
	}
	l := r.Subscribe()
	for v := range 3 {
		r.Publish(v)
	}
	if _, ok := l.WaitNext(context.Background()); !ok {
		t.Fatal("WaitNext found no value")
	}
	select {
	case <-r.Notify():
	default:
		t.Fatal("the doorbell token was consumed by WaitNext")
	}
	select {
	case <-r.Notify():
		t.Fatal("three publications left more than one doorbell token")
	default:
	}
}

// TestRelay29Nemotron_SlotNeverRegresses is the regression test for the
// store repair. It plays the interleaving of two concurrent publishers
// directly: the first reserves its sequence and is delayed; the second
// reserves the next sequence, which maps to the same slot of a one-slot ring,
// and completes; then the first completes. As delivered the slot ended up
// holding the older sequence, and the link could never reach the newer one.
func TestRelay29Nemotron_SlotNeverRegresses(t *testing.T) {
	r := NewRelay29Nemotron[int](1)
	l := r.Subscribe()
	delayed := r.pubSeq.Add(1) // first publisher: sequence reserved, not yet stored
	if seq := r.Publish(20); seq != delayed+1 {
		t.Fatalf("second publisher got sequence %d", seq)
	}
	r.store(delayed, 10) // first publisher resumes
	if v, ok := l.Next(); !ok || v != 20 {
		t.Fatalf("Next = %d, %v; want the newer publication 20", v, ok)
	}
	if lag := l.Lag(); lag != 0 {
		t.Fatalf("Lag = %d with the latest publication consumed, want 0", lag)
	}
	if _, ok := l.Next(); ok {
		t.Fatal("Next returned a second value")
	}
}

// TestRelay29Nemotron_ConcurrentPublishersAndLinks checks, under real
// concurrency, the two properties the lossy scenario relies on: a link's
// sequence only ever increases, and once publishing stops every link reaches
// the last publication without needing another one to nudge it.
func TestRelay29Nemotron_ConcurrentPublishersAndLinks(t *testing.T) {
	const publishers, perPublisher, links = 4, 2000, 3
	r := NewRelay29Nemotron[int](4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readers sync.WaitGroup
	caughtUp := make(chan struct{}, links)
	for range links {
		l := r.Subscribe()
		readers.Add(1)
		go func() {
			defer readers.Done()
			last := uint64(0)
			for {
				if _, ok := l.WaitNext(ctx); !ok {
					return
				}
				if seq := l.Seq(); seq <= last {
					t.Errorf("link sequence went from %d to %d", last, seq)
					return
				} else {
					last = seq
				}
				if last == publishers*perPublisher {
					caughtUp <- struct{}{}
					return
				}
			}
		}()
	}
	var writers sync.WaitGroup
	for p := range publishers {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range perPublisher {
				r.Publish(p*perPublisher + i)
			}
		}()
	}
	writers.Wait()
	for i := range links {
		select {
		case <-caughtUp:
		case <-timeAfterGuard():
			t.Fatalf("only %d of %d links reached the final publication after publishing stopped", i, links)
		}
	}
	cancel()
	readers.Wait()
}

// TestRelay29Nemotron_Allocations records the measured cost against the
// response's claims. Next allocates nothing, as claimed. Publish was claimed
// to allocate nothing; the integrated version boxes the value (the slot holds
// an atomic pointer so that a reader's snapshot cannot race a writer), which
// is one allocation per publication.
func TestRelay29Nemotron_Allocations(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	r := NewRelay29Nemotron[int](64)
	l := r.Subscribe()
	if n := testing.AllocsPerRun(200, func() { r.Publish(1) }); n != 1 {
		t.Errorf("Publish allocates %v per call, want 1 (the boxed value)", n)
	}
	if n := testing.AllocsPerRun(200, func() { l.Next() }); n != 0 {
		t.Errorf("Next allocates %v per call, want 0", n)
	}
}
