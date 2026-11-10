package bigbuff

// Tests written during integration. The response's own test file did not
// compile (an unused variable); these replace it and additionally pin the two
// integration repairs (Close during a barrier, Snapshot value/epoch pairing).

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

type cohort73Result struct {
	n   int
	err error
}

func cohort73Broadcast(c *Cohort73Meta[int], ctx context.Context, v int) <-chan cohort73Result {
	done := make(chan cohort73Result, 1)
	go func() {
		n, err := c.Broadcast(ctx, v)
		done <- cohort73Result{n, err}
	}()
	return done
}

func cohort73Join(t *testing.T, c *Cohort73Meta[int], n int) []*Member73Meta[int] {
	t.Helper()
	ms := make([]*Member73Meta[int], n)
	for i := range ms {
		m, ok := c.Join()
		if !ok {
			t.Fatal("Join failed on an open cohort")
		}
		ms[i] = m
	}
	return ms
}

func cohort73Expect(t *testing.T, done <-chan cohort73Result, n int, err error, what string) {
	t.Helper()
	select {
	case r := <-done:
		if r.n != n || !errors.Is(r.err, err) {
			t.Fatalf("%s = (%d, %v), want (%d, %v)", what, r.n, r.err, n, err)
		}
	case <-timeAfterGuard():
		t.Fatalf("%s did not return", what)
	}
}

func TestCohort73Meta_BarrierWaitsForEveryAck(t *testing.T) {
	c := NewCohort73Meta[int]()
	defer c.Close()
	ms := cohort73Join(t, c, 3)
	done := cohort73Broadcast(c, context.Background(), 42)

	epochs := make([]uint64, len(ms))
	for i, m := range ms {
		v, e, err := m.Wait(context.Background())
		if err != nil || v != 42 || e != 1 {
			t.Fatalf("Wait = (%d, %d, %v), want (42, 1, nil)", v, e, err)
		}
		epochs[i] = e
	}
	for i, m := range ms {
		apexStillBlocked(t, done, "Broadcast with acknowledgements outstanding")
		if c.Pending() != len(ms)-i {
			t.Fatalf("Pending = %d, want %d", c.Pending(), len(ms)-i)
		}
		if !m.Ack(epochs[i]) {
			t.Fatalf("member %d: Ack of the in-flight epoch reported false", i)
		}
		if m.Ack(epochs[i]) {
			t.Fatalf("member %d: duplicate Ack reported true", i)
		}
	}
	cohort73Expect(t, done, 3, nil, "Broadcast")
	c.CheckInvariants()
	if c.Pending() != 0 || c.Epoch() != 1 || c.Members() != 3 {
		t.Fatalf("Pending=%d Epoch=%d Members=%d, want 0, 1, 3", c.Pending(), c.Epoch(), c.Members())
	}
}

func TestCohort73Meta_LeaveCompensates(t *testing.T) {
	c := NewCohort73Meta[int]()
	defer c.Close()
	ms := cohort73Join(t, c, 2)
	done := cohort73Broadcast(c, context.Background(), 1)
	if _, _, err := ms[0].Next(context.Background()); err != nil {
		t.Fatalf("Next: %v", err)
	}
	apexStillBlocked(t, done, "Broadcast with a silent member")
	ms[1].Leave()
	ms[1].Leave() // idempotent
	cohort73Expect(t, done, 2, nil, "Broadcast")
	c.CheckInvariants()
	if c.Members() != 1 || !ms[1].IsLeft() {
		t.Fatalf("Members=%d IsLeft=%v, want 1, true", c.Members(), ms[1].IsLeft())
	}
	if _, _, err := ms[1].Wait(context.Background()); err == nil {
		t.Fatal("Wait succeeded for a member that left")
	}
}

// A sender deadline abandons the barrier; an acknowledgement of the abandoned
// epoch that arrives during the next barrier must not count towards it.
func TestCohort73Meta_AbandonedBarrierAndStaleAck(t *testing.T) {
	c := NewCohort73Meta[int]()
	defer c.Close()
	ms := cohort73Join(t, c, 2)

	ctx, cancel := context.WithCancel(context.Background())
	done := cohort73Broadcast(c, ctx, 1)
	if _, _, err := ms[0].Next(context.Background()); err != nil { // waits and acks epoch 1
		t.Fatalf("Next: %v", err)
	}
	_, stale, err := ms[1].Wait(context.Background()) // receives epoch 1, does not ack
	if err != nil || stale != 1 {
		t.Fatalf("Wait = (_, %d, %v), want epoch 1", stale, err)
	}
	apexStillBlocked(t, done, "Broadcast before its deadline")
	cancel()
	cohort73Expect(t, done, 0, context.Canceled, "abandoned Broadcast")
	c.CheckInvariants()

	done = cohort73Broadcast(c, context.Background(), 2)
	// ms[0] has acked epoch 1, so this Wait returns only once epoch 2 is published.
	v, e, err := ms[0].Wait(context.Background())
	if err != nil || v != 2 || e != 2 {
		t.Fatalf("Wait = (%d, %d, %v), want (2, 2, nil)", v, e, err)
	}
	if ms[1].Ack(stale) {
		t.Fatal("Ack of an abandoned epoch reported that it counted")
	}
	apexStillBlocked(t, done, "Broadcast after a stale acknowledgement")
	if c.Pending() != 2 {
		t.Fatalf("Pending = %d after a stale acknowledgement, want 2", c.Pending())
	}
	ms[0].Ack(e)
	apexStillBlocked(t, done, "Broadcast with one acknowledgement outstanding")
	// The member that stalled conflates to the latest value (recorded property).
	if v, e, err := ms[1].Next(context.Background()); err != nil || v != 2 || e != 2 {
		t.Fatalf("Next = (%d, %d, %v), want (2, 2, nil)", v, e, err)
	}
	cohort73Expect(t, done, 2, nil, "Broadcast")
	c.CheckInvariants()
}

// Integration repair: Close during the acknowledgement wait used to close the
// barrier channel itself, so Broadcast reported (n, nil) although nobody had
// acknowledged.
func TestCohort73Meta_CloseDuringBarrierReportsClosed(t *testing.T) {
	for range 2000 {
		c := NewCohort73Meta[int]()
		cohort73Join(t, c, 2) // members that never acknowledge
		done := cohort73Broadcast(c, context.Background(), 1)
		c.Close()
		cohort73Expect(t, done, 0, ErrClosed73Meta, "Broadcast racing Close")
	}
}

// Integration repair: the epoch was advanced after the value lock was
// released, so Snapshot could pair the new value with the previous epoch.
func TestCohort73Meta_SnapshotPairsValueWithEpoch(t *testing.T) {
	const rounds = 3000
	c := NewCohort73Meta[uint64]()
	defer c.Close()
	acker, _ := c.Join()
	observer, _ := c.Join()
	observer.Leave() // a departed member's Snapshot reports false
	if _, _, ok := observer.Snapshot(); ok {
		t.Fatal("Snapshot succeeded for a member that left")
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // acknowledges every epoch so the publisher can proceed
		defer wg.Done()
		for {
			if _, _, err := acker.Next(context.Background()); err != nil {
				return
			}
		}
	}()
	go func() { // Snapshot is a read-only observation, safe beside Next
		defer wg.Done()
		for !stop.Load() {
			if v, e, ok := acker.Snapshot(); ok && v != e {
				t.Errorf("Snapshot paired value %d with epoch %d", v, e)
				return
			}
		}
	}()
	for i := uint64(1); i <= rounds; i++ {
		if n, err := c.Broadcast(context.Background(), i); n != 1 || err != nil {
			t.Fatalf("Broadcast %d = (%d, %v), want (1, nil)", i, n, err)
		}
	}
	stop.Store(true)
	c.Close()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "member goroutines")
}

func TestCohort73Meta_AllAcksOnBreak(t *testing.T) {
	c := NewCohort73Meta[int]()
	defer c.Close()
	m := cohort73Join(t, c, 1)[0]
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for v := 1; v <= 2; v++ {
			if n, err := c.Broadcast(context.Background(), v); n != 1 || err != nil {
				t.Errorf("Broadcast %d = (%d, %v), want (1, nil)", v, n, err)
			}
		}
	}()
	want := 1
	for v := range m.All(context.Background()) {
		if v != want {
			t.Fatalf("All yielded %d, want %d", v, want)
		}
		if want++; want == 3 {
			break // the value in hand is acknowledged on the way out
		}
	}
	apexAwait(t, sent, "publisher")
	if m.Acked() != 2 {
		t.Fatalf("Acked = %d, want 2", m.Acked())
	}
}

func TestCohort73Meta_TryBroadcastBusy(t *testing.T) {
	c := NewCohort73Meta[int]()
	defer c.Close()
	if n, err := c.TryBroadcast(1); n != 0 || err != nil {
		t.Fatalf("TryBroadcast with no members = (%d, %v), want (0, nil)", n, err)
	}
	m := cohort73Join(t, c, 1)[0]
	done := cohort73Broadcast(c, context.Background(), 2)
	_, e, err := m.Wait(context.Background()) // the broadcast is now in flight
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n, err := c.TryBroadcast(3); n != 0 || !errors.Is(err, ErrBusy73Meta) {
		t.Fatalf("TryBroadcast during a barrier = (%d, %v), want (0, ErrBusy73Meta)", n, err)
	}
	m.Ack(e)
	cohort73Expect(t, done, 1, nil, "Broadcast")
}

func TestCohort73Meta_Closed(t *testing.T) {
	c := NewCohort73Meta[int]()
	m := cohort73Join(t, c, 1)[0]
	waited := make(chan error, 1)
	go func() { _, _, err := m.Wait(context.Background()); waited <- err }()
	c.Close()
	c.Close() // idempotent
	select {
	case err := <-waited:
		if !errors.Is(err, ErrClosed73Meta) {
			t.Fatalf("Wait = %v, want ErrClosed73Meta", err)
		}
	case <-timeAfterGuard():
		t.Fatal("Close did not release Wait")
	}
	if _, ok := c.Join(); ok {
		t.Fatal("Join succeeded on a closed cohort")
	}
	if n, err := c.Broadcast(context.Background(), 1); n != 0 || !errors.Is(err, ErrClosed73Meta) {
		t.Fatalf("Broadcast = (%d, %v), want (0, ErrClosed73Meta)", n, err)
	}
	if n, err := c.TryBroadcast(1); n != 0 || !errors.Is(err, ErrClosed73Meta) {
		t.Fatalf("TryBroadcast = (%d, %v), want (0, ErrClosed73Meta)", n, err)
	}
	select {
	case <-c.Closed():
	default:
		t.Fatal("Closed channel not closed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := m.Wait(ctx); err == nil {
		t.Fatal("Wait succeeded on a closed cohort")
	}
}

// Churn: members join, take a few values and leave while a publisher runs.
// Every Broadcast must return, and a member that stays for the whole run must
// see a strictly increasing sequence of values.
func TestCohort73Meta_ChurnNeverStallsPublisher(t *testing.T) {
	const rounds, churners = 500, 4
	c := NewCohort73Meta[int]()
	defer c.Close()
	steady := cohort73Join(t, c, 1)[0]

	var wg sync.WaitGroup
	var stop atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		last := 0
		for {
			v, _, err := steady.Next(context.Background())
			if err != nil {
				return
			}
			if v <= last {
				t.Errorf("steady member saw %d after %d", v, last)
				return
			}
			last = v
		}
	}()
	for range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // the churner never blocks: it takes what is there and leaves
			for !stop.Load() {
				m, ok := c.Join()
				if !ok {
					return
				}
				m.Next(ctx)
				m.Leave()
			}
		}()
	}
	for v := 1; v <= rounds; v++ {
		done := cohort73Broadcast(c, context.Background(), v)
		select {
		case r := <-done:
			if r.err != nil || r.n < 1 {
				t.Fatalf("Broadcast %d = (%d, %v), want at least the steady member and no error", v, r.n, r.err)
			}
		case <-timeAfterGuard():
			t.Fatalf("Broadcast %d stalled under churn", v)
		}
	}
	stop.Store(true)
	c.Close()
	all := make(chan struct{})
	go func() { wg.Wait(); close(all) }()
	apexAwait(t, all, "members")
	c.CheckInvariants()
}
