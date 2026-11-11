package bigbuff

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type baselineBarrierResult struct {
	acked int
	err   error
}

func baselineBarrierSend(b *BaselineDeadlineBarrier[int], ctx context.Context, v int) <-chan baselineBarrierResult {
	out := make(chan baselineBarrierResult, 1)
	go func() {
		n, err := b.Send(ctx, v)
		out <- baselineBarrierResult{n, err}
	}()
	return out
}

func baselineBarrierAwait(t *testing.T, ch <-chan baselineBarrierResult, what string) baselineBarrierResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-timeAfterGuard():
		t.Fatalf("%s: Send did not return", what)
		return baselineBarrierResult{}
	}
}

func baselineBarrierJoin(t *testing.T, b *BaselineDeadlineBarrier[int]) *BaselineDeadlineMember[int] {
	t.Helper()
	m, ok := b.Join()
	if !ok {
		t.Fatal("Join failed on an open barrier")
	}
	return m
}

func baselineBarrierRecv(t *testing.T, m *BaselineDeadlineMember[int], want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
	defer cancel()
	if v, ok := m.Recv(ctx); !ok || v != want {
		t.Fatalf("Recv = (%d, %v), want (%d, true)", v, ok, want)
	}
}

func TestBaselineDeadlineBarrier_NoMembers(t *testing.T) {
	b := NewBaselineDeadlineBarrier[int]()
	if n, err := b.Send(context.Background(), 1); n != 0 || err != nil {
		t.Fatalf("Send with no members = (%d, %v), want (0, nil)", n, err)
	}
}

func TestBaselineDeadlineBarrier_SendWaitsForEveryAck(t *testing.T) {
	b := NewBaselineDeadlineBarrier[int]()
	members := []*BaselineDeadlineMember[int]{baselineBarrierJoin(t, b), baselineBarrierJoin(t, b), baselineBarrierJoin(t, b)}
	sent := baselineBarrierSend(b, context.Background(), 7)
	// Members receive concurrently (the sender hands over in map order).
	var wg sync.WaitGroup
	for _, m := range members {
		wg.Add(1)
		go func() { defer wg.Done(); baselineBarrierRecv(t, m, 7) }()
	}
	wg.Wait()
	for i, m := range members {
		apexStillBlocked(t, sent, "Send, before every member acknowledged")
		m.Ack()
		m.Ack() // a second Ack for the same value is a no-op
		_ = i
	}
	if r := baselineBarrierAwait(t, sent, "after every acknowledgement"); r.acked != 3 || r.err != nil {
		t.Fatalf("Send = (%d, %v), want (3, nil)", r.acked, r.err)
	}
}

func TestBaselineDeadlineBarrier_LeavingReleasesTheSender(t *testing.T) {
	t.Run("before receiving", func(t *testing.T) {
		b := NewBaselineDeadlineBarrier[int]()
		m := baselineBarrierJoin(t, b)
		sent := baselineBarrierSend(b, context.Background(), 1)
		apexStillBlocked(t, sent, "Send, with a member that has not received")
		m.Leave()
		m.Leave() // idempotent
		if r := baselineBarrierAwait(t, sent, "after the member left"); r.acked != 0 || r.err != nil {
			t.Fatalf("Send = (%d, %v), want (0, nil)", r.acked, r.err)
		}
		if v, ok := m.Recv(context.Background()); ok {
			t.Fatalf("Recv after Leave returned %d", v)
		}
	})
	t.Run("after receiving, without acknowledging", func(t *testing.T) {
		b := NewBaselineDeadlineBarrier[int]()
		stays, leaves := baselineBarrierJoin(t, b), baselineBarrierJoin(t, b)
		sent := baselineBarrierSend(b, context.Background(), 1)
		var wg sync.WaitGroup
		for _, m := range []*BaselineDeadlineMember[int]{stays, leaves} {
			wg.Add(1)
			go func() { defer wg.Done(); baselineBarrierRecv(t, m, 1) }()
		}
		wg.Wait()
		stays.Ack()
		apexStillBlocked(t, sent, "Send, with one member holding the value unacknowledged")
		leaves.Leave()
		if r := baselineBarrierAwait(t, sent, "after the unacknowledging member left"); r.acked != 1 || r.err != nil {
			t.Fatalf("Send = (%d, %v), want (1, nil)", r.acked, r.err)
		}
	})
}

// A member that acknowledges and then leaves did acknowledge. The sender
// sees both its acknowledgement and its departure ready at once; it used to
// pick between them at random and sometimes report 0.
func TestBaselineDeadlineBarrier_AckThenLeaveIsCounted(t *testing.T) {
	for round := range 3000 {
		b := NewBaselineDeadlineBarrier[int]()
		m := baselineBarrierJoin(t, b)
		sent := baselineBarrierSend(b, context.Background(), round)
		baselineBarrierRecv(t, m, round)
		m.Ack()
		m.Leave()
		if r := baselineBarrierAwait(t, sent, "after ack then leave"); r.acked != 1 || r.err != nil {
			t.Fatalf("round %d: Send = (%d, %v), want (1, nil)", round, r.acked, r.err)
		}
	}
}

func TestBaselineDeadlineBarrier_SenderGivesUp(t *testing.T) {
	t.Run("member never receives", func(t *testing.T) {
		b := NewBaselineDeadlineBarrier[int]()
		m := baselineBarrierJoin(t, b)
		ctx, cancel := context.WithCancel(context.Background())
		sent := baselineBarrierSend(b, ctx, 1)
		cancel()
		if r := baselineBarrierAwait(t, sent, "after the sender's context ended"); r.acked != 0 || !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Send = (%d, %v), want (0, context.Canceled)", r.acked, r.err)
		}
		// The abandoned value is never delivered: the member's next value is
		// the next Send's.
		next := baselineBarrierSend(b, context.Background(), 2)
		baselineBarrierRecv(t, m, 2)
		m.Ack()
		if r := baselineBarrierAwait(t, next, "the following Send"); r.acked != 1 || r.err != nil {
			t.Fatalf("following Send = (%d, %v), want (1, nil)", r.acked, r.err)
		}
	})
	t.Run("late acknowledgement is harmless", func(t *testing.T) {
		b := NewBaselineDeadlineBarrier[int]()
		m := baselineBarrierJoin(t, b)
		ctx, cancel := context.WithCancel(context.Background())
		sent := baselineBarrierSend(b, ctx, 1)
		baselineBarrierRecv(t, m, 1)
		cancel()
		if r := baselineBarrierAwait(t, sent, "after the sender's context ended"); r.acked != 0 || !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Send = (%d, %v), want (0, context.Canceled)", r.acked, r.err)
		}
		next := baselineBarrierSend(b, context.Background(), 2)
		m.Ack() // for value 1, after its sender gave up
		// The late acknowledgement must not satisfy the next Send.
		baselineBarrierRecv(t, m, 2)
		apexStillBlocked(t, next, "the following Send, before its own acknowledgement")
		m.Ack()
		if r := baselineBarrierAwait(t, next, "the following Send"); r.acked != 1 || r.err != nil {
			t.Fatalf("following Send = (%d, %v), want (1, nil)", r.acked, r.err)
		}
	})
}

// A member that joins while a Send is in flight is not part of it.
func TestBaselineDeadlineBarrier_JoinDuringSend(t *testing.T) {
	b := NewBaselineDeadlineBarrier[int]()
	first := baselineBarrierJoin(t, b)
	sent := baselineBarrierSend(b, context.Background(), 1)
	baselineBarrierRecv(t, first, 1)
	late := baselineBarrierJoin(t, b)
	first.Ack()
	if r := baselineBarrierAwait(t, sent, "the Send in flight during the join"); r.acked != 1 || r.err != nil {
		t.Fatalf("Send = (%d, %v), want (1, nil): the late member is not part of it", r.acked, r.err)
	}
	next := baselineBarrierSend(b, context.Background(), 2)
	var wg sync.WaitGroup
	for _, m := range []*BaselineDeadlineMember[int]{first, late} {
		wg.Add(1)
		go func() { defer wg.Done(); baselineBarrierRecv(t, m, 2); m.Ack() }()
	}
	wg.Wait()
	if r := baselineBarrierAwait(t, next, "the next Send"); r.acked != 2 || r.err != nil {
		t.Fatalf("next Send = (%d, %v), want (2, nil)", r.acked, r.err)
	}
}

func TestBaselineDeadlineBarrier_Close(t *testing.T) {
	b := NewBaselineDeadlineBarrier[int]()
	m := baselineBarrierJoin(t, b)
	sent := baselineBarrierSend(b, context.Background(), 1)
	baselineBarrierRecv(t, m, 1) // the Send is now waiting for the acknowledgement
	b.Close()
	b.Close() // idempotent
	if r := baselineBarrierAwait(t, sent, "a Send in flight across Close"); !errors.Is(r.err, ErrBaselineDeadlineBarrierClosed) {
		t.Fatalf("Send across Close = (%d, %v), want ErrBaselineDeadlineBarrierClosed", r.acked, r.err)
	}
	if v, ok := m.Recv(context.Background()); ok {
		t.Fatalf("Recv after Close returned %d", v)
	}
	if _, ok := b.Join(); ok {
		t.Fatal("Join succeeded after Close")
	}
	if _, err := b.Send(context.Background(), 2); !errors.Is(err, ErrBaselineDeadlineBarrierClosed) {
		t.Fatalf("Send after Close returned %v", err)
	}
	m.Ack()   // no-op, must not block
	m.Leave() // still allowed
}

// Concurrent senders are serialised: every member sees every value exactly
// once and acknowledges it, and every Send counts every member.
func TestBaselineDeadlineBarrier_ConcurrentSenders(t *testing.T) {
	const senders, perSender, members = 4, 200, 3
	b := NewBaselineDeadlineBarrier[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make([]map[int]int, members)
	var receivers sync.WaitGroup
	for i := range members {
		m := baselineBarrierJoin(t, b)
		seen[i] = map[int]int{}
		receivers.Add(1)
		go func() {
			defer receivers.Done()
			for {
				v, ok := m.Recv(ctx)
				if !ok {
					return
				}
				seen[i][v]++
				m.Ack()
			}
		}()
	}
	var sending sync.WaitGroup
	for s := range senders {
		sending.Add(1)
		go func() {
			defer sending.Done()
			for i := range perSender {
				if n, err := b.Send(ctx, s*perSender+i); n != members || err != nil {
					t.Errorf("Send = (%d, %v), want (%d, nil)", n, err, members)
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { sending.Wait(); close(done) }()
	apexAwait(t, done, "concurrent senders")
	cancel()
	receivers.Wait()
	for i, got := range seen {
		if len(got) != senders*perSender {
			t.Fatalf("member %d saw %d distinct values, want %d", i, len(got), senders*perSender)
		}
		for v, n := range got {
			if n != 1 {
				t.Fatalf("member %d saw value %d %d times", i, v, n)
			}
		}
	}
}
