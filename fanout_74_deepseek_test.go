package bigbuff

// Tests written during integration. The response's own tests established
// "the sender is blocked" by sleeping; these use completion channels only.

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func fanout74Subs(t *testing.T, f *Fanout74Deepseek[int], n int) []*Sub74Deepseek[int] {
	t.Helper()
	subs := make([]*Sub74Deepseek[int], n)
	for i := range subs {
		s, err := f.Subscribe()
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		subs[i] = s
	}
	return subs
}

func fanout74Send(f *Fanout74Deepseek[int], ctx context.Context, v int) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.Send(ctx, v) }()
	return done
}

func TestFanout74Deepseek_SendWaitsForEveryReader(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	subs := fanout74Subs(t, f, 3)
	if f.Len() != 3 {
		t.Fatalf("Len = %d, want 3", f.Len())
	}
	done := fanout74Send(f, context.Background(), 7)
	for _, s := range subs[:2] {
		if v, ok := s.Next(context.Background()); !ok || v != 7 {
			t.Fatalf("Next = (%d, %v), want (7, true)", v, ok)
		}
	}
	apexStillBlocked(t, done, "Send with one subscriber yet to read")
	if v, ok := subs[2].Next(context.Background()); !ok || v != 7 {
		t.Fatalf("Next = (%d, %v), want (7, true)", v, ok)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	case <-timeAfterGuard():
		t.Fatal("Send did not return after every subscriber read")
	}
	for i, s := range subs {
		if _, ok := s.Take(); ok {
			t.Fatalf("subscriber %d: Take returned a second value", i)
		}
	}
}

func TestFanout74Deepseek_SubscriberCloseReleasesSend(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	subs := fanout74Subs(t, f, 2)
	done := fanout74Send(f, context.Background(), 1)
	if v, ok := subs[0].Next(context.Background()); !ok || v != 1 {
		t.Fatalf("Next = (%d, %v), want (1, true)", v, ok)
	}
	apexStillBlocked(t, done, "Send with a silent subscriber")
	if err := subs[1].Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := subs[1].Close(); !errors.Is(err, ErrSubClosed74Deepseek) {
		t.Fatalf("second Close = %v, want ErrSubClosed74Deepseek", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	case <-timeAfterGuard():
		t.Fatal("Send was not released by the subscriber closing")
	}
	if f.Len() != 1 {
		t.Fatalf("Len = %d, want 1", f.Len())
	}
}

// Recorded design property: a Next abandoned on its context has not read the
// value, so it does not release the sender (the response's prose says
// otherwise).
func TestFanout74Deepseek_AbandonedNextDoesNotReleaseSend(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	sub := fanout74Subs(t, f, 1)[0]

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	// No value is pending, so Next can only return on the cancelled context.
	if _, ok := sub.Next(cancelled); ok {
		t.Fatal("Next returned a value before any Send")
	}
	done := fanout74Send(f, context.Background(), 5)
	apexStillBlocked(t, done, "Send after an abandoned Next")
	if v, ok := sub.Next(context.Background()); !ok || v != 5 {
		t.Fatalf("Next = (%d, %v), want (5, true)", v, ok)
	}
	apexAwait(t, done, "Send")
}

// Recorded design property: a Send abandoned on its context leaves the value
// in the subscriber's single-slot mailbox, where the next Send overwrites it.
func TestFanout74Deepseek_SenderCancelThenOverwrite(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	sub := fanout74Subs(t, f, 1)[0]

	ctx, cancel := context.WithCancel(context.Background())
	done := fanout74Send(f, ctx, 1)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send = %v, want context.Canceled", err)
		}
	case <-timeAfterGuard():
		t.Fatal("Send ignored its context")
	}

	done = fanout74Send(f, context.Background(), 2)
	var seen []int
	for len(seen) == 0 || seen[len(seen)-1] != 2 {
		v, ok := sub.Next(context.Background())
		if !ok {
			t.Fatal("Next failed")
		}
		seen = append(seen, v)
	}
	apexAwait(t, done, "second Send")
	if len(seen) > 2 || (len(seen) == 2 && seen[0] != 1) {
		t.Fatalf("subscriber saw %v, want [2] or [1 2]", seen)
	}
}

func TestFanout74Deepseek_Close(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	subs := fanout74Subs(t, f, 2)

	// A Send in flight is released by Close; closed subscribers count as
	// departed, so it reports success.
	done := fanout74Send(f, context.Background(), 1)
	if v, ok := subs[0].Next(context.Background()); !ok || v != 1 {
		t.Fatalf("Next = (%d, %v), want (1, true)", v, ok)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("in-flight Send = %v, want nil", err)
		}
	case <-timeAfterGuard():
		t.Fatal("Close did not release the in-flight Send")
	}

	if err := f.Close(); !errors.Is(err, ErrFanoutClosed74Deepseek) {
		t.Fatalf("second Close = %v, want ErrFanoutClosed74Deepseek", err)
	}
	if err := f.Send(context.Background(), 2); !errors.Is(err, ErrFanoutClosed74Deepseek) {
		t.Fatalf("Send after Close = %v, want ErrFanoutClosed74Deepseek", err)
	}
	if _, err := f.Subscribe(); !errors.Is(err, ErrFanoutClosed74Deepseek) {
		t.Fatalf("Subscribe after Close = %v, want ErrFanoutClosed74Deepseek", err)
	}
	for i, s := range subs {
		if !s.Closed() {
			t.Fatalf("subscriber %d not closed", i)
		}
		if _, ok := s.Next(context.Background()); ok {
			t.Fatalf("subscriber %d: Next succeeded after Close", i)
		}
	}
	if f.Len() != 0 {
		t.Fatalf("Len = %d, want 0", f.Len())
	}
}

func TestFanout74Deepseek_Iter(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	sub := fanout74Subs(t, f, 1)[0]
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for v := 1; v <= 5; v++ {
			if f.Send(context.Background(), v) != nil {
				return
			}
		}
	}()
	want := 1
	for v := range sub.Iter(context.Background()) {
		if v != want {
			t.Fatalf("Iter yielded %d, want %d", v, want)
		}
		if want++; want == 4 {
			break
		}
	}
	// Breaking out does not close the subscriber: it still owes reads.
	if sub.Closed() {
		t.Fatal("breaking out of Iter closed the subscriber")
	}
	for ; want <= 5; want++ {
		if v, ok := sub.Next(context.Background()); !ok || v != want {
			t.Fatalf("Next = (%d, %v), want (%d, true)", v, ok, want)
		}
	}
	apexAwait(t, sent, "sender")
}

// Senders are serialised by the fanout: with several concurrent senders and
// subscribers every subscriber observes every value exactly once, in one
// global order that preserves each sender's order.
func TestFanout74Deepseek_ConcurrentSendersLossless(t *testing.T) {
	const senders, perSender, readers = 4, 300, 3
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	subs := fanout74Subs(t, f, readers)

	orders := make([][]int, readers)
	var rwg sync.WaitGroup
	for i, s := range subs {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for len(orders[i]) < senders*perSender {
				v, ok := s.Next(context.Background())
				if !ok {
					return
				}
				orders[i] = append(orders[i], v)
			}
		}()
	}
	var swg sync.WaitGroup
	for s := range senders {
		swg.Add(1)
		go func() {
			defer swg.Done()
			for n := range perSender {
				if err := f.Send(context.Background(), s*perSender+n); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		}()
	}
	all := make(chan struct{})
	go func() { swg.Wait(); rwg.Wait(); close(all) }()
	apexAwait(t, all, "senders and readers")

	for i, order := range orders {
		if len(order) != senders*perSender {
			t.Fatalf("reader %d saw %d values, want %d", i, len(order), senders*perSender)
		}
		var next [senders]int
		for _, v := range order {
			s, n := v/perSender, v%perSender
			if n != next[s] {
				t.Fatalf("reader %d: sender %d value %d arrived when %d was due", i, s, n, next[s])
			}
			next[s]++
		}
		for j := range order {
			if order[j] != orders[0][j] {
				t.Fatalf("readers 0 and %d disagree on the global order at %d", i, j)
			}
		}
	}
}

func TestFanout74Deepseek_NoSubscribers(t *testing.T) {
	f := NewFanout74Deepseek[int]()
	defer f.Close()
	if err := f.Send(context.Background(), 1); err != nil {
		t.Fatalf("Send with no subscribers: %v", err)
	}
	// A subscriber joining afterwards starts with an empty mailbox.
	sub := fanout74Subs(t, f, 1)[0]
	if _, ok := sub.Take(); ok {
		t.Fatal("late subscriber observed an earlier value")
	}
}

func TestFanout74Deepseek_SteadyStateZeroAlloc(t *testing.T) {
	if raceEnabled {
		// The -race build stores each value in an allocated cell by design.
		t.Skip("the zero-allocation claim is made for the default build only")
	}
	f := NewFanout74Deepseek[int]()
	sub := fanout74Subs(t, f, 1)[0]
	read := make(chan struct{})
	go func() {
		defer close(read)
		for {
			if _, ok := sub.Next(context.Background()); !ok {
				return
			}
		}
	}()
	if n := testing.AllocsPerRun(500, func() {
		if err := f.Send(context.Background(), 2); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Errorf("Send to one reading subscriber allocated %v objects per call, want 0", n)
	}
	f.Close()
	apexAwait(t, read, "reader")
}
