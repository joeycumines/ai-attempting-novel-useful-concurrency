package bigbuff

import (
	"context"
	"testing"
	"time"
)

// The 08-MuseSpark beacon family must capture its subscription point when
// SubscribeContext is called, not when the returned iterator is first invoked.
// Otherwise a publisher can slip a value into the registration/iteration gap;
// for ReliableBeacon that also leaves the new subscription counted in an ack
// barrier for a value it will never observe.

func TestBeaconSubscribeContextCapturesCallPoint(t *testing.T) {
	b := NewBeacon[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	it := b.SubscribeContext(ctx)
	b.Send(41)
	got := make(chan int, 1)
	go func() {
		it(func(v int) bool {
			got <- v
			return false
		})
	}()
	select {
	case v := <-got:
		if v != 41 {
			t.Fatalf("got %d want 41", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("iterator missed the value published before first invocation")
	}
}

func TestRingBeaconSubscribeContextCapturesCallPoint(t *testing.T) {
	b := NewRingBeacon[int](4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	it := b.SubscribeContext(ctx)
	b.Send(7)
	got := make(chan int, 1)
	go func() {
		it(func(v int) bool {
			got <- v
			return false
		})
	}()
	select {
	case v := <-got:
		if v != 7 {
			t.Fatalf("got %d want 7", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("iterator missed the value published before first invocation")
	}
}

// RingBeacon must not lose a wakeup when Send lands between the iterator's
// empty Next check and its Ready capture. Capacity exceeds the published volume,
// so every value is retained and the exact ordered sequence is the invariant.
func TestRingBeaconSubscribeContextNoLostWakeup(t *testing.T) {
	const n = 512
	b := NewRingBeacon[int](1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	it := b.SubscribeContext(ctx)
	got := make(chan int, n)
	go func() {
		it(func(v int) bool {
			got <- v
			return v != n
		})
		close(got)
	}()
	for v := 1; v <= n; v++ {
		b.Send(v)
	}
	for want := 1; want <= n; want++ {
		select {
		case v, ok := <-got:
			if !ok {
				t.Fatalf("iterator stopped before value %d", want)
			}
			if v != want {
				t.Fatalf("got %d want %d", v, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("lost wakeup or stall before value %d", want)
		}
	}
}

func TestReliableBeaconSubscribeContextCapturesArmedRound(t *testing.T) {
	x := NewReliableBeacon[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	it := x.SubscribeContext(ctx)
	sent := make(chan int, 1)
	go func() { sent <- x.Send(99) }()
	got := make(chan int, 1)
	go func() {
		it(func(v int) bool {
			got <- v
			return false
		})
	}()
	select {
	case v := <-got:
		if v != 99 {
			t.Fatalf("got %d want 99", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not observe the round armed after registration")
	}
	select {
	case n := <-sent:
		if n != 1 {
			t.Fatalf("Send=%d want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send ack barrier was not released")
	}
}

// A Send racing SubscribeContext's registration must never block on the new
// subscription. The round and the registration are ordered by sendingMu, but a
// registration that read its start point after releasing the lock could be
// counted in a round whose value it then skipped, and that Send never returned.
// The invariant holds on every interleaving of the fixed code, so the test cannot
// flake in the passing direction; a failure is a regression.
func TestReliableBeaconSubscribeRacesSend(t *testing.T) {
	const rounds = 20000
	for i := 0; i < rounds; i++ {
		x := NewReliableBeacon[int]()
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		sent := make(chan struct{})
		go func() {
			<-started
			x.Send(1)
			close(sent)
		}()
		close(started)
		it := x.SubscribeContext(ctx)
		iterDone := make(chan struct{})
		go func() {
			defer close(iterDone)
			it(func(int) bool { return false })
		}()
		select {
		case <-sent:
		case <-time.After(scenarioGuard):
			cancel()
			t.Fatalf("round %d: Send blocked on a subscription registered beside it", i)
		}
		cancel()
		<-iterDone
	}
}

// A tracked wait with no armed round must return once its context is cancelled,
// and must acknowledge nothing. The waiter parks in ackCond.Wait, so the wake is
// the context broadcast. The outcome is the same whichever side of the park the
// cancellation lands on, so the test is deterministic.
func TestReliableBeaconWaitWithoutRoundHonoursCancellation(t *testing.T) {
	x := NewReliableBeacon[int]()
	ctx, cancel := context.WithCancel(context.Background())
	outcome := make(chan bool, 1)
	go func() { outcome <- x.waitTracked(ctx, 1, nil, true) }()
	cancel()
	select {
	case acked := <-outcome:
		if acked {
			t.Fatal("waitTracked acknowledged a value with no round armed")
		}
	case <-time.After(scenarioGuard):
		t.Fatal("cancelled wait with no armed round did not return")
	}
	if p := x.pending.Load(); p != 0 {
		t.Fatalf("pending = %d after cancelled wait, want 0", p)
	}
}
