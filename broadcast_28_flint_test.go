package bigbuff

// Tests written during integration; the response shipped none. They pin the
// repair in Send (a retired subscriber that is not the newest) and the
// lifecycle paths the shared harness reaches only through the adapter.

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

// send28 runs Send on its own goroutine and returns a channel that yields
// its result.
func send28(b *Broadcast28Flint[int], v int) <-chan int {
	out := make(chan int, 1)
	go func() { out <- b.Send(v) }()
	return out
}

func subscribe28(t *testing.T, b *Broadcast28Flint[int]) *Subscription28Flint[int] {
	t.Helper()
	s, err := b.Subscribe(context.Background())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return s
}

// recv28 receives exactly one value from each subscription, in whatever
// order Send reaches them, then waits for Send to return. Delivery is over
// unbuffered channels, so a duplicate delivery would leave Send blocked and
// fail the final wait.
func recv28(t *testing.T, sent <-chan int, want int, subs ...*Subscription28Flint[int]) int {
	t.Helper()
	guard := timeAfterGuard()
	pending := make([]int, len(subs)) // select case index -> subscriber index
	for i := range subs {
		pending[i] = i
	}
	for len(pending) > 0 {
		cases := make([]reflect.SelectCase, 0, len(pending)+1)
		for _, i := range pending {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(subs[i].Ch())})
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(guard)})
		chosen, v, _ := reflect.Select(cases)
		if chosen == len(pending) {
			t.Fatalf("subscribers %v never received %d", pending, want)
		}
		if got := int(v.Int()); got != want {
			t.Fatalf("subscriber %d received %d, want %d", pending[chosen], got, want)
		}
		pending = append(pending[:chosen], pending[chosen+1:]...)
	}
	select {
	case n := <-sent:
		return n
	case <-timeAfterGuard():
		t.Fatalf("Send(%d) did not return after every subscriber received it", want)
		return 0
	}
}

// liveNodes28 walks the subscription list and counts nodes by state.
func liveNodes28(b *Broadcast28Flint[int]) (active, retired int) {
	for n := b.head.Load(); n != nil; n = n.next.Load() {
		if n.state.Load() == 1 {
			active++
		} else {
			retired++
		}
	}
	return
}

func TestBroadcast28Flint_NoSubscribers(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	if n := b.Send(1); n != 0 {
		t.Fatalf("Send with no subscribers = %d, want 0", n)
	}
	s := subscribe28(t, b)
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if n := b.Send(2); n != 0 {
		t.Fatalf("Send after the only subscriber stopped = %d, want 0", n)
	}
}

func TestBroadcast28Flint_DeliversToEverySubscriberOnce(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	a, c, d := subscribe28(t, b), subscribe28(t, b), subscribe28(t, b)
	for v := 1; v <= 3; v++ {
		if n := recv28(t, send28(b, v), v, a, c, d); n != 3 {
			t.Fatalf("Send(%d) = %d, want 3", v, n)
		}
	}
}

// TestBroadcast28Flint_RetiredSubscriberAnywhereInTheList is the regression
// test for the integration repair. As delivered, Send restarted its walk from
// the head whenever it met a retired subscriber that was not the head, never
// unlinked it, and so never returned: it delivered the same value to the
// newer subscribers over and over.
func TestBroadcast28Flint_RetiredSubscriberAnywhereInTheList(t *testing.T) {
	for _, tc := range []struct {
		name   string
		retire int // index in subscription order: 0 oldest (tail), 2 newest (head)
	}{{"oldest", 0}, {"middle", 1}, {"newest", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBroadcast28Flint[int]()
			subs := []*Subscription28Flint[int]{subscribe28(t, b), subscribe28(t, b), subscribe28(t, b)}
			subs[tc.retire].Stop()
			var live []*Subscription28Flint[int]
			for i, s := range subs {
				if i != tc.retire {
					live = append(live, s)
				}
			}
			for v := 1; v <= 3; v++ {
				sent := send28(b, v)
				if n := recv28(t, sent, v, live...); n != 2 {
					t.Fatalf("Send(%d) = %d, want 2", v, n)
				}
				// Send has returned, so nothing more can be in flight: a
				// duplicate would have kept it blocked on an unbuffered
				// channel, and recv28 would have failed.
			}
			if active, retired := liveNodes28(b); active != 2 || retired != 0 {
				t.Fatalf("list holds %d active and %d retired nodes after sends, want 2 and 0", active, retired)
			}
		})
	}
}

func TestBroadcast28Flint_AllButOneRetired(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	var subs []*Subscription28Flint[int]
	for range 6 {
		subs = append(subs, subscribe28(t, b))
	}
	for i, s := range subs {
		if i != 3 {
			s.Stop()
		}
	}
	if n := recv28(t, send28(b, 9), 9, subs[3]); n != 1 {
		t.Fatalf("Send = %d, want 1", n)
	}
	// Retired nodes behind the survivor were unlinked through it; those in
	// front of it through the head.
	if active, retired := liveNodes28(b); active != 1 || retired != 0 {
		t.Fatalf("list holds %d active and %d retired nodes, want 1 and 0", active, retired)
	}
}

func TestBroadcast28Flint_StopReleasesABlockedSend(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	idle := subscribe28(t, b)
	sent := send28(b, 1)
	apexStillBlocked(t, sent, "Send to a subscriber that is not receiving")
	idle.Stop()
	select {
	case n := <-sent:
		if n != 0 {
			t.Fatalf("Send = %d after its only subscriber stopped, want 0", n)
		}
	case <-timeAfterGuard():
		t.Fatal("Stop did not release the blocked Send")
	}
	if err := idle.Stop(); err != nil { // idempotent
		t.Fatalf("second Stop: %v", err)
	}
}

func TestBroadcast28Flint_ContextCancelUnsubscribes(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := b.Subscribe(ctx); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	keep := subscribe28(t, b)
	// The Send blocks on whichever subscriber it reaches first that is not
	// receiving. Cancelling the first one's context must take it out of the
	// walk; the Send then completes with the remaining subscriber.
	sent := send28(b, 5)
	cancel()
	if n := recv28(t, sent, 5, keep); n != 1 {
		t.Fatalf("Send = %d, want 1 (the cancelled subscriber must not count)", n)
	}
}

func TestBroadcast28Flint_NilContextSubscribes(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	//lint:ignore SA1012 the nil context is the case under test
	s, err := b.Subscribe(nil) //nolint:staticcheck
	if err != nil {
		t.Fatalf("Subscribe(nil): %v", err)
	}
	if n := recv28(t, send28(b, 4), 4, s); n != 1 {
		t.Fatalf("Send = %d, want 1", n)
	}
	if err := b.Unsubscribe(nil); err != nil {
		t.Fatalf("Unsubscribe(nil): %v", err)
	}
}

// Stop from several goroutines at once, with the context being cancelled at
// the same moment, retires the subscription exactly once. As delivered Stop
// read and cleared an unsynchronised field; the race detector reports that on
// the old code, and this test is the reason verify.race covers it.
func TestBroadcast28Flint_ConcurrentStop(t *testing.T) {
	const rounds, stoppers = 200, 4
	b := NewBroadcast28Flint[int]()
	for round := 0; round < rounds; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		s, err := b.Subscribe(ctx)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < stoppers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if err := s.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			cancel()
		}()
		close(start)
		wg.Wait()
		select {
		case <-s.sub.quit:
		default:
			t.Fatalf("round %d: Stop returned and the subscription is not retired", round)
		}
		if n := b.subscribers.Load(); n != 0 {
			t.Fatalf("round %d: %d subscribers counted after the only one stopped, want 0", round, n)
		}
	}
	if n := b.Send(1); n != 0 {
		t.Fatalf("Send = %d with every subscription stopped, want 0", n)
	}
}

// A subscription made with a context that is already cancelled is retired by
// the context callback, which may run before Subscribe has returned.
func TestBroadcast28Flint_SubscribeWithCancelledContext(t *testing.T) {
	b := NewBroadcast28Flint[int]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 100; i++ {
		s, err := b.Subscribe(ctx)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		select {
		case <-s.sub.quit:
		case <-timeAfterGuard():
			t.Fatalf("subscription %d was never retired by its cancelled context", i)
		}
		if err := s.Stop(); err != nil {
			t.Fatalf("Stop after the context retired the subscription: %v", err)
		}
	}
	if n := b.subscribers.Load(); n != 0 {
		t.Fatalf("%d subscribers counted, want 0", n)
	}
	if n := b.Send(1); n != 0 {
		t.Fatalf("Send = %d, want 0", n)
	}
}
