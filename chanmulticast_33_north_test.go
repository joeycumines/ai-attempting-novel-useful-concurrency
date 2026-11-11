package bigbuff

// Tests written in the completion phase; the response shipped none and the
// first integration relied on the shared harness.

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func newMulticast33() (*ChanMulticast33North[chan int, int], func()) {
	root := make(chan int)
	x := NewChanMulticast33North(root)
	var once sync.Once
	return x, func() { once.Do(func() { close(root) }) }
}

func multicast33Send(x *ChanMulticast33North[chan int, int], v int) <-chan int {
	out := make(chan int, 1)
	go func() { out <- x.Send(v) }()
	return out
}

func multicast33Recv(t *testing.T, ch <-chan int, what string) int {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-timeAfterGuard():
		t.Fatalf("%s: nothing received", what)
		return 0
	}
}

func TestChanMulticast33North_RejectsBufferedRoot(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a buffered root channel was accepted")
		}
	}()
	NewChanMulticast33North(make(chan int, 1))
}

func TestChanMulticast33North_NoSubscribersReturnsZeroWithoutBlocking(t *testing.T) {
	x, closeRoot := newMulticast33()
	defer closeRoot()
	if n := x.Send(1); n != 0 {
		t.Fatalf("Send with no subscribers = %d, want 0", n)
	}
	// A value sent before a subscription must not reach it.
	ch := x.Subscribe()
	if n := multicast33Recv(t, multicast33Send(x, 2), "Send"); n != 1 {
		t.Fatalf("Send = %d, want 1", n)
	}
	if v := multicast33Recv(t, ch, "subscriber"); v != 2 {
		t.Fatalf("received %d, want 2 (1 was sent before the subscription)", v)
	}
}

func TestChanMulticast33North_EverySubscriberGetsEveryValueInOrder(t *testing.T) {
	const subscribers, values = 6, 500
	x, closeRoot := newMulticast33()
	defer closeRoot()
	var wg sync.WaitGroup
	for i := range subscribers {
		ch := x.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for want := range values {
				if got := <-ch; got != want {
					t.Errorf("subscriber %d received %d, want %d", i, got, want)
					return
				}
			}
		}()
	}
	for v := range values {
		if n := x.Send(v); n != subscribers {
			t.Fatalf("Send(%d) = %d, want %d", v, n, subscribers)
		}
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "subscribers receiving every value")
}

// TestChanMulticast33North_SlackIsTwoValues pins what the type is: Send
// returns when the distributor accepts the value, the distributor holds one
// value and the subscriber's channel one more, and the third Send waits. It
// is neither a synchronous handoff nor lossy.
func TestChanMulticast33North_SlackIsTwoValues(t *testing.T) {
	x, closeRoot := newMulticast33()
	defer closeRoot()
	ch := x.Subscribe()
	if n := multicast33Recv(t, multicast33Send(x, 0), "first Send"); n != 1 {
		t.Fatalf("first Send = %d", n)
	}
	if n := multicast33Recv(t, multicast33Send(x, 1), "second Send"); n != 1 {
		t.Fatalf("second Send = %d", n)
	}
	third := multicast33Send(x, 2)
	apexStillBlocked(t, third, "third Send, with the subscriber not reading")
	for want := range 3 {
		if got := multicast33Recv(t, ch, "subscriber"); got != want {
			t.Fatalf("received %d, want %d: a value was dropped or reordered", got, want)
		}
	}
	multicast33Recv(t, third, "third Send, after the subscriber read")
}

// TestChanMulticast33North_UnsubscribeReleasesTheDistributor: a subscriber
// that stops reading stalls the distributor; removing it must release the
// distributor, the publisher and the other subscriber.
func TestChanMulticast33North_UnsubscribeReleasesTheDistributor(t *testing.T) {
	x, closeRoot := newMulticast33()
	defer closeRoot()
	stalled, live := x.Subscribe(), x.Subscribe()

	received := make(chan int, 8)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case v := <-live:
				received <- v
			case <-stop:
				return
			}
		}
	}()

	multicast33Recv(t, multicast33Send(x, 0), "first Send")  // fills stalled's channel
	multicast33Recv(t, multicast33Send(x, 1), "second Send") // distributor now blocked on stalled
	third := multicast33Send(x, 2)
	apexStillBlocked(t, third, "third Send, behind a stalled subscriber")

	x.Unsubscribe(stalled)
	x.Unsubscribe(stalled) // idempotent
	if n := multicast33Recv(t, third, "third Send, after the stalled subscriber left"); n != 1 {
		t.Fatalf("third Send = %d, want 1 remaining subscriber", n)
	}
	for want := range 3 {
		if got := multicast33Recv(t, received, "live subscriber"); got != want {
			t.Fatalf("live subscriber received %d, want %d", got, want)
		}
	}
	// The removed subscriber's channel is left open (repair 2) and may hold
	// the one value that was buffered before it left.
	select {
	case v, ok := <-stalled:
		if !ok {
			t.Fatal("Unsubscribe closed the subscriber's channel")
		}
		if v != 0 {
			t.Fatalf("the removed subscriber's buffered value is %d, want 0", v)
		}
	default:
		t.Fatal("the value buffered for the removed subscriber is missing")
	}
	x.Unsubscribe(make(chan int)) // unknown channel: no-op
}

func TestChanMulticast33North_ClosingTheRootStopsTheDistributor(t *testing.T) {
	x, closeRoot := newMulticast33()
	ch := x.Subscribe()
	x.Send(1)
	if v := multicast33Recv(t, ch, "subscriber"); v != 1 {
		t.Fatalf("received %d", v)
	}
	closeRoot()
	apexAwait(t, x.distDone, "the distributor exiting after the root was closed")
	select {
	case v, ok := <-ch:
		t.Fatalf("subscriber channel yielded (%d, %v) after the root was closed; it is documented to stay open and empty", v, ok)
	default:
	}
}

func TestChanMulticast33North_Iterator(t *testing.T) {
	x, closeRoot := newMulticast33()
	defer closeRoot()

	t.Run("break unsubscribes", func(t *testing.T) {
		values := x.SubscribeContext(context.Background())
		sent := make(chan struct{})
		go func() {
			defer close(sent)
			for v := range 3 {
				x.Send(v)
			}
		}()
		var seen []int
		for v := range values {
			seen = append(seen, v)
			if len(seen) == 2 {
				break
			}
		}
		if !sameInts(seen, []int{0, 1}) {
			t.Fatalf("iterator yielded %v, want [0 1]", seen)
		}
		if n := x.subCount.Load(); n != 0 {
			t.Fatalf("%d subscribers remain after break", n)
		}
		apexAwait(t, sent, "the publisher, after its only subscriber left")

		// The iterator is single-use: a second run yields nothing and does
		// not disturb the subscriber count.
		for v := range values {
			t.Fatalf("a second run of the iterator yielded %d", v)
		}
		if n := x.subCount.Load(); n != 0 {
			t.Fatalf("%d subscribers after a second run", n)
		}
	})

	t.Run("cancel ends a waiting iterator", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		values := x.SubscribeContext(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for v := range values {
				t.Errorf("iterator yielded %d although nothing was sent", v)
			}
		}()
		cancel()
		apexAwait(t, done, "a waiting iterator after cancel")
		if n := x.subCount.Load(); n != 0 {
			t.Fatalf("%d subscribers remain after cancel", n)
		}
	})

	// Regression test for repair 5: with an already cancelled context the
	// removal is queued on the context's AfterFunc goroutine, and the
	// iterator used to return without waiting for it.
	t.Run("has unsubscribed when it returns", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for round := range 200 {
			values := x.SubscribeContext(ctx)
			for v := range values {
				t.Fatalf("round %d: a cancelled iterator yielded %d", round, v)
			}
			if n := x.subCount.Load(); n != 0 {
				t.Fatalf("round %d: %d subscribers remain after the iterator returned", round, n)
			}
		}
	})

	t.Run("nil yield panics and unsubscribes", func(t *testing.T) {
		values := x.SubscribeContext(context.Background())
		func() {
			defer func() {
				if recover() == nil {
					t.Error("a nil yield func was accepted")
				}
			}()
			values(nil)
		}()
		if n := x.subCount.Load(); n != 0 {
			t.Fatalf("%d subscribers remain after the nil-yield panic", n)
		}
	})
}

// TestChanMulticast33North_ChurnAgainstSends: subscribers that come and go
// must never strand the publisher, and each must see increasing values.
func TestChanMulticast33North_ChurnAgainstSends(t *testing.T) {
	const cycles, churners = 1500, 4
	x, closeRoot := newMulticast33()
	defer closeRoot()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var completed atomic.Int64
	var wg sync.WaitGroup
	for range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				last, n := -1, 0
				for v := range x.SubscribeContext(ctx) {
					if v <= last {
						t.Errorf("a subscriber received %d after %d", v, last)
					}
					last = v
					if n++; n == 3 {
						break
					}
				}
				completed.Add(1)
			}
		}()
	}
	// The publisher runs until the subscribers have completed the required
	// number of join/receive/leave cycles; a fixed number of sends could all
	// be made before the first subscriber joined. With no subscriber Send
	// returns at once, so the loop yields the processor in that case.
	published := make(chan struct{})
	go func() {
		defer close(published)
		for v := 0; completed.Load() < cycles; v++ {
			if x.Send(v) == 0 {
				runtime.Gosched()
			}
		}
	}()
	apexAwait(t, published, "the publisher under subscriber churn")
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "subscribers leaving")
	if n := x.subCount.Load(); n != 0 {
		t.Fatalf("%d subscribers remain", n)
	}
}
