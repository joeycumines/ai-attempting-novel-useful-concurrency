package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSenseCasterZeroValue(t *testing.T) {
	var c SenseCaster[int]
	if n := c.Send(1); n != 0 {
		t.Fatalf("Send with no subscriptions = %d, want 0", n)
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}

	var s SenseSubscription[int]
	if !s.Closed() {
		t.Fatal("zero Subscription should report Closed")
	}
	if _, ok := s.TryRecv(); ok {
		t.Fatal("zero Subscription yielded a value")
	}
	if s.Ready() != nil {
		t.Fatal("zero Subscription should have a nil Ready channel")
	}
	s.Close()
	if _, err := s.Recv(context.Background()); !errors.Is(err, ErrSenseSubClosed) {
		t.Fatalf("Recv on zero Subscription: err = %v, want ErrSenseSubClosed", err)
	}
}

// sensecasterOrderedDelivery checks the core contract: every subscription sees every
// publish, exactly once, in order, and Send addresses exactly the registered set.
func sensecasterOrderedDelivery(t *testing.T, subs, msgs int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := NewSenseCaster[int]()
	handles := make([]SenseSubscription[int], subs)
	for i := range handles {
		handles[i] = c.Subscribe()
	}
	if got := c.Subscribers(); got != subs {
		t.Fatalf("Subscribers() = %d, want %d", got, subs)
	}

	errs := make(chan error, subs)
	var wg sync.WaitGroup
	for i, sub := range handles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			for want := range msgs {
				v, err := sub.Recv(ctx)
				if err != nil {
					errs <- fmt.Errorf("subscription %d: message %d: %w", i, want, err)
					return
				}
				if v != want {
					errs <- fmt.Errorf("subscription %d: got %d, want %d", i, v, want)
					return
				}
				if (i+want)%5 == 0 {
					runtime.Gosched() // interleave busy and parked receivers
				}
			}
		}()
	}

	for i := range msgs {
		if n := c.Send(i); n != subs {
			t.Fatalf("Send(%d) addressed %d subscriptions, want %d", i, n, subs)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() after drain = %d, want 0", got)
	}
	if n := c.Send(-1); n != 0 {
		t.Fatalf("Send after drain = %d, want 0", n)
	}
}

func TestSenseCasterOrderedExactlyOnce(t *testing.T) {
	sensecasterOrderedDelivery(t, 24, 3000)
}

func TestSenseCasterManySubscriptions(t *testing.T) {
	// Crosses two and a half parked-set segments, so the summary tier is exercised.
	sensecasterOrderedDelivery(t, 10000, 3)
}

func TestSenseCasterSelectIntegration(t *testing.T) {
	c := NewSenseCaster[string]()
	sub := c.Subscribe()
	quit := make(chan struct{})
	exited := make(chan struct{})
	got := make(chan string, 8)

	go func() {
		defer close(exited)
		defer sub.Close()
		for {
			if v, ok := sub.TryRecv(); ok {
				got <- v
				continue
			}
			select {
			case <-sub.Ready():
			case <-quit:
				return
			}
		}
	}()

	want := []string{"alpha", "beta", "gamma", "delta"}
	for _, w := range want {
		if n := c.Send(w); n != 1 {
			t.Fatalf("Send(%q) = %d, want 1", w, n)
		}
	}
	for _, w := range want {
		select {
		case g := <-got:
			if g != w {
				t.Fatalf("got %q, want %q", g, w)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %q", w)
		}
	}
	close(quit)
	<-exited
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

// TestSenseCasterChurn races subscribe, receive, and close, with some closes arriving
// from a second goroutine mid-receive. Within each subscription, values must be
// consecutive from its first receive onward. No close may deadlock a publisher.
func TestSenseCasterChurn(t *testing.T) {
	const churners, publishes = 6, 4000
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := NewSenseCaster[uint64]()
	var violations, received atomic.Int64
	var wg sync.WaitGroup
	for w := range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; parent.Err() == nil; r++ {
				sub := c.Subscribe()
				var closer sync.WaitGroup
				if r%5 == 4 {
					closer.Add(1)
					go func() {
						defer closer.Done()
						sub.Close() // races with this goroutine's receives
					}()
				}
				budget := 1 + (w+r)%7
				var last uint64
				for j := 0; j < budget; j++ {
					v, err := sub.Recv(parent)
					if err != nil {
						if parent.Err() == nil && !sub.Closed() {
							violations.Add(1)
						}
						break
					}
					if j > 0 && v != last+1 {
						violations.Add(1)
					}
					last = v
					received.Add(1)
				}
				sub.Close()
				closer.Wait()
			}
		}()
	}

	// Publish until the churners have demonstrably received values, not merely for a fixed
	// count: sends made before anyone subscribes reach nobody and would exercise nothing.
	const minReceived = 1000
	for i := uint64(0); (i < publishes || received.Load() < minReceived) && parent.Err() == nil; i++ {
		c.Send(i)
		if i%8 == 0 {
			runtime.Gosched() // let churners interleave subscribe and close between publishes
		}
	}
	cancel()
	wg.Wait()

	if n := violations.Load(); n != 0 {
		t.Fatalf("%d ordering or lifecycle violations", n)
	}
	if n := received.Load(); n == 0 {
		t.Fatal("churners received nothing; the test exercised no delivery")
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() after churn = %d, want 0", got)
	}
}

// TestSenseCasterCloseReleasesOwedSubscription: a subscription that never reads must not
// hang the publisher forever. Closing it discards its value and completes the Send.
func TestSenseCasterCloseReleasesOwedSubscription(t *testing.T) {
	c := NewSenseCaster[int]()
	a := c.Subscribe()
	b := c.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res := make(chan int, 1)
	go func() { res <- c.Send(7) }()

	if v, err := b.Recv(ctx); err != nil || v != 7 {
		t.Fatalf("b.Recv = (%d, %v), want (7, nil)", v, err)
	}
	select {
	case <-res:
		t.Fatal("Send returned while subscription a still owed the value")
	case <-time.After(20 * time.Millisecond):
	}

	a.Close()
	select {
	case n := <-res:
		if n != 2 {
			t.Fatalf("Send addressed %d subscriptions, want 2", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Send did not complete after Close released the owed subscription")
	}
	b.Close()
}

func TestSenseCasterCloseWakesParkedRecv(t *testing.T) {
	c := NewSenseCaster[int]()
	s := c.Subscribe()
	errc := make(chan error, 1)
	go func() {
		_, err := s.Recv(context.Background())
		errc <- err
	}()
	runtime.Gosched()
	s.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrSenseSubClosed) {
			t.Fatalf("Recv after Close: err = %v, want ErrSenseSubClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not wake the parked Recv")
	}
}

// TestSenseCasterStaleHandleIsInert: after its slot is recycled, an old handle must not
// read, acknowledge, or release the new tenant's subscription.
func TestSenseCasterStaleHandleIsInert(t *testing.T) {
	c := NewSenseCaster[int]()
	old := c.Subscribe()
	old.Close()
	fresh := c.Subscribe()
	if fresh.s != old.s {
		t.Fatal("expected LIFO slot reuse for this test to be meaningful")
	}

	old.Close()
	if !old.Closed() {
		t.Fatal("stale handle should report Closed")
	}
	if fresh.Closed() {
		t.Fatal("fresh subscription was closed through a stale handle")
	}
	if got := c.Subscribers(); got != 1 {
		t.Fatalf("Subscribers() = %d, want 1", got)
	}
	if _, ok := old.TryRecv(); ok {
		t.Fatal("stale handle received a value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := make(chan int, 1)
	go func() { res <- c.Send(5) }()
	if v, err := fresh.Recv(ctx); err != nil || v != 5 {
		t.Fatalf("fresh.Recv = (%d, %v), want (5, nil)", v, err)
	}
	if n := <-res; n != 1 {
		t.Fatalf("Send addressed %d, want 1", n)
	}
	fresh.Close()
}

// TestSenseCasterSharedSubscriptionDeliversOnce: copies of one subscription may receive
// concurrently. Each value must go to exactly one of them.
func TestSenseCasterSharedSubscriptionDeliversOnce(t *testing.T) {
	const readers, msgs = 4, 2000
	c := NewSenseCaster[int]()
	sub := c.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seen [msgs]atomic.Bool
	var dupes atomic.Int64
	var wg sync.WaitGroup
	for range readers {
		cp := sub
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v, err := cp.Recv(ctx)
				if err != nil {
					return
				}
				if !seen[v].CompareAndSwap(false, true) {
					dupes.Add(1)
				}
			}
		}()
	}

	for i := range msgs {
		if n := c.Send(i); n != 1 {
			t.Fatalf("Send(%d) addressed %d, want 1", i, n)
		}
	}
	cancel() // every Send has returned, so every value has been consumed
	wg.Wait()
	sub.Close()

	if n := dupes.Load(); n != 0 {
		t.Fatalf("%d values were delivered more than once", n)
	}
	for i := range seen {
		if !seen[i].Load() {
			t.Fatalf("value %d was never delivered", i)
		}
	}
}

// TestSenseCasterSelectChurn drives the subscription the way a user would: TryRecv,
// then a select over Ready and a separate exit channel, with closes from another
// goroutine. It covers the retired-waiter path that Recv alone rarely hits.
func TestSenseCasterSelectChurn(t *testing.T) {
	const churners, publishes = 6, 4000
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c := NewSenseCaster[uint64]()
	var violations, received atomic.Int64
	var wg sync.WaitGroup
	for w := range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; parent.Err() == nil; r++ {
				sub := c.Subscribe()
				var closer sync.WaitGroup
				if r%3 == 2 {
					closer.Add(1)
					go func() {
						defer closer.Done()
						sub.Close()
					}()
				}
				budget := 1 + (w+r)%5
				var last uint64
				for j := 0; j < budget; {
					if v, ok := sub.TryRecv(); ok {
						if j > 0 && v != last+1 {
							violations.Add(1)
						}
						last = v
						j++
						received.Add(1)
						continue
					}
					if sub.Closed() {
						break // closed by the closer goroutine, or the test is ending
					}
					select {
					case <-sub.Ready():
					case <-parent.Done():
						j = budget // stop
					}
				}
				sub.Close()
				closer.Wait()
			}
		}()
	}

	const minReceived = 1000
	for i := uint64(0); (i < publishes || received.Load() < minReceived) && parent.Err() == nil; i++ {
		c.Send(i)
		if i%8 == 0 {
			runtime.Gosched()
		}
	}
	cancel()
	wg.Wait()

	if n := violations.Load(); n != 0 {
		t.Fatalf("%d ordering violations", n)
	}
	if n := received.Load(); n == 0 {
		t.Fatal("churners received nothing")
	}
	if got := c.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() after churn = %d, want 0", got)
	}
}
