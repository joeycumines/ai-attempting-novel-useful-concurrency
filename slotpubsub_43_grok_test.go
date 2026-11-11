package bigbuff

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestSlotPubSub43GrokAcknowledgedFanout(t *testing.T) {
	x := NewSlotPubSub43Grok[int](2)
	subs := []*SlotSubscription43Grok[int]{x.Subscribe(), x.Subscribe()}
	started := make(chan struct{}, len(subs))
	received := make(chan int, len(subs))
	for _, s := range subs {
		go func(s *SlotSubscription43Grok[int]) {
			started <- struct{}{}
			v := <-s.C()
			s.Wait()
			received <- v
		}(s)
	}
	for range subs {
		<-started
	}
	n := x.Send(17)
	if n != 2 {
		t.Fatalf("Send=%d want 2", n)
	}
	for i := range subs {
		select {
		case v := <-received:
			if v != 17 {
				t.Fatalf("sub %d got %d", i, v)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not receive", i)
		}
	}
	for _, s := range subs {
		s.Unsubscribe()
	}
}

func TestSlotPubSub43GrokContextReceive(t *testing.T) {
	x := NewSlotPubSub43Grok[int](1)
	s := x.Subscribe()
	defer s.Unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := (&slotPubSub43Sub{sub: s}).Receive(ctx); ok {
		t.Fatal("Receive succeeded with cancelled context")
	}
}

// A subscriber that has stopped receiving can unsubscribe while a Send is
// blocked handing it a value. The source deadlocked here: Send held the
// membership mutex and Unsubscribe waited for it.
func TestSlotPubSub43GrokUnsubscribeDuringSend(t *testing.T) {
	x := NewSlotPubSub43Grok[int](4)
	leaving := x.Subscribe() // subscribed first, so it is last in the fan-out
	staying := x.Subscribe()
	got := make(chan int, 1)
	go func() {
		v := <-staying.C()
		got <- v // Send is now blocked on the leaving subscriber, holding the mutex
		staying.Wait()
	}()
	sent := make(chan int, 1)
	go func() { sent <- x.Send(7) }()
	select {
	case v := <-got:
		if v != 7 {
			t.Fatalf("staying subscriber received %d, want 7", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("staying subscriber did not receive")
	}
	unsubscribed := make(chan struct{})
	go func() { leaving.Unsubscribe(); close(unsubscribed) }()
	select {
	case <-unsubscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe did not return while a Send was blocked on the departing subscriber")
	}
	select {
	case n := <-sent:
		// The departing subscriber was registered when the fan-out began, so
		// it is counted: it is fully included, as Send documents.
		if n != 2 {
			t.Fatalf("Send returned %d, want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return after the departing subscriber left")
	}
	if n := x.NumSubscribers(); n != 1 {
		t.Fatalf("NumSubscribers = %d after one of two left, want 1", n)
	}
	// The next round involves the remaining subscriber only.
	go func() { <-staying.C(); staying.Wait() }()
	if n := x.Send(8); n != 1 {
		t.Fatalf("Send after the departure returned %d, want 1", n)
	}
	staying.Unsubscribe()
	leaving.Unsubscribe() // a second Unsubscribe is a no-op
	if n := x.NumSubscribers(); n != 0 {
		t.Fatalf("NumSubscribers = %d after both left, want 0", n)
	}
}

// The parked phase of the same wait: the Send is held up by another, slow
// subscriber, so the departing one must wait without spinning, and still take
// its value when the Send reaches it.
func TestSlotPubSub43GrokUnsubscribeParksBehindASlowSubscriber(t *testing.T) {
	x := NewSlotPubSub43Grok[int](4)
	leaving := x.Subscribe() // last in the fan-out
	slow := x.Subscribe()    // first in the fan-out
	sent := make(chan int, 1)
	go func() { sent <- x.Send(9) }()
	// Wait until the Send holds the mutex; it is then blocked on the slow subscriber.
	deadline := time.Now().Add(2 * time.Second)
	for x.mu.TryLock() {
		x.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("Send never took the membership mutex")
		}
		runtime.Gosched()
	}
	acquired := make(chan struct{})
	go func() {
		x.lockDrainingParked(leaving.s)
		x.mu.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
		t.Fatal("the mutex was acquired while a Send still held it")
	case n := <-sent:
		t.Fatalf("Send returned %d while the slow subscriber had not received", n)
	default:
	}
	if v := <-slow.C(); v != 9 {
		t.Fatalf("slow subscriber received %d, want 9", v)
	}
	slow.Wait()
	select {
	case n := <-sent:
		if n != 2 {
			t.Fatalf("Send returned %d, want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return: the parked subscriber did not take its value")
	}
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("the parked wait did not acquire the mutex after the Send finished")
	}
	leaving.Unsubscribe()
	slow.Unsubscribe()
}

// Concurrent Sends each complete, and every subscriber receives every value
// once. In the source the rounds shared one acknowledgement count and
// overlapped, so an acknowledgement of the second round could close the
// first, and the second Send then never returned.
func TestSlotPubSub43GrokConcurrentSends(t *testing.T) {
	const subscribers, senders, perSender = 3, 4, 50
	x := NewSlotPubSub43Grok[int](subscribers)
	type result struct{ seen map[int]int }
	results := make(chan result, subscribers)
	stop := make(chan struct{})
	for range subscribers {
		sub := x.Subscribe()
		go func() {
			seen := map[int]int{}
			for {
				select {
				case v := <-sub.C():
					sub.Wait()
					seen[v]++
				case <-stop:
					sub.Unsubscribe()
					results <- result{seen}
					return
				}
			}
		}()
	}
	done := make(chan struct{}, senders)
	for s := range senders {
		go func() {
			for i := range perSender {
				if n := x.Send(s*perSender + i); n != subscribers {
					t.Errorf("Send returned %d, want %d", n, subscribers)
				}
			}
			done <- struct{}{}
		}()
	}
	for range senders {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a concurrent Send did not return")
		}
	}
	close(stop)
	for range subscribers {
		r := <-results
		if len(r.seen) != senders*perSender {
			t.Errorf("a subscriber received %d distinct values, want %d", len(r.seen), senders*perSender)
		}
		for v, n := range r.seen {
			if n != 1 {
				t.Errorf("value %d was received %d times", v, n)
			}
		}
	}
}
