package bigbuff

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCM34(t *testing.T) {
	// no subscribers: Send must not block and must report zero
	{
		mc := NewChanMultiCast34Lightning[int](4)
		if got := mc.Send(1); got != 0 {
			t.Fatalf("got %d want 0", got)
		}
		mc.Close()
		mc.Close() // idempotent
		if got := mc.Send(2); got != 0 {
			t.Fatalf("got %d want 0 after close", got)
		}
		for range mc.Iter() {
			t.Fatal("unexpected value after close")
		}
	}

	// fan-out to 8 subscribers, in order; each reader returns once it has the
	// last value it expects, since subscriber channels are retired, not closed.
	mc := NewChanMultiCast34Lightning[int](8)
	const subs = 8
	const msgs = 100
	var wg sync.WaitGroup
	var received atomic.Int64
	for i := 0; i < subs; i++ {
		ch := mc.Subscribe()
		wg.Add(1)
		go func(i int, ch <-chan int) {
			defer wg.Done()
			last := 0
			for v := range ch {
				if v != last+1 {
					t.Errorf("sub %d out of order: got %d last %d", i, v, last)
					return
				}
				last = v
				received.Add(1)
				if last == msgs {
					return
				}
			}
		}(i, ch)
	}
	for i := 1; i <= msgs; i++ {
		if got := mc.Send(i); got != subs {
			t.Fatalf("send %d: got %d want %d", i, got, subs)
		}
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readers did not return")
	}
	if got, want := received.Load(), int64(subs*msgs); got != want {
		t.Fatalf("got %d want %d", got, want)
	}
	for i := 0; i < subs; i++ {
		mc.Unsubscribe()
	}
	// negative Add panics rather than underflowing
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		mc.Add(-1)
	}()
	mc.Close()
}

// Unsubscribing while a Send is blocked on the retired subscriber must not
// panic and must not deadlock the producer.
func TestCM34Churn(t *testing.T) {
	mc := NewChanMultiCast34Lightning[int](0)
	ch := mc.Subscribe()
	stop := make(chan struct{})
	var got atomic.Int64
	go func() {
		for {
			select {
			case <-ch:
				got.Add(1)
			case <-stop:
				return
			}
		}
	}()
	for i := 0; i < 50; i++ {
		if n := mc.Send(i); n != 1 {
			t.Fatalf("got %d want 1", n)
		}
	}
	mc.Unsubscribe() // must not panic even though a Send could be in flight
	close(stop)
	mc.Close()
}

// send34 runs Send on its own goroutine and returns a channel that yields its
// result.
func send34(mc *ChanMultiCast34Lightning[int], v int) <-chan int {
	out := make(chan int, 1)
	go func() { out <- mc.Send(v) }()
	return out
}

func recv34[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-timeAfterGuard():
		t.Fatalf("%s: nothing arrived", what)
		panic("unreachable")
	}
}

func empty34(t *testing.T, ch <-chan int, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s: received %d", what, v)
	default:
	}
}

// A subscriber that has stopped receiving can still be removed while a Send
// is blocked on it, and the Send then finishes. Once the first subscriber has
// the value the Send is, or is about to be, blocked on the second, so the
// order of events does not depend on timing. Before repair 7 the removal
// waited for the write lock, which the blocked Send's read lock held: both
// goroutines stayed parked for good.
func TestChanMultiCast34Lightning_LeaveWhileSendIsBlockedOnYou(t *testing.T) {
	mc := NewChanMultiCast34Lightning[int](4)
	first := mc.Subscribe()
	mc.Subscribe() // never receives
	sent := send34(mc, 7)
	if v := recv34(t, first, "first subscriber"); v != 7 {
		t.Fatalf("first subscriber received %d, want 7", v)
	}
	left := make(chan struct{})
	go func() { mc.Unsubscribe(); close(left) }() // removes the newest: the one not receiving
	recv34(t, left, "Unsubscribe while a Send is blocked on the subscriber being removed")
	if n := recv34(t, sent, "Send after the subscriber it was blocked on left"); n != 1 {
		t.Fatalf("Send = %d, want 1 (only the first subscriber took the value)", n)
	}
	mc.Close()
}

// Close releases a Send blocked on a subscriber that is not receiving.
func TestChanMultiCast34Lightning_CloseWhileSendIsBlocked(t *testing.T) {
	mc := NewChanMultiCast34Lightning[int](4)
	first := mc.Subscribe()
	mc.Subscribe() // never receives
	sent := send34(mc, 7)
	recv34(t, first, "first subscriber")
	closed := make(chan struct{})
	go func() { mc.Close(); close(closed) }()
	recv34(t, closed, "Close while a Send is blocked")
	if n := recv34(t, sent, "Send after Close"); n != 1 {
		t.Fatalf("Send = %d, want 1", n)
	}
	if n := mc.Send(8); n != 0 {
		t.Fatalf("Send after Close = %d, want 0", n)
	}
}

// UnsubscribeChan removes the subscriber it is given, not the newest one.
func TestChanMultiCast34Lightning_UnsubscribeChanRemovesThatSubscriber(t *testing.T) {
	mc := NewChanMultiCast34Lightning[int](4)
	a, b, c := mc.Subscribe(), mc.Subscribe(), mc.Subscribe()
	if !mc.UnsubscribeChan(a) {
		t.Fatal("UnsubscribeChan(a) = false for a live subscriber")
	}
	if mc.UnsubscribeChan(a) {
		t.Fatal("second UnsubscribeChan(a) = true")
	}
	if mc.UnsubscribeChan(nil) || mc.UnsubscribeChan(make(chan int)) {
		t.Fatal("UnsubscribeChan accepted a channel that is not a subscriber")
	}
	sent := send34(mc, 3)
	if v := recv34(t, b, "b"); v != 3 {
		t.Fatalf("b received %d, want 3", v)
	}
	if v := recv34(t, c, "c"); v != 3 {
		t.Fatalf("c received %d, want 3", v)
	}
	if n := recv34(t, sent, "Send"); n != 2 {
		t.Fatalf("Send = %d, want 2", n)
	}
	empty34(t, a, "removed subscriber")

	// The middle of what is left, then the last one through the delivered API.
	if !mc.UnsubscribeChan(b) {
		t.Fatal("UnsubscribeChan(b) = false")
	}
	mc.Unsubscribe()
	if n := mc.Send(4); n != 0 {
		t.Fatalf("Send with nobody subscribed = %d, want 0", n)
	}
	mc.Close()
	if mc.UnsubscribeChan(c) {
		t.Fatal("UnsubscribeChan after Close = true")
	}
}

// A Send walks the subscribers that existed when it started. One that joins
// while the Send is blocked takes the slot of one that left, and must not be
// handed the value: removal copies, so the walk still sees the old slot.
func TestChanMultiCast34Lightning_InFlightSendDoesNotSeeALaterSubscriber(t *testing.T) {
	mc := NewChanMultiCast34Lightning[int](4) // room to append in place
	a := mc.Subscribe()
	b := mc.Subscribe() // not receiving
	sent := send34(mc, 1)
	recv34(t, a, "a")
	if !mc.UnsubscribeChan(b) {
		t.Fatal("UnsubscribeChan(b) = false")
	}
	c := mc.Subscribe()
	if n := recv34(t, sent, "the in-flight Send"); n != 1 {
		t.Fatalf("in-flight Send = %d, want 1", n)
	}
	empty34(t, c, "subscriber that joined after the Send started")
	empty34(t, b, "subscriber that left")

	sent = send34(mc, 2)
	if v := recv34(t, a, "a"); v != 2 {
		t.Fatalf("a received %d, want 2", v)
	}
	if v := recv34(t, c, "c"); v != 2 {
		t.Fatalf("c received %d, want 2", v)
	}
	if n := recv34(t, sent, "second Send"); n != 2 {
		t.Fatalf("second Send = %d, want 2", n)
	}
	mc.Close()
}

// Add with a negative delta releases a Send blocked on the subscribers it
// removes. Add's subscribers have no channel anyone can read, so the Send
// below can only finish by being released.
func TestChanMultiCast34Lightning_NegativeAddReleasesABlockedSend(t *testing.T) {
	mc := NewChanMultiCast34Lightning[int](0)
	first := mc.Subscribe()
	if n := mc.Add(3); n != 4 {
		t.Fatalf("Add(3) = %d, want 4", n)
	}
	sent := send34(mc, 9)
	recv34(t, first, "first subscriber")
	done := make(chan int, 1)
	go func() { done <- mc.Add(-3) }()
	if n := recv34(t, done, "Add(-3) while a Send is blocked"); n != 1 {
		t.Fatalf("Add(-3) = %d, want 1", n)
	}
	if n := recv34(t, sent, "Send"); n != 1 {
		t.Fatalf("Send = %d, want 1", n)
	}
	mc.Close()
}

// Subscribers join, take one value and leave, in any order, while a sender
// runs flat out. Every subscriber gets a value, every one of them can leave,
// and nobody is left subscribed. This is the pattern the shared harness's
// concurrent-churn test uses, which this implementation used to be excused
// from.
func TestChanMultiCast34Lightning_ConcurrentChurn(t *testing.T) {
	const workers, cycles = 4, 100
	mc := NewChanMultiCast34Lightning[int](2)
	stop := make(chan struct{})
	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				mc.Send(i)
			}
		}
	}()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < cycles; i++ {
				ch := mc.Subscribe()
				select {
				case <-ch:
				case <-timeAfterGuard():
					t.Error("a subscriber never received a value")
				}
				if !mc.UnsubscribeChan(ch) {
					t.Error("UnsubscribeChan = false for a live subscriber")
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	recv34(t, done, "churning subscribers")
	close(stop)
	recv34(t, senderDone, "sender after every subscriber left")
	if n := mc.subscribers.Load(); n != 0 {
		t.Fatalf("%d subscribers counted after all left, want 0", n)
	}
	if n := len(mc.receivers); n != 0 {
		t.Fatalf("%d receiver slots left, want 0", n)
	}
	mc.Close()
}
