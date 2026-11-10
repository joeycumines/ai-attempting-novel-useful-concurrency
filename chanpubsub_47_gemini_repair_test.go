package bigbuff

// Regression tests for the two integration repairs to ChanPubSub47Gemini's
// unsubscribe accounting. Both failures were found by the shared broadcast
// op-stream fuzz target; neither was reachable from the response's own tests.

import (
	"context"
	"testing"
)

// TestChanPubSub47Gemini_UnsubscribeStraightAfterWait pins the first repair.
// As delivered, the pong barrier was released by the last subscriber's Wait,
// before the sender had cleared ping's sending state. A subscriber that
// unsubscribed immediately afterwards was treated as a mid-flight withdrawal
// and parked forever, receiving from the channel for a send that was never
// coming. The window is one goroutine switch wide, so the test repeats the
// sequence; it is hit on nearly every round at GOMAXPROCS=1.
func TestChanPubSub47Gemini_UnsubscribeStraightAfterWait(t *testing.T) {
	for _, subscribers := range []int{1, 3} {
		ch := make(chan int)
		ps := NewChanPubSub47Gemini(ch)
		for round := range 2000 {
			ps.Add(subscribers)
			left := make(chan struct{}, subscribers)
			for range subscribers {
				go func() {
					if v := <-ch; v != round {
						t.Errorf("received %d, want %d", v, round)
					}
					ps.Wait()
					ps.Unsubscribe()
					left <- struct{}{}
				}()
			}
			sent := make(chan int, 1)
			go func() { sent <- ps.Send(round) }()
			for range subscribers {
				select {
				case <-left:
				case <-timeAfterGuard():
					t.Fatalf("%d subscribers, round %d: Unsubscribe straight after Wait did not return", subscribers, round)
				}
			}
			select {
			case n := <-sent:
				if n != subscribers {
					t.Fatalf("%d subscribers, round %d: Send = %d", subscribers, round, n)
				}
			case <-timeAfterGuard():
				t.Fatalf("%d subscribers, round %d: Send did not return", subscribers, round)
			}
			if n := ps.Add(0); n != 0 {
				t.Fatalf("%d subscribers, round %d: %d subscribers remain", subscribers, round, n)
			}
		}
	}
}

// TestChanPubSub47Gemini_IteratorStopsWhileNextSendIsInFlight pins the second
// repair. An iterator subscriber that had acknowledged one value used to
// leave with a membership-only decrement, on the assumption that the Send it
// had acknowledged was the one in progress. If its loop body stopped the
// iteration after the next Send had armed, that Send still held a receive
// intent for it and blocked forever. Every step here is ordered through the
// implementation's own blocking operations, so the sequence is deterministic.
func TestChanPubSub47Gemini_IteratorStopsWhileNextSendIsInFlight(t *testing.T) {
	ch := make(chan int)
	ps := NewChanPubSub47Gemini(ch)

	iterating := ps.SubscribeContext(context.Background())
	gotFirst := make(chan struct{})
	stop := make(chan struct{})
	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		for v := range iterating {
			if v != 1 {
				t.Errorf("iterator received %d, want 1", v)
			}
			close(gotFirst)
			<-stop // hold the loop body open across the start of the next Send
			break
		}
	}()

	ps.Subscribe() // the manual subscriber, driven below
	recvManual := func(want int) <-chan struct{} {
		received := make(chan struct{})
		go func() {
			if v := <-ch; v != want {
				t.Errorf("manual subscriber received %d, want %d", v, want)
			}
			close(received)
			ps.Wait()
		}()
		return received
	}

	first := recvManual(1)
	if n := ps.Send(1); n != 2 {
		t.Fatalf("first Send = %d, want 2", n)
	}
	apexAwait(t, first, "manual subscriber's first value")
	apexAwait(t, gotFirst, "iterator's first value")

	// The second Send arms with both subscribers. The manual subscriber
	// receiving its value proves the Send is in its ping phase.
	second := recvManual(2)
	sent := make(chan int, 1)
	go func() { sent <- ps.Send(2) }()
	apexAwait(t, second, "manual subscriber's second value")

	close(stop) // the iterator leaves, owing the in-flight Send a receive
	apexAwait(t, iterDone, "iterator exit")
	select {
	case n := <-sent:
		if n != 1 {
			t.Fatalf("second Send = %d, want 1 (the iterator withdrew)", n)
		}
	case <-timeAfterGuard():
		t.Fatal("second Send blocked on the departed iterator subscriber")
	}
	if n := ps.Add(0); n != 1 {
		t.Fatalf("%d subscribers remain, want 1", n)
	}
}
