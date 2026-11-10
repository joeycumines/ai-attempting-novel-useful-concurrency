package bigbuff

import (
	"context"
	"testing"
)

// TestBaselineBarrierPubSub_LateSubscriberOwesNothingForTheRoundInFlight is
// the regression test for a defect the broadcast op-stream fuzz target found
// in this baseline: a subscriber that joined while a round was in flight and
// then left was counted against that round's WaitGroup although the round
// never included it. One such subscriber released Send before its real
// subscribers had acknowledged; a second drove the WaitGroup negative.
func TestBaselineBarrierPubSub_LateSubscriberOwesNothingForTheRoundInFlight(t *testing.T) {
	x := NewBaselineBarrierPubSub[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := x.SubscribeContext(ctx)  // registered now, starts consuming later
	quick := x.SubscribeContext(ctx) // consumes at once
	quickGot := make(chan int)
	go func() {
		for v := range quick {
			select {
			case quickGot <- v:
			case <-ctx.Done():
				return
			}
		}
	}()

	sent := make(chan int, 1)
	go func() { sent <- x.Send(1) }()
	// The quick subscriber receiving proves the round is in flight; it stays
	// in flight because the slow subscriber has not acknowledged.
	apexAwait(t, quickGot, "the quick subscriber's value")

	for range 3 {
		gone, leave := context.WithCancel(ctx)
		late := x.SubscribeContext(gone)
		leave()
		for range late {
			t.Error("a late subscriber received a value from a round that began before it joined")
		}
	}
	apexStillBlocked(t, sent, "Send, with the slow subscriber still owing its acknowledgement")

	slowGot := make(chan int)
	go func() {
		for v := range slow {
			select {
			case slowGot <- v:
			case <-ctx.Done():
				return
			}
		}
	}()
	apexAwait(t, slowGot, "the slow subscriber's value")
	select {
	case n := <-sent:
		if n != 2 {
			t.Fatalf("Send = %d, want 2", n)
		}
	case <-timeAfterGuard():
		t.Fatal("Send did not return after both of its subscribers acknowledged")
	}

	// The next round includes neither late subscriber and completes normally.
	go func() { sent <- x.Send(2) }()
	apexAwait(t, quickGot, "the quick subscriber's second value")
	apexAwait(t, slowGot, "the slow subscriber's second value")
	select {
	case n := <-sent:
		if n != 2 {
			t.Fatalf("second Send = %d, want 2", n)
		}
	case <-timeAfterGuard():
		t.Fatal("second Send did not return")
	}
}
