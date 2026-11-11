package bigbuff

// Lifecycle tests written in the completion-phase review of response 52.
// The response's own tests cover fan-out, joining during a round and a
// mid-round Unsubscribe on a handle; these cover the SubscribeContext
// iterator's exits and the exact-count claim under membership churn.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func cascade52Send(x *ChanCascade52Opus[chan int, int], v int) <-chan int {
	out := make(chan int, 1)
	go func() { out <- x.Send(v) }()
	return out
}

func cascade52AwaitSend(t *testing.T, sent <-chan int, want int, what string) {
	t.Helper()
	select {
	case n := <-sent:
		if n != want {
			t.Fatalf("%s: Send = %d, want %d", what, n, want)
		}
	case <-timeAfterGuard():
		t.Fatalf("%s: Send did not return", what)
	}
}

// A subscription whose context is cancelled before it is ever iterated is
// removed on the context's goroutine. Whichever of that removal and a Send
// goes first, the Send reports no delivery and the receiver is gone when it
// returns.
func TestChanCascade52Lifecycle_CancelledBeforeIteration(t *testing.T) {
	for round := range 500 {
		x := MakeChanCascade52Opus[int]()
		ctx, cancel := context.WithCancel(context.Background())
		values := x.SubscribeContext(ctx)
		if x.Receivers() != 1 {
			t.Fatalf("round %d: Receivers = %d after SubscribeContext, want 1", round, x.Receivers())
		}
		cancel()
		cascade52AwaitSend(t, cascade52Send(x, round), 0, "after the only subscription was cancelled")
		if x.Receivers() != 0 {
			t.Fatalf("round %d: Receivers = %d after the Send returned, want 0", round, x.Receivers())
		}
		for v := range values {
			t.Fatalf("round %d: an iterator run after cancellation yielded %d", round, v)
		}
	}
}

// A never-iterated subscription stalls a round (contract item 5); cancelling
// it must release the Send, which then counts only the receiver that took
// the value.
func TestChanCascade52Lifecycle_CancelReleasesAStalledRound(t *testing.T) {
	x := MakeChanCascade52Opus[int]()
	live := x.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = x.SubscribeContext(ctx) // registered, never iterated

	sent := cascade52Send(x, 7)
	got := make(chan int, 1)
	go func() { got <- live.Recv() }() // may have to relay to the stalled peer
	apexStillBlocked(t, sent, "Send, with one subscription neither receiving nor cancelled")
	cancel()
	cascade52AwaitSend(t, sent, 1, "after the stalled subscription was cancelled")
	select {
	case v := <-got:
		if v != 7 {
			t.Fatalf("the live receiver got %d, want 7", v)
		}
	case <-timeAfterGuard():
		t.Fatal("the live receiver's Recv did not return")
	}
	if x.Receivers() != 1 {
		t.Fatalf("Receivers = %d, want 1", x.Receivers())
	}
	live.Unsubscribe()
	if x.Receivers() != 0 {
		t.Fatalf("Receivers = %d after the last Unsubscribe, want 0", x.Receivers())
	}
}

func TestChanCascade52Lifecycle_IteratorExits(t *testing.T) {
	t.Run("break", func(t *testing.T) {
		x := MakeChanCascade52Opus[int]()
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
		if x.Receivers() != 0 {
			t.Fatalf("Receivers = %d after break, want 0", x.Receivers())
		}
		apexAwait(t, sent, "the publisher, after its only receiver left")
		for v := range values { // single-use: must not unsubscribe twice
			t.Fatalf("a second run of the iterator yielded %d", v)
		}
	})

	t.Run("cancel while waiting", func(t *testing.T) {
		x := MakeChanCascade52Opus[int]()
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
		// Whichever of the iterator and the context callback removed the
		// receiver, a Send now reports nothing and leaves nothing registered.
		cascade52AwaitSend(t, cascade52Send(x, 1), 0, "after cancel")
		if x.Receivers() != 0 {
			t.Fatalf("Receivers = %d, want 0", x.Receivers())
		}
	})

	t.Run("panic in the loop body", func(t *testing.T) {
		x := MakeChanCascade52Opus[int]()
		values := x.SubscribeContext(context.Background())
		sent := cascade52Send(x, 1)
		func() {
			defer func() {
				if recover() == nil {
					t.Error("the panic did not propagate")
				}
			}()
			for range values {
				panic("loop body")
			}
		}()
		cascade52AwaitSend(t, sent, 1, "delivered before the panic")
		if x.Receivers() != 0 {
			t.Fatalf("Receivers = %d after a panicking loop body, want 0", x.Receivers())
		}
	})

	t.Run("nil yield", func(t *testing.T) {
		x := MakeChanCascade52Opus[int]()
		values := x.SubscribeContext(context.Background())
		func() {
			defer func() {
				if recover() == nil {
					t.Error("a nil yield func was accepted")
				}
			}()
			values(nil)
		}()
		if x.Receivers() != 0 {
			t.Fatalf("Receivers = %d after the nil-yield panic, want 0", x.Receivers())
		}
	})
}

// TestChanCascade52Lifecycle_ExactCountUnderChurn checks the "exactly
// counted" claim where it is hardest: receivers joining, leaving after a few
// values and being cancelled mid-round. Every value an iterator yields was
// counted by exactly one Send, and nothing else was.
func TestChanCascade52Lifecycle_ExactCountUnderChurn(t *testing.T) {
	const cycles, churners = 2000, 5
	x := MakeChanCascade52Opus[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var yielded, completed atomic.Int64
	var wg sync.WaitGroup
	// One receiver stays for the whole test, so that every Send is a real
	// round (a Send with no receivers returns at once and would let the
	// publisher finish before any churn happened).
	steady := x.SubscribeContext(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range steady {
			yielded.Add(1)
		}
	}()
	for c := range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				// Alternate between leaving by break and leaving by cancel.
				sub, leave := context.WithCancel(ctx)
				last, n := -1, 0
				for v := range x.SubscribeContext(sub) {
					yielded.Add(1)
					if v <= last {
						t.Errorf("a receiver got %d after %d", v, last)
					}
					last = v
					if n++; n == 1+c%3 {
						if i%2 == 0 {
							break
						}
						leave()
					}
				}
				leave()
				completed.Add(1)
			}
		}()
	}

	// The publisher runs until the churning receivers have completed the
	// required number of join/receive/leave cycles. Each iteration blocks in
	// a round, so this is paced by delivery, not by a clock.
	var counted, sends int64
	published := make(chan struct{})
	go func() {
		defer close(published)
		for v := 0; completed.Load() < cycles; v++ {
			counted += int64(x.Send(v))
			sends++
		}
	}()
	apexAwait(t, published, "the publisher under receiver churn")
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "receivers leaving")

	if got := yielded.Load(); got != counted {
		t.Fatalf("receivers were handed %d values but Send counted %d", got, counted)
	}
	if x.Receivers() != 0 {
		t.Fatalf("Receivers = %d after every receiver left, want 0", x.Receivers())
	}
	if counted <= sends {
		t.Fatalf("%d sends made %d deliveries: the churning receivers took part in none", sends, counted)
	}
	t.Logf("%d sends, %d deliveries, %d receiver cycles", sends, counted, completed.Load())
}
