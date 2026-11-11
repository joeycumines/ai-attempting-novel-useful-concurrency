package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestSlabCaster40GeminiPoliciesAndClose(t *testing.T) {
	sc := NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(1), WithSlabCaster40Policy(SlabPolicy40DropOldest))
	sub := sc.Subscribe()
	if got := sc.Send(1); got != 1 {
		t.Fatalf("Send(1)=%d", got)
	}
	if got := sc.Send(2); got != 1 {
		t.Fatalf("Send(2)=%d", got)
	}
	if got := <-sub.C(); got != 2 {
		t.Fatalf("got %d want newest 2", got)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("channel remained open after caster Close")
		}
	default:
		t.Fatal("closed channel not observable")
	}
}

func TestSlabCaster40GeminiDropNewest(t *testing.T) {
	sc := NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(1), WithSlabCaster40Policy(SlabPolicy40DropNewest))
	sub := sc.Subscribe()
	defer sc.Close()
	if n := sc.Send(4); n != 1 {
		t.Fatalf("Send(4)=%d", n)
	}
	if n := sc.Send(5); n != 0 {
		t.Fatalf("full DropNewest Send=%d want 0", n)
	}
	if got := <-sub.C(); got != 4 {
		t.Fatalf("got %d want 4", got)
	}
}

func TestSlabCaster40GeminiIteratorCancellation(t *testing.T) {
	sc := NewSlabCaster40Gemini[int]()
	ctx, cancel := context.WithCancel(context.Background())
	seq := sc.SubscribeContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range seq {
			return
		}
	}()
	cancel()
	<-done
	if got := sc.Subscribers(); got != 0 {
		t.Fatalf("Subscribers=%d after cancellation", got)
	}
	_ = sc.Close()
}

// Subscribers come and go while Sends are in progress, under every policy.
// The source panicked here with a send on a closed channel, and raced on the
// slot's channel field.
func TestSlabCaster40GeminiChurnDuringSend(t *testing.T) {
	for name, policy := range map[string]SlabPolicy40Gemini{"drop-oldest": SlabPolicy40DropOldest, "drop-newest": SlabPolicy40DropNewest, "block": SlabPolicy40Block} {
		t.Run(name, func(t *testing.T) {
			sc := NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(1), WithSlabCaster40Policy(policy))
			const senders, churners, cycles = 2, 4, 300
			stop := make(chan struct{})
			var stopOnce sync.Once
			halt := func() { stopOnce.Do(func() { close(stop) }) }
			defer halt() // do not leave senders spinning if the test fails
			var sending, churning sync.WaitGroup
			for range senders {
				sending.Add(1)
				go func() {
					defer sending.Done()
					for v := 0; ; v++ {
						select {
						case <-stop:
							return
						default:
							sc.Send(v)
							// Yield between Sends. A closer waits for a Send
							// that is part-way through a delivery; on one CPU
							// a sender that never yields is only ever
							// descheduled by preemption, a full time slice
							// later, and the test would crawl.
							runtime.Gosched()
						}
					}
				}()
			}
			for range churners {
				churning.Add(1)
				go func() {
					defer churning.Done()
					for range cycles {
						sub := sc.Subscribe()
						ch := sub.C()
						// Take at most one value, then leave whether or not
						// one arrived: a Send may be mid-delivery either way.
						select {
						case <-ch:
						default:
							runtime.Gosched()
						}
						sub.Close()
						if _, open := <-ch; open {
							// A value buffered before the close is still
							// readable; after it the channel must be closed.
							if _, open = <-ch; open {
								t.Error("the channel of a closed subscription stayed open")
							}
						}
					}
				}()
			}
			waitOrFail(t, &churning, "subscriber churn did not finish")
			halt()
			waitOrFail(t, &sending, "a Send did not return after the last subscriber left")
			if n := sc.Subscribers(); n != 0 {
				t.Errorf("Subscribers = %d after every subscriber left, want 0", n)
			}
			if n := sc.Send(1); n != 0 {
				t.Errorf("Send with no subscribers delivered %d", n)
			}
			_ = sc.Close()
		})
	}
}

func waitOrFail(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

// Under the Block policy a Send waits for room in a full channel. The
// subscriber leaving, or the caster closing, releases it.
func TestSlabCaster40GeminiBlockedSendIsReleased(t *testing.T) {
	for name, release := range map[string]func(*SlabCaster40Gemini[int], *SlabSubscription40Gemini[int]){
		"unsubscribe": func(_ *SlabCaster40Gemini[int], sub *SlabSubscription40Gemini[int]) { sub.Close() },
		"close":       func(sc *SlabCaster40Gemini[int], _ *SlabSubscription40Gemini[int]) { _ = sc.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			sc := NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(1), WithSlabCaster40Policy(SlabPolicy40Block))
			sub := sc.Subscribe()
			ch := sub.C()
			if n := sc.Send(1); n != 1 {
				t.Fatalf("Send into an empty buffer delivered %d, want 1", n)
			}
			slot := &sub.slab40.slots[sub.index]
			sent := make(chan int, 1)
			go func() { sent <- sc.Send(2) }() // the buffer is full: this blocks
			deadline := time.Now().Add(5 * time.Second)
			for slot.inflight.Load() == 0 {
				if time.Now().After(deadline) {
					t.Fatal("the second Send never reached the slot")
				}
				runtime.Gosched()
			}
			released := make(chan struct{})
			go func() { release(sc, sub); close(released) }()
			select {
			case <-released:
			case <-time.After(5 * time.Second):
				t.Fatal("the close did not return while a Send was blocked on the subscriber")
			}
			select {
			case n := <-sent:
				// Not delivered: the subscriber was closed before it took the value.
				if n != 0 {
					t.Errorf("the released Send reported %d deliveries, want 0", n)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the blocked Send was not released")
			}
			for range ch { // whatever was buffered, then closed
			}
			_ = sc.Close()
		})
	}
}

// A handle is shared with the slot's next subscriber. A cancelled iterator
// must not close its subscription a second time after returning: in the
// source the context callback could run late and close whoever had taken the
// slot since.
func TestSlabCaster40GeminiIteratorClosesOnce(t *testing.T) {
	sc := NewSlabCaster40Gemini[int]()
	defer sc.Close()
	for i := range 500 {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			next, stop := iterPull40(sc.SubscribeContext(ctx), started)
			defer stop()
			next()
		}()
		<-started
		cancel()
		<-done
		// The slot is free again and is the lowest one, so this takes it.
		sub := sc.Subscribe()
		for range 20 {
			runtime.Gosched() // room for a late second close to run
		}
		if n := sc.Subscribers(); n != 1 {
			t.Fatalf("iteration %d: Subscribers = %d after one Subscribe, want 1", i, n)
		}
		if n := sc.Send(i); n != 1 {
			t.Fatalf("iteration %d: Send delivered %d, want 1: the new subscriber was closed", i, n)
		}
		if v, open := <-sub.C(); !open || v != i {
			t.Fatalf("iteration %d: received %d, open %v", i, v, open)
		}
		sub.Close()
	}
}

// iterPull40 starts seq and returns a function that takes one value from it.
// started is closed once the iterator is subscribed and waiting.
func iterPull40(seq func(func(int) bool), started chan struct{}) (next func(), stop func()) {
	values := make(chan int)
	quit := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		close(started)
		seq(func(v int) bool {
			select {
			case values <- v:
				return true
			case <-quit:
				return false
			}
		})
	}()
	return func() {
			select {
			case <-values:
			case <-finished:
			}
		}, func() {
			close(quit)
			<-finished
		}
}

// Subscribe racing the caster's Close either gets a subscription whose
// channel Close closes, or reports the caster closed. It never returns a
// subscription that is left open for ever.
func TestSlabCaster40GeminiSubscribeRacingClose(t *testing.T) {
	for range 300 {
		sc := NewSlabCaster40Gemini[int]()
		got := make(chan *SlabSubscription40Gemini[int], 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					if r != ErrSlabCaster40Closed {
						panic(r)
					}
					got <- nil
				}
			}()
			got <- sc.Subscribe()
		}()
		_ = sc.Close()
		sub := <-got
		if sub == nil {
			continue // reported closed
		}
		select {
		case _, open := <-sub.C():
			if open {
				t.Fatal("received a value from a caster nobody sent on")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a subscription returned during Close was left open")
		}
	}
}
