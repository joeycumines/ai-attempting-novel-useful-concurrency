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
