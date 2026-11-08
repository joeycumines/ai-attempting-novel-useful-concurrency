package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestChanPubSub57FanoutAndLateWaiters(t *testing.T) {
	const nsubs = 8
	const nvalues = 40
	x := NewChanPubSub57DeepSeek[chan int, int](make(chan int))
	var wg sync.WaitGroup
	got := make([][]int, nsubs)
	for i := range got {
		x.Subscribe()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				v, ok := <-x.C()
				if !ok {
					return
				}
				x.Wait()
				got[i] = append(got[i], v)
				if v == nvalues {
					return
				}
			}
		}(i)
	}
	for v := 1; v <= nvalues; v++ {
		if n := x.Send(v); n != nsubs {
			t.Fatalf("Send(%d) delivered to %d subscribers, want %d", v, n, nsubs)
		}
	}
	wg.Wait()
	for i := range got {
		if len(got[i]) != nvalues {
			t.Fatalf("subscriber %d received %d values, want %d", i, len(got[i]), nvalues)
		}
		for j, v := range got[i] {
			if want := j + 1; v != want {
				t.Fatalf("subscriber %d value[%d] = %d, want %d", i, j, v, want)
			}
		}
	}
	for range nsubs {
		x.Unsubscribe()
	}
}

func TestChanPubSub57UnsubscribeReleasesPendingSend(t *testing.T) {
	x := NewChanPubSub57DeepSeek[chan int, int](make(chan int))
	x.Subscribe()
	done := make(chan int, 1)
	go func() { done <- x.Send(99) }()

	deadline := time.Now().Add(2 * time.Second)
	for !x.sending.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !x.sending.Load() {
		t.Fatal("Send did not enter its active delivery phase")
	}
	x.Unsubscribe()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("Send after withdrawing the sole receiver returned %d, want 0", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unsubscribing the undelivered receiver did not release Send")
	}
	select {
	case v := <-x.C():
		t.Fatalf("withdrawn receiver unexpectedly left value %d on the transport", v)
	default:
	}
}

func TestChanPubSub57ZeroSubscribersAndBounds(t *testing.T) {
	x := NewChanPubSub57DeepSeek[chan int, int](make(chan int))
	if n := x.Send(1); n != 0 {
		t.Fatalf("Send without subscribers = %d, want 0", n)
	}

	deferredPanic := func(fn func()) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		fn()
		return false
	}
	if !deferredPanic(func() { x.Unsubscribe() }) {
		t.Fatal("Unsubscribe below zero did not panic")
	}
	if !deferredPanic(func() { NewChanPubSub57DeepSeek[chan int, int](make(chan int, 1)) }) {
		t.Fatal("buffered transport did not panic")
	}
}

func TestChanPubSub57AdapterTryReceive(t *testing.T) {
	b := newChanPubSub57Adapter().(*chanPubSub57Adapter)
	one := b.Subscribe()
	if v, ok := one.TryReceive(); ok {
		t.Fatalf("TryReceive on an empty transport = (%d, true)", v)
	}
	published := make(chan int, 1)
	go func() { published <- b.Publish(999) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := one.TryReceive(); ok {
			if v != 999 {
				t.Fatalf("TryReceive = %d, want 999", v)
			}
			select {
			case n := <-published:
				if n != 1 {
					t.Fatalf("Publish returned %d, want 1", n)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("single-subscriber Send did not finish after TryReceive acknowledgement")
			}
			one.Unsubscribe()
			return
		}
		runtime.Gosched()
	}
	t.Fatal("TryReceive did not observe the pending publication")
}

func BenchmarkChanPubSub57Send(b *testing.B) {
	const nsubs = 8
	x := NewChanPubSub57DeepSeek[chan int, int](make(chan int))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range nsubs {
		x.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer x.Unsubscribe()
			for {
				select {
				case <-stop:
					return
				case _, ok := <-x.C():
					if !ok {
						return
					}
					x.Wait()
				}
			}
		}()
	}
	for range 8 {
		x.Send(1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Send(i)
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}

func TestChanPubSub57ContextIterator(t *testing.T) {
	x := NewChanPubSub57DeepSeek[chan int, int](make(chan int))
	ctx, cancel := context.WithCancel(context.Background())
	seq := x.SubscribeContext(ctx)
	got := make(chan int, 1)
	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		for v := range seq {
			got <- v
		}
	}()
	done := make(chan int, 1)
	go func() { done <- x.Send(42) }()
	select {
	case v := <-got:
		if v != 42 {
			t.Fatalf("iterator yielded %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("iterator did not receive publication")
	}
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("Send returned %d, want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not complete after iterator acknowledgement")
	}
	cancel()
	select {
	case <-iterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("context cancellation did not stop the iterator")
	}
}
