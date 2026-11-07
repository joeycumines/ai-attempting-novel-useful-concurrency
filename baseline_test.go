package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestChanCaster_Basic(t *testing.T) {
	ch := make(chan int)
	cc := NewChanCaster(ch)

	var wg sync.WaitGroup
	const n = 3
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			cc.Add(1)
			val := <-ch
			if val != 42 {
				t.Errorf("expected 42, got %d", val)
			}
		}()
	}

	// Wait for receivers to register
	for cc.Add(0) < n {
	}

	sent := cc.Send(42)
	if sent != n {
		t.Fatalf("expected sent=%d, got %d", n, sent)
	}

	wg.Wait()
}

func TestChanPubSub_Basic(t *testing.T) {
	ch := make(chan string)
	ps := NewChanPubSub(ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numSubs = 4
	var wg sync.WaitGroup
	wg.Add(numSubs)

	for i := 0; i < numSubs; i++ {
		go func() {
			defer wg.Done()
			for v := range ps.SubscribeContext(ctx) {
				if v == "stop" {
					break
				}
			}
		}()
	}

	for ps.Add(0) < numSubs {
	}

	sent := ps.Send("hello")
	if sent != numSubs {
		t.Fatalf("expected %d sent, got %d", numSubs, sent)
	}

	sent = ps.Send("stop")
	if sent != numSubs {
		t.Fatalf("expected %d sent for stop, got %d", numSubs, sent)
	}

	wg.Wait()
}

func TestBaselineFanoutDelivery(t *testing.T) {
	b := NewBaselineFanout[int](8)
	defer b.Close()
	id1 := b.Subscribe()
	id2 := b.Subscribe()
	defer b.Unsubscribe(id2)
	defer b.Unsubscribe(id1)

	for i := 1; i <= 5; i++ {
		if n := b.Send(i); n != 2 {
			t.Fatalf("Send(%d) delivered to %d subscribers, want 2", i, n)
		}
	}
	for _, id := range []uint64{id1, id2} {
		ch := b.C(id)
		for i := 1; i <= 5; i++ {
			select {
			case v := <-ch:
				if v != i {
					t.Fatalf("sub %d: got %d, want %d", id, v, i)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("sub %d: timeout waiting for %d", id, i)
			}
		}
	}
}

func TestBaselineFanoutDropOldest(t *testing.T) {
	b := NewBaselineFanout[int](4)
	defer b.Close()
	id := b.Subscribe()
	defer b.Unsubscribe(id)
	// Flood without reading: buffer must keep the NEWEST values.
	for i := 1; i <= 100; i++ {
		b.Send(i)
	}
	ch := b.C(id)
	last := -1
	for {
		select {
		case v := <-ch:
			if v <= last {
				t.Fatalf("out of order: %d after %d", v, last)
			}
			last = v
		default:
			if last != 100 {
				t.Fatalf("final value = %d, want 100 (newest must be kept)", last)
			}
			return
		}
	}
}

func TestBaselineFanoutSubscribeContext(t *testing.T) {
	b := NewBaselineFanout[int](8)
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	seq := b.SubscribeContext(ctx) // registers synchronously
	done := make(chan int, 1)
	go func() {
		n := 0
		for range seq {
			n++
			if n == 3 {
				done <- n
				return
			}
		}
	}()
	for i := 1; i <= 3; i++ {
		b.Send(i)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SubscribeContext did not yield 3 values")
	}
	cancel()
}

func TestBaselineFanoutClose(t *testing.T) {
	b := NewBaselineFanout[int](8)
	id := b.Subscribe()
	b.Close()
	b.Close() // idempotent
	if n := b.Send(1); n != 0 {
		t.Fatalf("Send after Close delivered to %d, want 0", n)
	}
	_ = id
}

func TestBaselineRendezvousCasterBasic(t *testing.T) {
	x := NewBaselineRendezvousCaster[int]()
	if got := x.Send(1); got != 0 {
		t.Fatalf("Send with no receivers = %d, want 0", got)
	}
	const receivers = 3
	for i := 0; i < receivers; i++ {
		x.Add(1)
	}
	done := make(chan int, 1)
	go func() { done <- x.Send(42) }()
	for i := 0; i < receivers; i++ {
		select {
		case v := <-x.C:
			if v != 42 {
				t.Fatalf("received %d, want 42", v)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("receiver blocked")
		}
	}
	select {
	case n := <-done:
		if n != receivers {
			t.Fatalf("Send = %d, want %d", n, receivers)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked")
	}
}

func TestBaselineScatterAndGatherBasic(t *testing.T) {
	s := NewBaselineScatterQueue[int]()
	if got := s.Send(1); got != 0 {
		t.Fatalf("scatter Send with no receivers = %d, want 0", got)
	}
	s.Add(1)
	done := make(chan int, 1)
	go func() { done <- s.Send(7) }()
	select {
	case v := <-s.C:
		if v != 7 {
			t.Fatalf("scatter received %d, want 7", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scatter receiver blocked")
	}
	if n := <-done; n != 1 {
		t.Fatalf("scatter Send = %d, want 1", n)
	}

	g := NewBaselineGatherBarrier[int]()
	if got := g.Wait(); got != 0 {
		t.Fatalf("gather Wait with no senders = %d, want 0", got)
	}
	g.Add(2)
	go func() { g.C <- 1 }()
	go func() { g.C <- 2 }()
	waitDone := make(chan int, 1)
	go func() { waitDone <- g.Wait() }()
	select {
	case n := <-waitDone:
		if n != 2 {
			t.Fatalf("gather Wait = %d, want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gather Wait blocked")
	}
}

func TestBaselineLatestAndBufferedBasic(t *testing.T) {
	latest := NewBaselineLatestBroadcast[int]()
	defer latest.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq := latest.SubscribeContext(ctx)
	got := make(chan int, 4)
	go func() {
		for v := range seq {
			got <- v
			if v == 3 {
				return
			}
		}
	}()
	latest.Send(1)
	latest.Send(2)
	latest.Send(3)
	select {
	case v := <-got:
		if v != 3 && v != 1 && v != 2 {
			t.Fatalf("latest received unexpected %d", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("latest subscriber blocked")
	}

	buffered := NewBaselineBufferedBroadcast[int](4)
	defer buffered.Close()
	bseq := buffered.SubscribeContext(ctx)
	bgot := make(chan int, 4)
	go func() {
		n := 0
		for v := range bseq {
			bgot <- v
			n++
			if n == 3 {
				return
			}
		}
	}()
	for i := 1; i <= 3; i++ {
		buffered.Send(i)
	}
	for i := 1; i <= 3; i++ {
		select {
		case v := <-bgot:
			if v != i {
				t.Fatalf("buffered received %d, want %d", v, i)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("buffered subscriber blocked")
		}
	}
}
