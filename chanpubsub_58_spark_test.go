package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestChanPubSub58AtomicPong(t *testing.T) {
	x := NewChanPubSub58Spark(make(chan int))
	const subs, msgs = 8, 500
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, subs)
	results := make([][]int, subs)
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		seq := x.SubscribeContext(ctx)
		ready <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for v := range seq {
				results[i] = append(results[i], v)
			}
		}(i)
	}
	for i := 0; i < subs; i++ {
		<-ready
	}
	for v := 1; v <= msgs; v++ {
		done := make(chan int, 1)
		go func(v int) { done <- x.Send(v) }(v)
		select {
		case n := <-done:
			if n != subs {
				t.Fatalf("Send(%d)=%d want %d", v, n, subs)
			}
		case <-time.After(scenarioGuard):
			t.Fatalf("Send(%d) blocked", v)
		}
	}
	cancel()
	wg.Wait()
	for i, got := range results {
		if len(got) != msgs {
			t.Fatalf("subscriber %d got %d values want %d", i, len(got), msgs)
		}
		for j, v := range got {
			if v != j+1 {
				t.Fatalf("subscriber %d gap/dup at %d: %d", i, j, v)
			}
		}
	}
	if n := x.Add(0); n != 0 {
		t.Fatalf("subscribers after cancel=%d", n)
	}
}

func TestChanPubSub58CancellationOwnership(t *testing.T) {
	x := NewChanPubSub58Spark(make(chan int))
	for round := 0; round < 100; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		seq := x.SubscribeContext(ctx)
		started := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			close(started)
			for range seq {
			}
		}()
		<-started
		sendDone := make(chan int, 1)
		go func() { sendDone <- x.Send(round + 1) }()
		cancel()
		select {
		case <-sendDone:
		case <-time.After(scenarioGuard):
			t.Fatalf("round %d: Send stranded after cancellation", round)
		}
		<-done
		if n := x.Add(0); n != 0 {
			t.Fatalf("round %d leaked %d subscribers", round, n)
		}
	}
}

func BenchmarkChanPubSub58Send(b *testing.B) {
	x := NewChanPubSub58Spark(make(chan int))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const subs = 8
	var wg sync.WaitGroup
	ready := make(chan struct{}, subs)
	for i := 0; i < subs; i++ {
		seq := x.SubscribeContext(ctx)
		ready <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range seq {
			}
		}()
	}
	for i := 0; i < subs; i++ {
		<-ready
	}
	b.ReportAllocs()
	b.ResetTimer()
	for v := 1; v <= b.N; v++ {
		x.Send(v)
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}
