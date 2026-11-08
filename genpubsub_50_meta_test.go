package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestGenPubSub50Parity(t *testing.T) {
	x := NewGenPubSub50Meta(make(chan int))
	const subs, msgs = 8, 400
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make([][]int, subs)
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		seq := x.SubscribeContext(ctx)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for v := range seq {
				results[i] = append(results[i], v)
			}
		}(i)
	}
	for v := 1; v <= msgs; v++ {
		done := make(chan int, 1)
		go func(v int) { done <- x.Send(v) }(v)
		select {
		case n := <-done:
			if n != subs {
				t.Fatalf("Send=%d want %d", n, subs)
			}
		case <-time.After(scenarioGuard):
			t.Fatalf("Send(%d) blocked", v)
		}
	}
	cancel()
	wg.Wait()
	for i, got := range results {
		if len(got) != msgs {
			t.Fatalf("sub %d got %d", i, len(got))
		}
		for j, v := range got {
			if v != j+1 {
				t.Fatalf("sub %d index %d got %d", i, j, v)
			}
		}
	}
}

func TestGenPubSub50CancellationOwnership(t *testing.T) {
	x := NewGenPubSub50Meta(make(chan int))
	for round := 0; round < 100; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		seq := x.SubscribeContext(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range seq {
			}
		}()
		sendDone := make(chan int, 1)
		go func() { sendDone <- x.Send(round + 1) }()
		cancel()
		select {
		case <-sendDone:
		case <-time.After(scenarioGuard):
			t.Fatalf("round %d stranded", round)
		}
		<-done
		if n := x.Add(0); n != 0 {
			t.Fatalf("round %d leaked %d", round, n)
		}
	}
}

func BenchmarkGenPubSub50Send(b *testing.B) {
	x := NewGenPubSub50Meta(make(chan int))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const subs = 8
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		seq := x.SubscribeContext(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range seq {
			}
		}()
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
