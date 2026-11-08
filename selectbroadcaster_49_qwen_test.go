package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSelectBroadcaster49QwenBroadcastEpochAndEagerSubscribe(t *testing.T) {
	b := NewSelectBroadcaster49Qwen[int](8)
	state := b.Epoch()
	if state.Seq() != 0 {
		t.Fatalf("initial epoch sequence = %d, want 0", state.Seq())
	}

	const observers = 16
	got := make(chan int, observers)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(observers)
	done.Add(observers)
	for range observers {
		seq := b.SubscribeContext(context.Background())
		go func() {
			defer done.Done()
			ready.Done()
			seq(func(value int) bool {
				got <- value
				return false
			})
		}()
	}
	ready.Wait()
	b.Send(42)
	select {
	case <-state.Notify():
	case <-time.After(5 * time.Second):
		t.Fatal("published epoch did not wake the old state notification")
	}
	if value := state.Load(1); value != 42 {
		t.Fatalf("state.Load(1) = %d, want 42", value)
	}
	for i := 0; i < observers; i++ {
		select {
		case value := <-got:
			if value != 42 {
				t.Fatalf("observer got %d, want 42", value)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d observers woke", i, observers)
		}
	}
	done.Wait()
}

func TestSelectBroadcaster49QwenOverrunKeepsSnapshotsOrdered(t *testing.T) {
	type payload struct {
		seq   uint64
		words [4]uint64
	}
	makeValue := func(seq uint64) payload {
		v := payload{seq: seq}
		for i := range v.words {
			v.words[i] = seq*31 + uint64(i)
		}
		return v
	}

	b := NewSelectBroadcaster49Qwen[payload](4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq := b.SubscribeContext(ctx)
	started := make(chan struct{})
	release := make(chan struct{})
	values := make(chan payload, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		seq(func(value payload) bool {
			values <- value
			if value.seq == 1 {
				close(started)
				<-release
			}
			return value.seq != 20
		})
	}()

	b.Send(makeValue(1))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not reach the first value")
	}
	for i := uint64(2); i <= 20; i++ {
		b.Send(makeValue(i))
	}
	close(release)

	wantSeqs := []uint64{1, 17, 18, 19, 20}
	for _, wantSeq := range wantSeqs {
		select {
		case value := <-values:
			if want := makeValue(wantSeq); value != want {
				t.Fatalf("got %+v, want %+v", value, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("reader did not catch up to sequence %d", wantSeq)
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not stop after the terminal value")
	}
}

func TestSelectBroadcaster49QwenStateRejectsStaleOrFuture(t *testing.T) {
	b := NewSelectBroadcaster49Qwen[int](2)
	b.Send(1)
	state := b.Epoch()
	b.Send(2)
	b.Send(3)
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		fn()
	}
	mustPanic("stale sequence", func() { state.Load(1) })
	mustPanic("future sequence", func() { state.Load(4) })
}

func BenchmarkSelectBroadcaster49QwenSend(b *testing.B) {
	const subscribers = 8
	x := NewSelectBroadcaster49Qwen[int](64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan struct{}, subscribers)
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(subscribers)
	done.Add(subscribers)
	for range subscribers {
		seq := x.SubscribeContext(ctx)
		go func() {
			defer done.Done()
			ready.Done()
			seq(func(value int) bool {
				if value == b.N+1 {
					seen <- struct{}{}
					return false
				}
				return true
			})
		}()
	}
	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		x.Send(i)
	}
	b.StopTimer()
	x.Send(b.N + 1)
	for range subscribers {
		select {
		case <-seen:
		case <-time.After(5 * time.Second):
			b.Fatal("not all readers observed the terminal generation")
		}
	}
	cancel()
	done.Wait()
}
