package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBroadcaster_SingleConsumer_InOrder(t *testing.T) {
	const N = 10_000
	b := NewBroadcaster65Deepseek[int](N) // no overrun
	defer b.Close()

	sub := b.Subscribe(context.Background())
	defer sub.Close()

	for i := 0; i < N; i++ {
		if _, err := b.Publish(i); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		v, ok := sub.Next()
		if !ok {
			t.Fatalf("Next false at %d", i)
		}
		if v != i {
			t.Fatalf("got %d want %d", v, i)
		}
	}

	if _, r := sub.TryNext(); r != ReadNotReady65Deepseek {
		t.Fatalf("expected ReadNotReady65Deepseek, got %v", r)
	}
}

func TestBroadcaster_MultiConsumer_Fanout(t *testing.T) {
	const (
		N       = 2000
		workers = 32
	)
	b := NewBroadcaster65Deepseek[int](N)
	defer b.Close()

	// Subscribe all consumers BEFORE the first publish: Subscribe observes
	// only publishes issued after the call, so this is what guarantees every
	// consumer sees the full 0..N-1 sequence. (The original test synchronised
	// with a start channel but still raced: close(start) does not order
	// goroutine scheduling against the publish loop.)
	subs := make([]*Subscription65Deepseek[int], workers)
	for w := range subs {
		subs[w] = b.Subscribe(context.Background())
	}
	defer func() {
		for _, s := range subs {
			s.Close()
		}
	}()

	var wg sync.WaitGroup
	var ok int32
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(sub *Subscription65Deepseek[int]) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				v, good := sub.Next()
				if !good {
					t.Errorf("next false")
					return
				}
				if v != i {
					t.Errorf("got %d want %d", v, i)
					return
				}
			}
			atomic.AddInt32(&ok, 1)
		}(subs[w])
	}
	for i := 0; i < N; i++ {
		if _, err := b.Publish(i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	wg.Wait()
	if ok != workers {
		t.Fatalf("only %d/%d consumers completed", ok, workers)
	}
}

func TestBroadcaster_OverrunLoss(t *testing.T) {
	const cap = 8
	b := NewBroadcaster65Deepseek[int](cap)
	defer b.Close()

	sub := b.Subscribe(context.Background())
	defer sub.Close()

	const N = 1000
	for i := 0; i < N; i++ {
		if _, err := b.Publish(i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	// Drain via TryNext — we should reach the last value and observe skips.
	var last int = -1
	var count int
	for {
		v, r := sub.TryNext()
		if r != ReadOK65Deepseek {
			break
		}
		last = v
		count++
	}
	if last != N-1 {
		t.Fatalf("expected last=%d, got %d", N-1, last)
	}
	if count > cap+1 {
		t.Fatalf("expected ≤ cap+1 reads (%d), got %d (lossy semantics violated)", cap+1, count)
	}
	if sub.Skipped() == 0 {
		t.Fatalf("expected non-zero skipped, got 0")
	}
}

func TestBroadcaster_ContextCancellation(t *testing.T) {
	b := NewBroadcaster65Deepseek[int](16)
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sub := b.Subscribe(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, ok := sub.Next()
		if ok {
			t.Errorf("expected Next false after ctx cancel")
		}
	}()

	// Wait until the consumer is actually parked in Next before cancelling.
	deadline := time.Now().Add(10 * time.Second)
	for !sub.waiting.Load() {
		if time.Now().After(deadline) {
			t.Fatal("consumer did not park in Next before the deadline")
		}
		runtime.Gosched()
	}
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Next did not return after context cancellation")
	}
}

func TestBroadcaster_CloseDrain(t *testing.T) {
	b := NewBroadcaster65Deepseek[int](64)
	sub := b.Subscribe(context.Background())
	defer sub.Close()

	for i := 0; i < 100; i++ {
		if _, err := b.Publish(i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := b.Publish(999); err != ErrBroadcasterClosed65Deepseek {
		t.Fatalf("expected ErrBroadcasterClosed65Deepseek, got %v", err)
	}

	// Drain remaining (should reach 99) then terminate.
	var last int = -1
	for {
		v, ok := sub.Next()
		if !ok {
			break
		}
		last = v
	}
	if last != 99 {
		t.Fatalf("expected drain to 99, got %d", last)
	}
}

func TestBroadcaster_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		b := NewBroadcaster65Deepseek[int](16)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sub := b.Subscribe(ctx)
				for {
					if _, ok := sub.Next(); !ok {
						return
					}
				}
			}()
		}
		for j := 0; j < 100; j++ {
			b.Publish(j)
		}
		b.Close()
		cancel()
		wg.Wait()
	}
	// Allow scheduler to finalise goroutines: poll until the count settles
	// (all consumer goroutines returned before wg.Wait() observed them).
	deadline := time.Now().Add(10 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= before+2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: before=%d after=%d", before, after)
		}
		runtime.Gosched()
	}
}

func TestBroadcaster_Stress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	b := NewBroadcaster65Deepseek[int](256)
	defer b.Close()

	var wg sync.WaitGroup
	var consumerCount int32
	const consumers = 16
	const publishes = 200_000

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := b.Subscribe(context.Background())
			defer sub.Close()
			for {
				v, r := sub.TryNext()
				if r == ReadClosed65Deepseek {
					atomic.AddInt32(&consumerCount, 1)
					return
				}
				if r == ReadNotReady65Deepseek {
					// Yield to avoid pure spin; the lossy design tolerates it.
					runtime.Gosched()
					continue
				}
				_ = v
			}
		}()
	}

	for i := 0; i < publishes; i++ {
		if _, err := b.Publish(i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	b.Close()
	wg.Wait()
	if consumerCount != consumers {
		t.Fatalf("only %d/%d consumers completed", consumerCount, consumers)
	}
}

// TestBroadcaster_Linearizability verifies that for a single caught-up
// consumer, every value appears exactly once and in publish order.
func TestBroadcaster_Linearizability(t *testing.T) {
	const N = 50_000
	b := NewBroadcaster65Deepseek[int](N)
	defer b.Close()
	sub := b.Subscribe(context.Background())
	defer sub.Close()

	for i := 0; i < N; i++ {
		if _, err := b.Publish(i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	for i := 0; i < N; i++ {
		v, ok := sub.Next()
		if !ok {
			t.Fatalf("Next false at %d", i)
		}
		if v != i {
			t.Fatalf("got %d want %d", v, i)
		}
	}
}
