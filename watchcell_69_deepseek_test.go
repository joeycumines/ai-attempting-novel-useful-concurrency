package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitWatchers69Deepseek[V any](t *testing.T, x *WatchCell69Deepseek[V], want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if x.Watchers() >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timeout waiting for %d watchers, got %d", want, x.Watchers())
}

func TestWatchCell_LoadBeforePublish(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	defer x.Close()
	if v, ep := x.Load(); v != 0 || ep != 0 {
		t.Fatalf("want zero, got v=%d ep=%d", v, ep)
	}
	if x.Epoch() != 0 {
		t.Fatalf("epoch should be 0")
	}
}

func TestWatchCell_StoreLoad(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	defer x.Close()
	for i := 1; i <= 5; i++ {
		x.Store(i)
		v, ep := x.Load()
		if v != i || ep != uint64(i) {
			t.Fatalf("iter %d: got v=%d ep=%d", i, v, ep)
		}
	}
}

func TestWatchCell_WatchSingle(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	defer x.Close()
	x.Store(0) // prime so Watch yields immediately

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan int, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range x.Watch(ctx) {
			got <- v
		}
	}()
	waitWatchers69Deepseek(t, x, 1)

	if v := <-got; v != 0 {
		t.Fatalf("initial want 0, got %d", v)
	}
	for i := 1; i <= 5; i++ {
		x.Store(i)
		select {
		case v := <-got:
			if v != i {
				t.Fatalf("want %d, got %d", i, v)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %d", i)
		}
	}
	cancel()
	wg.Wait()
}

func TestWatchCell_MultipleWatchers(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	defer x.Close()
	x.Store(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 8
	results := make([]chan int, n)
	for i := range results {
		results[i] = make(chan int, 10)
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		ch := results[i]
		go func() {
			defer wg.Done()
			for v := range x.Watch(ctx) {
				ch <- v
			}
		}()
	}
	waitWatchers69Deepseek(t, x, n)

	// Drain initial zeros.
	for i := 0; i < n; i++ {
		if v := <-results[i]; v != 0 {
			t.Fatalf("watcher %d: initial want 0, got %d", i, v)
		}
	}
	for i := 1; i <= 3; i++ {
		x.Store(i)
		for j := 0; j < n; j++ {
			select {
			case v := <-results[j]:
				if v != i {
					t.Fatalf("watcher %d: want %d got %d", j, i, v)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("watcher %d: timeout for %d", j, i)
			}
		}
	}
	cancel()
	wg.Wait()
}

func TestWatchCell_LossyUnderLoad(t *testing.T) {
	// Under heavy publication, subscribers may skip intermediate values but
	// must eventually observe a monotone sequence ending at the latest value.
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lastSeen atomic.Uint64
	var yields atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range x.Watch(ctx) {
			prev := lastSeen.Swap(v)
			if v < prev {
				t.Errorf("non-monotone: %d after %d", v, prev)
			}
			yields.Add(1)
		}
	}()
	waitWatchers69Deepseek(t, x, 1)

	const publishes = 100_000
	for i := uint64(1); i <= publishes; i++ {
		x.Store(i)
	}
	// Give the watcher a moment to catch up to the final value.
	deadline := time.Now().Add(3 * time.Second)
	for lastSeen.Load() != publishes && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := lastSeen.Load(); got != publishes {
		t.Fatalf("final want %d, got %d (yields=%d)", publishes, got, yields.Load())
	}
	cancel()
	wg.Wait()
}

func TestWatchCell_ContextCancel(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	defer x.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range x.Watch(ctx) {
			// Should not run: no Store.
		}
	}()
	waitWatchers69Deepseek(t, x, 1)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not exit on cancel")
	}
}

func TestWatchCell_CloseWakesWatchers(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	const n = 4
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			for range x.Watch(context.Background()) {
			}
			done <- struct{}{}
		}()
	}
	waitWatchers69Deepseek(t, x, n)
	x.Close()
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("watcher %d did not exit on Close", i)
		}
	}
	if x.Watchers() != 0 {
		t.Fatalf("watchers leaked: %d", x.Watchers())
	}
}

func TestWatchCell_StoreAfterClose(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	x.Close()
	x.Store(1)     // no-op
	x.StoreSync(2) // no-op
	if v, _ := x.Load(); v != 0 {
		t.Fatalf("want 0, got %d", v)
	}
	x.Close() // idempotent
}

func TestWatchCell_StoreSyncConcurrent(t *testing.T) {
	x := NewWatchCell69Deepseek[int]()
	defer x.Close()

	const writers = 8
	const perWriter = 1000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				x.StoreSync(i)
			}
		}()
	}
	wg.Wait()
	if ep := x.Epoch(); ep != uint64(writers*perWriter) {
		t.Fatalf("want epoch %d, got %d", writers*perWriter, ep)
	}
}

func TestWatchCell_NoGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	for trial := 0; trial < 20; trial++ {
		x := NewWatchCell69Deepseek[int]()
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			for range x.Watch(ctx) {
			}
		}()
		waitWatchers69Deepseek(t, x, 1)
		x.Store(trial)
		cancel()
		x.Close()
	}
	// Poll until the watcher goroutines have exited (cancel+Close already
	// returned); fail after a hard deadline.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if now := runtime.NumGoroutine(); now <= base+5 {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: base=%d now=%d", base, now)
		}
		runtime.Gosched()
	}
}
