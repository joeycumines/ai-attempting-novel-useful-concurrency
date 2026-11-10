package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// baselinePtr is a simple atomic.Pointer[V] "latest value" cell for comparison.
type baselinePtr[V any] struct{ p atomic.Pointer[V] }

func (x *baselinePtr[V]) Store(v V) { x.p.Store(&v) }
func (x *baselinePtr[V]) Load() V {
	if p := x.p.Load(); p != nil {
		return *p
	}
	var zero V
	return zero
}

// baselineMutex is a mutex+condvar latest-value broadcast, functionally
// equivalent to WatchCell69Deepseek but without the seqlock, cache-line padding, spin,
// or iterator API.
type baselineMutex[V any] struct {
	mu   sync.Mutex
	cond *sync.Cond
	v    V
	ep   uint64
}

func newBaselineMutex[V any]() *baselineMutex[V] {
	x := &baselineMutex[V]{}
	x.cond = sync.NewCond(&x.mu)
	return x
}
func (x *baselineMutex[V]) Store(v V) {
	x.mu.Lock()
	x.v = v
	x.ep++
	x.cond.Broadcast()
	x.mu.Unlock()
}

func spawnWatchers[V any](b *testing.B, x *WatchCell69Deepseek[V], n int) (cancel func()) {
	b.Helper()
	ctx, c := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range x.Watch(ctx) {
			}
		}()
	}
	for x.Watchers() < n {
		runtime.Gosched()
	}
	return func() {
		c()
		wg.Wait()
	}
}

func BenchmarkWatchCell_Store_0Watchers(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkWatchCell_Store_1Watcher(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	cancel := spawnWatchers(b, x, 1)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkWatchCell_Store_10Watchers(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	cancel := spawnWatchers(b, x, 10)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkWatchCell_Store_100Watchers(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	cancel := spawnWatchers(b, x, 100)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkWatchCell_Store_1000Watchers(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	cancel := spawnWatchers(b, x, 1000)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkWatchCell_StoreSync_8P(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			x.StoreSync(i)
			i++
		}
	})
}

func BenchmarkWatchCell_Load(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	x.Store(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = x.Load()
	}
}

func BenchmarkWatchCell_Epoch(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = x.Epoch()
	}
}

func BenchmarkWatchCell_Watch_Iterate(b *testing.B) {
	x := NewWatchCell69Deepseek[uint64]()
	defer x.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for i := uint64(0); ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				x.Store(i)
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for range x.Watch(ctx) {
		n++
		if n == b.N {
			break
		}
	}
}

func BenchmarkBaselinePtr_Store(b *testing.B) {
	var x baselinePtr[uint64]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkBaselinePtr_Load(b *testing.B) {
	var x baselinePtr[uint64]
	x.Store(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = x.Load()
	}
}

func BenchmarkBaselineMutex_Store_0Waiters(b *testing.B) {
	x := newBaselineMutex[uint64]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
}

func BenchmarkBaselineMutex_Store_100Waiters(b *testing.B) {
	// Approximates the "naive Broadcast" cost.
	x := newBaselineMutex[uint64]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				x.mu.Lock()
				for {
					if ctx.Err() != nil {
						x.mu.Unlock()
						return
					}
					x.cond.Wait()
					break
				}
				_ = x.v
				x.mu.Unlock()
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Store(uint64(i))
	}
	b.StopTimer()
	cancel()
	x.mu.Lock()
	x.cond.Broadcast()
	x.mu.Unlock()
	wg.Wait()
}

func BenchmarkChanLatest_Store(b *testing.B) {
	// Idiomatic "latest value" via buffered channel of size 1.
	ch := make(chan uint64, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- uint64(i):
		default:
		}
	}
}
