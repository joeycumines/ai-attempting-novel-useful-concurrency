package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ==== Pulse61Deepseek: publish only ==========================================

func BenchmarkPulse_Set(b *testing.B) {
	p := NewPulse61Deepseek[uint64]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Set(uint64(i))
	}
}

func BenchmarkPulse_SetParallel(b *testing.B) {
	p := NewPulse61Deepseek[uint64]()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			_ = p.Set(i)
			i++
		}
	})
}

// ==== Pulse61Deepseek: read only =============================================

func BenchmarkPulse_Load(b *testing.B) {
	p := NewPulse61Deepseek[uint64]()
	_ = p.Set(1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = p.Load()
	}
}

func BenchmarkPulse_LoadParallel(b *testing.B) {
	p := NewPulse61Deepseek[uint64]()
	_ = p.Set(1)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = p.Load()
		}
	})
}

// ==== Pulse61Deepseek: M:1 contention (publishers only) ======================

func BenchmarkPulse_Set_Contended(b *testing.B) {
	for _, procs := range []int{1, 2, 4, 8, 16} {
		b.Run(procName(procs), func(b *testing.B) {
			p := NewPulse61Deepseek[uint64]()
			b.ReportAllocs()
			var wg sync.WaitGroup
			wg.Add(procs)
			b.ResetTimer()
			for i := 0; i < procs; i++ {
				go func(tag uint64) {
					defer wg.Done()
					for j := 0; j < b.N/procs; j++ {
						_ = p.Set(tag | uint64(j))
					}
				}(uint64(i) << 32)
			}
			wg.Wait()
		})
	}
}

// ==== Pulse61Deepseek: M:N wakeup fanout =====================================

func BenchmarkPulse_WaitFor_Wakeup(b *testing.B) {
	for _, subs := range []int{1, 8, 64, 256} {
		b.Run(procName(subs), func(b *testing.B) {
			p := NewPulse61Deepseek[uint64]()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(subs)
			var woke atomic.Int64
			for i := 0; i < subs; i++ {
				go func() {
					defer wg.Done()
					var since uint64
					for {
						v, ver, err := p.WaitFor(ctx, since)
						if err != nil {
							return
						}
						_ = v
						since = ver
						woke.Add(1)
					}
				}()
			}
			// Settle: bounded poll; fail rather than spin forever if a waiter never parks.
			deadline := time.Now().Add(10 * time.Second)
			for p.Waiters() < subs {
				if time.Now().After(deadline) {
					b.Fatalf("timed out waiting for %d waiters (got %d)", subs, p.Waiters())
				}
				runtime.Gosched()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = p.Set(uint64(i + 1))
			}
			b.StopTimer()
			cancel()
			wg.Wait()
			b.ReportMetric(float64(woke.Load())/float64(b.N), "wake/publish")
		})
	}
}

// ==== Baselines for comparison =====================================

func BenchmarkBaseline_RWMutex_Load(b *testing.B) {
	var mu sync.RWMutex
	var v uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.RLock()
			_ = v
			mu.RUnlock()
		}
	})
}

func BenchmarkBaseline_RWMutex_StoreLoad(b *testing.B) {
	var mu sync.RWMutex
	var v uint64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		v = uint64(i)
		mu.Unlock()
		mu.RLock()
		_ = v
		mu.RUnlock()
	}
}

func BenchmarkBaseline_AtomicValue_Load(b *testing.B) {
	var av atomic.Value
	av.Store(uint64(1))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = av.Load().(uint64)
		}
	})
}

func BenchmarkBaseline_AtomicUint64_Load(b *testing.B) {
	var v atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = v.Load()
		}
	})
}

func BenchmarkBaseline_ChanFanout(b *testing.B) {
	for _, subs := range []int{8, 64} {
		b.Run(procName(subs), func(b *testing.B) {
			// idiomatic channel broadcast is inherently O(subs)/publish;
			// we approximate the ideal cost by measuring N channel sends.
			ch := make(chan uint64, subs)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < subs; j++ {
					ch <- uint64(i)
				}
				for j := 0; j < subs; j++ {
					<-ch
				}
			}
		})
	}
}

func procName(n int) string {
	switch n {
	case 1:
		return "1P"
	case 2:
		return "2P"
	case 4:
		return "4P"
	case 8:
		return "8P"
	case 16:
		return "16P"
	case 32:
		return "32P"
	case 64:
		return "64P"
	case 128:
		return "128P"
	case 256:
		return "256P"
	}
	return "N"
}
