package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkRingPublish measures the publisher alone (no parked consumers) for two payload sizes.
func BenchmarkRingPublish(b *testing.B) {
	b.Run("u64", func(b *testing.B) { benchRingPublish(b, uint64(42)) })
	b.Run("64B", func(b *testing.B) { benchRingPublish(b, [8]uint64{1, 2, 3, 4, 5, 6, 7, 8}) })
}

func benchRingPublish[V any](b *testing.B, v V) {
	r := NewRing62Haiku[V](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Publish(v)
	}
}

// BenchmarkRingFanout: one publisher, N consumers using blocking Next.
func BenchmarkRingFanout(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			benchRingFanout(b, subs, false)
		})
	}
}

// BenchmarkRingFanoutDrain: same, but consumers batch with Drain and only park when empty.
func BenchmarkRingFanoutDrain(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			benchRingFanout(b, subs, true)
		})
	}
}

func benchRingFanout(b *testing.B, subs int, batch bool) {
	r := NewRing62Haiku[uint64](4096)
	var (
		wg        sync.WaitGroup
		delivered atomic.Uint64
	)
	ctx := context.Background()
	for range subs {
		c := r.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n uint64
			if batch {
				buf := make([]uint64, 64)
				for {
					k, _ := c.Drain(buf)
					n += uint64(k)
					if k == 0 {
						if _, _, err := c.Next(ctx); err != nil {
							break
						}
						n++
					}
				}
			} else {
				for {
					if _, _, err := c.Next(ctx); err != nil {
						break
					}
					n++
				}
			}
			delivered.Add(n)
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		r.Publish(uint64(i))
	}
	r.Close()
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(delivered.Load())/b.Elapsed().Seconds(), "deliveries/s")
}

// BenchmarkChanFanoutBaseline is the idiomatic lossy broadcast: one buffered channel per
// subscriber, non-blocking send, drop on full.
func BenchmarkChanFanoutBaseline(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			benchChanFanout(b, subs)
		})
	}
}

func benchChanFanout(b *testing.B, subs int) {
	chans := make([]chan uint64, subs)
	var (
		wg        sync.WaitGroup
		delivered atomic.Uint64
	)
	for i := range chans {
		ch := make(chan uint64, 4096)
		chans[i] = ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n uint64
			for range ch {
				n++
			}
			delivered.Add(n)
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		v := uint64(i)
		for _, ch := range chans {
			select {
			case ch <- v:
			default: // lossy: drop when this subscriber's buffer is full
			}
		}
	}
	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(delivered.Load())/b.Elapsed().Seconds(), "deliveries/s")
}
