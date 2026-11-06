package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkUnified_Broadcast benchmarks direct publication throughput
// across subscriber counts (1, 4, 16, 64, 256) for all implementations.
func BenchmarkUnified_Broadcast(b *testing.B) {
	subscriberCounts := []int{1, 4, 16, 64, 256}

	for _, subCount := range subscriberCounts {
		b.Run(fmt.Sprintf("subs=%d", subCount), func(b *testing.B) {
			for _, factory := range AllBroadcasters() {
				broadcaster := factory()
				b.Run(broadcaster.Name(), func(b *testing.B) {
					subs := make([]BroadcastSubscriber, subCount)
					for i := 0; i < subCount; i++ {
						subs[i] = broadcaster.Subscribe()
					}
					defer func() {
						for _, s := range subs {
							s.Unsubscribe()
						}
					}()

					var stop atomic.Bool
					var wg sync.WaitGroup
					wg.Add(subCount)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					for i := 0; i < subCount; i++ {
						go func(s BroadcastSubscriber) {
							defer wg.Done()
							for !stop.Load() {
								s.Receive(ctx)
							}
						}(subs[i])
					}

					// Warmup
					broadcaster.Publish(0)
					time.Sleep(1 * time.Millisecond)

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						broadcaster.Publish(i + 1)
					}

					b.StopTimer()
					stop.Store(true)
					cancel()
					wg.Wait()
				})
			}
		})
	}
}

// BenchmarkUnified_Churn measures the cost of creating and tearing down
// subscriptions under each architecture.
func BenchmarkUnified_Churn(b *testing.B) {
	for _, factory := range AllBroadcasters() {
		broadcaster := factory()
		b.Run(broadcaster.Name(), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				s := broadcaster.Subscribe()
				s.Unsubscribe()
			}
		})
	}
}

// BenchmarkUnified_Select measures select statement throughput
func BenchmarkUnified_Select(b *testing.B) {
	for _, factory := range AllBroadcasters() {
		broadcaster := factory()
		b.Run(broadcaster.Name(), func(b *testing.B) {
			sub := broadcaster.Subscribe()
			defer sub.Unsubscribe()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				go broadcaster.Publish(i + 1)
				sub.Receive(ctx)
			}
		})
	}
}
