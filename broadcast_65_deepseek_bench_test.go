package bigbuff

import (
	"context"
	"sync/atomic"
	"testing"
)

// BenchmarkBroadcaster_Publish_NoConsumers measures the pure single-writer
// publish cost when nobody is subscribed.
func BenchmarkBroadcaster_Publish_NoConsumers(b *testing.B) {
	bc := NewBroadcaster65Deepseek[uint64](1024)
	defer bc.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bc.Publish(uint64(i))
	}
}

// BenchmarkBroadcaster_Publish_WithConsumers measures publish cost with N
// consumers registered but idle (all caught up, none waiting).
func BenchmarkBroadcaster_Publish_WithConsumers(b *testing.B) {
	for _, n := range []int{1, 4, 16} {
		b.Run(benchName65Deepseek(n), func(b *testing.B) {
			bc := NewBroadcaster65Deepseek[uint64](1024)
			defer bc.Close()
			var subs []*Subscription65Deepseek[uint64]
			for i := 0; i < n; i++ {
				subs = append(subs, bc.Subscribe(context.Background()))
			}
			defer func() {
				for _, s := range subs {
					s.Close()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = bc.Publish(uint64(i))
			}
		})
	}
}

func benchName65Deepseek(n int) string {
	switch n {
	case 1:
		return "1_consumer"
	case 4:
		return "4_consumers"
	case 16:
		return "16_consumers"
	}
	return "n_consumers"
}

// BenchmarkBroadcaster_Read measured the consumer hot-path cost (TryNext).
func BenchmarkBroadcaster_Read(b *testing.B) {
	bc := NewBroadcaster65Deepseek[uint64](1024)
	defer bc.Close()
	sub := bc.Subscribe(context.Background())
	defer sub.Close()

	// Pre-fill so reads are always "caught up".
	for i := 0; i < 1024; i++ {
		_, _ = bc.Publish(uint64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink uint64
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			_, _ = bc.Publish(uint64(i))
		}
		v, r := sub.TryNext()
		if r == ReadOK65Deepseek {
			sink += v
		}
	}
	_ = sink
}

// BenchmarkChanFanout_Publish is the idiomatic baseline: a producer has one
// buffered channel per consumer and does a non-blocking send to each.
func BenchmarkChanFanout_Publish(b *testing.B) {
	for _, n := range []int{1, 4, 16} {
		b.Run(benchName65Deepseek(n), func(b *testing.B) {
			chans := make([]chan uint64, n)
			for i := range chans {
				chans[i] = make(chan uint64, 1)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v := uint64(i)
				for _, c := range chans {
					select {
					case c <- v:
					default:
					}
				}
			}
		})
	}
}

// BenchmarkBroadcaster_ProducerConsumer pairs a producer and a caught-up
// consumer in tight loops to measure round-trip cost.
func BenchmarkBroadcaster_ProducerConsumer(b *testing.B) {
	bc := NewBroadcaster65Deepseek[uint64](1024)
	defer bc.Close()
	sub := bc.Subscribe(context.Background())
	defer sub.Close()

	var stop atomic.Bool
	var read uint64

	go func() {
		for !stop.Load() {
			if _, r := sub.TryNext(); r == ReadOK65Deepseek {
				atomic.AddUint64(&read, 1)
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bc.Publish(uint64(i))
	}
	stop.Store(true)
	b.StopTimer()
	_ = atomic.LoadUint64(&read)
}

// BenchmarkChanFanout_Consumer measures the per-consumer receive cost of the
// baseline.
func BenchmarkChanFanout_Consumer(b *testing.B) {
	ch := make(chan uint64, 1024)
	var sink uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- uint64(i)
		select {
		case v := <-ch:
			sink += v
		default:
		}
	}
	_ = sink
}
