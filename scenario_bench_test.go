package bigbuff

import (
	"context"
	"testing"
	"time"
)

// BenchmarkScenarioBroadcast: per scenario/attachment publish throughput with
// 8 draining subscribers. ns/op covers Publish only; subscribers drain via
// the adapter. Exposure-limited attachments keep volume bounded by their
// documented limits (subscribers drain continuously).
func BenchmarkScenarioBroadcast(b *testing.B) {
	for _, sc := range Scenarios {
		if sc.Kind != KindBroadcast {
			continue
		}
		for _, a := range attachmentsFor(sc.ID, broadcastAttachments) {
			b.Run(string(sc.ID)+"/"+a.Impl, func(b *testing.B) {
				factory := scenarioBroadcastFactories()[a.Impl]
				br := factory()
				defer br.Close()
				const subs = 8
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make([]chan struct{}, subs)
				for i := 0; i < subs; i++ {
					s := br.Subscribe()
					defer s.Unsubscribe()
					d := make(chan struct{})
					done[i] = d
					go func(s BroadcastSubscriber) {
						defer close(d)
						for {
							if _, ok := s.Receive(ctx); !ok {
								return
							}
						}
					}(s)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for v := 1; v <= b.N; v++ {
					br.Publish(v)
				}
				b.StopTimer()
				cancel()
				for _, d := range done {
					<-d
				}
			})
		}
	}
}

// BenchmarkScenarioCaster times a Send that delivers to 8 registered
// receivers. A caster delivers only to receivers that are registered when Send
// is called, and each receiver registers again for every value, so a sender
// that loops without waiting finds nobody registered and times an empty call
// (see casterRounds). Each iteration here waits for the receivers and then
// sends:
//
//	ns/op          the whole round: waiting for the receivers, then Send
//	send-ns/op     Send alone, from call to return
//	delivered/op   deliveries per Send; the benchmark fails unless it is 8
func BenchmarkScenarioCaster(b *testing.B) {
	for _, a := range casterAttachments {
		b.Run(a.Impl, func(b *testing.B) {
			if a.ExposureLimited {
				b.Skip("exposure-limited attachment has no safe representative multi-receiver benchmark")
			}
			const receivers = 8
			rounds := startCasterRounds(casterLikes()[a.Impl](), receivers)
			if !rounds.arm() {
				b.Fatal("the receivers did not register")
			}
			var sending time.Duration
			delivered := 0
			b.ReportAllocs()
			b.ResetTimer()
			for v := 1; v <= b.N; v++ {
				d, k, ok := rounds.send(v)
				if !ok {
					b.Fatalf("send %d: a Send reported a delivery that no receiver took", v)
				}
				sending += d
				delivered += k
			}
			b.StopTimer()
			if !rounds.arm() || !rounds.finish() {
				b.Fatal("the receivers did not finish")
			}
			if got := rounds.received.Load(); delivered != receivers*b.N || got != int64(delivered) {
				b.Fatalf("%d Sends reported %d deliveries and the receivers took %d; want %d", b.N, delivered, got, receivers*b.N)
			}
			b.ReportMetric(float64(sending.Nanoseconds())/float64(b.N), "send-ns/op")
			b.ReportMetric(float64(delivered)/float64(b.N), "delivered/op")
		})
	}
}

func BenchmarkScenarioScatterGather(b *testing.B) {
	for _, a := range scatterAttachments {
		if a.Scenario != "work-distribution-scatter" || !a.ExpectedGoodFit {
			// Bounded lossy queues are measured by BenchmarkBoundedScatterAdmission
			// with exact eviction accounting, not by a lossless dispatch loop that
			// could silently overflow and report favourable invalid throughput.
			continue
		}
		b.Run("scatter/"+a.Impl, func(b *testing.B) {
			x := scatterLikes()[a.Impl]()
			const receivers = 8
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{}, receivers)
			ready := make(chan struct{}, receivers)
			for r := 0; r < receivers; r++ {
				go func() {
					defer func() { done <- struct{}{} }()
					x.add(1)
					ready <- struct{}{}
					defer x.add(-1)
					for {
						if _, ok := x.wait(ctx); !ok {
							return
						}
					}
				}()
			}
			for r := 0; r < receivers; r++ {
				<-ready
			}
			b.ReportAllocs()
			b.ResetTimer()
			for v := 1; v <= b.N; v++ {
				x.send(v)
			}
			b.StopTimer()
			cancel()
			for r := 0; r < receivers; r++ {
				<-done
			}
		})
	}
	for _, a := range gatherAttachments {
		b.Run("gather/"+a.Impl, func(b *testing.B) {
			x := gatherLikes()[a.Impl]()
			const senders = 8
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for s := 0; s < senders; s++ {
					x.add(1)
				}
				done := make(chan struct{}, senders)
				for s := 0; s < senders; s++ {
					go func() { x.send(); done <- struct{}{} }()
				}
				x.wait()
				for s := 0; s < senders; s++ {
					<-done
				}
			}
			b.StopTimer()
		})
	}
}
