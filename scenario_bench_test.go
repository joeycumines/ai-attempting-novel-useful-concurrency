package bigbuff

import (
	"context"
	"testing"
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

func BenchmarkScenarioCaster(b *testing.B) {
	for _, a := range casterAttachments {
		b.Run(a.Impl, func(b *testing.B) {
			if a.ExposureLimited {
				b.Skip("exposure-limited attachment has no safe representative multi-receiver benchmark")
			}
			mk := casterLikes()[a.Impl]
			x := mk()
			const receivers = 8
			stop := make(chan struct{})
			done := make(chan struct{}, receivers)
			for r := 0; r < receivers; r++ {
				go func() {
					defer func() { done <- struct{}{} }()
					if !x.rearmEach {
						x.add(1)
					}
					for {
						if x.rearmEach {
							x.add(1)
						}
						select {
						case <-x.c:
							if x.ack != nil {
								x.ack()
							}
						case <-stop:
							x.add(-1)
							return
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for v := 1; v <= b.N; v++ {
				x.send(v)
			}
			b.StopTimer()
			close(stop)
			for r := 0; r < receivers; r++ {
				<-done
			}
		})
	}
}

func BenchmarkScenarioScatterGather(b *testing.B) {
	for _, a := range scatterAttachments {
		b.Run("scatter/"+a.Impl, func(b *testing.B) {
			var ch chan int
			var send func(int) int
			var add func(int) int
			switch a.Impl {
			case "ChanScatter":
				x := NewChanScatter(make(chan int))
				ch, send, add = x.C, x.Send, x.Add
			default:
				x := NewBaselineScatterQueue[int]()
				ch, send, add = x.C, x.Send, x.Add
			}
			const receivers = 8
			stop := make(chan struct{})
			done := make(chan struct{}, receivers)
			for r := 0; r < receivers; r++ {
				go func() {
					defer func() { done <- struct{}{} }()
					for {
						add(1)
						select {
						case <-ch:
						case <-stop:
							add(-1)
							return
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for v := 1; v <= b.N; v++ {
				send(v)
			}
			b.StopTimer()
			close(stop)
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
