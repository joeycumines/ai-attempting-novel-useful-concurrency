package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// linearFanout is the reference "linear" design: membership is a mutex-guarded
// slice, and Send performs one channel send per subscriber, in a loop. Each
// subscriber has its own buffered channel, so the sender is not a rendezvous.
type linearFanout[V any] struct {
	mu    sync.RWMutex
	slack int
	subs  []*linearSub[V]
}

type linearSub[V any] struct {
	ch   chan V
	done chan struct{}
}

func (f *linearFanout[V]) subscribe() *linearSub[V] {
	s := &linearSub[V]{ch: make(chan V, f.slack), done: make(chan struct{})}
	f.mu.Lock()
	f.subs = append(f.subs, s)
	f.mu.Unlock()
	return s
}

func (f *linearFanout[V]) unsubscribe(s *linearSub[V]) {
	close(s.done) // release a sender blocked on s.ch before taking the write lock
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, x := range f.subs {
		if x == s {
			f.subs = append(f.subs[:i], f.subs[i+1:]...)
			return
		}
	}
}

func (f *linearFanout[V]) send(v V) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, s := range f.subs {
		select {
		case s.ch <- v:
		case <-s.done:
		}
	}
	return len(f.subs)
}

var benchFanoutSubs = []int{1, 8, 64}

// BenchmarkFanout measures end-to-end delivery: one sender, n consumers that
// each receive every value. Consumers run concurrently with the sender.
func BenchmarkFanout(b *testing.B) {
	for _, n := range benchFanoutSubs {
		b.Run(fmt.Sprintf("carillon/subs=%d", n), func(b *testing.B) {
			c := NewCarillon[int](64)
			subs := make([]CarillonSubscription[int], n)
			for i := range subs {
				subs[i] = c.Subscribe()
			}
			var wg sync.WaitGroup
			for _, s := range subs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						if _, err := s.Recv(context.Background()); err != nil {
							panic(err)
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
			}
			wg.Wait()
		})

		// ChanCaster is registration-per-message: Send clears the receiver count
		// when it completes, so every receiver must Add(1) before each message.
		// The sender therefore waits for n registrations (ready) before each
		// Send. This handshake is part of ChanCaster's cost in this benchmark.
		b.Run(fmt.Sprintf("chancaster/subs=%d", n), func(b *testing.B) {
			c := NewChanCaster(make(chan int))
			ready := make(chan struct{}, n)
			var wg sync.WaitGroup
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						c.Add(1)
						ready <- struct{}{}
						<-c.C
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				for range n {
					<-ready
				}
				c.Send(i)
			}
			wg.Wait()
		})

		b.Run(fmt.Sprintf("chanpubsub/subs=%d", n), func(b *testing.B) {
			ps := NewChanPubSub(make(chan int))
			for range n {
				ps.Subscribe()
			}
			ch := ps.C()
			var wg sync.WaitGroup
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						<-ch
						ps.Wait()
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				ps.Send(i)
			}
			wg.Wait()
		})

		b.Run(fmt.Sprintf("linear/subs=%d", n), func(b *testing.B) {
			f := &linearFanout[int]{slack: 64}
			var wg sync.WaitGroup
			for range n {
				s := f.subscribe()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range b.N {
						<-s.ch
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				f.send(i)
			}
			wg.Wait()
		})
	}
}

// BenchmarkSendTryRecv measures the single-goroutine cost of a send plus one
// non-blocking receive per subscriber. It isolates per-subscriber overhead on
// the busy path, where no subscriber is parked.
func BenchmarkSendTryRecv(b *testing.B) {
	for _, n := range benchFanoutSubs {
		b.Run(fmt.Sprintf("carillon/subs=%d", n), func(b *testing.B) {
			c := NewCarillon[int](64)
			subs := make([]CarillonSubscription[int], n)
			for i := range subs {
				subs[i] = c.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
				for _, s := range subs {
					s.TryRecv()
				}
			}
		})

		b.Run(fmt.Sprintf("linear/subs=%d", n), func(b *testing.B) {
			f := &linearFanout[int]{slack: 64}
			subs := make([]*linearSub[int], n)
			for i := range subs {
				subs[i] = f.subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				f.send(i)
				for _, s := range subs {
					<-s.ch
				}
			}
		})
	}
}

// BenchmarkChurn measures a subscribe followed by an unsubscribe, in parallel
// with a continuously running sender. This is where a shared membership lock
// shows its cost.
func BenchmarkChurn(b *testing.B) {
	b.Run("carillon", func(b *testing.B) {
		c := NewCarillon[int](64)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				c.Send(i)
			}
		}()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s := c.Subscribe()
				s.Unsubscribe()
			}
		})
		close(stop)
		wg.Wait()
	})

	b.Run("linear", func(b *testing.B) {
		f := &linearFanout[int]{slack: 64}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				f.send(i)
			}
		}()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				f.unsubscribe(f.subscribe())
			}
		})
		close(stop)
		wg.Wait()
	})

	b.Run("chancaster", func(b *testing.B) {
		c := NewChanCaster(make(chan int))
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				c.Send(i)
			}
		}()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
				c.Add(-1)
			}
		})
		close(stop)
		wg.Wait()
	})
}
