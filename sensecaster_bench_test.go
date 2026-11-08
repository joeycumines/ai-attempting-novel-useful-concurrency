package bigbuff

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// benchSink keeps receiver accumulators observable so the compiler cannot elide them.
var benchSink atomic.Int64

// linearCaster is the reference "linear" design: one unbuffered channel per subscriber,
// sent to in sequence. Select works, but every publish is O(subscribers) channel
// handoffs in series, and unsubscribing scans the list.
type linearCaster[V any] struct {
	mu    sync.Mutex
	chans []chan V
}

func (l *linearCaster[V]) subscribe() chan V {
	ch := make(chan V)
	l.mu.Lock()
	l.chans = append(l.chans, ch)
	l.mu.Unlock()
	return ch
}

func (l *linearCaster[V]) unsubscribe(ch chan V) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, c := range l.chans {
		if c == ch {
			last := len(l.chans) - 1
			l.chans[i] = l.chans[last]
			l.chans = l.chans[:last]
			return
		}
	}
}

func (l *linearCaster[V]) send(v V) {
	l.mu.Lock()
	chans := l.chans
	l.mu.Unlock()
	for _, ch := range chans {
		ch <- v
	}
}

func (l *linearCaster[V]) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.chans {
		close(ch)
	}
	l.chans = nil
}

var benchSubCounts = []int{1, 8, 64, 512}

// The receivers use the simplest receive each design offers: a single-case receive on
// Ready (for the caster) and a plain range (for the baseline). Shutdown is Close, which
// is part of the design under test. A multi-case select over a shared stop channel
// would measure selectgo's channel-lock contention rather than the caster.
func BenchmarkSenseCasterSend(b *testing.B) {
	for _, n := range benchSubCounts {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			c := NewSenseCaster[int]()
			subs := make([]SenseSubscription[int], n)
			for i := range subs {
				subs[i] = c.Subscribe()
			}
			var wg sync.WaitGroup
			for i := range subs {
				sub := subs[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					local := 0
					for {
						if v, ok := sub.TryRecv(); ok {
							local += v
							continue
						}
						if sub.Closed() {
							benchSink.Add(int64(local))
							return
						}
						<-sub.Ready()
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
			}
			b.StopTimer()
			for _, s := range subs {
				s.Close()
			}
			wg.Wait()
		})
	}
}

func BenchmarkLinearCasterSend(b *testing.B) {
	for _, n := range benchSubCounts {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			var l linearCaster[int]
			var wg sync.WaitGroup
			for range n {
				ch := l.subscribe()
				wg.Add(1)
				go func() {
					defer wg.Done()
					local := 0
					for v := range ch {
						local += v
					}
					benchSink.Add(int64(local))
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				l.send(i)
			}
			b.StopTimer()
			l.closeAll()
			wg.Wait()
		})
	}
}

// Churn: replace one standing subscriber per iteration, with the standing set held at n.
func BenchmarkSenseCasterChurn(b *testing.B) {
	for _, n := range []int{8, 512, 8192} {
		b.Run(fmt.Sprintf("standing=%d", n), func(b *testing.B) {
			c := NewSenseCaster[int]()
			standing := make([]SenseSubscription[int], n)
			for i := range standing {
				standing[i] = c.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				j := i % n
				standing[j].Close()
				standing[j] = c.Subscribe()
			}
			b.StopTimer()
			for _, s := range standing {
				s.Close()
			}
		})
	}
}

func BenchmarkLinearCasterChurn(b *testing.B) {
	for _, n := range []int{8, 512, 8192} {
		b.Run(fmt.Sprintf("standing=%d", n), func(b *testing.B) {
			var l linearCaster[int]
			standing := make([]chan int, n)
			for i := range standing {
				standing[i] = l.subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				j := i % n
				l.unsubscribe(standing[j])
				standing[j] = l.subscribe()
			}
			b.StopTimer()
			l.closeAll()
		})
	}
}
