/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

import (
	"runtime"
	"sync"
	"testing"
)

// The benchmarks in this file are deliberately apples-to-apples: every
// implementation broadcasts the same int to the same number of live,
// continuously consuming subscribers, from a single publisher, and every
// subscriber receives (synchronously - i.e. window 0) with no buffering. That
// is the strongest form of fan-out: it is the mode in which the publisher must
// synchronise with every subscriber.
//
// The four baselines are:
//
//   - CallbackNoop: a slice of no-op funcs - the floor for any fan-out.
//   - CallbackChan: a slice of funcs, each of which hands the value to a
//     per-consumer channel (i.e. the "callback channel" design).
//   - ChanCaster: the linear sequence of sends into one unbuffered channel.
//   - ChanPubSub: ChanCaster plus its ping/pong (Wait) protocol.

var benchmarkSizes = []int{1, 8, 64, 512}

func benchmarkSizeName(n int) string {
	switch n {
	case 1:
		return "1"
	case 8:
		return "8"
	case 64:
		return "64"
	default:
		return "512"
	}
}

// ---------------------------------------------------------------- baselines --

type benchmarkCallback func(int)

func BenchmarkCallbackNoop(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			handlers := make([]benchmarkCallback, subscribers)
			var sink int
			for i := range handlers {
				handlers[i] = func(v int) { sink = v }
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, handler := range handlers {
					handler(i)
				}
			}
			_ = sink
		})
	}
}

func BenchmarkCallbackChan(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			handlers := make([]benchmarkCallback, subscribers)
			stop := make([]chan struct{}, subscribers)
			for i := range handlers {
				channel := make(chan int)
				stop[i] = make(chan struct{})
				go func() {
					for {
						select {
						case <-channel:
						case <-stop[i]:
							return
						}
					}
				}()
				handlers[i] = func(v int) { channel <- v }
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, handler := range handlers {
					handler(i)
				}
			}
			b.StopTimer()
			for i := range stop {
				close(stop[i])
			}
		})
	}
}

func BenchmarkChanCaster(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			channel := make(chan int)
			caster := NewChanCaster(channel)
			for range subscribers {
				go func() {
					for range channel {
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			// N.B. ChanCaster is a one-shot broadcast primitive: Send consumes
			// the armed receivers, so each broadcast must re-arm them.
			for i := 0; i < b.N; i++ {
				caster.Add(subscribers)
				if n := caster.Send(i); n != subscribers {
					b.Fatalf("expected %d receivers, got %d", subscribers, n)
				}
			}
			b.StopTimer()
			close(channel)
		})
	}
}

func BenchmarkChanPubSub(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			channel := make(chan int)
			pubsub := NewChanPubSub(channel)
			for range subscribers {
				go func() {
					pubsub.Add(1)
					for range pubsub.C() {
						pubsub.Wait()
					}
				}()
			}
			for pubsub.Add(0) != subscribers {
				runtime.Gosched()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pubsub.Send(i)
			}
			b.StopTimer()
			close(channel)
		})
	}
}

// ---------------------------------------------------------------- ChanRelay --

func benchmarkChanRelaySend(b *testing.B, subscribers, window int) {
	relay := NewChanRelay[int](WithRelayWindow(window))
	subs := make([]*RelaySubscription[int], subscribers)
	for i := range subs {
		sub := relay.Subscribe()
		subs[i] = sub
		go func() {
			for {
				select {
				case <-sub.C():
				case <-sub.Done():
					return
				}
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relay.Send(i)
	}
	b.StopTimer()
	for _, sub := range subs {
		sub.Close()
	}
	relay.Close()
}

// BenchmarkChanRelay_Send measures synchronous broadcast (window 0).
func BenchmarkChanRelay_Send(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			benchmarkChanRelaySend(b, subscribers, 0)
		})
	}
}

// BenchmarkChanRelay_SendBuffered measures broadcast to subscriptions with a
// window of 16 (i.e. decoupled from the subscribers).
func BenchmarkChanRelay_SendBuffered(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			benchmarkChanRelaySend(b, subscribers, 16)
		})
	}
}

// BenchmarkChanRelay_TrySend measures non-blocking broadcast to synchronous
// subscriptions, which are all parked in a receive (the best case for latency).
func BenchmarkChanRelay_TrySend(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			relay := NewChanRelay[int]()
			subs := make([]*RelaySubscription[int], subscribers)
			for i := range subs {
				sub := relay.Subscribe()
				subs[i] = sub
				go func() {
					for {
						select {
						case <-sub.C():
						case <-sub.Done():
							return
						}
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				relay.TrySend(i)
			}
			b.StopTimer()
			for _, sub := range subs {
				sub.Close()
			}
			relay.Close()
		})
	}
}

// BenchmarkChanRelay_Churn measures parallel subscription churn (subscribe then
// close), against a fixed population of live subscribers.
func BenchmarkChanRelay_Churn(b *testing.B) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	for range 64 {
		sub := relay.Subscribe()
		go func() {
			for {
				select {
				case <-sub.C():
				case <-sub.Done():
					return
				}
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			relay.Subscribe().Close()
		}
	})
	b.StopTimer()
	relay.Close()
}

// BenchmarkChanPubSub_Churn is the equivalent subscription churn for
// ChanPubSub, whose Add(-1) must synchronise with in-flight sends.
func BenchmarkChanPubSub_Churn(b *testing.B) {
	channel := make(chan int)
	pubsub := NewChanPubSub(channel)
	for range 64 {
		go func() {
			pubsub.Add(1)
			for range pubsub.C() {
				pubsub.Wait()
			}
		}()
	}
	for pubsub.Add(0) != 64 {
		runtime.Gosched()
	}
	var publisher sync.WaitGroup
	stop := make(chan struct{})
	publisher.Add(1)
	go func() {
		defer publisher.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pubsub.Send(0)
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pubsub.Add(1)
			pubsub.Add(-1)
		}
	})
	b.StopTimer()
	close(stop)
	publisher.Wait()
	close(channel)
}

// BenchmarkChanRelay_Subscribe measures the cost of registering a subscription
// (including its transport), and BenchmarkChanRelay_SubscribeClose the cost of
// registering and detaching one, i.e. the churn cost with no publisher.
func BenchmarkChanRelay_SubscribeClose(b *testing.B) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relay.Subscribe().Close()
	}
	b.StopTimer()
	relay.Close()
}

// BenchmarkChanRelay_RegistryCompaction measures the amortised cost of
// subscription churn once the registry is large enough to require compaction.
func BenchmarkChanRelay_RegistryCompaction(b *testing.B) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	// A large live population, so that compaction is O(1024) per trigger.
	live := make([]*RelaySubscription[int], 1024)
	for i := range live {
		live[i] = relay.Subscribe()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relay.Subscribe().Close()
	}
	b.StopTimer()
	for _, sub := range live {
		sub.Close()
	}
	relay.Close()
}

// BenchmarkChanRelay_TrySendSkipped measures pure dispatch overhead: a broadcast
// to synchronous subscriptions that are not receiving, so that no value is
// transferred and no goroutine is woken. It is the floor for the relay's
// per-subscriber cost, and the closest analogue of BenchmarkCallbackNoop.
func BenchmarkChanRelay_TrySendSkipped(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			relay := NewChanRelay[int]()
			subs := make([]*RelaySubscription[int], subscribers)
			for i := range subs {
				subs[i] = relay.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				relay.TrySend(i)
			}
			b.StopTimer()
			for _, sub := range subs {
				sub.Close()
			}
			relay.Close()
		})
	}
}

// BenchmarkChanRelay_TrySendBuffered measures a broadcast that is absorbed by
// subscription buffers (no subscriber is woken during the measured loop, beyond
// the first window of values), i.e. the cost of queueing a value per subscriber.
func BenchmarkChanRelay_TrySendBuffered(b *testing.B) {
	for _, subscribers := range benchmarkSizes {
		b.Run(benchmarkSizeName(subscribers), func(b *testing.B) {
			relay := NewChanRelay[int](WithRelayWindow(1024))
			subs := make([]*RelaySubscription[int], subscribers)
			for i := range subs {
				subs[i] = relay.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				relay.TrySend(i)
			}
			b.StopTimer()
			for _, sub := range subs {
				sub.Close()
			}
			relay.Close()
		})
	}
}
