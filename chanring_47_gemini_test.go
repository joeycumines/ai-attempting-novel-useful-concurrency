// Provenance: 47-arena-gemini-3.8-flash.md (Gemini, response #47). Namespaced to preserve the existing implementations.
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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChanCaster_ConcurrentChurnAndSelect(t *testing.T) {
	ch := make(chan int)
	caster := NewChanCaster47Gemini(ch)

	const workers = 64
	const rounds = 200

	var received atomic.Int64
	var cancelled atomic.Int64
	var wg sync.WaitGroup

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := range rounds {
				caster.Add(1)
				if (id+r)%5 == 0 {
					// Simulate select timeout / cancellation mid-broadcast.
					if caster.Add(-1) < 0 {
						t.Errorf("negative receivers")
					}
					cancelled.Add(1)
					continue
				}
				v := <-caster.C
				received.Add(int64(v))
			}
		}(w)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			if caster.Len() != 0 {
				t.Fatalf("expected 0 remaining receivers, got %d", caster.Len())
			}
			return
		default:
			caster.Send(1)
		}
	}
}

func TestChanPubSub_ScalableBroadcastAndContextCancel(t *testing.T) {
	ch := make(chan int)
	ps := NewChanPubSub47Gemini(ch)

	const subscribers = 128
	const messages = 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ready sync.WaitGroup
	var done sync.WaitGroup
	var total atomic.Int64

	for i := range subscribers {
		ready.Add(1)
		done.Add(1)
		go func(id int) {
			defer done.Done()
			subCtx, subCancel := context.WithCancel(ctx)
			defer subCancel()

			seq := ps.SubscribeContext(subCtx)
			ready.Done()

			count := 0
			for v := range seq {
				total.Add(int64(v))
				count++
				if id%4 == 0 && count == 25 {
					subCancel()
				}
			}
		}(i)
	}

	ready.Wait()

	for m := 1; m <= messages; m++ {
		ps.Send(1)
	}

	cancel()
	done.Wait()

	if ps.Subscribers() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", ps.Subscribers())
	}
	if total.Load() == 0 {
		t.Fatal("expected messages to be received")
	}
}

func TestChanRing_PipelinedSelectAndBackpressure(t *testing.T) {
	ring := NewChanRing47Gemini[int](16)
	defer ring.Close()

	const subscribers = 32
	const messages = 250

	var wg sync.WaitGroup
	var ready sync.WaitGroup

	for s := range subscribers {
		wg.Add(1)
		ready.Add(1)
		go func(id int) {
			defer wg.Done()
			sub := ring.Subscribe()
			defer sub.Close()
			ready.Done()

			expected := 1
			for expected <= messages {
				select {
				case <-sub.Ready():
					for {
						v, ok := sub.TryRecv()
						if !ok {
							break
						}
						if v != expected {
							t.Errorf("sub %d: got %d, want %d", id, v, expected)
							return
						}
						expected++
					}
				case <-time.After(5 * time.Second):
					t.Errorf("sub %d: timed out at %d", id, expected)
					return
				}
			}
		}(s)
	}

	ready.Wait()

	for m := 1; m <= messages; m++ {
		if sent := ring.Send(m); sent != subscribers {
			t.Fatalf("msg %d: sent=%d, want=%d", m, sent, subscribers)
		}
	}

	wg.Wait()

	if ring.Subscribers() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", ring.Subscribers())
	}
}

func BenchmarkChanPubSub_Broadcast128(b *testing.B) {
	ch := make(chan int)
	ps := NewChanPubSub47Gemini(ch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const subs = 128
	var ready sync.WaitGroup
	var done sync.WaitGroup
	for range subs {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			seq := ps.SubscribeContext(ctx)
			ready.Done()
			for range seq {
			}
		}()
	}
	ready.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ps.Send(i)
	}
	b.StopTimer()
	cancel()
	done.Wait()
}

func BenchmarkChanRing_Broadcast128(b *testing.B) {
	ring := NewChanRing47Gemini[int](64)
	const subs = 128
	var ready sync.WaitGroup
	var done sync.WaitGroup
	for range subs {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			sub := ring.Subscribe()
			defer sub.Close()
			ready.Done()
			for !sub.Closed() {
				<-sub.Ready()
				for {
					if _, ok := sub.TryRecv(); !ok {
						break
					}
				}
			}
		}()
	}
	ready.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		ring.Send(i)
	}
	b.StopTimer()
	ring.Close()
	done.Wait()
}
