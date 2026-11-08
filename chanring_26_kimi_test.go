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

func waitForChanRing(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// Every live subscriber must observe every value, in order, when the ring
// comfortably out-lives the burst.
func TestChanRingFanout(t *testing.T) {
	x := NewChanRing26Kimi[int](4096, 64)
	const subs, msgs = 16, 500
	ctx, cancel := context.WithCancel(context.Background())
	var got [subs]atomic.Int32
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var want int
			for v := range x.SubscribeContext(ctx) {
				if v != want {
					t.Errorf("subscriber %d: got %d, want %d", i, v, want)
					return
				}
				want++
				got[i].Add(1)
			}
		}(i)
	}
	waitForChanRing(t, "all subscribers", func() bool { return x.Len() == subs })
	for v := 0; v < msgs; v++ {
		if sent := x.Send(v); sent != subs {
			t.Fatalf("Send(%d) signaled %d subscribers, want %d", v, sent, subs)
		}
	}
	waitForChanRing(t, "all messages at all subscribers", func() bool {
		for i := range got {
			if got[i].Load() != msgs {
				return false
			}
		}
		return true
	})
	cancel()
	wg.Wait()
	if n := x.Len(); n != 0 {
		t.Fatalf("Len() = %d after cancel, want 0", n)
	}
}

// With no iteration running while 100 values are sent into a 4-slot ring,
// only the retained tail (the last 4 positions) may be observed, exactly.
func TestChanRingBoundedLoss(t *testing.T) {
	x := NewChanRing26Kimi[int](4, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq := x.SubscribeContext(ctx) // subscribes eagerly; iterator not started
	const msgs = 100
	for v := 0; v < msgs; v++ {
		x.Send(v)
	}
	var received []int
	for v := range seq {
		received = append(received, v)
		if v == msgs-1 {
			cancel()
		}
	}
	want := []int{msgs - 4, msgs - 3, msgs - 2, msgs - 1}
	if len(received) != len(want) {
		t.Fatalf("received %v, want retained tail %v", received, want)
	}
	for i := range want {
		if received[i] != want[i] {
			t.Fatalf("received %v, want retained tail %v", received, want)
		}
	}
}

// Concurrent subscribe/iterate/cancel churn against a concurrent sender must
// be race-free and return the pool to empty. Run with -race.
func TestChanRingChurn(t *testing.T) {
	x := NewChanRing26Kimi[int](64, 128)
	stop := make(chan struct{})
	var wg, subWg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				x.Send(0)
			}
		}
	}()
	for i := 0; i < 32; i++ {
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			for j := 0; j < 20; j++ {
				c, ccancel := context.WithCancel(context.Background())
				func() {
					defer ccancel()
					n := 0
					for range x.SubscribeContext(c) {
						if n++; n >= 3 {
							ccancel()
						}
					}
				}()
			}
		}()
	}
	subWg.Wait()
	close(stop)
	wg.Wait()
	waitForChanRing(t, "subscriber pool to drain", func() bool { return x.Len() == 0 })
}

// Subscribing past the fixed pool size must panic (and leave state intact).
func TestChanRingExhaustion(t *testing.T) {
	x := NewChanRing26Kimi[int](1, 1)
	_ = x.SubscribeContext(context.Background())
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on exceeding max subscribers")
		}
	}()
	_ = x.SubscribeContext(context.Background())
}

// A new subscription starts at the live tail: values published before
// subscribing are not replayed, and the first post-subscription value is
// delivered exactly once.
func TestChanRingLiveTail(t *testing.T) {
	x := NewChanRing26Kimi[int](16, 4)
	for v := 0; v < 10; v++ {
		x.Send(v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Register eagerly: the iterator is obtained before the post-subscription
	// publication, so the live-tail contract is what is under test and not the
	// goroutine's start-up latency.
	seq := x.SubscribeContext(ctx)
	var count atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range seq {
			count.Add(1)
		}
	}()
	x.Send(10)
	waitForChanRing(t, "single live-tail value", func() bool { return count.Load() == 1 })
	cancel()
	<-done
}
