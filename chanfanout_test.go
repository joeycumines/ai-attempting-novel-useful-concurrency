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
	"testing"
)

func TestChanFanoutBasic(t *testing.T) {
	// Capacity comfortably exceeds the number of sends, so no scheduling
	// delay can cause drops: every subscriber must see every value.
	b := NewChanFanout[int](4096)
	const subs = 8
	const sends = 100

	created := make([]*FanoutSub[int], subs)
	for i := range created {
		created[i] = b.Subscribe()
		defer created[i].Close()
	}

	var wg sync.WaitGroup
	for _, sub := range created {
		wg.Add(1)
		go func(sub *FanoutSub[int]) {
			defer wg.Done()
			for want := 1; want <= sends; want++ {
				value, skipped, ok := sub.Recv()
				if !ok {
					t.Errorf("Recv: ok == false at want %d", want)
					return
				}
				if skipped != 0 {
					t.Errorf("Recv: skipped == %d at want %d, want 0", skipped, want)
					return
				}
				if value != want {
					t.Errorf("Recv: value == %d, want %d", value, want)
					return
				}
			}
		}(sub)
	}

	for i := 1; i <= sends; i++ {
		if n := b.Send(i); n != subs {
			t.Fatalf("Send returned %d, want %d", n, subs)
		}
	}
	wg.Wait()
	if n := b.Subscribers(); n != subs {
		t.Fatalf("Subscribers == %d, want %d", n, subs)
	}
}

func TestChanFanoutSelect(t *testing.T) {
	b := NewChanFanout[string](16)
	sub := b.Subscribe()
	defer sub.Close()
	defer b.Close()

	go func() { b.Send("hello") }()

	select {
	case <-sub.Ready():
		value, skipped, ok := sub.TryRecv()
		if !ok || value != "hello" || skipped != 0 {
			t.Fatalf("TryRecv == (%q, %d, %v), want (%q, 0, true)", value, skipped, ok, "hello")
		}
	case <-b.Done():
		t.Fatal("hub closed unexpectedly")
	}

	// Nothing pending now: Ready must not fire.
	select {
	case <-sub.Ready():
		t.Fatal("Ready fired with no value published")
	default:
	}
}

func TestChanFanoutSlowSubscriberDrops(t *testing.T) {
	b := NewChanFanout[int](8)
	slow := b.Subscribe()
	defer slow.Close()
	fast := b.Subscribe()
	defer fast.Close()

	// Interleave send and fast-read in one goroutine: fast never lags,
	// slow never reads. Fully deterministic, no scheduling involved.
	for i := 1; i <= 100; i++ {
		b.Send(i)
		value, skipped, ok := fast.TryRecv()
		if !ok || value != i || skipped != 0 {
			t.Fatalf("fast TryRecv == (%d, %d, %v), want (%d, 0, true)", value, skipped, ok, i)
		}
	}

	// Ring capacity is 8, so the slot for seq 1 now holds seq 97
	// (the latest seq == 1 mod 8 that is <= 100). Slow must be resynced
	// to 97 with 96 skipped.
	value, skipped, ok := slow.TryRecv()
	if !ok || value != 97 || skipped != 96 {
		t.Fatalf("slow first TryRecv == (%d, %d, %v), want (97, 96, true)", value, skipped, ok)
	}
	for want := 98; want <= 100; want++ {
		value, skipped, ok = slow.TryRecv()
		if !ok || value != want || skipped != 0 {
			t.Fatalf("slow TryRecv == (%d, %d, %v), want (%d, 0, true)", value, skipped, ok, want)
		}
	}
	if _, _, ok := slow.TryRecv(); ok {
		t.Fatal("slow TryRecv after draining: ok == true, want false")
	}
}

func TestChanFanoutLagAndSkip(t *testing.T) {
	b := NewChanFanout[int](64)
	sub := b.Subscribe()
	defer sub.Close()

	for i := 1; i <= 10; i++ {
		b.Send(i)
	}
	if lag := sub.Lag(); lag != 10 {
		t.Fatalf("Lag == %d, want 10", lag)
	}
	if dropped := sub.SkipToLatest(); dropped != 10 {
		t.Fatalf("SkipToLatest == %d, want 10", dropped)
	}
	if lag := sub.Lag(); lag != 0 {
		t.Fatalf("Lag after skip == %d, want 0", lag)
	}
	b.Send(11)
	value, skipped, ok := sub.TryRecv()
	if !ok || value != 11 || skipped != 0 {
		t.Fatalf("TryRecv == (%d, %d, %v), want (11, 0, true)", value, skipped, ok)
	}
}

func TestChanFanoutClose(t *testing.T) {
	b := NewChanFanout[int](16)
	sub := b.Subscribe()
	defer sub.Close()

	for i := 1; i <= 5; i++ {
		b.Send(i)
	}
	b.Close()

	// Close drains: all five values remain deliverable.
	for want := 1; want <= 5; want++ {
		value, _, ok := sub.Recv()
		if !ok || value != want {
			t.Fatalf("drain Recv == (%d, %v), want (%d, true)", value, ok, want)
		}
	}
	if _, _, ok := sub.Recv(); ok {
		t.Fatal("Recv after drain: ok == true, want false")
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Send after Close did not panic")
			}
		}()
		b.Send(6)
	}()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Subscribe after Close did not panic")
			}
		}()
		b.Subscribe()
	}()

	// Idempotency.
	b.Close()
	sub.Close()
	sub.Close()
	sub.Unsubscribe()
}

func TestChanFanoutConcurrentChurn(t *testing.T) {
	b := NewChanFanout[int](8192)
	const sends = 2000

	// Readers subscribe in the main goroutine so they are guaranteed to
	// be registered before the hub is closed.
	const readers = 4
	readerSubs := make([]*FanoutSub[int], readers)
	for i := range readerSubs {
		readerSubs[i] = b.Subscribe()
	}

	var readerWg sync.WaitGroup
	for _, sub := range readerSubs {
		readerWg.Add(1)
		go func(sub *FanoutSub[int]) {
			defer sub.Close()
			defer readerWg.Done()
			last := 0
			for {
				value, _, ok := sub.Recv()
				if !ok {
					return
				}
				if value <= last {
					t.Errorf("values out of order: %d after %d", value, last)
					return
				}
				last = value
			}
		}(sub)
	}

	// Churn: short-lived subscribers hammering Subscribe/TryRecv/Close.
	sending := make(chan struct{})
	var churnWg sync.WaitGroup
	for i := 0; i < 2; i++ {
		churnWg.Add(1)
		go func() {
			defer churnWg.Done()
			for {
				select {
				case <-sending:
					return
				default:
				}
				sub := b.Subscribe()
				sub.TryRecv()
				sub.Close()
			}
		}()
	}

	for i := 1; i <= sends; i++ {
		b.Send(i)
	}
	close(sending)
	churnWg.Wait() // no Subscribe can race with Close below
	b.Close()
	readerWg.Wait()
}

func TestChanFanoutSubscribeContext(t *testing.T) {
	b := NewChanFanout[int](4096)
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	seq := b.SubscribeContext(ctx) // subscribes synchronously, in this goroutine

	var got []int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range seq {
			got = append(got, v)
			if len(got) == 10 {
				cancel()
			}
		}
	}()

	for i := 1; i <= 100; i++ {
		b.Send(i)
	}
	wg.Wait()

	if len(got) != 10 {
		t.Fatalf("received %d values, want 10", len(got))
	}
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("got[%d] == %d, want %d", i, v, i+1)
		}
	}
}

func BenchmarkChanFanoutSend(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	sub := fanout.Subscribe()
	defer sub.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fanout.Send(i)
	}
}

func BenchmarkChanFanoutSendNoSubscribers(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := fanout.Send(i); n != 0 {
			b.Fatalf("Send returned %d, want 0", n)
		}
	}
}

func BenchmarkChanFanoutSendAndTryRecv(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	sub := fanout.Subscribe()
	defer sub.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fanout.Send(i)
		if _, _, ok := sub.TryRecv(); !ok {
			b.Fatal("TryRecv failed")
		}
	}
}

func BenchmarkChanFanoutSubscribeClose(b *testing.B) {
	fanout := NewChanFanout[int](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub := fanout.Subscribe()
		sub.Close()
	}
}
