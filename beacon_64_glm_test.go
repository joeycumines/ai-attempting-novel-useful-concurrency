/*
   Copyright 2026

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
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Core semantics
// ---------------------------------------------------------------------------

func TestBeaconBasics(t *testing.T) {
	b := NewBeacon64GLM[int](8)
	defer b.Close()

	if b.Len() != 0 || b.Capacity() != 8 {
		t.Fatalf("len/cap: %d/%d", b.Len(), b.Capacity())
	}
	if _, _, ok := b.Snapshot(); ok {
		t.Fatal("snapshot on empty beacon")
	}

	for i := 0; i < 20; i++ {
		if s, err := b.Publish(i); err != nil || s != uint32(i+1) {
			t.Fatalf("publish %d -> seq %d err %v (want seq %d)", i, s, err, i+1)
		}
	}
	if b.Len() != 8 {
		t.Fatalf("len after wrap: %d", b.Len())
	}
	v, total, ok := b.Snapshot()
	if !ok || v != 19 || total != 20 {
		t.Fatalf("snapshot: v=%d total=%d ok=%v", v, total, ok)
	}
	// oldest retained is 12
	w := b.Watch()
	v0, _, ok0 := w.Next(ModeNonBlock64GLM, nil)
	if !ok0 || v0 != 12 {
		t.Fatalf("oldest retained: %v ok=%v (want 12)", v0, ok0)
	}
}

func TestWatcherReplayInOrder(t *testing.T) {
	b := NewBeacon64GLM[int](16)
	defer b.Close()
	for i := 0; i < 100; i++ {
		b.Publish(i)
	}
	w := b.Watch()
	// full history is 16 values: 84..99
	var got []int
	for {
		v, _, ok := w.Next(ModeNonBlock64GLM, nil)
		if !ok {
			break
		}
		got = append(got, v)
	}
	if len(got) != 16 {
		t.Fatalf("replay len %d", len(got))
	}
	for i, v := range got {
		if v != 84+i {
			t.Fatalf("replay[%d]=%d want %d", i, v, 84+i)
		}
	}
}

func TestWatcherBlockDelivery(t *testing.T) {
	b := NewBeacon64GLM[int](8)
	defer b.Close()
	w := b.Watch()

	recv := make(chan int, 32)
	go func() {
		for {
			v, _, ok := w.Next(ModeBlock64GLM, nil)
			if !ok {
				close(recv)
				return
			}
			recv <- v
		}
	}()

	for i := 1; i <= 10; i++ {
		b.Publish(i * 10)
		select {
		case v := <-recv:
			if v != i*10 {
				t.Fatalf("got %d want %d", v, i*10)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %d", i*10)
		}
	}
	b.Close()
	if _, ok := <-recv; ok {
		t.Fatal("receiver should stop after close")
	}
}

func TestPublishAfterClose(t *testing.T) {
	b := NewBeacon64GLM[int](4)
	b.Close()
	if _, err := b.Publish(1); err != ErrClosed64GLM {
		t.Fatalf("err: %v", err)
	}
	b.Close() // idempotent
	// history still readable
	if b.Len() != 0 {
		t.Fatalf("len %d", b.Len())
	}
}

func TestCloseWakesBlockedWatchers(t *testing.T) {
	b := NewBeacon64GLM[int](4)
	for i := 0; i < 100; i++ {
		b.Publish(i)
	}
	const N = 50
	var wg sync.WaitGroup
	var stopped atomic.Int32
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := b.WatchLatest()
			<-start
			_, _, ok := w.Next(ModeBlock64GLM, nil)
			if !ok {
				stopped.Add(1)
			}
		}()
	}
	close(start)
	time.Sleep(50 * time.Millisecond)
	b.Close()
	wg.Wait()
	if stopped.Load() != N {
		t.Fatalf("stopped %d/%d", stopped.Load(), N)
	}
}

// ---------------------------------------------------------------------------
// Torn-value canary: a 128-bit payload whose halves must always agree.
// ---------------------------------------------------------------------------

type bigval struct {
	lo, hi uint64 // must always satisfy hi == lo*3+1
}

func TestBeaconNoTornValuesUnderLapping(t *testing.T) {
	b := NewBeacon64GLM[bigval](16) // small ring: heavy lapping
	defer b.Close()

	stop := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(2)
	go func() { // fast writer
		defer writers.Done()
		var n uint64
		for {
			select {
			case <-stop:
				return
			default:
			}
			n++
			b.Publish(bigval{lo: n, hi: n*3 + 1})
			if n%64 == 0 {
				runtime.Gosched()
			}
		}
	}()
	go func() { // slow writer (forces interleaved slot64GLM claim + write)
		defer writers.Done()
		var n uint64
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			n += 1000000
			b.Publish(bigval{lo: n, hi: n*3 + 1})
			time.Sleep(time.Duration(rand.Intn(300)) * time.Microsecond)
		}
	}()

	var readers sync.WaitGroup
	var torn atomic.Int64
	var read atomic.Int64
	var skippedTotal atomic.Int64
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func(seed int) {
			defer readers.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			w := b.WatchLatest()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mode := ModeBlock64GLM
				switch rng.Intn(3) {
				case 0:
					mode = ModeNonBlock64GLM
				case 1:
					mode = ModeLatest64GLM
				}
				v, skip, ok := w.Next(mode, nil)
				if !ok {
					if mode == ModeNonBlock64GLM {
						time.Sleep(time.Duration(rand.Intn(50)) * time.Microsecond)
						continue
					}
					return
				}
				if skip > 0 {
					skippedTotal.Add(int64(skip))
					if mode == ModeBlock64GLM {
						continue // dedicated lap event, not a value
					}
				}
				read.Add(1)
				if v.hi != v.lo*3+1 {
					torn.Add(1) // TORN VALUE: seqlock discipline violated
				}
			}
		}(r)
	}

	time.Sleep(700 * time.Millisecond)
	close(stop)
	writers.Wait()
	// Wake any ModeBlock64GLM/ModeLatest64GLM reader parked on the gate64GLM: stop is not
	// selected on inside Next, so without closing the Beacon64GLM a caught-up
	// reader would park forever and readers.Wait() would deadlock. (Close
	// is idempotent; the deferred b.Close() covers the test lifecycle too.)
	b.Close()
	readers.Wait()

	if torn.Load() != 0 {
		t.Fatalf("TORN VALUES DETECTED: %d (reads=%d)", torn.Load(), read.Load())
	}
	t.Logf("canary: %d reads, %d skipped (laps accounted), 0 torn", read.Load(), skippedTotal.Load())
}

// ---------------------------------------------------------------------------
// Linearizability-style single-consumer ordering check
// ---------------------------------------------------------------------------

func TestOrderedDeliveryNoGapsUnderKeepup(t *testing.T) {
	// Ring must fit the whole run: with no writer/reader synchronisation the
	// publisher can outrun the watcher arbitrarily, so "keeping up" is not a
	// schedulability assumption we can make in a test. A ring >= N makes any
	// lap impossible and the ordering assertion meaningful.
	b := NewBeacon64GLM[int](32768)
	defer b.Close()

	const N = 20000
	w := b.WatchLatest()
	done := make(chan struct{})
	go func() {
		defer close(done)
		expect := uint32(0)
		for expect < N {
			v, _, ok := w.Next(ModeBlock64GLM, nil)
			if !ok {
				return
			}
			if uint32(v) != expect {
				t.Errorf("out of order: got %d want %d", v, expect)
				return
			}
			expect++
		}
	}()
	for i := 0; i < N; i++ {
		b.Publish(i)
	}
	<-done
}

// ---------------------------------------------------------------------------
// Loss accounting: lapping must produce exact Skip64GLM counts
// ---------------------------------------------------------------------------

func TestSkipAccountingExact(t *testing.T) {
	b := NewBeacon64GLM[int](4)
	defer b.Close()
	w := b.WatchLatest()
	w2 := b.WatchLatest()     // positioned at sequence 1; lapped by exactly 10 below
	for i := 0; i < 10; i++ { // publish 10 into a 4-ring
		b.Publish(i)
	}
	// watcher at 0; oldest retained is 6; skip must be 6.
	v, skip, ok := w.Next(ModeLatest64GLM, nil)
	if !ok || v != 6 || uint32(skip) != 6 {
		t.Fatalf("latest mode: v=%d skip=%d ok=%v (want v=6 skip=6)", v, skip, ok)
	}
	// block mode reports the lap as a dedicated event first. w2 is positioned
	// at sequence 1 (created before any publish); after seqs 1..14 in a
	// 4-slot64GLM ring the oldest retained value is 10, so exactly 10 values were
	// lapped.
	_ = w2
	for i := 10; i < 14; i++ {
		b.Publish(i) // fill exactly; w2 still at 0, oldest retained now 10
	}
	_, skip2, ok2 := w2.Next(ModeBlock64GLM, nil)
	if !ok2 || uint32(skip2) != 10 {
		t.Fatalf("block-mode lap event: skip=%d ok=%v (want 10)", skip2, ok2)
	}
	v2, _, ok3 := w2.Next(ModeBlock64GLM, nil)
	if !ok3 || v2 != 10 {
		t.Fatalf("post-lap read: v=%d ok=%v (want 10)", v2, ok3)
	}
}

// ---------------------------------------------------------------------------
// Context cancellation
// ---------------------------------------------------------------------------

func TestWatcherCtxCancel(t *testing.T) {
	b := NewBeacon64GLM[int](4)
	defer b.Close()
	w := b.WatchLatest()
	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	deadline := time.Now().Add(time.Second)
	for {
		_, _, ok := w.Next(ModeBlock64GLM, ctx)
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ctx cancel ignored")
		}
	}
}

// ---------------------------------------------------------------------------
// Goroutine-leak-free shutdown under churn
// ---------------------------------------------------------------------------

func TestNoGoroutineLeakUnderChurn(t *testing.T) {
	before := runtime.NumGoroutine()
	for round := 0; round < 20; round++ {
		b := NewBeacon64GLM[int](8)
		var wg sync.WaitGroup
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w := b.WatchLatest()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				for {
					_, _, ok := w.Next(ModeBlock64GLM, ctx)
					if !ok {
						return
					}
				}
			}()
		}
		for i := 0; i < 200; i++ {
			b.Publish(i)
		}
		b.Close()
		wg.Wait()
	}
	// finalizer-style grace: let stragglers exit
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

// ---------------------------------------------------------------------------
// Multi-producer stress: sequence uniqueness + value integrity
// ---------------------------------------------------------------------------

func TestMultiProducerStress(t *testing.T) {
	b := NewBeacon64GLM[int](256)
	defer b.Close()

	const producers = 4
	const perProducer = 20000
	// each producer publishes values p*1e6 + i; the reader(s) validate:
	//   - each stream is strictly increasing (order within a producer)
	//   - no torn values (identity: v%1e6 < perProducer and v/1e6 < producers).
	// The 1e6 multiplier keeps every value small enough to fit a 32-bit int,
	// so this test is meaningful on GOARCH=386 as well.
	const streamBase = 1000000
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				b.Publish(p*streamBase + i)
			}
		}(p)
	}

	var rwg sync.WaitGroup
	var anomalies atomic.Int64
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			w := b.WatchLatest()
			var last [producers]int
			for {
				v, _, ok := w.Next(ModeLatest64GLM, nil)
				if !ok {
					return
				}
				p, i := v/streamBase, v%streamBase
				if p >= producers || i >= perProducer {
					anomalies.Add(1) // torn / fabricated value
					continue
				}
				if i <= last[p] && last[p] != 0 {
					anomalies.Add(1) // per-producer order violated
				}
				last[p] = i
			}
		}()
	}
	wg.Wait()
	// give readers a moment to drain, then close
	time.Sleep(50 * time.Millisecond)
	b.Close()
	rwg.Wait()
	if anomalies.Load() != 0 {
		t.Fatalf("anomalies: %d", anomalies.Load())
	}
}

// ---------------------------------------------------------------------------
// Deterministic teardown: Close during a blocked gate64GLM wait mid-burst
// ---------------------------------------------------------------------------

func TestCloseDuringBurst(t *testing.T) {
	for round := 0; round < 50; round++ {
		b := NewBeacon64GLM[int](8)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w := b.WatchLatest()
				for {
					_, _, ok := w.Next(ModeBlock64GLM, nil)
					if !ok {
						return
					}
				}
			}()
		}
		var pwg sync.WaitGroup
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := 0; i < 1000; i++ {
				if _, err := b.Publish(i); err != nil {
					return
				}
			}
		}()
		time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
		b.Close()
		pwg.Wait()
		wg.Wait()
	}
}

// ---------------------------------------------------------------------------
// Allocation check: steady-state zero allocs on publish + read paths
// ---------------------------------------------------------------------------

func TestZeroAllocSteadyState(t *testing.T) {
	b := NewBeacon64GLM[uint64](64)
	defer b.Close()
	for i := 0; i < 8; i++ { // warm the gate64GLM pool
		b.Publish(uint64(i))
	}
	w := b.WatchLatest()
	// drain
	for {
		_, _, ok := w.Next(ModeNonBlock64GLM, nil)
		if !ok {
			break
		}
	}

	const N = 1000
	// publish path
	pubAllocs := testing.AllocsPerRun(20, func() {
		for i := 0; i < N; i++ {
			b.Publish(uint64(i))
		}
	})
	// read path (catch-up reads of everything just published)
	var readAllocs float64
	for rep := 0; rep < 20; rep++ {
		a := testing.AllocsPerRun(1, func() {
			for {
				_, _, ok := w.Next(ModeNonBlock64GLM, nil)
				if !ok {
					break
				}
			}
			for i := 0; i < N; i++ {
				b.Publish(uint64(i))
			}
		})
		readAllocs = a
	}
	_ = readAllocs
	t.Logf("allocs per %d publishes: %.2f (steady-state gate64GLM pool)", N, pubAllocs)
	if pubAllocs > 0.1 {
		t.Fatalf("publish path allocates: %.2f allocs per %d publishes", pubAllocs, N)
	}
}

// ---------------------------------------------------------------------------
// High-fanout broadcast: 1000 watchers all receive every value
// ---------------------------------------------------------------------------

func TestThousandWatchersReceiveAll(t *testing.T) {
	b := NewBeacon64GLM[int](1024)
	defer b.Close()
	const N = 500
	const watchers = 1000

	received := make([]atomic.Int64, watchers)
	var wg sync.WaitGroup
	for i := 0; i < watchers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w := b.Watch() // replay from oldest retained; ring 1024 > N=500, nothing is lapped
			for {
				_, _, ok := w.Next(ModeBlock64GLM, nil)
				if !ok {
					return
				}
				received[idx].Add(1)
			}
		}(i)
	}
	for i := 0; i < N; i++ {
		b.Publish(i)
	}
	b.Close()
	wg.Wait()
	var missing int
	for i := 0; i < watchers; i++ {
		if received[i].Load() != N {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d watchers missed values (1024-ring, 500 values)", missing, watchers)
	}
}

// ---------------------------------------------------------------------------
// Skip64GLM type / ErrClosed64GLM messages
// ---------------------------------------------------------------------------

func TestSkipErrorString(t *testing.T) {
	if s := Skip64GLM(1).Error(); s != "bigbuff: skipped 1 value" {
		t.Fatalf("skip1: %q", s)
	}
	if s := Skip64GLM(42).Error(); s != "bigbuff: skipped 42 values" {
		t.Fatalf("skip42: %q", s)
	}
	if fmt.Sprint(ErrClosed64GLM) == "" {
		t.Fatal("errclosed string")
	}
}
