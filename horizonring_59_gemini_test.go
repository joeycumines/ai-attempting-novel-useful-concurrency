// Copyright 2026 Joseph Cumines
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type telemetryFrame struct {
	ProducerID uint32
	SeqLocal   uint32
	Timestamp  int64
	MetricA    float64
	MetricB    uint64
	Checksum   uint64
}

func makeFrame(prodID, seqLocal uint32) telemetryFrame {
	ts := int64(prodID)*1_000_000 + int64(seqLocal)
	a := float64(seqLocal) * 1.5
	b := uint64(prodID)<<32 | uint64(seqLocal)
	cs := uint64(prodID) ^ uint64(seqLocal) ^ uint64(ts) ^ b ^ 0xfeedfacecafebeef
	return telemetryFrame{
		ProducerID: prodID,
		SeqLocal:   seqLocal,
		Timestamp:  ts,
		MetricA:    a,
		MetricB:    b,
		Checksum:   cs,
	}
}

func verifyFrame(t *testing.T, f telemetryFrame) {
	t.Helper()
	expected := makeFrame(f.ProducerID, f.SeqLocal)
	if f != expected {
		t.Fatalf("torn or corrupted telemetryFrame: got %+v, want %+v", f, expected)
	}
}

func TestHorizonRing_PointerDetection(t *testing.T) {
	rVal := NewHorizonRing59Gemini[telemetryFrame](16)
	if !rVal.IsPointerFree() {
		t.Fatal("expected telemetryFrame to be detected as pointer-free")
	}

	type withPtr struct {
		Name string
		ID   int
	}
	rPtr := NewHorizonRing59Gemini[withPtr](16)
	if rPtr.IsPointerFree() {
		t.Fatal("expected struct containing string to be detected as pointer-containing")
	}
}

func TestHorizonRing_ExactSequenceConservationAndOverrun(t *testing.T) {
	const ringCap = 16
	const totalItems = 200
	r := NewHorizonRing59Gemini[uint64](ringCap)
	cur := r.NewCursor59Gemini(StartEarliest59Gemini)

	for i := uint64(1); i <= totalItems; i++ {
		seq, ok := r.Publish(i * 10)
		if !ok || seq != i {
			t.Fatalf("Publish(%d) = (%d, %v)", i, seq, ok)
		}
	}

	// Cursor59Gemini started at 1, but 200 items were written into a capacity-16 ring.
	// Oldest valid sequence must be 200 - 16 + 1 = 185, so 184 items were dropped.
	val, seq, dropped, ok := cur.TryRead()
	if !ok {
		t.Fatal("expected TryRead to succeed")
	}
	if dropped != 184 || seq != 185 || val != 1850 {
		t.Fatalf("got (val=%d, seq=%d, dropped=%d), want (1850, 185, 184)", val, seq, dropped)
	}

	// Drain remaining 15 items via ReadBatch and verify strict contiguity.
	batch := make([]uint64, 32)
	n, firstSeq, batchDropped := cur.ReadBatch(batch)
	if n != 15 || firstSeq != 186 || batchDropped != 0 {
		t.Fatalf("ReadBatch got (n=%d, firstSeq=%d, dropped=%d), want (15, 186, 0)", n, firstSeq, batchDropped)
	}
	for i := 0; i < n; i++ {
		wantSeq := firstSeq + uint64(i)
		if batch[i] != wantSeq*10 {
			t.Fatalf("batch[%d] = %d, want %d", i, batch[i], wantSeq*10)
		}
	}
}

func TestHorizonRing_StaleProducerElision(t *testing.T) {
	r := NewHorizonRing59Gemini[uint64](8)
	// Publish 16 items so slot[1] holds sequence 9 (raw seq 18).
	for i := uint64(1); i <= 16; i++ {
		r.Publish(i * 100)
	}

	// Simulate a preempted producer from sequence 1 waking up late and attempting
	// to write stale sequence 1 into slot[1].
	wrote := slotWrite59Gemini(&r.slots[1&r.mask], 1, 999999, r.IsPointerFree())
	if wrote {
		t.Fatal("expected stale producer write for seq=1 to be elided when slot holds seq=9")
	}

	// Verify slot[1] still holds sequence 9 intact.
	cur := r.NewCursorAt(9)
	val, seq, dropped, ok := cur.TryRead()
	if !ok || seq != 9 || dropped != 0 || val != 900 {
		t.Fatalf("corrupted slot after stale producer elision: val=%d, seq=%d, dropped=%d, ok=%v", val, seq, dropped, ok)
	}
}

func TestHorizonRing_ReadLatest(t *testing.T) {
	r := NewHorizonRing59Gemini[uint64](64)
	cur := r.NewCursor59Gemini(StartEarliest59Gemini)

	for i := uint64(1); i <= 50; i++ {
		r.Publish(i)
	}

	val, seq, skipped, ok := cur.ReadLatest()
	if !ok || val != 50 || seq != 50 || skipped != 49 {
		t.Fatalf("ReadLatest = (val=%d, seq=%d, skipped=%d, ok=%v), want (50, 50, 49, true)", val, seq, skipped, ok)
	}

	// Second ReadLatest with no new publications must return ok=false.
	if _, _, _, ok := cur.ReadLatest(); ok {
		t.Fatal("expected ReadLatest to return false when caught up")
	}
}

func TestHorizonRing_AdversarialMPMCNoTornReads(t *testing.T) {
	const (
		numProducers = 16
		msgsPerProd  = 4000
		numConsumers = 12
		ringCap      = 64 // Intentionally small to force aggressive wrap-around overwrites
	)

	r := NewHorizonRing59Gemini[telemetryFrame](ringCap)
	var prodWG sync.WaitGroup
	var consWG sync.WaitGroup
	doneProducing := make(chan struct{})

	// Launch heterogeneous consumers: TryRead pollers, ReadBatch drainers, and ReadLatest watchers.
	for cID := 0; cID < numConsumers; cID++ {
		consWG.Add(1)
		go func(id int) {
			defer consWG.Done()
			cur := r.NewCursor59Gemini(StartEarliest59Gemini)
			if id%3 == 2 {
				cur.SetPolicy(CursorPolicyWaitFreeSkip59Gemini)
			}
			// StartEarliest positions the cursor at the oldest retained
			// sequence at creation time, so the cursor may begin well past
			// sequence 1 once the ring has wrapped; take the actual start.
			expectedNext := cur.NextSeq()
			batch := make([]telemetryFrame, 16)

			for {
				// The read APIs report how many sequences were skipped even
				// when the call returns no value (the cursor has already moved
				// past those sequences), so the accounting below adds the skip
				// count on EVERY call. The delivered test only consulted it on
				// the value path, which leaks the skip accounting and fails a
				// spuriously large gap against an un-accumulated expectation.
				skippedSum := uint64(0)
				switch id % 3 {
				case 0:
					f, seq, dropped, ok := cur.TryRead()
					expectedNext += dropped
					skippedSum += dropped
					if ok {
						verifyFrame(t, f)
						if seq != expectedNext {
							t.Errorf("consumer %d sequence conservation violated: seq=%d, expectedNext=%d, dropped=%d",
								id, seq, expectedNext, dropped)
							return
						}
						expectedNext = seq + 1
						continue
					}
				case 1:
					n, firstSeq, dropped := cur.ReadBatch(batch)
					expectedNext += dropped
					skippedSum += dropped
					if n > 0 {
						if firstSeq != expectedNext {
							t.Errorf("batch consumer %d sequence conservation violated: firstSeq=%d, expected=%d, dropped=%d",
								id, firstSeq, expectedNext, dropped)
							return
						}
						for i := 0; i < n; i++ {
							verifyFrame(t, batch[i])
						}
						expectedNext = firstSeq + uint64(n)
						continue
					}
				case 2:
					f, seq, skipped, ok := cur.ReadLatest()
					expectedNext += skipped
					skippedSum += skipped
					if ok {
						verifyFrame(t, f)
						if seq != expectedNext {
							t.Errorf("latest consumer %d sequence conservation violated: seq=%d, expected=%d, skipped=%d",
								id, seq, expectedNext, skipped)
							return
						}
						expectedNext = seq + 1
						continue
					}
				}

				if skippedSum > 0 {
					// This call advanced the cursor without delivering
					// (overwritten window); keep draining rather than exiting.
					runtime.Gosched()
					continue
				}
				select {
				case <-doneProducing:
					// Nothing readable, no skip accounting pending, and
					// producers are done (they cannot be mid-write), so the
					// cursor is caught up. Exit without probing — a probe
					// would consume a frame without verifying or accounting
					// for it.
					return
				default:
					runtime.Gosched()
				}
			}
		}(cID)
	}

	for pID := 0; pID < numProducers; pID++ {
		prodWG.Add(1)
		go func(id uint32) {
			defer prodWG.Done()
			for m := uint32(1); m <= msgsPerProd; m++ {
				r.Publish(makeFrame(id, m))
			}
		}(uint32(pID + 1))
	}

	prodWG.Wait()
	close(doneProducing)
	consWG.Wait()
}

func TestHorizonRing_PointerContainingTypeConcurrency(t *testing.T) {
	type event struct {
		Topic   string
		Payload []byte
		SeqNum  uint64
	}

	r := NewHorizonRing59Gemini[event](32)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for c := 0; c < 6; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cur := r.NewCursor59Gemini(StartEarliest59Gemini)
			for seq, ev := range cur.Seq(ctx) {
				wantTopic := fmt.Sprintf("topic-%d", ev.SeqNum%7)
				if ev.Topic != wantTopic || len(ev.Payload) != 8 || ev.Payload[0] != byte(ev.SeqNum) || seq == 0 {
					t.Errorf("corrupted pointer-containing event at seq %d: %+v", seq, ev)
					return
				}
			}
		}()
	}

	for i := uint64(1); i <= 2000; i++ {
		r.Publish(event{
			Topic:   fmt.Sprintf("topic-%d", i%7),
			Payload: []byte{byte(i), 1, 2, 3, 4, 5, 6, 7},
			SeqNum:  i,
		})
	}
	r.Close()
	wg.Wait()
}

func TestHorizonRing_HighFanoutSelectAndCleanShutdown(t *testing.T) {
	const numWaiters = 256
	r := NewHorizonRing59Gemini[int](1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var readyWG sync.WaitGroup
	var doneWG sync.WaitGroup
	var receivedTotal atomic.Int64

	for i := 0; i < numWaiters; i++ {
		readyWG.Add(1)
		doneWG.Add(1)
		go func() {
			defer doneWG.Done()
			cur := r.NewCursor59Gemini(StartEarliest59Gemini)
			readyWG.Done()

			for {
				v, _, _, err := cur.ReadContext(ctx)
				if errors.Is(err, ErrClosed59Gemini) {
					return
				}
				if err != nil {
					t.Errorf("unexpected ReadContext error: %v", err)
					return
				}
				receivedTotal.Add(int64(v))
			}
		}()
	}

	readyWG.Wait()
	// Note: the ring's hasWaiters is a 0/1 epoch flag (close-and-recreate
	// channel broadcast), not a per-waiter counter, so there is no shared
	// state to poll for "all waiters parked". The sleep this replaced was
	// not needed for correctness: every cursor starts at the retained
	// oldest (ring capacity 1024 > 50 items), so however the 256 waiters
	// interleave they must each observe every value; Close wakes anyone
	// parked. The exact-total assertion below is the deterministic check.

	const items = 50
	expectedPerConsumer := 0
	for i := 1; i <= items; i++ {
		expectedPerConsumer += i
		r.Publish(i)
	}
	r.Close()
	doneWG.Wait()

	wantTotal := int64(expectedPerConsumer * numWaiters)
	if got := receivedTotal.Load(); got != wantTotal {
		t.Fatalf("high-fanout total mismatch: got %d, want %d", got, wantTotal)
	}
}

func TestStripedCombiner_ExactSequenceAssignment(t *testing.T) {
	const (
		producers   = 32
		perProducer = 500
		total       = producers * perProducer
	)

	r := NewHorizonRing59Gemini[uint64](total * 2)
	comb := NewStripedCombiner59Gemini(r, 64)

	seenSeqs := make([]atomic.Uint32, total+1)
	var wg sync.WaitGroup

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				val := uint64(id*100000 + i + 1)
				seq, ok := comb.Submit(val)
				if !ok || seq < 1 || seq > total {
					t.Errorf("Submit returned invalid (seq=%d, ok=%v)", seq, ok)
					return
				}
				if seenSeqs[seq].Add(1) != 1 {
					t.Errorf("duplicate sequence number %d assigned by StripedCombiner59Gemini", seq)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Verify every sequence in [1..total] was assigned exactly once and readable from ring.
	cur := r.NewCursor59Gemini(StartEarliest59Gemini)
	for expected := uint64(1); expected <= total; expected++ {
		if seenSeqs[expected].Load() != 1 {
			t.Fatalf("sequence %d assigned %d times, want 1", expected, seenSeqs[expected].Load())
		}
		v, seq, dropped, ok := cur.TryRead()
		if !ok || seq != expected || dropped != 0 || v == 0 {
			t.Fatalf("ring read mismatch at %d: v=%d, seq=%d, dropped=%d, ok=%v", expected, v, seq, dropped, ok)
		}
	}
}

// ============================================================================
// Adversarial Benchmarks: HorizonRing59Gemini vs. Standard Go Primitives
// ============================================================================

func BenchmarkPublish_SingleProducer(b *testing.B) {
	r := NewHorizonRing59Gemini[uint64](4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Publish(uint64(i))
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mmsgs/s")
}

func BenchmarkPublishBatch_64(b *testing.B) {
	r := NewHorizonRing59Gemini[uint64](4096)
	batch := make([]uint64, 64)
	for i := range batch {
		batch[i] = uint64(i + 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.PublishBatch(batch)
	}
	totalMsgs := float64(b.N) * 64
	b.ReportMetric(totalMsgs/b.Elapsed().Seconds()/1e6, "Mmsgs/s")
}

func Benchmark1P1C_HorizonRing59Gemini_ReadBatch(b *testing.B) {
	r := NewHorizonRing59Gemini[uint64](8192)
	cur := r.NewCursor59Gemini(StartLatest59Gemini)
	pubBatch := make([]uint64, 64)
	readBuf := make([]uint64, 64)
	for i := range pubBatch {
		pubBatch[i] = uint64(i + 1)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.PublishBatch(pubBatch)
		n, _, _ := cur.ReadBatch(readBuf)
		if n != 64 {
			b.Fatalf("expected 64 items, got %d", n)
		}
	}
	totalMsgs := float64(b.N) * 64
	b.ReportMetric(totalMsgs/b.Elapsed().Seconds()/1e6, "Mmsgs/s")
}

func Benchmark1P1C_StdBufferedChan(b *testing.B) {
	ch := make(chan uint64, 8192)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < 64; k++ {
			ch <- uint64(k)
		}
		for k := 0; k < 64; k++ {
			<-ch
		}
	}
	totalMsgs := float64(b.N) * 64
	b.ReportMetric(totalMsgs/b.Elapsed().Seconds()/1e6, "Mmsgs/s")
}

func BenchmarkFanout_1P_64C_HorizonRing59Gemini(b *testing.B) {
	const consumers = 64
	r := NewHorizonRing59Gemini[uint64](4096)
	cursors := make([]*Cursor59Gemini[uint64], consumers)
	for i := range cursors {
		cursors[i] = r.NewCursor59Gemini(StartLatest59Gemini)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func(c *Cursor59Gemini[uint64]) {
			defer wg.Done()
			<-start
			buf := make([]uint64, 64)
			for !r.IsClosed() {
				if n, _, _ := c.ReadBatch(buf); n == 0 {
					runtime.Gosched()
				}
			}
		}(cursors[i])
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	for i := 0; i < b.N; i++ {
		r.Publish(uint64(i))
	}
	r.Close()
	wg.Wait()
	b.ReportMetric(float64(b.N*consumers)/b.Elapsed().Seconds()/1e6, "Mdeliveries/s")
}

func BenchmarkFanout_1P_64C_MutexChanSliceBaseline(b *testing.B) {
	const consumers = 64
	var mu sync.RWMutex
	chans := make([]chan uint64, consumers)
	var wg sync.WaitGroup
	done := make(chan struct{})

	for i := 0; i < consumers; i++ {
		chans[i] = make(chan uint64, 256)
		wg.Add(1)
		go func(ch <-chan uint64) {
			defer wg.Done()
			for {
				select {
				case <-ch:
				case <-done:
					return
				}
			}
		}(chans[i])
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := uint64(i)
		mu.RLock()
		for _, ch := range chans {
			select {
			case ch <- v:
			default:
			}
		}
		mu.RUnlock()
	}
	close(done)
	wg.Wait()
	b.ReportMetric(float64(b.N*consumers)/b.Elapsed().Seconds()/1e6, "Mdeliveries/s")
}

func BenchmarkHighContentionMPMC_StripedCombiner(b *testing.B) {
	r := NewHorizonRing59Gemini[uint64](16384)
	comb := NewStripedCombiner59Gemini(r, 64)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		v := uint64(1)
		for pb.Next() {
			comb.Submit(v)
			v++
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mmsgs/s")
}

func BenchmarkHighContentionMPMC_StdChanBaseline(b *testing.B) {
	ch := make(chan uint64, 16384)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
			case <-done:
				return
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		v := uint64(1)
		for pb.Next() {
			select {
			case ch <- v:
			default:
				<-ch
				ch <- v
			}
			v++
		}
	})
	b.StopTimer()
	close(done)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mmsgs/s")
}
