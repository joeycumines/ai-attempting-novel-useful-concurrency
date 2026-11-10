package bigbuff

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHorizon66GeminiSwizzleBijection verifies that physIndex is a strict 1-to-1 bijection
// over [0, cap-1] and that consecutive sequence numbers map to separated slots.
func TestHorizon66GeminiSwizzleBijection(t *testing.T) {
	for _, capReq := range []int{8, 16, 32, 64, 256, 1024, 4096} {
		h := NewHorizon66Gemini[uint64](capReq)
		c := uint64(h.Capacity())
		seen := make([]bool, c)
		for seq := uint64(0); seq < c; seq++ {
			p := h.physIndex(seq)
			if p >= c {
				t.Fatalf("cap=%d: physIndex(%d)=%d out of range", c, seq, p)
			}
			if seen[p] {
				t.Fatalf("cap=%d: duplicate physIndex(%d)=%d", c, seq, p)
			}
			seen[p] = true
			if c >= 16 && seq > 0 {
				prev := h.physIndex(seq - 1)
				if prev == p || prev+1 == p || p+1 == prev {
					t.Fatalf("cap=%d: consecutive sequences %d and %d mapped to adjacent slots (%d, %d)",
						c, seq-1, seq, prev, p)
				}
			}
		}
	}
}

// TestHorizon66GeminiBasicPublishReadAndOverrun verifies exact sequential reads, overrun detection,
// and catch-up recovery policies.
func TestHorizon66GeminiBasicPublishReadAndOverrun(t *testing.T) {
	h := NewHorizon66Gemini[uint64](8)
	if h.Capacity() != 8 {
		t.Fatalf("expected capacity 8, got %d", h.Capacity())
	}

	if _, _, ok := h.Latest(); ok {
		t.Fatal("expected Latest() on empty Horizon66Gemini to return ok=false")
	}

	// Publish 8 items (filling the ring window [1..8]).
	for i := uint64(1); i <= 8; i++ {
		seq, ok := h.Publish(i * 10)
		if !ok || seq != i {
			t.Fatalf("Publish(%d) = (%d, %v), want (%d, true)", i, seq, ok, i)
		}
	}

	for i := uint64(1); i <= 8; i++ {
		v, st := h.TryRead(i)
		if st != StatusOK66Gemini || v != i*10 {
			t.Fatalf("TryRead(%d) = (%d, %v), want (%d, StatusOK66Gemini)", i, v, st, i*10)
		}
	}
	if _, st := h.TryRead(9); st != StatusNotReady66Gemini {
		t.Fatalf("TryRead(9) = %v, want StatusNotReady66Gemini", st)
	}

	// Publish 4 more items [9..12], overwriting [1..4].
	for i := uint64(9); i <= 12; i++ {
		h.Publish(i * 10)
	}

	for i := uint64(1); i <= 4; i++ {
		if _, st := h.TryRead(i); st != StatusOverrun66Gemini {
			t.Fatalf("TryRead(%d) = %v, want StatusOverrun66Gemini", i, st)
		}
	}
	for i := uint64(5); i <= 12; i++ {
		v, st := h.TryRead(i)
		if st != StatusOK66Gemini || v != i*10 {
			t.Fatalf("TryRead(%d) = (%d, %v), want (%d, StatusOK66Gemini)", i, v, st, i*10)
		}
	}

	// Verify ReadBatch fast-forward on overrun.
	batch := make([]uint64, 16)
	n, nextSeq, overrun := h.ReadBatch(1, batch)
	if !overrun || n != 8 || nextSeq != 13 {
		t.Fatalf("ReadBatch(1) = (count=%d, nextSeq=%d, overrun=%v), want (8, 13, true)", n, nextSeq, overrun)
	}
	for i := 0; i < 8; i++ {
		want := uint64(i+5) * 10
		if batch[i] != want {
			t.Fatalf("batch[%d] = %d, want %d", i, batch[i], want)
		}
	}

	// Verify Latest().
	latestVal, latestSeq, ok := h.Latest()
	if !ok || latestSeq != 12 || latestVal != 120 {
		t.Fatalf("Latest() = (%d, %d, %v), want (120, 12, true)", latestVal, latestSeq, ok)
	}
}

// TestHorizon66GeminiCatchUpPolicies verifies CatchUpSkipToOldest66Gemini, CatchUpSkipToLatest66Gemini, and CatchUpFailOnOverrun66Gemini.
func TestHorizon66GeminiCatchUpPolicies(t *testing.T) {
	h := NewHorizon66Gemini[int](8)
	for i := 1; i <= 20; i++ {
		h.Publish(i)
	}
	h.Close()

	ctx := context.Background()

	// 1. CatchUpFailOnOverrun66Gemini
	cur := uint64(1)
	if _, _, st := h.ReadNext(ctx, &cur, CatchUpFailOnOverrun66Gemini); st != StatusOverrun66Gemini {
		t.Fatalf("expected StatusOverrun66Gemini, got %v", st)
	}

	// 2. CatchUpSkipToOldest66Gemini should start at sequence 13 (20 - 8 + 1) and drain 13..20.
	var collected []int
	for seq, val := range h.Subscribe(ctx, 1, CatchUpSkipToOldest66Gemini) {
		if int(seq) != val {
			t.Fatalf("seq %d != val %d", seq, val)
		}
		collected = append(collected, val)
	}
	if len(collected) != 8 || collected[0] != 13 || collected[7] != 20 {
		t.Fatalf("CatchUpSkipToOldest66Gemini collected %v, want [13..20]", collected)
	}

	// 3. CatchUpSkipToLatest66Gemini should jump straight to sequence 20.
	cur = 1
	val, seq, st := h.ReadNext(ctx, &cur, CatchUpSkipToLatest66Gemini)
	if st != StatusOK66Gemini || seq != 20 || val != 20 || cur != 21 {
		t.Fatalf("CatchUpSkipToLatest66Gemini = (val=%d, seq=%d, st=%v, cur=%d), want (20, 20, StatusOK66Gemini, 21)",
			val, seq, st, cur)
	}
	if _, _, st := h.ReadNext(ctx, &cur, CatchUpSkipToLatest66Gemini); st != StatusClosed66Gemini {
		t.Fatalf("expected StatusClosed66Gemini at sequence 21, got %v", st)
	}
}

// TestHorizon66GeminiMPMCLinearizabilityAndZeroLoss verifies that under heavy MPMC contention
// when ring capacity is sufficient, every consumer receives 100% of items in exact sequence order.
func TestHorizon66GeminiMPMCLinearizabilityAndZeroLoss(t *testing.T) {
	const (
		numProducers = 8
		itemsPerProd = 1000
		totalItems   = numProducers * itemsPerProd
		numConsumers = 8
	)

	h := NewHorizon66Gemini[uint64](totalItems * 2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var consumerWG sync.WaitGroup
	consumerErrors := make(chan error, numConsumers)

	for c := 0; c < numConsumers; c++ {
		consumerWG.Add(1)
		go func(cid int) {
			defer consumerWG.Done()
			var sum uint64
			expectedSeq := uint64(1)
			for seq, val := range h.Subscribe(ctx, 1, CatchUpFailOnOverrun66Gemini) {
				if seq != expectedSeq {
					consumerErrors <- fmt.Errorf("consumer %d: seq gap: got %d, want %d", cid, seq, expectedSeq)
					return
				}
				sum += val
				expectedSeq++
			}
			if expectedSeq != totalItems+1 {
				consumerErrors <- fmt.Errorf("consumer %d: read %d items, want %d", cid, expectedSeq-1, totalItems)
				return
			}
			wantSum := uint64(totalItems) * uint64(totalItems+1) / 2
			if sum != wantSum {
				consumerErrors <- fmt.Errorf("consumer %d: sum=%d, want %d", cid, sum, wantSum)
			}
		}(c)
	}

	// Producers publish unique values 1..totalItems concurrently via both Publish and PublishBatch.
	var prodWG sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		prodWG.Add(1)
		go func(pid int) {
			defer prodWG.Done()
			base := uint64(pid * itemsPerProd)
			batch := make([]uint64, 0, 8)
			for i := uint64(1); i <= itemsPerProd; i++ {
				v := base + i
				if i%5 == 0 {
					batch = append(batch, v)
					if len(batch) == 4 {
						if _, _, ok := h.PublishBatch(batch); !ok {
							t.Errorf("PublishBatch failed unexpectedly")
							return
						}
						batch = batch[:0]
					}
				} else {
					if _, ok := h.Publish(v); !ok {
						t.Errorf("Publish failed unexpectedly")
						return
					}
				}
			}
			if len(batch) > 0 {
				if _, _, ok := h.PublishBatch(batch); !ok {
					t.Errorf("PublishBatch tail failed")
				}
			}
		}(p)
	}

	prodWG.Wait()
	if !h.Close() {
		t.Fatal("expected first Close() to return true")
	}
	if h.Close() {
		t.Fatal("expected second Close() to return false")
	}

	consumerWG.Wait()
	close(consumerErrors)
	for err := range consumerErrors {
		t.Fatal(err)
	}
}

// TestHorizon66GeminiAdversarialOverrunAndPointerGCSafety stresses small-ring wrap-around with
// pointer-bearing payloads while running concurrent garbage collection cycles and context cancellations.
func TestHorizon66GeminiAdversarialOverrunAndPointerGCSafety(t *testing.T) {
	type pointerPayload struct {
		Title string
		Slice []int
		Seq   uint64
	}

	h := NewHorizon66Gemini[pointerPayload](16)
	if !h.hasPtr {
		t.Fatal("expected typeHasPointers66Gemini to return true for pointerPayload")
	}

	baseGoroutines := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	// 12 concurrent producers hammering a 16-slot ring.
	for p := 0; p < 12; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				payload := pointerPayload{
					Title: fmt.Sprintf("prod-%d-item-%d", id, i),
					Slice: []int{id, i, id + i},
					Seq:   uint64(i),
				}
				if _, ok := h.Publish(payload); !ok {
					return
				}
			}
		}(p)
	}

	// 12 concurrent consumers mixing Latest(), Subscribe(CatchUpSkipToOldest66Gemini), and Subscribe(CatchUpSkipToLatest66Gemini).
	for c := 0; c < 12; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			policy := CatchUpSkipToOldest66Gemini
			if cid%2 == 1 {
				policy = CatchUpSkipToLatest66Gemini
			}
			var prevSeq uint64
			for seq, val := range h.Subscribe(ctx, 1, policy) {
				if seq <= prevSeq {
					t.Errorf("non-monotonic sequence observed: %d after %d", seq, prevSeq)
					return
				}
				if len(val.Slice) != 3 || val.Slice[0]+val.Slice[1] != val.Slice[2] || val.Title == "" {
					t.Errorf("corrupted pointer payload at seq %d: %+v", seq, val)
					return
				}
				prevSeq = seq
				if v, lseq, ok := h.Latest(); ok && lseq > 0 {
					if len(v.Slice) != 3 || v.Slice[0]+v.Slice[1] != v.Slice[2] {
						t.Errorf("corrupted Latest() pointer payload: %+v", v)
						return
					}
				}
			}
		}(c)
	}

	// Trigger concurrent GC to verify pointer write-barrier safety.
	runtime.GC()
	runtime.GC()
	cancel()
	h.Close()
	wg.Wait()

	// Verify zero goroutine leaks.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseGoroutines+2 {
			break
		}
		runtime.Gosched()
	}
	if after := runtime.NumGoroutine(); after > baseGoroutines+2 {
		t.Fatalf("potential goroutine leak: before=%d, after=%d", baseGoroutines, after)
	}
}

// ============================================================================
// Adversarial & Comparative Benchmarks
// ============================================================================

type telemetryEvent struct {
	Timestamp int64
	MetricID  uint64
	Value     float64
	Flags     uint64
}

// mutexRingBaseline is an idiomatic sync.RWMutex + Cond bounded ring baseline for comparison.
type mutexRingBaseline[V any] struct {
	mu    sync.RWMutex
	cond  *sync.Cond
	slots []V
	cap   uint64
	seq   uint64
}

func newMutexRingBaseline[V any](capacity int) *mutexRingBaseline[V] {
	r := &mutexRingBaseline[V]{
		slots: make([]V, capacity),
		cap:   uint64(capacity),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *mutexRingBaseline[V]) Publish(v V) uint64 {
	r.mu.Lock()
	r.seq++
	s := r.seq
	r.slots[s%r.cap] = v
	r.cond.Broadcast()
	r.mu.Unlock()
	return s
}

func (r *mutexRingBaseline[V]) Latest() (V, uint64) {
	r.mu.RLock()
	s := r.seq
	v := r.slots[s%r.cap]
	r.mu.RUnlock()
	return v, s
}

func (r *mutexRingBaseline[V]) TryRead(seq uint64) (V, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if seq > r.seq || r.seq-seq >= r.cap {
		var zero V
		return zero, false
	}
	return r.slots[seq%r.cap], true
}

// BenchmarkPublishAndLatestContention compares Horizon66Gemini vs sync.RWMutex baseline under
// concurrent writer + high-fanout reader contention (Regime A + Regime C).
func BenchmarkPublishAndLatestContention(b *testing.B) {
	ev := telemetryEvent{Timestamp: 1700000000, MetricID: 42, Value: 99.95, Flags: 1}

	b.Run("Horizon_32B_Payload", func(b *testing.B) {
		h := NewHorizon66Gemini[telemetryEvent](4096)
		h.Publish(ev)
		var opIdx atomic.Uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			id := opIdx.Add(1)
			if id%4 == 0 {
				// 25% Writers
				for pb.Next() {
					h.Publish(ev)
				}
			} else {
				// 75% Readers
				for pb.Next() {
					_, _, _ = h.Latest()
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	})

	b.Run("Baseline_RWMutexRing_32B_Payload", func(b *testing.B) {
		r := newMutexRingBaseline[telemetryEvent](4096)
		r.Publish(ev)
		var opIdx atomic.Uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			id := opIdx.Add(1)
			if id%4 == 0 {
				for pb.Next() {
					r.Publish(ev)
				}
			} else {
				for pb.Next() {
					_, _ = r.Latest()
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	})
}

// BenchmarkFanoutBroadcast1ToN compares 1-producer to N-consumer broadcast throughput
// between Horizon66Gemini and an idiomatic channel fanout hub.
func BenchmarkFanoutBroadcast1ToN(b *testing.B) {
	for _, numConsumers := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("Horizon_Consumers_%d", numConsumers), func(b *testing.B) {
			h := NewHorizon66Gemini[uint64](65536)
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			for c := 0; c < numConsumers; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					buf := make([]uint64, 64)
					seq := uint64(1)
					for {
						n, next, _ := h.ReadBatch(seq, buf)
						if n > 0 {
							seq = next
							continue
						}
						if _, st := h.WaitFor(ctx, seq); st == StatusClosed66Gemini || st == StatusCanceled66Gemini {
							return
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Publish(uint64(i))
			}
			b.StopTimer()
			cancel()
			h.Close()
			wg.Wait()
			b.ReportMetric(float64(b.N*numConsumers)/b.Elapsed().Seconds(), "deliveries/sec")
		})

		b.Run(fmt.Sprintf("Baseline_ChanHub_Consumers_%d", numConsumers), func(b *testing.B) {
			chans := make([]chan uint64, numConsumers)
			var wg sync.WaitGroup
			for c := 0; c < numConsumers; c++ {
				ch := make(chan uint64, 1024)
				chans[c] = ch
				wg.Add(1)
				go func(in <-chan uint64) {
					defer wg.Done()
					for range in {
					}
				}(ch)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v := uint64(i)
				for _, ch := range chans {
					select {
					case ch <- v:
					default: // lossy non-blocking send to match bounded ring semantics
					}
				}
			}
			b.StopTimer()
			for _, ch := range chans {
				close(ch)
			}
			wg.Wait()
			b.ReportMetric(float64(b.N*numConsumers)/b.Elapsed().Seconds(), "deliveries/sec")
		})
	}
}

// BenchmarkHorizonBatchThroughput measures vectorized PublishBatch + ReadBatch throughput.
func BenchmarkHorizonBatchThroughput(b *testing.B) {
	const batchSize = 32
	h := NewHorizon66Gemini[uint64](65536)
	inBatch := make([]uint64, batchSize)
	for i := range inBatch {
		inBatch[i] = uint64(i + 1)
	}
	outBatch := make([]uint64, batchSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		first, _, _ := h.PublishBatch(inBatch)
		_, _, _ = h.ReadBatch(first, outBatch)
	}
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "msgs/sec")
}
