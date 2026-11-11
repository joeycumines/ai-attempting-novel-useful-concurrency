package bigbuff

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// testPayload has two copies of the same value. A torn read would
// show X != Y, because an overwriting publisher writes a
// different i while the reader is mid-copy.
type testPayload struct {
	X int64
	Y int64
}

func TestNewHorizon_PanicsOnBadCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1, 3, 6, 100, 1023} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewHorizon70Meta(%d): expected panic", capacity)
				}
			}()
			_ = NewHorizon70Meta[int64](capacity)
		}()
	}
	for _, capacity := range []int{1, 2, 4, 16, 1024} {
		if h := NewHorizon70Meta[int64](capacity); h.Cap() != capacity {
			t.Fatalf("Cap() = %d, want %d", h.Cap(), capacity)
		}
	}
}

func TestHorizon_Basic_PublishSubscribe(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	c := h.Subscribe()
	if _, ok := c.Next(); ok {
		t.Fatal("Next on empty horizon should return false")
	}
	for i := int64(0); i < 10; i++ {
		seq, ok := h.Publish(i)
		if !ok || seq != uint64(i) {
			t.Fatalf("Publish(%d) = (%d,%v), want (%d,true)", i, seq, ok, i)
		}
		v, ok := c.Next()
		if !ok || v != i {
			t.Fatalf("Next() = (%d,%v), want (%d,true)", v, ok, i)
		}
	}
	if _, ok := c.Next(); ok {
		t.Fatal("Next past end should return false")
	}
}

func TestHorizon_Broadcast_EverySubscriberSeesSame(t *testing.T) {
	h := NewHorizon70Meta[int64](1024)
	defer h.Close()
	for i := int64(0); i < 100; i++ {
		if _, ok := h.Publish(i); !ok {
			t.Fatal("publish failed")
		}
	}
	// Three cursors from oldest must each see the identical stream.
	// This is broadcast, not competing consumption.
	for k := 0; k < 3; k++ {
		c := h.SubscribeFromOldest()
		for i := int64(0); i < 100; i++ {
			v, ok := c.Next()
			if !ok || v != i {
				t.Fatalf("cursor %d: Next() = (%d,%v), want (%d,true)", k, v, ok, i)
			}
		}
	}
}

func TestHorizon_Order_NoLoss_WhenCaughtUp(t *testing.T) {
	h := NewHorizon70Meta[int64](64)
	defer h.Close()
	c := h.Subscribe()
	var want int64
	for i := int64(0); i < 1000; i++ {
		if _, ok := h.Publish(i); !ok {
			t.Fatal("publish failed")
		}
		v, ok := c.Next()
		if !ok {
			t.Fatalf("lost message %d: Next returned false while caught up", i)
		}
		if v != want {
			t.Fatalf("Next() = %d, want %d (out of order / duplicate)", v, want)
		}
		want++
	}
}

func TestHorizon_LagSkipsButNeverTornOrDuplicate(t *testing.T) {
	h := NewHorizon70Meta[testPayload](16)
	defer h.Close()
	c := h.SubscribeFromOldest()
	// Publish far more than capacity without reading: the cursor
	// must skip, never see a torn payload, never see a duplicate.
	const n = 10000
	for i := int64(0); i < n; i++ {
		if _, ok := h.Publish(testPayload{X: i, Y: i}); !ok {
			t.Fatal("publish failed")
		}
	}
	// Lag/Missed are defined against the CURRENT cursor position, so they
	// are only meaningful before the drain fast-forwards the cursor: at this
	// instant the cursor is parked at 0 with tail=10000 (lag 10000; all but
	// the retained capacity-16 window are permanently lost).
	if lag := c.Lag(); lag != n {
		t.Fatalf("Lag() = %d, want %d", lag, n)
	}
	if missed := c.Missed(); missed != n-16 {
		t.Fatalf("Missed() = %d, want %d", missed, n-16)
	}
	var prev int64 = -1
	seen := 0
	for {
		p, ok := c.Next()
		if !ok {
			break
		}
		if p.X != p.Y {
			t.Fatalf("torn read: X=%d Y=%d", p.X, p.Y)
		}
		if p.X <= prev {
			t.Fatalf("duplicate / reorder: got %d after %d", p.X, prev)
		}
		prev = p.X
		seen++
	}
	// At most capacity messages can survive; at least 1 must.
	if seen == 0 || seen > 16 {
		t.Fatalf("seen = %d, want 1..16 after lagging by %d over cap 16", seen, n)
	}
	// The last message must be the newest one.
	if prev != n-1 {
		t.Fatalf("last seen = %d, want %d (newest must survive)", prev, n-1)
	}
	// After a full drain the cursor is caught up, so Lag/Missed read 0 by
	// definition; the pre-drain values above are the checks with teeth.
	if lag := c.Lag(); lag != 0 {
		t.Fatalf("Lag() after drain = %d, want 0", lag)
	}
}

func TestHorizon_PublishBatch_Order(t *testing.T) {
	h := NewHorizon70Meta[int64](64)
	defer h.Close()
	c := h.Subscribe()
	batch := []int64{10, 20, 30, 40}
	first, ok := h.PublishBatch(batch)
	if !ok || first != 0 {
		t.Fatalf("PublishBatch = (%d,%v), want (0,true)", first, ok)
	}
	for _, want := range batch {
		v, ok := c.Next()
		if !ok || v != want {
			t.Fatalf("Next() = (%d,%v), want (%d,true)", v, ok, want)
		}
	}
	if _, ok := h.PublishBatch(nil); !ok {
		t.Fatal("empty batch should succeed")
	}
}

func TestHorizon_Latest(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	if _, _, ok := h.Latest(); ok {
		t.Fatal("Latest on empty should return false")
	}
	for i := int64(0); i < 5; i++ {
		if _, ok := h.Publish(i); !ok {
			t.Fatal("publish failed")
		}
	}
	v, seq, ok := h.Latest()
	if !ok || v != 4 || seq != 4 {
		t.Fatalf("Latest() = (%d,%d,%v), want (4,4,true)", v, seq, ok)
	}
	// Latest does not consume: a cursor from oldest still sees all.
	c := h.SubscribeFromOldest()
	for i := int64(0); i < 5; i++ {
		if v, ok := c.Next(); !ok || v != i {
			t.Fatalf("cursor after Latest: Next() = (%d,%v), want (%d,true)", v, ok, i)
		}
	}
}

func TestHorizon_WaitNext_Wakes(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	c := h.Subscribe()
	done := make(chan int64, 1)
	go func() {
		v, ok := c.WaitNext()
		if !ok {
			done <- -1
			return
		}
		done <- v
	}()
	// Wait until the waiter has registered, then publish. WaitNext re-checks
	// under the Cond lock, so waking before/after this point is both correct.
	waitUntil(t, "waiter to register", func() bool { return h.waiters.Load() == 1 })
	if _, ok := h.Publish(42); !ok {
		t.Fatal("publish failed")
	}
	select {
	case v := <-done:
		if v != 42 {
			t.Fatalf("WaitNext got %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitNext did not wake")
	}
}

// A waiter holds the Cond mutex from its re-check until Wait releases it. A
// publish that arrives in that window must still wake it. As delivered, the
// publisher's TryLock failed, it skipped the broadcast, and the waiter parked
// for good.
func TestHorizon_PublishWakesAWaiterAboutToPark(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close() // wakes the waiter if the test fails, so it does not leak
	locked, proceed, woke := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		// A waiter that has registered and re-checked, and has not parked.
		h.waiters.Add(1)
		h.mu.Lock()
		close(locked)
		<-proceed
		h.cond.Wait()
		h.mu.Unlock()
		h.waiters.Add(-1)
		close(woke)
	}()
	<-locked
	published := make(chan struct{})
	go func() {
		defer close(published)
		if _, ok := h.Publish(42); !ok {
			t.Error("publish failed")
		}
	}()
	// Let the publish reach its notification while the mutex is held. A
	// publisher that waits for the mutex is still there when this expires; one
	// that gave up has returned.
	select {
	case <-published:
	case <-time.After(50 * time.Millisecond):
	}
	close(proceed)
	select {
	case <-woke:
	case <-timeAfterGuard():
		t.Fatal("a publish that arrived while the waiter held the mutex did not wake it")
	}
	select {
	case <-published:
	case <-timeAfterGuard():
		t.Fatal("Publish did not return")
	}
}

func TestHorizon_WaitNext_CloseWakes(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	c := h.Subscribe()
	done := make(chan bool, 1)
	go func() {
		_, ok := c.WaitNext()
		done <- ok
	}()
	waitUntil(t, "waiter to register", func() bool { return h.waiters.Load() == 1 })
	h.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("WaitNext after Close should return false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitNext did not wake on Close")
	}
	// Close is idempotent.
	h.Close()
}

func TestHorizon_WaitNextContext_Cancel(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	c := h.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		_, ok := c.WaitNextContext(ctx)
		done <- ok
	}()
	waitUntil(t, "waiter to register", func() bool { return h.waiters.Load() == 1 })
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("WaitNextContext after cancel should return false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitNextContext did not wake on cancel")
	}
}

func TestHorizon_WaitNextContext_DataWins(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	c := h.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Publish once the waiter has registered (the park-path wakeup
		// is what this test exercises).
		deadline := time.Now().Add(10 * time.Second)
		for h.waiters.Load() != 1 {
			if time.Now().After(deadline) {
				panic("waiter did not register before the deadline")
			}
			runtime.Gosched()
		}
		h.Publish(7)
	}()
	v, ok := c.WaitNextContext(ctx)
	if !ok || v != 7 {
		t.Fatalf("WaitNextContext = (%d,%v), want (7,true)", v, ok)
	}
}

func TestHorizon_SubscribeFrom_Clamping(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	for i := int64(0); i < 100; i++ {
		h.Publish(i)
	}
	// seq 0 is long overwritten: clamped to oldest retained (84).
	c := h.SubscribeFrom(0)
	if c.Seq() != 84 {
		t.Fatalf("SubscribeFrom(0).Seq() = %d, want 84", c.Seq())
	}
	// Future seq clamped to tail.
	c2 := h.SubscribeFrom(10000)
	if c2.Seq() != 100 {
		t.Fatalf("SubscribeFrom(future).Seq() = %d, want 100", c2.Seq())
	}
	// In-range seq preserved.
	c3 := h.SubscribeFrom(90)
	if c3.Seq() != 90 {
		t.Fatalf("SubscribeFrom(90).Seq() = %d, want 90", c3.Seq())
	}
}

func TestHorizon_Close_PublishAfterClose(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	h.Close()
	if _, ok := h.Publish(1); ok {
		t.Fatal("Publish after Close should return false")
	}
	if _, ok := h.PublishBatch([]int64{1, 2}); ok {
		t.Fatal("PublishBatch after Close should return false")
	}
}

func TestHorizon_All_Iterator(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	c := h.Subscribe()
	go func() {
		for i := int64(0); i < 5; i++ {
			h.Publish(i)
		}
		h.Close()
	}()
	var got []int64
	for v := range c.All() {
		got = append(got, v)
	}
	if len(got) != 5 {
		t.Fatalf("All() yielded %v, want 0..4", got)
	}
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("All()[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestHorizon_AllContext_Iterator(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	c := h.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan int64, 1)
	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		for v := range c.AllContext(ctx) {
			results <- v
		}
	}()
	h.Publish(99)
	// Deterministically wait until the iterator yielded the value before
	// cancelling: cancellation cannot race ahead of the expected yield.
	select {
	case v := <-results:
		if v != 99 {
			t.Fatalf("AllContext yielded %d, want 99", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AllContext did not yield the published value")
	}
	cancel()
	select {
	case <-iterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("AllContext did not return after cancel")
	}
}

func TestHorizon_ZeroAllocs(t *testing.T) {
	h := NewHorizon70Meta[int64](1024)
	defer h.Close()
	c := h.Subscribe()
	allocs := testing.AllocsPerRun(100, func() {
		h.Publish(1)
		c.Next()
	})
	if allocs != 0 {
		t.Fatalf("Publish+Next allocs = %v, want 0", allocs)
	}
	allocs = testing.AllocsPerRun(100, func() {
		h.Latest()
	})
	if allocs != 0 {
		t.Fatalf("Latest allocs = %v, want 0", allocs)
	}
	// PublishBatch of a reused slice must also be allocation-free.
	batch := []int64{1, 2, 3, 4}
	allocs = testing.AllocsPerRun(100, func() {
		h.PublishBatch(batch)
	})
	if allocs != 0 {
		t.Fatalf("PublishBatch allocs = %v, want 0", allocs)
	}
}

// TestHorizon_Concurrent is the adversarial test: M producers
// hammering one tail, N consumers each with their own cursor,
// all polling. No duplicate / reorder / torn per cursor, no
// deadlock, no race (run with -race).
func TestHorizon_Concurrent(t *testing.T) {
	const (
		capacity    = 1024
		producers   = 8
		consumers   = 8
		perProducer = 5000
	)
	h := NewHorizon70Meta[testPayload](capacity)
	defer h.Close()

	var wg sync.WaitGroup

	// Consumers start before producers so they can try to keep up.
	// They stop when they have seen the newest seq and Close has
	// been called, via polling + Lag check in the main goroutine
	// below. To avoid a flaky "consumer never finishes" hang, each
	// consumer runs until producers are done and it has drained to
	// within capacity of the tail, then it exits; the main test
	// verifies per-cursor invariants, not full delivery (lossy).
	doneProducing := make(chan struct{})
	consumerErr := make(chan error, consumers)
	for ci := 0; ci < consumers; ci++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := h.SubscribeFromOldest()
			// Claim order interleaves the producers, so per-cursor values
			// are not monotone by design. The invariants that DO hold:
			// a cursor never observes a torn payload, never observes the
			// same message twice, and never observes an out-of-range one.
			seenMsg := make([]bool, producers*perProducer)
			for {
				p, ok := c.Next()
				if !ok {
					select {
					case <-doneProducing:
						// Producers done. If we are within
						// capacity of tail, the remaining
						// messages are the newest ones and we
						// have already verified them on the way;
						// drain what is left, then exit.
						for {
							p2, ok2 := c.Next()
							if !ok2 {
								return
							}
							if p2.X != p2.Y {
								consumerErr <- fmt.Errorf("torn: %+v", p2)
								return
							}
							if p2.X < 0 || p2.X >= int64(len(seenMsg)) {
								consumerErr <- fmt.Errorf("out-of-range message: %d", p2.X)
								return
							}
							if seenMsg[p2.X] {
								consumerErr <- fmt.Errorf("duplicate message delivered: %d", p2.X)
								return
							}
							seenMsg[p2.X] = true
						}
					default:
						// Not done yet, poll again. Yield so
						// producers get CPU.
						runtime.Gosched()
						continue
					}
				}
				if p.X != p.Y {
					consumerErr <- fmt.Errorf("torn: %+v", p)
					return
				}
				if p.X < 0 || p.X >= int64(len(seenMsg)) {
					consumerErr <- fmt.Errorf("out-of-range message: %d", p.X)
					return
				}
				if seenMsg[p.X] {
					consumerErr <- fmt.Errorf("duplicate message delivered: %d", p.X)
					return
				}
				seenMsg[p.X] = true
			}
		}()
	}

	var prodWG sync.WaitGroup
	for pi := 0; pi < producers; pi++ {
		prodWG.Add(1)
		wg.Add(1)
		go func(pi int) {
			defer prodWG.Done()
			defer wg.Done()
			base := int64(pi * perProducer)
			for i := int64(0); i < perProducer; i++ {
				v := base + i
				if _, ok := h.Publish(testPayload{X: v, Y: v}); !ok {
					consumerErr <- fmt.Errorf("publish failed")
					return
				}
			}
		}(pi)
	}

	// Signal the consumers once all producers are done. Consumers
	// share wg, so wait on the separate prodWG instead of polling.
	go func() {
		prodWG.Wait()
		close(doneProducing)
	}()

	// Wait with a timeout so a deadlock fails fast, not via the
	// test timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent test timed out (deadlock / livelock)")
	}
	select {
	case err := <-consumerErr:
		t.Fatal(err)
	default:
	}
	if got := h.Tail(); got != uint64(producers*perProducer) {
		t.Fatalf("Tail() = %d, want %d", got, producers*perProducer)
	}
}

func TestHorizon_SkipToLatest(t *testing.T) {
	h := NewHorizon70Meta[int64](16)
	defer h.Close()
	for i := int64(0); i < 10; i++ {
		h.Publish(i)
	}
	c := h.SubscribeFromOldest()
	if c.Lag() != 10 {
		t.Fatalf("Lag() = %d, want 10", c.Lag())
	}
	c.SkipToLatest()
	if c.Lag() != 0 {
		t.Fatalf("Lag after SkipToLatest = %d, want 0", c.Lag())
	}
	h.Publish(10)
	v, ok := c.Next()
	if !ok || v != 10 {
		t.Fatalf("Next after SkipToLatest = (%d,%v), want (10,true)", v, ok)
	}
}

// ---- Benchmarks: Horizon70Meta vs idiomatic baselines ----

func BenchmarkHorizon_Publish_NoConsumer(b *testing.B) {
	h := NewHorizon70Meta[int64](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Publish(int64(i))
	}
}

func BenchmarkHorizon_Publish_Parallel(b *testing.B) {
	h := NewHorizon70Meta[int64](8192)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			h.Publish(i)
			i++
		}
	})
}

func BenchmarkHorizon_PublishBatch_64(b *testing.B) {
	h := NewHorizon70Meta[int64](8192)
	batch := make([]int64, 64)
	for i := range batch {
		batch[i] = int64(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.PublishBatch(batch)
	}
}

func BenchmarkHorizon_Latest(b *testing.B) {
	h := NewHorizon70Meta[int64](1024)
	h.Publish(42)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Latest()
	}
}

func BenchmarkHorizon_PublishConsume_1(b *testing.B) {
	h := NewHorizon70Meta[int64](1024)
	c := h.Subscribe()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Publish(int64(i))
		c.Next()
	}
}

func BenchmarkHorizon_Fanout_1(b *testing.B)   { benchmarkHorizonFanout(b, 1) }
func BenchmarkHorizon_Fanout_16(b *testing.B)  { benchmarkHorizonFanout(b, 16) }
func BenchmarkHorizon_Fanout_128(b *testing.B) { benchmarkHorizonFanout(b, 128) }

func benchmarkHorizonFanout(b *testing.B, n int) {
	h := NewHorizon70Meta[int64](4096)
	cursors := make([]*Cursor70Meta[int64], n)
	for i := range cursors {
		cursors[i] = h.Subscribe()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Publish(int64(i))
		for _, c := range cursors {
			c.Next()
		}
	}
}

// Baseline: single chan send/recv.
func BenchmarkChan_SendRecv(b *testing.B) {
	ch := make(chan int64, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- int64(i)
		<-ch
	}
}

// Baseline: chan fanout. Publisher must Send once per subscriber,
// non-blocking (lossy, like Horizon70Meta). This is the O(N) cost
// Horizon70Meta avoids: compare Fanout_16 / Fanout_128 against this.
func BenchmarkChan_Fanout_1(b *testing.B)   { benchmarkChanFanout(b, 1) }
func BenchmarkChan_Fanout_16(b *testing.B)  { benchmarkChanFanout(b, 16) }
func BenchmarkChan_Fanout_128(b *testing.B) { benchmarkChanFanout(b, 128) }

func benchmarkChanFanout(b *testing.B, n int) {
	chans := make([]chan int64, n)
	for i := range chans {
		chans[i] = make(chan int64, 1024)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := int64(i)
		for _, ch := range chans {
			select {
			case ch <- v:
			default:
			}
		}
		for _, ch := range chans {
			select {
			case <-ch:
			default:
			}
		}
	}
}

// Baseline: Mutex-protected latest value + Cond broadcast. The
// classic "watch" pattern. Publisher serialises on one mutex.
func BenchmarkMutex_Watch_Publish(b *testing.B) {
	var mu sync.Mutex
	var val int64
	cond := sync.NewCond(&mu)
	_ = cond
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		val = int64(i)
		mu.Unlock()
	}
	_ = val
}
