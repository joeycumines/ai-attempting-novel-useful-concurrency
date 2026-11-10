package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stressValue68Chatgpt struct {
	producer int
	seq      int
}

func TestEdgeFanInCapacityAndDropNewest(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[int]()
	p, err := q.NewProducer(3) // rounded up to 4
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Capacity(); got != 4 {
		t.Fatalf("capacity = %d, want 4", got)
	}
	for i := 0; i < 4; i++ {
		if !p.TryPush(i) {
			t.Fatalf("TryPush(%d) unexpectedly failed", i)
		}
	}
	if p.TryPush(4) {
		t.Fatal("TryPush on a full lane succeeded")
	}
	for i := 0; i < 4; i++ {
		v, ok := q.TryPop()
		if !ok || v != i {
			t.Fatalf("TryPop() = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
	if !p.TryPush(4) {
		t.Fatal("lane did not become writable after a pop")
	}
	p.Close()
	if p.TryPush(5) {
		t.Fatal("push succeeded after producer Close")
	}
	q.Close()
	v, ok := q.TryPop()
	if !ok || v != 4 {
		t.Fatalf("final TryPop() = (%d, %v), want (4, true)", v, ok)
	}
	if _, ok := q.TryPop(); ok {
		t.Fatal("queue returned a value after it was drained")
	}
}

func TestEdgeFanInPerLaneFIFOAndLoss(t *testing.T) {
	const (
		producerCount = 8
		itemsPerLane  = 20_000
	)
	q := NewEdgeFanIn68Chatgpt[stressValue68Chatgpt]()
	producers := make([]*EdgeProducer68Chatgpt[stressValue68Chatgpt], producerCount)
	for i := range producers {
		p, err := q.NewProducer(128)
		if err != nil {
			t.Fatal(err)
		}
		producers[i] = p
	}

	var accepted [producerCount]atomic.Uint64
	var wg sync.WaitGroup
	for id, p := range producers {
		wg.Add(1)
		go func(id int, p *EdgeProducer68Chatgpt[stressValue68Chatgpt]) {
			defer wg.Done()
			for seq := 0; seq < itemsPerLane; seq++ {
				if p.TryPush(stressValue68Chatgpt{producer: id, seq: seq}) {
					accepted[id].Add(1)
				}
			}
		}(id, p)
	}
	wg.Wait()
	q.Close()

	var last [producerCount]int
	for i := range last {
		last[i] = -1
	}
	var consumed uint64
	for {
		v, ok := q.TryPop()
		if !ok {
			break
		}
		if v.producer < 0 || v.producer >= producerCount {
			t.Fatalf("invalid producer id %d", v.producer)
		}
		if v.seq <= last[v.producer] {
			t.Fatalf("lane %d out of order: got %d after %d", v.producer, v.seq, last[v.producer])
		}
		last[v.producer] = v.seq
		consumed++
	}

	var want uint64
	for i := range accepted {
		want += accepted[i].Load()
	}
	if consumed != want {
		t.Fatalf("consumed %d values, accepted %d", consumed, want)
	}
}

func TestEdgeFanInConcurrentClose(t *testing.T) {
	const producers = 12
	q := NewEdgeFanIn68Chatgpt[uint64]()
	ps := make([]*EdgeProducer68Chatgpt[uint64], producers)
	for i := range ps {
		p, err := q.NewProducer(256)
		if err != nil {
			t.Fatal(err)
		}
		ps[i] = p
	}

	start := make(chan struct{})
	var accepted atomic.Uint64
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Add(1)
		go func(p *EdgeProducer68Chatgpt[uint64]) {
			defer wg.Done()
			<-start
			for n := uint64(0); ; n++ {
				if !p.TryPush(n) {
					return
				}
				accepted.Add(1)
			}
		}(p)
	}
	close(start)
	runtime.Gosched()

	// Multiple Close callers must converge on one completed shutdown.
	var closers sync.WaitGroup
	for i := 0; i < 3; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			q.Close()
		}()
	}
	closers.Wait()
	wg.Wait()

	var consumed uint64
	for {
		_, ok := q.TryPop()
		if !ok {
			break
		}
		consumed++
	}
	if got := accepted.Load(); consumed != got {
		t.Fatalf("consumed %d, accepted %d", consumed, got)
	}
	if _, err := q.NewProducer(1); err != ErrClosed68Chatgpt {
		t.Fatalf("NewProducer after Close error = %v, want ErrClosed68Chatgpt", err)
	}
}

func TestEdgeFanInCloseWakesNextAndDrains(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[string]()
	p, err := q.NewProducer(4)
	if err != nil {
		t.Fatal(err)
	}
	if !p.TryPush("first") || !p.TryPush("second") {
		t.Fatal("initial pushes failed")
	}

	result := make(chan struct {
		value string
		ok    bool
		err   error
	}, 1)
	go func() {
		v, ok, err := q.Next(context.Background())
		result <- struct {
			value string
			ok    bool
			err   error
		}{v, ok, err}
	}()

	q.Close()
	got := <-result
	if got.err != nil || !got.ok || got.value != "first" {
		t.Fatalf("first Next = (%q, %v, %v)", got.value, got.ok, got.err)
	}
	v, ok, err := q.Next(context.Background())
	if err != nil || !ok || v != "second" {
		t.Fatalf("second Next = (%q, %v, %v)", v, ok, err)
	}
	v, ok, err = q.Next(context.Background())
	if err != nil || ok || v != "" {
		t.Fatalf("drained Next = (%q, %v, %v), want empty close", v, ok, err)
	}
}

func TestEdgeFanInConcurrentBatchWrap(t *testing.T) {
	const (
		producerCount = 6
		itemsPerLane  = 30_000
	)
	q := NewEdgeFanIn68Chatgpt[stressValue68Chatgpt]()
	producers := make([]*EdgeProducer68Chatgpt[stressValue68Chatgpt], producerCount)
	for i := range producers {
		p, err := q.NewProducer(64)
		if err != nil {
			t.Fatal(err)
		}
		producers[i] = p
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		var batch [73]stressValue68Chatgpt
		var last [producerCount]int
		for i := range last {
			last[i] = -1
		}
		var consumed int
		for {
			n := q.Drain(batch[:])
			for _, v := range batch[:n] {
				if v.seq <= last[v.producer] {
					t.Errorf("lane %d out of order: got %d after %d", v.producer, v.seq, last[v.producer])
				}
				last[v.producer] = v.seq
				consumed++
			}
			if n == 0 {
				if q.closed.Load() {
					if consumed != producerCount*itemsPerLane {
						t.Errorf("consumed %d, want %d", consumed, producerCount*itemsPerLane)
					}
					return
				}
				runtime.Gosched()
			}
		}
	}()

	var wg sync.WaitGroup
	for id, p := range producers {
		wg.Add(1)
		go func(id int, p *EdgeProducer68Chatgpt[stressValue68Chatgpt]) {
			defer wg.Done()
			var batch [11]stressValue68Chatgpt
			for next := 0; next < itemsPerLane; {
				n := len(batch)
				if n > itemsPerLane-next {
					n = itemsPerLane - next
				}
				for i := 0; i < n; i++ {
					batch[i] = stressValue68Chatgpt{producer: id, seq: next + i}
				}
				accepted := p.TryPushBatch(batch[:n])
				next += accepted
				if accepted == 0 {
					runtime.Gosched()
				}
			}
		}(id, p)
	}
	wg.Wait()
	q.Close()
	<-consumerDone
}

func TestEdgeFanInConcurrentRegistrationAndClose(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[int]()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, err := q.NewProducer(4)
			if err == nil && p == nil {
				t.Error("NewProducer returned nil producer without error")
			}
			if err != nil && err != ErrClosed68Chatgpt {
				t.Errorf("NewProducer error = %v", err)
			}
		}()
	}
	close(start)
	q.Close()
	wg.Wait()
}

func TestEdgeFanInNextCancellation(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, ok, err := q.Next(ctx)
	if ok || err != context.DeadlineExceeded {
		t.Fatalf("Next = (ok=%v, err=%v), want deadline exceeded", ok, err)
	}
	q.Close()
}

func TestEdgeFanInBlockedNextDoesNotLeakOnClose(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[int]()
	result := make(chan error, 1)
	go func() {
		_, ok, err := q.Next(context.Background())
		if ok {
			result <- context.Canceled // impossible sentinel
			return
		}
		result <- err
	}()
	q.Close()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Next after close returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Next did not return after Close")
	}
}

func TestEdgeFanInHotPathDoesNotAllocate(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[uint64]()
	p, err := q.NewProducer(8)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if !p.TryPush(42) {
			panic("unexpected full lane")
		}
		if v, ok := q.TryPop(); !ok || v != 42 {
			panic("unexpected pop result")
		}
	})
	if allocs != 0 {
		t.Fatalf("hot path allocated %.2f objects/op, want 0", allocs)
	}
	q.Close()
}

func TestEdgeFanInInvalidCapacity(t *testing.T) {
	q := NewEdgeFanIn68Chatgpt[int]()
	for _, n := range []int{0, -1} {
		if _, err := q.NewProducer(n); err != ErrInvalidCapacity68Chatgpt {
			t.Fatalf("NewProducer(%d) error = %v, want ErrInvalidCapacity68Chatgpt", n, err)
		}
	}
	q.Close()
}

// Benchmarks compare the edge-triggered SPSC-lane fan-in with a buffered Go
// channel. They measure the full M-producer/1-consumer path, not isolated ops.
type bench128 struct{ payload [128]byte }

func BenchmarkEdgeFanIn(b *testing.B) {
	for _, producers := range []int{1, 4, 16} {
		b.Run("uint64/edge/producers="+benchInt(producers), func(b *testing.B) {
			benchmarkEdgeFanIn[uint64](b, producers, 7)
		})
		b.Run("uint64/channel/producers="+benchInt(producers), func(b *testing.B) {
			benchmarkChannelFanIn[uint64](b, producers, 7)
		})
		b.Run("128B/edge/producers="+benchInt(producers), func(b *testing.B) {
			benchmarkEdgeFanIn[bench128](b, producers, bench128{})
		})
		b.Run("128B/channel/producers="+benchInt(producers), func(b *testing.B) {
			benchmarkChannelFanIn[bench128](b, producers, bench128{})
		})
		b.Run("uint64/edge-batch32/producers="+benchInt(producers), func(b *testing.B) {
			benchmarkEdgeFanInBatch[uint64](b, producers, 7)
		})
	}
}

func benchmarkEdgeFanIn[V any](b *testing.B, producerCount int, value V) {
	q := NewEdgeFanIn68Chatgpt[V]()
	producers := make([]*EdgeProducer68Chatgpt[V], producerCount)
	for i := range producers {
		p, err := q.NewProducer(1024)
		if err != nil {
			b.Fatal(err)
		}
		producers[i] = p
	}

	start := make(chan struct{})
	consumerDone := make(chan struct{})
	var consumed atomic.Int64
	go func() {
		var batch [64]V
		<-start
		for consumed.Load() < int64(b.N) {
			n := q.Drain(batch[:])
			if n == 0 {
				runtime.Gosched()
				continue
			}
			consumed.Add(int64(n))
		}
		close(consumerDone)
	}()

	var wg sync.WaitGroup
	for i, p := range producers {
		count := b.N / producerCount
		if i < b.N%producerCount {
			count++
		}
		wg.Add(1)
		go func(p *EdgeProducer68Chatgpt[V], count int) {
			defer wg.Done()
			<-start
			for sent := 0; sent < count; {
				if p.TryPush(value) {
					sent++
				} else {
					runtime.Gosched()
				}
			}
		}(p, count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
	<-consumerDone
	b.StopTimer()
	q.Close()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func benchmarkEdgeFanInBatch[V any](b *testing.B, producerCount int, value V) {
	q := NewEdgeFanIn68Chatgpt[V]()
	producers := make([]*EdgeProducer68Chatgpt[V], producerCount)
	for i := range producers {
		p, err := q.NewProducer(1024)
		if err != nil {
			b.Fatal(err)
		}
		producers[i] = p
	}

	start := make(chan struct{})
	consumerDone := make(chan struct{})
	var consumed atomic.Int64
	go func() {
		var batch [128]V
		<-start
		for consumed.Load() < int64(b.N) {
			n := q.Drain(batch[:])
			if n == 0 {
				runtime.Gosched()
				continue
			}
			consumed.Add(int64(n))
		}
		close(consumerDone)
	}()

	var wg sync.WaitGroup
	for i, p := range producers {
		count := b.N / producerCount
		if i < b.N%producerCount {
			count++
		}
		wg.Add(1)
		go func(p *EdgeProducer68Chatgpt[V], count int) {
			defer wg.Done()
			<-start
			var values [32]V
			for i := range values {
				values[i] = value
			}
			for sent := 0; sent < count; {
				n := len(values)
				if n > count-sent {
					n = count - sent
				}
				accepted := p.TryPushBatch(values[:n])
				sent += accepted
				if accepted == 0 {
					runtime.Gosched()
				}
			}
		}(p, count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
	<-consumerDone
	b.StopTimer()
	q.Close()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func benchmarkChannelFanIn[V any](b *testing.B, producerCount int, value V) {
	ch := make(chan V, producerCount*1024)
	start := make(chan struct{})
	consumerDone := make(chan struct{})
	go func() {
		<-start
		for i := 0; i < b.N; i++ {
			<-ch
		}
		close(consumerDone)
	}()

	var wg sync.WaitGroup
	for i := 0; i < producerCount; i++ {
		count := b.N / producerCount
		if i < b.N%producerCount {
			count++
		}
		wg.Add(1)
		go func(count int) {
			defer wg.Done()
			<-start
			for j := 0; j < count; j++ {
				ch <- value
			}
		}(count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	wg.Wait()
	<-consumerDone
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

func benchInt(n int) string {
	const digits = "0123456789"
	if n < 10 {
		return string(digits[n])
	}
	return benchInt(n/10) + string(digits[n%10])
}
