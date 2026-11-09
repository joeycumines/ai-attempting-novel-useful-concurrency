## Chosen primitive: `EdgeFanIn`

The target is many independent producer goroutines feeding one consumer, especially telemetry, ingestion, and work-dispatch paths where bounded buffering and dropping the newest item are acceptable.

The intended advantages over a general-purpose channel are:

- One SPSC ring per producer, avoiding a shared per-message reservation counter.
- FIFO delivery within each producer; no promised global ordering between producers.
- A shared, intrusive ready stack that queues each lane only on an idle-to-ready transition.
- Coalesced wakeups, bounded memory, and no background goroutines.
- Explicit producer shutdown and draining semantics.

One important qualification from the first benchmark run: the initial implementation was slower than a buffered Go channel on this host. For example, the one-producer `uint64` result was 50.9 ns/op versus 33.3 ns/op for the channel. I’m treating that as a failed optimization, not hiding it; the next iteration reduces per-message synchronization and makes batch operations first-class.

## Implementation delivered



Race detector passed

Zero-allocation hot path



Complete Go module

Download bigbuff-edgefanin.zip

Download tar.gz

Includes:

- `edgefanin.go` — production implementation.
- `edgefanin_test.go` — concurrency stress tests and benchmarks.
- `README.md` — design thesis, API contract, benchmark results.
- `doc.go` and `go.mod`.

Individual files:

- Implementation
- Tests and benchmarks
- Design documentation

## Final benchmark results

Go 1.23.2 · Linux/amd64 · AMD EPYC 9V74 · 1-second benchmark runs

| Workload             | EdgeFanIn | Go channel |
| -------------------- | --------- | ---------- |
| uint64, 1 producer   | 58.90 ns  | 36.17 ns   |
| uint64, 4 producers  | 29.70 ns  | 36.63 ns   |
| uint64, 16 producers | 21.67 ns  | 47.01 ns   |
| 128 B, 4 producers   | 50.07 ns  | 45.09 ns   |
| 128 B, 16 producers  | 27.09 ns  | 54.92 ns   |

Batch-32 performance

# 5.52–5.79 ns

Per message, across 1, 4 and 16 producers.

# 0 B/op · 0 allocs/op

The important finding is that the implementation has a specific performance envelope. It does not beat channels everywhere. Its advantage emerges with multiple producers and batching.

## Verification

All completed successfully:

```
go test ./...
go test -race -count=5 ./...
go vet ./...
```

Additional correctness coverage includes concurrent shutdown, ring wraparound, FIFO ordering per producer, accepted-message accounting, registration racing with close, cancellation and blocked-consumer wakeup.

One integration limitation: I could not access `/Users/joeyc/dev/go-bigbuff` from this execution environment. Consequently, this is a standalone module, not a modification of your existing checkout.

The implementation deliberately avoids `unsafe`, assembly and runtime internals. Its optimization comes from specializing the synchronization topology rather than attempting to replace Go's general-purpose channel machinery.

---------

````txtar
-- doc.go --
// Package bigbuff contains specialized concurrent buffers.
//
// EdgeFanIn is a bounded, lossy M:1 fan-in primitive for independent SPSC
// producer lanes and one batch-oriented consumer. See the README for its
// operational envelope, ordering guarantees, and benchmark methodology.
package bigbuff
-- edgefanin_test.go --
package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stressValue struct {
	producer int
	seq      int
}

func TestEdgeFanInCapacityAndDropNewest(t *testing.T) {
	q := NewEdgeFanIn[int]()
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
	q := NewEdgeFanIn[stressValue]()
	producers := make([]*EdgeProducer[stressValue], producerCount)
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
		go func(id int, p *EdgeProducer[stressValue]) {
			defer wg.Done()
			for seq := 0; seq < itemsPerLane; seq++ {
				if p.TryPush(stressValue{producer: id, seq: seq}) {
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
	q := NewEdgeFanIn[uint64]()
	ps := make([]*EdgeProducer[uint64], producers)
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
		go func(p *EdgeProducer[uint64]) {
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
	if _, err := q.NewProducer(1); err != ErrClosed {
		t.Fatalf("NewProducer after Close error = %v, want ErrClosed", err)
	}
}

func TestEdgeFanInCloseWakesNextAndDrains(t *testing.T) {
	q := NewEdgeFanIn[string]()
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
	q := NewEdgeFanIn[stressValue]()
	producers := make([]*EdgeProducer[stressValue], producerCount)
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
		var batch [73]stressValue
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
		go func(id int, p *EdgeProducer[stressValue]) {
			defer wg.Done()
			var batch [11]stressValue
			for next := 0; next < itemsPerLane; {
				n := len(batch)
				if n > itemsPerLane-next {
					n = itemsPerLane - next
				}
				for i := 0; i < n; i++ {
					batch[i] = stressValue{producer: id, seq: next + i}
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
	q := NewEdgeFanIn[int]()
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
			if err != nil && err != ErrClosed {
				t.Errorf("NewProducer error = %v", err)
			}
		}()
	}
	close(start)
	q.Close()
	wg.Wait()
}

func TestEdgeFanInNextCancellation(t *testing.T) {
	q := NewEdgeFanIn[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, ok, err := q.Next(ctx)
	if ok || err != context.DeadlineExceeded {
		t.Fatalf("Next = (ok=%v, err=%v), want deadline exceeded", ok, err)
	}
	q.Close()
}

func TestEdgeFanInBlockedNextDoesNotLeakOnClose(t *testing.T) {
	q := NewEdgeFanIn[int]()
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
	q := NewEdgeFanIn[uint64]()
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
	q := NewEdgeFanIn[int]()
	for _, n := range []int{0, -1} {
		if _, err := q.NewProducer(n); err != ErrInvalidCapacity {
			t.Fatalf("NewProducer(%d) error = %v, want ErrInvalidCapacity", n, err)
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
	q := NewEdgeFanIn[V]()
	producers := make([]*EdgeProducer[V], producerCount)
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
		go func(p *EdgeProducer[V], count int) {
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
	q := NewEdgeFanIn[V]()
	producers := make([]*EdgeProducer[V], producerCount)
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
		go func(p *EdgeProducer[V], count int) {
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
-- edgefanin.go --
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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	edgeCachePad  = 128
	edgeBatchSize = 32
)

var (
	// ErrClosed is returned by NewProducer after the fan-in has started closing.
	ErrClosed = errors.New("bigbuff: edgefanin: closed")
	// ErrInvalidCapacity indicates that a producer capacity was not positive or
	// could not be rounded up to a representable power of two.
	ErrInvalidCapacity = errors.New("bigbuff: edgefanin: invalid capacity")
)

// EdgeFanIn is a bounded, lossy M:1 fan-in queue for a set of producer
// goroutines. Its intended workload is many independent writers feeding one
// batch-oriented reader.
//
// Each Producer is an SPSC lane: exactly one goroutine may call TryPush or
// TryPushBatch on a given Producer, while exactly one goroutine consumes from
// the EdgeFanIn. Different Producer values may be used concurrently. The
// consumer may use TryPop, Drain, or Next, but must not use them concurrently
// with one another.
//
// A full lane drops new values; it never overwrites accepted values. Ordering
// is FIFO within each producer lane. There is intentionally no global ordering
// between producers. Capacity is bounded independently per producer.
//
// The zero value is not ready for use; construct with NewEdgeFanIn.
type EdgeFanIn[V any] struct {
	ready atomic.Pointer[edgeLane[V]]
	wake  chan struct{}
	done  chan struct{}

	// mu and lanes are control-plane state only. The hot path never takes mu.
	mu      sync.Mutex
	lanes   []*edgeLane[V]
	closing atomic.Bool
	closed  atomic.Bool

	// These fields belong exclusively to the single consumer.
	pending *edgeLane[V]
	current *edgeLane[V]
	budget  uint32
}

// EdgeProducer is one SPSC ingress lane belonging to an EdgeFanIn.
// Do not copy an EdgeProducer after first use. TryPush and TryPushBatch must
// be called by only one goroutine at a time; concurrent calls panic rather than
// silently corrupt the SPSC ring.
type EdgeProducer[V any] struct {
	queue *EdgeFanIn[V]
	lane  *edgeLane[V]
}

// Producer and consumer cursors occupy distinct padded regions. This reduces
// false sharing between the two sides of a lane. The padding is conservative
// for contemporary x86-64 and arm64 systems, not a cache-line-size guarantee.
type edgeProducerState struct {
	_         [edgeCachePad]byte
	active    atomic.Bool
	tail      uint64 // producer goroutine only
	published atomic.Uint64
	_         [104]byte
}

type edgeConsumerState struct {
	_        [edgeCachePad]byte
	head     uint64 // consumer goroutine only
	consumed atomic.Uint64
	_        [112]byte
}

type edgeLane[V any] struct {
	producer edgeProducerState
	consumer edgeConsumerState

	slots []V
	mask  uint64

	// queued protects the intrusive ready-stack node. A lane is present in the
	// MPSC ready stack at most once, however many values it contains.
	queued atomic.Bool
	next   atomic.Pointer[edgeLane[V]]
	closed atomic.Bool
	queue  *EdgeFanIn[V]
}

// NewEdgeFanIn constructs an empty fan-in. It starts no goroutines.
func NewEdgeFanIn[V any]() *EdgeFanIn[V] {
	return &EdgeFanIn[V]{
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

// NewProducer adds a bounded SPSC lane. Capacity is rounded up to a power of
// two, so the actual capacity may be greater than requested. A capacity of 1
// is valid. The lane may be added while the queue is being consumed, but not
// after Close has begun.
func (q *EdgeFanIn[V]) NewProducer(capacity int) (*EdgeProducer[V], error) {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn")
	}
	n, ok := edgeRoundCapacity(capacity)
	if !ok {
		return nil, ErrInvalidCapacity
	}

	l := &edgeLane[V]{
		slots: make([]V, n),
		mask:  uint64(n - 1),
		queue: q,
	}

	q.mu.Lock()
	if q.closing.Load() {
		q.mu.Unlock()
		return nil, ErrClosed
	}
	q.lanes = append(q.lanes, l)
	q.mu.Unlock()

	return &EdgeProducer[V]{queue: q, lane: l}, nil
}

func edgeRoundCapacity(n int) (int, bool) {
	if n < 1 {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	v := 1
	for v < n {
		if v > maxInt/2 {
			return 0, false
		}
		v <<= 1
	}
	return v, true
}

// Capacity reports the lane's rounded-up capacity.
func (p *EdgeProducer[V]) Capacity() int {
	if p == nil || p.lane == nil {
		return 0
	}
	return len(p.lane.slots)
}

// TryPush inserts value if the lane has room and remains open. It is a
// non-blocking drop-newest operation: false means either full or closed.
// For sustained high-rate traffic, TryPushBatch amortizes publication and is
// the preferred producer API.
func (p *EdgeProducer[V]) TryPush(value V) bool {
	var one [1]V
	one[0] = value
	return p.TryPushBatch(one[:]) == 1
}

// TryPushBatch inserts the largest prefix of values that currently fits and
// returns the number accepted. The remaining suffix is dropped by the caller.
// It publishes the entire accepted prefix with one cursor store, making this
// method particularly effective when producers naturally emit batches.
//
// TryPushBatch performs no allocation. A zero-length input returns zero.
func (p *EdgeProducer[V]) TryPushBatch(values []V) int {
	if p == nil || p.lane == nil {
		panic("bigbuff: edgefanin: nil producer")
	}
	if len(values) == 0 {
		return 0
	}
	q, l := p.queue, p.lane
	if q.closing.Load() || l.closed.Load() {
		return 0
	}
	if !l.producer.active.CompareAndSwap(false, true) {
		if q.closing.Load() || l.closed.Load() {
			return 0
		}
		panic("bigbuff: edgefanin: concurrent push on one producer")
	}

	// This second check forms the admission boundary. A push that passes it is
	// in-flight, and Close waits for active to clear before returning.
	if q.closing.Load() || l.closed.Load() {
		l.producer.active.Store(false)
		return 0
	}

	tail := l.producer.tail
	consumed := l.consumer.consumed.Load()
	used := tail - consumed
	capacity := uint64(len(l.slots))
	if used > capacity {
		l.producer.active.Store(false)
		panic("bigbuff: edgefanin: producer/consumer cursor invariant violated")
	}
	free := int(capacity - used)
	n := len(values)
	if n > free {
		n = free
	}
	if n == 0 {
		l.producer.active.Store(false)
		return 0
	}

	index := int(tail & l.mask)
	first := len(l.slots) - index
	if first > n {
		first = n
	}
	copy(l.slots[index:index+first], values[:first])
	if first < n {
		copy(l.slots[:n-first], values[first:n])
	}

	tail += uint64(n)
	l.producer.tail = tail
	l.producer.published.Store(tail)

	// Read-only queued loads keep the shared ready bit in Shared state during
	// bursts. Only an idle-to-ready edge performs a contended CAS and stack push.
	if !l.queued.Load() && l.queued.CompareAndSwap(false, true) {
		q.pushReady(l)
		q.signal()
	}

	l.producer.active.Store(false)
	return n
}

// Close prevents future pushes on this lane and waits for an in-flight push.
// Values already accepted remain available to the EdgeFanIn consumer.
func (p *EdgeProducer[V]) Close() {
	if p == nil || p.lane == nil {
		return
	}
	p.lane.close()
}

func (l *edgeLane[V]) close() {
	l.closed.Store(true)
	for spins := 0; l.producer.active.Load(); spins++ {
		if spins&63 == 63 {
			runtime.Gosched()
		}
	}
}

// TryPop returns one value if any producer lane has one ready. It is intended
// for low-latency consumers; Drain is more efficient for batch consumers.
// Ordering is FIFO within each producer lane; inter-lane selection is
// intentionally unspecified.
func (q *EdgeFanIn[V]) TryPop() (V, bool) {
	var one [1]V
	if q.Drain(one[:]) == 0 {
		var zero V
		return zero, false
	}
	return one[0], true
}

// Drain appends up to len(dst) values to dst and returns the number written.
// It amortizes cursor loads/stores across each lane batch and performs no
// allocation. A zero-length destination is a no-op.
func (q *EdgeFanIn[V]) Drain(dst []V) int {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn")
	}
	written := 0
	for written < len(dst) {
		l := q.current
		if l == nil {
			if q.pending == nil {
				q.pending = q.ready.Swap(nil)
			}
			l = q.pending
			if l == nil {
				return written
			}
			// Capture next before re-arm can reuse this intrusive node.
			q.pending = l.next.Load()
			q.current = l
			q.budget = edgeBatchSize
		}

		published := l.producer.published.Load()
		head := l.consumer.head
		available := published - head
		if available == 0 {
			q.rearm(l)
			q.current = nil
			continue
		}
		if available > uint64(len(l.slots)) {
			panic("bigbuff: edgefanin: published cursor invariant violated")
		}

		n := len(dst) - written
		if n > int(available) {
			n = int(available)
		}
		if n > int(q.budget) {
			n = int(q.budget)
		}

		index := int(head & l.mask)
		first := len(l.slots) - index
		if first > n {
			first = n
		}
		copy(dst[written:written+first], l.slots[index:index+first])
		clear(l.slots[index : index+first])
		if first < n {
			copy(dst[written+first:written+n], l.slots[:n-first])
			clear(l.slots[:n-first])
		}

		head += uint64(n)
		l.consumer.head = head
		l.consumer.consumed.Store(head)
		q.budget -= uint32(n)
		written += n

		// Retain this lane as current when dst filled before the lane batch did.
		// Otherwise rotate it back to the ready stack after a bounded quantum.
		if q.budget == 0 || head == published {
			q.rearm(l)
			q.current = nil
		}
	}
	return written
}

// Next blocks until a value is available, ctx is cancelled, or Close has
// stopped all producers and the queue has been drained. Buffered values are
// returned before a cancellation is reported. A nil context means Background.
// The bool is false only when the queue is fully closed and drained; on context
// cancellation, Next returns ctx.Err().
func (q *EdgeFanIn[V]) Next(ctx context.Context) (value V, ok bool, err error) {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if value, ok = q.TryPop(); ok {
			return value, true, nil
		}
		if q.closed.Load() {
			return value, false, nil
		}
		select {
		case <-ctx.Done():
			return value, false, ctx.Err()
		case <-q.wake:
		case <-q.done:
		}
	}
}

// Close rejects new pushes, waits for all admitted pushes to finish, then
// wakes blocked Next calls. It does not discard already accepted values and
// starts no teardown goroutines. Close is idempotent and safe for concurrent
// callers.
func (q *EdgeFanIn[V]) Close() {
	if q == nil || q.wake == nil {
		panic("bigbuff: edgefanin: must use NewEdgeFanIn")
	}

	q.mu.Lock()
	if q.closing.Load() {
		done := q.done
		q.mu.Unlock()
		<-done
		return
	}
	q.closing.Store(true)
	lanes := q.lanes
	q.mu.Unlock()

	for _, l := range lanes {
		l.close()
	}

	q.closed.Store(true)
	close(q.done)
}

func (q *EdgeFanIn[V]) pushReady(l *edgeLane[V]) {
	for {
		head := q.ready.Load()
		l.next.Store(head)
		if q.ready.CompareAndSwap(head, l) {
			return
		}
	}
}

// rearm closes the lost-wakeup window around clearing queued. If a producer
// published while queued was true, the post-clear published check puts the
// lane back. If it publishes later, that producer wins the false->true edge.
func (q *EdgeFanIn[V]) rearm(l *edgeLane[V]) {
	l.queued.Store(false)
	if l.consumer.head != l.producer.published.Load() &&
		l.queued.CompareAndSwap(false, true) {
		q.pushReady(l) // the consumer is already active; no wake token is needed
	}
}

func (q *EdgeFanIn[V]) signal() {
	select {
	case q.wake <- struct{}{}:
	default: // one buffered token coalesces arbitrarily many ready edges
	}
}
-- go.mod --
module bigbuff

go 1.23
-- README.md --
# bigbuff: EdgeFanIn

`EdgeFanIn[V]` is a specialized, bounded M:1 ingress primitive for many
independent producer goroutines feeding one consumer. It is designed for
telemetry, ingestion, and work-dispatch paths where per-producer FIFO ordering
matters, a single global order does not, and dropping the newest item when a
producer's lane is full is acceptable.

It is not intended to replace Go channels generally. The optimized envelope is
**4–64+ continuously active producers, one consumer, bounded per-producer
buffers, and batch-oriented reads/writes**. For one producer, small messages,
or code that needs `select` on send, channel closure semantics, or a global FIFO,
use a channel.

## API and semantics

```go
q := bigbuff.NewEdgeFanIn[Message]()

// Create one lane per producer goroutine. Capacity rounds up to a power of 2.
p, err := q.NewProducer(1024)
if err != nil {
    return err
}

// Producer goroutine: one goroutine per Producer value.
if !p.TryPush(msg) {
    // Full or closed. This is drop-newest; older accepted messages remain.
}

// Prefer batch publication where the producer naturally has bursts.
n := p.TryPushBatch(batch) // accepts the largest prefix that fits

// Consumer goroutine: use Drain to amortize cursor operations.
var batch [128]Message
for {
    n := q.Drain(batch[:])
    if n == 0 {
        msg, ok, err := q.Next(ctx)
        if err != nil {
            return err
        }
        if !ok { // Close has completed and all accepted values were drained.
            return nil
        }
        handle(msg)
        continue
    }
    for _, msg := range batch[:n] {
        handle(msg)
    }
}

// Stop producers first or let this close them; accepted data is preserved.
q.Close()
```

### Contract

- One goroutine may call `TryPush` / `TryPushBatch` on a given producer handle.
  Concurrent calls on the same handle panic. Different handles are concurrent.
- Exactly one consumer goroutine may call `TryPop`, `Drain`, or `Next`. These
  consumer methods must not be used concurrently with each other.
- FIFO is guaranteed within each lane. Ordering between different producers is
  intentionally unspecified; there is no global sequence counter.
- `TryPush` returns false on either full or closed. `TryPushBatch` returns the
  accepted prefix length; a short result means the suffix was not accepted.
- Values are copied shallowly. If `V` contains pointers, slices, maps, or other
  references, callers must not mutate the referenced data concurrently with
  queue access. Ownership of the value itself transfers on successful push.
- Each lane is independently bounded. Total storage is approximately the sum
  of rounded lane capacities times `sizeof(V)`, plus lane metadata.
- `Producer.Close` rejects new values on that lane and waits for an admitted
  push. `EdgeFanIn.Close` rejects new producers and pushes, waits for all
  in-flight pushes, then wakes blocked `Next` calls. Already accepted values
  remain drainable. Both closes are idempotent.
- The implementation starts no goroutines. `Next` parks the caller directly
  using a coalescing, one-element notification channel and the caller's context.

## Mechanical-sympathy thesis

A Go channel is an excellent general-purpose rendezvous and queue. It must also
support general multi-sender/multi-receiver behavior, blocking sends, close,
select integration, and scheduler interaction. In the target envelope, those
capabilities are broader than required.

`EdgeFanIn` specializes the topology:

1. **Per-producer SPSC rings.** A producer never reserves a global ticket and
   never CASes a shared tail for each message. Its write cursor is private; it
   reads the consumer cursor to check free space and publishes a new cursor
   after writing payload bytes.
2. **Batch cursor publication.** `TryPushBatch` copies a prefix and performs
   one publication store. `Drain` obtains a producer cursor once, copies a
   bounded run, clears consumed references, and publishes the consumer cursor
   once. These operations amortize atomics over many values.
3. **Edge-queued lane scheduling.** A lane contributes one intrusive node to a
   shared MPSC ready stack only on an idle-to-ready transition. It is not
   enqueued once per message. The single consumer detaches the stack with one
   atomic swap and processes up to 32 values per lane before rotating it.
4. **Lost-wakeup-safe rearming.** The consumer clears a lane's `queued` bit and
   rechecks its published cursor. A producer publishes before checking that
   bit. Whichever side observes the other side's transition is responsible for
   re-queuing the lane. This closes the clear/check race without a global lock.
5. **Coalesced parking.** Producers send a non-blocking token only when a lane
   becomes newly ready. A capacity-one channel coalesces many lane transitions
   and is used only for the blocking `Next` path; the data path does not send
   one notification per message.
6. **Separated cursors.** Producer and consumer hot cursors sit in separate
   padded regions. Payload slots are contiguous and indexed with a power-of-two
   mask. Consumed values are cleared so pointer-bearing payloads are not retained
   until the next overwrite.

The distinctive contribution is the combination of per-writer cursor rings,
edge-triggered intrusive readiness, a race-safe rearm protocol, and batch
publication under explicit lossy M:1 semantics. This is a specialized design,
not a claim that SPSC rings or MPSC intrusive stacks are individually new.

No `unsafe`, assembly, cgo, runtime internals, or race-detector exceptions are
used. The cursor protocol uses Go atomics to publish payload writes before a
consumer reads them and to publish slot reuse before a producer overwrites
those slots. The race detector passes on the supported API usage pattern.

## Correctness and lifecycle checks

The tests cover:

- drop-newest behavior and power-of-two capacity rounding;
- FIFO ordering within every producer lane, including ring wrap under
  concurrent batch producers and a draining consumer;
- accepted-value accounting (every successful push is delivered exactly once);
- concurrent queue shutdown, concurrent `Close` calls, and producer admission
  racing with shutdown;
- blocked `Next` wakeup, context cancellation, and draining after close;
- a zero-allocation single-value hot path.

Run:

```sh
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench BenchmarkEdgeFanIn -benchmem -benchtime=1s
```

## Benchmark observations

A local 1-second benchmark run was performed with Go 1.23.2 on Linux/amd64,
AMD EPYC 9V74. It compares the full M-producer/1-consumer path against a
buffered Go channel. Queue producer capacities were 1024 each; channel capacity
was `producerCount * 1024`. The edge benchmark uses `Drain` with batches up to
64 values. `128B` means a value containing a 128-byte array. All listed paths
reported `0 B/op` and `0 allocs/op`.

| Payload | Producers | EdgeFanIn `TryPush` + `Drain` | Buffered channel |
|---|---:|---:|---:|
| `uint64` | 1 | 58.90 ns/op | 36.17 ns/op |
| `uint64` | 4 | 29.70 ns/op | 36.63 ns/op |
| `uint64` | 16 | 21.67 ns/op | 47.01 ns/op |
| 128 B | 1 | 94.88 ns/op | 33.40 ns/op |
| 128 B | 4 | 50.07 ns/op | 45.09 ns/op |
| 128 B | 16 | 27.09 ns/op | 54.92 ns/op |

The batch-32 `uint64` path measured 5.52–5.79 ns/message for 1, 4, and 16
producers in the same run. This is amortized throughput, not single-message
latency; the channel baseline sends individual values. It illustrates why the
batch API is central to the intended workload.

These are one host's measurements, not a universal ranking. In particular,
EdgeFanIn loses for the one-producer case, and the 4-producer 128-byte case is
slightly behind the channel. Benchmark on the deployment CPU, Go version,
`GOMAXPROCS`, payload shape, and actual batching pattern before adopting it.

````
