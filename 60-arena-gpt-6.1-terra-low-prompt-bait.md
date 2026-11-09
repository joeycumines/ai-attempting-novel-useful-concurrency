The implementation below targets a deliberately narrow envelope: **latest-value telemetry encoded in 32 bits**, with many independent readers and one or more publishers. Readers poll; they do not register, block, or receive every update.

The distinguishing specialization is that the value, revision, and closed flag share **one atomic word**. A read is one atomic load; publication is one CAS loop. There are no allocations or goroutines on the hot path. This avoids channel rendezvous, per-subscriber bookkeeping, and wakeup storms, but deliberately does **not** provide event delivery or blocking notification.

This is a proposed design, not a demonstrated hardware-ceiling winner: benchmark results depend on the machine and workload. Revision exhaustion is explicit rather than silently allowing ABA.

### `latest32.go`

```go
package bigbuff

import "sync/atomic"

const (
	latest32ValueMask  uint64 = 1<<32 - 1
	latest32RevisionMax       = 1<<31 - 1
	latest32ClosedBit  uint64 = 1 << 63
)

// Latest32 is a nonblocking, lossy latest-value register.
//
// Its zero value is ready to use. It must not be copied after first use.
//
// A successful Publish replaces the current value and increments its revision.
// Every successful publication has a distinct revision. Readers may skip any
// number of intermediate publications.
//
// Close is linearizable with Publish. A publication that wins the CAS before
// Close succeeds; one that loses to Close returns PublishClosed. Close retains
// the final value and revision. No goroutines or resources require teardown.
//
// The finite revision space is intentional: after 2^31-1 successful publishes,
// further publishes return PublishExhausted instead of wrapping and creating
// an ABA ambiguity. Replace the register when this limit is reached.
//
// Latest32 makes no fairness or bounded-latency guarantee for competing
// publishers. Its polling API does not park goroutines or support select.
type Latest32 struct {
	word atomic.Uint64
}

// PublishResult describes the outcome of a publication.
type PublishResult uint8

const (
	Published PublishResult = iota
	PublishClosed
	PublishExhausted
)

// Snapshot is one coherent observation of a Latest32.
//
// Revision zero means that no value has yet been published. Value is zero in
// that initial state, but a published value of zero is valid.
type Snapshot struct {
	Value    uint32
	Revision uint32
	Closed   bool
}

// Load returns a linearizable snapshot.
func (x *Latest32) Load() Snapshot {
	return decodeLatest32(x.word.Load())
}

// Publish attempts to publish value.
//
// On success it returns the revision assigned to the publication. On failure,
// the revision is zero. A register that is both closed and exhausted reports
// PublishClosed.
func (x *Latest32) Publish(value uint32) (revision uint32, result PublishResult) {
	old := x.word.Load()
	for {
		if old&latest32ClosedBit != 0 {
			return 0, PublishClosed
		}

		rev := uint32(old >> 32)
		if rev == latest32RevisionMax {
			return 0, PublishExhausted
		}

		nextRev := rev + 1
		next := uint64(nextRev)<<32 | uint64(value)
		if x.word.CompareAndSwap(old, next) {
			return nextRev, Published
		}
		old = x.word.Load()
	}
}

// Close prevents subsequent publications and preserves the last value.
// It returns true only for the call that transitions the register to closed.
func (x *Latest32) Close() bool {
	old := x.word.Load()
	for {
		if old&latest32ClosedBit != 0 {
			return false
		}
		if x.word.CompareAndSwap(old, old|latest32ClosedBit) {
			return true
		}
		old = x.word.Load()
	}
}

func decodeLatest32(word uint64) Snapshot {
	return Snapshot{
		Value:    uint32(word & latest32ValueMask),
		Revision: uint32((word &^ latest32ClosedBit) >> 32),
		Closed:   word&latest32ClosedBit != 0,
	}
}

// Latest32Cursor tracks observations for one reader. It must not be used
// concurrently without external synchronization.
//
// A fresh cursor reports an initial closed state, but not the initial open,
// unpublished state. Poll reports a publication or closure once, then reports
// it again only if the revision or closed state changes. The caller can compute
// skipped publications from the difference between successive revisions.
//
// A cursor is tied by the caller to one Latest32 for its lifetime; do not reuse
// it with another register without resetting it to its zero value.
type Latest32Cursor struct {
	revision uint32
	closed   bool
}

// Poll returns the current snapshot and whether it differs from the cursor's
// previous observation in revision or closed state.
func (c *Latest32Cursor) Poll(x *Latest32) (snapshot Snapshot, changed bool) {
	snapshot = x.Load()
	changed = snapshot.Revision != c.revision || snapshot.Closed != c.closed
	c.revision = snapshot.Revision
	c.closed = snapshot.Closed
	return
}
```

### `latest32_test.go`

```go
package bigbuff

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLatest32CursorAndClose(t *testing.T) {
	var x Latest32
	var c Latest32Cursor

	if s, changed := c.Poll(&x); changed || s.Revision != 0 || s.Closed {
		t.Fatalf("initial: %+v changed=%v", s, changed)
	}
	if rev, result := x.Publish(0); rev != 1 || result != Published {
		t.Fatalf("publish zero: rev=%d result=%v", rev, result)
	}
	if s, changed := c.Poll(&x); !changed || s.Value != 0 || s.Revision != 1 {
		t.Fatalf("first: %+v changed=%v", s, changed)
	}
	x.Publish(10)
	x.Publish(20)
	if s, changed := c.Poll(&x); !changed || s.Value != 20 || s.Revision != 3 {
		t.Fatalf("skipped: %+v changed=%v", s, changed)
	}
	if _, changed := c.Poll(&x); changed {
		t.Fatal("duplicate observation")
	}
	if !x.Close() || x.Close() {
		t.Fatal("Close must transition exactly once")
	}
	if s, changed := c.Poll(&x); !changed || !s.Closed || s.Value != 20 {
		t.Fatalf("closed: %+v changed=%v", s, changed)
	}
	if _, changed := c.Poll(&x); changed {
		t.Fatal("duplicate closed observation")
	}
	if _, result := x.Publish(30); result != PublishClosed {
		t.Fatalf("publish after close: %v", result)
	}
}

func TestLatest32Exhaustion(t *testing.T) {
	var x Latest32
	// Set up the boundary directly; exercising billions of publishes is
	// unnecessary to test the arithmetic and state transition.
	x.word.Store(uint64(latest32RevisionMax-1)<<32 | 7)

	if rev, result := x.Publish(9); rev != latest32RevisionMax || result != Published {
		t.Fatalf("last publish: %d %v", rev, result)
	}
	if _, result := x.Publish(10); result != PublishExhausted {
		t.Fatalf("expected exhaustion, got %v", result)
	}
	if !x.Close() {
		t.Fatal("close exhausted register")
	}
	if _, result := x.Publish(11); result != PublishClosed {
		t.Fatalf("closed takes precedence: %v", result)
	}
	s := x.Load()
	if s.Value != 9 || s.Revision != latest32RevisionMax || !s.Closed {
		t.Fatalf("final: %+v", s)
	}
}

func TestLatest32ConcurrentPublishers(t *testing.T) {
	const producers, each = 16, 4000
	var x Latest32
	revisions := make(chan uint32, producers*each)

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				rev, result := x.Publish(uint32(p*each + i))
				if result != Published {
					t.Errorf("publish: %v", result)
					return
				}
				revisions <- rev
			}
		}(p)
	}
	wg.Wait()
	close(revisions)

	seen := make([]bool, producers*each+1)
	for rev := range revisions {
		if rev == 0 || int(rev) >= len(seen) || seen[rev] {
			t.Fatalf("invalid or duplicate revision %d", rev)
		}
		seen[rev] = true
	}
	for rev := 1; rev < len(seen); rev++ {
		if !seen[rev] {
			t.Fatalf("missing revision %d", rev)
		}
	}
	if s := x.Load(); s.Revision != producers*each {
		t.Fatalf("final: %+v", s)
	}
}

func TestLatest32CoherentReaders(t *testing.T) {
	const count = 100000
	var x Latest32
	var readers sync.WaitGroup

	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var last uint32
			for {
				s := x.Load()
				// This test's sole publisher writes value == revision.
				if s.Revision < last || s.Value != s.Revision {
					t.Errorf("incoherent or reversed snapshot: %+v after %d", s, last)
					return
				}
				last = s.Revision
				if s.Closed {
					if s.Revision != count {
						t.Errorf("premature close: %+v", s)
					}
					return
				}
				runtime.Gosched()
			}
		}()
	}
	for i := uint32(1); i <= count; i++ {
		if rev, result := x.Publish(i); result != Published || rev != i {
			t.Fatalf("publish %d: %d %v", i, rev, result)
		}
	}
	x.Close()
	readers.Wait()
}

func TestLatest32PublishCloseRace(t *testing.T) {
	for round := 0; round < 100; round++ {
		var x Latest32
		var wg sync.WaitGroup
		var successes atomic.Uint32

		for p := 0; p < 8; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					_, result := x.Publish(1)
					if result == PublishClosed {
						return
					}
					if result != Published {
						t.Errorf("unexpected result: %v", result)
						return
					}
					successes.Add(1)
				}
			}()
		}
		runtime.Gosched()
		x.Close()
		wg.Wait()

		s := x.Load()
		if !s.Closed || s.Revision != successes.Load() {
			t.Fatalf("final %+v, successes %d", s, successes.Load())
		}
	}
}

// mutexLatest is a comparable latest-value register, not an event queue.
// It intentionally implements only the operations needed for the benchmark.
type mutexLatest struct {
	mu    sync.Mutex
	value uint32
	rev   uint32
}

func (m *mutexLatest) publish(v uint32) {
	m.mu.Lock()
	m.rev++
	m.value = v
	m.mu.Unlock()
}

func (m *mutexLatest) load() (uint32, uint32) {
	m.mu.Lock()
	v, r := m.value, m.rev
	m.mu.Unlock()
	return v, r
}

// Each parallel worker independently performs one write per writeEvery
// operations. Thus the ratio is approximate during startup but does not
// introduce an extra shared atomic counter into the measurement.
//
// Benchmark runs are restricted below the finite revision limit.
func BenchmarkLatest32(b *testing.B) {
	for _, writeEvery := range []int{1, 8, 128} {
		b.Run(fmt.Sprintf("atomic/writeEvery=%d", writeEvery), func(b *testing.B) {
			var x Latest32
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var n uint32
				for pb.Next() {
					n++
					if n%uint32(writeEvery) == 0 {
						x.Publish(n)
					} else {
						x.Load()
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})

		b.Run(fmt.Sprintf("mutex/writeEvery=%d", writeEvery), func(b *testing.B) {
			var x mutexLatest
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var n uint32
				for pb.Next() {
					n++
					if n%uint32(writeEvery) == 0 {
						x.publish(n)
					} else {
						x.load()
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}
```

Run with:

```sh
go test ./...
go test -race ./...
go test -run '^$' -bench '^BenchmarkLatest32$' -benchmem -cpu 1,2,4,8 ./...
```

The benchmark compares the supported **32-bit payload** only; larger payloads require a different representation and cannot be inferred from these results. Keep individual benchmark runs below the revision limit—otherwise the atomic benchmark starts measuring rejected publishes. The tests exercise key invariants and close races, but are not an exhaustive linearizability proof or a tail-latency measurement.
