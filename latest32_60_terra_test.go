package bigbuff

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLatest32CursorAndClose(t *testing.T) {
	var x Latest3260Terra
	var c Latest32Cursor60Terra

	if s, changed := c.Poll(&x); changed || s.Revision != 0 || s.Closed {
		t.Fatalf("initial: %+v changed=%v", s, changed)
	}
	if rev, result := x.Publish(0); rev != 1 || result != Published60Terra {
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
	if _, result := x.Publish(30); result != PublishClosed60Terra {
		t.Fatalf("publish after close: %v", result)
	}
}

func TestLatest32Exhaustion(t *testing.T) {
	var x Latest3260Terra
	// Set up the boundary directly; exercising billions of publishes is
	// unnecessary to test the arithmetic and state transition.
	x.word.Store(uint64(latest3260TerraRevisionMax-1)<<32 | 7)

	if rev, result := x.Publish(9); rev != latest3260TerraRevisionMax || result != Published60Terra {
		t.Fatalf("last publish: %d %v", rev, result)
	}
	if _, result := x.Publish(10); result != PublishExhausted60Terra {
		t.Fatalf("expected exhaustion, got %v", result)
	}
	if !x.Close() {
		t.Fatal("close exhausted register")
	}
	if _, result := x.Publish(11); result != PublishClosed60Terra {
		t.Fatalf("closed takes precedence: %v", result)
	}
	s := x.Load()
	if s.Value != 9 || s.Revision != latest3260TerraRevisionMax || !s.Closed {
		t.Fatalf("final: %+v", s)
	}
}

func TestLatest32ConcurrentPublishers(t *testing.T) {
	const producers, each = 16, 4000
	var x Latest3260Terra
	revisions := make(chan uint32, producers*each)

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				rev, result := x.Publish(uint32(p*each + i))
				if result != Published60Terra {
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
	var x Latest3260Terra
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
		if rev, result := x.Publish(i); result != Published60Terra || rev != i {
			t.Fatalf("publish %d: %d %v", i, rev, result)
		}
	}
	x.Close()
	readers.Wait()
}

func TestLatest32PublishCloseRace(t *testing.T) {
	for round := 0; round < 100; round++ {
		var x Latest3260Terra
		var wg sync.WaitGroup
		var successes atomic.Uint32

		for p := 0; p < 8; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					_, result := x.Publish(1)
					if result == PublishClosed60Terra {
						return
					}
					if result != Published60Terra {
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

// mutexLatest60Terra is a comparable latest-value register, not an event queue.
// It intentionally implements only the operations needed for the benchmark.
type mutexLatest60Terra struct {
	mu    sync.Mutex
	value uint32
	rev   uint32
}

func (m *mutexLatest60Terra) publish(v uint32) {
	m.mu.Lock()
	m.rev++
	m.value = v
	m.mu.Unlock()
}

func (m *mutexLatest60Terra) load() (uint32, uint32) {
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
			var x Latest3260Terra
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
			var x mutexLatest60Terra
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
