package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRing_Basic(t *testing.T) {
	r := NewTelemetryRing63Qwen[int](4, false)

	_, ok := r.Latest()
	if ok {
		t.Fatal("expected false on empty ring")
	}

	r.Publish(10)
	v, ok := r.Latest()
	if !ok || v != 10 {
		t.Fatalf("expected 10, got %v", v)
	}

	r.Publish(20)
	r.Publish(30)
	r.Publish(40)
	r.Publish(50) // Overwrites 10 (capacity 4, mask 3)

	var cursor uint64
	v, ok = r.Next(&cursor)
	if !ok || v != 50 {
		t.Fatalf("expected 50 (fast-forwarded), got %v", v)
	}
	// The ring's cursor is a next-ticket: after delivering sequence 5 the
	// cursor reads 6 (the next sequence to deliver). The delivered test
	// expected 5, which does not match the implementation's cursor =
	// last-delivered + 1 postcondition (and Next's own *cursor = c + 1).
	if cursor != 6 {
		t.Fatalf("expected cursor 6, got %d", cursor)
	}
}

func TestRing_ConcurrentStress(t *testing.T) {
	r := NewTelemetryRing63Qwen[int](1024, true)

	var wg sync.WaitGroup
	var published atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 100000; i++ {
			r.Publish(i)
			published.Add(1)
		}
	}()

	var maxSeen atomic.Int64
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var cursor uint64
			for {
				v, ok := r.Next(&cursor)
				if ok {
					if int64(v) > maxSeen.Load() {
						maxSeen.Store(int64(v))
					}
					if v == 100000 {
						return
					}
				} else {
					runtime.Gosched()
				}
			}
		}()
	}

	wg.Wait()
	if maxSeen.Load() != 100000 {
		t.Fatalf("consumers missed final value, max seen: %d", maxSeen.Load())
	}
}

func TestRing_Subscribe(t *testing.T) {
	r := NewTelemetryRing63Qwen[int](1024, true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		expected := 1
		for v := range r.Subscribe(ctx, 0) {
			if v != expected {
				t.Errorf("expected %d, got %d", expected, v)
				return
			}
			expected++
			if v == 1000 {
				return
			}
		}
	}()

	for i := 1; i <= 1000; i++ {
		r.Publish(i)
	}

	wg.Wait()
}

func BenchmarkRing_Publish(b *testing.B) {
	r := NewTelemetryRing63Qwen[int](1024, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Publish(i)
	}
}

func BenchmarkRing_Latest(b *testing.B) {
	r := NewTelemetryRing63Qwen[int](1024, false)
	r.Publish(42)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Latest()
	}
}

// TestRing_HeadTailAttach covers Head, added in the completion phase: a
// cursor of Head()+1 skips everything already published, where a zero cursor
// replays the retained history.
func TestRing_HeadTailAttach(t *testing.T) {
	r := NewTelemetryRing63Qwen[int](8, false)
	if h := r.Head(); h != 0 {
		t.Fatalf("Head of an empty ring = %d, want 0", h)
	}
	for v := 1; v <= 5; v++ {
		r.Publish(v)
	}
	if h := r.Head(); h != 5 {
		t.Fatalf("Head after 5 publishes = %d, want 5", h)
	}
	tail := r.Head() + 1
	if v, ok := r.Next(&tail); ok {
		t.Fatalf("a tail-attached cursor read %d before anything new was published", v)
	}
	replay := uint64(0)
	if v, ok := r.Next(&replay); !ok || v != 1 {
		t.Fatalf("a zero cursor read (%d, %v), want the first retained value (1, true)", v, ok)
	}
	r.Publish(6)
	if v, ok := r.Next(&tail); !ok || v != 6 {
		t.Fatalf("the tail-attached cursor read (%d, %v), want (6, true)", v, ok)
	}
	if v, ok := r.Next(&tail); ok {
		t.Fatalf("the tail-attached cursor read %d again", v)
	}
}
