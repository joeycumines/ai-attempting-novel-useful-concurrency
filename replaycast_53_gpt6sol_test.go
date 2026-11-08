package bigbuff

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestReplayCast53LossAccounting(t *testing.T) {
	x := NewReplayCast53Gpt6Sol[int](3)
	sub, err := x.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	if err := x.PublishBatch([]int{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-sub.Ready():
	default:
		t.Fatal("Ready did not signal already-buffered publications")
	}
	for i, want := range []struct {
		value  int
		missed uint64
	}{{3, 2}, {4, 0}, {5, 0}} {
		got, missed, err := sub.Next()
		if err != nil || got != want.value || missed != want.missed {
			t.Fatalf("Next #%d = (%d,%d,%v), want (%d,%d,nil)", i, got, missed, err, want.value, want.missed)
		}
	}
	ready := sub.Ready()
	select {
	case <-ready:
		t.Fatal("Ready stayed signalled after the cursor caught up")
	default:
	}
	if err := x.Publish(6); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	default:
		t.Fatal("publication did not close the prior notification generation")
	}
	if got, missed, err := sub.Next(); err != nil || got != 6 || missed != 0 {
		t.Fatalf("Next after notification = (%d,%d,%v), want (6,0,nil)", got, missed, err)
	}
	if _, _, err := sub.Next(); !errors.Is(err, ErrReplayEmpty53Gpt6Sol) {
		t.Fatalf("caught-up Next error = %v, want ErrReplayEmpty", err)
	}
	sub.Unsubscribe()
}

func TestReplayCast53EdgesAndConcurrency(t *testing.T) {
	// Publications with no active cursor advance the sequence but leave no
	// stale ring values for a later subscriber.
	x := NewReplayCast53Gpt6Sol[int](2)
	for _, value := range []int{10, 11, 12} {
		if err := x.Publish(value); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := x.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Publish(13); err != nil {
		t.Fatal(err)
	}
	if got, missed, err := sub.Next(); err != nil || got != 13 || missed != 0 {
		t.Fatalf("late cursor got (%d,%d,%v), want (13,0,nil)", got, missed, err)
	}

	// Concurrent publishers are serialized into a unique sequence; with more
	// capacity than publications, every value is readable exactly once.
	concurrent := NewReplayCast53Gpt6Sol[int](512)
	reader, err := concurrent.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	const publishers, perPublisher = 4, 100
	var wg sync.WaitGroup
	errCh := make(chan error, publishers)
	for publisher := 0; publisher < publishers; publisher++ {
		publisher := publisher
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				if err := concurrent.Publish(publisher*perPublisher + i); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	concurrent.Close()
	seen := make(map[int]bool, publishers*perPublisher)
	for i := 0; i < publishers*perPublisher; i++ {
		value, missed, err := reader.Next()
		if err != nil || missed != 0 {
			t.Fatalf("concurrent Next #%d = (%d,%d,%v), want a value with no misses", i, value, missed, err)
		}
		if seen[value] {
			t.Fatalf("duplicate concurrent publication %d", value)
		}
		seen[value] = true
	}
	if _, _, err := reader.Next(); !errors.Is(err, ErrReplayClosed53Gpt6Sol) {
		t.Fatalf("drained Next error = %v, want ErrReplayClosed", err)
	}
	if _, err := concurrent.Subscribe(); !errors.Is(err, ErrReplayClosed53Gpt6Sol) {
		t.Fatalf("Subscribe after Close error = %v, want ErrReplayClosed", err)
	}
	if err := concurrent.Publish(1); !errors.Is(err, ErrReplayClosed53Gpt6Sol) {
		t.Fatalf("Publish after Close error = %v, want ErrReplayClosed", err)
	}
	concurrent.Close() // idempotent
	reader.Unsubscribe()

	// A subscription added after an idle history gap starts from the current
	// sequence, so stale cells cannot be mistaken for catch-up values.
	if _, _, err := sub.Next(); !errors.Is(err, ErrReplayEmpty53Gpt6Sol) {
		t.Fatalf("late cursor unexpectedly observed stale values: %v", err)
	}
	sub.Unsubscribe()
	select {
	case <-sub.Done():
	default:
		t.Fatal("Done did not close on Unsubscribe")
	}
	if _, _, err := sub.Next(); !errors.Is(err, ErrReplayUnsubscribed53Gpt6Sol) {
		t.Fatalf("Next after Unsubscribe = %v", err)
	}
	sub.Unsubscribe() // idempotent

	// Exhaustion reserves MaxUint64 so next=seq+1 never wraps to zero.
	boundary := NewReplayCast53Gpt6Sol[int](1)
	boundary.seq = math.MaxUint64 - 1
	last, err := boundary.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	if err := boundary.Publish(1); !errors.Is(err, ErrReplayExhausted53Gpt6Sol) {
		t.Fatalf("Publish at sequence boundary error = %v, want ErrReplayExhausted", err)
	}
	if _, _, err := last.Next(); !errors.Is(err, ErrReplayEmpty53Gpt6Sol) {
		t.Fatalf("boundary Next error = %v, want ErrReplayEmpty", err)
	}
	boundary.Close()
	if _, _, err := last.Next(); !errors.Is(err, ErrReplayClosed53Gpt6Sol) {
		t.Fatalf("boundary Next after Close error = %v, want ErrReplayClosed", err)
	}
	last.Unsubscribe()

	var zero ReplayCast53Gpt6Sol[int]
	mustPanic(t, func() { _, _ = zero.Subscribe() })
}

func mustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	f()
}

func BenchmarkReplayCast53Publish(b *testing.B) {
	x := NewReplayCast53Gpt6Sol[int](64)
	sub, err := x.Subscribe()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if err := x.Publish(i); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := x.Publish(i); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sub.Unsubscribe()
}

func FuzzReplayCast53RingModel(f *testing.F) {
	f.Add([]byte{3, 0, 0, 1, 2, 0, 5, 1, 0})
	f.Add([]byte{0, 0, 1, 2, 0, 2, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 128 {
			return
		}
		capacity := int(ops[0]%8) + 1
		x := NewReplayCast53Gpt6Sol[int](capacity)
		sub, err := x.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		var published []int
		var nextValue = 1
		cursor := 0
		for _, op := range ops[1:] {
			if op%3 == 0 {
				count := int(op%5) + 1
				batch := make([]int, count)
				for i := range batch {
					batch[i] = nextValue
					published = append(published, nextValue)
					nextValue++
				}
				if err := x.PublishBatch(batch); err != nil {
					t.Fatal(err)
				}
				continue
			}

			oldest := len(published) - capacity
			if oldest < 0 {
				oldest = 0
			}
			missed := 0
			if cursor < oldest {
				missed = oldest - cursor
				cursor = oldest
			}
			if cursor == len(published) {
				if _, _, err := sub.Next(); !errors.Is(err, ErrReplayEmpty53Gpt6Sol) {
					t.Fatalf("empty model Next error = %v", err)
				}
				continue
			}
			want := published[cursor]
			cursor++
			got, gotMissed, err := sub.Next()
			if err != nil || got != want || gotMissed != uint64(missed) {
				t.Fatalf("Next = (%d,%d,%v), model=(%d,%d,nil), capacity=%d cursor=%d published=%v", got, gotMissed, err, want, missed, capacity, cursor, published)
			}
		}

		// Drain the model tail and verify the final empty result.
		for cursor < len(published) {
			oldest := len(published) - capacity
			if oldest < 0 {
				oldest = 0
			}
			missed := 0
			if cursor < oldest {
				missed = oldest - cursor
				cursor = oldest
			}
			want := published[cursor]
			cursor++
			got, gotMissed, err := sub.Next()
			if err != nil || got != want || gotMissed != uint64(missed) {
				t.Fatalf("drain Next = (%d,%d,%v), model=(%d,%d,nil)", got, gotMissed, err, want, missed)
			}
		}
		if _, _, err := sub.Next(); !errors.Is(err, ErrReplayEmpty53Gpt6Sol) {
			t.Fatalf("final Next error = %v, want ErrReplayEmpty", err)
		}
	})
}
