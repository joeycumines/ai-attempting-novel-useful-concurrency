package bigbuff

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"
)

func TestRingCaster55LapResumeAndConflation(t *testing.T) {
	rc := NewRingCaster55Haiku[int](4)
	live := rc.Subscribe(new(RingSub55Haiku[int]))
	for i := 0; i < 10; i++ {
		rc.Publish(i)
	}
	for want := 6; want < 10; want++ {
		got, ok := live.TryRecv()
		if !ok || got != want {
			t.Fatalf("live TryRecv = (%d,%v), want (%d,true)", got, ok, want)
		}
	}
	if got := live.Dropped(); got != 6 {
		t.Fatalf("live dropped %d, want 6", got)
	}
	if _, ok := live.TryRecv(); ok {
		t.Fatal("live cursor should be caught up")
	}

	resume := rc.SubscribeFrom(new(RingSub55Haiku[int]), 0)
	if got := resume.Dropped(); got != 6 {
		t.Fatalf("initial resume gap = %d, want 6", got)
	}
	for want := 6; want < 10; want++ {
		got, ok := resume.TryRecv()
		if !ok || got != want {
			t.Fatalf("resume TryRecv = (%d,%v), want (%d,true)", got, ok, want)
		}
	}
	if got := resume.Dropped(); got != 6 {
		t.Fatalf("resume dropped %d, want 6", got)
	}

	latest := rc.SubscribeFrom(new(RingSub55Haiku[int]), 10)
	for i := 10; i < 13; i++ {
		rc.Publish(i)
	}
	if got, ok := latest.TryRecvLatest(); !ok || got != 12 {
		t.Fatalf("TryRecvLatest = (%d,%v), want (12,true)", got, ok)
	}
	if got := latest.Dropped(); got != 0 {
		t.Fatalf("intentional conflation counted as loss: %d", got)
	}
	if _, ok := latest.TryRecvLatest(); ok {
		t.Fatal("TryRecvLatest returned a duplicate")
	}
}

func TestRingCaster55SelectWakeAndClose(t *testing.T) {
	rc := NewRingCaster55Haiku[string](8)
	sub := rc.Subscribe(new(RingSub55Haiku[string]))
	armed := make(chan struct{})
	got := make(chan string, 1)
	ended := make(chan error, 1)
	go func() {
		for {
			if value, ok := sub.TryRecv(); ok {
				got <- value
				continue
			}
			wake := sub.Arm()
			if value, ok := sub.TryRecv(); ok {
				got <- value
				continue
			}
			if sub.Ended() {
				ended <- ErrRingClosed55Haiku
				return
			}
			armed <- struct{}{}
			<-wake
		}
	}()

	// The goroutine signals after Arm and the mandatory re-check, so this
	// tests the race boundary without timing sleeps.
	select {
	case <-armed:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver failed to arm")
	}
	rc.Publish("hello")
	select {
	case value := <-got:
		if value != "hello" {
			t.Fatalf("received %q, want hello", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("armed receiver missed a publication")
	}
	select {
	case <-armed:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver failed to re-arm")
	}
	rc.Close()
	select {
	case err := <-ended:
		if err != ErrRingClosed55Haiku {
			t.Fatalf("ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close failed to wake the armed receiver")
	}
	if !sub.Ended() {
		t.Fatal("Ended should remain true after drain")
	}
	if _, err := sub.Recv(context.Background()); err != ErrRingClosed55Haiku {
		t.Fatalf("Recv after Close = %v, want ErrRingClosed", err)
	}
	rc.Close()
	mustPanic(t, func() { rc.Publish("after close") })
}

func TestRingCaster55ConcurrentPublicationAndAccounting(t *testing.T) {
	const publishers, perPublisher = 4, 100
	rc := NewRingCaster55Haiku[int](512)
	sub := rc.Subscribe(new(RingSub55Haiku[int]))
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				rc.Publish(p*perPublisher + i)
			}
		}()
	}
	wg.Wait()
	rc.Close()

	seen := make(map[int]bool, publishers*perPublisher)
	for i := 0; i < publishers*perPublisher; i++ {
		value, err := sub.Recv(context.Background())
		if err != nil {
			t.Fatalf("Recv #%d: %v", i, err)
		}
		if seen[value] {
			t.Fatalf("duplicate value %d", value)
		}
		seen[value] = true
	}
	if uint64(len(seen))+sub.Dropped() != publishers*perPublisher {
		t.Fatalf("received %d + dropped %d != publications %d", len(seen), sub.Dropped(), publishers*perPublisher)
	}
	if sub.Dropped() != 0 {
		t.Fatalf("unexpected drops with adequate capacity: %d", sub.Dropped())
	}
	if _, err := sub.Recv(context.Background()); err != ErrRingClosed55Haiku {
		t.Fatalf("drained Recv = %v, want ErrRingClosed", err)
	}
}

func TestRingCaster55ZeroValueAndSequenceBoundary(t *testing.T) {
	var zero RingCaster55Haiku[int]
	mustPanic(t, func() { zero.Publish(1) })
	mustPanic(t, func() { zero.Subscribe(new(RingSub55Haiku[int])) })
	mustPanic(t, func() { NewRingCaster55Haiku[int](0) })

	rc := NewRingCaster55Haiku[int](1)
	rc.tail.Store(math.MaxUint64)
	mustPanic(t, func() { rc.Publish(1) })
	if rc.tail.Load() != math.MaxUint64 {
		t.Fatal("sequence exhaustion wrapped the tail")
	}
}

func TestRingCaster55LateArmAfterCloseDoesNotPark(t *testing.T) {
	rc := NewRingCaster55Haiku[int](4)
	sub := rc.Subscribe(new(RingSub55Haiku[int]))
	rc.Close()
	wake := sub.Arm()
	select {
	case <-wake:
	default:
		t.Fatal("Arm after Close returned an open channel")
	}
	if !sub.Ended() {
		t.Fatal("new subscription after Close should be ended")
	}
	if ch, _ := rc.wake.Load().(chan struct{}); ch != nil {
		t.Fatal("late Arm left an unclosed wake channel retained")
	}
}

func TestRingCaster55SubscribeFromClampsToRetainedHead(t *testing.T) {
	rc := NewRingCaster55Haiku[int](4)
	for value := 0; value < 10; value++ {
		rc.Publish(value)
	}
	sub := rc.SubscribeFrom(new(RingSub55Haiku[int]), 0)
	if sub.Seq() != 6 || sub.Dropped() != 6 {
		t.Fatalf("resume cursor=%d dropped=%d, want 6/6", sub.Seq(), sub.Dropped())
	}
	if value, ok := sub.TryRecv(); !ok || value != 6 {
		t.Fatalf("first retained value=(%d,%v), want (6,true)", value, ok)
	}
}

func TestRingCaster55AllocationProfile(t *testing.T) {
	writer := NewRingCaster55Haiku[int](256)
	if got := testing.AllocsPerRun(100, func() {
		for i := 0; i < 256; i++ {
			writer.Publish(i)
		}
	}); got != 1 {
		t.Fatalf("slab-backed Publish allocations per 256 values = %v, want 1", got)
	}

	var sub RingSub55Haiku[int]
	if got := testing.AllocsPerRun(100, func() { writer.Subscribe(&sub) }); got != 0 {
		t.Fatalf("caller-owned Subscribe allocations = %v, want 0", got)
	}

	readerCaster := NewRingCaster55Haiku[int](256)
	for i := 0; i < 101; i++ {
		readerCaster.Publish(i)
	}
	reader := readerCaster.SubscribeFrom(new(RingSub55Haiku[int]), 0)
	if got := testing.AllocsPerRun(100, func() {
		if _, ok := reader.TryRecv(); !ok {
			panic("unexpected empty receive")
		}
	}); got != 0 {
		t.Fatalf("TryRecv fast-path allocations = %v, want 0", got)
	}
}

func BenchmarkRingCaster55Publish(b *testing.B) {
	rc := NewRingCaster55Haiku[int](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc.Publish(i)
	}
}

func BenchmarkRingCaster55TryRecv(b *testing.B) {
	const batch = 1024
	rc := NewRingCaster55Haiku[int](batch)
	sub := rc.Subscribe(new(RingSub55Haiku[int]))
	b.ReportAllocs()
	b.ResetTimer()
	published := 0
	for start := 0; start < b.N; start += batch {
		count := min(batch, b.N-start)
		b.StopTimer()
		for i := 0; i < count; i++ {
			rc.Publish(published)
			published++
		}
		sub = rc.SubscribeFrom(sub, uint64(published-count))
		b.StartTimer()
		for i := 0; i < count; i++ {
			if _, ok := sub.TryRecv(); !ok {
				b.Fatalf("TryRecv stopped at batch offset %d", i)
			}
		}
	}
	b.StopTimer()
}

func FuzzRingCaster55Model(f *testing.F) {
	f.Add([]byte{3, 0, 1, 0, 2, 1, 0, 0, 2})
	f.Add([]byte{0, 0, 0, 1, 2, 2, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 128 {
			return
		}
		capacity := 1 << (ops[0] % 4)
		rc := NewRingCaster55Haiku[int](capacity)
		capacity = len(rc.ring) // constructor rounds up to a power of two
		sub := rc.Subscribe(new(RingSub55Haiku[int]))
		var published []int
		cursor, dropped := 0, uint64(0)
		for i, op := range ops[1:] {
			if op%3 == 0 {
				value := i + 1
				rc.Publish(value)
				published = append(published, value)
				continue
			}
			if op%3 == 2 {
				got, ok := sub.TryRecvLatest()
				if cursor == len(published) {
					if ok {
						t.Fatalf("TryRecvLatest unexpectedly returned %d", got)
					}
					continue
				}
				cursor = len(published) - 1
				want := published[cursor]
				cursor++
				if !ok || got != want || sub.Dropped() != dropped {
					t.Fatalf("latest=(%d,%v), want %d; dropped=%d want %d", got, ok, want, sub.Dropped(), dropped)
				}
				continue
			}

			oldest := len(published) - capacity
			if oldest < 0 {
				oldest = 0
			}
			if cursor < oldest {
				dropped += uint64(oldest - cursor)
				cursor = oldest
			}
			got, ok := sub.TryRecv()
			if cursor == len(published) {
				if ok || sub.Dropped() != dropped {
					t.Fatalf("empty TryRecv=(%d,%v), dropped=%d want %d", got, ok, sub.Dropped(), dropped)
				}
				continue
			}
			want := published[cursor]
			cursor++
			if !ok || got != want || sub.Dropped() != dropped {
				t.Fatalf("TryRecv=(%d,%v), want %d; dropped=%d want %d", got, ok, want, sub.Dropped(), dropped)
			}
		}
	})
}
