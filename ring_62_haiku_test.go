package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

const ringTestMagic = 0x9E3779B97F4A7C15

type ringPayload62Haiku struct {
	Seq   uint64
	Check uint64
	Tag   [6]byte
}

func newRingPayload(seq uint64) ringPayload62Haiku {
	p := ringPayload62Haiku{Seq: seq, Check: seq * ringTestMagic}
	for i := range p.Tag {
		p.Tag[i] = byte(seq) + byte(i)
	}
	return p
}

// intact detects torn payloads: every field is a function of Seq.
func (p ringPayload62Haiku) intact() bool {
	if p.Check != p.Seq*ringTestMagic {
		return false
	}
	for i, b := range p.Tag {
		if b != byte(p.Seq)+byte(i) {
			return false
		}
	}
	return true
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func TestRingInOrder(t *testing.T) {
	ctx := context.Background()
	r := NewRing62Haiku[uint64](8)
	c := r.Subscribe()
	for i := range uint64(5) {
		r.Publish(i)
	}
	for want := range uint64(5) {
		got, dropped, err := c.Next(ctx)
		if err != nil || dropped != 0 || got != want {
			t.Fatalf("got (%d, %d, %v), want (%d, 0, nil)", got, dropped, err, want)
		}
	}
	if lag := c.Lag(); lag != 0 {
		t.Fatalf("lag = %d, want 0", lag)
	}
}

func TestRingLapAccounting(t *testing.T) {
	const capacity, total = 8, 20
	r := NewRing62Haiku[uint64](capacity)
	c := r.Subscribe()
	for i := range uint64(total) {
		r.Publish(i)
	}
	var dropped uint64
	var got []uint64
	for c.Lag() > 0 {
		v, d, err := c.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		dropped += d
		got = append(got, v)
	}
	if dropped != total-capacity {
		t.Fatalf("dropped = %d, want %d", dropped, total-capacity)
	}
	if len(got) != capacity {
		t.Fatalf("delivered %d entries, want %d", len(got), capacity)
	}
	for i, v := range got {
		if want := uint64(total - capacity + i); v != want {
			t.Fatalf("entry %d = %d, want %d", i, v, want)
		}
	}
}

func TestRingReplayAndLatest(t *testing.T) {
	ctx := context.Background()
	r := NewRing62Haiku[uint64](8)
	for i := range uint64(20) {
		r.Publish(i)
	}
	rc := r.SubscribeReplay() // retained window is 12..19
	if v, d, err := rc.Next(ctx); err != nil || v != 12 || d != 0 {
		t.Fatalf("replay first = (%d, %d, %v), want (12, 0, nil)", v, d, err)
	}

	lc := r.Subscribe() // starts at 20
	for i := range uint64(10) {
		r.Publish(20 + i)
	}
	if v, d, ok := lc.Latest(); !ok || v != 29 || d != 9 {
		t.Fatalf("Latest = (%d, %d, %v), want (29, 9, true)", v, d, ok)
	}
	if _, _, ok := lc.Latest(); ok {
		t.Fatal("Latest returned a value with nothing new published")
	}
	r.Publish(30)
	if v, d, ok := lc.Latest(); !ok || v != 30 || d != 0 {
		t.Fatalf("Latest after publish = (%d, %d, %v), want (30, 0, true)", v, d, ok)
	}
}

func TestRingDrain(t *testing.T) {
	r := NewRing62Haiku[uint64](16)
	c := r.Subscribe()
	for i := range uint64(10) {
		r.Publish(i)
	}
	buf := make([]uint64, 4)
	var all []uint64
	for {
		n, dropped := c.Drain(buf)
		if dropped != 0 {
			t.Fatalf("unexpected drops: %d", dropped)
		}
		if n == 0 {
			break
		}
		all = append(all, buf[:n]...)
	}
	if len(all) != 10 {
		t.Fatalf("drained %d entries, want 10", len(all))
	}
	for i, v := range all {
		if v != uint64(i) {
			t.Fatalf("entry %d = %d, want %d", i, v, i)
		}
	}
}

func TestRingRejectsPointerTypes(t *testing.T) {
	cases := map[string]func(){
		"string":    func() { NewRing62Haiku[string](4) },
		"slice":     func() { NewRing62Haiku[[]byte](4) },
		"pointer":   func() { NewRing62Haiku[*int](4) },
		"map":       func() { NewRing62Haiku[map[int]int](4) },
		"interface": func() { NewRing62Haiku[any](4) },
		"nested":    func() { NewRing62Haiku[struct{ S string }](4) },
		"array":     func() { NewRing62Haiku[[2]*int](4) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic for a pointerful element type")
				}
			}()
			fn()
		})
	}
}

type ringOddLayout62Haiku struct {
	A byte
	B uint16
	C [3]byte
}

type ringNineBytes62Haiku [9]byte

func roundTrip[V comparable](t *testing.T, in []V) {
	t.Helper()
	r := NewRing62Haiku[V](len(in))
	c := r.Subscribe()
	for _, v := range in {
		if !r.Publish(v) {
			t.Fatal("publish failed")
		}
	}
	for i, want := range in {
		got, dropped, err := c.Next(context.Background())
		if err != nil || dropped != 0 || got != want {
			t.Fatalf("entry %d: got (%v, %d, %v), want %v", i, got, dropped, err, want)
		}
	}
}

func TestRingRoundTripLayouts(t *testing.T) {
	roundTrip(t, []struct{}{struct{}{}, struct{}{}, struct{}{}})                                   // size 0
	roundTrip(t, []byte{1, 2, 3})                                                                  // sub-word
	roundTrip(t, []ringOddLayout62Haiku{{1, 2, [3]byte{3, 4, 5}}, {255, 65535, [3]byte{9, 8, 7}}}) // tail word
	roundTrip(t, []ringNineBytes62Haiku{{1, 2, 3, 4, 5, 6, 7, 8, 9}, {9, 9, 9, 9, 9, 9, 9, 9, 0}})
	roundTrip(t, []complex128{1 + 2i, -3.5i})
	roundTrip(t, []ringPayload62Haiku{newRingPayload(1), newRingPayload(1 << 40)})
}

func TestRingCloseSemantics(t *testing.T) {
	ctx := context.Background()
	r := NewRing62Haiku[int](4)
	c := r.Subscribe()
	r.Publish(7)
	r.Close()
	if r.Publish(8) {
		t.Fatal("Publish succeeded after Close")
	}
	if v, _, err := c.Next(ctx); err != nil || v != 7 {
		t.Fatalf("pre-close entry = (%d, %v), want (7, nil)", v, err)
	}
	if _, _, err := c.Next(ctx); !errors.Is(err, ErrClosed62Haiku) {
		t.Fatalf("after drain: err = %v, want ErrClosed62Haiku", err)
	}
	r.Close() // idempotent
}

func TestRingCloseWakesParkedConsumer(t *testing.T) {
	r := NewRing62Haiku[int](4)
	c := r.Subscribe()
	errc := make(chan error, 1)
	go func() {
		_, _, err := c.Next(context.Background())
		errc <- err
	}()
	waitUntil(t, "consumer to park", func() bool { return r.waiters.Load() == 1 })
	r.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed62Haiku) {
			t.Fatalf("err = %v, want ErrClosed62Haiku", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not unpark the consumer")
	}
	waitUntil(t, "parked count to drop", func() bool { return r.waiters.Load() == 0 })
}

func TestRingContextCancelUnparks(t *testing.T) {
	r := NewRing62Haiku[int](4)
	c := r.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, _, err := c.Next(ctx)
		errc <- err
	}()
	waitUntil(t, "consumer to park", func() bool { return r.waiters.Load() == 1 })
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled consumer did not unpark")
	}
	waitUntil(t, "parked count to drop", func() bool { return r.waiters.Load() == 0 })
	r.Close()
}

// TestRingStressLateJoinersAndLaps exercises the strongest invariant: for every cursor,
// each delivered entry's Seq equals the running position (start + all prior dropped +
// delivered). This proves no torn reads, no duplicates, no reordering, and exact loss
// accounting, with late joiners, slow consumers that lap, and frequent parking.
func TestRingStressLateJoinersAndLaps(t *testing.T) {
	total := uint64(100_000)
	if testing.Short() {
		total = 10_000
	}
	const joins = 8
	joinEvery := total / joins

	r := NewRing62Haiku[ringPayload62Haiku](64)
	ctx := context.Background()
	errs := make(chan error, joins)
	var wg sync.WaitGroup

	consume := func(id int, c *Cursor62Haiku[ringPayload62Haiku], start uint64, slow bool) {
		defer wg.Done()
		next := start
		var got uint64
		for {
			p, dropped, err := c.Next(ctx)
			next += dropped
			if errors.Is(err, ErrClosed62Haiku) {
				break
			}
			if err != nil {
				errs <- err
				return
			}
			if !p.intact() || p.Seq != next {
				errs <- fmt.Errorf("consumer %d: got seq %d intact=%v, want seq %d", id, p.Seq, p.intact(), next)
				return
			}
			next++
			got++
			if slow && got%256 == 0 {
				// Lap stimulant (not a readiness wait): the slow lane must
				// regularly fall a full capacity behind so the ring overwrites
				// entries before it reads them.
				time.Sleep(100 * time.Microsecond)
			}
		}
		if next != total {
			errs <- fmt.Errorf("consumer %d: accounted up to %d, want %d", id, next, total)
		}
	}

	for i := range total {
		if i%joinEvery == 0 {
			id := int(i / joinEvery)
			c := r.Subscribe() // taken on the publisher goroutine: starts exactly at position i
			wg.Add(1)
			go consume(id, c, i, id%2 == 0)
		}
		r.Publish(newRingPayload(i))
	}
	r.Close()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("%d consumers still registered as parked", w)
	}
}

// TestRingSteadyStateZeroAlloc verifies the hot path (publish + read with data pending)
// does not allocate.
func TestRingSteadyStateZeroAlloc(t *testing.T) {
	r := NewRing62Haiku[ringPayload62Haiku](256)
	c := r.Subscribe()
	p := newRingPayload(3)
	allocs := testing.AllocsPerRun(1000, func() {
		r.Publish(p)
		if _, _, ok := c.TryNext(); !ok {
			t.Error("expected a pending entry")
		}
	})
	if allocs != 0 {
		t.Fatalf("publish+read allocates %v times per operation", allocs)
	}
}
