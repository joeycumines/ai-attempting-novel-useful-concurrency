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
	"testing"
	"time"
	"unsafe"
)

// ringMix67Haiku is a splitmix64-style finaliser used to build self-checking payloads.
func ringMix67Haiku(x, salt uint64) uint64 {
	x += salt * 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// ringEvent67Haiku is a 16-byte self-checking payload. A torn copy almost certainly fails Check.
type ringEvent67Haiku struct {
	Producer uint32
	Local    uint32
	Check    uint64
}

func newRingEvent67Haiku(p, l uint32) ringEvent67Haiku {
	return ringEvent67Haiku{Producer: p, Local: l, Check: ringMix67Haiku(uint64(p)<<32|uint64(l), 1)}
}

func (e ringEvent67Haiku) decode() (uint32, uint32, bool) {
	return e.Producer, e.Local, e.Check == ringMix67Haiku(uint64(e.Producer)<<32|uint64(e.Local), 1)
}

// ringWide67Haiku is a 64-byte payload that spans eight words, so a torn copy has many more ways to
// go wrong than ringEvent67Haiku. It is also too big for lane striping.
type ringWide67Haiku struct {
	Producer uint32
	Local    uint32
	Words    [6]uint64
	Check    uint64
}

func newRingWide67Haiku(p, l uint32) ringWide67Haiku {
	k := uint64(p)<<32 | uint64(l)
	w := ringWide67Haiku{Producer: p, Local: l, Check: ringMix67Haiku(k, 99)}
	for i := range w.Words {
		w.Words[i] = ringMix67Haiku(k, uint64(i))
	}
	return w
}

func (w ringWide67Haiku) decode() (uint32, uint32, bool) {
	k := uint64(w.Producer)<<32 | uint64(w.Local)
	if w.Check != ringMix67Haiku(k, 99) {
		return 0, 0, false
	}
	for i, word := range w.Words {
		if word != ringMix67Haiku(k, uint64(i)) {
			return 0, 0, false
		}
	}
	return w.Producer, w.Local, true
}

// waitUntil polls cond until it holds, failing the test after a generous deadline.

func TestRingRejectsUnsupportedElements(t *testing.T) {
	type withString struct {
		A int
		B string
	}
	type withIface struct{ A any }
	cases := map[string]func(){
		"string":       func() { NewRing67Haiku[string](4) },
		"pointer":      func() { NewRing67Haiku[*int](4) },
		"slice":        func() { NewRing67Haiku[[]int](4) },
		"map":          func() { NewRing67Haiku[map[int]int](4) },
		"interface":    func() { NewRing67Haiku[any](4) },
		"func":         func() { NewRing67Haiku[func()](4) },
		"chan":         func() { NewRing67Haiku[chan int](4) },
		"unsafe ptr":   func() { NewRing67Haiku[unsafe.Pointer](4) },
		"nested ptr":   func() { NewRing67Haiku[withString](4) },
		"iface field":  func() { NewRing67Haiku[withIface](4) },
		"oversize":     func() { NewRing67Haiku[[65]byte](4) },
		"zero cap":     func() { NewRing67Haiku[uint64](0) },
		"negative cap": func() { NewRing67Haiku[uint64](-1) },
		"huge cap":     func() { NewRing67Haiku[uint64](ringMaxCapacity67Haiku + 1) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { mustPanic(t, fn) })
	}
}

func TestRingAcceptsSupportedElements(t *testing.T) {
	type padded struct {
		A uint8
		_ [3]byte
		B float32
		C [2]complex64
	}
	NewRing67Haiku[uint8](1)
	NewRing67Haiku[padded](3)
	NewRing67Haiku[[8]uint64](5) // exactly 64 bytes
	NewRing67Haiku[[7]byte](2)   // size not a multiple of 8
	NewRing67Haiku[struct{}](2)  // zero-sized
	NewRing67Haiku[ringWide67Haiku](16)
}

func TestRingSequentialAndReplay(t *testing.T) {
	ctx := context.Background()
	r := NewRing67Haiku[uint64](8)
	if r.Cap() != 8 {
		t.Fatalf("Cap() = %d, want 8", r.Cap())
	}
	for i := range 5 {
		seq, err := r.Publish(uint64(i))
		if err != nil || seq != uint64(i) {
			t.Fatalf("Publish %d = (%d, %v), want (%d, nil)", i, seq, err, i)
		}
	}
	c := r.SubscribeReplay(100) // capped at Cap and at the 5 published
	for i := range 5 {
		v, skipped, err := c.Next(ctx)
		if err != nil || skipped != 0 || v != uint64(i) {
			t.Fatalf("Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, i)
		}
	}

	// Caught up: a cancelled context surfaces immediately with no value.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := c.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("caught-up Next err = %v, want context.Canceled", err)
	}

	// Subscribe starts at the next publish, so it must not replay history.
	fresh := r.Subscribe()
	if _, _, err := fresh.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("fresh subscriber replayed history: err = %v", err)
	}
	if _, err := r.Publish(77); err != nil {
		t.Fatal(err)
	}
	if v, skipped, err := fresh.Next(ctx); err != nil || skipped != 0 || v != 77 {
		t.Fatalf("fresh Next = (%d, %d, %v), want (77, 0, nil)", v, skipped, err)
	}

	// Replay is bounded by the window.
	r2 := NewRing67Haiku[uint64](4)
	for i := range 10 {
		r2.Publish(uint64(i))
	}
	c2 := r2.SubscribeReplay(3)
	for want := uint64(7); want < 10; want++ {
		v, skipped, err := c2.Next(ctx)
		if err != nil || skipped != 0 || v != want {
			t.Fatalf("replay Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, want)
		}
	}
}

func TestRingLappedCursorAccounting(t *testing.T) {
	ctx := context.Background()
	r := NewRing67Haiku[uint64](8)
	c := r.Subscribe() // positioned at ticket 0
	const total = 100
	for i := range total {
		r.Publish(uint64(i))
	}
	// The window is the last 8 tickets, so the first readable ticket is 93 and 93 are skipped.
	want := uint64(total - 8 + 1)
	v, skipped, err := c.Next(ctx)
	if err != nil || v != want || skipped != want {
		t.Fatalf("first lapped Next = (%d, %d, %v), want (%d, %d, nil)", v, skipped, err, want, want)
	}
	for i := want + 1; i < total; i++ {
		v, skipped, err := c.Next(ctx)
		if err != nil || v != i || skipped != 0 {
			t.Fatalf("Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, i)
		}
	}
}

func TestRingCloseDrainsThenErrors(t *testing.T) {
	ctx := context.Background()
	r := NewRing67Haiku[uint64](4)
	for i := range 3 {
		r.Publish(uint64(i))
	}
	c := r.SubscribeReplay(4)
	r.Close()
	r.Close() // idempotent
	if _, err := r.Publish(9); !errors.Is(err, ErrRingClosed67Haiku) {
		t.Fatalf("Publish after Close err = %v, want ErrRingClosed67Haiku", err)
	}
	for i := range 3 {
		v, skipped, err := c.Next(ctx)
		if err != nil || skipped != 0 || v != uint64(i) {
			t.Fatalf("drain Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, i)
		}
	}
	if _, _, err := c.Next(ctx); !errors.Is(err, ErrRingClosed67Haiku) {
		t.Fatalf("after drain err = %v, want ErrRingClosed67Haiku", err)
	}
}

func TestRingBlockedNextWakesOnPublish(t *testing.T) {
	r := NewRing67Haiku[uint64](16)
	c := r.Subscribe()

	type result struct {
		v   uint64
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, _, err := c.Next(context.Background())
		done <- result{v, err}
	}()
	waitUntil(t, "cursor to park", func() bool { return r.waiters.Load() == 1 })
	if _, err := r.Publish(42); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.v != 42 {
			t.Fatalf("woken Next = (%d, %v), want (42, nil)", got.v, got.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parked Next was not woken by Publish")
	}
	waitUntil(t, "waiters to drain", func() bool { return r.waiters.Load() == 0 })
}

func TestRingCloseWakesParkedCursors(t *testing.T) {
	r := NewRing67Haiku[uint64](16)
	const cursors = 8
	errs := make(chan error, cursors)
	for range cursors {
		c := r.Subscribe()
		go func() {
			_, _, err := c.Next(context.Background())
			errs <- err
		}()
	}
	waitUntil(t, "all cursors to park", func() bool { return r.waiters.Load() == cursors })
	r.Close()
	for range cursors {
		select {
		case err := <-errs:
			if !errors.Is(err, ErrRingClosed67Haiku) {
				t.Fatalf("parked Next err = %v, want ErrRingClosed67Haiku", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not wake every parked cursor")
		}
	}
}

func TestRingCancelledWaitsDoNotLeak(t *testing.T) {
	r := NewRing67Haiku[uint64](8)
	c := r.Subscribe()
	before := runtime.NumGoroutine()
	for range 200 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _, err := c.Next(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Next err = %v, want DeadlineExceeded", err)
		}
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("waiters = %d after cancelled waits, want 0", w)
	}
	// The ring starts no goroutines of its own, so the count must return to its baseline.
	waitUntil(t, "goroutine count to return to baseline", func() bool {
		return runtime.NumGoroutine() <= before
	})
}

func TestRingLatest(t *testing.T) {
	r := NewRing67Haiku[uint64](4)
	if _, _, ok := r.Latest(); ok {
		t.Fatal("Latest on an empty ring reported a value")
	}
	for i := range 10 {
		r.Publish(uint64(i))
	}
	v, tk, ok := r.Latest()
	if !ok || v != 9 || tk != 9 {
		t.Fatalf("Latest = (%d, %d, %v), want (9, 9, true)", v, tk, ok)
	}
}

func TestRingIndexIsBijectiveAndLaneStriped(t *testing.T) {
	for _, capacity := range []int{1, 2, 4, 8, 64, 1024} {
		for _, striped := range []bool{false, true} {
			r := newRing67Haiku[uint64](capacity, striped)
			perLine := uint64(1) << r.slotBits
			lanes := r.n / perLine
			seen := make([]bool, r.n)
			for tk := range r.n {
				idx := r.index(tk)
				if idx >= r.n || seen[idx] {
					t.Fatalf("cap=%d striped=%v: index(%d)=%d is out of range or duplicated", capacity, striped, tk, idx)
				}
				seen[idx] = true
				if r.index(tk+r.n) != idx {
					t.Fatalf("cap=%d striped=%v: tickets %d and %d map to different slots", capacity, striped, tk, tk+r.n)
				}
				if striped && lanes >= 2 && (idx>>r.slotBits) == (r.index(tk+1)>>r.slotBits) && tk+1 < r.n {
					t.Fatalf("cap=%d: consecutive tickets %d and %d share a lane", capacity, tk, tk+1)
				}
			}
		}
	}
	// Elements too big for a line get no striping.
	if r := newRing67Haiku[ringWide67Haiku](64, true); r.slotBits != 0 {
		t.Fatalf("72-byte slot striped with %d slots per lane", 1<<r.slotBits)
	}
}

func TestRingPayloadRoundTrip(t *testing.T) {
	type shape struct {
		A uint8
		B int16
		C [3]uint32
		D float64
		E complex64
	}
	testRingRoundTrip(t, shape{1, -2, [3]uint32{3, 4, 5}, 6.5, 7 + 8i}, shape{A: 255, D: -1})
	testRingRoundTrip(t, [8]uint64{1, 2, 3, 4, 5, 6, 7, 8}, [8]uint64{})
	testRingRoundTrip(t, struct{ X [7]byte }{[7]byte{1, 2, 3, 4, 5, 6, 7}}, struct{ X [7]byte }{})
	testRingRoundTrip(t, uint8(200), uint8(1))
	testRingRoundTrip(t, struct{}{}, struct{}{})
	testRingRoundTrip(t, newRingWide67Haiku(9, 9), newRingWide67Haiku(1, 2))
}

func testRingRoundTrip[V comparable](t *testing.T, values ...V) {
	t.Helper()
	ctx := context.Background()
	r := NewRing67Haiku[V](4)
	for _, v := range values {
		if _, err := r.Publish(v); err != nil {
			t.Fatal(err)
		}
	}
	c := r.SubscribeReplay(len(values))
	for i, want := range values {
		got, skipped, err := c.Next(ctx)
		if err != nil || skipped != 0 || got != want {
			t.Fatalf("%T value %d: got (%v, %d, %v), want (%v, 0, nil)", want, i, got, skipped, err, want)
		}
	}
}

func TestRingPublishIsAllocationFree(t *testing.T) {
	if ringRaceEnabled67Haiku {
		t.Skip("the race detector instruments and allocates")
	}
	r := NewRing67Haiku[ringEvent67Haiku](64)
	ev := newRingEvent67Haiku(1, 2)
	if allocs := testing.AllocsPerRun(10000, func() { r.Publish(ev) }); allocs != 0 {
		t.Fatalf("Publish allocated %v times per call, want 0", allocs)
	}
	// Next on a cursor with data available is also allocation-free.
	big := NewRing67Haiku[ringEvent67Haiku](1 << 14)
	for range 1 << 13 {
		big.Publish(ev)
	}
	c := big.SubscribeReplay(1 << 13)
	ctx := context.Background()
	if allocs := testing.AllocsPerRun(4000, func() {
		if _, _, err := c.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Next allocated %v times per call, want 0", allocs)
	}
}

// runRingStress67Haiku runs several producers against a deliberately small ring with fast and slow
// cursors. It checks that:
//   - every payload is intact (no torn reads);
//   - each producer's values arrive in the order it published them;
//   - tickets are dense and unique across producers;
//   - for every cursor, delivered + skipped equals the total number of tickets;
//   - Latest never goes backwards.
func runRingStress67Haiku[V any](t *testing.T, mk func(p, l uint32) V, decode func(V) (uint32, uint32, bool)) {
	t.Helper()
	producers := max(2, runtime.GOMAXPROCS(0))
	perProducer := 150_000
	switch {
	case ringRaceEnabled67Haiku:
		perProducer = 15_000
	case testing.Short():
		perProducer = 5_000
	}
	total := producers * perProducer
	r := NewRing67Haiku[V](256)

	const fast, slow = 2, 2
	cursors := make([]*RingCursor67Haiku[V], fast+slow)
	for i := range cursors {
		cursors[i] = r.Subscribe() // all positioned at ticket 0, before any publish
	}

	type report struct {
		delivered, skipped, reordered, corrupt uint64
		err                                    error
	}
	reports := make([]report, len(cursors))
	var cwg sync.WaitGroup
	for i := range cursors {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			c := cursors[i]
			slowCursor := i >= fast
			last := make([]int64, producers)
			for p := range last {
				last[p] = -1
			}
			var rep report
			for {
				v, skipped, err := c.Next(context.Background())
				rep.skipped += skipped
				if err != nil {
					rep.err = err
					break
				}
				p, l, ok := decode(v)
				if !ok || int(p) >= producers {
					rep.corrupt++
					continue
				}
				if int64(l) <= last[p] {
					rep.reordered++
				}
				last[p] = int64(l)
				rep.delivered++
				if slowCursor && rep.delivered%256 == 0 {
					time.Sleep(50 * time.Microsecond) // fall behind so the ring laps us
				}
			}
			reports[i] = rep
		}()
	}

	stopWatch := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	go func() {
		defer watchWG.Done()
		var last uint64
		seenAny := false
		for {
			select {
			case <-stopWatch:
				return
			default:
			}
			v, tk, ok := r.Latest()
			if !ok {
				continue
			}
			if _, _, valid := decode(v); !valid {
				t.Errorf("Latest returned a corrupt value at ticket %d", tk)
			}
			if seenAny && tk < last {
				t.Errorf("Latest went backwards: %d after %d", tk, last)
			}
			last, seenAny = tk, true
		}
	}()

	seen := make([]atomic.Bool, total)
	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			lastTicket := int64(-1)
			for l := range perProducer {
				tk, err := r.Publish(mk(uint32(p), uint32(l)))
				if err != nil {
					t.Errorf("producer %d: Publish: %v", p, err)
					return
				}
				if int64(tk) <= lastTicket {
					t.Errorf("producer %d: ticket %d after %d", p, tk, lastTicket)
				}
				lastTicket = int64(tk)
				if tk >= uint64(total) || seen[tk].Swap(true) {
					t.Errorf("ticket %d duplicated or out of range", tk)
				}
			}
		}()
	}
	pwg.Wait()
	close(stopWatch)
	watchWG.Wait()
	r.Close()
	cwg.Wait()

	var totalSkipped uint64
	for i, rep := range reports {
		if !errors.Is(rep.err, ErrRingClosed67Haiku) {
			t.Errorf("cursor %d: terminal err = %v, want ErrRingClosed67Haiku", i, rep.err)
		}
		if rep.corrupt != 0 {
			t.Errorf("cursor %d: %d corrupt payloads (torn reads)", i, rep.corrupt)
		}
		if rep.reordered != 0 {
			t.Errorf("cursor %d: %d per-producer ordering violations", i, rep.reordered)
		}
		if got := rep.delivered + rep.skipped; got != uint64(total) {
			t.Errorf("cursor %d: delivered %d + skipped %d = %d, want %d",
				i, rep.delivered, rep.skipped, got, total)
		}
		totalSkipped += rep.skipped
		t.Logf("cursor %d (slow=%v): delivered %d, skipped %d", i, i >= fast, rep.delivered, rep.skipped)
	}
	if totalSkipped == 0 {
		t.Error("no cursor was ever lapped; the stress test did not exercise overwrite")
	}
	for tk := range seen {
		if !seen[tk].Load() {
			t.Fatalf("ticket %d was never issued", tk)
		}
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("waiters = %d after shutdown, want 0", w)
	}
}

func TestRingStressSmallPayload(t *testing.T) {
	runRingStress67Haiku(t, newRingEvent67Haiku, ringEvent67Haiku.decode)
}

func TestRingStressLargePayload(t *testing.T) {
	runRingStress67Haiku(t, newRingWide67Haiku, ringWide67Haiku.decode)
}

// TestRingChurn mixes cancelled waits, cursor creation and teardown, and close while publishers run.
func TestRingChurn(t *testing.T) {
	r := NewRing67Haiku[ringEvent67Haiku](64)
	const publishers = 4
	perPublisher := 50_000
	if ringRaceEnabled67Haiku {
		perPublisher = 5_000
	}

	var cwg sync.WaitGroup
	var badValues atomic.Uint64
	for range 8 {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				c := r.Subscribe() // a fresh cursor each round, then discarded
				for range 4 {
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
					v, _, err := c.Next(ctx)
					cancel()
					if errors.Is(err, ErrRingClosed67Haiku) {
						return
					}
					if err == nil {
						if _, _, ok := v.decode(); !ok {
							badValues.Add(1)
						}
					}
				}
			}
		}()
	}

	var pwg sync.WaitGroup
	for p := range publishers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for l := range perPublisher {
				if _, err := r.Publish(newRingEvent67Haiku(uint32(p), uint32(l))); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}()
	}
	pwg.Wait()
	r.Close()
	cwg.Wait()

	if n := badValues.Load(); n != 0 {
		t.Fatalf("%d corrupt values observed under churn", n)
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("waiters = %d after churn, want 0", w)
	}
}
