package bigbuff

// Tests written during integration. As extracted the response did not
// compile, and with that fixed its own early-break test deadlocked; these
// tests pin the integration repairs and the recorded design properties.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

type phase72Result struct {
	n   int
	err error
}

func phase72Go(f func() (int, error)) <-chan phase72Result {
	done := make(chan phase72Result, 1)
	go func() {
		n, err := f()
		done <- phase72Result{n, err}
	}()
	return done
}

func phase72Expect(t *testing.T, done <-chan phase72Result, n int, err error, what string) {
	t.Helper()
	select {
	case r := <-done:
		if r.n != n || !errors.Is(r.err, err) {
			t.Fatalf("%s = (%d, %v), want (%d, %v)", what, r.n, r.err, n, err)
		}
	case <-timeAfterGuard():
		t.Fatalf("%s did not return", what)
	}
}

type phase72Sub struct {
	sub Subscription72Gemini[int]
	ch  <-chan int
}

func phase72Subs(t *testing.T, c *PhaseCaster72Gemini[int], n int) []phase72Sub {
	t.Helper()
	subs := make([]phase72Sub, n)
	for i := range subs {
		sub, ch, err := c.SubscribeManaged()
		if err != nil {
			t.Fatalf("SubscribeManaged: %v", err)
		}
		subs[i] = phase72Sub{sub, ch}
	}
	return subs
}

// phase72ReceiveAll receives one value from every subscriber. The caster
// delivers sequentially in slot order, so each subscriber needs its own
// receiver for the duration.
func phase72ReceiveAll(t *testing.T, subs []phase72Sub, want int) {
	t.Helper()
	var wg sync.WaitGroup
	for i, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v := <-s.ch; v != want {
				t.Errorf("subscriber %d received %d, want %d", i, v, want)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "delivery to every subscriber")
}

func phase72Healthy(t *testing.T, c *PhaseCaster72Gemini[int]) {
	t.Helper()
	if s := c.Inspect(); s.Poisoned || s.InFlight != 0 {
		t.Fatalf("caster state %+v: want not poisoned and nothing in flight", s)
	}
}

func TestPhaseCaster72Gemini_BroadcastDeliversToAll(t *testing.T) {
	c := NewPhaseCaster72Gemini[int](0)
	defer c.Close()
	if n, err := c.Broadcast(1); n != 0 || err != nil {
		t.Fatalf("Broadcast with no subscribers = (%d, %v), want (0, nil)", n, err)
	}
	subs := phase72Subs(t, c, 5)
	done := phase72Go(func() (int, error) { return c.Broadcast(7) })
	phase72ReceiveAll(t, subs, 7)
	phase72Expect(t, done, 5, nil, "Broadcast")
	if v, _, ok := c.Latest(); !ok || v != 7 {
		t.Fatalf("Latest = (%d, _, %v), want (7, _, true)", v, ok)
	}
	phase72Healthy(t, c)
}

func TestPhaseCaster72Gemini_PhasedWaitsForEveryAck(t *testing.T) {
	c := NewPhaseCaster72Gemini[int](0)
	defer c.Close()
	subs := phase72Subs(t, c, 3)
	done := phase72Go(func() (int, error) { return c.BroadcastPhased(context.Background(), 9) })
	phase72ReceiveAll(t, subs, 9)
	for _, s := range subs {
		apexStillBlocked(t, done, "BroadcastPhased with acknowledgements outstanding")
		s.sub.Ack()
	}
	phase72Expect(t, done, 3, nil, "BroadcastPhased")
	phase72Healthy(t, c)
}

// Integration repair: Ack issued immediately after the receive could observe
// the slot before the broadcaster recorded the hand-off, which poisoned the
// caster. SeqPhased acknowledges as soon as the (empty) loop body returns.
func TestPhaseCaster72Gemini_ImmediateAckNeverPoisons(t *testing.T) {
	const consumers, perConsumer = 4, 2000
	c := NewPhaseCaster72Gemini[int](0)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var counts [consumers]atomic.Int64
	var wg sync.WaitGroup
	for i := range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range c.SeqPhased(ctx) {
				counts[i].Add(1)
			}
		}()
	}
	behind := func() bool {
		for i := range counts {
			if counts[i].Load() < perConsumer {
				return true
			}
		}
		return false
	}
	// Bounded by delivery progress, not by time: every consumer subscribes
	// eventually and then receives every broadcast.
	for v := 0; behind(); v++ {
		n, err := c.BroadcastPhased(context.Background(), v)
		if err != nil || n > consumers {
			t.Fatalf("BroadcastPhased = (%d, %v)", n, err)
		}
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "consumers")
	phase72Healthy(t, c)
	if s := c.Inspect(); s.Active != 0 {
		t.Fatalf("Active = %d after every consumer returned, want 0", s.Active)
	}
}

// Integration repair: a sender deadline during the acknowledgement wait marks
// the slot as owing one late Ack; the marker used to be erased when the next
// broadcast claimed the slot, so the late Ack then poisoned the caster or was
// taken for the new broadcast's acknowledgement.
func TestPhaseCaster72Gemini_LateAckAfterSenderDeadline(t *testing.T) {
	for range 500 {
		c := NewPhaseCaster72Gemini[int](0)
		s := phase72Subs(t, c, 1)[0]

		ctx, cancel := context.WithCancel(context.Background())
		first := phase72Go(func() (int, error) { return c.BroadcastPhased(ctx, 1) })
		if v := <-s.ch; v != 1 {
			t.Fatalf("received %d, want 1", v)
		}
		apexStillBlocked(t, first, "BroadcastPhased before its deadline")
		cancel()
		select {
		case r := <-first:
			if !errors.Is(r.err, context.Canceled) {
				t.Fatalf("BroadcastPhased = (%d, %v), want context.Canceled", r.n, r.err)
			}
		case <-timeAfterGuard():
			t.Fatal("BroadcastPhased ignored its deadline")
		}

		// The late Ack races the next broadcast's claim of the slot.
		second := phase72Go(func() (int, error) { return c.BroadcastPhased(context.Background(), 2) })
		s.sub.Ack() // late: belongs to the abandoned broadcast
		if v := <-s.ch; v != 2 {
			t.Fatalf("received %d, want 2", v)
		}
		apexStillBlocked(t, second, "BroadcastPhased after a late acknowledgement")
		s.sub.Ack()
		phase72Expect(t, second, 1, nil, "BroadcastPhased")
		phase72Healthy(t, c)
		c.Close()
	}
}

// Integration repair: Close during the acknowledgement wait used to make
// BroadcastPhased report success.
func TestPhaseCaster72Gemini_CloseDuringAckWait(t *testing.T) {
	c := NewPhaseCaster72Gemini[int](0)
	s := phase72Subs(t, c, 1)[0]
	done := phase72Go(func() (int, error) { return c.BroadcastPhased(context.Background(), 1) })
	if v := <-s.ch; v != 1 {
		t.Fatalf("received %d, want 1", v)
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case r := <-done:
		if !errors.Is(r.err, ErrClosed72Gemini) {
			t.Fatalf("BroadcastPhased = (%d, %v), want ErrClosed72Gemini", r.n, r.err)
		}
	case <-timeAfterGuard():
		t.Fatal("Close did not release the acknowledgement wait")
	}
	apexAwait(t, closed, "Close")

	if _, err := c.Broadcast(2); !errors.Is(err, ErrClosed72Gemini) {
		t.Fatalf("Broadcast after Close = %v, want ErrClosed72Gemini", err)
	}
	if _, _, err := c.SubscribeManaged(); !errors.Is(err, ErrClosed72Gemini) {
		t.Fatalf("SubscribeManaged after Close = %v, want ErrClosed72Gemini", err)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("Done not closed")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Integration repair: Seq() consumers never acknowledge, so as generated a
// BroadcastPhased with a Seq() consumer attached never returned. Seq()
// subscriptions now take one-phase delivery inside a phased broadcast.
func TestPhaseCaster72Gemini_SeqConsumerUnderPhasedBroadcast(t *testing.T) {
	const want = 200
	c := NewPhaseCaster72Gemini[int](0)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seen atomic.Int64
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		last := -1
		for v := range c.Seq(ctx) {
			if v <= last {
				t.Errorf("Seq yielded %d after %d", v, last)
				return
			}
			last = v
			seen.Add(1)
		}
	}()
	for v := 0; seen.Load() < want; v++ {
		done := phase72Go(func() (int, error) { return c.BroadcastPhased(context.Background(), v) })
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("BroadcastPhased: %v", r.err)
			}
		case <-timeAfterGuard():
			t.Fatal("BroadcastPhased waited for an acknowledgement a Seq consumer cannot give")
		}
	}
	cancel()
	apexAwait(t, consumed, "Seq consumer")
	phase72Healthy(t, c)
}

// Breaking out of SeqPhased while holding an unacknowledged value must
// release the broadcaster (the response's own test of this deadlocked).
func TestPhaseCaster72Gemini_SeqPhasedBreakCompensates(t *testing.T) {
	for range 50 {
		c := NewPhaseCaster72Gemini[int](0)
		started := make(chan struct{})
		consumed := make(chan struct{})
		go func() {
			defer close(consumed)
			for range c.SeqPhased(context.Background()) {
				close(started)
				break
			}
		}()
		// Broadcast until the consumer has taken one value. It closes
		// started before breaking out, and the broadcast that delivered
		// that value cannot return before the break compensates it, so
		// the check after each broadcast is exact. (The delivery count is
		// not usable here: a compensated delivery may be reported as 0.)
		for taken := false; !taken; {
			done := phase72Go(func() (int, error) { return c.BroadcastPhased(context.Background(), 1) })
			select {
			case r := <-done:
				if r.err != nil {
					t.Fatalf("BroadcastPhased: %v", r.err)
				}
			case <-timeAfterGuard():
				t.Fatal("BroadcastPhased was not released by the consumer breaking out")
			}
			select {
			case <-started:
				taken = true
			default:
			}
		}
		apexAwait(t, consumed, "consumer exit")
		phase72Healthy(t, c)
		c.Close()
	}
}

func TestPhaseCaster72Gemini_UnsubscribeReleasesBlockedSend(t *testing.T) {
	for _, phased := range []bool{false, true} {
		for range 300 {
			c := NewPhaseCaster72Gemini[int](0)
			subs := phase72Subs(t, c, 2)
			done := phase72Go(func() (int, error) {
				if phased {
					return c.BroadcastPhased(context.Background(), 1)
				}
				return c.Broadcast(1)
			})
			// Subscriber 0 participates; subscriber 1 never receives and
			// leaves while the broadcast may be parked on it.
			got := make(chan struct{})
			go func() {
				defer close(got)
				<-subs[0].ch
				if phased {
					subs[0].sub.Ack()
				}
			}()
			subs[1].sub.Unsubscribe()
			select {
			case r := <-done:
				if r.err != nil || r.n < 1 || r.n > 2 {
					t.Fatalf("broadcast = (%d, %v), want 1 or 2 deliveries and no error", r.n, r.err)
				}
			case <-timeAfterGuard():
				t.Fatal("broadcast was not released by Unsubscribe")
			}
			apexAwait(t, got, "participating subscriber")
			phase72Healthy(t, c)
			if s := c.Inspect(); s.Active != 1 {
				t.Fatalf("Active = %d, want 1", s.Active)
			}
			subs[0].sub.Unsubscribe()
			c.Close()
		}
	}
}

func TestPhaseCaster72Gemini_FixedCapacity(t *testing.T) {
	c := NewPhaseCaster72Gemini[int](64)
	defer c.Close()
	subs := phase72Subs(t, c, 64)
	if _, _, err := c.SubscribeManaged(); !errors.Is(err, ErrCapacity72Gemini) {
		t.Fatalf("subscription beyond capacity = %v, want ErrCapacity72Gemini", err)
	}
	subs[17].sub.Unsubscribe()
	s, _, err := c.SubscribeManaged()
	if err != nil {
		t.Fatalf("SubscribeManaged after a slot was released: %v", err)
	}
	// The released slot is reused under a new generation: the old handle
	// and the new one are distinguishable.
	if s == subs[17].sub {
		t.Fatal("a reused slot produced an identical subscription handle")
	}
	if !s.Valid() || (Subscription72Gemini[int]{}).Valid() {
		t.Fatal("Valid misreports")
	}
}

// Integration repairs: every broadcast allocated its phase-gate variable, and
// casters with more than four banks (more than 256 slots) also allocated
// their mask snapshot.
func TestPhaseCaster72Gemini_ZeroAllocBroadcast(t *testing.T) {
	// The -race build publishes the latest-value snapshot through an
	// allocated cell by design (one allocation per completed broadcast);
	// only the default build makes the zero-allocation claim.
	want := 0.0
	if raceEnabled {
		want = 1
	}
	for _, slots := range []int{0, 1024} {
		c := NewPhaseCaster72Gemini[int](slots)
		s := phase72Subs(t, c, 1)[0]
		stop := make(chan struct{})
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for {
				select {
				case <-s.ch:
				case <-stop:
					return
				}
			}
		}()
		if n := testing.AllocsPerRun(500, func() {
			if n, err := c.Broadcast(1); n != 1 || err != nil {
				panic("broadcast failed")
			}
		}); n != want {
			t.Errorf("capacity %d: Broadcast allocated %v objects per call, want %v", slots, n, want)
		}
		close(stop)
		apexAwait(t, drained, "receiver")
		s.sub.Unsubscribe()
		c.Close()
	}
}

// The two benchmarks below isolate the parts of the broadcaster that the
// bitmap kernels serve, without any channel handoff in the measurement.

// BenchmarkPhaseCaster72Gemini_BroadcastNoSubscribers is the cost of a
// broadcast that finds nobody: the turnstile, the active-mask snapshot and
// population count over every bank, and the coordination word.
func BenchmarkPhaseCaster72Gemini_BroadcastNoSubscribers(b *testing.B) {
	for _, banks := range []int{4, 64, 1024} {
		b.Run("banks="+itoa72(banks), func(b *testing.B) {
			c := NewPhaseCaster72Gemini[int](banks * slotsPerBank72Gemini)
			defer c.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if n, err := c.Broadcast(i); n != 0 || err != nil {
					b.Fatalf("Broadcast = (%d, %v)", n, err)
				}
			}
		})
	}
}

// BenchmarkPhaseCaster72Gemini_SubscribeLastBank is one Subscribe and
// Unsubscribe when every bank but the last is full, so the free-slot search
// has to pass over banks-1 full allocation masks.
func BenchmarkPhaseCaster72Gemini_SubscribeLastBank(b *testing.B) {
	for _, banks := range []int{4, 64, 1024} {
		b.Run("banks="+itoa72(banks), func(b *testing.B) {
			c := NewPhaseCaster72Gemini[int](banks * slotsPerBank72Gemini)
			ch := make(chan int)
			held := make([]Subscription72Gemini[int], 0, (banks-1)*slotsPerBank72Gemini)
			for range (banks - 1) * slotsPerBank72Gemini {
				s, err := c.Subscribe(ch)
				if err != nil {
					b.Fatal(err)
				}
				held = append(held, s)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := c.Subscribe(ch)
				if err != nil {
					b.Fatal(err)
				}
				s.Unsubscribe()
			}
			b.StopTimer()
			for _, s := range held {
				s.Unsubscribe()
			}
			c.Close()
		})
	}
}
