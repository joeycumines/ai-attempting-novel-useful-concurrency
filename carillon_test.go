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

func carillonMustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	fn()
}

func carillonEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCarillonSlotIsOneCacheLine(t *testing.T) {
	if got := unsafe.Sizeof(carillonSlot{}); got != 64 {
		t.Fatalf("slot size = %d, want 64", got)
	}
}

func TestCarillonSlackRounding(t *testing.T) {
	for in, want := range map[int]uint64{1: 1, 2: 2, 3: 4, 64: 64, 65: 128} {
		if got := NewCarillon[int](in).size; got != want {
			t.Errorf("slack %d: size %d, want %d", in, got, want)
		}
	}
	carillonMustPanic(t, "slack 0", func() { NewCarillon[int](0) })
}

func TestCarillonBroadcastOrdered(t *testing.T) {
	const n, nsubs = 5000, 8
	c := NewCarillon[int](16)
	subs := make([]CarillonSubscription[int], nsubs)
	for i := range subs {
		subs[i] = c.Subscribe()
	}
	if got := c.Len(); got != nsubs {
		t.Fatalf("Len = %d, want %d", got, nsubs)
	}

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range n {
				v, err := s.Recv(context.Background())
				if err != nil {
					t.Errorf("recv %d: %v", i, err)
					return
				}
				if v != i {
					t.Errorf("recv %d: got %d", i, v)
					return
				}
			}
		}()
	}
	for i := range n {
		if got := c.Send(i); got != nsubs {
			t.Fatalf("Send(%d) reported %d subscribers, want %d", i, got, nsubs)
		}
	}
	wg.Wait()
	for _, s := range subs {
		s.Unsubscribe()
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len after unsubscribe = %d, want 0", got)
	}
}

func TestCarillonLateJoinStartsAtPublished(t *testing.T) {
	c := NewCarillon[int](4)
	for i := range 5 {
		c.Send(i) // no subscribers: nothing is retained for later joiners
	}
	s := c.Subscribe()
	if lag := s.Lag(); lag != 0 {
		t.Fatalf("lag at join = %d, want 0", lag)
	}
	for i := 5; i < 8; i++ {
		c.Send(i)
	}
	if lag := s.Lag(); lag != 3 {
		t.Fatalf("lag = %d, want 3", lag)
	}
	for want := 5; want < 8; want++ {
		if v, ok := s.TryRecv(); !ok || v != want {
			t.Fatalf("TryRecv = (%d, %v), want (%d, true)", v, ok, want)
		}
	}
	if _, ok := s.TryRecv(); ok {
		t.Fatal("TryRecv returned a value after draining")
	}
}

func TestCarillonReadyAndTryRecv(t *testing.T) {
	c := NewCarillon[string](2)
	s := c.Subscribe()
	if _, ok := s.TryRecv(); ok {
		t.Fatal("empty subscription yielded a value")
	}
	select {
	case <-s.Ready():
		t.Fatal("doorbell fired with nothing published")
	default:
	}
	c.Send("a")
	// The doorbell was armed before the send, so the sender rang it.
	select {
	case <-s.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("doorbell did not fire after Send")
	}
	if v, ok := s.TryRecv(); !ok || v != "a" {
		t.Fatalf("TryRecv = (%q, %v), want (\"a\", true)", v, ok)
	}

	// Ready on a non-empty subscription must fire without any further send,
	// because the recheck inside Ready rings the doorbell itself.
	c.Send("b")
	select {
	case <-s.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Ready did not fire with a value pending")
	}
	if v, ok := s.TryRecv(); !ok || v != "b" {
		t.Fatalf("TryRecv = (%q, %v), want (\"b\", true)", v, ok)
	}
}

func TestCarillonBackpressureStallsAtSlackUntilUnsubscribe(t *testing.T) {
	const slack, total = 2, 10
	c := NewCarillon[int](slack)
	slow := c.Subscribe()

	var sent atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range total {
			c.Send(i)
			sent.Add(1)
		}
	}()

	carillonEventually(t, "sender to reach slack", func() bool { return sent.Load() == slack })
	for i := 0; i < 1000; i++ {
		runtime.Gosched()
		if got := sent.Load(); got != slack {
			t.Fatalf("sender ran past slack: sent %d, want %d", got, slack)
		}
	}

	slow.Unsubscribe()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sender still blocked after the lagging subscription unsubscribed")
	}
	if got := sent.Load(); got != total {
		t.Fatalf("sent %d, want %d", got, total)
	}
}

func TestCarillonLagIsBoundedBySlack(t *testing.T) {
	const slack, total = 4, 20000
	c := NewCarillon[int](slack)
	s := c.Subscribe()

	go func() {
		for i := range total {
			c.Send(i)
		}
	}()

	var maxLag uint64
	for i := range total {
		if i%251 == 0 {
			runtime.Gosched()
		}
		if lag := s.Lag(); lag > maxLag {
			maxLag = lag
		}
		v, err := s.Recv(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if v != i {
			t.Fatalf("got %d, want %d", v, i)
		}
	}
	if maxLag > slack {
		t.Fatalf("observed lag %d exceeds slack %d", maxLag, slack)
	}
	if maxLag == 0 {
		t.Fatal("never observed any lag; the test did not exercise pipelining")
	}
}

func TestCarillonSlotReuseIsolatesSubscriptions(t *testing.T) {
	c := NewCarillon[int](8)
	a := c.Subscribe()
	for i := range 5 {
		c.Send(i)
	}
	if v, ok := a.TryRecv(); !ok || v != 0 {
		t.Fatalf("a.TryRecv = (%d, %v), want (0, true)", v, ok)
	}
	a.Unsubscribe()
	for i := 5; i < 7; i++ {
		c.Send(i) // nobody is subscribed
	}

	b := c.Subscribe() // very likely reuses a's slot
	if lag := b.Lag(); lag != 0 {
		t.Fatalf("reused slot: lag = %d, want 0", lag)
	}
	c.Send(7)
	if v, ok := b.TryRecv(); !ok || v != 7 {
		t.Fatalf("b.TryRecv = (%d, %v), want (7, true)", v, ok)
	}

	// The stale handle must neither consume b's values nor change the count.
	if _, ok := a.TryRecv(); ok {
		t.Fatal("stale handle yielded a value")
	}
	if lag := a.Lag(); lag != 0 {
		t.Fatalf("stale handle lag = %d, want 0", lag)
	}
	a.Unsubscribe()
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d after stale unsubscribe, want 1", got)
	}
	if v, ok := b.TryRecv(); ok {
		t.Fatalf("b yielded %d after its own drain", v)
	}
	c.Send(8)
	if v, ok := b.TryRecv(); !ok || v != 8 {
		t.Fatalf("b.TryRecv = (%d, %v), want (8, true)", v, ok)
	}
	b.Unsubscribe()
}

func TestCarillonDirectoryGrowsAndReuses(t *testing.T) {
	const n = 200
	c := NewCarillon[int](4)
	subs := make([]CarillonSubscription[int], 0, n)
	for range n {
		subs = append(subs, c.Subscribe())
	}
	if got := len(*c.chunks.Load()); got != 4 {
		t.Fatalf("chunks = %d, want 4 for %d subscriptions", got, n)
	}
	c.Send(1)
	for i, s := range subs {
		if v, ok := s.TryRecv(); !ok || v != 1 {
			t.Fatalf("sub %d: TryRecv = (%d, %v), want (1, true)", i, v, ok)
		}
	}
	for _, s := range subs {
		s.Unsubscribe()
	}
	for range n {
		c.Subscribe().Unsubscribe() // churn through the free slots
	}
	subs = subs[:0]
	for range n {
		subs = append(subs, c.Subscribe()) // re-fill from the existing chunks
	}
	if got := len(*c.chunks.Load()); got != 4 {
		t.Fatalf("chunks after churn = %d, want 4 (slots must be reused)", got)
	}
}

func TestCarillonSelectNoLostWakeups(t *testing.T) {
	const nsubs, total = 4, 20000
	c := NewCarillon[int](8)
	subs := make([]CarillonSubscription[int], nsubs)
	for i := range subs {
		subs[i] = c.Subscribe()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range total {
				var v int
				for {
					got, ok := s.TryRecv()
					if ok {
						v = got
						break
					}
					select {
					case <-s.Ready():
					case <-ctx.Done():
						t.Errorf("timed out waiting for value %d", i)
						return
					}
				}
				if v != i {
					t.Errorf("got %d, want %d", v, i)
					return
				}
			}
		}()
	}
	for i := range total {
		c.Send(i)
		if i%509 == 0 {
			runtime.Gosched()
		}
	}
	wg.Wait()
	for _, s := range subs {
		s.Unsubscribe()
	}
}

func TestCarillonRecvContextAndUnsubscribe(t *testing.T) {
	c := NewCarillon[int](2)
	s := c.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := s.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv error = %v, want DeadlineExceeded", err)
	}
	s.Unsubscribe()
	if _, err := s.Recv(context.Background()); !errors.Is(err, ErrCarillonUnsubscribed) {
		t.Fatalf("Recv after unsubscribe = %v, want ErrCarillonUnsubscribed", err)
	}

	var zero CarillonSubscription[int]
	zero.Unsubscribe()
	if lag := zero.Lag(); lag != 0 {
		t.Fatalf("zero Lag = %d", lag)
	}
	if _, ok := zero.TryRecv(); ok {
		t.Fatal("zero TryRecv yielded a value")
	}
	if zero.Ready() != nil {
		t.Fatal("zero Ready returned a non-nil channel")
	}
	if _, err := zero.Recv(context.Background()); !errors.Is(err, ErrCarillonUnsubscribed) {
		t.Fatalf("zero Recv = %v, want ErrCarillonUnsubscribed", err)
	}
}

func TestCarillonAllIterator(t *testing.T) {
	c := NewCarillon[int](8)
	s := c.Subscribe()
	for i := range 5 {
		c.Send(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []int
	for v := range s.All(ctx) {
		got = append(got, v)
		if len(got) == 5 {
			break
		}
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("All yielded %v", got)
		}
	}
	if len(got) != 5 {
		t.Fatalf("All yielded %d values, want 5", len(got))
	}
}

func TestCarillonSteadyStateZeroAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector instruments allocations")
	}

	t.Run("send_and_tryrecv", func(t *testing.T) {
		c := NewCarillon[int](64)
		subs := make([]CarillonSubscription[int], 8)
		for i := range subs {
			subs[i] = c.Subscribe()
		}
		var n int
		allocs := testing.AllocsPerRun(1000, func() {
			n++
			c.Send(n)
			for _, s := range subs {
				s.TryRecv()
			}
		})
		if allocs != 0 {
			t.Fatalf("Send+TryRecv allocs/op = %v, want 0", allocs)
		}
	})

	t.Run("parked_ready_path", func(t *testing.T) {
		c := NewCarillon[int](64)
		s := c.Subscribe()
		var n int
		allocs := testing.AllocsPerRun(1000, func() {
			n++
			s.Ready() // arm, then the sender rings the doorbell
			c.Send(n)
			s.TryRecv() // disarm and consume
		})
		if allocs != 0 {
			t.Fatalf("Ready+Send+TryRecv allocs/op = %v, want 0", allocs)
		}
	})

	t.Run("subscribe_unsubscribe", func(t *testing.T) {
		c := NewCarillon[int](64)
		c.Subscribe().Unsubscribe() // warm: first claim allocates the chunk
		allocs := testing.AllocsPerRun(1000, func() {
			s := c.Subscribe()
			s.Unsubscribe()
		})
		if allocs != 0 {
			t.Fatalf("Subscribe+Unsubscribe allocs/op = %v, want 0", allocs)
		}
	})
}
