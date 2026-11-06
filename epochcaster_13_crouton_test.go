package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEpochCasterBasicFanout(t *testing.T) {
	x := NewEpochCaster13Crouton[int]()
	subs := []*EpochSub13Crouton[int]{x.Subscribe(), x.Subscribe(), x.Subscribe()}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	if got := x.Send(42); got != 1 {
		t.Fatalf("Send epoch = %d, want 1", got)
	}
	for i, s := range subs {
		v, ok := s.Recv()
		if !ok || v != 42 {
			t.Fatalf("sub %d: got (%d,%v), want (42,true)", i, v, ok)
		}
	}
}

func TestEpochCasterNoSubscribers(t *testing.T) {
	x := NewEpochCaster13Crouton[int]()
	if got := x.Send(1); got != 0 {
		t.Fatalf("Send with no subscribers = %d, want 0", got)
	}
	sub := x.Subscribe()
	defer sub.Unsubscribe()
	if got := x.Send(2); got != 1 {
		t.Fatalf("first published epoch = %d, want 1", got)
	}
	if v, ok := sub.Recv(); !ok || v != 2 {
		t.Fatalf("got (%d,%v), want (2,true)", v, ok)
	}
}

func TestEpochCasterSelectAndDrain(t *testing.T) {
	x := NewEpochCaster13Crouton[int]()
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	x.Send(1)
	x.Send(2)
	x.Send(3) // wake tokens coalesce into one

	select {
	case <-sub.Wake():
		for want := 1; want <= 3; want++ {
			v, ok, closed := sub.TryRecv()
			if closed || !ok || v != want {
				t.Fatalf("drain: got (%d,%v,%v), want (%d,true,false)", v, ok, closed, want)
			}
		}
		if _, ok, _ := sub.TryRecv(); ok {
			t.Fatal("drained sub still reports a value")
		}
	case <-time.After(time.Second):
		t.Fatal("no wake received")
	}
}

func TestEpochCasterRingWrapBlocksPublisher(t *testing.T) {
	x := NewEpochCasterWithRingSize13Crouton[int](4)
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	for i := 1; i <= 4; i++ {
		x.Send(i)
	}
	done := make(chan uint64, 1)
	go func() { done <- x.Send(5) }()

	select {
	case <-done:
		t.Fatal("Send returned before ring slot was freed")
	case <-time.After(50 * time.Millisecond):
	}

	if v, ok := sub.Recv(); !ok || v != 1 {
		t.Fatalf("got (%d,%v), want (1,true)", v, ok)
	}
	select {
	case got := <-done:
		if got != 5 {
			t.Fatalf("blocked Send epoch = %d, want 5", got)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Send did not complete after slot freed")
	}
	for want := 2; want <= 5; want++ {
		if v, ok := sub.Recv(); !ok || v != want {
			t.Fatalf("got (%d,%v), want (%d,true)", v, ok, want)
		}
	}
}

func TestEpochCasterUnsubscribeFreesRing(t *testing.T) {
	x := NewEpochCasterWithRingSize13Crouton[int](4)
	a := x.Subscribe()
	b := x.Subscribe()
	defer b.Unsubscribe()

	x.Send(1)
	if v, ok := b.Recv(); !ok || v != 1 {
		t.Fatalf("got (%d,%v), want (1,true)", v, ok)
	}
	a.Unsubscribe() // a never reads epoch 1; must not stall the ring

	for want := 2; want <= 20; want++ {
		x.Send(want)
		if v, ok := b.Recv(); !ok || v != want {
			t.Fatalf("got (%d,%v), want (%d,true)", v, ok, want)
		}
	}
}

func TestEpochCasterRelayFanout(t *testing.T) {
	const n = 100 // crosses the relay threshold in both blocks
	x := NewEpochCaster13Crouton[int]()
	subs := make([]*EpochSub13Crouton[int], n)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	x.Send(7)
	for i, s := range subs {
		if v, ok := s.Recv(); !ok || v != 7 {
			t.Fatalf("sub %d: got (%d,%v), want (7,true)", i, v, ok)
		}
	}
}

func TestEpochCasterClose(t *testing.T) {
	x := NewEpochCaster13Crouton[int]()
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	x.Send(7)
	x.Close()
	x.Close() // idempotent

	if v, ok := sub.Recv(); !ok || v != 7 {
		t.Fatalf("got (%d,%v), want (7,true)", v, ok)
	}
	if _, ok := sub.Recv(); ok {
		t.Fatal("Recv after drain of closed caster returned ok")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Send after Close did not panic")
		}
	}()
	x.Send(8)
}

func TestEpochCasterSubscribeAfterClose(t *testing.T) {
	x := NewEpochCaster13Crouton[int]()
	x.Close()
	sub := x.Subscribe()
	if _, ok := sub.Recv(); ok {
		t.Fatal("subscription to closed caster returned a value")
	}
	sub.Unsubscribe()
}

func TestEpochCasterRecvContextCancelled(t *testing.T) {
	x := NewEpochCaster13Crouton[int]()
	sub := x.Subscribe()
	defer sub.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.RecvContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEpochCasterChurn(t *testing.T) {
	x := NewEpochCasterWithRingSize13Crouton[int](64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				sub := x.Subscribe()
				last := 0
				func() {
					defer sub.Unsubscribe()
					for {
						v, err := sub.RecvContext(ctx)
						if err != nil {
							return
						}
						if v <= last {
							t.Errorf("out-of-order delivery: %d after %d", v, last)
							return
						}
						last = v
					}
				}()
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}

	for i := 1; i <= 2000; i++ {
		x.Send(i)
	}
	cancel()
	wg.Wait()
}

func TestEpochCasterConcurrentPublishers(t *testing.T) {
	const publishers, perPublisher = 8, 100
	x := NewEpochCaster13Crouton[int]()
	subs := []*EpochSub13Crouton[int]{x.Subscribe(), x.Subscribe(), x.Subscribe(), x.Subscribe()}
	defer func() {
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()

	var counter atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				x.Send(int(counter.Add(1)))
			}
		}()
	}
	wg.Wait()

	const total = publishers * perPublisher
	for i, s := range subs {
		seen := make(map[int]bool, total)
		for j := 0; j < total; j++ {
			v, ok := s.Recv()
			if !ok {
				t.Fatalf("sub %d: closed after %d values", i, j)
			}
			if seen[v] {
				t.Fatalf("sub %d: duplicate value %d", i, v)
			}
			seen[v] = true
		}
	}
}

// BenchmarkEpochCasterSend measures publisher cost plus an in-process
// drain of every subscriber (TryRecv does not depend on wake delivery).
func BenchmarkEpochCasterSend(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			x := NewEpochCaster13Crouton[int]()
			subs := make([]*EpochSub13Crouton[int], n)
			for i := range subs {
				subs[i] = x.Subscribe()
			}
			defer func() {
				for _, s := range subs {
					s.Unsubscribe()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(i)
				for _, s := range subs {
					if _, ok, _ := s.TryRecv(); !ok {
						b.Fatal("missing value")
					}
				}
			}
		})
	}
}

func BenchmarkEpochCasterSubscribeUnsubscribe(b *testing.B) {
	x := NewEpochCaster13Crouton[int]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Subscribe().Unsubscribe()
	}
}
