package bigbuff

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuedFanout24BacklogIsolation(t *testing.T) {
	b := NewQueuedFanout24Meta[int]()
	slow, fast := b.Subscribe(), b.Subscribe()
	defer slow.Unsubscribe()
	defer fast.Unsubscribe()
	const msgs = 200
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := 1; v <= msgs; v++ {
			if n := b.Send(v); n != 2 {
				t.Errorf("Send(%d)=%d want 2", v, n)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(scenarioGuard):
		t.Fatal("Send blocked on slow consumer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
	defer cancel()
	for want := 1; want <= msgs; want++ {
		if v, ok := fast.RecvContext(ctx); !ok || v != want {
			t.Fatalf("fast got (%d,%v) want %d", v, ok, want)
		}
	}
	if slow.QueueLen() == 0 {
		t.Fatal("slow consumer did not accumulate private overflow")
	}
	for want := 1; want <= msgs; want++ {
		if v, ok := slow.RecvContext(ctx); !ok || v != want {
			t.Fatalf("slow got (%d,%v) want %d", v, ok, want)
		}
	}
	if slow.QueueLen() != 0 {
		t.Fatalf("queue not drained: %d", slow.QueueLen())
	}
}

func TestQueuedFanout24WithdrawLeases(t *testing.T) {
	b := NewQueuedFanout24Meta[int]()
	s := b.Subscribe()
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				accepted.Add(int64(b.Send(g*100 + i + 1)))
			}
		}(g)
	}
	s.Unsubscribe()
	wg.Wait()
	for i := 0; i < 20; i++ {
		if n := b.Send(10000 + i); n != 0 {
			t.Fatalf("post-withdraw Send accepted %d", n)
		}
	}
	got := 0
	for {
		if _, ok := s.Recv(); !ok {
			break
		}
		got++
	}
	if int64(got) != accepted.Load() {
		t.Fatalf("drained %d accepted %d", got, accepted.Load())
	}
	s.Unsubscribe()
}

func TestQueuedFanout24CancelAndPump(t *testing.T) {
	b := NewQueuedFanout24Meta[int]()
	s := b.Subscribe()
	for v := 1; v <= 5; v++ {
		b.Send(v)
	}
	select {
	case v := <-s.C():
		if v != 1 {
			t.Fatalf("C got %d", v)
		}
	default:
		t.Fatal("mailbox empty")
	}
	if !s.Pump() {
		t.Fatal("Pump did not refill from overflow")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := s.RecvContext(ctx); ok {
		t.Fatal("cancelled RecvContext returned a value")
	}
	s.Unsubscribe()
	var got []int
	for {
		v, ok := s.Recv()
		if !ok {
			break
		}
		got = append(got, v)
	}
	want := []int{2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("retained backlog %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backlog %v want %v", got, want)
		}
	}
}

func BenchmarkQueuedFanout24Paths(b *testing.B) {
	for _, path := range []string{"mailbox", "overflow"} {
		b.Run(path, func(b *testing.B) {
			hub := NewQueuedFanout24Meta[int]()
			s := hub.Subscribe()
			defer s.Unsubscribe()
			if path == "overflow" {
				for i := 0; i < 1024; i++ {
					hub.Send(i)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hub.Send(i)
				if _, ok := s.Recv(); !ok {
					b.Fatal("paired drain failed")
				}
			}
			b.StopTimer()
			if path == "overflow" {
				for i := 0; i < 1024; i++ {
					if _, ok := s.Recv(); !ok {
						b.Fatal("backlog not accounted")
					}
				}
			}
		})
	}
}

func FuzzQueuedFanout24FIFO(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 2, 1, 1})
	f.Add([]byte{0, 0, 0, 3, 1, 1, 1, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 32 {
			return
		}
		b := NewQueuedFanout24Meta[int]()
		s := b.Subscribe()
		var model []int
		next := 1
		withdrawn := false
		for _, op := range ops {
			switch op % 4 {
			case 0:
				n := b.Send(next)
				if withdrawn {
					if n != 0 {
						t.Fatalf("accepted after withdrawal")
					}
				} else {
					if n != 1 {
						t.Fatalf("Send=%d", n)
					}
					model = append(model, next)
					next++
				}
			case 1, 2:
				v, ok := s.TryRecv()
				if len(model) == 0 {
					if ok {
						t.Fatalf("unexpected value %d", v)
					}
					continue
				}
				if !ok || v != model[0] {
					t.Fatalf("TryRecv=(%d,%v) want %d", v, ok, model[0])
				}
				model = model[1:]
			case 3:
				if !withdrawn {
					s.Unsubscribe()
					withdrawn = true
				}
			}
		}
		s.Unsubscribe()
		for _, want := range model {
			if v, ok := s.Recv(); !ok || v != want {
				t.Fatalf("drain=(%d,%v) want %d", v, ok, want)
			}
		}
		if _, ok := s.Recv(); ok {
			t.Fatal("backlog not exhausted")
		}
	})
}

func TestQueuedFanout24ConcurrentPublisherFIFO(t *testing.T) {
	b := NewQueuedFanout24Meta[int]()
	s := b.Subscribe()
	defer s.Unsubscribe()
	const publishers, each = 4, 100
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < publishers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 1; i <= each; i++ {
				accepted.Add(int64(b.Send(g*1000 + i)))
			}
		}(g)
	}
	wg.Wait()
	last := make(map[int]int)
	got := 0
	for {
		v, ok := s.TryRecv()
		if !ok {
			break
		}
		got++
		g, seq := v/1000, v%1000
		if seq <= last[g] {
			t.Fatalf("publisher %d reordered: %d after %d", g, seq, last[g])
		}
		last[g] = seq
	}
	if int64(got) != accepted.Load() || got != publishers*each {
		t.Fatalf("drained %d accepted %d want %d", got, accepted.Load(), publishers*each)
	}
}

func TestNonblockingOverflowBaseline(t *testing.T) {
	for _, mk := range []func() interface {
		Send(int) int
		Subscribe() interface {
			Receive(context.Context) (int, bool)
			TryReceive() (int, bool)
			Unsubscribe()
		}
	}{
		func() interface {
			Send(int) int
			Subscribe() interface {
				Receive(context.Context) (int, bool)
				TryReceive() (int, bool)
				Unsubscribe()
			}
		} {
			return queuedBaselineTestAdapter{NewQueuedFanout24Meta[int]()}
		},
		func() interface {
			Send(int) int
			Subscribe() interface {
				Receive(context.Context) (int, bool)
				TryReceive() (int, bool)
				Unsubscribe()
			}
		} {
			return baselineQueuedTestAdapter{NewBaselineQueuedFanout[int]()}
		},
	} {
		b := mk()
		s := b.Subscribe()
		const msgs = 50
		for v := 1; v <= msgs; v++ {
			if n := b.Send(v); n != 1 {
				t.Fatalf("Send=%d", n)
			}
		}
		s.Unsubscribe()
		ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
		defer cancel()
		for want := 1; want <= msgs; want++ {
			if v, ok := s.Receive(ctx); !ok || v != want {
				t.Fatalf("post-withdraw drain=(%d,%v) want %d", v, ok, want)
			}
		}
		if _, ok := s.TryReceive(); ok {
			t.Fatal("backlog not exhausted")
		}
		if n := b.Send(999); n != 0 {
			t.Fatalf("post-withdraw Send=%d", n)
		}
	}
}

type queuedBaselineTestAdapter struct{ b *QueuedFanout24Meta[int] }

func (a queuedBaselineTestAdapter) Send(v int) int { return a.b.Send(v) }
func (a queuedBaselineTestAdapter) Subscribe() interface {
	Receive(context.Context) (int, bool)
	TryReceive() (int, bool)
	Unsubscribe()
} {
	return queuedBaselineTestSub{a.b.Subscribe()}
}

type queuedBaselineTestSub struct {
	s *QueuedFanout24Subscription[int]
}

func (s queuedBaselineTestSub) Receive(ctx context.Context) (int, bool) { return s.s.RecvContext(ctx) }
func (s queuedBaselineTestSub) TryReceive() (int, bool)                 { return s.s.TryRecv() }
func (s queuedBaselineTestSub) Unsubscribe()                            { s.s.Unsubscribe() }

// A value that Send parks in the overflow queue behind a full mailbox must reach a
// receiver that has just taken the mailbox value. The receiver's pump used to test
// the queue without the enqueue lock, so it could conclude there was nothing to
// move while Send sat between its mailbox test and its append, and the receiver
// then slept with the value queued. The invariant holds on every interleaving of
// the fixed code, so the test cannot flake in the passing direction; a failure is
// a regression.
func TestQueuedFanout24MetaOverflowHandoffNoLostWakeup(t *testing.T) {
	const iters = 20000
	for i := 0; i < iters; i++ {
		b := NewQueuedFanout24Meta[int]()
		sub := b.Subscribe()
		b.Send(1) // fills the capacity-one mailbox
		ctx, cancel := context.WithCancel(context.Background())
		first := make(chan int, 1)
		second := make(chan int, 1)
		go func() {
			v1, _ := sub.RecvContext(ctx)
			first <- v1
			v2, _ := sub.RecvContext(ctx)
			second <- v2
		}()
		go b.Send(2)
		select {
		case v := <-second:
			if v != 2 {
				t.Fatalf("iteration %d: second value = %d, want 2", i, v)
			}
		case <-time.After(scenarioGuard):
			cancel()
			t.Fatalf("iteration %d: overflowed value was never handed to the receiver", i)
		}
		cancel()
		<-first
		sub.Unsubscribe()
	}
}

type baselineQueuedTestAdapter struct{ b *BaselineQueuedFanout[int] }

func (a baselineQueuedTestAdapter) Send(v int) int { return a.b.Send(v) }
func (a baselineQueuedTestAdapter) Subscribe() interface {
	Receive(context.Context) (int, bool)
	TryReceive() (int, bool)
	Unsubscribe()
} {
	return a.b.Subscribe()
}
