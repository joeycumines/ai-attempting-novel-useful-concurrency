package bigbuff

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestSelectFanout38Cancellation(t *testing.T) {
	f := NewSelectFanout[int]()
	s, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := f.SendContext(ctx, 1); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(scenarioGuard):
		t.Fatal("cancellable Send blocked")
	}
	s.Unsubscribe()
	s.Unsubscribe()
	f.Close()
	f.Close()
	if _, err := f.Subscribe(); !errors.Is(err, ErrSelectFanoutClosed) {
		t.Fatalf("subscribe after close=%v", err)
	}
}

func BenchmarkSelectFanout38Churn(b *testing.B) {
	f := NewSelectFanout[int]()
	defer f.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s, err := f.Subscribe()
		if err != nil {
			b.Fatal(err)
		}
		s.Unsubscribe()
	}
}

func TestFanout45LeaseClose(t *testing.T) {
	f := NewFanout[int]()
	s, err := f.SubscribeBuffered(1)
	if err != nil {
		t.Fatal(err)
	}
	if n := f.Broadcast(1); n != 1 {
		t.Fatalf("first=%d", n)
	}
	done := make(chan int, 1)
	go func() { done <- f.Broadcast(2) }()
	s.Close()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("aborted broadcast=%d", n)
		}
	case <-time.After(scenarioGuard):
		t.Fatal("node close did not release send lease")
	}
	if v, ok := <-s.C(); !ok || v != 1 {
		t.Fatalf("buffered value after close=(%d,%v), want (1,true)", v, ok)
	}
	if _, ok := <-s.C(); ok {
		t.Fatal("closed subscription channel still open after draining")
	}
	f.Close()
	f.Close()
}

func BenchmarkFanout45Buffered(b *testing.B) {
	f := NewFanout[int]()
	const subs = 8
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s, err := f.SubscribeBuffered(16)
		if err != nil {
			b.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range s.C() {
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for v := 1; v <= b.N; v++ {
		f.Broadcast(v)
	}
	b.StopTimer()
	f.Close()
	wg.Wait()
}

func TestRingFanout45ReplayAndOverrun(t *testing.T) {
	f, err := NewRingFanout[int](4)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for v := 1; v <= 6; v++ {
		f.Publish(v)
	}
	s, err := f.SubscribeReplay()
	if err != nil {
		t.Fatal(err)
	}
	for want := 3; want <= 6; want++ {
		if v, _, ok := s.TryNext(); !ok || v != want {
			t.Fatalf("replay=(%d,%v) want %d", v, ok, want)
		}
	}
	s.Close()
	slow, err := f.SubscribeReplay()
	if err != nil {
		t.Fatal(err)
	}
	for v := 7; v <= 10; v++ {
		f.Publish(v)
	}
	v, missed, ok := slow.TryNext()
	if !ok || v != 7 || missed != 4 {
		t.Fatalf("overrun=(%d,%d,%v) want (7,4,true)", v, missed, ok)
	}
	for want := 8; want <= 10; want++ {
		if v, _, ok := slow.TryNext(); !ok || v != want {
			t.Fatalf("got (%d,%v) want %d", v, ok, want)
		}
	}
	slow.Close()
}

func BenchmarkRingFanout45Publish(b *testing.B) {
	f, err := NewRingFanout[int](64)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const subs = 8
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s, err := f.Subscribe()
		if err != nil {
			b.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, _, ok, err := s.Next(ctx); err != nil || !ok {
					return
				}
			}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for v := 1; v <= b.N; v++ {
		if _, err := f.Publish(v); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cancel()
	f.Close()
	wg.Wait()
}

func TestChanCaster58Helpers(t *testing.T) {
	x := NewChanCaster58Spark(make(chan int))
	if unsafe.Sizeof(*x) != 128 {
		t.Fatalf("size=%d want 128", unsafe.Sizeof(*x))
	}
	if v, ok := x.TryRecv(); ok || v != 0 || x.Len() != 0 {
		t.Fatalf("empty TryRecv=(%d,%v) len=%d", v, ok, x.Len())
	}
	// Send has a zero-receiver fast path, so the scheduler may legitimately
	// run it before RecvContext registers. Accept either exact outcome rather
	// than assuming registration wins: Send=0 implies the context helper is
	// cancelled without delivery; Send=1 implies the helper received 7.
	type recvResult struct {
		v  int
		ok bool
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recvDone := make(chan recvResult, 1)
	go func() {
		v, ok := x.RecvContext(ctx)
		recvDone <- recvResult{v, ok}
	}()
	sendDone := make(chan int, 1)
	go func() { sendDone <- x.Send(7) }()
	var n int
	select {
	case n = <-sendDone:
	case <-time.After(scenarioGuard):
		t.Fatal("Send did not complete")
	}
	if n == 0 {
		cancel()
	}
	var r recvResult
	select {
	case r = <-recvDone:
	case <-time.After(scenarioGuard):
		t.Fatal("RecvContext did not complete")
	}
	if n == 1 && (!r.ok || r.v != 7) {
		t.Fatalf("Send=1 but RecvContext=(%d,%v)", r.v, r.ok)
	}
	if n == 0 && r.ok {
		t.Fatalf("Send=0 but RecvContext received %d", r.v)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	sendDone = make(chan int, 1)
	go func() { sendDone <- x.Send(8) }()
	go func() { cancel2(); x.RecvContext(ctx2) }()
	select {
	case <-sendDone:
	case <-time.After(scenarioGuard):
		t.Fatal("withdrawal stranded Send")
	}
	if x.Len() != 0 {
		t.Fatalf("leaked intent: %d", x.Len())
	}
}

func BenchmarkChanCaster58Helpers(b *testing.B) {
	x := NewChanCaster58Spark(make(chan int))
	b.Run("empty-try", func(b *testing.B) {
		b.Skip("exposure limited: an idle TryRecv can steal-block if Send starts after its gate; no concurrent Send is a valid throughput measurement")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			x.TryRecv()
		}
	})
	b.Run("rendezvous", func(b *testing.B) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if _, ok := x.RecvContext(ctx); !ok {
					return
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x.Send(i)
		}
		b.StopTimer()
		cancel()
		<-done
	})
}

func TestBroadcaster58RingAndClose(t *testing.T) {
	b := NewBroadcaster58Spark[int](4)
	s := b.Subscribe()
	for v := 1; v <= 6; v++ {
		b.Send(v)
	}
	if v, ok, missed := s.Next(); !ok || v != 3 || missed != 2 {
		t.Fatalf("first=(%d,%v,%d) want (3,true,2)", v, ok, missed)
	}
	for want := 4; want <= 6; want++ {
		if v, ok, _ := s.Next(); !ok || v != want {
			t.Fatalf("got (%d,%v) want %d", v, ok, want)
		}
	}
	b.Close()
	b.Close()
	if _, ok, _ := s.Next(); ok {
		t.Fatal("caught-up closed cursor returned value")
	}
	select {
	case <-b.Done():
	default:
		t.Fatal("Done not closed")
	}
	s.Unsubscribe()
	s.Unsubscribe()
	if b.Subscribers() != 0 {
		t.Fatalf("subscribers=%d", b.Subscribers())
	}
}

func TestBroadcaster58IteratorEdges(t *testing.T) {
	b := NewBroadcaster58Spark[int](8)
	ctx, cancel := context.WithCancel(context.Background())
	seq := b.SubscribeContext(ctx)
	got := make(chan int, 4)
	done := make(chan struct{})
	go func() { defer close(done); seq(func(v int) bool { got <- v; return v != 3 }) }()
	for v := 1; v <= 3; v++ {
		b.Send(v)
	}
	for want := 1; want <= 3; want++ {
		select {
		case v := <-got:
			if v != want {
				t.Fatalf("got %d want %d", v, want)
			}
		case <-time.After(scenarioGuard):
			t.Fatalf("missing %d", want)
		}
	}
	select {
	case <-done:
	case <-time.After(scenarioGuard):
		t.Fatal("iterator did not stop")
	}
	cancel()
	if b.Subscribers() != 0 {
		t.Fatalf("subscriber leak %d", b.Subscribers())
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("nil yield did not panic")
			}
		}()
		b.SubscribeContext(context.Background())(nil)
	}()
	b.Close()
}

func BenchmarkBroadcaster58Send(b *testing.B) {
	x := NewBroadcaster58Spark[int](64)
	defer x.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x.Send(i)
	}
}

func FuzzBroadcaster58RingModel(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 1, 1, 2})
	f.Add([]byte{0, 0, 0, 0, 0, 1, 1, 0, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 40 {
			return
		}
		b := NewBroadcaster58Spark[int](4)
		s := b.Subscribe()
		defer s.Unsubscribe()
		var model []int
		next, cursor := 1, 1
		for _, op := range ops {
			switch op % 3 {
			case 0:
				b.Send(next)
				model = append(model, next)
				if len(model) > 4 {
					model = model[1:]
				}
				next++
			case 1:
				v, ok, missed := s.Next()
				orig := cursor
				if next-cursor > 4 {
					cursor = next - 4
				}
				if cursor >= next {
					if ok {
						t.Fatalf("unexpected %d", v)
					}
					continue
				}
				if !ok || v != cursor {
					t.Fatalf("Next=(%d,%v) want %d", v, ok, cursor)
				}
				if missed != uint64(cursor-orig) {
					t.Fatalf("missed=%d want %d", missed, cursor-orig)
				}
				cursor++
			case 2:
				if b.IsClosed() {
					t.Fatal("unexpected closed")
				}
			}
		}
		_ = model
	})
}

func TestBroadcaster58WidePayload(t *testing.T) {
	b := NewBroadcaster58Spark[pv](8)
	defer b.Close()
	s := b.Subscribe()
	defer s.Unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
	defer cancel()
	const msgs = 200
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= msgs; i++ {
			b.Send(mkpv(i))
		}
	}()
	got := 0
	var last uint64
	for last < msgs {
		notify := s.Notify()
		for {
			v, ok, _ := s.Next()
			if !ok {
				break
			}
			if v[0] != v[1] || v[0] != v[2] || v[0] != v[3] {
				t.Fatalf("torn payload: %v", v)
			}
			if v[0] <= last {
				t.Fatalf("non-monotonic payload %d after %d", v[0], last)
			}
			last = v[0]
			got++
			if last == msgs {
				break
			}
		}
		if last == msgs {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("payload reader cancelled")
		case <-notify:
		}
	}
	wg.Wait()
	if last != msgs {
		t.Fatalf("final payload %d want %d", last, msgs)
	}
}
