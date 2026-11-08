package bigbuff

import (
	"context"
	"errors"
	"testing"
)

func TestRingFanOut14BurstEviction(t *testing.T) {
	x := newRingFanOut14Xing[chan int, int](4, 2, 2, false)
	ch, err := x.Add()
	if err != nil {
		t.Fatal(err)
	}
	for v := 1; v <= 10; v++ {
		if err := x.Send(v); err != nil {
			t.Fatal(err)
		}
	}
	if got := x.InputDropped(); got != 6 {
		t.Fatalf("input dropped=%d want 6", got)
	}
	for len(x.input) > 0 {
		x.dispatch(<-x.input)
	}
	if got := x.DispatchDropped(); got != 2 {
		t.Fatalf("dispatch dropped=%d want 2", got)
	}
	for _, want := range []int{9, 10} {
		select {
		case v := <-ch:
			if v != want {
				t.Fatalf("got %d want %d", v, want)
			}
		default:
			t.Fatalf("missing retained value %d", want)
		}
	}
	select {
	case v := <-ch:
		t.Fatalf("unexpected extra value %d", v)
	default:
	}
}

func TestRingFanOut14PoolReuse(t *testing.T) {
	x := newRingFanOut14Xing[chan int, int](4, 1, 2, false)
	ch, err := x.Add()
	if err != nil {
		t.Fatal(err)
	}
	x.dispatch(1)
	if !x.Remove(ch) {
		t.Fatal("Remove did not find active channel")
	}
	if x.Remove(ch) {
		t.Fatal("double Remove reported active")
	}
	ch2, err := x.Add()
	if err != nil {
		t.Fatal(err)
	}
	if ch2 != ch {
		t.Fatal("pool did not reuse the fixed channel")
	}
	select {
	case v := <-ch2:
		t.Fatalf("stale pooled value %d", v)
	default:
	}
	x.dispatch(2)
	if v := <-ch2; v != 2 {
		t.Fatalf("got %d want 2", v)
	}
	if _, err := x.Add(); err == nil {
		t.Fatal("pool exhaustion did not error")
	}
}

func TestRingFanOut14Close(t *testing.T) {
	x := NewRingFanOut14Xing[chan int, int](4, 2, 2)
	ch, err := x.Add()
	if err != nil {
		t.Fatal(err)
	}
	x.Close()
	x.Close()
	if err := x.Send(1); !errors.Is(err, ErrRingFanOut14Closed) {
		t.Fatalf("Send after Close=%v", err)
	}
	if _, err := x.Add(); !errors.Is(err, ErrRingFanOut14Closed) {
		t.Fatalf("Add after Close=%v", err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("pooled channel not closed")
	}
}

func TestBaselineAsyncFanoutFinalValue(t *testing.T) {
	x := NewBaselineAsyncFanout[int](4, 2)
	defer x.Close()
	id := x.Subscribe()
	defer x.Unsubscribe(id)
	for v := 1; v <= 100; v++ {
		x.Send(v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
	defer cancel()
	last := 0
	for last != 100 {
		select {
		case <-ctx.Done():
			t.Fatalf("final value not observed, last=%d", last)
		case v, ok := <-x.C(id):
			if !ok {
				t.Fatal("subscriber closed before final value")
			}
			if v <= last {
				t.Fatalf("non-monotonic %d after %d", v, last)
			}
			last = v
		}
	}
}

func BenchmarkRingFanOut14Admission(b *testing.B) {
	b.Run("input-full", func(b *testing.B) {
		x := newRingFanOut14Xing[chan int, int](8, 1, 8, false)
		if _, err := x.Add(); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < 8; i++ {
			_ = x.Send(i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = x.Send(i)
		}
		b.StopTimer()
		if got := x.InputDropped(); got != uint64(b.N) {
			b.Fatalf("dropped=%d want %d", got, b.N)
		}
	})
	b.Run("dispatch-full", func(b *testing.B) {
		x := newRingFanOut14Xing[chan int, int](8, 1, 8, false)
		ch, err := x.Add()
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < 8; i++ {
			x.dispatch(i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x.dispatch(i)
		}
		b.StopTimer()
		if got := x.DispatchDropped(); got != uint64(b.N) {
			b.Fatalf("dropped=%d want %d", got, b.N)
		}
		for len(ch) > 0 {
			<-ch
		}
	})
}

func FuzzRingFanOut14TwoStageModel(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 2})
	f.Add([]byte{0, 1, 0, 1, 2, 3, 0, 1, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 48 {
			return
		}
		x := newRingFanOut14Xing[chan int, int](4, 1, 2, false)
		ch, err := x.Add()
		if err != nil {
			t.Fatal(err)
		}
		var input, output []int
		var inDrop, outDrop uint64
		next := 1
		for _, op := range ops {
			switch op % 5 {
			case 0:
				if err := x.Send(next); err != nil {
					t.Fatal(err)
				}
				if len(input) == 4 {
					input = input[1:]
					inDrop++
				}
				input = append(input, next)
				next++
			case 1:
				if len(input) == 0 {
					continue
				}
				v := input[0]
				input = input[1:]
				<-x.input
				x.dispatch(v)
				if len(output) == 2 {
					output = output[1:]
					outDrop++
				}
				output = append(output, v)
			case 2:
				select {
				case v := <-ch:
					if len(output) == 0 || v != output[0] {
						t.Fatalf("unexpected mailbox value %d", v)
					}
					output = output[1:]
				default:
					if len(output) != 0 {
						t.Fatalf("mailbox empty, model has %v", output)
					}
				}
			case 3:
				if !x.Remove(ch) {
					t.Fatal("Remove failed")
				}
				output = nil
				ch, err = x.Add()
				if err != nil {
					t.Fatal(err)
				}
			case 4:
				if got := x.InputDropped(); got != inDrop {
					t.Fatalf("input drop=%d want %d", got, inDrop)
				}
				if got := x.DispatchDropped(); got != outDrop {
					t.Fatalf("dispatch drop=%d want %d", got, outDrop)
				}
			}
		}
		for _, want := range output {
			if v := <-ch; v != want {
				t.Fatalf("drain=%d want %d", v, want)
			}
		}
		select {
		case v := <-ch:
			t.Fatalf("extra drained value %d", v)
		default:
		}
	})
}
