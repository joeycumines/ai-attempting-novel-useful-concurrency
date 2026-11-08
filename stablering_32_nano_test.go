package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStableRing32CompetingDrain(t *testing.T) {
	r := NewStableRing32Nano(make(chan int), 8)
	const workers, msgs = 4, 200
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, workers)
	got := make(chan int, msgs)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Add(1)
			ready <- struct{}{}
			defer r.Add(-1)
			for {
				v, ok := r.Wait(ctx)
				if !ok {
					return
				}
				got <- v
			}
		}()
	}
	for i := 0; i < workers; i++ {
		<-ready
	}
	seen := make(map[int]int)
	for v := 1; v <= msgs; v++ {
		if n := r.Send(v); n != 1 {
			t.Fatalf("Send(%d)=%d", v, n)
		}
		select {
		case x := <-got:
			seen[x]++
		case <-time.After(scenarioGuard):
			t.Fatalf("value %d not dispatched", v)
		}
	}
	cancel()
	wg.Wait()
	if len(seen) != msgs || r.Dropped() != 0 {
		t.Fatalf("seen=%d dropped=%d", len(seen), r.Dropped())
	}
	for v, n := range seen {
		if n != 1 {
			t.Fatalf("value %d received %d times", v, n)
		}
	}
}

func TestStableRing32CancelAndBounds(t *testing.T) {
	r := NewStableRing32Nano(make(chan int), 8)
	if n := r.Send(1); n != 0 {
		t.Fatalf("no-receiver Send=%d", n)
	}
	r.Add(1)
	for v := 1; v <= 11; v++ {
		r.Send(v)
	}
	if r.Len() != 8 || r.Dropped() != 3 {
		t.Fatalf("len=%d dropped=%d", r.Len(), r.Dropped())
	}
	for want := 4; want <= 11; want++ {
		if v, ok := r.TryWait(); !ok || v != want {
			t.Fatalf("got (%d,%v) want %d", v, ok, want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := r.Wait(ctx); ok {
		t.Fatal("cancelled empty Wait returned value")
	}
	r.Add(-1)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("underflow did not panic")
			}
		}()
		r.Add(-1)
	}()
}

func BenchmarkStableRing32Dispatch(b *testing.B) {
	r := NewStableRing32Nano(make(chan int), 8)
	const workers = 8
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, workers)
	got := make(chan int, 1)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Add(1)
			ready <- struct{}{}
			defer r.Add(-1)
			for {
				v, ok := r.Wait(ctx)
				if !ok {
					return
				}
				got <- v
			}
		}()
	}
	for i := 0; i < workers; i++ {
		<-ready
	}
	b.ReportAllocs()
	b.ResetTimer()
	for v := 1; v <= b.N; v++ {
		r.Send(v)
		<-got
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}

func FuzzStableRing32QueueModel(f *testing.F) {
	f.Add([]byte{0, 1, 1, 2, 1, 2, 2})
	f.Add([]byte{1, 1, 1, 1, 1, 2, 0, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 40 {
			return
		}
		r := NewStableRing32Nano(make(chan int), 4)
		r.Add(1)
		var model []int
		var dropped uint64
		next := 1
		for _, op := range ops {
			switch op % 3 {
			case 0:
				if n := r.Send(next); n != 1 {
					t.Fatalf("Send=%d", n)
				}
				if len(model) == 4 {
					model = model[1:]
					dropped++
				}
				model = append(model, next)
				next++
			case 1:
				v, ok := r.TryWait()
				if len(model) == 0 {
					if ok {
						t.Fatalf("unexpected %d", v)
					}
					continue
				}
				if !ok || v != model[0] {
					t.Fatalf("TryWait=(%d,%v) want %d", v, ok, model[0])
				}
				model = model[1:]
			case 2:
				if got := r.Len(); got != len(model) {
					t.Fatalf("Len=%d want %d", got, len(model))
				}
				if got := r.Dropped(); got != dropped {
					t.Fatalf("Dropped=%d want %d", got, dropped)
				}
			}
		}
	})
}

func TestBoundedScatterEviction(t *testing.T) {
	for name, mk := range map[string]func() scatterLike{
		"StableRing32Nano": func() scatterLike {
			x := NewStableRing32Nano(make(chan int), 4)
			return scatterLike{send: x.Send, add: x.Add, wait: x.Wait, try: x.TryWait, dropped: x.Dropped}
		},
		"BaselineBoundedScatterQueue": func() scatterLike {
			x := NewBaselineBoundedScatterQueue[int](4)
			return scatterLike{send: x.Send, add: x.Add, wait: x.Wait, try: x.TryWait, dropped: x.Dropped}
		},
	} {
		t.Run(name, func(t *testing.T) {
			x := mk()
			x.add(1)
			for v := 1; v <= 9; v++ {
				if n := x.send(v); n != 1 {
					t.Fatalf("send=%d", n)
				}
			}
			if d := x.dropped(); d != 5 {
				t.Fatalf("dropped=%d want 5", d)
			}
			for want := 6; want <= 9; want++ {
				if v, ok := x.try(); !ok || v != want {
					t.Fatalf("got (%d,%v) want %d", v, ok, want)
				}
			}
			if _, ok := x.try(); ok {
				t.Fatal("queue not empty")
			}
		})
	}
}

func BenchmarkBoundedScatterAdmission(b *testing.B) {
	for name, mk := range scatterLikes() {
		if name != "StableRing32Nano (32-Nano)" && name != "BaselineBoundedScatterQueue (baseline)" {
			continue
		}
		b.Run(name, func(b *testing.B) {
			x := mk()
			x.add(1)
			for i := 0; i < 8; i++ {
				x.send(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.send(i)
			}
			b.StopTimer()
			if d := x.dropped(); d != uint64(b.N) {
				b.Fatalf("dropped=%d want %d", d, b.N)
			}
			for {
				if _, ok := x.try(); !ok {
					break
				}
			}
		})
	}
}

func TestBoundedScatterCompetingExactlyOne(t *testing.T) {
	for name, mk := range scatterLikes() {
		if name != "StableRing32Nano (32-Nano)" && name != "BaselineBoundedScatterQueue (baseline)" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			_ = mk // factory existence is checked by the registry; the facet owns its instance
			runScatterFacet(t, Attachment{Scenario: "bounded-competing-consumer-overflow", Impl: name}, 4, 100)
		})
	}
}

func FuzzBoundedScatterModel(f *testing.F) {
	f.Add([]byte{0, 1, 1, 2, 1, 2, 2})
	f.Add([]byte{1, 1, 1, 1, 1, 2, 0, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 40 {
			return
		}
		for name, mk := range scatterLikes() {
			if name != "StableRing32Nano (32-Nano)" && name != "BaselineBoundedScatterQueue (baseline)" {
				continue
			}
			x := mk()
			x.add(1)
			var model []int
			var dropped uint64
			next := 1
			for _, op := range ops {
				switch op % 3 {
				case 0:
					if n := x.send(next); n != 1 {
						t.Fatalf("%s: Send=%d", name, n)
					}
					if len(model) == 8 {
						model = model[1:]
						dropped++
					}
					model = append(model, next)
					next++
				case 1:
					v, ok := x.try()
					if len(model) == 0 {
						if ok {
							t.Fatalf("%s: unexpected %d", name, v)
						}
						continue
					}
					if !ok || v != model[0] {
						t.Fatalf("%s: try=(%d,%v) want %d", name, v, ok, model[0])
					}
					model = model[1:]
				case 2:
					if got := x.dropped(); got != dropped {
						t.Fatalf("%s: dropped=%d want %d", name, got, dropped)
					}
				}
			}
			for _, want := range model {
				if v, ok := x.try(); !ok || v != want {
					t.Fatalf("%s: drain=(%d,%v) want %d", name, v, ok, want)
				}
			}
			if _, ok := x.try(); ok {
				t.Fatalf("%s: model exhausted but queue returned a value", name)
			}
		}
	})
}
