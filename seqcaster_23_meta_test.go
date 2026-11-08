package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestSeqCaster23MetaLossAndClose(t *testing.T) {
	x := NewSeqCaster23Meta[int](4, 3) // ring capacity 16
	sub := x.Subscribe()
	for i := 1; i <= 20; i++ {
		if got := x.Send(i); got != uint64(i) {
			t.Fatalf("Send(%d) sequence = %d", i, got)
		}
	}
	ctx := context.Background()
	v, ok, lost := sub.Next(ctx)
	if !ok || v != 6 || lost != 5 {
		t.Fatalf("first Next = (%d,%v,%d), want (6,true,5)", v, ok, lost)
	}
	if got := sub.Lost(); got != 5 {
		t.Fatalf("Lost() = %d, want 5", got)
	}
	for want := 7; want <= 20; want++ {
		v, ok, lost = sub.Next(ctx)
		if !ok || v != want || lost != 0 {
			t.Fatalf("Next = (%d,%v,%d), want (%d,true,0)", v, ok, lost, want)
		}
	}
	sub.Unsubscribe()
	x.Close()
	if v, ok, _ := sub.Next(ctx); ok {
		t.Fatalf("Next after Unsubscribe = %d,true", v)
	}
}

func TestSeqCaster23MetaCloseWakesWaiter(t *testing.T) {
	x := NewSeqCaster23Meta[int](4, 2)
	sub := x.Subscribe()
	result := make(chan bool, 1)
	go func() { _, ok, _ := sub.Next(context.Background()); result <- ok }()
	x.Close()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("Next returned a value on close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake a parked subscriber")
	}
}

func TestAdaptiveCaster23MetaRedirect(t *testing.T) {
	x := NewAdaptiveCaster23Meta[int](2)
	defer x.Close()
	if x.Redirecting() {
		t.Fatal("redirecting before threshold")
	}
	if got := x.Add(1); got != 1 || x.Redirecting() {
		t.Fatalf("Add(1) = %d, redirect=%v", got, x.Redirecting())
	}
	if got := x.Add(1); got != 2 || !x.Redirecting() {
		t.Fatalf("Add(1) = %d, redirect=%v", got, x.Redirecting())
	}
	if got := x.Add(-1); got != 1 || x.Redirecting() {
		t.Fatalf("Add(-1) = %d, redirect=%v", got, x.Redirecting())
	}
	sub := x.SubscribeSeq()
	defer x.UnsubscribeSeq(sub)
	gotSmall := make(chan int, 1)
	go func() { gotSmall <- <-x.C() }()
	x.Send(42)
	select {
	case got := <-gotSmall:
		if got != 42 {
			t.Fatalf("rendezvous path returned %d, want 42", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rendezvous path blocked despite its registered receiver")
	}
	v, ok, _ := sub.Next(context.Background())
	if !ok || v != 42 {
		t.Fatalf("sequence path returned (%d,%v), want (42,true)", v, ok)
	}
}

func BenchmarkSeqCaster23MetaSendNoSubscribers(b *testing.B) {
	x := NewSeqCaster23Meta[int](8, 4)
	defer x.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Send(i)
	}
}
