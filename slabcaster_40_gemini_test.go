package bigbuff

import (
	"context"
	"testing"
)

func TestSlabCaster40GeminiPoliciesAndClose(t *testing.T) {
	sc := NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(1), WithSlabCaster40Policy(SlabPolicy40DropOldest))
	sub := sc.Subscribe()
	if got := sc.Send(1); got != 1 {
		t.Fatalf("Send(1)=%d", got)
	}
	if got := sc.Send(2); got != 1 {
		t.Fatalf("Send(2)=%d", got)
	}
	if got := <-sub.C(); got != 2 {
		t.Fatalf("got %d want newest 2", got)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("channel remained open after caster Close")
		}
	default:
		t.Fatal("closed channel not observable")
	}
}

func TestSlabCaster40GeminiDropNewest(t *testing.T) {
	sc := NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(1), WithSlabCaster40Policy(SlabPolicy40DropNewest))
	sub := sc.Subscribe()
	defer sc.Close()
	if n := sc.Send(4); n != 1 {
		t.Fatalf("Send(4)=%d", n)
	}
	if n := sc.Send(5); n != 0 {
		t.Fatalf("full DropNewest Send=%d want 0", n)
	}
	if got := <-sub.C(); got != 4 {
		t.Fatalf("got %d want 4", got)
	}
}

func TestSlabCaster40GeminiIteratorCancellation(t *testing.T) {
	sc := NewSlabCaster40Gemini[int]()
	ctx, cancel := context.WithCancel(context.Background())
	seq := sc.SubscribeContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range seq {
			return
		}
	}()
	cancel()
	<-done
	if got := sc.Subscribers(); got != 0 {
		t.Fatalf("Subscribers=%d after cancellation", got)
	}
	_ = sc.Close()
}
