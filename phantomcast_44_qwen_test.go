package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPhantomCast44QwenIndependentCursorsAndLoss(t *testing.T) {
	p := NewPhantomCast44Qwen[int](4)
	a, b := p.Subscribe(), p.Subscribe()
	for i := 1; i <= 6; i++ {
		p.Send(i)
	}
	for name, s := range map[string]*PhantomSubscription44Qwen[int]{"a": a, "b": b} {
		v, ok := s.Next()
		if !ok || v != 3 {
			t.Fatalf("%s first=(%d,%v), want (3,true)", name, v, ok)
		}
		if got := s.Missed(); got != 2 {
			t.Fatalf("%s Missed=%d want 2", name, got)
		}
		for want := 4; want <= 6; want++ {
			v, ok = s.Next()
			if !ok || v != want {
				t.Fatalf("%s next=(%d,%v), want %d", name, v, ok, want)
			}
		}
	}
	a.Unsubscribe()
	a.Unsubscribe()
	b.Unsubscribe()
	if p.Len() != 0 {
		t.Fatalf("Len after unsubscribe=%d", p.Len())
	}
	p.Close()
	p.Close()
}

type phantom44Payload struct{ A, B, C, D uint64 }

func TestPhantomCast44QwenConcurrentWidePayload(t *testing.T) {
	p := NewPhantomCast44Qwen[phantom44Payload](64)
	s := p.Subscribe()
	const n = 2000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= n; i++ {
			p.Send(phantom44Payload{i, i, i, i})
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	last := uint64(0)
	for last < n {
		v, ok := s.Receive(ctx)
		if !ok {
			t.Fatalf("receive stopped at %d", last)
		}
		if v.A != v.B || v.B != v.C || v.C != v.D {
			t.Fatalf("torn payload: %+v", v)
		}
		if v.A <= last {
			t.Fatalf("non-increasing payload %d after %d", v.A, last)
		}
		last = v.A
	}
	wg.Wait()
	s.Unsubscribe()
	p.Close()
}

func TestPhantomCast44QwenCloseWakesReceive(t *testing.T) {
	p := NewPhantomCast44Qwen[int](2)
	s := p.Subscribe()
	done := make(chan bool, 1)
	go func() { _, ok := s.Receive(context.Background()); done <- ok }()
	p.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("receive succeeded after close with no data")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake receiver")
	}
}
