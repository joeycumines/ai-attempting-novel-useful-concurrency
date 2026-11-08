package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestSelectCaster37GeminiPolicies(t *testing.T) {
	t.Run("sliding", func(t *testing.T) {
		c := NewSelectCaster37Gemini[int](WithSelectCaster37BufferSize(1), WithSelectCaster37Policy(SelectPolicy37Sliding))
		s := c.Subscribe()
		if n := c.Send(1); n != 1 {
			t.Fatalf("Send(1)=%d", n)
		}
		if n := c.Send(2); n != 1 {
			t.Fatalf("Send(2)=%d", n)
		}
		if got := <-s.C(); got != 2 {
			t.Fatalf("got %d want latest 2", got)
		}
		if got := s.Dropped(); got != 1 {
			t.Fatalf("Dropped=%d want 1", got)
		}
		s.Close()
		c.Close()
	})
	t.Run("dropping", func(t *testing.T) {
		c := NewSelectCaster37Gemini[int](WithSelectCaster37BufferSize(1), WithSelectCaster37Policy(SelectPolicy37Dropping))
		s := c.Subscribe()
		if n := c.Send(1); n != 1 {
			t.Fatalf("Send(1)=%d", n)
		}
		if n := c.Send(2); n != 0 {
			t.Fatalf("Send(2)=%d want dropped", n)
		}
		if got := <-s.C(); got != 1 {
			t.Fatalf("got %d want 1", got)
		}
		if got := s.Dropped(); got != 1 {
			t.Fatalf("Dropped=%d want 1", got)
		}
		s.Close()
		c.Close()
	})
	t.Run("blocking and close", func(t *testing.T) {
		c := NewSelectCaster37Gemini[int](WithSelectCaster37BufferSize(1), WithSelectCaster37Policy(SelectPolicy37Blocking))
		s := c.Subscribe()
		c.Send(1)
		done := make(chan int, 1)
		go func() { done <- c.Send(2) }()
		if got := <-s.C(); got != 1 {
			t.Fatalf("got %d want 1", got)
		}
		select {
		case n := <-done:
			if n != 1 {
				t.Fatalf("Send count=%d", n)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Send remained blocked after receive")
		}
		if got := <-s.C(); got != 2 {
			t.Fatalf("got %d want 2", got)
		}
		s.Close()
		c.Close()
	})
}

func TestSelectCaster37GeminiConcurrentSubscribeClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := NewSelectCaster37Gemini[int]()
		start := make(chan struct{})
		subscribed := make(chan *SelectSubscription37Gemini[int], 1)
		go func() {
			<-start
			defer func() {
				if recover() != nil {
					subscribed <- nil
				}
			}()
			subscribed <- c.Subscribe()
		}()
		close(start)
		c.Close()
		if s := <-subscribed; s != nil {
			s.Close()
		}
		if got := c.Len(); got != 0 {
			t.Fatalf("iteration %d: Len=%d after Close", i, got)
		}
	}
}

func TestSelectCaster37GeminiContextSubscription(t *testing.T) {
	c := NewSelectCaster37Gemini[int]()
	ctx, cancel := context.WithCancel(context.Background())
	s := c.SubscribeContext(ctx)
	if c.Len() != 1 {
		t.Fatalf("Len=%d want 1", c.Len())
	}
	cancel()
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("context did not close subscription")
	}
	c.Close()
}
