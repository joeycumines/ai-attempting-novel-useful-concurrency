package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestChanHub46GrokBackpressureAndClose(t *testing.T) {
	h := NewChanHub46Grok[int](2)
	fast := h.Cursor()
	slow := h.Cursor()

	if seq := h.Publish(10); seq != 0 {
		t.Fatalf("first sequence = %d, want 0", seq)
	}
	if seq := h.Publish(11); seq != 1 {
		t.Fatalf("second sequence = %d, want 1", seq)
	}
	if _, ok := h.TryPublish(12); ok {
		t.Fatal("publish succeeded while both cursor slots were occupied")
	}

	if v, ok := slow.Peek(); !ok || v != 10 || slow.Seq() != 0 {
		t.Fatalf("slow cursor Peek = (%d, %v), seq=%d; want (10, true), seq=0", v, ok, slow.Seq())
	}
	if v, seq, ok := h.Load(); !ok || v != 11 || seq != 1 {
		t.Fatalf("Load = (%d, %d, %v), want (11, 1, true)", v, seq, ok)
	}
	if v, ok := fast.Next(); !ok || v != 10 {
		t.Fatalf("fast cursor first = (%d, %v), want (10, true)", v, ok)
	}
	if _, ok := h.TryPublish(12); ok {
		t.Fatal("publish succeeded while slow cursor still pinned sequence 0")
	}
	if v, ok := slow.Next(); !ok || v != 10 {
		t.Fatalf("slow cursor first = (%d, %v), want (10, true)", v, ok)
	}
	if seq, ok := h.TryPublish(12); !ok || seq != 2 {
		t.Fatalf("publish after both cursors advanced = (%d, %v), want (2, true)", seq, ok)
	}
	for _, c := range []*ChanHubCursor46Grok[int]{fast, slow} {
		for want := 11; want <= 12; want++ {
			if got, ok := c.Next(); !ok || got != want {
				t.Fatalf("cursor value = (%d, %v), want (%d, true)", got, ok, want)
			}
		}
	}

	waiting := h.Cursor()
	ready := waiting.Wait()
	closed := make(chan struct{})
	go func() {
		h.Close()
		close(closed)
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("Close did not wake a waiting cursor")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return")
	}
	if v, ok := waiting.Recv(); ok || v != 0 {
		t.Fatalf("closed cursor with no pending values = (%d, %v), want (0, false)", v, ok)
	}
	for _, c := range []*ChanHubCursor46Grok[int]{fast, slow, waiting} {
		c.Detach()
	}
}

func TestChanHub46GrokSubscribeContextCancellation(t *testing.T) {
	h := NewChanHub46Grok[int](2)
	ctx, cancel := context.WithCancel(context.Background())
	seq := h.SubscribeContext(ctx)
	if got := h.N(); got != 1 {
		t.Fatalf("subscriber count immediately after SubscribeContext = %d, want 1", got)
	}
	values := make([]int, 0, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		seq(func(v int) bool { values = append(values, v); return true })
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled iterator did not return")
	}
	if len(values) != 0 {
		t.Fatalf("cancelled iterator yielded %v, want no values", values)
	}
	if got := h.N(); got != 0 {
		t.Fatalf("subscriber count after cancelled iterator = %d, want 0", got)
	}
	h.Close()
}

func TestChanHub46GrokWidePayloadConcurrentRead(t *testing.T) {
	type payload struct {
		a, b, c, d, e, f, g, h uint64
	}
	const writes = 1000
	hub := NewChanHub46Grok[payload](2)
	cursor := hub.Cursor()
	defer cursor.Detach()
	got := make(chan payload, 1)
	go func() {
		for i := 0; i < writes; i++ {
			v, ok := cursor.Recv()
			if !ok {
				got <- payload{}
				return
			}
			got <- v
		}
	}()
	for i := uint64(1); i <= writes; i++ {
		want := payload{i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7}
		published := make(chan struct{})
		go func() { hub.Publish(want); close(published) }()
		select {
		case <-published:
		case <-time.After(time.Second):
			t.Fatal("Publish blocked despite an active receiver")
		}
		select {
		case got := <-got:
			if got != want {
				t.Fatalf("wide payload torn: got %+v want %+v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("cursor did not receive a published payload")
		}
	}
	hub.Close()
}
