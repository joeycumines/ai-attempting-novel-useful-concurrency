package bigbuff

import "testing"

func TestShardCast39MistralPolicies(t *testing.T) {
	t.Run("drop", func(t *testing.T) {
		h := NewShardCast39Mistral[int](ShardCastOptions39Mistral{Shards: 2, Buffer: 1, Policy: ShardCastPolicy39Drop})
		s := h.Subscribe()
		defer s.Unsubscribe()
		if n := h.Send(1); n != 1 {
			t.Fatalf("first Send=%d", n)
		}
		if n := h.Send(2); n != 0 {
			t.Fatalf("full-buffer Send=%d want drop", n)
		}
		if v, ok := s.TryRecv(); !ok || v != 1 {
			t.Fatalf("TryRecv=(%d,%v), want (1,true)", v, ok)
		}
	})
	t.Run("latest", func(t *testing.T) {
		h := NewShardCast39Mistral[int](ShardCastOptions39Mistral{Shards: 2, Buffer: 1, Policy: ShardCastPolicy39Latest})
		s := h.Subscribe()
		defer s.Unsubscribe()
		if n := h.Send(1); n != 1 {
			t.Fatalf("first Send=%d", n)
		}
		if n := h.Send(2); n != 1 {
			t.Fatalf("latest Send=%d", n)
		}
		if v, ok := s.TryRecv(); !ok || v != 2 {
			t.Fatalf("TryRecv=(%d,%v), want (2,true)", v, ok)
		}
	})
}

func TestShardCast39MistralUnsubscribeAndClose(t *testing.T) {
	h := NewShardCast39Mistral[int](ShardCastOptions39Mistral{Shards: 2, Buffer: 1, Policy: ShardCastPolicy39Latest})
	s := h.Subscribe()
	if h.Len() != 1 {
		t.Fatalf("Len=%d", h.Len())
	}
	s.Unsubscribe()
	s.Unsubscribe()
	if h.Len() != 0 {
		t.Fatalf("Len after unsubscribe=%d", h.Len())
	}
	h.Close()
	h.Close()
	if !h.Closed() {
		t.Fatal("Closed false after Close")
	}
	if n := h.Send(3); n != 0 {
		t.Fatalf("Send after Close=%d", n)
	}
}
