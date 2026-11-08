package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestChanRing47GeminiWidePayloadIntegrity(t *testing.T) {
	type payload struct {
		seq uint64
		a   [5]uint64
	}
	const messages = 2000

	ring := NewChanRing47Gemini[payload](2)
	reader := ring.Subscribe()
	defer reader.Close()

	values := make(chan payload, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if value, ok := reader.TryRecv(); ok {
				values <- value
				continue
			}
			if reader.Closed() {
				return
			}
			<-reader.Ready()
		}
	}()

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		for seq := uint64(1); seq <= messages; seq++ {
			value := payload{seq: seq}
			for i := range value.a {
				value.a[i] = seq + uint64(i)*17
			}
			if sent := ring.Send(value); sent != 1 {
				t.Errorf("Send(%d) = %d, want 1", seq, sent)
				return
			}
		}
	}()

	for seq := uint64(1); seq <= messages; seq++ {
		select {
		case value := <-values:
			want := payload{seq: seq}
			for i := range want.a {
				want.a[i] = seq + uint64(i)*17
			}
			if value != want {
				t.Fatalf("payload torn/reordered at %d: got %+v want %+v", seq, value, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for payload %d", seq)
		}
	}
	select {
	case <-publishDone:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not finish after all values were consumed")
	}
	ring.Close()
	select {
	case <-readerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not stop after ring close")
	}
}

func TestChanRing47GeminiSubscribeContextCancellation(t *testing.T) {
	ring := NewChanRing47Gemini[int](2)
	ctx, cancel := context.WithCancel(context.Background())
	seq := ring.SubscribeContext(ctx)
	if got := ring.Subscribers(); got != 1 {
		t.Fatalf("Subscribers immediately after SubscribeContext = %d, want 1", got)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		seq(func(int) bool { return true })
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled ring iterator did not return")
	}
	if got := ring.Subscribers(); got != 0 {
		t.Fatalf("Subscribers after cancelled iterator = %d, want 0", got)
	}
	ring.Close()
}

func TestChanRing47GeminiFullSlotReleasedByUnsubscribe(t *testing.T) {
	ring := NewChanRing47Gemini[int](2)
	slow := ring.Subscribe()
	if sent := ring.Send(1); sent != 1 {
		t.Fatalf("Send(1) = %d, want 1", sent)
	}
	if sent := ring.Send(2); sent != 1 {
		t.Fatalf("Send(2) = %d, want 1", sent)
	}
	if _, ok := ring.TrySend(3); ok {
		t.Fatal("TrySend succeeded while unread values filled the ring")
	}
	slow.Close()

	fresh := ring.Subscribe()
	defer fresh.Close()
	if fresh.Pending() != 0 {
		t.Fatalf("fresh reader inherited old history: pending=%d", fresh.Pending())
	}
	if sent := ring.Send(3); sent != 1 {
		t.Fatalf("Send after slow-reader unsubscribe = %d, want 1", sent)
	}
	if got, ok := fresh.TryRecv(); !ok || got != 3 {
		t.Fatalf("fresh reader got (%d, %v), want (3, true)", got, ok)
	}
	ring.Close()
}
