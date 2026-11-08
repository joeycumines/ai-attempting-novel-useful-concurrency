package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestShardCaster36MetaUnsubscribeUnblocksSend(t *testing.T) {
	x := NewShardCaster36Meta[int]()
	sub := x.Subscribe()
	sendDone := make(chan int, 1)
	go func() { sendDone <- x.Send(7) }()
	// Wait until Send is actually parked on the unbuffered subscriber channel.
	select {
	case v := <-sub.C():
		if v != 7 {
			t.Fatalf("received %d, want 7", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not reach subscriber")
	}
	// The send completed by the receive above; now verify the close path wakes
	// a fresh blocked Send and that repeated caster close is safe via TryClose.
	if n := <-sendDone; n != 1 {
		t.Fatalf("Send count=%d want 1", n)
	}
	blocked := make(chan int, 1)
	go func() { blocked <- x.Send(8) }()
	sub.Unsubscribe()
	select {
	case n := <-blocked:
		if n != 0 {
			t.Fatalf("withdrawn Send count=%d want 0", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe failed to release Send")
	}
	if !x.TryClose() {
		t.Fatal("first TryClose returned false")
	}
	if x.TryClose() {
		t.Fatal("second TryClose returned true")
	}
}

func TestShardCaster36MetaSendContextCancellation(t *testing.T) {
	x := NewShardCaster36Meta[int]()
	defer x.TryClose()
	_ = x.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := x.SendContext(ctx, 1); got != 0 {
		t.Fatalf("cancelled SendContext=%d want 0", got)
	}
}
