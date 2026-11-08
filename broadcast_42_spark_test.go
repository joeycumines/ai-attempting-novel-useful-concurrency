package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestBroadcast42SparkBatchReplayAndMissed(t *testing.T) {
	b := NewBroadcast42Spark[int](4)
	if got := b.SendBatch([]int{1, 2, 3, 4, 5, 6}); got != 6 {
		t.Fatalf("SendBatch seq=%d", got)
	}
	r := b.NewReceiverFromOldest()
	if got := r.Pending(); got != 4 {
		t.Fatalf("Pending=%d want 4", got)
	}
	for want := 3; want <= 6; want++ {
		v, seq, ok := r.TryRecv()
		if !ok || v != want || seq != uint64(want) {
			t.Fatalf("TryRecv=(%d,%d,%v), want (%d,%d,true)", v, seq, ok, want, want)
		}
	}
	if got := r.Missed(); got != 0 {
		t.Fatalf("Missed=%d for explicit oldest replay", got)
	}
	late := b.NewReceiverFrom(1)
	if v, seq, ok := late.TryRecv(); !ok || v != 3 || seq != 3 {
		t.Fatalf("overrun TryRecv=(%d,%d,%v), want (3,3,true)", v, seq, ok)
	}
	if got := late.Missed(); got != 2 {
		t.Fatalf("Missed=%d want 2", got)
	}
	for want := 4; want <= 6; want++ {
		v, seq, ok := late.TryRecv()
		if !ok || v != want || seq != uint64(want) {
			t.Fatalf("drain=(%d,%d,%v), want (%d,%d,true)", v, seq, ok, want, want)
		}
	}
	b.Close()
	if _, _, ok := late.TryRecv(); ok {
		t.Fatal("unexpected value after draining closed broadcast")
	}
}

func TestBroadcast42SparkCloseWakesReceiver(t *testing.T) {
	b := NewBroadcast42Spark[int](2)
	r := b.NewReceiver()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, e := r.RecvContext(ctx); done <- e }()
	b.Close()
	select {
	case e := <-done:
		if e != ErrBroadcast42Closed {
			t.Fatalf("RecvContext error=%v", e)
		}
	case <-ctx.Done():
		t.Fatal("Close did not wake receiver")
	}
}
