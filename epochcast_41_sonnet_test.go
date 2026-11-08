package bigbuff

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEpochCast41SonnetLossPolicies(t *testing.T) {
	x := NewEpochCast41Sonnet[int](4, EpochLossy41Sonnet)
	s := x.Subscribe()
	for i := 0; i < 10; i++ {
		if n, e := x.TryPublish(i); n != 1 || e != nil {
			t.Fatalf("TryPublish(%d)=(%d,%v)", i, n, e)
		}
	}
	if _, lost, st := s.TryRecv(); st != EpochLagged41Sonnet || lost != 6 {
		t.Fatalf("lag=(%d,%d), want (6,%d)", lost, st, EpochLagged41Sonnet)
	}
	for want := 6; want < 10; want++ {
		if got, _, st := s.TryRecv(); st != EpochValue41Sonnet || got != want {
			t.Fatalf("TryRecv=(%d,%d), want (%d,%d)", got, st, want, EpochValue41Sonnet)
		}
	}
	s.Close()
	x.Close()
}

func TestEpochCast41SonnetLosslessCancelAndClose(t *testing.T) {
	x := NewEpochCast41Sonnet[int](1, EpochLossless41Sonnet)
	s := x.Subscribe()
	if n, e := x.Publish(context.Background(), 1); n != 1 || e != nil {
		t.Fatalf("first Publish=(%d,%v)", n, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pub := make(chan error, 1)
	go func() { _, e := x.Publish(ctx, 2); pub <- e }()
	// Receiving releases the sole ring slot; the second publish must then
	// complete and the receiver sees values in order.
	if got, e := s.Recv(ctx); e != nil || got != 1 {
		t.Fatalf("first Recv=(%d,%v)", got, e)
	}
	select {
	case e := <-pub:
		if e != nil {
			t.Fatalf("second Publish: %v", e)
		}
	case <-ctx.Done():
		t.Fatal("publisher did not resume after a ring slot was released")
	}
	if got, e := s.Recv(ctx); e != nil || got != 2 {
		t.Fatalf("second Recv=(%d,%v)", got, e)
	}
	x.Close()
	if _, e := s.Recv(ctx); !errors.Is(e, ErrEpochClosed41Sonnet) {
		t.Fatalf("Recv after drained Close=%v", e)
	}
	s.Close()
}

func TestEpochCast41SonnetCancellationWakesParkedPublisher(t *testing.T) {
	x := NewEpochCast41Sonnet[int](1, EpochLossless41Sonnet)
	s := x.Subscribe()
	defer s.Close()
	defer x.Close()
	if _, e := x.Publish(context.Background(), 1); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := x.Publish(ctx, 2); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled publish error=%v", e)
	}
}
