package bigbuff

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChanCaster11MetaCASBoundaries(t *testing.T) {
	x := NewChanCaster11Meta(make(chan int))
	if n := x.Send(1); n != 0 {
		t.Fatalf("empty Send=%d", n)
	}
	const adders = 16
	if n := x.Add(adders); n != adders {
		t.Fatalf("Add=%d", n)
	}
	var real, stolen atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		x.Add(-1)
		stolen.Add(1)
	}()
	sendDone := make(chan int, 1)
	go func() {
		<-start
		sendDone <- x.Send(7)
	}()
	close(start)
	for range adders - 1 {
		select {
		case v := <-x.C:
			if v != 7 {
				t.Fatalf("value=%d", v)
			}
			real.Add(1)
		case <-time.After(scenarioGuard):
			t.Fatal("real receive blocked")
		}
	}
	var n int
	select {
	case n = <-sendDone:
	case <-time.After(scenarioGuard):
		t.Fatal("Send blocked after withdrawal")
	}
	wg.Wait()
	if n != adders-1 || real.Load() != int64(n) || stolen.Load() != 1 || x.Add(0) != 0 {
		t.Fatalf("Send=%d real=%d stolen=%d idle=%d", n, real.Load(), stolen.Load(), x.Add(0))
	}
}

// The source's receivers+MaxInt32 tracker aliases this legal idle state. A
// separate arming sentinel keeps arming distinct without consuming a channel.
func TestChanCaster11MetaHighIdleCountIsNotArmed(t *testing.T) {
	x := NewChanCaster11Meta(make(chan int))
	const count = math.MaxInt32
	x.state.Store(uint64(count)<<32 | uint64(count))
	if n := x.Add(0); n != count {
		t.Fatalf("idle count=%d", n)
	}
	if n := x.Add(-count); n != 0 {
		t.Fatalf("withdrawal count=%d", n)
	}
	select {
	case <-x.C:
		t.Fatal("idle withdrawal stole a rendezvous")
	default:
	}
}

func TestChanCaster11MetaBoundsRollback(t *testing.T) {
	x := NewChanCaster11Meta(make(chan int))
	x.Add(2)
	for _, tc := range []struct {
		name  string
		delta int
		want  int
	}{
		{"positive overflow", math.MaxInt32, 2},
		{"negative underflow", -3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("invalid Add did not panic")
				}
			}()
			x.Add(tc.delta)
		})
		if n := x.Add(0); n != tc.want {
			t.Fatalf("state after rollback=%d want %d", n, tc.want)
		}
	}
	x.Add(-2)
}
