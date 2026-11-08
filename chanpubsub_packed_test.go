package bigbuff

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func waitPackedState(t *testing.T, x *ChanPubSubPacked[chan int, int], wantCount uint32, wantPhase uint32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		count, phase := unpackChanPubSubPackedState(x.state.Load())
		if count == wantCount && phase == wantPhase {
			return
		}
		runtime.Gosched()
	}
	count, phase := unpackChanPubSubPackedState(x.state.Load())
	t.Fatalf("state = (count=%d, phase=%d), want (count=%d, phase=%d)", count, phase, wantCount, wantPhase)
}

func TestChanPubSubPackedBroadcast(t *testing.T) {
	c := make(chan int)
	x := NewChanPubSubPacked(c)
	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		x.Subscribe()
		go func() {
			defer wg.Done()
			v, ok := <-x.C()
			if !ok {
				t.Error("unexpected closed channel")
				return
			}
			if v != 42 {
				t.Errorf("received %d, want 42", v)
			}
			x.Wait()
			x.Unsubscribe()
		}()
	}
	if got := x.Send(42); got != n {
		t.Fatalf("Send returned %d, want %d", got, n)
	}
	wg.Wait()
	if got := x.Add(0); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

func TestChanPubSubPackedRemovalDuringSend(t *testing.T) {
	c := make(chan int)
	x := NewChanPubSubPacked(c)
	x.Add(2)

	sendDone := make(chan int, 1)
	go func() { sendDone <- x.Send(7) }()
	waitPackedState(t, x, 2, chanPubSubPackedPhaseSending)

	removeDone := make(chan int, 1)
	go func() { removeDone <- x.Add(-1) }()
	// Once count is one, the remover has committed its CAS and is the
	// receiver for exactly one of the two sends.
	waitPackedState(t, x, 1, chanPubSubPackedPhaseSending)

	v := <-c
	if v != 7 {
		t.Fatalf("received %d, want 7", v)
	}
	x.Wait()

	if got := <-removeDone; got != 1 {
		t.Fatalf("Add(-1) returned %d, want 1", got)
	}
	if got := <-sendDone; got != 1 {
		t.Fatalf("Send returned %d, want 1", got)
	}
	x.Unsubscribe()
	if got := x.Add(0); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

func TestChanPubSubPackedConcurrentRemovalsDuringSend(t *testing.T) {
	const subscribers = 32
	const removals = 16
	c := make(chan int)
	x := NewChanPubSubPacked(c)
	x.Add(subscribers)

	sendDone := make(chan int, 1)
	go func() { sendDone <- x.Send(31) }()
	waitPackedState(t, x, subscribers, chanPubSubPackedPhaseSending)

	var removeWG sync.WaitGroup
	removeWG.Add(removals)
	for range removals {
		go func() {
			defer removeWG.Done()
			x.Unsubscribe()
		}()
	}
	waitPackedState(t, x, subscribers-removals, chanPubSubPackedPhaseSending)

	var receiveWG sync.WaitGroup
	receiveWG.Add(subscribers - removals)
	for range subscribers - removals {
		go func() {
			defer receiveWG.Done()
			v := <-c
			if v != 31 {
				t.Errorf("received %d, want 31", v)
			}
			x.Wait()
			x.Unsubscribe()
		}()
	}

	if got := <-sendDone; got != subscribers-removals {
		t.Fatalf("Send returned %d, want %d", got, subscribers-removals)
	}
	removeWG.Wait()
	receiveWG.Wait()
	if got := x.Add(0); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

func TestChanPubSubPackedSubscribeDuringAcknowledgement(t *testing.T) {
	c := make(chan int)
	x := NewChanPubSubPacked(c)
	x.Subscribe()

	firstReceived := make(chan struct{})
	allowFirstWait := make(chan struct{})
	firstReceiverDone := make(chan struct{})
	go func() {
		defer close(firstReceiverDone)
		v := <-c
		if v != 1 {
			t.Errorf("first receiver got %d, want 1", v)
		}
		close(firstReceived)
		<-allowFirstWait
		x.Wait()
	}()

	firstSendDone := make(chan int, 1)
	go func() { firstSendDone <- x.Send(1) }()
	select {
	case <-firstReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("first value was not received")
	}

	// Send has completed its channel rendezvous and is waiting for Wait.
	// Positive registration must already be allowed, but it must not be
	// counted as a recipient of the in-flight broadcast.
	added := make(chan int, 1)
	go func() { added <- x.Add(1) }()
	select {
	case got := <-added:
		if got != 2 {
			t.Fatalf("Add(1) returned %d, want 2", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Add(1) blocked behind the acknowledgement barrier")
	}

	close(allowFirstWait)
	<-firstReceiverDone
	if got := <-firstSendDone; got != 1 {
		t.Fatalf("first Send returned %d, want 1", got)
	}

	var receiveWG sync.WaitGroup
	receiveWG.Add(2)
	for range 2 {
		go func() {
			defer receiveWG.Done()
			v := <-c
			if v != 2 {
				t.Errorf("second receiver got %d, want 2", v)
			}
			x.Wait()
			x.Unsubscribe()
		}()
	}
	if got := x.Send(2); got != 2 {
		t.Fatalf("second Send returned %d, want 2", got)
	}
	receiveWG.Wait()
	if got := x.Add(0); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

func TestChanPubSubPackedSelectAndFinalizingBoundary(t *testing.T) {
	c := make(chan int)
	x := NewChanPubSubPacked(c)
	x.Subscribe()

	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		select {
		case v := <-x.C():
			if v != 19 {
				t.Errorf("received %d, want 19", v)
			}
			x.Wait()
		case <-time.After(3 * time.Second):
			t.Error("select receive timed out")
		}
	}()
	if got := x.Send(19); got != 1 {
		t.Fatalf("Send returned %d, want 1", got)
	}
	<-recvDone
	// The receiver has acknowledged the value. This removal must be an
	// ordinary count decrement, never a compensating channel receive.
	x.Unsubscribe()
	if got := x.Add(0); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

func TestChanPubSubPackedContextCancellationBeforeIteration(t *testing.T) {
	x := NewChanPubSubPacked(make(chan int))
	ctx, cancel := context.WithCancel(context.Background())
	seq := x.SubscribeContext(ctx)
	cancel()
	waitPackedState(t, x, 0, chanPubSubPackedPhaseIdle)
	// Calling the already-cancelled single-use iterator must not unsubscribe
	// a second time.
	for range seq {
		t.Fatal("unexpected value")
	}
}

func TestChanPubSubPackedContextCancellationWhileWaiting(t *testing.T) {
	x := NewChanPubSubPacked(make(chan int))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seq := x.SubscribeContext(ctx)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for range seq {
		}
	}()
	waitPackedState(t, x, 1, chanPubSubPackedPhaseIdle)
	cancel()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("iterator did not stop after cancellation")
	}
	waitPackedState(t, x, 0, chanPubSubPackedPhaseIdle)
}

func TestChanPubSubPackedContextCancellationDoesNotUnsubscribeBeforeWait(t *testing.T) {
	c := make(chan int)
	x := NewChanPubSubPacked(c)
	ctx, cancel := context.WithCancel(context.Background())
	seq := x.SubscribeContext(ctx)

	received := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for v := range seq {
			if v != 23 {
				t.Errorf("received %d, want 23", v)
			}
			close(received)
			// Keep the iterator in its yield callback after Wait. Cancellation
			// must not invoke Unsubscribe concurrently with the active receive.
			time.Sleep(20 * time.Millisecond)
			return
		}
	}()
	waitPackedState(t, x, 1, chanPubSubPackedPhaseIdle)

	sendDone := make(chan int, 1)
	go func() { sendDone <- x.Send(23) }()
	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("iterator did not receive")
	}
	cancel()
	if got := <-sendDone; got != 1 {
		t.Fatalf("Send returned %d, want 1", got)
	}
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("iterator did not finish")
	}
	waitPackedState(t, x, 0, chanPubSubPackedPhaseIdle)
}

func TestChanPubSubPackedConcurrentContextChurn(t *testing.T) {
	x := NewChanPubSubPacked(make(chan int))
	const workers = 12
	const rounds = 80

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	cancels := make([]context.CancelFunc, 0, workers)
	for range workers {
		workerCtx, workerCancel := context.WithCancel(ctx)
		cancels = append(cancels, workerCancel)
		seq := x.SubscribeContext(workerCtx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range seq {
			}
		}()
	}
	waitPackedState(t, x, workers, chanPubSubPackedPhaseIdle)

	for i := range rounds {
		if got := x.Send(i); got < 0 || got > workers {
			t.Fatalf("Send returned impossible count %d", got)
		}
		if i%5 == 0 {
			idx := (i / 5) % len(cancels)
			cancels[idx]()
			workerCtx, workerCancel := context.WithCancel(ctx)
			cancels[idx] = workerCancel
			seq := x.SubscribeContext(workerCtx)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range seq {
				}
			}()
		}
	}
	for _, workerCancel := range cancels {
		workerCancel()
	}
	cancel()
	wg.Wait()
	waitPackedState(t, x, 0, chanPubSubPackedPhaseIdle)
}

func TestChanPubSubPackedRejectsBufferedChannel(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for buffered channel")
		}
	}()
	_ = NewChanPubSubPacked(make(chan int, 1))
}

func BenchmarkChanPubSubPacked(b *testing.B) {
	for _, subscribers := range []int{1, 4, 16, 64} {
		b.Run(strconv.Itoa(subscribers), func(b *testing.B) {
			c := make(chan int)
			x := NewChanPubSubPacked(c)
			var wg sync.WaitGroup
			for range subscribers {
				x.Subscribe()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range c {
						x.Wait()
					}
				}()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(i)
			}
			b.StopTimer()
			close(c)
			wg.Wait()
		})
	}
}

func BenchmarkCallbackChannelFanout(b *testing.B) {
	for _, subscribers := range []int{1, 4, 16, 64} {
		b.Run(strconv.Itoa(subscribers), func(b *testing.B) {
			channels := make([]chan int, subscribers)
			var wg sync.WaitGroup
			for i := range channels {
				channels[i] = make(chan int)
				wg.Add(1)
				go func(c <-chan int) {
					defer wg.Done()
					for range c {
					}
				}(channels[i])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, c := range channels {
					c <- i
				}
			}
			b.StopTimer()
			for _, c := range channels {
				close(c)
			}
			wg.Wait()
		})
	}
}
