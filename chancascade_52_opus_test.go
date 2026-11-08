package bigbuff

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestChanCascade52FanoutAndHandoff(t *testing.T) {
	for _, receivers := range []int{1, 2, 3, 8, 32} {
		t.Run(strconv.Itoa(receivers), func(t *testing.T) {
			const messages = 30
			x := MakeChanCascade52Opus[int]()
			handles := make([]ChanCascadeReceiver52Opus[chan int, int], receivers)
			for i := range handles {
				handles[i] = x.Subscribe()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			readerDone := make(chan struct{}, receivers)
			errors := make(chan string, receivers*(messages+1))
			for i := range handles {
				r := handles[i] // transfer this handle to one goroutine before first use
				go func(id int, r ChanCascadeReceiver52Opus[chan int, int]) {
					defer func() {
						r.Unsubscribe()
						readerDone <- struct{}{}
					}()
					for want := 1; want <= messages; want++ {
						select {
						case value := <-r.C():
							r.Received(value)
							if value != want {
								errors <- "reader received an out-of-order value"
							}
						case <-ctx.Done():
							errors <- "reader timed out"
							return
						}
					}
				}(i, r)
			}

			publishDone := make(chan struct{})
			go func() {
				defer close(publishDone)
				for value := 1; value <= messages; value++ {
					if got := x.Send(value); got != receivers {
						errors <- "Send returned an incorrect delivery count"
					}
				}
			}()
			select {
			case <-publishDone:
			case <-ctx.Done():
				t.Fatal("cascade publisher failed to finish")
			}
			for range handles {
				select {
				case <-readerDone:
				case <-ctx.Done():
					t.Fatal("cascade readers failed to finish")
				}
			}
			close(errors)
			for message := range errors {
				t.Error(message)
			}
		})
	}
}

func TestChanCascade52JoinAndUnsubscribeDuringSend(t *testing.T) {
	x := MakeChanCascade52Opus[int]()
	r1 := x.Subscribe()

	firstDone := make(chan int, 1)
	go func() { firstDone <- x.Send(7) }()
	waitChanCascade52Sending(t, x)
	r2 := x.Subscribe() // joins the next phase, not the in-flight round
	if got := <-r1.C(); got != 7 {
		t.Fatalf("first receiver got %d, want 7", got)
	}
	r1.Received(7)
	if got := <-firstDone; got != 1 {
		t.Fatalf("in-flight Send delivered to %d receivers, want 1", got)
	}

	unsubscribeDone := make(chan struct{})
	go func() {
		defer close(unsubscribeDone)
		r2.Unsubscribe()
	}()
	// Publish to a second receiver pair only if both are still registered. The
	// second subscription is removed above, so its next message must go only
	// to r1; this also confirms phase advancement after the join boundary.
	secondDone := make(chan int, 1)
	go func() { secondDone <- x.Send(8) }()
	if got := <-r1.C(); got != 8 {
		t.Fatalf("remaining receiver got %d, want 8", got)
	}
	r1.Received(8)
	select {
	case <-unsubscribeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("next-round unsubscribe did not finish")
	}
	if got := <-secondDone; got != 1 {
		t.Fatalf("second Send delivered to %d receivers, want 1", got)
	}
	r1.Unsubscribe()

	// Force an active round whose second receiver withdraws before accepting
	// its relay. The withdrawing handle must absorb exactly one receipt.
	x = MakeChanCascade52Opus[int]()
	r1, r2 = x.Subscribe(), x.Subscribe()
	activeDone := make(chan int, 1)
	go func() { activeDone <- x.Send(9) }()
	waitChanCascade52Sending(t, x)
	unsubDone := make(chan struct{})
	go func() {
		defer close(unsubDone)
		r2.Unsubscribe()
	}()
	if got := <-r1.C(); got != 9 {
		t.Fatalf("active receiver got %d, want 9", got)
	}
	r1.Received(9)
	select {
	case <-unsubDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mid-send unsubscribe did not absorb its owed receipt")
	}
	if got := <-activeDone; got != 1 {
		t.Fatalf("mid-send Send counted %d deliveries, want 1 real receipt", got)
	}
	r1.Unsubscribe()
}

func waitChanCascade52Sending(t *testing.T, x *ChanCascade52Opus[chan int, int]) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for x.state.Load()&uint64(chanCascade52OpusSendingBit) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("sender did not publish the in-flight state")
		}
		runtime.Gosched()
	}
}

func TestChanCascade52DisabledInstancePanicsOnSubscribe(t *testing.T) {
	x := NewChanCascade52Opus[chan int, int](nil, nil)
	if got := x.Send(1); got != 0 {
		t.Fatalf("disabled Send = %d, want 0", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Subscribe on a disabled instance did not panic")
		}
	}()
	x.Subscribe()
}

func BenchmarkChanCascade52Send(b *testing.B) {
	const receivers = 8
	x := MakeChanCascade52Opus[int]()
	stop := make(chan struct{})
	done := make(chan struct{}, receivers)
	started := make(chan struct{}, receivers)
	for i := 0; i < receivers; i++ {
		r := x.Subscribe()
		go func(r ChanCascadeReceiver52Opus[chan int, int]) {
			defer func() {
				r.Unsubscribe()
				done <- struct{}{}
			}()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				case value := <-r.C():
					r.Received(value)
				}
			}
		}(r)
	}
	for i := 0; i < receivers; i++ {
		<-started
	}
	for i := 0; i < 16; i++ { // pre-warm receiver goroutine stacks outside measurement
		x.Send(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Send(i)
	}
	b.StopTimer()
	close(stop)
	for i := 0; i < receivers; i++ {
		<-done
	}
}
