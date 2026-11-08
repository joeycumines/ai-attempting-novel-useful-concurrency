package bigbuff

import (
	"testing"
	"time"
)

func TestChanPubSub35ChatJimmySingleReceiver(t *testing.T) {
	x := NewChanPubSub35ChatJimmy(make(chan int))
	sub := x.Subscribe()
	done := make(chan int, 1)
	go func() { done <- x.Send(73) }()
	select {
	case got := <-x.C():
		if got != 73 {
			t.Fatalf("got %d want 73", got)
		}
		x.Wait()
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not get the value")
	}
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("Send=%d want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not finish after Wait")
	}
	sub.Unsubscribe()
	x.Close()
}
