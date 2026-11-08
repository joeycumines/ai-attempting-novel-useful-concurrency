package bigbuff

import (
	"context"
	"testing"
	"time"
)

func TestSlotPubSub43GrokAcknowledgedFanout(t *testing.T) {
	x := NewSlotPubSub43Grok[int](2)
	subs := []*SlotSubscription43Grok[int]{x.Subscribe(), x.Subscribe()}
	started := make(chan struct{}, len(subs))
	received := make(chan int, len(subs))
	for _, s := range subs {
		go func(s *SlotSubscription43Grok[int]) {
			started <- struct{}{}
			v := <-s.C()
			s.Wait()
			received <- v
		}(s)
	}
	for range subs {
		<-started
	}
	n := x.Send(17)
	if n != 2 {
		t.Fatalf("Send=%d want 2", n)
	}
	for i := range subs {
		select {
		case v := <-received:
			if v != 17 {
				t.Fatalf("sub %d got %d", i, v)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not receive", i)
		}
	}
	for _, s := range subs {
		s.Unsubscribe()
	}
}

func TestSlotPubSub43GrokContextReceive(t *testing.T) {
	x := NewSlotPubSub43Grok[int](1)
	s := x.Subscribe()
	defer s.Unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := (&slotPubSub43Sub{sub: s}).Receive(ctx); ok {
		t.Fatal("Receive succeeded with cancelled context")
	}
}
