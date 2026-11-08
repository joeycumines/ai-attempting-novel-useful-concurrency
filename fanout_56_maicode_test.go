package bigbuff

import (
	"sync"
	"testing"
	"time"
)

func TestFanout56BufferedAndClose(t *testing.T) {
	f := NewFanout56MaiCode[int]()
	s1, s2 := f.Subscribe(), f.Subscribe()
	if got := f.Send(42); got != 2 {
		t.Fatalf("Send(42) = %d, want 2", got)
	}
	for i, sub := range []*Subscription56MaiCode[int]{s1, s2} {
		if got := <-sub.Chan(); got != 42 {
			t.Fatalf("subscriber %d got %d, want 42", i, got)
		}
	}
	if got := f.Send(7); got != 2 {
		t.Fatalf("Send(7) = %d, want 2", got)
	}

	s1.Close()
	s1.Close()
	if f.Len() != 1 {
		t.Fatalf("Len after Close = %d, want 1", f.Len())
	}
	select {
	case <-s1.Done():
	default:
		t.Fatal("Done did not close")
	}

	// The remaining subscriber's one-element queue is full, so the next Send
	// must wait for its read; the closed subscriber is excluded from the new
	// snapshot and receives no later value.
	sendDone := make(chan int, 1)
	go func() { sendDone <- f.Send(8) }()
	if got := <-s2.Chan(); got != 7 {
		t.Fatalf("queued value = %d, want 7", got)
	}
	select {
	case got := <-sendDone:
		if got != 1 {
			t.Fatalf("Send(8) = %d, want 1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not proceed after freeing the remaining queue")
	}
	if got := <-s2.Chan(); got != 8 {
		t.Fatalf("remaining subscriber got %d, want 8", got)
	}
	if got := <-s1.Chan(); got != 7 {
		t.Fatalf("queued value should remain drainable after Close, got %d", got)
	}
	select {
	case value := <-s1.Chan():
		t.Fatalf("closed subscription received later value %d", value)
	default:
	}
	s2.Close()
	if got := f.Send(9); got != 0 {
		t.Fatalf("Send with no active subscriptions = %d, want 0", got)
	}
}

func TestFanout56BlockedSendReleasedByClose(t *testing.T) {
	f := NewFanout56MaiCode[int]()
	sub := f.Subscribe() // cap-one queue
	if got := f.Send(1); got != 1 {
		t.Fatalf("initial Send = %d, want 1", got)
	}
	done := make(chan int, 1)
	go func() { done <- f.Send(2) }() // blocks behind the full queue
	sub.Close()
	select {
	case got := <-done:
		if got != 0 {
			t.Fatalf("blocked Send counted %d deliveries after Close", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close failed to release a Send blocked on this subscription")
	}
	if got := <-sub.Chan(); got != 1 {
		t.Fatalf("queued pre-close value = %d, want 1", got)
	}
	select {
	case got := <-sub.Chan():
		t.Fatalf("value %d was sent after Close returned", got)
	default:
	}
}

func TestFanout56ConcurrentOrder(t *testing.T) {
	const subscribers, perPublisher = 2, 50
	f := NewFanout56MaiCode[int]()
	subs := make([]*Subscription56MaiCode[int], subscribers)
	for i := range subs {
		subs[i] = f.SubscribeChan(make(chan int, perPublisher*2))
	}
	var wg sync.WaitGroup
	counts := make(chan int, 2)
	for p := 0; p < 2; p++ {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			for i := 0; i < perPublisher; i++ {
				count += f.Send(p*perPublisher + i)
			}
			counts <- count
		}()
	}
	wg.Wait()
	close(counts)
	for count := range counts {
		if count != perPublisher*subscribers {
			t.Fatalf("publisher accepted %d sends, want %d", count, perPublisher*subscribers)
		}
	}
	values := make([][]int, subscribers)
	for i, sub := range subs {
		for j := 0; j < perPublisher*2; j++ {
			values[i] = append(values[i], <-sub.Chan())
		}
	}
	for i := 1; i < subscribers; i++ {
		for j := range values[0] {
			if values[i][j] != values[0][j] {
				t.Fatalf("subscriber order differs at %d: %d vs %d", j, values[0][j], values[i][j])
			}
		}
	}
	for _, sub := range subs {
		sub.Close()
	}
}

func TestFanout56ZeroValue(t *testing.T) {
	var f Fanout56MaiCode[int]
	sub := f.Subscribe()
	if got := f.Send(17); got != 1 {
		t.Fatalf("zero-value Send = %d, want 1", got)
	}
	if got := <-sub.Chan(); got != 17 {
		t.Fatalf("zero-value broadcaster delivered %d, want 17", got)
	}
	sub.Close()
}

func BenchmarkFanout56Send(b *testing.B) {
	const subscribers = 8
	f := NewFanout56MaiCode[int]()
	subs := make([]*Subscription56MaiCode[int], subscribers)
	started := make(chan struct{}, subscribers)
	stop := make(chan struct{})
	done := make(chan struct{}, subscribers)
	for i := range subs {
		subs[i] = f.Subscribe()
		sub := subs[i]
		go func() {
			defer func() { done <- struct{}{} }()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				case <-sub.Chan():
				}
			}
		}()
	}
	for range subs {
		<-started
	}
	for i := 0; i < 16; i++ {
		f.Send(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Send(i)
	}
	b.StopTimer()
	close(stop)
	for range subs {
		<-done
	}
	for _, sub := range subs {
		sub.Close()
	}
}
