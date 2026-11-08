package bigbuff

import (
	"testing"
	"time"
)

func TestEpochBroadcast54Fanout(t *testing.T) {
	const subscribers, messages = 8, 40
	x := NewEpochBroadcast54Terra[int]()
	subs := make([]*EpochSubscription54Terra[int], subscribers)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	stop := make(chan struct{})
	readerDone := make(chan struct{}, subscribers)
	results := make([][]int, subscribers)
	for i, sub := range subs {
		go func(i int, sub *EpochSubscription54Terra[int]) {
			defer func() {
				sub.Close()
				readerDone <- struct{}{}
			}()
			for {
				select {
				case <-stop:
					return
				case <-sub.Ready():
					results[i] = append(results[i], sub.Next())
				}
			}
		}(i, sub)
	}

	counts := make(chan int, messages)
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		for value := 1; value <= messages; value++ {
			counts <- x.Send(value)
		}
	}()
	select {
	case <-publishDone:
	case <-time.After(5 * time.Second):
		close(stop)
		t.Fatal("publisher did not finish fan-out")
	}
	for i := 0; i < messages; i++ {
		if got := <-counts; got != subscribers {
			t.Errorf("Send count = %d, want %d", got, subscribers)
		}
	}
	close(stop)
	for range subs {
		select {
		case <-readerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("readers did not stop")
		}
	}
	for i, values := range results {
		if len(values) != messages {
			t.Errorf("subscriber %d got %d values, want %d", i, len(values), messages)
			continue
		}
		for j, value := range values {
			if value != j+1 {
				t.Errorf("subscriber %d value[%d] = %d, want %d", i, j, value, j+1)
				break
			}
		}
	}
}

func TestEpochBroadcast54MembershipAndUnsubscribe(t *testing.T) {
	x := NewEpochBroadcast54Terra[int]()
	if got := x.Send(0); got != 0 {
		t.Fatalf("Send with no subscribers = %d, want 0", got)
	}
	r1 := x.Subscribe()
	first := make(chan int, 1)
	go func() { first <- x.Send(1) }()
	select {
	case <-r1.Ready(): // proves the first publication snapshot is active
	case <-time.After(5 * time.Second):
		t.Fatal("first publication did not become ready")
	}
	r2 := x.Subscribe() // starts at the next epoch, not the in-flight one
	if got := r1.Next(); got != 1 {
		t.Fatalf("first subscriber got %d, want 1", got)
	}
	if got := <-first; got != 1 {
		t.Fatalf("first Send count = %d, want 1", got)
	}
	select {
	case <-r2.Ready():
		t.Fatal("subscriber added during Send joined the in-flight epoch")
	default:
	}

	second := make(chan int, 1)
	go func() { second <- x.Send(2) }()
	select {
	case <-r1.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("second publication did not become ready for first subscriber")
	}
	select {
	case <-r2.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("second publication did not become ready for late subscriber")
	}
	if got := r2.Next(); got != 2 {
		t.Fatalf("late subscriber got %d, want 2", got)
	}
	if got := r1.Next(); got != 2 {
		t.Fatalf("first subscriber got %d, want 2", got)
	}
	if got := <-second; got != 2 {
		t.Fatalf("second Send count = %d, want 2", got)
	}
	r1.Close()
	r1.Close() // source-level Close is idempotent
	r2.Close()

	// Closing a subscription with an outstanding receive obligation releases
	// that share but does not count it as a delivered value.
	y := NewEpochBroadcast54Terra[int]()
	r3, r4 := y.Subscribe(), y.Subscribe()
	active := make(chan int, 1)
	go func() { active <- y.Send(3) }()
	select {
	case <-r3.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("active publication did not start")
	}
	r4.Close()
	if got := r3.Next(); got != 3 {
		t.Fatalf("remaining subscriber got %d, want 3", got)
	}
	if got := <-active; got != 1 {
		t.Fatalf("active Send count = %d, want 1", got)
	}
	r3.Close()

	var zero EpochBroadcast54Terra[int]
	mustPanic(t, func() { zero.Subscribe() })
}

func BenchmarkEpochBroadcast54Send(b *testing.B) {
	const subscribers = 8
	x := NewEpochBroadcast54Terra[int]()
	subs := make([]*EpochSubscription54Terra[int], subscribers)
	started := make(chan struct{}, subscribers)
	stop := make(chan struct{})
	done := make(chan struct{}, subscribers)
	for i := range subs {
		subs[i] = x.Subscribe()
		sub := subs[i]
		go func() {
			defer func() { done <- struct{}{} }()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				case <-sub.Ready():
					sub.Next()
				}
			}
		}()
	}
	for range subs {
		<-started
	}
	x.Send(0) // warm receiver goroutines outside the measured loop
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x.Send(i)
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
