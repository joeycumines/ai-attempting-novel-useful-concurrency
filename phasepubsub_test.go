package bigbuff

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func awaitPhasePublication[V any](t *testing.T, x *PhasePubSub[V], epoch uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		x.mu.Lock()
		active, current := x.active, x.epoch
		x.mu.Unlock()
		if active && current == epoch {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("publication %d did not become active; current=%d active=%v", epoch, current, active)
		}
		runtime.Gosched()
	}
}

func awaitPhaseSend(t *testing.T, done <-chan int, want int) {
	t.Helper()
	select {
	case got := <-done:
		if got != want {
			t.Fatalf("Send returned %d, want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not complete")
	}
}

func phaseSend[V any](x *PhasePubSub[V], value V) <-chan int {
	done := make(chan int, 1)
	go func() { done <- x.Send(value) }()
	return done
}

func TestPhasePubSubOrderedDelivery(t *testing.T) {
	const readers, publications = 12, 300
	x := NewPhasePubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range readers {
		s := x.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			for want := range publications {
				select {
				case <-s.C():
					if got := s.Read(); got != want {
						t.Errorf("received %d, want %d", got, want)
						return
					}
				case <-ctx.Done():
					t.Error(ctx.Err())
					return
				}
			}
		}()
	}
	for value := range publications {
		if got := x.Send(value); got != readers {
			t.Errorf("publication %d: sent %d, want %d", value, got, readers)
			break
		}
	}
	cancel()
	wg.Wait()
	if got := x.Send(-1); got != 0 {
		t.Fatalf("empty Send returned %d", got)
	}
}

func TestPhasePubSubSharedSlotGracePeriod(t *testing.T) {
	x := NewPhasePubSub[int]()
	s := x.Subscribe()
	defer s.Unsubscribe()

	first := phaseSend(x, 101)
	<-s.C() // Deliberately pause before Read.
	awaitPhaseSend(t, first, 1)

	second := phaseSend(x, 202)
	awaitPhasePublication(t, x, 1)
	thirdStarted := make(chan struct{})
	third := make(chan int, 1)
	go func() {
		close(thirdStarted)
		third <- x.Send(303)
	}()
	<-thirdStarted

	// The next publication can start, but the third cannot reuse our slot
	// while we have not even read the first publication.
	select {
	case <-third:
		t.Fatal("third publication completed before the first Read")
	case <-time.After(10 * time.Millisecond):
	}
	if got := s.Read(); got != 101 {
		t.Fatalf("first shared value overwritten: got %d", got)
	}

	<-s.C() // Pause before Read again; the next slot may now be reused.
	awaitPhaseSend(t, second, 1)
	awaitPhasePublication(t, x, 2)
	if got := s.Read(); got != 202 {
		t.Fatalf("second shared value overwritten: got %d", got)
	}
	<-s.C()
	if got := s.Read(); got != 303 {
		t.Fatalf("third shared value: got %d", got)
	}
	awaitPhaseSend(t, third, 1)
}

func TestPhasePubSubMembershipDuringPublication(t *testing.T) {
	x := NewPhasePubSub[int]()
	a, b := x.Subscribe(), x.Subscribe()
	first := phaseSend(x, 10)
	awaitPhasePublication(t, x, 0)

	<-a.C()
	if got := a.Read(); got != 10 {
		t.Fatalf("first value: got %d", got)
	}
	a.Unsubscribe() // Already received: must not drain b's notification.

	c := x.Subscribe() // Joins publication 1 while b still owes publication 0.
	d := x.Subscribe()
	d.Unsubscribe() // Has no debt to the current publication.
	select {
	case <-c.C():
		t.Fatal("new subscriber stole a notification from the current publication")
	default:
	}
	b.Unsubscribe() // Has not received: must settle its current debt.
	awaitPhaseSend(t, first, 1)

	second := phaseSend(x, 20)
	<-c.C()
	if got := c.Read(); got != 20 {
		t.Fatalf("new subscriber's first value: got %d", got)
	}
	c.Unsubscribe()
	awaitPhaseSend(t, second, 1)
	if got := x.Send(30); got != 0 {
		t.Fatalf("Send after all subscriptions ended: got %d", got)
	}
}

func TestPhasePubSubConcurrentCancellation(t *testing.T) {
	const count = 32
	x := NewPhasePubSub[int]()
	s := make([]PhaseSubscription[int], count)
	for i := range s {
		s[i] = x.Subscribe()
	}
	done := phaseSend(x, 1)
	awaitPhasePublication(t, x, 0)
	var wg sync.WaitGroup
	for i := range s {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s[i].Unsubscribe()
			s[i].Unsubscribe()
		}()
	}
	awaitPhaseSend(t, done, 0)
	wg.Wait()
	for i := range s {
		if s[i].C() != nil {
			t.Fatalf("unsubscribed handle %d still has an enabled channel", i)
		}
	}
}

func TestPhasePubSubConcurrentSenders(t *testing.T) {
	const readers, writers, perWriter = 4, 4, 100
	const publications = writers * perWriter
	x := NewPhasePubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan []int, readers)
	for range readers {
		s := x.Subscribe()
		go func() {
			defer s.Unsubscribe()
			values := make([]int, 0, publications)
			for range publications {
				select {
				case <-s.C():
					values = append(values, s.Read())
				case <-ctx.Done():
					t.Error(ctx.Err())
					results <- values
					return
				}
			}
			results <- values
		}()
	}
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				if n := x.Send(writer*perWriter + i); n != readers {
					t.Errorf("Send returned %d, want %d", n, readers)
					return
				}
			}
		}()
	}
	wg.Wait()
	first := <-results
	if len(first) != publications {
		t.Fatalf("received %d publications, want %d", len(first), publications)
	}
	seen := make(map[int]bool, publications)
	for _, value := range first {
		if seen[value] {
			t.Fatalf("duplicate publication %d", value)
		}
		seen[value] = true
	}
	for range readers - 1 {
		other := <-results
		if !slices.Equal(other, first) {
			t.Fatal("subscribers observed different publication orders")
		}
	}
}

func TestPhasePubSubEpochWrap(t *testing.T) {
	x := NewPhasePubSub[int]()
	x.epoch = math.MaxUint64 - 1
	s := x.Subscribe()
	defer s.Unsubscribe()
	for want := range 4 {
		done := phaseSend(x, want)
		<-s.C()
		if got := s.Read(); got != want {
			t.Fatalf("across epoch wrap: got %d, want %d", got, want)
		}
		awaitPhaseSend(t, done, 1)
	}
}

func TestPhasePubSubAllocationFreeChurn(t *testing.T) {
	x := NewPhasePubSub[int]()
	if got := testing.AllocsPerRun(1000, func() {
		s := x.Subscribe()
		s.Unsubscribe()
	}); got != 0 {
		t.Fatalf("subscribe/unsubscribe: got %g allocations, want zero", got)
	}
}

// Copying a large inline value stresses the interval between handoff and Read,
// which is intentionally not part of PhasePubSub.Send's completion contract.
func TestPhasePubSubValueLargerThanChannelElement(t *testing.T) {
	const readers, rounds = 5, 7
	x := NewPhasePubSub[[65536]byte]()
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		s := x.Subscribe()
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			for sequence := 1; sequence <= rounds; sequence++ {
				<-s.C()
				value := s.Read()
				for j, got := range value {
					if want := byte(j*13 + sequence); got != want {
						t.Errorf("sequence %d byte %d = %d, want %d", sequence, j, got, want)
						break
					}
				}
			}
		}()
	}
	for sequence := 1; sequence <= rounds; sequence++ {
		var value [65536]byte
		for j := range value {
			value[j] = byte(j*13 + sequence)
		}
		if got := x.Send(value); got != readers {
			t.Errorf("Send = %d, want %d", got, readers)
		}
	}
	wg.Wait()
}

func ExamplePhasePubSub() {
	x := NewPhasePubSub[string]()
	s := x.Subscribe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		x.Send("hello")
	}()
	select {
	case <-s.C():
		fmt.Println(s.Read())
	}
	s.Unsubscribe()
	<-done
	// Output: hello
}
