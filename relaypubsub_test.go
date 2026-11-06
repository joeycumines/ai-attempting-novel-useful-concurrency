/*
Copyright 2026 Joseph Cumines

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bigbuff

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func relayAssertBlocked[T any](t *testing.T, c <-chan T, operation string) {
	t.Helper()
	select {
	case got := <-c:
		t.Fatalf("%s completed unexpectedly: %v", operation, got)
	default:
	}
}

func relayAssertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected a panic")
		}
	}()
	f()
}

func TestRelayPubSubEmptyAndHandleLifecycle(t *testing.T) {
	overflowWidth := int64(math.MaxInt32) + 1
	for _, width := range []int{0, -1, int(overflowWidth)} {
		t.Run(fmt.Sprintf("invalid-width=%d", width), func(t *testing.T) {
			relayAssertPanics(t, func() { NewRelayPubSub[int](width) })
		})
	}
	var uninitialized RelayPubSub[int]
	relayAssertPanics(t, func() { uninitialized.Subscribe() })
	relayAssertPanics(t, func() { uninitialized.Send(1) })

	var zero RelayPubSubSubscription[int]
	if zero.C() != nil {
		t.Fatal("zero subscription has a nonnil channel")
	}
	zero.Unsubscribe()
	relayAssertPanics(t, func() { zero.Read() })

	bus := NewRelayPubSub[int](1)
	if got := bus.Send(1); got != 0 {
		t.Fatalf("empty Send = %d, want 0", got)
	}
	sub := bus.Subscribe()
	if sub.C() == nil {
		t.Fatal("active subscription has a nil channel")
	}
	sub.Unsubscribe()
	sub.Unsubscribe()
	if sub.C() != nil {
		t.Fatal("unsubscribed handle has a nonnil channel")
	}
	if got := bus.Send(2); got != 0 {
		t.Fatalf("Send after unsubscribe = %d, want 0", got)
	}
	relayAssertPanics(t, func() { sub.Read() })
}

// Holding a received notification simulates preemption between the select
// operation and Read. Delivery is incomplete until the payload has been copied.
func TestRelayPubSubWaitsForRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewRelayPubSub[int](1)
		sub := bus.Subscribe()
		defer sub.Unsubscribe()
		for _, value := range []int{101, 202, 303} {
			sent := make(chan int, 1)
			go func() { sent <- bus.Send(value) }()
			<-sub.C()
			synctest.Wait()
			relayAssertBlocked(t, sent, "Send before Read")
			if got := sub.Read(); got != value {
				t.Fatalf("Read = %d, want %d", got, value)
			}
			if got := <-sent; got != 1 {
				t.Fatalf("Send = %d, want 1", got)
			}
			relayAssertBlocked(t, sub.C(), "next publication notification")
		}
	})
}

// A cancellation cannot merely decrement the reader count: with width one,
// the only credit may already belong to a different, preempted reader.
func TestRelayPubSubCancellationWaitsForCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewRelayPubSub[int](1)
		held := bus.Subscribe()
		defer held.Unsubscribe()
		canceled := bus.Subscribe()
		sent := make(chan int, 1)
		go func() { sent <- bus.Send(41) }()
		<-held.C()

		unsubscribed := make(chan struct{})
		go func() {
			canceled.Unsubscribe()
			close(unsubscribed)
		}()
		synctest.Wait()
		relayAssertBlocked(t, unsubscribed, "Unsubscribe without a credit")
		relayAssertBlocked(t, sent, "Send with one unread payload")

		if got := held.Read(); got != 41 {
			t.Fatalf("Read = %d, want 41", got)
		}
		<-unsubscribed
		if canceled.C() != nil {
			t.Fatal("canceled handle remains active")
		}
		if got := <-sent; got != 1 {
			t.Fatalf("Send = %d, want one copied value", got)
		}

		go func() { sent <- bus.Send(42) }()
		<-held.C()
		if got := held.Read(); got != 42 {
			t.Fatalf("next Read = %d, want 42", got)
		}
		if got := <-sent; got != 1 {
			t.Fatalf("next Send = %d, want 1", got)
		}
	})
}

func TestRelayPubSubJoiningSkipsInProgressPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := NewRelayPubSub[int](2)
		first := bus.Subscribe()
		defer first.Unsubscribe()
		sent := make(chan int, 1)
		go func() { sent <- bus.Send(11) }()
		oldChannel := first.C()
		<-oldChannel

		// The first publication has certainly started, but cannot finish.
		second := bus.Subscribe()
		defer second.Unsubscribe()
		if second.C() == oldChannel {
			t.Fatal("new subscriber joined the in-progress phase")
		}
		relayAssertBlocked(t, second.C(), "new subscriber's first notification")
		temporary := bus.Subscribe()
		temporary.Unsubscribe() // Does not owe a credit from the old phase.
		if got := first.Read(); got != 11 {
			t.Fatalf("first Read = %d, want 11", got)
		}
		if got := <-sent; got != 1 {
			t.Fatalf("first Send = %d, want 1", got)
		}

		go func() { sent <- bus.Send(22) }()
		<-second.C()
		if got := second.Read(); got != 22 {
			t.Fatalf("new subscriber Read = %d, want 22", got)
		}
		synctest.Wait()
		relayAssertBlocked(t, sent, "next Send before first subscriber Read")
		<-first.C()
		if got := first.Read(); got != 22 {
			t.Fatalf("continuing subscriber Read = %d, want 22", got)
		}
		if got := <-sent; got != 2 {
			t.Fatalf("next Send = %d, want 2", got)
		}
	})
}

func TestRelayPubSubAllSubscribersCancel(t *testing.T) {
	for _, width := range []int{1, 3, 32} {
		for _, n := range []int{1, 2, 7, 31, 32, 33, 64} {
			t.Run(fmt.Sprintf("width=%d/subscribers=%d", width, n), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					bus := NewRelayPubSub[int](width)
					subs := make([]RelayPubSubSubscription[int], n)
					for i := range subs {
						subs[i] = bus.Subscribe()
					}
					sent := make(chan int, 1)
					go func() { sent <- bus.Send(10) }()
					synctest.Wait()
					relayAssertBlocked(t, sent, "Send without any receivers")
					var wg sync.WaitGroup
					wg.Add(n)
					for i := range subs {
						go func(s *RelayPubSubSubscription[int]) {
							defer wg.Done()
							s.Unsubscribe()
						}(&subs[i])
					}
					wg.Wait()
					if got := <-sent; got != 0 {
						t.Fatalf("Send = %d, want 0 copied values", got)
					}
					for i := range subs {
						if subs[i].C() != nil {
							t.Fatalf("subscription %d remains active", i)
						}
					}

					// A fully canceled round must leave no reusable-channel residue.
					next := bus.Subscribe()
					defer next.Unsubscribe()
					go func() { sent <- bus.Send(20) }()
					<-next.C()
					if got := next.Read(); got != 20 {
						t.Fatalf("Read after cancellation = %d, want 20", got)
					}
					if got := <-sent; got != 1 {
						t.Fatalf("Send after cancellation = %d, want 1", got)
					}
				})
			})
		}
	}
}

type relayTestPayload struct {
	Sequence uint64
	Words    [128]uint64
	Name     string
	Pointer  *uint64
	Bytes    []byte
}

func TestRelayPubSubOrderedPayloadDelivery(t *testing.T) {
	for _, tc := range []struct{ n, width int }{
		{1, 1}, {2, 1}, {7, 1}, {7, 3}, {31, 32}, {32, 32},
		{33, 32}, {65, 8}, {257, 32},
	} {
		t.Run(fmt.Sprintf("width=%d/subscribers=%d", tc.width, tc.n), func(t *testing.T) {
			const rounds = 128
			const marker = uint64(0x43d6f728ab52ec91)
			pointed := marker
			immutableBytes := []byte("immutable pointed data")
			bus := NewRelayPubSub[relayTestPayload](tc.width)
			subs := make([]RelayPubSubSubscription[relayTestPayload], tc.n)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(tc.n)
			for i := range subs {
				subs[i] = bus.Subscribe()
				go func(s *RelayPubSubSubscription[relayTestPayload]) {
					defer wg.Done()
					defer s.Unsubscribe()
					for sequence := uint64(1); sequence <= rounds; sequence++ {
						select {
						case <-stop:
							t.Error("stopped before all publications")
							return
						case <-s.C():
							v := s.Read()
							if v.Sequence != sequence || v.Name != "published value" ||
								v.Pointer != &pointed || !slices.Equal(v.Bytes, immutableBytes) {
								t.Errorf("payload header corrupted at sequence %d: sequence=%d, name=%q, pointer=%p, bytes=%q",
									sequence, v.Sequence, v.Name, v.Pointer, v.Bytes)
							}
							for j, word := range v.Words {
								if want := sequence ^ (uint64(j) * marker); word != want {
									t.Errorf("payload word %d at sequence %d = %x, want %x", j, sequence, word, want)
									break
								}
							}
						}
					}
				}(&subs[i])
			}
			for sequence := uint64(1); sequence <= rounds; sequence++ {
				v := relayTestPayload{Sequence: sequence, Name: "published value", Pointer: &pointed, Bytes: immutableBytes}
				for j := range v.Words {
					v.Words[j] = sequence ^ (uint64(j) * marker)
				}
				if got := bus.Send(v); got != tc.n {
					t.Errorf("Send at sequence %d = %d, want %d", sequence, got, tc.n)
				}
			}
			wg.Wait()
			close(stop)
		})
	}
}

func TestRelayPubSubConcurrentPublishers(t *testing.T) {
	for _, width := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			const subscribers, publishers, perPublisher = 17, 4, 64
			const total = publishers * perPublisher
			bus := NewRelayPubSub[int](width)
			subs := make([]RelayPubSubSubscription[int], subscribers)
			observed := make([][]int, subscribers)
			var readers sync.WaitGroup
			readers.Add(subscribers)
			for i := range subs {
				subs[i] = bus.Subscribe()
				observed[i] = make([]int, 0, total)
				go func(i int) {
					defer readers.Done()
					defer subs[i].Unsubscribe()
					for range total {
						<-subs[i].C()
						observed[i] = append(observed[i], subs[i].Read())
					}
				}(i)
			}
			start := make(chan struct{})
			var writers sync.WaitGroup
			writers.Add(publishers)
			for p := range publishers {
				go func(p int) {
					defer writers.Done()
					<-start
					for j := range perPublisher {
						if got := bus.Send(p*perPublisher + j); got != subscribers {
							t.Errorf("Send = %d, want %d", got, subscribers)
						}
					}
				}(p)
			}
			close(start)
			writers.Wait()
			readers.Wait()
			seen := make([]bool, total)
			for _, value := range observed[0] {
				if value < 0 || value >= total || seen[value] {
					t.Fatalf("unexpected or duplicate publication %d", value)
				}
				seen[value] = true
			}
			for i := 1; i < subscribers; i++ {
				if !slices.Equal(observed[0], observed[i]) {
					t.Fatalf("subscriber %d observed a different global publication order", i)
				}
			}
		})
	}
}

func TestRelayPubSubConcurrentChurnAccounting(t *testing.T) {
	for _, width := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			const rounds, workers, cycles = 1200, 24, 256
			bus := NewRelayPubSub[uint64](width)
			anchor := bus.Subscribe()
			stop := make(chan struct{})
			abandon := make(chan struct{})
			close(abandon)
			var observed atomic.Int64
			var wg sync.WaitGroup
			wg.Add(workers + 1)
			go func() {
				defer wg.Done()
				defer anchor.Unsubscribe()
				for want := uint64(1); want <= rounds; want++ {
					<-anchor.C()
					if got := anchor.Read(); got != want {
						t.Errorf("anchor Read = %d, want %d", got, want)
					}
					observed.Add(1)
				}
			}()
			for worker := range workers {
				go func(worker int) {
					defer wg.Done()
					var last uint64
					seed := uint64(worker + 1)
					for cycle := range cycles {
						seed ^= seed << 13
						seed ^= seed >> 7
						seed ^= seed << 17
						sub := bus.Subscribe()
						if seed&3 == 0 {
							sub.Unsubscribe()
							continue
						}
						var cancellation <-chan struct{}
						if seed&3 == 1 {
							cancellation = abandon
						}
						for range 1 + int((seed>>2)&3) {
							select {
							case <-stop:
								sub.Unsubscribe()
								return
							case <-cancellation:
								goto leave
							case <-sub.C():
								got := sub.Read()
								observed.Add(1)
								if got <= last || got > rounds {
									t.Errorf("worker %d Read = %d after %d", worker, got, last)
								}
								last = got
							}
						}
					leave:
						sub.Unsubscribe()
						if cycle&7 == 0 {
							runtime.Gosched()
						}
					}
				}(worker)
			}
			var reported int64
			for value := uint64(1); value <= rounds; value++ {
				reported += int64(bus.Send(value))
			}
			close(stop)
			wg.Wait()
			if got := observed.Load(); got != reported {
				t.Fatalf("Send reported %d copies, subscribers observed %d", reported, got)
			}
			if got := bus.Send(rounds + 1); got != 0 {
				t.Fatalf("Send after all subscribers left = %d, want 0", got)
			}
		})
	}
}

// AllocsPerRun measures a warmed, single-P path. It is not a claim about
// first-use runtime waiter allocations, stack growth, or caller-created data.
func TestRelayPubSubSteadyStateAllocations(t *testing.T) {
	bus := NewRelayPubSub[int](8)
	sub := bus.Subscribe()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sub.Unsubscribe()
		for {
			select {
			case <-stop:
				return
			case <-sub.C():
				if got := sub.Read(); got != 1 {
					t.Errorf("Read = %d, want 1", got)
				}
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	for range 100 {
		bus.Send(1)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		transient := bus.Subscribe()
		transient.Unsubscribe()
		if got := bus.Send(1); got != 1 {
			t.Errorf("Send = %d, want 1", got)
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed Subscribe/Unsubscribe/Send/Read = %g allocations, want 0", allocations)
	}
}

// A chan carrying this value directly is rejected by the compiler because its
// element is 64 KiB. Notification channels impose no such limit on the payload.
func TestRelayPubSubValueLargerThanChannelElement(t *testing.T) {
	const subscribers, rounds = 3, 3
	bus := NewRelayPubSub[[65536]byte](2)
	subs := make([]RelayPubSubSubscription[[65536]byte], subscribers)
	var wg sync.WaitGroup
	wg.Add(subscribers)
	for i := range subs {
		subs[i] = bus.Subscribe()
		go func(s *RelayPubSubSubscription[[65536]byte]) {
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
		}(&subs[i])
	}
	for sequence := 1; sequence <= rounds; sequence++ {
		var value [65536]byte
		for j := range value {
			value[j] = byte(j*13 + sequence)
		}
		if got := bus.Send(value); got != subscribers {
			t.Errorf("Send = %d, want %d", got, subscribers)
		}
	}
	wg.Wait()
}

// A single goroutine owns all three handles. The selected Read must return
// before that goroutine can select and receive the other two notifications.
func TestRelayPubSubOneGoroutineOwnsSeveralSubscriptions(t *testing.T) {
	for _, width := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const rounds = 12
				bus := NewRelayPubSub[int](width)
				a, b, c := bus.Subscribe(), bus.Subscribe(), bus.Subscribe()
				defer a.Unsubscribe()
				defer b.Unsubscribe()
				defer c.Unsubscribe()
				sent := make(chan int, rounds)
				go func() {
					for value := 1; value <= rounds; value++ {
						sent <- bus.Send(value)
					}
				}()
				var counts [3]int
				read := func(s *RelayPubSubSubscription[int], index int) {
					got := s.Read()
					counts[index]++
					if got != counts[index] {
						t.Errorf("handle %d Read = %d, want %d", index, got, counts[index])
					}
				}
				for range 3 * rounds {
					select {
					case <-a.C():
						read(&a, 0)
					case <-b.C():
						read(&b, 1)
					case <-c.C():
						read(&c, 2)
					}
				}
				for range rounds {
					if got := <-sent; got != 3 {
						t.Errorf("Send = %d, want 3", got)
					}
				}
				if want := [3]int{rounds, rounds, rounds}; counts != want {
					t.Fatalf("per-handle deliveries = %v, want %v", counts, want)
				}
			})
		})
	}
}

func TestPhasePubSubOneGoroutineOwnsSeveralSubscriptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const rounds = 12
		bus := NewPhasePubSub[int]()
		a, b, c := bus.Subscribe(), bus.Subscribe(), bus.Subscribe()
		defer a.Unsubscribe()
		defer b.Unsubscribe()
		defer c.Unsubscribe()
		sent := make(chan int, rounds)
		go func() {
			for value := 1; value <= rounds; value++ {
				sent <- bus.Send(value)
			}
		}()
		var counts [3]int
		read := func(s *PhaseSubscription[int], index int) {
			got := s.Read()
			counts[index]++
			if got != counts[index] {
				t.Errorf("handle %d Read = %d, want %d", index, got, counts[index])
			}
		}
		for range 3 * rounds {
			select {
			case <-a.C():
				read(&a, 0)
			case <-b.C():
				read(&b, 1)
			case <-c.C():
				read(&c, 2)
			}
		}
		for range rounds {
			if got := <-sent; got != 3 {
				t.Errorf("Send = %d, want 3", got)
			}
		}
		if want := [3]int{rounds, rounds, rounds}; counts != want {
			t.Fatalf("per-handle deliveries = %v, want %v", counts, want)
		}
	})
}
