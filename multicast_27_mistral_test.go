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
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func multicastMustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}()
	f()
}

func TestMulticastSendReceive(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	const subs, values = 3, 100
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := m.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			for n := 0; n < values; n++ {
				select {
				case v := <-s.C():
					if v != n {
						t.Errorf("expected %d, got %d", n, v)
						return
					}
				case <-s.Done():
					t.Error("done prematurely")
					return
				}
			}
		}()
	}
	for n := 0; n < values; n++ {
		if d := m.Send(n); d != subs {
			t.Fatalf("Send(%d) delivered %d, want %d", n, d, subs)
		}
	}
	wg.Wait()
}

func TestMulticastSelect(t *testing.T) {
	m := NewMulticast27Mistral(make(chan string, 1))
	s := m.Subscribe()
	defer s.Unsubscribe()
	go func() { m.Send("hello") }()
	select {
	case v := <-s.C():
		if v != "hello" {
			t.Fatalf("got %q", v)
		}
	case <-s.Done():
		t.Fatal("done prematurely")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for value")
	}
}

func TestMulticastNoSubscribers(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	if m.Len() != 0 {
		t.Fatal("Len != 0")
	}
	if n := m.Send(1); n != 0 {
		t.Fatalf("Send returned %d", n)
	}
	if d, dr := m.TrySend(1); d != 0 || dr != 0 {
		t.Fatalf("TrySend returned %d, %d", d, dr)
	}
	var s Multicast27Subscription[chan int, int]
	m.Register(&s)
	if m.Len() != 1 {
		t.Fatal("Len != 1")
	}
	s.Unsubscribe()
	if m.Len() != 0 {
		t.Fatal("Len != 0 after unsubscribe")
	}
	if n := m.Send(2); n != 0 {
		t.Fatalf("Send returned %d", n)
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0 (graves must be cleaned)", st)
	}
}

func TestMulticastUnsubscribeUnblocksSend(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	s := m.Subscribe()
	if m.Send(1) != 1 {
		t.Fatal("first send should fill the mailbox")
	}
	done := make(chan int, 1)
	go func() { done <- m.Send(2) }()
	select {
	case n := <-done:
		t.Fatalf("Send returned %d while mailbox full", n)
	case <-time.After(50 * time.Millisecond):
	}
	s.Unsubscribe()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("Send returned %d, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not unblock after Unsubscribe")
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0", st)
	}
}

func TestMulticastClose(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	s1 := m.Subscribe()
	s2 := m.Subscribe()
	if m.Send(1) != 2 {
		t.Fatal("expected 2 deliveries")
	}
	done := make(chan int, 1)
	go func() { done <- m.Send(2) }() // blocks: mailboxes full
	for i := 0; i < 1000; i++ {
		runtime.Gosched()
	}
	m.Close()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("Send returned %d after Close, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not unblock after Close")
	}
	select {
	case <-s1.Done():
	default:
		t.Fatal("s1.Done not closed")
	}
	select {
	case <-s2.Done():
	default:
		t.Fatal("s2.Done not closed")
	}
	multicastMustPanic(t, func() { m.Send(3) })
	multicastMustPanic(t, func() { m.TrySend(3) })
	multicastMustPanic(t, func() { m.Subscribe() })
	m.Close() // idempotent
	m.Close()
	if m.Len() != 0 {
		t.Fatal("Len != 0 after Close")
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0 after Close", st)
	}
}

func TestMulticastTrySend(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	slow := m.Subscribe() // never drains
	fast := m.Subscribe()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-fast.C():
			}
		}
	}()
	const attempts = 100
	var delivered, dropped int
	for i := 0; i < attempts; i++ {
		d, dr := m.TrySend(i)
		delivered += d
		dropped += dr
	}
	if delivered+dropped != 2*attempts {
		t.Fatalf("delivered(%d)+dropped(%d) != %d", delivered, dropped, 2*attempts)
	}
	if dropped == 0 {
		t.Fatal("expected drops from the slow subscriber")
	}
	close(stop)
	wg.Wait()
	slow.Unsubscribe()
	fast.Unsubscribe()
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0", st)
	}
}

func TestMulticastConcurrentSends(t *testing.T) {
	const senders, each, subs = 2, 100, 3
	m := NewMulticast27Mistral(make(chan int, senders*each+8)) // ample: deterministic totals
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := m.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Unsubscribe()
			var sum int
			for n := 0; n < senders*each; n++ {
				select {
				case v := <-s.C():
					sum += v
				case <-s.Done():
					t.Error("done prematurely")
					return
				}
			}
			if want := senders * each * (each - 1) / 2; sum != want {
				t.Errorf("sum = %d, want %d", sum, want)
			}
		}()
	}
	var swg sync.WaitGroup
	for i := 0; i < senders; i++ {
		swg.Add(1)
		go func() {
			defer swg.Done()
			for n := 0; n < each; n++ {
				m.Send(n)
			}
		}()
	}
	swg.Wait()
	wg.Wait()
}

func TestMulticastChurn(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	const churners, iterations = 16, 100
	var churnWG sync.WaitGroup
	churnDone := make(chan struct{})
	for i := 0; i < churners; i++ {
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			for j := 0; j < iterations; j++ {
				s := m.Subscribe()
				for n := 0; n < 3; n++ {
					select {
					case <-s.C():
					case <-s.Done():
						return
					}
				}
				s.Unsubscribe()
			}
		}()
	}
	go func() { churnWG.Wait(); close(churnDone) }()
	var sendWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		sendWG.Add(1)
		go func() {
			defer sendWG.Done()
			for {
				select {
				case <-churnDone:
					return
				default:
					m.Send(0)
				}
			}
		}()
	}
	sendWG.Wait()
	m.Close()
	if m.Len() != 0 {
		t.Fatalf("Len = %d after churn", m.Len())
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x after churn + Close, want 0", st)
	}
}

// waitMulticastLinked reports whether m's subscription list gains a node within
// timeout. Send reads the list from its head, so a linked head is the exact
// precondition for delivery. Len is not a substitute: register counts a
// subscription before it links it, so Len can read 1 while Send still sees an
// empty list. It polls, as waitPackedState does, and returns as soon as the
// link is observed.
func waitMulticastLinked(m *Multicast27Mistral[chan int, int], timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for m.head.Load() == nil {
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
	}
	return true
}

func TestMulticastSubscribeContext(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 16))
	// The iteration ends with a shortfall error, not a hang, if the values never arrive.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// SubscribeContext registers its subscription when the range starts, and a
	// value sent before that subscription is linked is never delivered to it. The
	// sender therefore waits for the link.
	go func() {
		if !waitMulticastLinked(m, 10*time.Second) {
			return // the iteration times out and reports the shortfall
		}
		for n := 0; n < 5; n++ {
			m.Send(n)
		}
	}()
	var got []int
	for v := range m.SubscribeContext(ctx) {
		got = append(got, v)
		if len(got) == 5 {
			break
		}
	}
	if len(got) != 5 {
		t.Fatalf("got %d values, want 5", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("got[%d] = %d, want %d", i, v, i)
		}
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d after break", m.Len())
	}
	ended := make(chan int, 1)
	iterating := make(chan struct{})
	go func() {
		close(iterating)
		n := 0
		for range m.SubscribeContext(ctx) {
			n++
		}
		ended <- n
	}()
	<-iterating
	cancel()
	select {
	case n := <-ended:
		if n != 0 {
			t.Fatalf("iterator received %d values after cancel", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("iterator did not end on ctx cancel")
	}
}

func TestMulticastSubscriptionPanics(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	var s Multicast27Subscription[chan int, int]
	multicastMustPanic(t, func() { s.Unsubscribe() })
	multicastMustPanic(t, func() { _ = s.C() })
	multicastMustPanic(t, func() { _ = s.Done() })
	m.Register(&s)
	if s.C() == nil || s.Done() == nil {
		t.Fatal("nil channels after register")
	}
	multicastMustPanic(t, func() { m.Register(&s) }) // not fresh
	s.Unsubscribe()
	s.Unsubscribe() // idempotent
	if s.C() == nil {
		t.Fatal("mailbox still accessible after unsubscribe")
	}
}

func TestMulticastMailboxCapacity(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 3))
	s := m.Subscribe()
	for i := 0; i < 3; i++ {
		if m.Send(i) != 1 {
			t.Fatalf("Send(%d) should not block", i)
		}
	}
	done := make(chan int, 1)
	started := make(chan struct{})
	go func() { close(started); done <- m.Send(3) }() // blocks: fourth value
	<-started
	for i := 0; i < 1000; i++ {
		runtime.Gosched()
	}
	select {
	case n := <-done:
		t.Fatalf("Send returned %d with a full mailbox", n)
	default:
	}
	select {
	case v := <-s.C():
		if v != 0 {
			t.Fatalf("drained %d, want 0", v)
		}
	default:
		t.Fatal("mailbox should not be empty")
	}
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("Send returned %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not unblock after drain")
	}
	s.Unsubscribe()
}

func TestMulticastUnbufferedMailbox(t *testing.T) {
	m := NewMulticast27Mistral((chan int)(nil)) // rendezvous mode
	s := m.Subscribe()
	received := make(chan int, 1)
	go func() { received <- (<-s.C()) }()
	if m.Send(42) != 1 {
		t.Fatal("expected 1 delivery")
	}
	select {
	case v := <-received:
		if v != 42 {
			t.Fatalf("got %d, want 42", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rendezvous did not complete")
	}
	s.Unsubscribe()
}

func TestMulticastZeroValue(t *testing.T) {
	var m Multicast27Mistral[chan int, int]
	s := m.Subscribe()
	go func() { m.Send(1) }()
	if v := <-s.C(); v != 1 {
		t.Fatalf("got %d, want 1", v)
	}
	s.Unsubscribe()
	m.Close()
	multicastMustPanic(t, func() { m.Send(2) })
}

func TestMulticastGraveAccounting(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	const n = 100
	subs := make([]*Multicast27Subscription[chan int, int], 0, n)
	for i := 0; i < n; i++ {
		subs = append(subs, m.Subscribe())
	}
	if m.Len() != n {
		t.Fatalf("Len = %d, want %d", m.Len(), n)
	}
	for _, s := range subs {
		s.Unsubscribe()
	}
	if m.Len() != 0 {
		t.Fatal("Len != 0 after unsubscribing all")
	}
	if d := m.Send(0); d != 0 {
		t.Fatalf("Send returned %d", d)
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0 (graves must be fully cleaned)", st)
	}
}

func TestMulticastConcurrentClose(t *testing.T) {
	m := NewMulticast27Mistral(make(chan int, 1))
	subs := make([]*Multicast27Subscription[chan int, int], 64)
	for i := range subs {
		subs[i] = m.Subscribe()
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.Close() }()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, s := range subs {
				s.Unsubscribe()
			}
		}()
	}
	wg.Wait()
	m.Close()
	if m.Len() != 0 {
		t.Fatal("Len != 0")
	}
	if st := m.state.Load(); st != 0 {
		t.Fatalf("state = %#x, want 0", st)
	}
	for _, s := range subs {
		select {
		case <-s.Done():
		default:
			t.Fatal("Done not closed")
		}
	}
}

func benchmarkMulticastSend(b *testing.B, subs int) {
	m := NewMulticast27Mistral(make(chan int, 1))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		s := m.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-s.C():
				}
			}
		}()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Send(i)
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
	m.Close()
}

func BenchmarkMulticastSend1(b *testing.B)  { benchmarkMulticastSend(b, 1) }
func BenchmarkMulticastSend4(b *testing.B)  { benchmarkMulticastSend(b, 4) }
func BenchmarkMulticastSend16(b *testing.B) { benchmarkMulticastSend(b, 16) }
func BenchmarkMulticastSend64(b *testing.B) { benchmarkMulticastSend(b, 64) }

func benchmarkCallbackChannelSend(b *testing.B, subs int) {
	chs := make([]chan int, subs)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range chs {
		chs[i] = make(chan int, 1)
		wg.Add(1)
		go func(ch chan int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-ch:
				}
			}
		}(chs[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ch := range chs {
			ch <- i
		}
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}

func BenchmarkCallbackChannelSend1(b *testing.B)  { benchmarkCallbackChannelSend(b, 1) }
func BenchmarkCallbackChannelSend4(b *testing.B)  { benchmarkCallbackChannelSend(b, 4) }
func BenchmarkCallbackChannelSend16(b *testing.B) { benchmarkCallbackChannelSend(b, 16) }
func BenchmarkCallbackChannelSend64(b *testing.B) { benchmarkCallbackChannelSend(b, 64) }

func BenchmarkMulticastChurn(b *testing.B) {
	m := NewMulticast27Mistral(make(chan int, 1))
	for i := 0; i < b.N; i++ {
		s := m.Subscribe()
		s.Unsubscribe()
	}
	m.Close()
}
