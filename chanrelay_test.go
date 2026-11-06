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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testTimeout is the deadline used to detect a stuck implementation.
const testTimeout = 10 * time.Second

// testPanic is used to convert a suspected deadlock into a test failure (rather
// than a hung test binary).
func testPanic() {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	os.Stderr.Write(buf[:n])
	panic(`bigbuff: chanrelay test: timed out (deadlock?)`)
}

// TestChanRelay_SubscribeClose covers the basic lifecycle.
func TestChanRelay_SubscribeClose(t *testing.T) {
	relay := NewChanRelay[int]()
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}

	sub := relay.Subscribe()
	if n := relay.Len(); n != 1 {
		t.Fatalf("expected 1 subscriber, got %d", n)
	}
	if sub.Window() != 0 {
		t.Fatalf("expected synchronous subscription, got window %d", sub.Window())
	}
	if sub.Closed() {
		t.Fatal("expected subscription to be open")
	}

	if !sub.Close() {
		t.Fatal("expected first Close to close the subscription")
	}
	if sub.Close() {
		t.Fatal("expected second Close to be a no-op")
	}
	if !sub.Closed() {
		t.Fatal("expected subscription to be closed")
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("expected Done to be closed")
	}
	if _, ok := sub.Next(); ok {
		t.Fatal("expected Next to report closure")
	}
	if _, ok := sub.TryNext(); ok {
		t.Fatal("expected TryNext to report closure")
	}
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}

	relay.Close()
	if !relay.Closed() {
		t.Fatal("expected relay to be closed")
	}
	select {
	case <-relay.Done():
	default:
		t.Fatal("expected relay Done to be closed")
	}
}

// TestChanRelay_SingleSubscription covers delivery to (and consumption from) a
// single subscription.
func TestChanRelay_SingleSubscription(t *testing.T) {
	relay := NewChanRelay[int]()
	sub := relay.Subscribe()
	defer sub.Close()

	received := make(chan []int, 1)
	go func() {
		var values []int
		for range 5 {
			v, ok := sub.Next()
			if !ok {
				received <- values
				return
			}
			values = append(values, v)
		}
		received <- values
	}()

	for i := range 5 {
		if n := relay.Send(i); n != 1 {
			t.Fatalf("Send: expected 1 receiver, got %d", n)
		}
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if got := <-received; fmt.Sprint(got) != "[0 1 2 3 4]" {
		t.Fatalf("unexpected values: %v", got)
	}
}

// TestChanRelay_BroadcastOrdering covers broadcast to many concurrent
// subscribers, verifying that every subscription observes the same, total,
// order of values.
func TestChanRelay_BroadcastOrdering(t *testing.T) {
	const (
		subscribers = 8
		values      = 200
	)

	relay := NewChanRelay[int]()
	defer relay.Close()

	var (
		wg       sync.WaitGroup
		results  = make([][]int, subscribers)
		start    = make(chan struct{})
		finished = make(chan struct{})
	)

	for i := range subscribers {
		sub := relay.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			<-start
			var got []int
			for {
				select {
				case v := <-sub.C():
					got = append(got, v)
					if len(got) == values {
						results[i] = got
						return
					}
				case <-sub.Done():
					results[i] = got
					return
				}
			}
		}()
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	close(start)
	go func() {
		defer close(finished)
		for i := range values {
			if n := relay.Send(i); n != subscribers {
				panic(fmt.Sprintf("Send: expected %d receivers, got %d", subscribers, n))
			}
		}
	}()

	wg.Wait()
	<-finished

	for i, got := range results {
		if len(got) != values {
			t.Fatalf("subscriber %d: expected %d values, got %d", i, values, len(got))
		}
		for j, v := range got {
			if v != j {
				t.Fatalf("subscriber %d: expected value %d at index %d, got %d", i, j, j, v)
			}
		}
	}
}

// TestChanRelay_SendIsSynchronous verifies that a send to a synchronous
// subscription does not complete until the subscriber has received, and that a
// receive is the acknowledgement (there is no separate handshake).
func TestChanRelay_SendIsSynchronous(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	sent := make(chan int, 1)
	go func() { sent <- relay.Send(42) }()

	select {
	case <-sent:
		t.Fatal("Send returned before the subscriber received")
	case <-time.After(20 * time.Millisecond):
	}

	v, ok := sub.Next()
	if !ok || v != 42 {
		t.Fatalf("unexpected receive: %v %v", v, ok)
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
}

// TestChanRelay_BufferedWindow verifies that a buffered subscription decouples
// the publisher from the subscriber by up to its window, and not further.
func TestChanRelay_BufferedWindow(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.SubscribeWindow(4)
	defer sub.Close()

	if sub.Window() != 4 {
		t.Fatalf("expected window 4, got %d", sub.Window())
	}

	for i := range 4 {
		if n := relay.Send(i); n != 1 {
			t.Fatalf("Send: expected 1 receiver, got %d", n)
		}
	}
	if n := sub.Pending(); n != 4 {
		t.Fatalf("expected 4 pending, got %d", n)
	}
	if n := sub.Dropped(); n != 0 {
		t.Fatalf("expected no drops, got %d", n)
	}

	// The fifth send blocks: the window is full, and nothing is consuming.
	sent := make(chan int, 1)
	go func() { sent <- relay.Send(4) }()

	select {
	case n := <-sent:
		t.Fatalf("Send returned (%d) while the window was full", n)
	case <-time.After(20 * time.Millisecond):
	}

	for i := range 5 {
		v, ok := sub.Next()
		if !ok || v != i {
			t.Fatalf("expected %d, got %d (ok %v)", i, v, ok)
		}
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	if n := sub.Pending(); n != 0 {
		t.Fatalf("expected no pending, got %d", n)
	}
}

// TestChanRelay_TrySendConflates verifies the "latest value wins" behaviour of
// TrySend on a buffered subscription.
func TestChanRelay_TrySendConflates(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.SubscribeWindow(2)
	defer sub.Close()

	const values = 10
	for i := range values {
		if n := relay.TrySend(i); n != 1 {
			t.Fatalf("TrySend: expected 1 receiver, got %d", n)
		}
	}

	if n := sub.Pending(); n != 2 {
		t.Fatalf("expected 2 pending, got %d", n)
	}
	if n := sub.Dropped(); n != values-2 {
		t.Fatalf("expected %d dropped, got %d", values-2, n)
	}

	// The retained values are the most recent ones, in order.
	for _, want := range []int{values - 2, values - 1} {
		v, ok := sub.Next()
		if !ok || v != want {
			t.Fatalf("expected %d, got %d (ok %v)", want, v, ok)
		}
	}
	if _, ok := sub.TryNext(); ok {
		t.Fatal("expected no more values")
	}
}

// TestChanRelay_TrySendSynchronous verifies that TrySend hands values directly
// to a parked receiver of a synchronous subscription, and skips it (counting a
// drop) when it is not receiving.
func TestChanRelay_TrySendSynchronous(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	if n := relay.TrySend(1); n != 0 {
		t.Fatalf("expected no receivers, got %d", n)
	}
	if n := sub.Dropped(); n != 1 {
		t.Fatalf("expected 1 drop, got %d", n)
	}

	received := make(chan int, 1)
	go func() {
		if v, ok := sub.Next(); ok {
			received <- v
		}
		close(received)
	}()

	// Wait for the receiver to park.
	for i := 0; ; i++ {
		if n := relay.TrySend(2); n == 1 {
			break
		}
		if i > 10000 {
			t.Fatal("failed to deliver to a parked receiver")
		}
		time.Sleep(time.Millisecond)
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if v := <-received; v != 2 {
		t.Fatalf("expected 2, got %d", v)
	}
}

// TestChanRelay_SendContext verifies that SendContext bounds the wait, and
// unblocks a publisher.
func TestChanRelay_SendContext(t *testing.T) {
	t.Run("Delivered", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()
		sub := relay.SubscribeWindow(1)
		defer sub.Close()

		n, err := relay.SendContext(context.Background(), 1)
		if err != nil || n != 1 {
			t.Fatalf("expected (1, nil), got (%d, %v)", n, err)
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()
		sub := relay.Subscribe() // synchronous, and never receiving
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		timer := time.AfterFunc(testTimeout, testPanic)
		defer timer.Stop()

		n, err := relay.SendContext(ctx, 1)
		if n != 0 {
			t.Fatalf("expected no receivers, got %d", n)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	})

	t.Run("NilContext", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()
		sub := relay.SubscribeWindow(1)
		defer sub.Close()

		n, err := relay.SendContext(nil, 1)
		if err != nil || n != 1 {
			t.Fatalf("expected (1, nil), got (%d, %v)", n, err)
		}
	})
}

// TestChanRelay_SendSkipsClosed verifies that a subscription closed concurrently
// with (or before) a send neither receives the value, nor blocks the send, nor
// contributes to the count.
func TestChanRelay_SendSkipsClosed(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	var subs []*RelaySubscription[int]
	for range 4 {
		subs = append(subs, relay.SubscribeWindow(2))
	}
	subs[1].Close()
	subs[3].Close()

	if n := relay.Send(7); n != 2 {
		t.Fatalf("expected 2 receivers, got %d", n)
	}
	if n := relay.TrySend(8); n != 2 {
		t.Fatalf("expected 2 receivers, got %d", n)
	}

	for i, sub := range subs {
		if i == 1 || i == 3 {
			continue
		}
		defer sub.Close()
		for _, want := range []int{7, 8} {
			v, ok := sub.Next()
			if !ok || v != want {
				t.Fatalf("subscriber %d: expected %d, got %d (ok %v)", i, want, v, ok)
			}
		}
	}
}

// TestChanRelay_CloseReleasesPublisher verifies the robustness property that
// closing a subscription releases a publisher blocked on it (which is the class
// of deadlock that ChanPubSub must panic to avoid).
func TestChanRelay_CloseReleasesPublisher(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	live := relay.SubscribeWindow(1)
	defer live.Close()
	stalled := relay.Subscribe() // synchronous, and never receiving

	sent := make(chan int, 1)
	go func() { sent <- relay.Send(1) }()

	select {
	case <-sent:
		t.Fatal("Send returned while a synchronous subscriber was not receiving")
	case <-time.After(20 * time.Millisecond):
	}

	// Closing the stalled subscriber from a *different* goroutine must release
	// the publisher, even though the publisher is blocked on it.
	if !stalled.Close() {
		t.Fatal("expected Close to close the subscription")
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	if v, ok := live.Next(); !ok || v != 1 {
		t.Fatalf("expected 1, got %d (ok %v)", v, ok)
	}
}

// TestChanRelay_RelayClose verifies that closing the relay closes every
// subscription, and that subsequent sends are no-ops.
func TestChanRelay_RelayClose(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(10))

	const subscribers = 32
	var subs []*RelaySubscription[int]
	for range subscribers {
		subs = append(subs, relay.Subscribe())
	}
	for i := range 10 {
		if n := relay.Send(i); n != subscribers {
			t.Fatalf("Send: expected %d receivers, got %d", subscribers, n)
		}
	}
	if n := relay.Len(); n != subscribers {
		t.Fatalf("expected %d subscribers, got %d", subscribers, n)
	}

	relay.Close()
	relay.Close() // idempotent

	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}
	if n := relay.Send(1); n != 0 {
		t.Fatalf("expected no receivers, got %d", n)
	}
	if n := relay.TrySend(1); n != 0 {
		t.Fatalf("expected no receivers, got %d", n)
	}
	if _, err := relay.SendContext(context.Background(), 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i, sub := range subs {
		select {
		case <-sub.Done():
		default:
			t.Fatalf("subscriber %d: expected Done to be closed", i)
		}
		if !sub.Closed() {
			t.Fatalf("subscriber %d: expected Closed", i)
		}
		// Closure is preferred over buffered values, by Next and TryNext.
		if _, ok := sub.TryNext(); ok {
			t.Fatalf("subscriber %d: expected closure to take precedence", i)
		}
		// The values are still buffered for a raw receive, though.
		var got []int
		for {
			select {
			case v := <-sub.C():
				got = append(got, v)
				continue
			default:
			}
			break
		}
		if len(got) != 10 { // window
			t.Fatalf("subscriber %d: expected 10 values, got %v", i, got)
		}
	}

	// Subscribing to a closed relay yields a closed subscription.
	sub := relay.Subscribe()
	if !sub.Closed() {
		t.Fatal("expected subscription to a closed relay to be closed")
	}
	sub.Close()
}

// TestChanRelay_SubscribeAfterClose covers subscription to a closed relay.
func TestChanRelay_SubscribeAfterClose(t *testing.T) {
	relay := NewChanRelay[int]()
	relay.Close()

	for _, sub := range []*RelaySubscription[int]{
		relay.Subscribe(),
		relay.SubscribeWindow(1),
		relay.SubscribeChan(make(chan int, 1)),
	} {
		if !sub.Closed() {
			t.Fatal("expected a closed subscription")
		}
		if _, ok := sub.Next(); ok {
			t.Fatal("expected Next to report closure")
		}
		sub.Close()
	}
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers, got %d", n)
	}
}

// TestChanRelay_SubscribeChan covers delivery through a caller-owned channel.
func TestChanRelay_SubscribeChan(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	channel := make(chan int, 3)
	sub := relay.SubscribeChan(channel)
	if sub.C() != (chan int)(channel) {
		// (<-chan int) vs (chan int) comparison is not directly possible; the
		// assertion below is the meaningful one.
		_ = channel
	}
	if sub.Window() != 3 {
		t.Fatalf("expected window 3, got %d", sub.Window())
	}

	if n := relay.Send(1); n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	for i := 2; i <= 6; i++ {
		relay.TrySend(i) // conflates, once full
	}
	var got []int
	for {
		v, ok := sub.TryNext()
		if !ok {
			break
		}
		got = append(got, v)
	}
	if fmt.Sprint(got) != "[4 5 6]" {
		t.Fatalf("unexpected values: %v", got)
	}
	if n := sub.Dropped(); n != 3 {
		t.Fatalf("expected 3 drops, got %d", n)
	}

	sub.Close()
	close(channel) // ownership remains with the caller

	if _, ok := channelOk(channel); ok {
		_ = ok
	}
}

// channelOk drains a channel, reporting whether it was open.
func channelOk(ch chan int) (int, bool) {
	v, ok := <-ch
	return v, ok
}

// TestChanRelay_SubscribeContext covers the iterator API.
func TestChanRelay_SubscribeContext(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		seq := relay.SubscribeContext(context.Background())

		go func() {
			for i := range 5 {
				relay.Send(i)
			}
		}()

		var got []int
		for v := range seq {
			got = append(got, v)
			if len(got) == 5 {
				break
			}
		}
		if fmt.Sprint(got) != "[0 1 2 3 4]" {
			t.Fatalf("unexpected values: %v", got)
		}
		if n := relay.Len(); n != 0 {
			t.Fatalf("expected the subscription to be released, got %d", n)
		}
	})

	t.Run("Cancel", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		ctx, cancel := context.WithCancel(context.Background())
		sub := relay.SubscribeWindow(1)
		defer sub.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for range relay.SubscribeContext(ctx) {
				// unreachable
			}
		}()

		// One extra subscription (from SubscribeContext), plus ours.
		for relay.Len() != 2 {
			runtime.Gosched()
		}
		cancel()

		timer := time.AfterFunc(testTimeout, testPanic)
		defer timer.Stop()

		<-done
		if n := relay.Len(); n != 1 {
			t.Fatalf("expected 1 remaining subscription, got %d", n)
		}
	})

	t.Run("NeverIterated", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		ctx, cancel := context.WithCancel(context.Background())
		_ = relay.SubscribeContext(ctx)
		if n := relay.Len(); n != 1 {
			t.Fatalf("expected 1 subscription, got %d", n)
		}
		cancel()

		timer := time.AfterFunc(testTimeout, testPanic)
		defer timer.Stop()

		for relay.Len() != 0 {
			runtime.Gosched()
		}
	})

	t.Run("NilYield", func(t *testing.T) {
		relay := NewChanRelay[int]()
		defer relay.Close()

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected a panic")
			}
			if n := relay.Len(); n != 0 {
				t.Fatalf("expected the subscription to be released, got %d", n)
			}
		}()

		relay.SubscribeContext(context.Background())(nil)
	})
}

// TestChanRelay_Churn covers subscription churn, exercising registry growth and
// (amortised) compaction, and verifying that surviving subscriptions are
// neither lost nor delivered to more than once.
func TestChanRelay_Churn(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	defer relay.Close()

	const (
		generations = 12
		perGen      = relayChunkSize + 13 // force chunk chains and compaction
	)

	var live []*RelaySubscription[int]
	for gen := range generations {
		var next []*RelaySubscription[int]
		for range perGen {
			next = append(next, relay.Subscribe())
		}
		// Half of the live subscriptions survive this generation.
		for i, sub := range live {
			if i%2 == 0 {
				next = append(next, sub)
			} else {
				sub.Close()
			}
		}

		value := gen
		if n := relay.Send(value); n != len(next) {
			t.Fatalf("generation %d: expected %d receivers, got %d", gen, len(next), n)
		}
		if n := relay.Len(); n != len(next) {
			t.Fatalf("generation %d: expected Len %d, got %d", gen, len(next), n)
		}

		// Every subscription must have received exactly this generation's value,
		// i.e. exactly once (duplicate registry entries would show up here).
		for i, sub := range next {
			if got, ok := sub.TryNext(); !ok || got != value {
				t.Fatalf("generation %d subscriber %d: expected %d, got %d (ok %v)", gen, i, value, got, ok)
			}
			if _, ok := sub.TryNext(); ok {
				t.Fatalf("generation %d subscriber %d: expected a single delivery", gen, i)
			}
		}

		live = next
	}
}

// TestChanRelay_ConcurrentPublishers verifies that concurrent publishers produce
// a single total order, observed identically by every subscription.
func TestChanRelay_ConcurrentPublishers(t *testing.T) {
	const (
		publishers  = 4
		perPub      = 250
		subscribers = 4
	)

	relay := NewChanRelay[int](WithRelayWindow(8))
	defer relay.Close()

	var (
		wg      sync.WaitGroup
		results = make([][]int, subscribers)
	)

	for i := range subscribers {
		sub := relay.SubscribeWindow(16)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			for {
				v, ok := sub.Next()
				if !ok {
					results[i] = nil
					return
				}
				results[i] = append(results[i], v)
				if len(results[i]) == publishers*perPub {
					return
				}
			}
		}()
	}

	var pubWg sync.WaitGroup
	for p := range publishers {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for i := range perPub {
				relay.Send(p*perPub + i)
			}
		}()
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	pubWg.Wait()
	wg.Wait()

	for i := 1; i < subscribers; i++ {
		if fmt.Sprint(results[i]) != fmt.Sprint(results[0]) {
			t.Fatalf("subscriber %d observed a different order than subscriber 0", i)
		}
	}
	for _, v := range results[0] {
		_ = v
	}
	if len(results[0]) != publishers*perPub {
		t.Fatalf("expected %d values, got %d", publishers*perPub, len(results[0]))
	}
	seen := make(map[int]struct{}, len(results[0]))
	for _, v := range results[0] {
		if _, ok := seen[v]; ok {
			t.Fatalf("duplicate value %d", v)
		}
		seen[v] = struct{}{}
	}
}

// TestChanRelay_Stress runs publishers, subscribers, consumers, and churn
// concurrently, and checks the invariants that must hold under any interleaving:
// no panics, no deadlocks, and per-subscription ordering (a subscription may miss
// values, because it was created late, or because TrySend conflated, but never
// observes them out of order, or twice).
func TestChanRelay_Stress(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(2))
	defer relay.Close()

	const (
		duration   = 250 * time.Millisecond
		publishers = 3
	)

	var (
		stop     = make(chan struct{})
		wg       sync.WaitGroup
		failures atomic.Int32
	)

	// Publishers. Each publisher stamps its values with (publisher, sequence),
	// so that consumers can verify the total order guarantee: values are
	// dispatched in a global order, so every subscription must observe each
	// publisher's values in strictly increasing sequence.
	for p := range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seq int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				value := int(int64(p)<<20 | (seq & 0xfffff))
				seq++
				if rand.Intn(2) == 0 {
					relay.Send(value)
				} else {
					relay.TrySend(value)
				}
			}
		}()
	}

	// Consumer manager: churning subscribers, each of which verifies that it
	// observes values (identified by their publish sequence number) in strictly
	// increasing order.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				window := rand.Intn(3)
				var sub *RelaySubscription[int]
				if rand.Intn(2) == 0 {
					sub = relay.SubscribeWindow(window)
				} else {
					sub = relay.SubscribeChan(make(chan int, window))
				}
				last := [publishers]int{}
				for i := range last {
					last[i] = -1
				}
				for {
					v, ok := sub.TryNext()
					if !ok {
						break
					}
					p, seq := v>>20, v&0xfffff
					if p >= publishers || seq <= last[p] {
						failures.Add(1)
					} else {
						last[p] = seq
					}
					if rand.Intn(8) == 0 {
						break
					}
				}
				sub.Close()
			}
		}()
	}

	// A long-lived synchronous consumer, to exercise blocking sends. It is
	// released by closing its subscription from another goroutine, while it is
	// blocked in Next - which the relay supports by construction.
	sub := relay.Subscribe()
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-stop
		sub.Close()
	}()
	go func() {
		defer wg.Done()
		defer sub.Close()
		for {
			if _, ok := sub.Next(); !ok {
				return
			}
		}
	}()

	time.AfterFunc(duration, func() { close(stop) })
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		buf := make([]byte, 1<<22)
		n := runtime.Stack(buf, true)
		os.Stderr.Write(buf[:n])
		panic(`stress: hang`)
	}

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d ordering violations observed", n)
	}

	// The relay must still be usable, and consistent.
	relay.Close()
	if n := relay.Len(); n != 0 {
		t.Fatalf("expected no subscribers after Close, got %d", n)
	}
}

// TestChanRelay_NoAllocationOnSend verifies the zero-allocation publish path.
func TestChanRelay_NoAllocationOnSend(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	const subscribers = 16
	var subs []*RelaySubscription[int]
	for range subscribers {
		subs = append(subs, relay.SubscribeWindow(1))
	}
	defer func() {
		for _, sub := range subs {
			sub.Close()
		}
	}()

	drain := func() {
		for _, sub := range subs {
			if _, ok := sub.TryNext(); !ok {
				t.Fatal("expected a value")
			}
		}
	}

	if n := testing.AllocsPerRun(100, func() {
		relay.Send(1)
		drain()
		relay.TrySend(2)
		drain()
	}); n != 0 {
		t.Fatalf("expected zero allocations per send, got %f", n)
	}
}

// TestChanRelay_NoAllocationOnSubscribe ensures subscription churn is cheap: no
// allocation is attributable to the relay's registry.
func TestChanRelay_AllocationsPerSubscription(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	n := testing.AllocsPerRun(100, func() {
		sub := relay.SubscribeWindow(1)
		sub.Close()
	})
	// The subscription itself, its transport, and its done channel.
	if n > 3 {
		t.Fatalf("expected at most 3 allocations per subscription, got %f", n)
	}
}

// TestChanRelay_CloseIsIndependentOfPublishers verifies that closing a
// subscription neither blocks on, nor is blocked by, a publisher that is stalled
// on a different subscription. It is the property that lets a churning
// subscriber population coexist with an arbitrary slow consumer, and it follows
// from the fact that no lock is shared between Close and the publish path.
func TestChanRelay_CloseIsIndependentOfPublishers(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	stalled := relay.Subscribe() // never receives
	awaiting := relay.SubscribeWindow(1)
	defer awaiting.Close()
	deferred := relay.SubscribeWindow(1)

	sent := make(chan int, 1)
	go func() { sent <- relay.Send(1) }()

	// Wait for the publisher to stall on the first subscription.
	select {
	case <-sent:
		t.Fatal("Send returned while a synchronous subscriber was not receiving")
	case <-time.After(20 * time.Millisecond):
	}

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	// Closing an unrelated subscription must not wait for the stalled publisher.
	closed := make(chan bool, 1)
	go func() { closed <- deferred.Close() }()

	select {
	case ok := <-closed:
		if !ok {
			t.Fatal("expected Close to close the subscription")
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked on a publisher stalled elsewhere")
	}

	// Closing the stalled subscription releases the publisher.
	stalled.Close()

	if n := <-sent; n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}
	if v, ok := awaiting.Next(); !ok || v != 1 {
		t.Fatalf("expected 1, got %d (ok %v)", v, ok)
	}
}

// TestChanRelay_TrySendDoesNotWaitForSubscribers verifies that TrySend neither
// waits for a subscriber that is not receiving, nor for one whose buffer is
// full.
func TestChanRelay_TrySendDoesNotWaitForSubscribers(t *testing.T) {
	relay := NewChanRelay[int]()
	defer relay.Close()

	stalled := relay.Subscribe() // synchronous, and never receiving
	defer stalled.Close()
	full := relay.SubscribeWindow(1)
	defer full.Close()

	relay.TrySend(1) // fills the buffered subscription

	timer := time.AfterFunc(testTimeout, testPanic)
	defer timer.Stop()

	done := make(chan int, 1)
	go func() { done <- relay.TrySend(2) }()

	select {
	case n := <-done:
		// Only the buffered subscription is counted (it conflated 1 -> 2); the
		// stalled one is skipped.
		if n != 1 {
			t.Fatalf("expected 1 receiver, got %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("TrySend waited for a subscriber")
	}

	if n := stalled.Dropped(); n != 2 {
		t.Fatalf("expected 2 drops, got %d", n)
	}
	if v, ok := full.Next(); !ok || v != 2 {
		t.Fatalf("expected 2, got %d (ok %v)", v, ok)
	}
}

// TestChanRelay_MultipleConsumers verifies that a subscription may be consumed
// by multiple goroutines, splitting its values, without loss or duplication.
func TestChanRelay_MultipleConsumers(t *testing.T) {
	relay := NewChanRelay[int](WithRelayWindow(1))
	defer relay.Close()

	sub := relay.Subscribe()
	defer sub.Close()

	const (
		consumers = 4
		values    = 500
	)

	var (
		wg       sync.WaitGroup
		once     sync.Once
		finished = make(chan struct{})
		mu       sync.Mutex
		seen     = make(map[int]int, values)
	)

	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-finished:
					return
				default:
				}
				v, ok := sub.TryNext()
				if !ok {
					if sub.Closed() {
						return
					}
					runtime.Gosched()
					continue
				}
				mu.Lock()
				seen[v]++
				complete := len(seen) == values
				mu.Unlock()
				if complete {
					once.Do(func() { close(finished) })
					return
				}
			}
		}()
	}

	// Safety net: release the consumers if anything goes wrong.
	time.AfterFunc(testTimeout/2, func() { sub.Close() })

	for i := range values {
		relay.Send(i)
	}

	wg.Wait()

	for i := range values {
		if got := seen[i]; got != 1 {
			t.Fatalf("value %d delivered %d times", i, got)
		}
	}
}

