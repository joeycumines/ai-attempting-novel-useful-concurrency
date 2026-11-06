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
	"io"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func ringTestNotReady(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
		t.Fatal("unexpected readiness for a subscription with no pending value")
	default:
	}
}

func ringTestReceive(t *testing.T, s *RingSubscription[int], want int) {
	t.Helper()
	select {
	case <-s.Ready():
		if got, ok := s.TryRecv(); !ok || got != want {
			t.Fatalf("TryRecv after Ready = (%d, %v), want (%d, true)", got, ok, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription never became ready")
	}
}

func TestRingPubSubCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1, int(^uint(0) >> 1)} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid capacity did not panic")
				}
			}()
			NewRingPubSub[int](capacity)
		})
	}
}

func TestRingPubSubNoSubscribersAndClosedSubscription(t *testing.T) {
	p := NewRingPubSub[int](1)
	for i := range 100 {
		if n, err := p.TrySend(i); n != 0 || err != nil {
			t.Fatalf("TrySend without subscribers = (%d, %v)", n, err)
		}
	}
	s := p.Subscribe()
	ringTestNotReady(t, s.Ready())
	if v, ok := s.TryRecv(); v != 0 || ok {
		t.Fatalf("new subscriber received historical value: (%d, %v)", v, ok)
	}
	s.Close()
	s.Close()
	if v, err := s.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Recv on closed subscription = (%d, %v)", v, err)
	}
	if !p.Close() || p.Close() {
		t.Fatal("Close must report exactly one transition")
	}
	if n, err := p.TrySend(1); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("TrySend on closed hub = (%d, %v)", n, err)
	}
	if n, err := p.Send(nil, 1); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Send on closed hub = (%d, %v)", n, err)
	}
	late := p.Subscribe()
	defer late.Close()
	if v, err := late.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Recv on subscription created after hub Close = (%d, %v)", v, err)
	}
}

// The extra lane is required when either an existing reader catches up or a
// reader joins a full ring. Neither may receive an older cohort's wake token.
func TestRingPubSubSpareLane(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 7} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			p := NewRingPubSub[int](capacity)
			defer p.Close()
			slow, fast := p.Subscribe(), p.Subscribe()
			defer slow.Close()
			defer fast.Close()
			for i := range capacity {
				if n, err := p.TrySend(i); n != 2 || err != nil {
					t.Fatalf("publish %d = (%d, %v)", i, n, err)
				}
			}
			for i := range capacity {
				ringTestReceive(t, &fast, i)
			}
			ringTestNotReady(t, fast.Ready())
			late := p.Subscribe()
			defer late.Close()
			ringTestNotReady(t, late.Ready())
			if n, err := p.TrySend(99); n != 0 || !errors.Is(err, ErrRingFull) {
				t.Fatalf("full ring accepted publication: (%d, %v)", n, err)
			}
			for i := range capacity {
				ringTestReceive(t, &slow, i)
			}
			for i := range 5 * (capacity + 1) {
				if n, err := p.TrySend(100 + i); n != 3 || err != nil {
					t.Fatalf("publish after wrap = (%d, %v)", n, err)
				}
				ringTestReceive(t, &late, 100+i)
				ringTestReceive(t, &fast, 100+i)
				ringTestReceive(t, &slow, 100+i)
				ringTestNotReady(t, late.Ready())
			}
		})
	}
}

func TestRingPubSubSelectedHintThenUnsubscribe(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	a, b := p.Subscribe(), p.Subscribe()
	defer b.Close()
	if n, err := p.TrySend(7); n != 2 || err != nil {
		t.Fatalf("TrySend = (%d, %v)", n, err)
	}
	select {
	case <-a.Ready():
	default:
		t.Fatal("publication did not produce a wake")
	}
	// A selected wake has not acknowledged a value. Closing this reader must
	// hand notification to B, even though A consumed the only queued wake.
	a.Close()
	ringTestReceive(t, &b, 7)
	if n, err := p.TrySend(8); n != 1 || err != nil {
		t.Fatalf("TrySend after unsubscribe = (%d, %v)", n, err)
	}
	ringTestReceive(t, &b, 8)
}

func TestRingPubSubMultipleInFlightHints(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	a, b, c := p.Subscribe(), p.Subscribe(), p.Subscribe()
	defer a.Close()
	defer b.Close()
	defer c.Close()
	if _, err := p.TrySend(11); err != nil {
		t.Fatal(err)
	}
	<-a.Ready()
	// A still owes this generation. B may poll without owning a notification.
	if got, ok := b.TryRecv(); !ok || got != 11 {
		t.Fatalf("polling B = (%d, %v)", got, ok)
	}
	select {
	case <-c.Ready():
	default:
		t.Fatal("polling reader did not relay notification")
	}
	if _, err := p.TrySend(12); !errors.Is(err, ErrRingFull) {
		t.Fatalf("in-flight readers failed to pin generation: %v", err)
	}
	if got, ok := a.TryRecv(); !ok || got != 11 {
		t.Fatalf("selected A = (%d, %v)", got, ok)
	}
	if _, err := p.TrySend(12); !errors.Is(err, ErrRingFull) {
		t.Fatalf("last in-flight reader failed to pin generation: %v", err)
	}
	c.Close()
	if n, err := p.TrySend(12); n != 2 || err != nil {
		t.Fatalf("publish after final retirement = (%d, %v)", n, err)
	}
	ringTestReceive(t, &a, 12)
	ringTestReceive(t, &b, 12)
	ringTestNotReady(t, a.Ready())
	ringTestNotReady(t, b.Ready())
}

func TestRingPubSubCloseDrainsEachCohort(t *testing.T) {
	p := NewRingPubSub[int](4)
	a := p.Subscribe()
	defer a.Close()
	p.TrySend(1)
	b := p.Subscribe()
	defer b.Close()
	p.TrySend(2)
	p.TrySend(3)
	ringTestReceive(t, &a, 1)
	if !p.Close() {
		t.Fatal("first close returned false")
	}
	for _, s := range []*RingSubscription[int]{&a, &b} {
		for _, want := range []int{2, 3} {
			ringTestReceive(t, s, want)
		}
		select {
		case <-s.Ready():
		default:
			t.Fatal("drained closed hub did not signal EOF")
		}
		if got, err := s.Recv(nil); got != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("Recv after drain = (%d, %v)", got, err)
		}
	}
}

func TestRingPubSubCloseWakesCaughtUpReaders(t *testing.T) {
	p := NewRingPubSub[int](2)
	const readers = 16
	subs := make([]RingSubscription[int], readers)
	for i := range subs {
		subs[i] = p.Subscribe()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, readers)
	for i := range subs {
		go func(s *RingSubscription[int]) {
			defer s.Close()
			_, err := s.Recv(ctx)
			results <- err
		}(&subs[i])
	}
	p.Close()
	for range readers {
		select {
		case err := <-results:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("waiting reader returned %v, want EOF", err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not wake every reader")
		}
	}
}

func TestRingPubSubCancellationPreservesSubscription(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	s := p.Subscribe()
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if v, err := s.Recv(ctx); v != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Recv = (%d, %v)", v, err)
	}
	if n, err := p.Send(nil, 4); n != 1 || err != nil {
		t.Fatalf("Send after canceled Recv = (%d, %v)", n, err)
	}
	if n, err := p.Send(ctx, 5); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled blocked Send = (%d, %v)", n, err)
	}
	ringTestReceive(t, &s, 4)
	ringTestNotReady(t, s.Ready())
	if n, err := p.TrySend(6); n != 1 || err != nil {
		t.Fatalf("subscriber lost after cancellation: (%d, %v)", n, err)
	}
	ringTestReceive(t, &s, 6)
}

func TestRingPubSubCanceledRecvPreservesBacklogAndRelaysWake(t *testing.T) {
	for _, hubClosed := range []bool{false, true} {
		for _, selected := range []bool{false, true} {
			t.Run(fmt.Sprintf("closed=%v/selected=%v", hubClosed, selected), func(t *testing.T) {
				p := NewRingPubSub[int](2)
				defer p.Close()
				a, b := p.Subscribe(), p.Subscribe()
				defer a.Close()
				defer b.Close()
				for _, v := range []int{1, 2} {
					if _, err := p.TrySend(v); err != nil {
						t.Fatal(err)
					}
				}
				if selected {
					select {
					case <-a.Ready():
					default:
						t.Fatal("publication did not signal the reader")
					}
				}
				if hubClosed {
					p.Close()
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if got, err := a.Recv(ctx); got != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled Recv with backlog = (%d, %v)", got, err)
				}
				// A keeps its obligation, but must not keep the only notification.
				// B can drain both messages while A is paused after cancellation.
				ringTestReceive(t, &b, 1)
				ringTestReceive(t, &b, 2)
				if !hubClosed {
					if _, err := p.TrySend(3); !errors.Is(err, ErrRingFull) {
						t.Fatalf("cancellation unexpectedly acknowledged backlog: %v", err)
					}
				}
				ringTestReceive(t, &a, 1)
				ringTestReceive(t, &a, 2)
				if hubClosed {
					if _, err := a.Recv(nil); !errors.Is(err, io.EOF) {
						t.Fatalf("drained reader returned %v, want EOF", err)
					}
				} else {
					ringTestNotReady(t, a.Ready())
				}
			})
		}
	}
}

func TestRingSubscriptionZeroValue(t *testing.T) {
	var s RingSubscription[int]
	select {
	case _, ok := <-s.Ready():
		if ok {
			t.Fatal("zero-value subscription notification was not closed")
		}
	default:
		t.Fatal("zero-value subscription is not immediately terminal")
	}
	if v, ok := s.TryRecv(); v != 0 || ok {
		t.Fatalf("zero-value TryRecv = (%d, %v)", v, ok)
	}
	output := []int{1, 2}
	if n := s.Read(output); n != 0 || !slices.Equal(output, []int{1, 2}) {
		t.Fatalf("zero-value Read = (%v, %d)", output, n)
	}
	if v, err := s.Recv(nil); v != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("zero-value Recv = (%d, %v)", v, err)
	}
	s.Close()
	s.Close()
}

func TestRingPubSubReleasesPayloadReferences(t *testing.T) {
	type payload struct{ value int }
	p := NewRingPubSub[*payload](3)
	defer p.Close()
	a, b := p.Subscribe(), p.Subscribe()
	defer a.Close()
	defer b.Close()
	values := []*payload{{1}, {2}, {3}}
	for _, v := range values {
		if _, err := p.TrySend(v); err != nil {
			t.Fatal(err)
		}
	}
	var output [3]*payload
	if n := a.Read(output[:]); n != len(output) || !slices.Equal(output[:], values) {
		t.Fatalf("first reader = %v (%d)", output, n)
	}
	for i, v := range values {
		if p.slots[i].value != v {
			t.Fatalf("slot %d cleared before the remaining subscriber consumed it", i)
		}
	}
	if v, ok := b.TryRecv(); !ok || v != values[0] {
		t.Fatalf("final receive = (%v, %v)", v, ok)
	}
	if p.slots[0].value != nil {
		t.Fatal("last receive retained a payload reference")
	}
	p.Close()
	b.Close() // abandon the two remaining values after hub shutdown
	for i := range p.slots {
		if p.slots[i].value != nil {
			t.Fatalf("slot %d retained an abandoned payload reference", i)
		}
	}
}

// Hold the hub mutex to pause the final reader after it has released its atomic
// obligation. A publisher must be able to help reclaim and publish before that
// reader runs its reaper. The delayed reaper must then inspect the current
// prefix, rather than retiring its captured old slot a second time.
func TestRingPubSubPublisherHelpsDelayedFinalReader(t *testing.T) {
	p := NewRingPubSub[int](1)
	defer p.Close()
	s := p.Subscribe()
	if _, err := p.TrySend(1); err != nil {
		t.Fatal(err)
	}
	type result struct {
		value int
		ok    bool
	}
	read := make(chan result, 1)
	p.mu.Lock()
	head := p.head
	go func() {
		value, ok := s.TryRecv()
		read <- result{value, ok}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for p.slots[head].remaining.Load() != 0 {
		if time.Now().After(deadline) {
			p.mu.Unlock()
			t.Fatal("last reader did not release its atomic obligation")
		}
		runtime.Gosched()
	}
	// Test the locked production path while deliberately excluding the
	// delayed reader's lock acquisition; no test hook is needed in the code.
	n, err := p.trySendLocked(2)
	p.mu.Unlock()
	if n != 1 || err != nil {
		t.Fatalf("publisher failed to help final reader: (%d, %v)", n, err)
	}
	select {
	case got := <-read:
		if !got.ok || got.value != 1 {
			t.Fatalf("delayed reader = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delayed reader did not finish")
	}
	defer s.Close()
	if _, err := p.TrySend(3); !errors.Is(err, ErrRingFull) {
		t.Fatalf("delayed reaper corrupted the newly occupied slot: %v", err)
	}
	ringTestReceive(t, &s, 2)
	if _, err := p.TrySend(3); err != nil {
		t.Fatal(err)
	}
	ringTestReceive(t, &s, 3)
}

// A batch retirement coalesces its space notifications. Publishers must relay
// that hint while capacity remains, or only one blocked publisher will wake.
func TestRingPubSubSpaceHintReachesAllPublishers(t *testing.T) {
	const capacity = 8
	p := NewRingPubSub[int](capacity)
	defer p.Close()
	s := p.Subscribe()
	defer s.Close()
	for i := range capacity {
		if _, err := p.TrySend(i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, capacity)
	results := make(chan error, capacity)
	for i := range capacity {
		go func(v int) {
			started <- struct{}{}
			n, err := p.Send(ctx, 100+v)
			if err == nil && n != 1 {
				err = fmt.Errorf("recipient count %d, want 1", n)
			}
			results <- err
		}(i)
	}
	for range capacity {
		<-started
	}
	// The assertion does not depend on a scheduling deadline; this just makes
	// it likely that all producers exercise the blocked path in normal runs.
	for range capacity * 4 {
		runtime.Gosched()
	}
	var out [capacity]int
	if n := s.Read(out[:]); n != capacity {
		t.Fatalf("Read freed %d values, want %d", n, capacity)
	}
	for i, v := range out {
		if v != i {
			t.Fatalf("Read[%d] = %d", i, v)
		}
	}
	for range capacity {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("free capacity stranded blocked publishers")
		}
	}
	if n := s.Read(out[:]); n != capacity {
		t.Fatalf("published %d values, want %d", n, capacity)
	}
	slices.Sort(out[:])
	for i, v := range out {
		if v != 100+i {
			t.Fatalf("publication[%d] = %d, want %d", i, v, 100+i)
		}
	}
}

func TestRingPubSubCloseWakesBlockedPublishers(t *testing.T) {
	p := NewRingPubSub[int](1)
	s := p.Subscribe()
	defer s.Close()
	p.TrySend(1)
	const publishers = 16
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, publishers)
	for range publishers {
		go func() {
			_, err := p.Send(ctx, 2)
			results <- err
		}()
	}
	p.Close()
	for range publishers {
		select {
		case err := <-results:
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("blocked Send returned %v, want ErrClosedPipe", err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not wake every publisher")
		}
	}
	ringTestReceive(t, &s, 1)
}

func TestRingPubSubCanceledPublishersDoNotStrandCapacity(t *testing.T) {
	for iteration := range 50 {
		p := NewRingPubSub[int](1)
		s := p.Subscribe()
		p.TrySend(-1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cancelCtx, cancelBlocked := context.WithCancel(ctx)
		const canceled = 4
		results := make(chan error, canceled)
		for range canceled {
			go func() {
				_, err := p.Send(cancelCtx, -2)
				results <- err
			}()
		}
		live := make(chan error, 1)
		go func() {
			_, err := p.Send(ctx, iteration)
			live <- err
		}()
		for range 16 {
			runtime.Gosched()
		}
		cancelBlocked()
		type readResult struct {
			values []int
			err    error
		}
		read := make(chan readResult, 1)
		go func() {
			defer s.Close()
			var r readResult
			for {
				v, err := s.Recv(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						r.err = err
					}
					read <- r
					return
				}
				r.values = append(r.values, v)
			}
		}()
		// A publisher can have passed its cancellation check before being
		// delayed on the hub mutex. Such an overlapping successful publication
		// is allowed, and must correspond to exactly one delivered value.
		committed := 0
		for range canceled {
			select {
			case err := <-results:
				if err == nil {
					committed++
				} else if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled blocked publisher returned %v", err)
				}
			case <-ctx.Done():
				t.Fatal("canceled publisher remained blocked")
			}
		}
		select {
		case err := <-live:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("canceled publisher consumed the sole space hint")
		}
		p.Close()
		select {
		case r := <-read:
			if r.err != nil || len(r.values) != committed+2 {
				t.Fatalf("drain after cancellation = %v, %v; successful canceled sends=%d", r.values, r.err, committed)
			}
			if r.values[0] != -1 {
				t.Fatalf("initial value = %d, want -1", r.values[0])
			}
			seenLive, seenCanceled := 0, 0
			for _, v := range r.values[1:] {
				switch v {
				case iteration:
					seenLive++
				case -2:
					seenCanceled++
				default:
					t.Fatalf("unexpected delivered value %d", v)
				}
			}
			if seenLive != 1 || seenCanceled != committed {
				t.Fatalf("delivered live=%d, canceled=%d; committed canceled=%d", seenLive, seenCanceled, committed)
			}
		case <-ctx.Done():
			t.Fatal("consumer did not drain after cancellation")
		}
		cancel()
	}
}

func TestRingPubSubConcurrentPublishersAgreeOnOrder(t *testing.T) {
	const publishers, perPublisher, subscribers = 4, 250, 8
	p := NewRingPubSub[int](8)
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	subs := make([]RingSubscription[int], subscribers)
	for i := range subs {
		subs[i] = p.Subscribe()
	}
	type result struct {
		values []int
		err    error
	}
	consumed := make(chan result, subscribers)
	for i := range subs {
		go func(s *RingSubscription[int]) {
			defer s.Close()
			r := result{values: make([]int, 0, publishers*perPublisher)}
			for {
				v, err := s.Recv(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						r.err = err
					}
					consumed <- r
					return
				}
				r.values = append(r.values, v)
			}
		}(&subs[i])
	}
	produced := make(chan error, publishers)
	for producer := range publishers {
		go func(id int) {
			for i := range perPublisher {
				n, err := p.Send(ctx, id*perPublisher+i)
				if err != nil {
					produced <- err
					return
				}
				if n != subscribers {
					produced <- fmt.Errorf("Send recipients %d, want %d", n, subscribers)
					return
				}
			}
			produced <- nil
		}(producer)
	}
	for range publishers {
		select {
		case err := <-produced:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("publishers did not complete")
		}
	}
	p.Close()
	var first []int
	for reader := range subscribers {
		select {
		case r := <-consumed:
			if r.err != nil {
				t.Fatal(r.err)
			}
			if len(r.values) != publishers*perPublisher {
				t.Fatalf("reader %d received %d values, want %d", reader, len(r.values), publishers*perPublisher)
			}
			if reader == 0 {
				first = r.values
			} else if !slices.Equal(first, r.values) {
				t.Fatal("subscribers observed different publication orders")
			}
		case <-ctx.Done():
			t.Fatal("readers did not drain")
		}
	}
	seen := make([]bool, publishers*perPublisher)
	previous := make([]int, publishers)
	for i := range previous {
		previous[i] = -1
	}
	for _, v := range first {
		if v < 0 || v >= len(seen) || seen[v] {
			t.Fatalf("out-of-range or duplicate value %d", v)
		}
		seen[v] = true
		producer, index := v/perPublisher, v%perPublisher
		if index != previous[producer]+1 {
			t.Fatalf("publisher %d order: got %d after %d", producer, index, previous[producer])
		}
		previous[producer] = index
	}
}

func TestRingPubSubConcurrentSubscriptionChurn(t *testing.T) {
	const publications, churners = 1000, 8
	p := NewRingPubSub[int](4)
	defer p.Close()
	stable := p.Subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan error, churners+2)
	go func() {
		defer stable.Close()
		for want := range publications {
			got, err := stable.Recv(ctx)
			if err != nil || got != want {
				results <- fmt.Errorf("stable reader got (%d, %v), want %d", got, err, want)
				return
			}
		}
		results <- nil
	}()
	var ready sync.WaitGroup
	ready.Add(churners)
	for id := range churners {
		go func(id int) {
			ready.Done()
			for iteration := range 100 {
				s := p.Subscribe()
				last := -1
				for range (iteration + id) % 5 {
					v, err := s.Recv(ctx)
					if errors.Is(err, io.EOF) {
						s.Close()
						results <- nil
						return
					}
					if err != nil || v < 0 || v >= publications || (last >= 0 && v != last+1) {
						s.Close()
						results <- fmt.Errorf("churn reader %d got (%d, %v) after %d", id, v, err, last)
						return
					}
					last = v
				}
				s.Close()
			}
			results <- nil
		}(id)
	}
	ready.Wait()
	go func() {
		for i := range publications {
			if _, err := p.Send(ctx, i); err != nil {
				results <- err
				return
			}
		}
		p.Close()
		results <- nil
	}()
	for range churners + 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("subscription churn stalled progress")
		}
	}
}

// This oracle stores one ordinary FIFO per subscription. It has no knowledge
// of ring indices, reference counts, notification lanes, or the implementation's
// state. The longest FIFO determines retention because live readers receive
// every publication made after their respective subscriptions.
func ringTestModel(t *testing.T, capacity int, operations []byte) {
	t.Helper()
	p := NewRingPubSub[int](capacity)
	defer p.Close()
	type reader struct {
		s       *RingSubscription[int]
		pending []int
	}
	var readers [8]reader
	defer func() {
		for _, r := range readers {
			if r.s != nil {
				r.s.Close()
			}
		}
	}()
	serial := 0
	for step, op := range operations {
		index := int(op>>3) % len(readers)
		r := &readers[index]
		switch op & 7 {
		case 0, 1:
			if r.s == nil {
				s := p.Subscribe()
				r.s = &s
			}
		case 2, 3:
			active, retained := 0, 0
			for _, q := range readers {
				if q.s != nil {
					active++
					retained = max(retained, len(q.pending))
				}
			}
			serial++
			n, err := p.TrySend(serial)
			if retained == capacity {
				if n != 0 || !errors.Is(err, ErrRingFull) {
					t.Fatalf("step %d: full TrySend = (%d, %v)", step, n, err)
				}
			} else {
				if n != active || err != nil {
					t.Fatalf("step %d: TrySend = (%d, %v), want (%d, nil)", step, n, err, active)
				}
				for i := range readers {
					if readers[i].s != nil {
						readers[i].pending = append(readers[i].pending, serial)
					}
				}
			}
		case 4, 5:
			if r.s == nil {
				break
			}
			if op&7 == 5 {
				select {
				case <-r.s.Ready():
					if len(r.pending) == 0 {
						t.Fatalf("step %d: empty subscriber %d became ready", step, index)
					}
				default:
					if len(r.pending) != 0 {
						t.Fatalf("step %d: subscriber %d has data without a wake", step, index)
					}
				}
			}
			got, ok := r.s.TryRecv()
			if len(r.pending) == 0 {
				if got != 0 || ok {
					t.Fatalf("step %d: empty TryRecv = (%d, %v)", step, got, ok)
				}
			} else {
				if !ok || got != r.pending[0] {
					t.Fatalf("step %d: TryRecv = (%d, %v), want %d", step, got, ok, r.pending[0])
				}
				r.pending = r.pending[1:]
			}
		case 6:
			if r.s != nil {
				var output [9]int
				for i := range output {
					output[i] = -1
				}
				size := int(op>>4) % len(output)
				wantN := min(size, len(r.pending))
				n := r.s.Read(output[:size])
				if n != wantN || !slices.Equal(output[:n], r.pending[:wantN]) {
					t.Fatalf("step %d: Read = %v (%d), want %v", step, output, n, r.pending[:wantN])
				}
				for _, v := range output[n:] {
					if v != -1 {
						t.Fatalf("step %d: Read changed output beyond returned count", step)
					}
				}
				r.pending = r.pending[n:]
			}
		case 7:
			if r.s != nil {
				r.s.Close()
				r.s.Close()
				r.s, r.pending = nil, nil
			}
		}
		for i := range readers {
			if readers[i].s != nil && len(readers[i].pending) == 0 {
				select {
				case <-readers[i].s.Ready():
					t.Fatalf("step %d: caught-up subscriber %d received another cohort's wake", step, i)
				default:
				}
			}
		}
	}
	p.Close()
	for i := range readers {
		r := &readers[i]
		if r.s == nil {
			continue
		}
		for _, want := range r.pending {
			ringTestReceive(t, r.s, want)
		}
		if _, err := r.s.Recv(nil); !errors.Is(err, io.EOF) {
			t.Fatalf("final subscriber %d drain = %v", i, err)
		}
	}
}

func TestRingPubSubModel(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 7, 16} {
		for seed := range uint64(8) {
			t.Run(fmt.Sprintf("capacity=%d/seed=%d", capacity, seed), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, seed+1))
				operations := make([]byte, 2000)
				for i := range operations {
					operations[i] = byte(rng.Uint32())
				}
				ringTestModel(t, capacity, operations)
			})
		}
	}
}

func FuzzRingPubSubModel(f *testing.F) {
	f.Add([]byte{1, 0, 8, 2, 4, 13, 7, 2, 2, 2, 14, 15})
	f.Add([]byte{3, 0, 8, 16, 2, 2, 2, 5, 13, 21, 7, 15, 23})
	f.Add([]byte{7})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		if len(input) > 4097 {
			input = input[:4097]
		}
		ringTestModel(t, int(input[0]%16)+1, input[1:])
	})
}

func TestRingPubSubZeroSteadyStateAllocations(t *testing.T) {
	t.Run("subscription churn", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		if n := testing.AllocsPerRun(1000, func() {
			s := p.Subscribe()
			s.Close()
		}); n != 0 {
			t.Fatalf("Subscribe/Close allocated %.3f objects per cycle", n)
		}
	})
	t.Run("publication and selected receive", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		a, b := p.Subscribe(), p.Subscribe()
		defer a.Close()
		defer b.Close()
		var failed bool
		allocs := testing.AllocsPerRun(1000, func() {
			if n, err := p.Send(nil, 42); n != 2 || err != nil {
				failed = true
			}
			<-a.Ready()
			if got, ok := a.TryRecv(); !ok || got != 42 {
				failed = true
			}
			<-b.Ready()
			if got, ok := b.TryRecv(); !ok || got != 42 {
				failed = true
			}
		})
		if failed || allocs != 0 {
			t.Fatalf("Send/select/TryRecv: failure=%v, allocations=%.3f", failed, allocs)
		}
	})
	t.Run("queued unsubscribe", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		var failed bool
		allocs := testing.AllocsPerRun(1000, func() {
			s := p.Subscribe()
			for i := range 4 {
				if n, err := p.TrySend(i); n != 1 || err != nil {
					failed = true
				}
			}
			s.Close()
		})
		if failed || allocs != 0 {
			t.Fatalf("Subscribe/publish/Close: failure=%v, allocations=%.3f", failed, allocs)
		}
	})
	t.Run("batch read", func(t *testing.T) {
		p := NewRingPubSub[int](4)
		defer p.Close()
		s := p.Subscribe()
		defer s.Close()
		var output [4]int
		var failed bool
		allocs := testing.AllocsPerRun(1000, func() {
			for i := range output {
				if _, err := p.TrySend(i); err != nil {
					failed = true
				}
			}
			if n := s.Read(output[:]); n != len(output) {
				failed = true
			}
		})
		if failed || allocs != 0 {
			t.Fatalf("publish/Read: failure=%v, allocations=%.3f", failed, allocs)
		}
	})
}
