package bigbuff

// Tests written in the completion phase; the response shipped none and the
// first integration relied on the shared harness, which subscribes from one
// goroutine. Repairs 7 to 11 (see the header of vecast_31_nemotron.go) each
// have a regression test here.

import (
	"cmp"
	"slices"
	"sync"
	"testing"
)

// vecastReceivers starts one goroutine per subscription that receives until
// its channel is closed or stop closes, counting values per subscription.
type vecastReceivers struct {
	wg     sync.WaitGroup
	stop   chan struct{}
	counts []int
}

func startVecastReceivers(subs []*VecastSubscription[int]) *vecastReceivers {
	r := &vecastReceivers{stop: make(chan struct{}), counts: make([]int, len(subs))}
	for i, s := range subs {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for {
				select {
				case _, ok := <-s.C():
					if !ok {
						return
					}
					r.counts[i]++
				case <-r.stop:
					return
				}
			}
		}()
	}
	return r
}

// finish stops the receivers and returns the per-subscription counts.
func (r *vecastReceivers) finish(t *testing.T) []int {
	t.Helper()
	close(r.stop)
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	apexAwait(t, done, "receivers stopping")
	return r.counts
}

// vecastInSendOrder subscribes n times and returns the subscriptions in the
// order a Send visits them (shard, then slot), in both of its phases. Tests
// that need one subscriber served before another must not assume Subscribe
// order: two calls from one goroutine normally share a shard, but the shard
// is derived from a stack address and a stack can move.
func vecastInSendOrder(v *Vecast[int], n int) []*VecastSubscription[int] {
	subs := make([]*VecastSubscription[int], n)
	for i := range subs {
		subs[i] = v.Subscribe()
	}
	shardIndex := func(s *VecastSubscription[int]) int {
		for i := range v.shards {
			if &v.shards[i] == s.shard {
				return i
			}
		}
		panic("subscription belongs to no shard")
	}
	slices.SortFunc(subs, func(a, b *VecastSubscription[int]) int {
		return cmp.Or(cmp.Compare(shardIndex(a), shardIndex(b)), cmp.Compare(a.idx, b.idx))
	})
	return subs
}

func vecastSend(v *Vecast[int], value int) <-chan int {
	out := make(chan int, 1)
	go func() { out <- v.Send(value) }()
	return out
}

func vecastAwaitSend(t *testing.T, sent <-chan int, want int, what string) {
	t.Helper()
	select {
	case n := <-sent:
		if n != want {
			t.Fatalf("%s: Send = %d, want %d", what, n, want)
		}
	case <-timeAfterGuard():
		t.Fatalf("%s: Send did not return", what)
	}
}

func TestVecast_FanoutCountsHandoffs(t *testing.T) {
	v := NewVecast[int]()
	if n := v.Send(1); n != 0 {
		t.Fatalf("Send with no subscribers = %d", n)
	}
	subs := make([]*VecastSubscription[int], 5)
	for i := range subs {
		subs[i] = v.Subscribe()
	}
	if v.Len() != 5 {
		t.Fatalf("Len = %d, want 5", v.Len())
	}
	r := startVecastReceivers(subs)
	for round := range 20 {
		vecastAwaitSend(t, vecastSend(v, round), 5, "fan-out")
	}
	for i, n := range r.finish(t) {
		if n != 20 {
			t.Errorf("subscriber %d received %d values, want 20", i, n)
		}
	}
}

// TestVecast_SendWaitsForASubscriberThatIsNotReady covers the second phase:
// the value is handed over when the subscriber gets round to receiving.
func TestVecast_SendWaitsForASubscriberThatIsNotReady(t *testing.T) {
	v := NewVecast[int]()
	s := v.Subscribe()
	sent := vecastSend(v, 7)
	apexStillBlocked(t, sent, "Send, before its only subscriber has received")
	select {
	case got := <-s.C():
		if got != 7 {
			t.Fatalf("received %d, want 7", got)
		}
	case <-timeAfterGuard():
		t.Fatal("the value was never offered to the subscriber")
	}
	vecastAwaitSend(t, sent, 1, "slow handoff")
}

// TestVecast_ConcurrentSubscribersGetDistinctSlots is the regression test for
// repair 7. As delivered, concurrent Subscribe calls could take the same
// slot; the loser's channel was overwritten and it never received anything.
func TestVecast_ConcurrentSubscribersGetDistinctSlots(t *testing.T) {
	const subscribers = 8
	for round := range 300 {
		v := NewVecast[int]()
		subs := make([]*VecastSubscription[int], subscribers)
		var wg sync.WaitGroup
		for i := range subs {
			wg.Add(1)
			go func() { defer wg.Done(); subs[i] = v.Subscribe() }()
		}
		wg.Wait()
		if n := v.Len(); n != subscribers {
			t.Fatalf("round %d: %d of %d concurrent subscriptions are registered", round, n, subscribers)
		}
		r := startVecastReceivers(subs)
		vecastAwaitSend(t, vecastSend(v, round), subscribers, "after concurrent Subscribe")
		for i, n := range r.finish(t) {
			if n != 1 {
				t.Fatalf("round %d: subscriber %d received %d values, want 1", round, i, n)
			}
		}
	}
}

// TestVecast_GrowthAndSlotReuse fills one shard past its initial capacity
// (all subscriptions come from this goroutine, so they share a shard), then
// churns it.
func TestVecast_GrowthAndSlotReuse(t *testing.T) {
	const total = vecastInitialCap + 44
	v := NewVecast[int]()
	subs := make([]*VecastSubscription[int], total)
	for i := range subs {
		subs[i] = v.Subscribe()
	}
	for _, s := range subs[1:] {
		if s.shard != subs[0].shard {
			t.Fatal("subscriptions from one goroutine landed in different shards; the test needs one shard to grow")
		}
	}
	if gen := subs[0].shard.vec.Load().gen; gen != 2 {
		t.Fatalf("shard vector generation = %d after %d subscriptions, want 2 (grown once)", gen, total)
	}
	// Withdraw every third subscriber and replace it.
	for i := 0; i < total; i += 3 {
		v.Unsubscribe(subs[i])
		v.Unsubscribe(subs[i]) // idempotent
	}
	if want := total - (total+2)/3; v.Len() != want {
		t.Fatalf("Len = %d after withdrawals, want %d", v.Len(), want)
	}
	for i := 0; i < total; i += 3 {
		subs[i] = v.Subscribe()
	}
	if v.Len() != total {
		t.Fatalf("Len = %d after replacing the withdrawn subscribers, want %d", v.Len(), total)
	}
	if gen := subs[0].shard.vec.Load().gen; gen != 2 {
		t.Fatalf("shard grew again (generation %d) although freed slots were available", gen)
	}
	r := startVecastReceivers(subs)
	vecastAwaitSend(t, vecastSend(v, 1), total, "after growth and reuse")
	for i, n := range r.finish(t) {
		if n != 1 {
			t.Fatalf("subscriber %d received %d values, want exactly 1", i, n)
		}
	}
}

// TestVecast_GoroutinesSpreadAcrossShards is the regression test for repair
// 11: as delivered every subscriber landed in the same shard.
func TestVecast_GoroutinesSpreadAcrossShards(t *testing.T) {
	const goroutines = 64
	v := NewVecast[int]()
	subs := make([]*VecastSubscription[int], goroutines)
	var subscribed, release sync.WaitGroup
	release.Add(1)
	for i := range subs {
		subscribed.Add(1)
		go func() {
			subs[i] = v.Subscribe()
			subscribed.Done()
			release.Wait() // keep every stack alive, so no two goroutines share one
		}()
	}
	subscribed.Wait()
	release.Done()
	shards := map[*vecastShard[int]]bool{}
	for _, s := range subs {
		shards[s.shard] = true
	}
	if len(shards) < 2 {
		t.Fatalf("%d goroutines subscribed into %d shard(s)", goroutines, len(shards))
	}
	t.Logf("%d goroutines used %d of %d shards", goroutines, len(shards), vecastShards)
}

// TestVecast_UnsubscribeReleasesAWaitingSend is the regression test for
// repair 8. The idle subscriber never receives. As delivered, Unsubscribe
// drained the channel, so it swallowed the value the Send was offering and
// the Send reported two handoffs for one.
func TestVecast_UnsubscribeReleasesAWaitingSend(t *testing.T) {
	v := NewVecast[int]()
	subs := vecastInSendOrder(v, 2)
	busy, idle := subs[0], subs[1] // busy is served first in either phase
	sent := vecastSend(v, 1)
	select {
	case <-busy.C(): // proves the Send is under way
	case <-timeAfterGuard():
		t.Fatal("the ready subscriber never received")
	}
	apexStillBlocked(t, sent, "Send, with one subscriber neither receiving nor gone")
	v.Unsubscribe(idle)
	vecastAwaitSend(t, sent, 1, "after the idle subscriber withdrew")
	if v.Len() != 1 {
		t.Fatalf("Len = %d, want 1", v.Len())
	}
}

// TestVecast_WithdrawnChannelIsNotHandedToTheNextSubscriber is the
// regression test for repair 9. A Send is waiting for a subscriber that then
// withdraws; a new subscriber joins before the Send finishes. As delivered
// the new subscriber was given the withdrawn one's channel from the pool, on
// which that Send was still trying to deliver.
func TestVecast_WithdrawnChannelIsNotHandedToTheNextSubscriber(t *testing.T) {
	v := NewVecast[int]()
	subs := vecastInSendOrder(v, 2)
	busy, leaving := subs[0], subs[1] // busy is served first in either phase
	sent := vecastSend(v, 1)
	select {
	case <-busy.C():
	case <-timeAfterGuard():
		t.Fatal("the ready subscriber never received")
	}
	v.Unsubscribe(leaving)
	joined := v.Subscribe()
	if joined.C() == leaving.C() {
		t.Fatal("the new subscriber was given the withdrawn subscriber's channel")
	}
	vecastAwaitSend(t, sent, 1, "after the withdrawal")
	// The Send has returned, so nothing can still be in flight towards the
	// new subscriber: it must have received nothing from a Send that began
	// before it subscribed.
	select {
	case got := <-joined.C():
		t.Fatalf("the new subscriber received %d from a Send that began before it subscribed", got)
	default:
	}
}

func TestVecast_Close(t *testing.T) {
	v := NewVecast[int]()
	subs := vecastInSendOrder(v, 2)
	other, idle := subs[0], subs[1] // other is served first in either phase
	sent := vecastSend(v, 1)
	select {
	case <-other.C():
	case <-timeAfterGuard():
		t.Fatal("the ready subscriber never received")
	}
	if err := v.Close(); err != nil { // releases the Send waiting on idle
		t.Fatalf("Close: %v", err)
	}
	vecastAwaitSend(t, sent, 1, "released by Close")
	if err := v.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !v.Closed() {
		t.Fatal("Closed() = false after Close")
	}
	for name, s := range map[string]*VecastSubscription[int]{"idle": idle, "other": other} {
		select {
		case _, ok := <-s.C():
			if ok {
				t.Fatalf("%s subscriber received a value after Close", name)
			}
		case <-timeAfterGuard():
			t.Fatalf("%s subscriber's channel was not closed by Close", name)
		}
	}
	if n := v.Send(2); n != 0 {
		t.Fatalf("Send after Close = %d", n)
	}
	if v.Len() != 0 {
		t.Fatalf("Len = %d after Close", v.Len())
	}
	v.Unsubscribe(idle) // no-op, must not panic or block
	v.Unsubscribe(nil)
	defer func() {
		if recover() == nil {
			t.Error("Subscribe after Close did not panic")
		}
	}()
	v.Subscribe()
}

// TestVecast_CloseDuringSends is the regression test for repair 10: Close
// used to close channels that a concurrent Send had already loaded, and a
// send on a closed channel panics.
func TestVecast_CloseDuringSends(t *testing.T) {
	for round := range 2000 {
		v := NewVecast[int]()
		subs := []*VecastSubscription[int]{v.Subscribe(), v.Subscribe(), v.Subscribe()}
		r := startVecastReceivers(subs)
		sending := make(chan struct{})
		go func() {
			defer close(sending)
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("round %d: Send panicked during Close: %v", round, p)
				}
			}()
			for i := range 20 {
				v.Send(i)
			}
		}()
		v.Close()
		apexAwait(t, sending, "sends racing Close")
		r.finish(t)
		if t.Failed() {
			return
		}
	}
}

// TestVecast_ChurnAgainstSends runs subscribers that join, take a few values
// and leave, against a publisher. The publisher must never be stranded by a
// departure, and one Send must never reach a subscriber twice.
func TestVecast_ChurnAgainstSends(t *testing.T) {
	const rounds, churners = 4000, 4
	v := NewVecast[int]()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				s := v.Subscribe()
				last := -1
				for range 3 {
					select {
					case got := <-s.C():
						if got <= last {
							t.Errorf("a subscriber received %d after %d", got, last)
						}
						last = got
					case <-stop:
						v.Unsubscribe(s)
						return
					}
				}
				v.Unsubscribe(s)
			}
		}()
	}
	// One subscriber stays, so that every Send has a handoff to wait for.
	steady := v.Subscribe()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-steady.C():
			case <-stop:
				v.Unsubscribe(steady)
				return
			}
		}
	}()
	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := range rounds {
			if v.Send(i) < 1 {
				t.Error("Send did not reach the steady subscriber")
				return
			}
		}
	}()
	select {
	case <-published:
	case <-timeAfterGuard():
		t.Fatal("the publisher was stranded by a departing subscriber")
	}
	close(stop)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "subscribers leaving")
	if v.Len() != 0 {
		t.Fatalf("Len = %d after every subscriber left", v.Len())
	}
}

// TestVecast_Allocations records the measured cost against the response's
// "zero allocations on all hot paths (Send, Subscribe, Unsubscribe)".
func TestVecast_Allocations(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	v := NewVecast[int]()
	if n := testing.AllocsPerRun(100, func() { v.Send(1) }); n != 0 {
		t.Errorf("Send with no subscribers allocates %v per call, want 0", n)
	}
	if n := testing.AllocsPerRun(100, func() { v.Unsubscribe(v.Subscribe()) }); n != 4 {
		t.Errorf("Subscribe+Unsubscribe allocates %v per pair, want 4 (handle, record, two channels)", n)
	}
}
