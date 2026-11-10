package bigbuff

// Tests for EpochPubSub's manual subscription path (Notify / Current / Ack),
// written during integration. The response shipped no tests for it, and the
// shared harness reached it only through an adapter whose two races (found
// by the broadcast op-stream fuzz target) are pinned here at the source.

import (
	"sync"
	"testing"
)

// epochManualNext blocks until a round newer than last is published, or stop
// closes. It reads Notify before Current, so a round published in between is
// seen by the re-check rather than slept through.
func epochManualNext(x *EpochPubSub[int], last uint64, stop <-chan struct{}) (v int, seq uint64, ok bool) {
	for {
		notify := x.Notify()
		if v, s, ok := x.Current(); ok && s > last {
			return v, s, true
		}
		select {
		case <-notify:
		case <-stop:
			return 0, last, false
		}
	}
}

func epochSend(x *EpochPubSub[int], v int) <-chan int {
	out := make(chan int, 1)
	go func() { out <- x.Send(v) }()
	return out
}

func epochAwaitSend(t *testing.T, sent <-chan int, want int, what string) {
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

func TestEpochPubSub_SubscribeEpochReturnsLastCompletedRound(t *testing.T) {
	x := NewEpochPubSub[int]()
	first := x.SubscribeEpoch()
	if first != 0 {
		t.Fatalf("SubscribeEpoch before any round = %d, want 0", first)
	}
	sent := epochSend(x, 7)
	v, seq, _ := epochManualNext(x, first, nil)
	if v != 7 {
		t.Fatalf("received %d, want 7", v)
	}
	x.Ack(seq)
	epochAwaitSend(t, sent, 1, "round one")

	// A subscriber that joins now owes nothing for the completed round.
	second := x.SubscribeEpoch()
	if second != seq {
		t.Fatalf("SubscribeEpoch after round %d = %d", seq, second)
	}
	sent = epochSend(x, 8)
	for _, last := range []uint64{seq, second} { // both subscribers ack
		_, s, _ := epochManualNext(x, last, nil)
		x.Ack(s)
	}
	epochAwaitSend(t, sent, 2, "round two")
	x.UnsubscribeAcked(seq + 1)
	x.UnsubscribeAcked(seq + 1)
	if n := x.Add(0); n != 0 {
		t.Fatalf("%d subscribers remain", n)
	}
}

// TestEpochPubSub_LeavingAfterAckDoesNotReleaseTheRoundTwice pins the
// UnsubscribeAcked repair: a subscriber that acknowledged the round in flight
// and then left used to be compensated as well, so Send returned while
// another subscriber still owed its acknowledgement.
func TestEpochPubSub_LeavingAfterAckDoesNotReleaseTheRoundTwice(t *testing.T) {
	x := NewEpochPubSub[int]()
	a, b := x.SubscribeEpoch(), x.SubscribeEpoch()
	sent := epochSend(x, 1)

	_, seq, _ := epochManualNext(x, a, nil)
	x.Ack(seq)
	x.UnsubscribeAcked(seq)
	// Both calls above are synchronous, so the barrier is exactly one short.
	if n := x.pongNLoad(); n != 1 {
		t.Fatalf("barrier holds %d after one of two subscribers acknowledged and left, want 1", n)
	}
	apexStillBlocked(t, sent, "Send, with one subscriber still owing its acknowledgement")

	_, seq, _ = epochManualNext(x, b, nil)
	x.Ack(seq)
	epochAwaitSend(t, sent, 2, "the round")
	x.UnsubscribeAcked(seq)
}

// TestEpochPubSub_LeavingWithoutAckCompensatesTheRound covers the other half
// of the contract: a subscriber that leaves mid-round without acknowledging
// is compensated exactly once, by either unsubscribe method.
func TestEpochPubSub_LeavingWithoutAckCompensatesTheRound(t *testing.T) {
	for name, leave := range map[string]func(x *EpochPubSub[int], joinedAt uint64){
		"Unsubscribe":      func(x *EpochPubSub[int], _ uint64) { x.Unsubscribe() },
		"UnsubscribeAcked": func(x *EpochPubSub[int], joinedAt uint64) { x.UnsubscribeAcked(joinedAt) },
	} {
		t.Run(name, func(t *testing.T) {
			x := NewEpochPubSub[int]()
			a, b := x.SubscribeEpoch(), x.SubscribeEpoch()
			sent := epochSend(x, 1)

			// Seeing the round proves it is in flight; leave without acking.
			epochManualNext(x, a, nil)
			leave(x, a)
			if n := x.pongNLoad(); n != 1 {
				t.Fatalf("barrier holds %d after one of two subscribers left unacknowledged, want 1", n)
			}
			_, seq, _ := epochManualNext(x, b, nil)
			x.Ack(seq)
			epochAwaitSend(t, sent, 2, "the round")
			x.UnsubscribeAcked(seq)
		})
	}
}

// TestEpochPubSub_ManualSubscribersChurnAgainstSends drives the sequence the
// SubscribeEpoch repair exists for: subscribers registering while rounds are
// starting. Each acknowledges exactly the rounds newer than the sequence it
// registered at; a round that counted a subscriber which then never
// acknowledged it would block Send for good.
func TestEpochPubSub_ManualSubscribersChurnAgainstSends(t *testing.T) {
	const rounds = 3000
	x := NewEpochPubSub[int]()
	stop := make(chan struct{})
	var subscribers sync.WaitGroup
	for range 4 {
		subscribers.Add(1)
		go func() {
			defer subscribers.Done()
			for {
				last := x.SubscribeEpoch()
				// Take up to two rounds, then leave and rejoin.
				for range 2 {
					_, seq, ok := epochManualNext(x, last, stop)
					if !ok {
						x.UnsubscribeAcked(last)
						return
					}
					x.Ack(seq)
					last = seq
				}
				x.UnsubscribeAcked(last)
			}
		}()
	}
	// One subscriber stays for the whole run, so that every Send has to wait
	// for an acknowledgement and the churning subscribers get to run between
	// (and during) rounds rather than after the publisher has finished.
	subscribers.Add(1)
	steady := x.SubscribeEpoch()
	go func() {
		defer subscribers.Done()
		for last := steady; ; {
			_, seq, ok := epochManualNext(x, last, stop)
			if !ok {
				x.UnsubscribeAcked(last)
				return
			}
			x.Ack(seq)
			last = seq
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := range rounds {
			if x.Send(v) < 1 {
				t.Error("Send did not count the steady subscriber")
				return
			}
		}
	}()
	select {
	case <-done:
	case <-timeAfterGuard():
		t.Fatal("Send blocked on a round one of its counted subscribers never acknowledged")
	}
	close(stop)
	finished := make(chan struct{})
	go func() { subscribers.Wait(); close(finished) }()
	apexAwait(t, finished, "subscribers leaving")
	if n := x.Add(0); n != 0 {
		t.Fatalf("%d subscribers remain", n)
	}
}
