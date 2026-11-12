package bigbuff

// What ChanCaster and ChanPubSub do in the schedules that Part 1 of
// 02-prompt-kernel.md asks about.
//
// The prompt asks a model to predict these outcomes from the source and to
// give each prediction as a test. These are the tests, so they are also the
// key to that part of the prompt: a response's own tests can be set against
// them. They run inside testing/synctest bubbles, where synctest.Wait returns
// once every other goroutine is blocked on a channel or a condition variable,
// which is how "blocked for good" is shown without a sleep.
//
// Some of the outcomes the questions ask for cannot be forced from outside,
// because they need a step of one goroutine to fall between two statements of
// another. They are described where they belong and are not tested.

import (
	"testing"
	"testing/synctest"
)

// promptKernelPanics runs f and returns what it panicked with, or nil.
func promptKernelPanics(f func()) (r any) {
	defer func() { r = recover() }()
	f()
	return nil
}

// Question 1. A receiver that withdraws while a Send is under way receives the
// channel send that was addressed to it, so the sender is not left waiting,
// and Send reports the receivers that remained.
func TestPromptKernelQ1WithdrawalDuringSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		x := NewChanCaster(make(chan int))
		for range 3 { // R1, R2, R3
			x.Add(1)
		}
		sent := make(chan int, 1)
		go func() { sent <- x.Send(7) }()
		synctest.Wait() // Send holds the lock and is blocked on its first channel send

		if v := <-x.C; v != 7 { // R1
			t.Fatalf("R1 received %d, want 7", v)
		}
		synctest.Wait() // blocked on the second

		// R3 has received nothing and leaves. The subtraction shows it that a
		// Send is under way, and it takes the second channel send itself.
		if got := x.Add(-1); got != 2 {
			t.Fatalf("R3's Add(-1) = %d, want 2", got)
		}
		synctest.Wait() // blocked on the third
		select {
		case n := <-sent:
			t.Fatalf("Send returned %d before R2 received", n)
		default:
		}

		if v := <-x.C; v != 7 { // R2
			t.Fatalf("R2 received %d, want 7", v)
		}
		// Send made three channel sends and this test received two of them:
		// the third receive was inside Add(-1).
		if n := <-sent; n != 2 {
			t.Errorf("Send = %d, want 2 (three registered, one withdrew)", n)
		}
		if s := x.state.Load(); s != 0 {
			t.Errorf("state = %#x after the Send, want 0", s)
		}
	})
}

// Question 2, first ending. R3 receives and then withdraws anyway. Its
// Add(-1) takes the channel send that was addressed to R2. Send returns 2,
// nothing panics, and R2 is left receiving with no registration: no Send will
// send for it, and when it does receive, the value was addressed to someone
// else.
//
// Second ending, not forced here: R2's receive falls between the subtraction
// in R3's Add(-1) and its channel receive (or the subtraction falls between
// Send's last channel send and its closing load). Then R2 has the value, Send
// still returns 2, and it is R3's Add(-1) that is left blocked on the channel.
func TestPromptKernelQ2ReceiveThenWithdrawStrandsAnother(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		x := NewChanCaster(make(chan int))
		for range 3 {
			x.Add(1)
		}
		sent := make(chan int, 1)
		go func() { sent <- x.Send(7) }()
		synctest.Wait()
		<-x.C // R1
		synctest.Wait()
		<-x.C           // R3
		synctest.Wait() // one channel send left, addressed to R2

		if got := x.Add(-1); got != 2 { // R3, against the contract
			t.Fatalf("R3's Add(-1) = %d, want 2", got)
		}
		if n := <-sent; n != 2 {
			t.Fatalf("Send = %d, want 2 (it cannot tell)", n)
		}
		if s := x.state.Load(); s != 0 {
			t.Fatalf("state = %#x, want 0: nothing was detected", s)
		}

		r2 := make(chan int, 1)
		go func() { r2 <- <-x.C }()
		synctest.Wait()
		select {
		case v := <-r2:
			t.Fatalf("R2 received %d; its value was taken by R3's Add(-1)", v)
		default:
		}
		// Nobody is registered, so a Send sends nothing and R2 stays where it is.
		if n := x.Send(8); n != 0 {
			t.Fatalf("Send with nobody registered = %d, want 0", n)
		}
		synctest.Wait()
		select {
		case v := <-r2:
			t.Fatalf("R2 received %d from a Send that reported no receivers", v)
		default:
		}
		// The next registration is somebody else's, and R2 takes its value.
		x.Add(1)
		go func() { sent <- x.Send(9) }()
		if v := <-r2; v != 9 {
			t.Errorf("R2 received %d, want 9", v)
		}
		if n := <-sent; n != 1 {
			t.Errorf("Send = %d, want 1", n)
		}
	})
}

// Question 2, third ending. R2 receives first, so the Send finishes, counts 3
// and resets the state. R3's Add(-1) then subtracts from zero. This is the one
// ending that is detected: Add panics, and the state it leaves behind makes
// every later Send and Add panic too.
func TestPromptKernelQ2WithdrawAfterTheSendIsDetected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		x := NewChanCaster(make(chan int))
		for range 3 {
			x.Add(1)
		}
		sent := make(chan int, 1)
		go func() { sent <- x.Send(7) }()
		for range 3 { // R1, R3, R2
			<-x.C
		}
		if n := <-sent; n != 3 {
			t.Fatalf("Send = %d, want 3", n)
		}
		const violation = `bigbuff: chancaster: add: state invariant violation`
		if r := promptKernelPanics(func() { x.Add(-1) }); r != violation {
			t.Fatalf("Add(-1) after the Send: panic %v, want %q", r, violation)
		}
		if s := x.state.Load(); s != 0xFFFFFFFE_FFFFFFFF {
			t.Errorf("state = %#x, want 0xFFFFFFFEFFFFFFFF", s)
		}
		if r := promptKernelPanics(func() { x.Send(8) }); r == nil {
			t.Errorf("Send on the damaged state did not panic")
		}
		if r := promptKernelPanics(func() { x.Add(1) }); r == nil {
			t.Errorf("Add(1) on the damaged state did not panic")
		}
	})
}

// Question 3, the ChanCaster half. A receiver that receives twice on one
// registration takes the other receiver's value. Send cannot tell: it made two
// channel sends and two were received.
func TestPromptKernelQ3ReceivingTwiceTakesAnothersValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		x := NewChanCaster(make(chan int))
		x.Add(1) // R1
		x.Add(1) // R2
		sent := make(chan int, 1)
		go func() { sent <- x.Send(7) }()
		synctest.Wait()
		<-x.C // R1
		synctest.Wait()
		<-x.C // R1 again
		if n := <-sent; n != 2 {
			t.Fatalf("Send = %d, want 2", n)
		}
		if s := x.state.Load(); s != 0 {
			t.Fatalf("state = %#x, want 0", s)
		}
		r2 := make(chan int, 1)
		go func() { r2 <- <-x.C }()
		synctest.Wait()
		select {
		case v := <-r2:
			t.Fatalf("R2 received %d", v)
		default:
		}
		if n := x.Send(8); n != 0 {
			t.Fatalf("Send with nobody registered = %d, want 0", n)
		}
		x.Add(1) // R1 registers for the next value, and R2 takes it
		go func() { sent <- x.Send(9) }()
		if v := <-r2; v != 9 {
			t.Errorf("R2 received %d, want 9", v)
		}
		if n := <-sent; n != 1 {
			t.Errorf("Send = %d, want 1", n)
		}
	})
}

// Question 3, the ChanPubSub half. Wait does not return while a ping of the
// same Send is still to be received: pongN is set only after the pings are
// done. So a subscriber that keeps its contract cannot be back at the channel
// in time to take a second one.
func TestPromptKernelQ3WaitHoldsUntilThePingsAreDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ps := NewChanPubSub(make(chan int))
		ps.Add(2) // S1, S2
		sent := make(chan int, 1)
		go func() { sent <- ps.Send(7) }()
		synctest.Wait()
		<-ps.C() // S1
		waited := make(chan struct{})
		go func() { ps.Wait(); close(waited) }()
		synctest.Wait() // Send is blocked on the ping for S2, S1 inside Wait
		select {
		case <-waited:
			t.Fatal("S1's Wait returned while a ping of the same Send was outstanding")
		default:
		}
		<-ps.C() // S2
		ps.Wait()
		<-waited
		if n := <-sent; n != 2 {
			t.Errorf("Send = %d, want 2", n)
		}
	})
}

// Question 4. A subscriber that has not received unsubscribes while the Send
// holds sendingMu for writing and is blocked on the ping addressed to that
// subscriber. A blocking RLock here would wait for the Send, which is waiting
// for this subscriber. TryRLock fails, ping is seen to be armed, and the
// unsubscribe goes through ping.Add, which receives the ping.
//
// The loop is for the other reason TryRLock fails: a Send that has taken or
// queued for the write lock and has not yet added to ping (or has finished its
// pings and not yet unlocked). That state passes by itself, and the loop
// retries until the lock is free or ping is armed.
func TestPromptKernelQ4UnsubscribeDuringThePings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ps := NewChanPubSub(make(chan int))
		ps.Add(2) // S1, S2
		sent := make(chan int, 1)
		go func() { sent <- ps.Send(7) }()
		synctest.Wait()
		<-ps.C() // S1
		waited := make(chan struct{})
		go func() { ps.Wait(); close(waited) }()
		synctest.Wait() // the Send holds sendingMu and is blocked on S2's ping

		if got := ps.Add(-1); got != 1 { // S2 leaves
			t.Fatalf("Add(-1) = %d, want 1", got)
		}
		<-waited
		if n := <-sent; n != 1 {
			t.Errorf("Send = %d, want 1", n)
		}
	})
}

// Question 5, first case: the unsubscribe lands while pings are outstanding.
// It is routed through ping.Add, which receives a ping. The subscriber already
// has its own, so the one it takes was addressed to another subscriber (T),
// which never sees the value. The Send counts one pong too many and waits for
// it for good, holding sendMu, so every later Send waits behind it. broken
// stays open.
func TestPromptKernelQ5NoWaitDuringThePings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ps := NewChanPubSub(make(chan int))
		ps.Add(2) // S, T
		sent := make(chan int, 1)
		go func() { sent <- ps.Send(7) }()
		synctest.Wait()
		<-ps.C()        // S receives
		synctest.Wait() // the Send is blocked on the ping for T

		if got := ps.Add(-1); got != 1 { // S unsubscribes without Wait
			t.Fatalf("Add(-1) = %d, want 1", got)
		}
		tgot := make(chan int, 1)
		go func() { tgot <- <-ps.C() }() // T
		synctest.Wait()
		select {
		case n := <-sent:
			t.Fatalf("Send returned %d; it is owed a pong nobody will give", n)
		case v := <-tgot:
			t.Fatalf("T received %d; its ping was taken by S's unsubscribe", v)
		case <-ps.broken:
			t.Fatal("broken closed")
		default:
		}

		// Let the test end: supply the missing pong, then show T is intact.
		ps.Wait()
		if n := <-sent; n != 1 {
			t.Errorf("Send = %d, want 1", n)
		}
		go func() { sent <- ps.Send(8) }()
		if v := <-tgot; v != 8 {
			t.Errorf("T received %d, want 8", v)
		}
		ps.Wait()
		if n := <-sent; n != 1 {
			t.Errorf("second Send = %d, want 1", n)
		}
	})
}

// Question 5, second case: the unsubscribe lands after the pings, when
// sendingMu is free again. It is a plain decrement and returns. Nobody loses
// a value, and the Send still waits for good for the pong of the subscriber
// that left. broken stays open.
//
// Two further cases need a step to fall between two statements of the Send
// and are not forced here. If the unsubscribe sees ping armed and its
// subtraction lands after the last ping was received and before the caster's
// closing load, the Send returns normally with one fewer, and it is the
// unsubscribe that blocks on the channel until the next Send, whose ping it
// then takes from a subscriber. If the caster has already reset when the
// subtraction lands, it underflows: ChanCaster.Add panics, and that is the
// only case in which broken closes.
func TestPromptKernelQ5NoWaitAfterThePings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ps := NewChanPubSub(make(chan int))
		ps.Add(2) // S, T
		sent := make(chan int, 1)
		go func() { sent <- ps.Send(7) }()
		synctest.Wait()
		<-ps.C() // S receives
		tdone := make(chan struct{})
		go func() { <-ps.C(); ps.Wait(); close(tdone) }() // T keeps the contract
		<-tdone
		synctest.Wait() // the Send has one pong left to wait for

		if got := ps.Add(-1); got != 1 { // S unsubscribes without Wait
			t.Fatalf("Add(-1) = %d, want 1", got)
		}
		synctest.Wait()
		select {
		case n := <-sent:
			t.Fatalf("Send returned %d; it is owed a pong nobody will give", n)
		case <-ps.broken:
			t.Fatal("broken closed")
		default:
		}
		ps.Wait() // let the test end
		if n := <-sent; n != 2 {
			t.Errorf("Send = %d, want 2", n)
		}
	})
}
