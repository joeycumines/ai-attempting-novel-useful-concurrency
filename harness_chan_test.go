package bigbuff

import "context"

// Adapters for the channel pubsub, relay, latest-value and epoch-tape families.

// 1. Baseline ChanPubSub
type chanPubSubAdapter struct {
	ch chan int
	ps *ChanPubSub[chan int, int]
}

func newChanPubSubAdapter() Broadcaster {
	ch := make(chan int)
	return &chanPubSubAdapter{ch: ch, ps: NewChanPubSub(ch)}
}

func (a *chanPubSubAdapter) Name() string        { return "ChanPubSub (Baseline)" }
func (a *chanPubSubAdapter) Publish(val int) int { return a.ps.Send(val) }
func (a *chanPubSubAdapter) IsCoalescing() bool  { return false }
func (a *chanPubSubAdapter) Close()              { close(a.ch) }
func (a *chanPubSubAdapter) Subscribe() BroadcastSubscriber {
	a.ps.Subscribe()
	return &chanPubSubSub{a: a}
}

type chanPubSubSub struct {
	a      *chanPubSubAdapter
	closed bool
}

func (s *chanPubSubSub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case val, ok := <-s.a.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	}
}

func (s *chanPubSubSub) TryReceive() (int, bool) {
	select {
	case val, ok := <-s.a.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	default:
		return 0, false
	}
}

func (s *chanPubSubSub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.a.ps.Unsubscribe()
	}
}

// 2. RingPubSub (18-chatgpt)
type ringPubSubAdapter struct {
	hub *RingPubSub[int]
}

func newRingPubSubAdapter() Broadcaster {
	return &ringPubSubAdapter{hub: NewRingPubSub[int](64)}
}

func (a *ringPubSubAdapter) Name() string { return "RingPubSub (18-ChatGPT)" }
func (a *ringPubSubAdapter) Publish(val int) int {
	n, _ := a.hub.Send(context.Background(), val)
	return n
}
func (a *ringPubSubAdapter) IsCoalescing() bool { return false }
func (a *ringPubSubAdapter) Close()             { a.hub.Close() }
func (a *ringPubSubAdapter) Subscribe() BroadcastSubscriber {
	sub := a.hub.Subscribe()
	return &ringPubSubSub{sub: &sub}
}

type ringPubSubSub struct {
	sub *RingSubscription[int]
}

func (s *ringPubSubSub) Receive(ctx context.Context) (int, bool) {
	v, err := s.sub.Recv(ctx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *ringPubSubSub) TryReceive() (int, bool) {
	v, ok := s.sub.TryRecv()
	return v, ok
}
func (s *ringPubSubSub) Unsubscribe() { s.sub.Close() }

// 3. PhasePubSub (19-chatgpt)
type phasePubSubAdapter struct {
	bus *PhasePubSub[int]
}

func newPhasePubSubAdapter() Broadcaster {
	return &phasePubSubAdapter{bus: NewPhasePubSub[int]()}
}

func (a *phasePubSubAdapter) Name() string        { return "PhasePubSub (19-ChatGPT)" }
func (a *phasePubSubAdapter) Publish(val int) int { return a.bus.Send(val) }
func (a *phasePubSubAdapter) IsCoalescing() bool  { return false }
func (a *phasePubSubAdapter) Close()              {}
func (a *phasePubSubAdapter) Subscribe() BroadcastSubscriber {
	sub := a.bus.Subscribe()
	return &phasePubSubSub{sub: sub}
}

type phasePubSubSub struct {
	sub PhaseSubscription[int]
}

func (s *phasePubSubSub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.C():
		return s.sub.Read(), true
	}
}
func (s *phasePubSubSub) TryReceive() (int, bool) {
	select {
	case <-s.sub.C():
		return s.sub.Read(), true
	default:
		return 0, false
	}
}
func (s *phasePubSubSub) Unsubscribe() { s.sub.Unsubscribe() }

// 4. RelayPubSub (19-chatgpt)
type relayPubSubAdapter struct {
	bus *RelayPubSub[int]
}

func newRelayPubSubAdapter() Broadcaster {
	return &relayPubSubAdapter{bus: NewRelayPubSub[int](8)}
}

func (a *relayPubSubAdapter) Name() string        { return "RelayPubSub (19-ChatGPT)" }
func (a *relayPubSubAdapter) Publish(val int) int { return a.bus.Send(val) }
func (a *relayPubSubAdapter) IsCoalescing() bool  { return false }
func (a *relayPubSubAdapter) Close()              {}
func (a *relayPubSubAdapter) Subscribe() BroadcastSubscriber {
	sub := a.bus.Subscribe()
	return &relayPubSubSub{sub: sub}
}

type relayPubSubSub struct {
	sub RelayPubSubSubscription[int]
}

func (s *relayPubSubSub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.C():
		return s.sub.Read(), true
	}
}
func (s *relayPubSubSub) TryReceive() (int, bool) {
	select {
	case <-s.sub.C():
		return s.sub.Read(), true
	default:
		return 0, false
	}
}
func (s *relayPubSubSub) Unsubscribe() { s.sub.Unsubscribe() }

// 5. ChanRelay (20-arena-agent)
type chanRelayAdapter struct {
	relay *ChanRelay[int]
}

func newChanRelayAdapter() Broadcaster {
	return &chanRelayAdapter{relay: NewChanRelay[int]()}
}

func (a *chanRelayAdapter) Name() string { return "ChanRelay (20-Arena)" }
func (a *chanRelayAdapter) Publish(val int) int {
	return a.relay.Send(val)
}
func (a *chanRelayAdapter) IsCoalescing() bool { return false }
func (a *chanRelayAdapter) Close()             { a.relay.Close() }
func (a *chanRelayAdapter) Subscribe() BroadcastSubscriber {
	sub := a.relay.Subscribe()
	return &chanRelaySub{sub: sub}
}

type chanRelaySub struct {
	sub *RelaySubscription[int]
}

func (s *chanRelaySub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Done():
		return 0, false
	case v := <-s.sub.C():
		return v, true
	}
}
func (s *chanRelaySub) TryReceive() (int, bool) {
	val, ok := s.sub.TryNext()
	return val, ok
}
func (s *chanRelaySub) Unsubscribe() { s.sub.Close() }

// 6. ChanLatest (03-chatgpt)
type chanLatestAdapter struct {
	latest *ChanLatest[int]
}

func newChanLatestAdapter() Broadcaster {
	return &chanLatestAdapter{latest: NewChanLatest[int]()}
}

func (a *chanLatestAdapter) Name() string        { return "ChanLatest (03-ChatGPT)" }
func (a *chanLatestAdapter) Publish(val int) int { return a.latest.Send(val) }
func (a *chanLatestAdapter) IsCoalescing() bool  { return true }
func (a *chanLatestAdapter) Close()              { a.latest.Close() }
func (a *chanLatestAdapter) Subscribe() BroadcastSubscriber {
	sub := a.latest.Subscribe()
	return &chanLatestSub{sub: sub}
}

type chanLatestSub struct {
	sub *ChanLatestSubscription[int]
}

func (s *chanLatestSub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	}
}
func (s *chanLatestSub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.sub.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *chanLatestSub) Unsubscribe() { s.sub.Unsubscribe() }

// 7. EpochTape (10-gemini)
type epochTapeAdapter struct {
	tape *EpochTape[int]
}

func newEpochTapeAdapter() Broadcaster {
	return &epochTapeAdapter{tape: NewEpochTape[int](64)}
}

func (a *epochTapeAdapter) Name() string { return "EpochTape (10-Gemini)" }
func (a *epochTapeAdapter) Publish(val int) int {
	a.tape.Publish(val)
	return 1
}
func (a *epochTapeAdapter) IsCoalescing() bool { return false }
func (a *epochTapeAdapter) Close()             { a.tape.Close() }
func (a *epochTapeAdapter) Subscribe() BroadcastSubscriber {
	sub := a.tape.Subscribe()
	return &epochTapeSub{sub: sub}
}

type epochTapeSub struct {
	sub EpochSub[int]
}

func (s *epochTapeSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		v, _, closed, ok := s.sub.TryNext()
		if ok {
			return v, true
		}
		if closed {
			return 0, false
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *epochTapeSub) TryReceive() (int, bool) {
	v, _, _, ok := s.sub.TryNext()
	return v, ok
}
func (s *epochTapeSub) Unsubscribe() {}
