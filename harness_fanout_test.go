package bigbuff

import "context"

// Adapters for the caster, fan-out, feed, rotor and hub families.

// 8. GenCaster (06-meta)
type genCasterAdapter struct {
	caster *GenCaster[int]
}

func newGenCasterAdapter() Broadcaster {
	return &genCasterAdapter{caster: NewGenCaster[int]()}
}

func (a *genCasterAdapter) Name() string { return "GenCaster (06-Meta)" }
func (a *genCasterAdapter) Publish(val int) int {
	a.caster.Send(val)
	return 1
}
func (a *genCasterAdapter) IsCoalescing() bool { return false }
func (a *genCasterAdapter) Close()             {}
func (a *genCasterAdapter) Subscribe() BroadcastSubscriber {
	sub := a.caster.Subscribe()
	return &genCasterSub{sub: sub}
}

type genCasterSub struct {
	sub GenCasterSub[int]
}

func (s *genCasterSub) Receive(ctx context.Context) (int, bool) {
	ch := s.sub.C()
	if val, ok := s.sub.TryNext(); ok {
		return val, true
	}
	select {
	case <-ctx.Done():
		return 0, false
	case <-ch:
		return s.sub.Next()
	}
}
func (s *genCasterSub) TryReceive() (int, bool) {
	return s.sub.TryNext()
}
func (s *genCasterSub) Unsubscribe() {}

// 9. ChanFanout (12-openrouter)
type chanFanoutAdapter struct {
	fanout *ChanFanout[int]
}

func newChanFanoutAdapter() Broadcaster {
	return &chanFanoutAdapter{fanout: NewChanFanout[int](64)}
}

func (a *chanFanoutAdapter) Name() string        { return "ChanFanout (12-OpenRouter)" }
func (a *chanFanoutAdapter) Publish(val int) int { return a.fanout.Send(val) }
func (a *chanFanoutAdapter) IsCoalescing() bool  { return false }
func (a *chanFanoutAdapter) Close()              { a.fanout.Close() }
func (a *chanFanoutAdapter) Subscribe() BroadcastSubscriber {
	sub := a.fanout.Subscribe()
	return &chanFanoutSub{sub: sub}
}

type chanFanoutSub struct {
	sub *FanoutSub[int]
}

func (s *chanFanoutSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		if v, _, ok := s.sub.TryRecv(); ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *chanFanoutSub) TryReceive() (int, bool) {
	v, _, ok := s.sub.TryRecv()
	return v, ok
}
func (s *chanFanoutSub) Unsubscribe() { s.sub.Unsubscribe() }

// 10. EpochCaster13Crouton (13-openrouter)
type epochCaster13Adapter struct {
	caster *EpochCaster13Crouton[int]
}

func newEpochCaster13Adapter() Broadcaster {
	return &epochCaster13Adapter{caster: NewEpochCasterWithRingSize13Crouton[int](64)}
}

func (a *epochCaster13Adapter) Name() string        { return "EpochCaster (13-OpenRouter)" }
func (a *epochCaster13Adapter) Publish(val int) int { return int(a.caster.Send(val)) }
func (a *epochCaster13Adapter) IsCoalescing() bool  { return false }
func (a *epochCaster13Adapter) Close()              { a.caster.Close() }
func (a *epochCaster13Adapter) Subscribe() BroadcastSubscriber {
	sub := a.caster.Subscribe()
	return &epochCaster13Sub{sub: sub}
}

type epochCaster13Sub struct {
	sub *EpochSub13Crouton[int]
}

func (s *epochCaster13Sub) Receive(ctx context.Context) (int, bool) {
	v, err := s.sub.RecvContext(ctx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *epochCaster13Sub) TryReceive() (int, bool) {
	v, ok, _ := s.sub.TryRecv()
	return v, ok
}
func (s *epochCaster13Sub) Unsubscribe() { s.sub.Unsubscribe() }

// 11. ChanBeacon (07-qwen)
type chanBeaconAdapter struct {
	beacon *ChanBeacon[int]
}

func newChanBeaconAdapter() Broadcaster {
	return &chanBeaconAdapter{beacon: NewChanBeacon[int](64)}
}

func (a *chanBeaconAdapter) Name() string        { return "ChanBeacon (07-Qwen)" }
func (a *chanBeaconAdapter) Publish(val int) int { return a.beacon.Send(val) }
func (a *chanBeaconAdapter) IsCoalescing() bool  { return false }
func (a *chanBeaconAdapter) Close()              { a.beacon.Close() }
func (a *chanBeaconAdapter) Subscribe() BroadcastSubscriber {
	sub := a.beacon.Subscribe()
	return &chanBeaconSub{sub: sub}
}

type chanBeaconSub struct {
	sub *RelaySub[int]
}

func (s *chanBeaconSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		v, ok := s.sub.Consume()
		if ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *chanBeaconSub) TryReceive() (int, bool) {
	return s.sub.Consume()
}
func (s *chanBeaconSub) Unsubscribe() {}

// 12. ChanFeed (04-deepseek)
type chanFeedAdapter struct {
	feed *ChanFeed[int]
}

func newChanFeedAdapter() Broadcaster {
	return &chanFeedAdapter{feed: NewChanFeed[int](64, ChanFeedDropOldest)}
}

func (a *chanFeedAdapter) Name() string        { return "ChanFeed (04-DeepSeek)" }
func (a *chanFeedAdapter) Publish(val int) int { return a.feed.Send(val) }
func (a *chanFeedAdapter) IsCoalescing() bool  { return false }
func (a *chanFeedAdapter) Close()              { a.feed.Close() }
func (a *chanFeedAdapter) Subscribe() BroadcastSubscriber {
	sub := a.feed.Subscribe()
	return &chanFeedSub{sub: sub}
}

type chanFeedSub struct {
	sub *ChanFeedSub[int]
}

func (s *chanFeedSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		v, ok := s.sub.Next()
		if ok {
			return v, true
		}
		if s.sub.Closed() {
			return 0, false
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *chanFeedSub) TryReceive() (int, bool) {
	return s.sub.Next()
}
func (s *chanFeedSub) Unsubscribe() { s.sub.Unsubscribe() }

// 13. ChanRotor (17-gemini)
type chanRotorAdapter struct {
	rotor *ChanRotor[chan int, int]
}

func newChanRotorAdapter() Broadcaster {
	return &chanRotorAdapter{rotor: NewChanRotor[chan int, int](make(chan int), make(chan int))}
}

func (a *chanRotorAdapter) Name() string { return "ChanRotor (17-Gemini)" }
func (a *chanRotorAdapter) Publish(val int) int {
	a.rotor.Send(val)
	return 1
}
func (a *chanRotorAdapter) IsCoalescing() bool { return false }
func (a *chanRotorAdapter) Close()             {}
func (a *chanRotorAdapter) Subscribe() BroadcastSubscriber {
	sub := a.rotor.Subscribe()
	return &chanRotorSub{sub: sub}
}

type chanRotorSub struct {
	sub ChanRotorSub[chan int, int]
}

func (s *chanRotorSub) Receive(ctx context.Context) (int, bool) {
	ch := s.sub.C()
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-ch:
		if !ok {
			return 0, false
		}
		s.sub.Advance()
		return v, true
	}
}
func (s *chanRotorSub) TryReceive() (int, bool) {
	ch := s.sub.C()
	select {
	case v, ok := <-ch:
		if !ok {
			return 0, false
		}
		s.sub.Advance()
		return v, true
	default:
		return 0, false
	}
}
func (s *chanRotorSub) Unsubscribe() { s.sub.Unsubscribe() }

// 14. ChanGathererPubSub (05-qwen)
type chanGathererAdapter struct {
	ping chan int
	pong chan struct{}
	ps   *ChanGathererPubSub[chan int, int, chan struct{}]
}

func newChanGathererAdapter() Broadcaster {
	ping := make(chan int)
	pong := make(chan struct{})
	return &chanGathererAdapter{
		ping: ping,
		pong: pong,
		ps:   NewChanGathererPubSub(ping, pong),
	}
}

func (a *chanGathererAdapter) Name() string        { return "ChanGathererPubSub (05-Qwen)" }
func (a *chanGathererAdapter) Publish(val int) int { return a.ps.Send(val) }
func (a *chanGathererAdapter) IsCoalescing() bool  { return false }
func (a *chanGathererAdapter) Close()              { close(a.ping) }
func (a *chanGathererAdapter) Subscribe() BroadcastSubscriber {
	a.ps.Subscribe()
	return &chanGathererSub{a: a}
}

type chanGathererSub struct {
	a      *chanGathererAdapter
	closed bool
}

func (s *chanGathererSub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case val, ok := <-s.a.ping:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	}
}
func (s *chanGathererSub) TryReceive() (int, bool) {
	select {
	case val, ok := <-s.a.ping:
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
func (s *chanGathererSub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.a.ps.Unsubscribe()
	}
}

// 15. ChanHub (15-nemotron)
type chanHubAdapter struct {
	hub *ChanHub[int]
}

func newChanHubAdapter() Broadcaster {
	return &chanHubAdapter{hub: NewChanHub[int]()}
}

func (a *chanHubAdapter) Name() string        { return "ChanHub (15-Nemotron)" }
func (a *chanHubAdapter) Publish(val int) int { return a.hub.Send(val) }
func (a *chanHubAdapter) IsCoalescing() bool  { return false }
func (a *chanHubAdapter) Close()              {}
func (a *chanHubAdapter) Subscribe() BroadcastSubscriber {
	ch := make(chan int, 64)
	id := a.hub.Subscribe(ch)
	return &chanHubSub{hub: a.hub, ch: ch, id: id}
}

type chanHubSub struct {
	hub    *ChanHub[int]
	ch     chan int
	id     uint64
	closed bool
}

func (s *chanHubSub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case val, ok := <-s.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		return val, true
	}
}
func (s *chanHubSub) TryReceive() (int, bool) {
	select {
	case val, ok := <-s.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		return val, true
	default:
		return 0, false
	}
}
func (s *chanHubSub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.hub.Unsubscribe(s.id)
	}
}
