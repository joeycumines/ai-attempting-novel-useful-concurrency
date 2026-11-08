package bigbuff

import "context"

type epochCast41Adapter struct{ x *EpochCast41Sonnet[int] }

func newEpochCast41Adapter() Broadcaster {
	return &epochCast41Adapter{NewEpochCast41Sonnet[int](64, EpochLossy41Sonnet)}
}
func (a *epochCast41Adapter) Name() string { return "EpochCast41Sonnet/lossy (41-Sonnet)" }
func (a *epochCast41Adapter) Publish(v int) int {
	n, _ := a.x.Publish(context.Background(), v)
	return n
}
func (a *epochCast41Adapter) Close()             { a.x.Close() }
func (a *epochCast41Adapter) IsCoalescing() bool { return true }
func (a *epochCast41Adapter) Subscribe() BroadcastSubscriber {
	return &epochCast41Sub{sub: a.x.Subscribe()}
}

type epochCast41Sub struct{ sub *EpochSub41Sonnet[int] }

func (s *epochCast41Sub) Receive(ctx context.Context) (int, bool) {
	for {
		v, _, st := s.sub.TryRecv()
		switch st {
		case EpochValue41Sonnet:
			return v, true
		case EpochLagged41Sonnet:
			continue
		case EpochClosed41Sonnet:
			return 0, false
		}
		ready := s.sub.Ready()
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *epochCast41Sub) TryReceive() (int, bool) {
	for {
		v, _, st := s.sub.TryRecv()
		switch st {
		case EpochValue41Sonnet:
			return v, true
		case EpochLagged41Sonnet:
			continue
		default:
			return 0, false
		}
	}
}
func (s *epochCast41Sub) Unsubscribe() { s.sub.Cancel() }
