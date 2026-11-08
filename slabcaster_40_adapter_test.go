package bigbuff

import "context"

type slabCaster40Adapter struct{ x *SlabCaster40Gemini[int] }

func newSlabCaster40Adapter() Broadcaster {
	return &slabCaster40Adapter{NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(16), WithSlabCaster40Policy(SlabPolicy40DropOldest))}
}
func (a *slabCaster40Adapter) Name() string       { return "SlabCaster40Gemini/drop-oldest (40-Gemini)" }
func (a *slabCaster40Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *slabCaster40Adapter) Close()             { _ = a.x.Close() }
func (a *slabCaster40Adapter) IsCoalescing() bool { return true }
func (a *slabCaster40Adapter) Subscribe() BroadcastSubscriber {
	return &slabCaster40Sub{sub: a.x.Subscribe()}
}
func (a *slabCaster40Adapter) SkipStandardConcurrentChurn() bool { return true }

type slabCaster40Sub struct {
	sub *SlabSubscription40Gemini[int]
}

func (s *slabCaster40Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	}
}
func (s *slabCaster40Sub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.sub.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *slabCaster40Sub) Unsubscribe() { s.sub.Close() }
