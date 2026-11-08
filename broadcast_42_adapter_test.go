package bigbuff

import (
	"context"
	"sync/atomic"
)

type broadcast42Adapter struct {
	x    *Broadcast42Spark[int]
	subs atomic.Int64
}

func newBroadcast42Adapter() Broadcaster         { return &broadcast42Adapter{x: NewBroadcast42Spark[int](64)} }
func (a *broadcast42Adapter) Name() string       { return "Broadcast42Spark (42-MuseSpark)" }
func (a *broadcast42Adapter) Publish(v int) int  { a.x.Send(v); return int(a.subs.Load()) }
func (a *broadcast42Adapter) Close()             { a.x.Close() }
func (a *broadcast42Adapter) IsCoalescing() bool { return true }
func (a *broadcast42Adapter) Subscribe() BroadcastSubscriber {
	a.subs.Add(1)
	return &broadcast42Sub{a: a, r: a.x.NewReceiver()}
}

type broadcast42Sub struct {
	a      *broadcast42Adapter
	r      *Receiver42Spark[int]
	closed atomic.Bool
}

func (s *broadcast42Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed.Load() {
		return 0, false
	}
	v, _, e := s.r.RecvContext(ctx)
	return v, e == nil
}
func (s *broadcast42Sub) TryReceive() (int, bool) {
	if s.closed.Load() {
		return 0, false
	}
	v, _, ok := s.r.TryRecv()
	return v, ok
}
func (s *broadcast42Sub) Unsubscribe() {
	if s.closed.CompareAndSwap(false, true) {
		s.a.subs.Add(-1)
	}
}
