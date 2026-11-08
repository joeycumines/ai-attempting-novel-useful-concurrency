package bigbuff

import (
	"context"
	"sync/atomic"
)

type phantomCast44Adapter struct {
	x    *PhantomCast44Qwen[int]
	subs atomic.Int64
}

func newPhantomCast44Adapter() Broadcaster {
	return &phantomCast44Adapter{x: NewPhantomCast44Qwen[int](64)}
}
func (a *phantomCast44Adapter) Name() string       { return "PhantomCast44Qwen (44-Qwen)" }
func (a *phantomCast44Adapter) Publish(v int) int  { a.x.Send(v); return int(a.subs.Load()) }
func (a *phantomCast44Adapter) Close()             { a.x.Close() }
func (a *phantomCast44Adapter) IsCoalescing() bool { return true }
func (a *phantomCast44Adapter) Subscribe() BroadcastSubscriber {
	a.subs.Add(1)
	return &phantomCast44Sub{a: a, sub: a.x.Subscribe()}
}

type phantomCast44Sub struct {
	a      *phantomCast44Adapter
	sub    *PhantomSubscription44Qwen[int]
	closed atomic.Bool
}

func (s *phantomCast44Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed.Load() {
		return 0, false
	}
	return s.sub.Receive(ctx)
}
func (s *phantomCast44Sub) TryReceive() (int, bool) {
	if s.closed.Load() {
		return 0, false
	}
	return s.sub.Next()
}
func (s *phantomCast44Sub) Unsubscribe() {
	if s.closed.CompareAndSwap(false, true) {
		s.a.subs.Add(-1)
		s.sub.Unsubscribe()
	}
}
