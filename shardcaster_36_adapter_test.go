package bigbuff

import "context"

type shardCaster36Adapter struct{ x *ShardCaster36Meta[int] }

func newShardCaster36Adapter() Broadcaster         { return &shardCaster36Adapter{NewShardCaster36Meta[int]()} }
func (a *shardCaster36Adapter) Name() string       { return "ShardCaster36Meta (36-MetaAI)" }
func (a *shardCaster36Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *shardCaster36Adapter) Close()             { a.x.TryClose() }
func (a *shardCaster36Adapter) IsCoalescing() bool { return false }
func (a *shardCaster36Adapter) Subscribe() BroadcastSubscriber {
	return &shardCaster36Sub{sub: a.x.Subscribe()}
}

type shardCaster36Sub struct{ sub *ShardSubscription36Meta[int] }

func (s *shardCaster36Sub) Receive(ctx context.Context) (int, bool) { return s.sub.Recv(ctx) }
func (s *shardCaster36Sub) TryReceive() (int, bool) {
	select {
	case <-s.sub.sub.done:
		return 0, false
	case v := <-s.sub.C():
		return v, true
	default:
		return 0, false
	}
}
func (s *shardCaster36Sub) Unsubscribe() { s.sub.Unsubscribe() }
