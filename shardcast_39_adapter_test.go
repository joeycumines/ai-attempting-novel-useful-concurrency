package bigbuff

import "context"

type shardCast39Adapter struct{ x *ShardCast39Mistral[int] }

func newShardCast39Adapter() Broadcaster {
	return &shardCast39Adapter{NewShardCast39Mistral[int](ShardCastOptions39Mistral{Shards: 8, Buffer: 16, Policy: ShardCastPolicy39Latest})}
}
func (a *shardCast39Adapter) Name() string       { return "ShardCast39Mistral/latest (39-Mistral)" }
func (a *shardCast39Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *shardCast39Adapter) Close()             { a.x.Close() }
func (a *shardCast39Adapter) IsCoalescing() bool { return true }
func (a *shardCast39Adapter) Subscribe() BroadcastSubscriber {
	return &shardCast39Sub{sub: a.x.Subscribe()}
}

type shardCast39Sub struct {
	sub *ShardCastSubscription39Mistral[int]
}

func (s *shardCast39Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	}
}
func (s *shardCast39Sub) TryReceive() (int, bool) {
	select {
	case <-s.sub.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *shardCast39Sub) Unsubscribe() { s.sub.Unsubscribe() }
