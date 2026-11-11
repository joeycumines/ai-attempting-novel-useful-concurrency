package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

// FuzzUnifiedBroadcaster tests broadcasters against randomized operational
// streams (subscribes, publishes, receives, unsubscribes) to assert safety.
func FuzzUnifiedBroadcaster(f *testing.F) {
	// Seed corpora
	f.Add([]byte{0, 1, 2, 3, 4})
	f.Add([]byte{1, 1, 1, 0, 2, 2, 4})
	f.Add([]byte{0, 0, 1, 2, 1, 2, 4, 4})
	f.Add([]byte{1, 0, 3, 1, 3, 4})

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 40 {
			return
		}

		// Representative architectures for this legacy op-stream fuzz, which
		// runs every listed implementation on every input. The broad targets
		// are FuzzScenarioBroadcast (one broadcast implementation per input,
		// chosen by the input from every scenario attachment) and
		// FuzzScenarioCasterScatterGather; this one keeps a small fixed
		// subset so seed-corpus runs in `go test` stay quick.
		factories := []func() Broadcaster{
			func() Broadcaster { return newRingPubSubAdapter() },
			func() Broadcaster { return newPhasePubSubAdapter() },
			func() Broadcaster { return newRelayPubSubAdapter() },
			func() Broadcaster { return newChanRelayAdapter() },
			func() Broadcaster { return newChanLatestAdapter() },
			func() Broadcaster { return newEpochTapeAdapter() },
		}

		for _, factory := range factories {
			b := factory()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)

			var subs []BroadcastSubscriber
			var wg sync.WaitGroup

			for _, op := range ops {
				switch op % 5 {
				case 0: // Subscribe
					if len(subs) < 8 {
						subs = append(subs, b.Subscribe())
					}
				case 1: // Publish
					wg.Add(1)
					go func() {
						defer wg.Done()
						b.Publish(42)
					}()
				case 2: // Receive
					if len(subs) > 0 {
						s := subs[len(subs)-1]
						s.Receive(ctx)
					}
				case 3: // TryReceive
					if len(subs) > 0 {
						s := subs[0]
						s.TryReceive()
					}
				case 4: // Unsubscribe
					if len(subs) > 0 {
						last := subs[len(subs)-1]
						subs = subs[:len(subs)-1]
						last.Unsubscribe()
					}
				}
			}

			cancel()
			for _, s := range subs {
				s.Unsubscribe()
			}
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s: publishers did not finish (deadlock)", b.Name())
			}
			b.Close()
		}
	})
}
