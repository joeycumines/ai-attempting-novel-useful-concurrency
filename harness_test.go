package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Broadcaster is the unified adapter interface for standardizing across
// all broadcast implementations in the repository.
type Broadcaster interface {
	Name() string
	Publish(val int) int
	Subscribe() BroadcastSubscriber
	Close()
	IsCoalescing() bool
}

// BroadcastSubscriber is the unified adapter interface for receivers.
type BroadcastSubscriber interface {
	Receive(ctx context.Context) (val int, ok bool)
	TryReceive() (val int, ok bool)
	Unsubscribe()
}

// AllBroadcasters returns a slice of factory functions that create
// each broadcaster implementation configured for standard testing.
func AllBroadcasters() []func() Broadcaster {
	return []func() Broadcaster{
		func() Broadcaster { return newChanPubSubAdapter() },
		func() Broadcaster { return newChanPubSub57Adapter() },
		func() Broadcaster { return newRingPubSubAdapter() },
		func() Broadcaster { return newPhasePubSubAdapter() },
		func() Broadcaster { return newRelayPubSubAdapter() },
		func() Broadcaster { return newChanRelayAdapter() },
		func() Broadcaster { return newChanLatestAdapter() },
		func() Broadcaster { return newEpochTapeAdapter() },
		func() Broadcaster { return newGenCasterAdapter() },
		func() Broadcaster { return newChanFanoutAdapter() },
		func() Broadcaster { return newEpochCaster13Adapter() },
		func() Broadcaster { return newChanBeaconAdapter() },
		func() Broadcaster { return newChanFeedAdapter() },
		func() Broadcaster { return newChanRotorAdapter() },
		func() Broadcaster { return newChanGathererAdapter() },
		func() Broadcaster { return newChanHubAdapter() },
		func() Broadcaster { return newBaselineFanoutAdapter() },
		func() Broadcaster { return newBeaconAdapter() },
		func() Broadcaster { return newRingBeaconAdapter() },
		func() Broadcaster { return newEpochCasterGeminiAdapter() },
		func() Broadcaster { return newEpochPubSubAdapter() },
		func() Broadcaster { return newReliableBeaconAdapter() },
		pubsubBroadcaster("ChanPubSubV2_06meta"),
		pubsubBroadcaster("ChanPubSub09Gemini"),
		pubsubBroadcaster("ChanPubSub10Gemini"),
		pubsubBroadcaster("ChanPubSub14Xing"),
		pubsubBroadcaster("ChanPubSub16DeepSeek"),
		pubsubBroadcaster("ChanPubSub17Gemini"),
		func() Broadcaster { return newBaselineBarrierAdapter() },
		func() Broadcaster { return newBaselineLatestAdapter() },
		func() Broadcaster { return newBaselineBufferedAdapter() },
		func() Broadcaster { return newChanRelayTrySendAdapter() },
		func() Broadcaster { return newChanHubTrySendAdapter() },
		func() Broadcaster { return newRingPubSubTrySendAdapter() },
		func() Broadcaster { return newCarillon21Adapter() },
		func() Broadcaster { return newChanPubSubPacked22Adapter() },
		func() Broadcaster { return newSeqCaster23Adapter() },
		func() Broadcaster { return newSenseCaster25Adapter() },
		func() Broadcaster { return newChanRing26Adapter() },
		func() Broadcaster { return newMulticast27Adapter() },
		func() Broadcaster { return newBroadcast28Adapter() },
		func() Broadcaster { return newRelay29Adapter() },
		func() Broadcaster { return newArcadia30Adapter() },
		func() Broadcaster { return newVecast31Adapter() },
		func() Broadcaster { return newChanMulticast33Adapter() },
		func() Broadcaster { return newChanMultiCast34Adapter() },
		func() Broadcaster { return newShardCaster36Adapter() },
		func() Broadcaster { return newSelectCaster37Adapter() },
		func() Broadcaster { return newShardCast39Adapter() },
		func() Broadcaster { return newSlabCaster40Adapter() },
		func() Broadcaster { return newEpochCast41Adapter() },
		func() Broadcaster { return newBroadcast42Adapter() },
		func() Broadcaster { return newSlotPubSub43Adapter() },
		func() Broadcaster { return newPhantomCast44Adapter() },
		func() Broadcaster { return newChanHub46Adapter() },
		func() Broadcaster { return newChanPubSub47Adapter() },
		func() Broadcaster { return newChanRing47Adapter() },
		func() Broadcaster { return newWatchCaster48Adapter() },
		func() Broadcaster { return newChanReplay51Adapter() },
		func() Broadcaster { return newBaselineBoundedReplayAdapter() },
		func() Broadcaster { return newChanCascade52Adapter() },
		func() Broadcaster { return newReplayCast53Adapter() },
		func() Broadcaster { return newEpochBroadcast54Adapter() },
		func() Broadcaster { return newRingCaster55Adapter() },
		func() Broadcaster { return newFanout56Adapter() },
		func() Broadcaster { return newSelectBroadcaster49Adapter() },
		func() Broadcaster { return newQueuedFanout24Adapter() },
		func() Broadcaster { return newBaselineQueuedAdapter() },
		func() Broadcaster { return newSelectFanout38Adapter() },
		func() Broadcaster { return newFanout45Adapter() },
		func() Broadcaster { return newFanout45BufferedAdapter() },
		func() Broadcaster { return newRingFanout45Adapter() },
		func() Broadcaster { return newChanPubSub58Adapter() },
		func() Broadcaster { return newGenPubSub50Adapter() },
		func() Broadcaster { return newBroadcaster58Adapter() },
		func() Broadcaster { return newRingFanOut14Adapter() },
		func() Broadcaster { return newBaselineAsyncAdapter() },
	}
}

// --- Standardized Test Suite verifying all Broadcasters under identical conditions ---

func TestStandardHarness_SingleSubscriber(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			const messages = 5
			for i := 1; i <= messages; i++ {
				go func(val int) {
					b.Publish(val)
				}(i)

				val, ok := sub.Receive(ctx)
				if !ok || val != i {
					t.Fatalf("expected (%d, true), got (%d, %v)", i, val, ok)
				}
			}
		})
	}
}

func TestStandardHarness_NoSubscribers(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			// Publishing with zero subscribers must never block.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 1; i <= 100; i++ {
					b.Publish(i)
				}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("Publish blocked with no subscribers")
			}
		})
	}
}

func TestStandardHarness_MultiSubscriberFanout(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			const numSubs = 8
			const numMsgs = 10

			subs := make([]BroadcastSubscriber, numSubs)
			for i := range subs {
				subs[i] = b.Subscribe()
				defer subs[i].Unsubscribe()
			}

			var wg sync.WaitGroup
			wg.Add(numSubs)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			for i := 0; i < numSubs; i++ {
				go func(s BroadcastSubscriber) {
					defer wg.Done()
					if b.IsCoalescing() {
						for {
							val, ok := s.Receive(ctx)
							if !ok {
								return
							}
							if val == numMsgs {
								return
							}
						}
					} else {
						for m := 1; m <= numMsgs; m++ {
							val, ok := s.Receive(ctx)
							if !ok {
								t.Errorf("expected value, got ok=false")
								return
							}
							if val <= 0 {
								t.Errorf("invalid value %d", val)
								return
							}
						}
					}
				}(subs[i])
			}

			for m := 1; m <= numMsgs; m++ {
				b.Publish(m)
			}

			wg.Wait()
		})
	}
}

func TestStandardHarness_InOrderDelivery(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		if b.IsCoalescing() {
			b.Close()
			continue // skip coalescing broadcasters for strict sequence ordering
		}
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			const total = 20
			for i := 1; i <= total; i++ {
				go b.Publish(i)
				v, ok := sub.Receive(ctx)
				if !ok {
					t.Fatalf("receive failed at %d", i)
				}
				if v != i {
					t.Fatalf("out of order: expected %d, got %d", i, v)
				}
			}
		})
	}
}

func TestStandardHarness_ConcurrentChurn(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		if limited, ok := b.(interface{ SkipStandardConcurrentChurn() bool }); ok && limited.SkipStandardConcurrentChurn() {
			b.Close()
			continue
		}
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var stop atomic.Bool
			var wg, subWg sync.WaitGroup

			// Publisher runs until the bounded subscriber churn completes.
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := 1
				for !stop.Load() {
					b.Publish(m)
					m++
				}
			}()

			// Fixed churn cycles per subscriber; no readiness or duration sleep.
			for i := 0; i < 4; i++ {
				subWg.Add(1)
				go func() {
					defer subWg.Done()
					for j := 0; j < 20; j++ {
						func() {
							s := b.Subscribe()
							defer s.Unsubscribe()
							s.Receive(ctx)
						}()
					}
				}()
			}

			subWg.Wait()
			stop.Store(true)
			cancel()
			wg.Wait()
		})
	}
}

func TestStandardHarness_TryReceive(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			if limited, ok := b.(interface{ SkipStandardTryReceive() bool }); ok && limited.SkipStandardTryReceive() {
				t.Skip("exposure-limited TryReceive is covered by a dedicated safe facet")
			}
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			// Initially without publication, TryReceive returns false
			val, ok := sub.TryReceive()
			if ok {
				t.Fatalf("expected ok=false when empty, got val=%d", val)
			}

			// A barrier publication may need its receiver to participate in the
			// operation being polled. Completion, not elapsed time, is the bound.
			published := make(chan int, 1)
			go func() { published <- b.Publish(999) }()
			for {
				val, ok = sub.TryReceive()
				if ok {
					if val != 999 {
						t.Fatalf("expected 999, got %d", val)
					}
					break
				}
				// Publication completion is not delivery completion for async
				// adapters. If it finished first, one blocking Receive supplies
				// the deterministic completion boundary (and still checks value).
				select {
				case <-published:
					ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
					val, ok = sub.Receive(ctx)
					cancel()
					if !ok || val != 999 {
						t.Fatalf("post-publish Receive=(%d,%v), want (999,true)", val, ok)
					}
					return
				default:
					runtime.Gosched()
				}
			}
			select {
			case <-published:
			case <-time.After(scenarioGuard):
				t.Fatal("publication did not finish after receipt")
			}
		})
	}
}
