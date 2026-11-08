package bigbuff

import (
	"context"
	"iter"
	"testing"
	"time"
)

type boundedReplayDriver struct {
	send      func(int) int
	subscribe func(context.Context) iter.Seq[int]
	replay    func() []int
	close     func()
}

func boundedReplayDrivers() map[string]func() boundedReplayDriver {
	return map[string]func() boundedReplayDriver{
		"ChanReplay51DeepSeek (51-DeepSeek)": func() boundedReplayDriver {
			x := NewChanReplay51DeepSeek[chan int, int](make(chan int), 3)
			return boundedReplayDriver{x.Send, x.SubscribeContextWithReplay, x.Replay, func() {}}
		},
		"BaselineBoundedReplay (baseline)": func() boundedReplayDriver {
			x := NewBaselineBoundedReplay[int](3)
			return boundedReplayDriver{x.Send, x.SubscribeContextWithReplay, x.Replay, x.Close}
		},
	}
}

func TestScenarioBoundedReplayCatchup(t *testing.T) {
	for _, attachment := range attachmentsFor("bounded-catchup-replay", broadcastAttachments) {
		factory, ok := boundedReplayDrivers()[attachment.Impl]
		if !ok {
			t.Fatalf("no replay driver for %q", attachment.Impl)
		}
		t.Run(attachment.Impl, func(t *testing.T) {
			driver := factory()
			defer driver.close()
			for value := 1; value <= 5; value++ {
				if sent := driver.send(value); sent != 0 {
					t.Fatalf("Send(%d) with no subscribers = %d, want 0", value, sent)
				}
			}
			before := driver.replay()
			if len(before) != 3 || before[0] != 3 || before[1] != 4 || before[2] != 5 {
				t.Fatalf("retained suffix = %v, want [3 4 5]", before)
			}
			before[0] = -1
			if after := driver.replay(); len(after) != 3 || after[0] != 3 {
				t.Fatalf("Replay returned aliased storage: %v", after)
			}

			got := make(chan int, 3)
			published := make(chan int, 1)
			done := make(chan struct{})
			seq := driver.subscribe(context.Background())
			go func() {
				defer close(done)
				seq(func(value int) bool {
					if value == 4 {
						go func() { published <- driver.send(6) }()
					}
					got <- value
					return value != 6
				})
			}()

			for _, want := range []int{3, 4, 5, 6} {
				select {
				case value := <-got:
					if value != want {
						t.Fatalf("catch-up/live sequence value = %d, want %d", value, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("timed out waiting for catch-up/live value %d", want)
				}
			}
			select {
			case sent := <-published:
				if sent != 1 {
					t.Fatalf("concurrent live Send delivered to %d subscribers, want 1", sent)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("live Send did not finish after receiver acknowledgement")
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("replay iterator did not stop after yield returned false")
			}
		})
	}
}

func BenchmarkScenarioBoundedReplaySnapshot(b *testing.B) {
	for impl, factory := range boundedReplayDrivers() {
		b.Run(impl, func(b *testing.B) {
			driver := factory()
			defer driver.close()
			const capacity = 3
			for i := 1; i <= capacity; i++ {
				driver.send(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				seq := driver.subscribe(context.Background())
				seq(func(int) bool {
					count++
					return count < capacity
				})
				if count != capacity {
					b.Fatalf("%s replayed %d values, want %d", impl, count, capacity)
				}
			}
		})
	}
}
