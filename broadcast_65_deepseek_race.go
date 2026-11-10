//go:build race

package bigbuff

import "sync"

// slotBuffer65Deepseek under -race: RWMutex-guarded so ThreadSanitizer's
// happens-before analysis sees the required synchronisation. The seqlock
// atomics already provide correct ordering for real programs; the mutex is
// purely a race-detector appeasement. It serialises Publish against read()
// per slot, which the -race build tolerates (correctness > throughput).
type slotBuffer65Deepseek[V any] struct {
	mu sync.RWMutex
	v  V
}

func (b *slotBuffer65Deepseek[V]) load() V {
	b.mu.RLock()
	v := b.v
	b.mu.RUnlock()
	return v
}

func (b *slotBuffer65Deepseek[V]) store(v V) {
	b.mu.Lock()
	b.v = v
	b.mu.Unlock()
}
