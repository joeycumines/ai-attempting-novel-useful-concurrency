package bigbuff

import (
	"math"
	"sync"
)

// Provenance: 48-grok-chat.md (Grok, response #48). The latest-value and
// generation model is preserved; the shared buffered signal was changed to a
// broadcast-by-close channel, and value/generation now share a lock-protected
// snapshot. Both changes are required for multiple observers and coherent reads.

type WatchCaster48Grok[V any] struct {
	mu     sync.RWMutex
	value  V
	gen    uint64
	notify chan struct{}
}

func NewWatchCaster48Grok[V any]() *WatchCaster48Grok[V] {
	return &WatchCaster48Grok[V]{notify: make(chan struct{})}
}

// Send publishes the latest value and returns its generation. Every Send
// closes the current notification channel, waking all observers; intermediate
// values may coalesce before an observer calls Load.
func (w *WatchCaster48Grok[V]) Send(value V) uint64 {
	w.checkFactory()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gen == math.MaxUint64 {
		panic("bigbuff: watchcaster48: generation overflow")
	}
	w.value = value
	w.gen++
	close(w.notify)
	w.notify = make(chan struct{})
	return w.gen
}

// Load returns a coherent value/generation snapshot. The lock prevents readers
// from observing a value paired with a generation from a different publication.
func (w *WatchCaster48Grok[V]) Load() (value V, generation uint64) {
	w.checkFactory()
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.value, w.gen
}

// Notify returns the current broadcast notification channel. It is closed on
// the next Send, waking every goroutine already waiting on that generation.
// Call Load after wake-up; a burst can advance the generation by several steps.
func (w *WatchCaster48Grok[V]) Notify() <-chan struct{} {
	w.checkFactory()
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.notify
}

func (w *WatchCaster48Grok[V]) Generation() uint64 {
	w.checkFactory()
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.gen
}

func (w *WatchCaster48Grok[V]) checkFactory() {
	if w == nil {
		panic("bigbuff: watchcaster48: must use constructor")
	}
	w.mu.RLock()
	ready := w.notify != nil
	w.mu.RUnlock()
	if !ready {
		panic("bigbuff: watchcaster48: must use constructor")
	}
}
