package bigbuff

import (
	"sync/atomic"
)

// Notifier63Qwen is a zero-allocation, coalescing wakeup primitive.
// It eliminates thundering herds by using a single buffered channel to signal
// consumers, allowing them to batch-drain high-frequency updates. It has no
// Close: consumers stop by cancelling their context (see Subscribe).
type Notifier63Qwen struct {
	epoch atomic.Uint64
	ch    chan struct{}
}

func NewNotifier63Qwen() *Notifier63Qwen {
	return &Notifier63Qwen{
		ch: make(chan struct{}, 1),
	}
}

// Notify signals waiting consumers. Subsequent calls while the channel is full
// are discarded, achieving O(1) wakeup coalescing.
func (n *Notifier63Qwen) Notify() {
	n.epoch.Add(1)
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

func (n *Notifier63Qwen) WaitChan() <-chan struct{} {
	return n.ch
}
