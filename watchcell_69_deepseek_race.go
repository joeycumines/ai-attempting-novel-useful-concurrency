//go:build race

package bigbuff

// storeValue publishes v under the race-build RWMutex. seq advances by 2 per
// publish so that seq/2 is the epoch and remains even (matching !race
// semantics).
func (x *WatchCell69Deepseek[V]) storeValue(v V) uint64 {
	x.valMu.Lock()
	x.val = v
	s := x.seq.Add(2)
	x.valMu.Unlock()
	return s / 2
}

func (x *WatchCell69Deepseek[V]) loadValue() (V, uint64) {
	x.valMu.RLock()
	v := x.val
	// Read seq while holding RLock so the (value, epoch) pair is consistent:
	// writers perform val write and seq advance under the same Lock.
	s := x.seq.Load()
	x.valMu.RUnlock()
	return v, s / 2
}
