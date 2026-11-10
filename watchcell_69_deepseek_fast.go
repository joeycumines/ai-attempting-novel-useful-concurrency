//go:build !race

package bigbuff

// storeValue publishes v and returns the new epoch. Seqlock: seq goes odd
// during the write, back to even on completion. Epoch = seq/2.
func (x *WatchCell69Deepseek[V]) storeValue(v V) uint64 {
	x.seq.Add(1) // odd — writing
	x.val = v
	s := x.seq.Add(1) // even — stable
	return s / 2
}

// loadValue returns a consistent snapshot of val and its epoch. Retries until
// the seqlock reads agree, so torn writes are never observed.
func (x *WatchCell69Deepseek[V]) loadValue() (V, uint64) {
	for {
		s1 := x.seq.Load()
		if s1&1 != 0 {
			continue // writer in flight
		}
		v := x.val
		s2 := x.seq.Load()
		if s1 == s2 {
			return v, s1 / 2
		}
	}
}
