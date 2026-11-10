//go:build race
// +build race

package bigbuff

// snSet under -race uses a dedicated RWMutex over a single buffer slot.
// The public semantics are identical; only the seqlock is replaced so
// ThreadSanitizer can trace the happens-before edges.
func (p *Pulse61Deepseek[T]) snSet(v T) error {
	p.valMu.Lock()
	if p.closed.Load() {
		p.valMu.Unlock()
		return ErrPulseClosed61Deepseek
	}
	p.buf[0] = v
	p.ver.Add(1)
	p.valMu.Unlock()
	return nil
}

func (p *Pulse61Deepseek[T]) snapshot() (T, uint64, error) {
	p.valMu.RLock()
	v := p.buf[0]
	ver := p.ver.Load()
	p.valMu.RUnlock()
	if p.closed.Load() {
		return v, ver, ErrPulseClosed61Deepseek
	}
	return v, ver, nil
}
