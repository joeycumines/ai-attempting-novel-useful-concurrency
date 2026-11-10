//go:build !race
// +build !race

package bigbuff

import "runtime"

// snSet acquires the seqlock via CAS, writes to the inactive buffer,
// publishes the active index, bumps the version, and releases the lock.
// Fully lock-free among publishers.
func (p *Pulse61Deepseek[T]) snSet(v T) error {
	var spins uint
	for {
		s := p.seq.Load()
		if s&1 != 0 {
			spins++
			if spins > 64 {
				runtime.Gosched()
				spins = 0
			}
			continue
		}
		if p.seq.CompareAndSwap(s, s+1) {
			break
		}
	}
	if p.closed.Load() {
		p.seq.Add(1) // release as even
		return ErrPulseClosed61Deepseek
	}
	idle := int32(1) - p.active.Load()
	p.buf[idle] = v
	p.active.Store(idle)
	p.ver.Add(1)
	p.seq.Add(1) // release: even
	return nil
}

// snapshot reads buf[active] under the seqlock. T is copied into a local
// so callers cannot observe a torn value across a concurrent write;
// the seq check makes the reader retry if a write started or completed
// during the copy.
func (p *Pulse61Deepseek[T]) snapshot() (T, uint64, error) {
	closed := p.closed.Load()
	var spins uint
	for {
		s1 := p.seq.Load()
		if s1&1 == 0 {
			idx := p.active.Load()
			v := p.buf[idx]
			ver := p.ver.Load()
			s2 := p.seq.Load()
			if s1 == s2 {
				if closed {
					return v, ver, ErrPulseClosed61Deepseek
				}
				return v, ver, nil
			}
		}
		spins++
		if spins > 64 {
			runtime.Gosched()
			spins = 0
		}
	}
}
