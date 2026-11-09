package bigbuff

// Caster facets: rendezvous casters and the withdrawal scenario.

import (
	"testing"
)

type casterLike struct {
	send       func(int) int
	add        func(int) int
	c          chan int
	close      func()
	ack        func()
	skipChurn  bool
	rearmEach  bool
	singleOnly bool
}

func casterLikes() map[string]func() casterLike {
	m := map[string]func() casterLike{}
	for name, mk := range casterImpls() {
		mk := mk
		name := name
		m[name] = func() casterLike {
			a := mk()
			return casterLike{a.send, a.add, a.c, a.close, a.ack, a.skipChurn, a.rearmEach, a.singleOnly}
		}
	}
	m["BaselineRendezvousCaster"] = func() casterLike {
		x := NewBaselineRendezvousCaster[int]()
		return casterLike{x.Send, x.Add, x.C, nil, nil, false, true, false}
	}
	return m
}

func TestScenarioCaster(t *testing.T) {
	for _, a := range casterAttachments {
		if a.ExposureLimited {
			t.Run(a.Impl+"/exposure-limited", func(t *testing.T) {
				mk := casterLikes()[a.Impl]
				l := mk()
				if l.close != nil {
					defer l.close()
				}
				runSingleAckCasterFacet(t, casterAPI{l.send, l.add, l.c, l.close, l.ack, l.skipChurn, l.rearmEach, l.singleOnly})
			})
			continue
		}
		t.Run(a.Impl+"/steady", func(t *testing.T) {
			mk, ok := casterLikes()[a.Impl]
			if !ok {
				t.Fatalf("no caster factory %q", a.Impl)
			}
			runCasterStress(t, func() casterAPI {
				l := mk()
				return casterAPI{l.send, l.add, l.c, l.close, l.ack, l.skipChurn, l.rearmEach, l.singleOnly}
			}, 16, 1000, false)
		})
		t.Run(a.Impl+"/churn", func(t *testing.T) {
			mk := casterLikes()[a.Impl]
			runCasterStress(t, func() casterAPI {
				l := mk()
				return casterAPI{l.send, l.add, l.c, l.close, l.ack, l.skipChurn, l.rearmEach, l.singleOnly}
			}, 16, 1000, true)
		})
	}
}
