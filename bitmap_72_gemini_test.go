package bigbuff

// The bitmap scanners are not wired into PhaseCaster72Gemini (the response
// left them unconnected); they are tested here as stand-alone helpers against
// the pure-Go reference. On architectures without the assembly these tests
// compare the reference with itself, which still pins its contract.

import (
	"math/rand"
	"sync/atomic"
	"testing"
)

func bitmap72Masks(vals []uint64) []atomic.Uint64 {
	m := make([]atomic.Uint64, len(vals))
	for i, v := range vals {
		m[i].Store(v)
	}
	return m
}

func TestBitmap72Gemini_ScanParity(t *testing.T) {
	rng := rand.New(rand.NewSource(72))
	for n := 0; n <= 67; n++ {
		for range 20 {
			vals := make([]uint64, n)
			for i := range vals {
				switch rng.Intn(4) {
				case 0:
					vals[i] = 0
				case 1:
					vals[i] = ^uint64(0)
				default:
					vals[i] = rng.Uint64()
				}
			}
			src := bitmap72Masks(vals)
			got, want := make([]uint64, n+1), make([]uint64, n+1)
			const canary = 0xfeedfacecafebeef
			got[n], want[n] = canary, canary
			gn := scanActiveBanks72Gemini(got, src)
			wn := scanActiveBanksRef72Gemini(want, src)
			if gn != wn {
				t.Fatalf("n=%d: population %d, reference %d", n, gn, wn)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("n=%d: dst[%d] = %#x, reference %#x", n, i, got[i], want[i])
				}
			}
		}
	}
}

func TestBitmap72Gemini_ScanShortDstPanics(t *testing.T) {
	src := bitmap72Masks([]uint64{1, 2, 3})
	defer func() {
		if recover() == nil {
			t.Fatal("scan into a destination shorter than the source did not panic")
		}
	}()
	scanActiveBanks72Gemini(make([]uint64, 2), src)
}

func TestBitmap72Gemini_FindParity(t *testing.T) {
	rng := rand.New(rand.NewSource(7272))
	if got := findNonFullBank72Gemini(nil, 3); got != -1 {
		t.Fatalf("empty slice: %d, want -1", got)
	}
	for n := 1; n <= 33; n++ {
		for range 40 {
			vals := make([]uint64, n)
			for i := range vals {
				vals[i] = ^uint64(0)
				if rng.Intn(3) == 0 {
					vals[i] &^= 1 << rng.Intn(64)
				}
			}
			masks := bitmap72Masks(vals)
			// Includes negative and out-of-range starts.
			for start := -n - 2; start <= 2*n+2; start++ {
				got := findNonFullBank72Gemini(masks, start)
				want := findNonFullBankRef72Gemini(masks, start)
				if got != want {
					t.Fatalf("n=%d start=%d masks=%x: %d, reference %d", n, start, vals, got, want)
				}
			}
		}
	}
}

func TestBitmap72Gemini_FindWrapsFromStart(t *testing.T) {
	full := ^uint64(0)
	masks := bitmap72Masks([]uint64{full &^ 1, full, full, full &^ 2, full})
	for start, want := range []int{0, 3, 3, 3, 0} {
		if got := findNonFullBank72Gemini(masks, start); got != want {
			t.Fatalf("start %d: %d, want %d", start, got, want)
		}
	}
	if got := findNonFullBank72Gemini(bitmap72Masks([]uint64{full, full}), 1); got != -1 {
		t.Fatalf("all banks full: %d, want -1", got)
	}
	cpuPause72Gemini() // must be callable; it has no observable effect
}

func FuzzBitmap72Gemini(f *testing.F) {
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 2, 3, 4, 5, 6, 7, 8}, 1)
	f.Add([]byte{}, 0)
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, -5)
	f.Fuzz(func(t *testing.T, raw []byte, start int) {
		if len(raw) > 1024 {
			raw = raw[:1024]
		}
		vals := make([]uint64, len(raw)/8)
		for i := range vals {
			for j := 0; j < 8; j++ {
				vals[i] |= uint64(raw[i*8+j]) << (8 * j)
			}
		}
		masks := bitmap72Masks(vals)
		got, want := make([]uint64, len(vals)), make([]uint64, len(vals))
		if g, w := scanActiveBanks72Gemini(got, masks), scanActiveBanksRef72Gemini(want, masks); g != w {
			t.Fatalf("population %d, reference %d", g, w)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("dst[%d] = %#x, reference %#x", i, got[i], want[i])
			}
		}
		if g, w := findNonFullBank72Gemini(masks, start), findNonFullBankRef72Gemini(masks, start); g != w {
			t.Fatalf("find from %d: %d, reference %d", start, g, w)
		}
	})
}

func BenchmarkBitmap72Gemini_Scan(b *testing.B) {
	for _, n := range []int{4, 16, 64} {
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = 0x5555555555555555
		}
		masks := bitmap72Masks(vals)
		dst := make([]uint64, n)
		b.Run("asm/banks="+itoa72(n), func(b *testing.B) {
			for b.Loop() {
				scanActiveBanks72Gemini(dst, masks)
			}
		})
		b.Run("reference/banks="+itoa72(n), func(b *testing.B) {
			for b.Loop() {
				scanActiveBanksRef72Gemini(dst, masks)
			}
		})
	}
}

func itoa72(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for ; n > 0; n /= 10 {
		i--
		buf[i] = byte('0' + n%10)
	}
	return string(buf[i:])
}
