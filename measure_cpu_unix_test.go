//go:build unix

package bigbuff

import (
	"syscall"
	"time"
)

// processCPUTime returns the CPU time this process has consumed, user plus
// system. ok is false where the platform cannot say.
func processCPUTime() (d time.Duration, ok bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), true
}
