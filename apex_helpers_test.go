package bigbuff

import (
	"testing"
	"time"
)

// apexAwait fails the test if ch is not closed (or readable) within the
// scenario guard. The guard is a deadlock detector only: no assertion depends
// on how quickly ch becomes ready.
func apexAwait[T any](t testing.TB, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(scenarioGuard):
		t.Fatalf("%s: not complete within %v", what, scenarioGuard)
	}
}

// apexStillBlocked asserts that ch is not ready at the moment of the call.
// It never waits, so a correct implementation can never fail it; it catches
// an implementation that releases a blocked operation too early only when
// the release has already happened, which is why each use is paired with a
// positive assertion on the final state.
func apexStillBlocked[T any](t testing.TB, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s: completed early", what)
	default:
	}
}

// timeAfterGuard is the deadlock-detector timer used in select statements.
func timeAfterGuard() <-chan time.Time { return time.After(scenarioGuard) }
