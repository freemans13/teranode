// Package health provides progress tracking for liveness probes.
package health

import (
	"sync/atomic"
	"time"
)

// Heartbeat records the last time a loop proved it could still make progress,
// so a liveness probe can tell a wedged service from a healthy one (issue 1447).
//
// The distinction that makes this safe is WHAT gets recorded. A heartbeat must
// mean "this loop is still servicing its work channel", NOT "this loop did
// useful work" — a node with no blocks arriving is perfectly healthy, and on
// mainnet the gap between blocks is routinely tens of minutes. Loops therefore
// beat on a periodic tick inside the same select they service work on: if the
// loop is deadlocked, or wedged inside a handler, the tick cannot be served
// either and the heartbeat goes stale. That is exactly the silent freeze a
// liveness probe exists to catch, and it stays quiet when the node is merely
// idle.
//
// The zero value is usable and reports healthy until the first Beat, so a
// service that has not started its loop yet is never killed during startup.
type Heartbeat struct {
	lastBeat atomic.Int64 // unix nanos; 0 = never beaten
	now      func() time.Time
}

// New returns a Heartbeat that is already beating, so a service is not
// declared dead in the window between construction and its first tick.
func New() *Heartbeat {
	h := &Heartbeat{}
	h.Beat()

	return h
}

// Beat records that the loop is still making progress.
func (h *Heartbeat) Beat() {
	h.lastBeat.Store(h.clock().UnixNano())
}

// Age returns how long since the last beat. A Heartbeat that has never beaten
// reports zero age: nothing has claimed responsibility for it yet, so it must
// not be read as a stall.
func (h *Heartbeat) Age() time.Duration {
	last := h.lastBeat.Load()
	if last == 0 {
		return 0
	}

	age := h.clock().UnixNano() - last
	if age < 0 {
		// A backwards clock step must not be read as a stall.
		return 0
	}

	return time.Duration(age)
}

// Stalled reports whether the last beat is older than deadline. A deadline of
// zero or less disables the check, so an operator can turn the probe's
// restart behaviour off without redeploying different code.
func (h *Heartbeat) Stalled(deadline time.Duration) bool {
	if deadline <= 0 {
		return false
	}

	return h.Age() > deadline
}

// SetLastBeatForTest forces the last-beat time. Test-only seam so a caller can
// stand in for a loop that has stopped being serviced without sleeping.
func (h *Heartbeat) SetLastBeatForTest(t time.Time) {
	h.lastBeat.Store(t.UnixNano())
}

func (h *Heartbeat) clock() time.Time {
	if h.now != nil {
		return h.now()
	}

	return time.Now()
}
