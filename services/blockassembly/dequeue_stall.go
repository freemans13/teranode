package blockassembly

import "time"

// dequeueStallThreshold is the hard-coded 30s bound beyond which a non-empty
// queue with no dequeue activity is unambiguously wrong in every deployment -
// see issue #1429. Deliberately not a setting: nobody can yet justify a
// different number, and during CATCHINGBLOCKS the queue should be near-empty
// anyway (subtree validation calls AddTXToBlockAssembly(false), which bypasses
// this queue).
const dequeueStallThreshold = 30 * time.Second

// dequeueStallWarnRepeat bounds how often the "consumer stalled" warning
// repeats while the condition persists. Chosen so an operator sees it promptly
// but is not paged every 5 seconds for the duration of an incident that may run
// for many minutes; 2 minutes still gives ~15+ log lines across the 35-minute
// incident on record (see dequeueDuringBlockMovement's docstring) without
// flooding logs.
const dequeueStallWarnRepeat = 2 * time.Minute

// dequeueStallEvent is what an observation tells the caller to log, if
// anything.
type dequeueStallEvent int

const (
	// dequeueStallNone means nothing to report on this tick.
	dequeueStallNone dequeueStallEvent = iota
	// dequeueStallBegan is the rising edge: log immediately, every time.
	dequeueStallBegan
	// dequeueStallContinues is the reduced-cadence repeat while stalled.
	dequeueStallContinues
	// dequeueStallEnded means the consumer genuinely resumed.
	dequeueStallEnded
)

// dequeueStallState is the state carried between observations. Its zero value
// is the correct starting state: not stalled, nothing warned yet.
type dequeueStallState struct {
	stalled bool
	// stalledSince is when the consumer stopped, not when we noticed. Set
	// once on the rising edge and never touched again until recovery, so one
	// incident is reported as one incident.
	stalledSince time.Time
	// lastWarn is when a warning was last emitted, for the repeat cadence.
	lastWarn time.Time
}

// observeDequeueStall folds one observation of the intake queue into the stall
// state and reports what, if anything, the operator should be told. It is pure:
// every input including the clock is a parameter, so the whole state machine is
// table-testable without waiting on real tick intervals.
//
// The entry and exit conditions are deliberately asymmetric, and that asymmetry
// is the point:
//
//   - A stall BEGINS only when the queue is non-empty and the consumer has not
//     touched it for dequeueStallThreshold. A stale consumer with an empty
//     queue is not the failure this signal exists for - issue #1429 is about
//     intake growing without bound.
//
//   - A stall ENDS only when staleness drops back to the threshold, regardless
//     of queue depth. Keying the exit on depth instead looks equivalent but is
//     not: reorgBlocks and moveForwardBlock drain the queue from inside the
//     very select branches that stop the consumer dequeuing, via
//     dequeueDuringBlockMovement, which does not stamp lastDequeueMillis. A
//     long handler therefore empties its own queue partway through and would
//     otherwise be reported as recovered while still parked, with the staleness
//     gauge still climbing - a false all-clear on the exact signal this
//     machinery exists to provide. The same applies at startup, where
//     BlockAssembler.Start calls DrainQueue before SubtreeProcessor.Start
//     re-seeds the timestamp.
//
// Staleness is the trustworthy half because the consumer stamps on every loop
// iteration, including iterations where the queue is empty and it dequeues
// nothing. Low staleness therefore means the consumer really is running.
//
// Returns the next state, the event to log, and - for dequeueStallEnded only -
// how long the incident lasted end to end.
func observeDequeueStall(state dequeueStallState, now time.Time, queueLength int64, staleness time.Duration) (dequeueStallState, dequeueStallEvent, time.Duration) {
	if !state.stalled {
		if queueLength > 0 && staleness > dequeueStallThreshold {
			return dequeueStallState{
				stalled: true,
				// Backdate to the last dequeue rather than to detection: the
				// stall began when the consumer stopped, which is at least
				// dequeueStallThreshold plus up to one tick ago. lastWarn
				// must stay at now, or a stall already older than
				// dequeueStallWarnRepeat would repeat on the very next tick.
				stalledSince: now.Add(-staleness),
				lastWarn:     now,
			}, dequeueStallBegan, 0
		}

		return state, dequeueStallNone, 0
	}

	if staleness <= dequeueStallThreshold {
		// End the incident when the consumer resumed, not when this tick
		// noticed - the same correction stalledSince applies to the start.
		// Recovery is only visible once staleness has fallen back to the
		// threshold, so by now the consumer has been running again for
		// staleness, which is up to dequeueStallThreshold plus a tick. Timing
		// to now would add that to every reported duration. The result cannot
		// go negative: staleness here is at most the threshold, and the
		// staleness that opened the incident exceeded it.
		return dequeueStallState{}, dequeueStallEnded, now.Add(-staleness).Sub(state.stalledSince)
	}

	if now.Sub(state.lastWarn) >= dequeueStallWarnRepeat {
		state.lastWarn = now
		return state, dequeueStallContinues, 0
	}

	return state, dequeueStallNone, 0
}
