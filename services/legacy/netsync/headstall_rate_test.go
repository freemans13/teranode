// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for F2 (never execute a peer that is still delivering): the residual
// head-of-line disconnect that fires once the F1 re-fetch race has run out of
// road is gated on the head peer's measured association throughput. On mainnet
// IBD that disconnect fires against peers that are merely SLOW — a fat frontier
// block genuinely takes longer than HeadStallDisconnectTimeout to transfer — and
// dropping the peer mid-transfer throws away the partial download and restarts it
// on someone else, which is strictly slower. svnode only drops a sole blocker
// that is ALSO under a byte-rate floor. The gate keeps the PEER and nothing
// more: it never rewinds the stall clock, because that would put the block back
// inside the F1 race window and re-race it to a fresh peer forever. These tests
// pin that behaviour, including the rollback lever (BlockStallMinRate=0) that
// makes the path byte-identical to pre-F2.
//
// F1 (race-don't-kill) is covered separately by headstall_race_test.go; this file
// only ever exercises the post-race-window path.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sampleState says what must have happened to the peer's stored F2 byte sample.
type sampleState int

const (
	// sampleRefreshed: the gate took a new measurement this tick.
	sampleRefreshed sampleState = iota
	// sampleUntouched: the gate deliberately left the stored baseline in place —
	// either because it short-circuited before sampling (rate gate disabled) or
	// because resampling would restart the measurement window on every tick and
	// it would never mature.
	sampleUntouched
)

// headStallRateCase describes one trip through the post-race-window disconnect
// path. Each case controls two things: the configured rate floor and the byte
// sample the peer carries into the tick. Both are expressed as functions of the
// peer's live association read-byte total so a case can pin the floor exactly at
// the rate it is about to manufacture, rather than hard-coding a number that
// depends on how many handshake bytes the test peer happened to read.
type headStallRateCase struct {
	name string
	// minRate is the legacy_blockStallMinRate floor to configure.
	minRate func(current uint64) int64
	// seedSample returns the baseline byte total and the wall clock it was taken
	// at. A zero prevAt means "this peer has never been sampled".
	prevBytes func(current uint64) uint64
	prevAge   time.Duration // 0 => never sampled
	// wantDisconnect is whether the head peer must be dropped this tick.
	wantDisconnect bool
	wantSample     sampleState
	reason         string
}

// TestCheckHeadStall_ThroughputGate walks the residual disconnect decision across
// every branch of the F2 rate gate.
//
// The measurement is manufactured honestly rather than mocked: the gate reads the
// peer's real AssociationReadBytes() at call time, and the test seeds only the
// stored baseline (bytes + timestamp) and passes checkHeadStall an explicit `now`.
// The delta and the elapsed window are therefore both exact, and any stray bytes
// the test peer reads between arrange and act can only push the measured rate UP,
// which never flips a case's expectation (the pass cases are already passing and
// the fail cases sit four orders of magnitude below their floor).
func TestCheckHeadStall_ThroughputGate(t *testing.T) {
	const (
		headHeight       = 100
		disconnectWindow = 30 * time.Second
		// A floor no trickle can reach, for the cases that must end in a drop.
		unreachableFloor = 100 * 1024
	)

	cases := []headStallRateCase{
		{
			name:           "delivering well above the floor is never disconnected",
			minRate:        func(current uint64) int64 { return int64(current / 2) },
			prevBytes:      func(uint64) uint64 { return 0 },
			prevAge:        time.Second,
			wantDisconnect: false,
			wantSample:     sampleRefreshed,
			reason:         "a peer moving bytes faster than the floor is delivering, not stalled",
		},
		{
			name:           "delivering exactly at the floor is never disconnected",
			minRate:        func(current uint64) int64 { return int64(current) },
			prevBytes:      func(uint64) uint64 { return 0 },
			prevAge:        time.Second,
			wantDisconnect: false,
			wantSample:     sampleRefreshed,
			reason:         "the floor is inclusive: at the floor is still delivering",
		},
		{
			name:           "below the floor for the full window is still disconnected",
			minRate:        func(uint64) int64 { return unreachableFloor },
			prevBytes:      func(current uint64) uint64 { return current },
			prevAge:        10 * time.Second,
			wantDisconnect: true,
			wantSample:     sampleRefreshed,
			reason:         "patience, not disabling: a sole blocker that moved no bytes all window is dead",
		},
		{
			name:           "rate gate disabled reproduces pre-F2 behaviour",
			minRate:        func(uint64) int64 { return 0 },
			prevBytes:      func(uint64) uint64 { return 0 },
			prevAge:        time.Second, // a baseline that WOULD have suppressed the drop
			wantDisconnect: true,
			wantSample:     sampleUntouched,
			reason:         "the rollback lever must short-circuit before any sample is taken",
		},
		{
			name:           "an unmeasured gate never vetoes the disconnect",
			minRate:        func(uint64) int64 { return unreachableFloor },
			prevBytes:      func(uint64) uint64 { return 0 },
			prevAge:        0, // never sampled
			wantDisconnect: true,
			wantSample:     sampleRefreshed,
			reason:         "with no baseline there is no evidence of delivery, and a gate that measured nothing must not override the backstop (it would also veto the frontier carve-out)",
		},
		{
			name:           "stale baseline is discarded like a first sample",
			minRate:        func(uint64) int64 { return unreachableFloor },
			prevBytes:      func(uint64) uint64 { return 0 },
			prevAge:        headStallRateSampleMaxAge + time.Minute,
			wantDisconnect: true,
			wantSample:     sampleRefreshed,
			reason:         "averaging over minutes of an earlier episode is not a measurement of now, so it cannot justify a veto either",
		},
		{
			name:           "maturing baseline waits and is not resampled",
			minRate:        func(uint64) int64 { return unreachableFloor },
			prevBytes:      func(current uint64) uint64 { return current },
			prevAge:        headStallRateWindow / 2,
			wantDisconnect: false,
			wantSample:     sampleUntouched,
			reason:         "resampling inside the window would restart it every tick and it would never mature",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sm, headPeer, otherPeer, head := buildHeadStallManager(t, headHeight)
			sm.settings.Legacy.HeadStallDisconnectTimeout = disconnectWindow

			// Frontier head (head == cached+1) so the self-backpressure suppression is
			// carved out and the stall logic is actually reached.
			sm.cachedBlockAssemblyHeight.Store(headHeight - 1)
			sm.baHeightPolled.Store(true)

			now := time.Now()

			// The F1 race window has already expired for THIS head block, so the tick
			// lands on the residual-disconnect path where the F2 gate lives.
			stalledSince := now.Add(-(disconnectWindow + time.Second))
			sm.headStallHash = head
			sm.headStallSince = stalledSince

			peerState, ok := sm.peerStates.Get(headPeer)
			require.True(t, ok, "precondition: the head peer must be in the peer-state map")

			current := headPeer.AssociationReadBytes()
			require.Greater(t, current, uint64(1),
				"precondition: the connected test peer must have read handshake bytes, otherwise no rate can be manufactured")

			sm.settings.Legacy.BlockStallMinRate = tc.minRate(current)

			seededBytes := tc.prevBytes(current)
			peerState.lastAssocReadBytes.Store(seededBytes)

			var seededAt int64
			if tc.prevAge > 0 {
				seededAt = now.Add(-tc.prevAge).UnixNano()
			}

			peerState.lastAssocSampleAt.Store(seededAt)

			sm.checkHeadStall(now)

			if tc.wantDisconnect {
				require.Eventually(t, func() bool { return !headPeer.Connected() }, 2*time.Second, 5*time.Millisecond,
					"head peer must be disconnected: %s", tc.reason)
			} else {
				require.Never(t, func() bool { return !headPeer.Connected() }, 300*time.Millisecond, 20*time.Millisecond,
					"head peer must be kept: %s", tc.reason)
			}

			require.True(t, otherPeer.Connected(),
				"the non-head peer is never in scope for the head-of-line disconnect")

			// The gate decides whether to KEEP the peer and nothing else. It must
			// never rewind the cross-race stall clock, on any branch: rewinding it
			// puts the head block back inside the F1 race window, and because the
			// gate measures association-wide throughput rather than progress on the
			// head block itself, a peer streaming its OTHER assignments passes the
			// floor forever — so the block would be re-raced to a fresh peer every
			// BlockStallTimeout indefinitely, a permanent duplicate-fetch loop.
			if !tc.wantDisconnect {
				require.Equal(t, stalledSince, sm.headStallSince,
					"suppressing the disconnect must not put the head block back inside the race window: %s", tc.reason)
			}

			switch tc.wantSample {
			case sampleRefreshed:
				require.Equal(t, now.UnixNano(), peerState.lastAssocSampleAt.Load(),
					"the gate must record the sample it just took")
				require.Equal(t, current, peerState.lastAssocReadBytes.Load(),
					"the recorded byte total must be the one the gate measured")
			case sampleUntouched:
				require.Equal(t, seededAt, peerState.lastAssocSampleAt.Load(),
					"the stored baseline must be left in place: %s", tc.reason)
				require.Equal(t, seededBytes, peerState.lastAssocReadBytes.Load(),
					"the stored baseline must be left in place: %s", tc.reason)
			}
		})
	}
}
