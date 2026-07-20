package peer

import (
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// The tip constants these tests pin. They are written out literally rather than
// referenced from peer.go so that changing a constant fails a test instead of
// silently moving the assertion with it: the whole point of F3 is that with the
// IBD callback unset the deadlines are byte-identical to what mainnet ran
// before, and a test that reads the value it is checking cannot prove that.
const (
	tipBlockStallBudget   = 5 * time.Minute
	tipHeadersBudget      = 90 * time.Second
	tipMaxBlockDownload   = 30 * time.Minute
	tipPeerIdleTimeout    = 125 * time.Second
	deadlineArmTolerance  = 2 * time.Second
	expectedIdleResetSite = 4
)

// newBudgetPeer builds the minimum Peer the budget selectors read: they touch
// only cfg and settings, never the connection, so no handshake harness is
// needed. ibd and headerStalled are pointers so that nil can express "callback
// not wired at all", which is the case the rollback lever depends on and is
// behaviourally different from a callback returning false.
func newBudgetPeer(tSettings *settings.Settings, ibd, headerStalled *bool) *Peer {
	cfg := Config{}
	if ibd != nil {
		cfg.InitialBlockDownload = func() bool { return *ibd }
	}

	if headerStalled != nil {
		cfg.HeaderProgressStalled = func() bool { return *headerStalled }
	}

	return &Peer{cfg: cfg, settings: tSettings}
}

func boolPtr(b bool) *bool { return &b }

// TestNilIBDCallbackSelectsTipConstantsExactly is the rollback-lever test and
// the most important one in this file.
//
// Config.InitialBlockDownload left nil must make every deadline in peer.go
// identical to the pre-F3 code, so that dialling the feature back is a matter of
// not wiring the callback rather than reverting a diff. Production settings are
// loaded rather than hand-built so the assertion covers the real defaults
// (settings.conf plus the code fallbacks), not values the test itself invented.
func TestNilIBDCallbackSelectsTipConstantsExactly(t *testing.T) {
	tSettings := settings.NewSettings()

	// Guard against the test passing vacuously. If the IBD budgets were all zero
	// or all happened to equal the tip values, every assertion below would hold
	// no matter which branch ibdBudget took, and the test would prove nothing.
	require.NotEqual(t, tipBlockStallBudget, tSettings.Legacy.IBDBlockStallTimeout,
		"IBD block budget must differ from the tip value or this test cannot detect the wrong branch")
	require.NotEqual(t, tipMaxBlockDownload, tSettings.Legacy.IBDMaxBlockDownloadTime,
		"IBD download cap must differ from the tip value or this test cannot detect the wrong branch")
	require.NotEqual(t, tipPeerIdleTimeout, tSettings.Legacy.IBDPeerIdleTimeout,
		"IBD idle budget must differ from the tip value or this test cannot detect the wrong branch")

	p := newBudgetPeer(tSettings, nil, nil)

	require.False(t, p.catchingUp(), "a nil InitialBlockDownload callback must mean never catching up")

	require.Equal(t, tipBlockStallBudget, p.blockStallBudget())
	require.Equal(t, tipHeadersBudget, headersTipBudget())
	require.Equal(t, tipMaxBlockDownload, p.maxBlockDownloadTime())
	require.Equal(t, tipPeerIdleTimeout, p.idleTimeout())

	// The idle budget comes from settings rather than a constant, so also pin
	// that the production default is still the 125s sized against the 2-minute
	// ping interval — a settings.conf edit could otherwise move it unnoticed.
	require.Equal(t, tipPeerIdleTimeout, tSettings.Legacy.PeerIdleTimeout)

	// With no way to prove the header frontier is moving, no headers extension is
	// ever granted: at the tip a blown headers deadline kills the peer as before.
	require.False(t, p.shouldExtendHeadersDeadline(wire.CmdHeaders, time.Now().Add(-time.Minute), time.Now()))
}

// TestIBDBudgetsWidenWhenCatchingUp checks each budget takes its catch-up value
// once the callback reports true.
//
// Headers are the deliberate exception: the ARMED headers deadline stays at the
// tip 90s even during IBD, because the widened window is handed out one tip-sized
// slice at a time by shouldExtendHeadersDeadline and only while the frontier is
// provably advancing. Asserting that here stops a well-meaning future change from
// "fixing the inconsistency" by arming headers at the IBD value, which would
// reopen the warm-but-headerless freeze this design exists to prevent.
func TestIBDBudgetsWidenWhenCatchingUp(t *testing.T) {
	tSettings := settings.NewSettings()
	p := newBudgetPeer(tSettings, boolPtr(true), nil)

	require.True(t, p.catchingUp())

	require.Equal(t, tSettings.Legacy.IBDBlockStallTimeout, p.blockStallBudget())
	require.Equal(t, tSettings.Legacy.IBDMaxBlockDownloadTime, p.maxBlockDownloadTime())
	require.Equal(t, tSettings.Legacy.IBDPeerIdleTimeout, p.idleTimeout())

	// Each must actually be wider than the tip value — patience, never a
	// tightening, and never a disable.
	require.Greater(t, p.blockStallBudget(), tipBlockStallBudget)
	require.Greater(t, p.maxBlockDownloadTime(), tipMaxBlockDownload)
	require.Greater(t, p.idleTimeout(), tipPeerIdleTimeout)

	// Headers stay armed at the tip budget by design (see doc comment above).
	require.Equal(t, tipHeadersBudget, headersTipBudget())
}

// TestIBDBudgetFallsBackToTipWhenUnset covers the "never a route to disabling"
// rule in ibdBudget: an operator who blanks or mis-sets a catch-up duration must
// get the old tip deadline back, not a zero deadline that either disconnects
// instantly or (read the other way) never fires at all.
func TestIBDBudgetFallsBackToTipWhenUnset(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.Legacy.PeerIdleTimeout = tipPeerIdleTimeout
	// All IBD durations deliberately left at their zero value.

	p := newBudgetPeer(tSettings, boolPtr(true), boolPtr(false))

	require.True(t, p.catchingUp())
	require.Equal(t, tipBlockStallBudget, p.blockStallBudget())
	require.Equal(t, tipMaxBlockDownload, p.maxBlockDownloadTime())
	require.Equal(t, tipPeerIdleTimeout, p.idleTimeout())

	// A zero headers budget must not be read as "extend forever" either: with no
	// configured window there is no extension, even with a healthy frontier.
	require.False(t, p.shouldExtendHeadersDeadline(wire.CmdHeaders, time.Now().Add(-time.Second), time.Now()))
}

// TestArmAndRefreshBudgetsStayInLockstep pins the two places that independently
// compute a response's allowance — maybeAddDeadline when the request goes out,
// and responseStallBudget when deadlines are refreshed after a block fetch
// completes — to the same numbers, in both regimes.
//
// These are separate switch statements over the same commands. Widening one and
// not the other is the easy, silent bug: a refresh would move a deadline the
// arming logic never intended, and the peer would be judged on a budget nobody
// chose.
func TestArmAndRefreshBudgetsStayInLockstep(t *testing.T) {
	tSettings := settings.NewSettings()

	for _, tc := range []struct {
		name string
		ibd  *bool
	}{
		{name: "tip", ibd: nil},
		{name: "ibd", ibd: boolPtr(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newBudgetPeer(tSettings, tc.ibd, nil)
			blockBudget := p.blockStallBudget()

			// getheaders arms a CmdHeaders deadline; a refresh must restore the
			// same allowance rather than cutting it to the 30s base.
			pending := map[string]time.Time{}
			armedAt := time.Now()
			p.maybeAddDeadline(pending, wire.CmdGetHeaders)

			headersDeadline, ok := pending[wire.CmdHeaders]
			require.True(t, ok, "getheaders must arm a headers deadline")
			require.WithinDuration(t, armedAt.Add(responseStallBudget(wire.CmdHeaders, blockBudget)),
				headersDeadline, deadlineArmTolerance,
				"maybeAddDeadline and responseStallBudget disagree on the headers allowance")
			require.WithinDuration(t, armedAt.Add(tipHeadersBudget), headersDeadline, deadlineArmTolerance)

			// getdata arms the whole block-response group; every member must
			// refresh to the same block budget it was armed with.
			pending = map[string]time.Time{}
			armedAt = time.Now()
			p.maybeAddDeadline(pending, wire.CmdGetData)

			for _, cmd := range []string{wire.CmdBlock, wire.CmdMerkleBlock, wire.CmdTx, wire.CmdNotFound} {
				deadline, ok := pending[cmd]
				require.True(t, ok, "getdata must arm a %s deadline", cmd)
				require.WithinDuration(t, armedAt.Add(responseStallBudget(cmd, blockBudget)), deadline,
					deadlineArmTolerance, "arm/refresh budgets disagree for %s", cmd)
			}

			// getblocks arms an inv deadline on the block budget; responseStallBudget
			// maps CmdInv to the block budget for exactly this case.
			pending = map[string]time.Time{}
			armedAt = time.Now()
			p.maybeAddDeadline(pending, wire.CmdGetBlocks)

			invDeadline, ok := pending[wire.CmdInv]
			require.True(t, ok, "getblocks must arm an inv deadline")
			require.WithinDuration(t, armedAt.Add(responseStallBudget(wire.CmdInv, blockBudget)), invDeadline,
				deadlineArmTolerance, "arm/refresh budgets disagree for inv")
		})
	}
}

// TestClearBlockResponseGroupRefreshesOnTheSameBudget exercises the lockstep
// property through the real refresh path rather than through responseStallBudget
// directly: a headers reply queued behind a completing block must come out of the
// refresh with a full headers allowance, in both regimes.
func TestClearBlockResponseGroupRefreshesOnTheSameBudget(t *testing.T) {
	tSettings := settings.NewSettings()

	for _, tc := range []struct {
		name string
		ibd  *bool
	}{
		{name: "tip", ibd: nil},
		{name: "ibd", ibd: boolPtr(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newBudgetPeer(tSettings, tc.ibd, nil)

			now := time.Now()
			pending := map[string]time.Time{
				wire.CmdBlock: now.Add(time.Minute),
				// Head-of-line blocked behind the block and already past due.
				wire.CmdHeaders: now.Add(-time.Second),
			}

			require.True(t, clearBlockResponseGroup(pending, now, p.blockStallBudget(), false))
			require.NotContains(t, pending, wire.CmdBlock)

			require.WithinDuration(t, now.Add(tipHeadersBudget), pending[wire.CmdHeaders], deadlineArmTolerance,
				"a refreshed headers deadline must match the allowance maybeAddDeadline would arm")
		})
	}
}

// TestHeadersExtensionRefusedWhenFrontierStalled is the critical one.
//
// A sync peer can keep its transport warm with ping/pong, inv and addr traffic
// while never answering getheaders. Neither the widened peer deadlines nor
// netsync's silence detector (which resets on ANY association traffic) would
// notice, and because headers are single-sourced the frontier — and all block
// download queued behind it — would freeze for the whole widened window. So a
// pending headers response past the 90s tip budget must NOT be granted the
// 10-minute IBD extension when HeaderProgressStalled reports true: warm but
// headerless has to die on the tip clock.
func TestHeadersExtensionRefusedWhenFrontierStalled(t *testing.T) {
	tSettings := settings.NewSettings()
	require.Positive(t, tSettings.Legacy.IBDHeadersStallTimeout,
		"the IBD headers window must be configured or this test cannot distinguish the branches")

	now := time.Now()
	// Past the 90s tip budget, but still well inside the 10-minute IBD window —
	// so the frontier clock is the only thing that can decide the peer's fate.
	requestStart := now.Add(-3 * time.Minute)
	require.Greater(t, now.Sub(requestStart), tipHeadersBudget)
	require.Less(t, now.Sub(requestStart), tSettings.Legacy.IBDHeadersStallTimeout)

	stalled := newBudgetPeer(tSettings, boolPtr(true), boolPtr(true))
	require.False(t, stalled.shouldExtendHeadersDeadline(wire.CmdHeaders, requestStart, now),
		"a peer whose header frontier has stopped moving must be cut on the tip budget, not granted IBD patience")

	// The inverse: frontier still advancing, so the extension IS granted and the
	// only source of our headers is not executed mid-flight.
	advancing := newBudgetPeer(tSettings, boolPtr(true), boolPtr(false))
	require.True(t, advancing.shouldExtendHeadersDeadline(wire.CmdHeaders, requestStart, now))
}

// TestHeadersExtensionGuards covers the remaining ways the extension must be
// refused, so that "patience" can never become "forever".
func TestHeadersExtensionGuards(t *testing.T) {
	tSettings := settings.NewSettings()
	now := time.Now()
	inWindow := now.Add(-3 * time.Minute)

	t.Run("not catching up", func(t *testing.T) {
		// At the tip nothing is extended, however healthy the frontier looks.
		p := newBudgetPeer(tSettings, boolPtr(false), boolPtr(false))
		require.False(t, p.shouldExtendHeadersDeadline(wire.CmdHeaders, inWindow, now))
	})

	t.Run("nil liveness clock", func(t *testing.T) {
		// No way to prove the frontier is moving means no extra patience.
		p := newBudgetPeer(tSettings, boolPtr(true), nil)
		require.False(t, p.shouldExtendHeadersDeadline(wire.CmdHeaders, inWindow, now))
	})

	t.Run("window exhausted", func(t *testing.T) {
		// Beyond the IBD window the peer dies even while delivering headers, so
		// the widened budget is a bound rather than an open-ended reprieve.
		p := newBudgetPeer(tSettings, boolPtr(true), boolPtr(false))
		exhausted := now.Add(-tSettings.Legacy.IBDHeadersStallTimeout - time.Minute)
		require.False(t, p.shouldExtendHeadersDeadline(wire.CmdHeaders, exhausted, now))
	})

	t.Run("no outstanding request", func(t *testing.T) {
		// A zero request start means no getheaders is in flight; nothing to extend.
		p := newBudgetPeer(tSettings, boolPtr(true), boolPtr(false))
		require.False(t, p.shouldExtendHeadersDeadline(wire.CmdHeaders, time.Time{}, now))
	})

	t.Run("non headers command", func(t *testing.T) {
		// The headers reprieve must never leak onto block or inv deadlines.
		p := newBudgetPeer(tSettings, boolPtr(true), boolPtr(false))
		require.False(t, p.shouldExtendHeadersDeadline(wire.CmdBlock, inWindow, now))
		require.False(t, p.shouldExtendHeadersDeadline(wire.CmdInv, inWindow, now))
	})
}

// TestIdleTimeoutIsReEvaluatedNotLatched proves the idle budget is read from the
// live callback on every call rather than captured once.
//
// A budget latched at connection time would mean a node that reaches the tip
// mid-connection keeps 20-minute patience for dead peers, and — worse for IBD — a
// peer that connects at the tip and then falls behind never gets the widened
// window at all.
func TestIdleTimeoutIsReEvaluatedNotLatched(t *testing.T) {
	tSettings := settings.NewSettings()

	catchingUp := true
	p := newBudgetPeer(tSettings, &catchingUp, nil)

	require.Equal(t, tSettings.Legacy.IBDPeerIdleTimeout, p.idleTimeout())

	catchingUp = false
	require.Equal(t, tipPeerIdleTimeout, p.idleTimeout(),
		"reaching the tip mid-connection must tighten the idle budget on the next read")

	catchingUp = true
	require.Equal(t, tSettings.Legacy.IBDPeerIdleTimeout, p.idleTimeout(),
		"falling behind mid-connection must widen the idle budget again")
}

// TestIdleTimerUsesTheBudgetSelectorAtEverySite guards the arm/reset pairing in
// inHandler.
//
// Widening only the initial arm is a silent no-op: the timer is Stop()ed on every
// read and Reset() after every processed message, so the very first message from
// the peer would revert the timeout to whatever the reset site hardcoded. That
// failure leaves the widened budget looking configured while never actually
// applying, which is exactly the kind of thing that survives review and then
// costs days of IBD.
//
// This is a source-level assertion, not a behavioural one — see the honest
// limitation noted in the package report: driving inHandler's timer for real
// needs a completed version handshake over a pipe plus wall-clock waits on
// minute-scale budgets, which is not something to run in unit tests. It can still
// fail, which is the point: it catches a literal or a stale local sneaking into
// any arm or reset.
func TestIdleTimerUsesTheBudgetSelectorAtEverySite(t *testing.T) {
	src, err := os.ReadFile("peer.go")
	require.NoError(t, err)

	// The initial arm.
	require.Contains(t, string(src), "time.AfterFunc(p.idleTimeout(), func() {",
		"the idle timer must be armed from the budget selector")

	// Every reset. The two accepted forms are a direct call, and the local the
	// timer callback assigns from the selector on each firing.
	require.Contains(t, string(src), "idleTimeout := p.idleTimeout()",
		"the idle timer callback must re-read the budget on every firing")

	// Greedy up to the last ')' on the line, so a nested call like
	// p.idleTimeout() is captured whole rather than truncated at its own paren.
	resets := regexp.MustCompile(`idleTimer\.Reset\((.*)\)`).FindAllStringSubmatch(string(src), -1)
	require.Len(t, resets, expectedIdleResetSite,
		"idleTimer.Reset sites changed; re-check each one uses the budget selector and update this count")

	for _, m := range resets {
		require.Contains(t, []string{"idleTimeout", "p.idleTimeout()"}, m[1],
			"idleTimer.Reset(%s) does not use the IBD-aware budget selector; a widened arm that "+
				"reverts on the first received message is a silent no-op", m[1])
	}
}
