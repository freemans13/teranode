package peer

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// budgetPeer builds the minimum Peer needed to exercise blockDownloadBudget.
func budgetPeer(t *testing.T, interval time.Duration, catchingUp bool, peersDownloading int) *Peer {
	t.Helper()

	tSettings := settings.NewSettings()
	params := *tSettings.ChainCfgParams
	params.TargetTimePerBlock = interval

	p := &Peer{settings: tSettings, logger: ulogger.TestLogger{}}
	p.cfg = Config{
		ChainParams:             &params,
		CatchingUp:              func() bool { return catchingUp },
		PeersWithBlockDownloads: func() int { return peersDownloading },
	}

	return p
}

// TestBlockDownloadBudget mirrors svnode's calculation:
//
//	nPowTargetSpacing * (timeoutBase + timeoutPerPeer * nOtherPeers) / 100
//
// with base 100% at the tip and 600% while catching up, plus 50% for each OTHER
// peer we are pulling blocks from.
func TestBlockDownloadBudget(t *testing.T) {
	const interval = 10 * time.Minute // mainnet

	tests := []struct {
		name             string
		catchingUp       bool
		peersDownloading int
		want             time.Duration
	}{
		// 100% of one 10-minute interval.
		{"tip, sole downloader", false, 1, 10 * time.Minute},
		// 600% while catching up — the whole point of the change.
		{"catching up, sole downloader", true, 1, 60 * time.Minute},
		// Only OTHER peers count, so one downloader adds nothing.
		{"tip, one other peer", false, 2, 15 * time.Minute},
		// svnode's mainnet-shaped case: 600 + 50*7 = 950% of 10 minutes.
		{"catching up, eight peers", true, 8, 95 * time.Minute},
		// A peer count below one must never subtract from the ceiling.
		{"zero reported peers", true, 0, 60 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := budgetPeer(t, interval, tt.catchingUp, tt.peersDownloading)
			require.Equal(t, tt.want, p.blockDownloadBudget())
		})
	}
}

// TestBlockDownloadBudgetNeverZero guards the failure mode that matters most: a
// zero ceiling would disconnect every peer the instant a block was requested.
func TestBlockDownloadBudgetNeverZero(t *testing.T) {
	t.Run("zero block interval falls back", func(t *testing.T) {
		p := budgetPeer(t, 0, true, 1)
		require.Equal(t, MaxBlockDownloadTime, p.blockDownloadBudget())
	})

	t.Run("zero percentages fall back", func(t *testing.T) {
		p := budgetPeer(t, 10*time.Minute, true, 1)
		p.settings.Legacy.BlockDownloadTimeoutBasePercent = 0
		p.settings.Legacy.BlockDownloadTimeoutBaseIBDPercent = 0
		p.settings.Legacy.BlockDownloadTimeoutPerPeerPercent = 0
		require.Equal(t, MaxBlockDownloadTime, p.blockDownloadBudget())
	})

	// A percentage large enough to overflow the multiplication wraps the result
	// negative, which disconnects every peer just as surely as a zero would. The
	// guard has to run after the multiply, not only before it.
	//
	// 15372287% of ten minutes is 9223372200000000000ns, which is just past the
	// largest positive int64 (9223372036854775807) and so wraps.
	t.Run("percentage large enough to overflow falls back", func(t *testing.T) {
		p := budgetPeer(t, 10*time.Minute, true, 1)
		p.settings.Legacy.BlockDownloadTimeoutBaseIBDPercent = 15372287
		require.Equal(t, MaxBlockDownloadTime, p.blockDownloadBudget())
	})
}

// TestBlockDownloadBudgetUnwiredIsSafe pins the behaviour when the callbacks are
// absent. Nil must mean "at the tip, no compensation" — the SHORTEST ceiling —
// so a wiring mistake can never silently hand a peer more patience than intended.
func TestBlockDownloadBudgetUnwiredIsSafe(t *testing.T) {
	tSettings := settings.NewSettings()
	params := *tSettings.ChainCfgParams
	params.TargetTimePerBlock = 10 * time.Minute

	p := &Peer{settings: tSettings, logger: ulogger.TestLogger{}}
	p.cfg = Config{ChainParams: &params}

	require.Equal(t, 10*time.Minute, p.blockDownloadBudget())
}
