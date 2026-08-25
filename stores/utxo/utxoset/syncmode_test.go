package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestSyncModeFollowsTheCheckpoint wires up the one thing that makes M1 worth having.
//
// SetSyncMode existed with no production caller at all, so the spend journal ran for the
// entire initial sync. That is a heap insert plus an index insert per input, on top of
// the delete, for every spend from genesis to the tip: precisely the per-spend write
// amplification the below-checkpoint mode exists to remove.
//
// Below the hardcoded checkpoint a reorg is impossible by rule, so there is nothing the
// journal could ever be replayed for and skipping it costs nothing. Above it, the journal
// is what makes a spend reversible, so it MUST come back on. Driving both edges off the
// height means it cannot be left in the wrong state by an operator or a restart.
func TestSyncModeFollowsTheCheckpoint(t *testing.T) {
	s, _ := newTestStore(t)

	s.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000}}

	require.NoError(t, s.SetBlockHeight(500))
	require.False(t, s.JournalEnabled(),
		"below the checkpoint a reorg is impossible, so the journal is dead weight")

	require.NoError(t, s.SetBlockHeight(1_000))
	require.False(t, s.JournalEnabled(), "the checkpoint height itself is still below")

	require.NoError(t, s.SetBlockHeight(1_001))
	require.True(t, s.JournalEnabled(),
		"past the checkpoint a spend must be reversible again")

	// And it must flip back if the node reorgs down below the checkpoint.
	require.NoError(t, s.SetBlockHeight(900))
	require.False(t, s.JournalEnabled())
}
