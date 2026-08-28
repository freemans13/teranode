package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestJournalRunsBelowTheCheckpoint is the regression guard for a decision that was made
// twice, the wrong way round the first time.
//
// SetBlockHeight used to drive sync mode off model.BelowCheckpoint and switch the journal
// off for the entire initial sync. The reasoning was correct as far as it went: below the
// hardcoded checkpoint a reorg is impossible by rule, so the undo payload can never be
// replayed, and writing it is a heap insert plus an index insert per input for nothing.
//
// What that missed is that the journal is also the prune engine. Every spend writes a row
// grouped by height, which is the only record of WHICH transactions had an output spent in
// a given window -- the work list the pruner reads to decide what is now fully spent. With
// the journal off, nothing at all can be reclaimed below the checkpoint. Mainnet's highest
// checkpoint is 945,000, roughly 6.88 billion transactions are mined below it, and the
// unreclaimable residue is 165 to 444 GB on an 875 GB disk.
//
// So the journal has no off-switch, and this test pins the height case that used to
// disable it: deep below the checkpoint, a spend must still be journalled.
func TestJournalRunsBelowTheCheckpoint(t *testing.T) {
	s, ctx := newTestStore(t)

	s.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	require.NoError(t, s.SetBlockHeight(200))

	parent := mkTx(t, 1, 555)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	_, err = spendOnly(ctx, s, child, 200)
	require.NoError(t, err)

	var journalled int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal`).Scan(&journalled))
	require.Equal(t, 1, journalled,
		"the journal is the prune engine, not just reorg insurance: without it nothing below the checkpoint can ever be reclaimed")
}
