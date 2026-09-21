package utxoset

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// These tests pin cases where a UTXO, or the undo copy of a spent UTXO, ends up carrying block
// facts that are wrong once its membership window has retired. Each one needs a reorg or a
// transaction seen before its block, so none of them can happen during below-checkpoint sync,
// and all of them can at the tip. All four FAIL on today's code, which is the point: they
// reproduce known defects rather than guard working behaviour.
//
// The rule every test checks: once the window holding a transaction's membership rows is gone,
// whatever still answers for that transaction must name the block that is on the longest chain.
//
// They are skipped by default so the suite stays green and a red run always means a
// regression. The design that fixes them is docs/superpowers/specs/
// 2026-09-16-utxoset-block-facts-spec.md, whose test plan requires all four to pass
// unmodified; three should pass once containment lands, before the new stamp exists. Remove
// the skip from each as its fix lands.

// knownDefectEnv, when set to any value, runs the known-defect tests instead of skipping them,
// which is how they are driven while the fix is being built.
const knownDefectEnv = "UTXOSET_RUN_KNOWN_DEFECTS"

// skipKnownDefect skips a test that reproduces a defect the store still has.
func skipKnownDefect(t *testing.T, defect string) {
	t.Helper()

	if os.Getenv(knownDefectEnv) != "" {
		return
	}

	t.Skipf("known defect, not a regression: %s. Fixed by the design in "+
		"docs/superpowers/specs/2026-09-16-utxoset-block-facts-spec.md; set %s=1 to run it",
		defect, knownDefectEnv)
}

// TestRetiringWindowStampsTheBlockThatWonTheReorg: the transaction is mined in M, a competing
// block F also includes it, and F's chain then wins.
//
// This is the call sequence block assembly makes. F arrives as a fork, so its stamp only appends
// a membership row. When F's branch becomes the longest chain, Reset keeps the transaction out of
// the mark-off because it is in a move-forward block, and stampMoveForwardBlockAsMined stamps F
// again with OnLongestChain set. M stays a valid block on a side chain, so nothing un-mines it.
//
// The rows are then M first and F second, and the retirement stamp takes the earliest row, so
// the UTXO is stamped with M, the losing block. After the window drops, a child spending this
// UTXO asks for a parent in a block that is not on the chain.
func TestRetiringWindowStampsTheBlockThatWonTheReorg(t *testing.T) {
	skipKnownDefect(t, "the retirement stamp takes the earliest membership row, so a reorg loser is stamped onto the UTXO")

	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
	require.NoError(t, err)

	// F's branch wins, and the move-forward stamp names F on the longest chain.
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(8), b, "the block that won the reorg, not the first block that stamped the transaction")
}

// TestSideChainCreateIsCorrectedWhenTheMainChainBlockStampsIt: a fork block F is applied first
// and creates the transaction on the block path, then the main chain block M includes it too.
//
// The legacy path applies side-chain blocks as well as main-chain ones, and a block-path create
// writes the creating block's facts onto every UTXO. When M later stamps the same transaction,
// the store only appends a membership row. The retirement stamp touches only UTXOs still at
// height 0, so these UTXOs keep F's facts for good.
func TestSideChainCreateIsCorrectedWhenTheMainChainBlockStampsIt(t *testing.T) {
	skipKnownDefect(t, "a side-chain block-path create writes its own block onto the UTXO, and the stamp only touches UTXOs at height 0")

	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(7), b, "the main chain block, not the side-chain block that created the UTXO")
}

// TestParentSpentWhileUnconfirmedIsStillAnswerableAfterItsWindowRetires: a transaction seen before its block
// is mined, and an unmined child spends its only UTXO before the membership window retires.
//
// The spend copies the UTXO into the journal with the UTXO's facts, and a UTXO created before its block
// still carries height 0 at that point, because mining does not touch UTXOs. The retirement
// stamp finds no live UTXO, so it stamps nothing, and nothing ever stamps journal rows. Once the
// window is gone, the lookup's last step reads the journal but skips rows at height 0, so the
// parent answers "not found". Validating a block that contains the child then retries forever.
func TestParentSpentWhileUnconfirmedIsStillAnswerableAfterItsWindowRetires(t *testing.T) {
	skipKnownDefect(t, "a UTXO spent before it is stamped leaves an undo copy at height 0 that nothing stamps, so the parent is not found once its window retires")

	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 101)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err, "a mined parent whose last UTXO was spent must still be found after its window retires")
	require.Equal(t, []uint32{7}, got.BlockIDs)
}

// TestUnspendOfAnUnminedParentRestoresAnUnconfirmedUTXO: a parent is mined in M, a child spends
// one of its UTXOs, M is un-mined so the parent is unmined again, and then the child's
// spend is undone.
//
// Un-mining deletes the parent's membership rows and resets its live UTXOs to height 0. The
// restore then re-resolves block facts from the membership table, finds nothing, and falls back
// to the journal copy, which still names M. So the restored UTXO says mined in M while its
// transaction is unmined. If the parent is re-mined in another block, the move leaves
// the UTXO alone and the retirement stamp skips it because it is not at height 0, so it names M
// for good.
func TestUnspendOfAnUnminedParentRestoresAnUnconfirmedUTXO(t *testing.T) {
	skipKnownDefect(t, "unspend restores the undo copy's stale block onto a UTXO whose transaction is unmined again")

	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: parent.Outputs[0].Satoshis - 1_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(200))

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, parent), "un-mining puts the parent back in the identity table")

	require.NoError(t, s.Unspend(ctx, spends, false))

	h, b := utxoFacts(t, s, ctx, parent)
	require.Equal(t, int32(0), h, "a restored UTXO of an unmined transaction is unconfirmed")
	require.Equal(t, int32(0), b)
}
