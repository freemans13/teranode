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
// facts that are wrong once its containment window has retired. Each one needs a reorg or a
// transaction seen before its block, so none of them can happen during below-checkpoint sync,
// and all of them can at the tip. They reproduce known defects rather than guard working
// behaviour.
//
// The rule every test checks: once the window holding a transaction's containment rows is gone,
// whatever still answers for that transaction must name the block that is on the longest chain.
//
// Three of the four are EXPECTED FAILURES, not plain skips, so they cannot be forgotten. Each
// test runs its scenario every time. While the defect is present the test reports itself as a
// known defect and the suite stays green. The moment a fix makes the correct behaviour appear,
// the test FAILS and says so, and it keeps failing until its knownDefect guard is replaced by
// the plain assertion. So a fix cannot merge while its proof is still switched off.
//
// The design that fixes them is
// docs/superpowers/specs/2026-09-21-utxoset-block-facts-spec-rebuilt.md. Its build order puts
// the fixes in two steps, and only ONE of the four passes at the first. Step 2, containment,
// makes the un-mine a point delete and stops anything writing a block onto a UTXO from
// containment rows ranked by arrival, which is what closes reproduction 4; its guard is
// replaced by plain assertions below. Reproductions 1 and 2 need the chain-aware stamp and
// reproduction 3 needs the new drop rule, both of which are step 5, so their guards stay. An
// earlier version of this comment said three of the four should pass once containment lands;
// that was wrong, and section 13 of the design says why.

// knownDefectEnv, when set to any value, runs the real assertions instead, which is how these
// tests are driven while a fix is being built: they fail with the full expected-versus-actual
// output rather than reporting a known defect.
const knownDefectEnv = "UTXOSET_RUN_KNOWN_DEFECTS"

// knownDefect is the expected-failure guard. fixed reports whether the store already gives the
// correct answer; assert holds the real assertions.
func knownDefect(t *testing.T, defect string, fixed bool, assert func()) {
	t.Helper()

	if os.Getenv(knownDefectEnv) != "" {
		assert()

		return
	}

	if fixed {
		t.Fatalf("known defect appears FIXED: %s. Replace the knownDefect guard in this test with "+
			"its plain assertions, so the test guards the fix from now on", defect)
	}

	t.Skipf("known defect still present, not a regression: %s. See "+
		"docs/superpowers/specs/2026-09-21-utxoset-block-facts-spec-rebuilt.md; set %s=1 for the full failure",
		defect, knownDefectEnv)
}

// TestRetiringWindowStampsTheBlockThatWonTheReorg: the transaction is mined in M, a competing
// block F also includes it, and F's chain then wins.
//
// This is the call sequence block assembly makes. F arrives as a fork, so recording it adds a
// containment row. When F's branch becomes the longest chain, Reset keeps the transaction out
// of the mark-off because it is in a move-forward block, and stampMoveForwardBlockAsMined
// records F again with OnLongestChain set. M stays a valid block on a side chain, so nothing
// un-mines it.
//
// The retirement stamp that used to take the earliest row, and so stamped M onto the UTXO, is
// deleted with the containment change. Nothing stamps at all until the chain-aware stamp of
// build step 5 exists: the UTXO stays at (0,0), and the interim guard refuses to drop the
// window while the transaction's identity row exists. Either way the UTXO does not name the
// block that won, which is the defect this guards until step 5.
func TestRetiringWindowStampsTheBlockThatWonTheReorg(t *testing.T) {
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

	knownDefect(t, "no stamp exists yet: the UTXO of a transaction seen before its block never learns the block that won",
		h == 100 && b == 8, func() {
			require.Equal(t, int32(100), h)
			require.Equal(t, int32(8), b, "the block that won the reorg, not the first block that stamped the transaction")
		})
}

// TestSideChainCreateIsCorrectedWhenTheMainChainBlockStampsIt: a fork block F is applied first
// and creates the transaction on the block path, then the main chain block M includes it too.
//
// The store applies the checkpoint test itself, so on a network with no checkpoints (this
// store's) a block-carrying create takes the identity route: an identity row, a containment
// row for F and UTXOs at (0,0). When M later records the same transaction it adds its own
// containment row and clears the marker. Nothing stamps a UTXO until the chain-aware stamp of
// build step 5, and the interim guard refuses to drop the window while the identity row
// exists, so the UTXO stays at (0,0) and does not name M, the main-chain block. At step 5 the
// stamp, told that M won, writes (100, 7) and deletes F's row, which is what the guard waits
// for. Below the checkpoint this scenario cannot arise: a fork block cannot be header-proven.
func TestSideChainCreateIsCorrectedWhenTheMainChainBlockStampsIt(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	h, b := utxoFacts(t, s, ctx, tx)

	knownDefect(t, "no stamp exists yet: a UTXO born above the checkpoint from a side-chain block stays at (0,0) until the stamp names the main-chain block",
		h == 100 && b == 7, func() {
			require.Equal(t, int32(100), h)
			require.Equal(t, int32(7), b, "the main chain block, not the side-chain block that created the UTXO")
		})
}

// TestParentSpentWhileUnconfirmedIsStillAnswerableAfterItsWindowRetires: a transaction seen
// before its block is mined, and a child spends its only UTXO before the containment window
// retires.
//
// The spend copies the UTXO into the journal with the UTXO's pair, and a UTXO created before its
// block still carries (0,0) at that point, because recording mined does not touch UTXOs. The
// stamp finds no live UTXO, so it stamps nothing, and nothing ever stamps journal rows. Once the
// window is gone, the lookup's last step reads the journal but skips rows at height 0, so the
// parent answers "not found". Validating a block that contains the child then retries forever.
//
// The identity rows are deleted by hand before the drop, standing in for the stamp of build
// step 5, which is the one thing that deletes an identity row after mining: the window drop this
// reproduction needs is refused while any identity row exists, and after the stamp a fully
// spent parent has exactly this shape, no identity row, no UTXO, an undo copy at (0,0) and its
// containment window as its only home. What closes the defect is the drop rule of step 5, which
// keeps the window attached past every undo copy of its UTXOs.
func TestParentSpentWhileUnconfirmedIsStillAnswerableAfterItsWindowRetires(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 101)

	dropIdentityRow(t, s, ctx, parent)
	dropIdentityRow(t, s, ctx, child)

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "the window has to be gone for this reproduction to mean anything")

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)

	knownDefect(t, "a UTXO spent before it is stamped leaves an undo copy at height 0 that nothing stamps, so the parent is not found once its window retires",
		err == nil && got != nil && len(got.BlockIDs) == 1 && got.BlockIDs[0] == 7, func() {
			require.NoError(t, err, "a mined parent whose last UTXO was spent must still be found after its window retires")
			require.Equal(t, []uint32{7}, got.BlockIDs)
		})
}

// TestUnspendOfAnUnminedParentRestoresAnUnconfirmedUTXO: a parent seen before its block is
// mined in M, a child spends one of its UTXOs, M is un-mined so the parent is unmined again,
// and then the child's spend is undone.
//
// This is the fourth reproduction, and the containment change closes it, so its guard is a
// plain assertion. It runs on a store with no checkpoints, because an un-mine at or below the
// checkpoint is refused. The parent is created unmined rather than through the block path,
// because a block-path create writes no identity row and the un-mine, now a point delete,
// produces none, so the identity assertion below would have nothing to find. With an unmined create the
// identity row is there because it is kept alive until the stamp. The un-mine deletes the one
// containment row and sets the marker; the undo copy holds (0,0) because recording mined never
// touched the UTXO; and the restore, finding no containment row, falls back to that copy and
// puts (0,0) back. The old code's move-back deleted every row and its restore could resurrect a
// stale pair from the copy, which is the hazard the block-born variant still carries and which
// rests on the open decision about invalidation at or below the checkpoint.
func TestUnspendOfAnUnminedParentRestoresAnUnconfirmedUTXO(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
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
	require.True(t, identExists(t, s, ctx, parent), "the identity row was there all along and the un-mine marks it")
	require.NotNil(t, markerOf(t, s, ctx, parent), "the parent is unmined again")
	require.Equal(t, 0, minedRows(t, s, ctx, parent), "and the one containment row is gone")

	require.NoError(t, s.Unspend(ctx, spends, false))

	h, b := utxoFacts(t, s, ctx, parent)
	require.Equal(t, int32(0), h, "a restored UTXO of an unmined transaction is unconfirmed")
	require.Equal(t, int32(0), b)
}
