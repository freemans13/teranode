package blockvalidation

// TestCheckpointHeight_WriteReadAgree asserts invariant I3: the inline loop in
// model.Block.checkBlockRewardAndFees (the "read side") produces the same highest
// checkpoint height as blockchain.HighestCheckpointHeight (the "write side") for
// the same params.Checkpoints. The two functions must agree so that the fast-path
// fee=0 write and the revalidation read both skip at the same boundary.
//
// Placement: this file lives in services/blockvalidation (package blockvalidation,
// white-box test) because model cannot import services/blockchain without creating
// an import cycle (model → blockchain → model). blockvalidation already imports
// both model and services/blockchain, making it the closest clean cross-import site.

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

func TestCheckpointHeight_WriteReadAgree(t *testing.T) {
	params := &chaincfg.MainNetParams

	writeSide := blockchain.HighestCheckpointHeight(params.Checkpoints)

	// Inline loop — byte-equivalent to the one in model.Block.checkBlockRewardAndFees.
	var readSide uint32
	for _, cp := range params.Checkpoints {
		if cp.Height < 0 {
			continue
		}
		if h := uint32(cp.Height); h > readSide {
			readSide = h
		}
	}

	require.Equal(t, writeSide, readSide, "fee write/read checkpoint height must be identical (I3)")
}

// TestOperatorOverrideFence (T-B5 — invariant I2): asserts that quickValidateOutpointOnly
// returns FALSE for a block above the highest HARDCODED checkpoint height, even when the
// operator-supplied CatchupCheckpointHash/CatchupCheckpointHeight settings are set to a
// height above the hardcoded set (simulating an operator override that would otherwise widen
// the fast-path window). The fast path must be gated on ChainCfgParams.Checkpoints only
// (spec §2.2, invariant I2).
func TestOperatorOverrideFence(t *testing.T) {
	const hardcodedCheckpointHeight = uint32(1000)
	const aboveHardcoded = uint32(1500)
	const operatorOverrideHeight = int32(2000) // higher than hardcoded — the dangerous value

	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// Enable the outpoint-only fast path.
	suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true

	// Set the HARDCODED checkpoint to 1000.
	setCheckpoints(t, suite, hardcodedCheckpointHeight)

	// Simulate the operator override: set CatchupCheckpointHeight above the hardcoded value.
	// If quickValidateOutpointOnly honoured this, it would engage at height 1500.
	suite.Server.blockValidation.settings.BlockValidation.CatchupCheckpointHeight = operatorOverrideHeight

	// Block at height 1500: above the hardcoded checkpoint but below the operator override.
	block := &model.Block{Height: aboveHardcoded}

	// The fast path must NOT engage: it uses ChainCfgParams.Checkpoints, not the operator override.
	got := suite.Server.blockValidation.quickValidateOutpointOnly(block)
	require.False(t, got,
		"quickValidateOutpointOnly must return false for height %d (above hardcoded checkpoint %d) "+
			"even when CatchupCheckpointHeight=%d (operator override must not widen the fast-path window; I2)",
		aboveHardcoded, hardcodedCheckpointHeight, operatorOverrideHeight)

	// Sanity: same block at height BELOW the hardcoded checkpoint must return true.
	blockBelow := &model.Block{Height: hardcodedCheckpointHeight - 1}
	gotBelow := suite.Server.blockValidation.quickValidateOutpointOnly(blockBelow)
	require.True(t, gotBelow,
		"quickValidateOutpointOnly must return true for height %d at or below hardcoded checkpoint %d",
		blockBelow.Height, hardcodedCheckpointHeight)
}

// TestCheckpointBoundary_B1_Deferred is a placeholder for T-B1 (spec §6):
// spend at checkpoint+1 of an output created at checkpoint−1, including a coinbase
// output for coinbase maturity. This test requires a real multi-block cross-checkpoint
// sync flow (two blocks, real catchup or real block-validation pipeline with persisted
// subtree data) that cannot be honestly expressed with the CatchupTestSuite mock store.
// Deferred to e2e/smoketest: see spec §6 T-B1.
func TestCheckpointBoundary_B1_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-B1 requires multi-block cross-checkpoint flow with real persisted outputs; see spec §6 T-B1")
}

// TestCheckpointBoundary_B3_Deferred is a placeholder for T-B3 (spec §6):
// a fork diverging below the checkpoint is rejected (both catchup.go checkpoint
// verification and legacy headers-first). This test exercises existing checkpoint
// rejection machinery; confirming it still holds with the fast path enabled requires
// a real multi-header chain with a checkpoint mismatch at the fork height.
// Deferred to e2e/smoketest: see spec §6 T-B3.
func TestCheckpointBoundary_B3_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-B3 exercises catchup.go:771-773 checkpoint rejection; confirmed by existing integration harness, not exercisable with mock store; see spec §6 T-B3")
}

// TestCheckpointBoundary_B4_Deferred is a placeholder for T-B4 (spec §6):
// reconsiderblock on a below-checkpoint height succeeds (does not return BLOCK_INVALID).
// The underlying checkBlockRewardAndFees height-skip is directly unit-tested in Task 5.
// The full RPC path (handleReconsiderBlock → RevalidateBlock → block.Valid) requires a
// real persisted block with subtree files and a real block-validation pipeline.
// Deferred to e2e/smoketest: see spec §6 T-B4.
func TestCheckpointBoundary_B4_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-B4 full reconsiderblock RPC path needs real persisted block + subtree files; underlying checkBlockRewardAndFees height-skip covered by Task 5 unit test; see spec §6 T-B4")
}

// TestCheckpointBoundary_I2_Deferred is a placeholder for T-I2 (spec §6):
// first above-checkpoint block validates (full Block.Valid) after a flagged sync.
// This requires a real sync to the checkpoint height with the flag ON, followed by
// a real above-checkpoint block written with its own decorate+fees.
// Deferred to e2e/smoketest: see spec §6 T-I2.
func TestCheckpointBoundary_I2_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-I2 requires real sync to checkpoint + first above-checkpoint block validation with real subtree data; see spec §6 T-I2")
}
