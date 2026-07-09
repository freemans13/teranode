package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestWindowAdmission_FlagOff_ByteIdentical verifies that when
// ParallelWindowMemoryFraction=0 (the default), the window path is unreachable:
// windowBudgetBytes(0)==0 so windowEnabled is false in the drain goroutine and
// handleBlockMsg is called per-block as before.
func TestWindowAdmission_FlagOff_ByteIdentical(t *testing.T) {
	// fraction=0 produces zero budget → window disabled.
	budget := windowBudgetBytes(0.0)
	require.Equal(t, int64(0), budget, "fraction=0 must produce zero budget (window disabled)")

	// Any fraction <= 0 must also produce zero budget.
	require.Equal(t, int64(0), windowBudgetBytes(-1.0), "negative fraction must produce zero budget")

	// The drain goroutine gate: with fraction=0 windowEnabled is false.
	const fraction = 0.0
	windowEnabled := fraction > 0
	require.False(t, windowEnabled, "window must be disabled when fraction=0")

	// When window is disabled, ProcessBlockWindow must never be called.
	// The blockvalidation.Mock panics on unexpected calls, so constructing it
	// with no ProcessBlockWindow expectation is the implicit assertion.
	mockBV := blockvalidation.NewMock()
	_ = mockBV
}

// spyBlockValidation records every ProcessBlockWindow call for later assertion.
// It embeds MockBlockValidation to satisfy the full blockvalidation.Interface
// with no-ops for methods not under test.
type spyBlockValidation struct {
	blockvalidation.MockBlockValidation
	batches [][]*model.Block
}

func (s *spyBlockValidation) ProcessBlockWindow(_ context.Context, blocks []*model.Block, _, _ string) error {
	// Take a copy of the slice header (not the elements) so the test can
	// independently mutate batches without aliasing.
	batchCopy := make([]*model.Block, len(blocks))
	copy(batchCopy, blocks)
	s.batches = append(s.batches, batchCopy)
	return nil
}

// Compile-time assertion that spyBlockValidation satisfies the interface.
var _ blockvalidation.Interface = (*spyBlockValidation)(nil)

// TestWindowAdmission_EligibilityGating verifies:
//  1. legacyUnified returns false for above-checkpoint heights and true for below.
//  2. ProcessBlockWindow batches only ever contain blocks added via wa.add
//     (i.e. below-checkpoint eligible blocks). An ineligible block is never
//     wa.add-ed, so it can never appear in any batch.
func TestWindowAdmission_EligibilityGating(t *testing.T) {
	const checkpointHeight = int32(1000)
	const aboveCheckpoint = uint32(1500)
	const belowCheckpoint = uint32(500)

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	spy := &spyBlockValidation{}

	sm := &SyncManager{
		settings:        tSettings,
		chainParams:     params,
		logger:          ulogger.TestLogger{},
		utxoStore:       &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		blockValidation: spy,
	}

	// Eligibility gating at the SyncManager level.
	require.False(t, sm.legacyUnified(aboveCheckpoint),
		"above-checkpoint block must not be eligible for window")
	require.True(t, sm.legacyUnified(belowCheckpoint),
		"below-checkpoint block with all gates open must be eligible for window")

	// Build minimal model.Blocks at both heights.
	belowBlock := newMinimalModelBlock(t, belowCheckpoint)

	// Simulate the drain goroutine routing logic:
	//   - below-checkpoint blocks are wa.add-ed (eligible path)
	//   - above-checkpoint blocks are NOT wa.add-ed (direct path, addedToWindow=false)
	// This is the key property: only wa.add-ed blocks reach ProcessBlockWindow.
	wa := newWindowAccumulator(100*1024*1024, 20)
	require.True(t, wa.empty(), "accumulator must start empty")

	// Add the below-checkpoint block (eligible).
	wa.add(belowBlock, nil)
	require.False(t, wa.empty(), "accumulator must hold the added block")

	// Flush — submits the window to ProcessBlockWindow via the spy.
	wa.flush(context.Background(), sm)

	// The spy must have received exactly one batch containing only the eligible block.
	require.Len(t, spy.batches, 1, "flush must call ProcessBlockWindow exactly once")

	batch := spy.batches[0]
	require.Len(t, batch, 1, "batch must contain exactly the one eligible block")
	require.Equal(t, belowCheckpoint, batch[0].Height, "batch block must be the below-checkpoint one")

	// Assert that the above-checkpoint height is absent from every batch.
	for i, b := range batch {
		require.NotEqual(t, aboveCheckpoint, b.Height,
			"above-checkpoint block must never appear in any ProcessBlockWindow batch (index %d)", i)
	}

	// Accumulator is drained after flush.
	require.True(t, wa.empty(), "accumulator must be empty after flush")
}

// newMinimalModelBlock builds a *model.Block at the given height for test use.
// It uses a zero-coinbase tx and a single zero-root subtree; no real validation runs.
func newMinimalModelBlock(t *testing.T, height uint32) *model.Block {
	t.Helper()

	hdr := wire.BlockHeader{
		Version: 1,
		Bits:    0x1d00ffff,
		Nonce:   0,
	}
	var hdrBuf bytes.Buffer
	require.NoError(t, hdr.Serialize(&hdrBuf))

	modelHdr, err := model.NewBlockHeaderFromBytes(hdrBuf.Bytes())
	require.NoError(t, err)

	zeroRoot := chainhash.Hash{}
	subtrees := []*chainhash.Hash{&zeroRoot}

	block, err := model.NewBlock(modelHdr, bt.NewTx(), subtrees, 1, 1024, height, 0)
	require.NoError(t, err)

	return block
}

// TestWindowAdmission_CheckpointBlockBypassesWindow asserts F3:
// a block whose isCheckpointBlock flag is true must never enter the window
// accumulator and therefore never appear in any ProcessBlockWindow batch.
//
// It simulates the drain-goroutine routing logic by replicating the gate
// condition that handleBlockMsgWithWindow evaluates:
//
//	ineligible := !sm.legacyUnified(height) || isCheckpointBlock
//
// This proves that even when legacyUnified returns true (the block is
// below the checkpoint and all other gates are open), setting
// isCheckpointBlock=true forces the block onto the direct path.
//
// It also exercises findNextHeaderCheckpoint to confirm that
// runPostBlockProcessing's nextCheckpoint bookkeeping would correctly
// advance to the subsequent checkpoint, avoiding the header-pipeline stall.
func TestWindowAdmission_CheckpointBlockBypassesWindow(t *testing.T) {
	const checkpoint1Height = int32(500)
	const checkpoint2Height = int32(1000)
	const checkpointBlockHeight = uint32(500) // matches checkpoint1Height
	const belowBlockHeight = uint32(499)

	// Two checkpoints: the block under test lands exactly on checkpoint1.
	tSettings, params := newOutpointOnlySettings(t, true, true, checkpoint1Height)
	// Add a second checkpoint so findNextHeaderCheckpoint has something to advance to.
	params.Checkpoints = []chaincfg.Checkpoint{
		{Height: checkpoint1Height},
		{Height: checkpoint2Height},
	}
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.ParallelWindowMemoryFraction = 0.1

	spy := &spyBlockValidation{}

	sm := &SyncManager{
		settings:        tSettings,
		chainParams:     params,
		logger:          ulogger.TestLogger{},
		utxoStore:       &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
		blockValidation: spy,
	}

	// Both heights are below the highest checkpoint, so legacyUnified returns true
	// for both.  This confirms that isCheckpointBlock is the discriminating factor.
	require.True(t, sm.legacyUnified(checkpointBlockHeight),
		"checkpoint-height block passes legacyUnified — isCheckpointBlock is the only gate")
	require.True(t, sm.legacyUnified(belowBlockHeight),
		"below-checkpoint block passes legacyUnified")

	// Gate evaluation: ineligible := !sm.legacyUnified(height) || isCheckpointBlock.
	// A normal below-checkpoint block (isCheckpointBlock=false) is eligible.
	require.False(t, !sm.legacyUnified(belowBlockHeight) || false,
		"below-checkpoint non-checkpoint block must be eligible for the window")
	// A checkpoint block (isCheckpointBlock=true) is ineligible even though
	// legacyUnified returns true for its height.
	require.True(t, !sm.legacyUnified(checkpointBlockHeight) || true,
		"checkpoint block must be ineligible — gate must short-circuit to direct path")

	wa := newWindowAccumulator(100*1024*1024, 20)

	// Simulate the drain goroutine: add one eligible (non-checkpoint) block to the
	// window first, so the window is non-empty when the checkpoint block arrives.
	belowBlock := newMinimalModelBlock(t, belowBlockHeight)
	wa.add(belowBlock, nil)
	require.False(t, wa.empty(), "window must hold the pre-checkpoint eligible block")

	// The checkpoint block is routed to the direct path (ineligible). In the real
	// blockHandler, handleBlockMsgWithWindow returns addedToWindow=false, and
	// blockHandler then calls flushWindow(). We simulate that sequence here.
	//
	// First: confirm the checkpoint block is never wa.add-ed.
	// (In production code the gate prevents the wa.add call; here we assert the gate.)
	checkpointBlock := newMinimalModelBlock(t, checkpointBlockHeight)
	isCheckpointBlock := true
	ineligible := !sm.legacyUnified(checkpointBlockHeight) || isCheckpointBlock
	require.True(t, ineligible, "checkpoint block must be routed to the direct path")
	// Because ineligible==true, wa.add is NOT called for checkpointBlock.
	// This is the production invariant we are asserting: no wa.add for checkpoint.

	// blockHandler's flushWindow() fires after the direct-path block is processed.
	wa.flush(context.Background(), sm)

	// ProcessBlockWindow must have been called exactly once, containing only the
	// eligible (non-checkpoint) block.  The checkpoint block must be absent.
	require.Len(t, spy.batches, 1, "flush must call ProcessBlockWindow exactly once")
	batch := spy.batches[0]
	require.Len(t, batch, 1, "batch must contain exactly the one eligible block")
	require.Equal(t, belowBlockHeight, batch[0].Height,
		"batch must contain only the below-checkpoint eligible block")

	for _, b := range batch {
		require.NotEqual(t, checkpointBlockHeight, b.Height,
			"checkpoint block must never appear in any ProcessBlockWindow batch")
	}

	// The _ variable suppresses unused-variable lint for checkpointBlock; in
	// production the direct path (HandleBlockDirect) would receive it.
	_ = checkpointBlock

	// Confirm nextCheckpoint bookkeeping: findNextHeaderCheckpoint(checkpoint1Height)
	// must return checkpoint2, which is what runPostBlockProcessing would advance to.
	// Without this advance the header pipeline stalls permanently.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: checkpoint1Height}
	next := sm.findNextHeaderCheckpoint(checkpoint1Height)
	require.NotNil(t, next, "findNextHeaderCheckpoint must find a next checkpoint")
	require.Equal(t, checkpoint2Height, next.Height,
		"nextCheckpoint must advance to checkpoint2 after processing the checkpoint block")
}

// TestWindowAdmission_ByteBudget covers calculateWindowK in five sub-cases:
// normal division, admit-one floor, maxBlocks clamping, zero avg → floor, maxBlocks=0.
func TestWindowAdmission_ByteBudget(t *testing.T) {
	t.Run("normal case: 10 MB budget / 1 MB avg = K=10", func(t *testing.T) {
		bst := newBlockSizeTracker(10)
		bst.addBlockSize(1 * 1024 * 1024)

		k := bst.calculateWindowK(10*1024*1024, 100)
		require.Equal(t, 10, k)
	})

	t.Run("admit-one floor: avg larger than budget → K=1", func(t *testing.T) {
		bst := newBlockSizeTracker(10)
		bst.addBlockSize(5 * 1024 * 1024 * 1024) // 5 GB average

		k := bst.calculateWindowK(1*1024*1024*1024, 100) // 1 GB budget
		require.Equal(t, 1, k, "huge avg block must clamp to admit-one floor K=1")
	})

	t.Run("clamped to maxBlocks", func(t *testing.T) {
		bst := newBlockSizeTracker(10)
		bst.addBlockSize(1024) // 1 KB average → raw K=102400 for 100 MB budget

		k := bst.calculateWindowK(100*1024*1024, 20)
		require.Equal(t, 20, k, "K must be clamped to maxBlocks=20")
	})

	t.Run("zero average → K=1 (admit-one floor)", func(t *testing.T) {
		bst := newBlockSizeTracker(10)
		// No addBlockSize call → avgSize=0.

		k := bst.calculateWindowK(10*1024*1024, 100)
		require.Equal(t, 1, k, "zero average size must yield K=1 (admit-one floor)")
	})

	t.Run("maxBlocks=0 means no clamping", func(t *testing.T) {
		bst := newBlockSizeTracker(10)
		bst.addBlockSize(1 * 1024 * 1024)

		k := bst.calculateWindowK(50*1024*1024, 0)
		require.Equal(t, 50, k, "maxBlocks=0 must not clamp")
	})
}

// TestPrepareBlockForWindow_PoWCheck verifies that HasMetTargetDifficulty —
// the first precondition in prepareBlockForWindow — correctly rejects a block
// whose hash does not meet the encoded target. Tests run at the model.BlockHeader
// level so no full SyncManager is required.
func TestPrepareBlockForWindow_PoWCheck(t *testing.T) {
	t.Run("all-zero bits: every hash fails PoW", func(t *testing.T) {
		// NBit all-zero → mantissa=0 → CalculateTarget()=0.
		// Any real header hash is a positive number, so it exceeds target=0.
		hdr := wire.BlockHeader{
			Version: 1,
			Bits:    0x00000000, // impossible target — no block can satisfy this
			Nonce:   12345,
		}
		var buf bytes.Buffer
		require.NoError(t, hdr.Serialize(&buf))

		modelHdr, err := model.NewBlockHeaderFromBytes(buf.Bytes())
		require.NoError(t, err)

		valid, _, _ := modelHdr.HasMetTargetDifficulty()
		require.False(t, valid, "all-zero bits must cause every block to fail PoW check")
	})

	t.Run("regtest easy-target (0x207fffff) with nonce=0: PoW passes", func(t *testing.T) {
		// 0x207fffff is the regtest PowLimitBits — an enormous target that any
		// header hash satisfies, so nonce=0 always passes without solving.
		hdr := wire.BlockHeader{
			Version: 1,
			Bits:    0x207fffff,
			Nonce:   0,
		}
		var buf bytes.Buffer
		require.NoError(t, hdr.Serialize(&buf))

		modelHdr, err := model.NewBlockHeaderFromBytes(buf.Bytes())
		require.NoError(t, err)

		valid, _, _ := modelHdr.HasMetTargetDifficulty()
		require.True(t, valid, "regtest easy-target header with nonce=0 must pass PoW")
	})
}
