package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/stretchr/testify/require"
)

// TestHandleConvertedBlock_CommitsAboveTheCheckpointWithoutTheUnifiedFlag is
// task 13's required end-to-end test (task-13-brief.md Step 2), driving a
// block above the chain's final checkpoint through the real pipelineBlockSink
// and the real HandleConvertedBlock, with NEITHER
// BlockValidation.LegacyUnifiedBelowCheckpoint NOR
// BlockValidation.OutpointOnlyBelowCheckpoint set — unlike every other test in
// handle_converted_block_test.go, which sets both so their below-checkpoint
// fixtures are eligible for the (now-removed) legacyUnified gate. This is the
// one case that gate used to refuse outright, and it must now commit exactly
// like the below-checkpoint tests beside it.
//
// Infrastructure is the same real stack pipeline_sink_test.go and
// handle_converted_block_test.go already use: a real subtree blob store, a
// real sqlitememory-backed blockchain client (blockchainstore.NewStore +
// blockchain2.NewLocalClient, inside newPipelineManager), and a real blockPark
// over an in-memory blob store. blockValidation is
// convertedRouteSpyValidation, the same spy every other test in
// handle_converted_block_test.go uses.
//
// HONEST LIMIT, stated here and in this task's commit message: this does NOT
// reach real validation or a real Kafka round trip. No test in this package
// constructs a real blockvalidation.Server — every one of them, this one
// included, stands in a spy for it, because driving a block through the real
// server needs a live SV Node / the full service wiring this package's tests
// do not build (see TestHandleConvertedBlock_CommitsWithoutTheBlock's own doc
// comment, which states the identical limit for the below-checkpoint case).
// What this test actually proves: a converted record built above the
// checkpoint, carrying blockID 0, reaches blockValidation.ProcessBlock with
// the correct height and content, through the real routing and height
// re-derivation HandleConvertedBlock performs — the class of change that
// wedged mainnet at block 386,817 on 2026-05-31 was a downstream consumer
// nobody had listed, which a spy standing in for that exact consumer cannot
// surface any more than the below-checkpoint tests already in this file can.
// The honest fallback for that gap is a soak on a live node, not a unit test.
func TestHandleConvertedBlock_CommitsAboveTheCheckpointWithoutTheUnifiedFlag(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	// BelowCheckpoint (model/checkpoint.go) requires highest > 0, so a
	// checkpoint height of 0 means "no checkpoint reaches this chain", and
	// every real height — including this fixture's resolved height of 1 —
	// reads as above it. That makes quickValidationAllowed false and
	// legacyUnified false (it requires BelowCheckpoint too, so leaving
	// LegacyUnifiedBelowCheckpoint/OutpointOnlyBelowCheckpoint at their
	// newPipelineManager defaults would make no difference here, but they are
	// left off below anyway to say plainly that this test does not depend on
	// them).
	sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 0}}
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = false
	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy
	// sm.blockAssembly stays nil: WaitForBlockAssemblyReady treats a nil
	// client as "skip" (util/blockassemblyutil), the same as every other test
	// in this file.

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block above the checkpoint must convert cleanly, not be refused")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	// Confirm the premise directly rather than trusting the checkpoint
	// arithmetic: this record must actually be above the checkpoint (not
	// eligible for quick validation), or this test is not exercising the case
	// it claims to.
	require.False(t, sm.quickValidationAllowed(1), "sanity: height 1 must read as above the checkpoint for this test to mean anything")

	record, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, uint32(1), record.Height, "sanity: the fixture's parent is genesis, so the resolved height must be 1")

	for _, h := range record.Subtrees {
		toCheck, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.True(t, toCheck, "above the checkpoint the structure file must be FileTypeSubtreeToCheck")
	}

	err = sm.HandleConvertedBlock(ctx, nil, *blk.Hash(), record)
	require.NoError(t, err, "a valid converted record above the checkpoint must commit, with neither legacyUnified conjunct set")

	require.Equal(t, 1, spy.callCount(), "HandleConvertedBlock must hand the record to blockValidation exactly once")

	call := spy.lastCall()
	require.Equal(t, record.Header.Hash().String(), call.block.Header.Hash().String(), "the committed block must be the record's own")
	require.Equal(t, record.Height, call.blockHeight, "the committed height must be the record's own height, re-verified against the chain")
	require.Equal(t, uint32(0), call.blockID, "blockID must be 0 above the checkpoint too: the universal assign-server-side convention full validation reads (Step 1 of task 13)")
	require.Equal(t, uint64(6), call.block.TransactionCount, "the record's transaction count must reach the committer unchanged")
	require.Equal(t, len(record.Subtrees), len(call.block.Subtrees), "the record's subtree list must reach the committer unchanged")
}
