package subtreeprocessor

// Tests for Task 2 and Task 3: moveForwardBlock / MoveForwardBlock blockMeta threading.
//
// Task 2: the internal moveForwardBlock accepts an optional blockMeta param.
// Task 3: the exported MoveForwardBlock threads the param from caller → channel
//         request → internal moveForwardBlock.
//
// When a non-nil blockMeta is supplied by the caller, the IBD fast-path gate
// must reuse it and skip the GetBlockHeader gRPC call entirely.
// When nil is passed, the gate must fall back to the existing GetBlockHeader call
// (exactly one RPC, same behaviour as before Task 2/3).
//
// These tests use the existing buildIBDFastPathSTP / blockchain.Mock harness so
// they can precisely count GetBlockHeader invocations via mock assertions.
//
// Test index:
//  1. TestMoveForwardBlock_UsesPassedMetaSkipsGetBlockHeader    — non-nil meta → 0 gRPC calls (internal)
//  2. TestMoveForwardBlock_NilMetaFallsBackToGetBlockHeader     — nil meta     → 1 gRPC call  (internal)
//  3. TestExportedMoveForwardBlock_ThreadsMetaToFastPath        — exported path + non-nil meta → 0 gRPC calls

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildBelowCpFastPathBlock returns a below-checkpoint block with a fake subtree
// hash that chains onto stp's currentBlockHeader. All maps (currentTxMap, queue,
// removeMap) are empty, so the emptyMaps precondition holds. The fake subtree hash
// will cause the full path to error if it fires, acting as the discriminator.
func buildBelowCpFastPathBlock(t *testing.T, stp *SubtreeProcessor) *model.Block {
	t.Helper()

	fakeSubtreeHash := chainhash.HashH([]byte("nonexistent-subtree-moveforward-meta-test"))
	return ibdBlock(
		stp.currentBlockHeader.Load(),
		uint32(ibdTestBlockHeight), // below ibdTestCheckpointHeight
		[]*chainhash.Hash{&fakeSubtreeHash},
	)
}

// TestMoveForwardBlock_UsesPassedMetaSkipsGetBlockHeader verifies that when a
// non-nil blockMeta (MinedSet=true, QuickValidated=true) is passed directly to
// moveForwardBlock, the IBD fast-path gate uses it without calling GetBlockHeader.
//
// RED  (before Task 2): compile error — moveForwardBlock takes 6 args, not 7.
// GREEN (after Task 2): 0 GetBlockHeader calls; fast-path fires; nil,nil,nil returned.
func TestMoveForwardBlock_UsesPassedMetaSkipsGetBlockHeader(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)
	stp.InitCurrentBlockHeader(prevBlockHeader)

	block := buildBelowCpFastPathBlock(t, stp)
	meta := &model.BlockHeaderMeta{MinedSet: true, QuickValidated: true}

	txMap, losingMap, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true, meta,
	)

	require.NoError(t, err, "fast-path must fire when caller passes MinedSet+QuickValidated meta")
	require.Nil(t, txMap, "fast-path returns nil transactionMap")
	require.Nil(t, losingMap, "fast-path returns nil losingTxHashesMap")

	// GetBlockHeader must NOT have been called — the passed meta short-circuits it.
	bcMock.AssertNotCalled(t, "GetBlockHeader")
}

// TestMoveForwardBlock_NilMetaFallsBackToGetBlockHeader verifies that when nil
// is passed as blockMeta, the gate falls back to the existing GetBlockHeader RPC
// (exactly one call) and the fast-path fires based on the returned meta.
//
// RED  (before Task 2): compile error — moveForwardBlock takes 6 args, not 7.
// GREEN (after Task 2): exactly 1 GetBlockHeader call; fast-path fires; nil,nil,nil returned.
func TestMoveForwardBlock_NilMetaFallsBackToGetBlockHeader(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)
	stp.InitCurrentBlockHeader(prevBlockHeader)

	block := buildBelowCpFastPathBlock(t, stp)

	// Wire the mock to return a valid fast-path meta for the gRPC fallback.
	bcMock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(prevBlockHeader, &model.BlockHeaderMeta{MinedSet: true, QuickValidated: true}, nil)

	txMap, losingMap, err := stp.moveForwardBlock(
		context.Background(), block, false, map[chainhash.Hash]struct{}{}, false, true, nil,
	)

	require.NoError(t, err, "fast-path must fire via gRPC fallback when nil meta is passed")
	require.Nil(t, txMap, "fast-path returns nil transactionMap")
	require.Nil(t, losingMap, "fast-path returns nil losingTxHashesMap")

	// GetBlockHeader must have been called exactly once — the nil-meta fallback path.
	bcMock.AssertNumberOfCalls(t, "GetBlockHeader", 1)
}

// TestExportedMoveForwardBlock_ThreadsMetaToFastPath verifies that the exported
// MoveForwardBlock(block, meta) correctly threads the meta through the channel
// request to the internal moveForwardBlock, allowing the IBD fast-path to fire
// without issuing a GetBlockHeader gRPC call.
//
// RED  (before Task 3): compile error — MoveForwardBlock(block) takes 1 arg, not 2.
// GREEN (after Task 3): 0 GetBlockHeader calls; fast-path fires; no error returned.
func TestExportedMoveForwardBlock_ThreadsMetaToFastPath(t *testing.T) {
	stp, bcMock := buildIBDFastPathSTP(t)
	stp.InitCurrentBlockHeader(prevBlockHeader)
	stp.Start(t.Context())

	block := buildBelowCpFastPathBlock(t, stp)
	meta := &model.BlockHeaderMeta{MinedSet: true, QuickValidated: true}

	require.NoError(t, stp.MoveForwardBlock(block, meta),
		"exported MoveForwardBlock must succeed when caller passes MinedSet+QuickValidated meta")

	// GetBlockHeader must NOT have been called — the meta threaded through the
	// channel request short-circuits the gRPC call entirely.
	bcMock.AssertNotCalled(t, "GetBlockHeader")
}
