package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// buildFutureChain stores medianTimeBlocks (11) linked blocks on top of the
// current best block, stamped with strictly increasing timestamps starting at
// baseTime, and returns the new tip header. With timestamps baseTime..baseTime+10
// the median-time-past seen by a candidate on the tip is baseTime+5.
func buildFutureChain(t *testing.T, ctx context.Context, server *BlockAssembly, baseTime uint32) *model.BlockHeader {
	t.Helper()

	prevHeader, _, err := server.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	nbits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	for i := uint32(0); i < 11; i++ {
		header := &model.BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  prevHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      baseTime + i,
			Bits:           *nbits,
			Nonce:          i,
		}

		require.NoError(t, server.blockchainClient.AddBlock(ctx, &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}, ""))

		prevHeader = header
	}

	return prevHeader
}

func TestCandidateTime(t *testing.T) {
	t.Run("tracks the wall clock when the median-time-past is in the past", func(t *testing.T) {
		server, _ := setupServer(t)
		require.NoError(t, server.blockAssembler.Start(t.Context()))

		bestHeader, _, err := server.blockchainClient.GetBestBlockHeader(t.Context())
		require.NoError(t, err)

		before := time.Now().Unix()
		got, err := server.blockAssembler.candidateTime(t.Context(), bestHeader)
		after := time.Now().Unix()

		require.NoError(t, err)
		require.GreaterOrEqual(t, got, before)
		require.LessOrEqual(t, got, after)
	})

	t.Run("floors at median-time-past+1 when the median is at or above the wall clock", func(t *testing.T) {
		server, _ := setupServer(t)
		require.NoError(t, server.blockAssembler.Start(t.Context()))

		// Stamp the last 11 blocks ~100 minutes into the future (within the
		// 2-hour future window peers accept), dragging the median above the
		// local clock — the scenario where the unfloored candidate violated
		// the median-time rule.
		baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
		tipHeader := buildFutureChain(t, t.Context(), server, baseTime)

		got, err := server.blockAssembler.candidateTime(t.Context(), tipHeader)
		require.NoError(t, err)

		medianTimePast := int64(baseTime + 5)
		require.Equal(t, medianTimePast+1, got)
	})
}

// TestMiningCandidateTimeFlooredAtMedianTimePast drives the public
// GetMiningCandidate path: with the best block on a future-stamped chain, the
// candidate's Time must come back strictly greater than the median-time-past
// of the previous 11 blocks. Before the floor it came back at the (lagging)
// wall clock, producing a block the node's own validation rejects.
func TestMiningCandidateTimeFlooredAtMedianTimePast(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, t.Context(), server, baseTime)
	server.blockAssembler.setBestBlockHeader(tipHeader, 11)

	candidate, _, err := server.blockAssembler.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.NotNil(t, candidate)

	medianTimePast := baseTime + 5
	require.Greater(t, candidate.Time, medianTimePast, "candidate time must be strictly greater than the median-time-past of the previous 11 blocks")
	require.Equal(t, medianTimePast+1, candidate.Time)
}

// TestEmptyBlockCandidateTimeFlooredAtMedianTimePast covers the empty-block
// path directly, which previously had the same unfloored wall-clock gap.
func TestEmptyBlockCandidateTimeFlooredAtMedianTimePast(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, t.Context(), server, baseTime)

	candidate, _, err := server.blockAssembler.generateEmptyBlockCandidate(t.Context(), tipHeader, 11)
	require.NoError(t, err)
	require.NotNil(t, candidate)

	medianTimePast := baseTime + 5
	require.Equal(t, medianTimePast+1, candidate.Time)
}
