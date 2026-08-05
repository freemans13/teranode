package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildFutureChain stores medianTimeBlocks (11) linked blocks on top of the
// current best block, stamped with strictly increasing timestamps starting at
// baseTime, and returns the new tip header. With timestamps baseTime..baseTime+10
// the median-time-past seen by a candidate on the tip is baseTime+5.
func buildFutureChain(t *testing.T, ctx context.Context, blockchainClient blockchain.ClientI, baseTime uint32) *model.BlockHeader {
	t.Helper()

	prevHeader, _, err := blockchainClient.GetBestBlockHeader(ctx)
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

		require.NoError(t, blockchainClient.AddBlock(ctx, &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}, ""))

		prevHeader = header
	}

	return prevHeader
}

// linkedHeaders builds count in-memory headers linked via HashPrevBlock with
// timestamps baseTime, baseTime+1, ... and returns them newest-first — the
// order GetBlockHeaders returns them in.
func linkedHeaders(t *testing.T, count int, baseTime uint32) []*model.BlockHeader {
	t.Helper()

	nbits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	prev := &chainhash.Hash{1}
	newestFirst := make([]*model.BlockHeader, count)

	for i := 0; i < count; i++ {
		header := &model.BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  prev,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      baseTime + uint32(i),
			Bits:           *nbits,
			Nonce:          uint32(i),
		}
		newestFirst[count-1-i] = header
		prev = header.Hash()
	}

	return newestFirst
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
		tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)

		got, err := server.blockAssembler.candidateTime(t.Context(), tipHeader)
		require.NoError(t, err)

		medianTimePast := int64(baseTime + 5)
		require.Equal(t, medianTimePast+1, got)
	})
}

// TestCandidateTimeFallback covers the defensive branches: when the batched
// GetBlockHeaders call errors or returns a run that is not anchored at the
// requested parent or not correctly linked (a reorg race in the store's
// batched lookup), candidateTime must recover via the hash-keyed parent-chain
// walk — and only error when both attempts fail.
func TestCandidateTimeFallback(t *testing.T) {
	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	headers := linkedHeaders(t, 11, baseTime)
	tip := headers[0]
	wantFloor := int64(baseTime+5) + 1

	stubWalk := func(mockClient *blockchain.Mock) {
		for _, h := range headers {
			mockClient.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
		}
		// the walk stops at depth 11 before dereferencing the oldest header's parent
	}

	newAssembler := func(mockClient *blockchain.Mock) *BlockAssembler {
		return &BlockAssembler{logger: ulogger.TestLogger{}, blockchainClient: mockClient}
	}

	t.Run("anchored batched run needs no fallback", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})

	t.Run("batched fetch error falls back to the hash-keyed walk", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
	})

	t.Run("mis-anchored batched run falls back to the hash-keyed walk", func(t *testing.T) {
		// The run is a correctly-linked chain anchored at the WRONG block with
		// different timestamps — what a reorg between the batched lookup's
		// height probe and its range scan produces (the winning fork's blocks
		// at the same heights). Its median differs from the true chain's, so
		// this test fails if the anchor guard is ever weakened: the floor
		// would come out of the wrong fork's timestamps.
		wrongFork := linkedHeaders(t, 11, baseTime+1000)

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(wrongFork, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNumberOfCalls(t, "GetBlockHeader", 11)
	})

	t.Run("broken link in the batched run falls back to the hash-keyed walk", func(t *testing.T) {
		// Replace one mid-run header with a differently-stamped stray so the
		// run's median differs from the true chain's — swapping two elements
		// would leave the (sorted) median identical and the link guard
		// unpinned under mutation.
		broken := make([]*model.BlockHeader, len(headers))
		copy(broken, headers)
		broken[6] = linkedHeaders(t, 1, baseTime+1000)[0]

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(broken, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNumberOfCalls(t, "GetBlockHeader", 11)
	})

	t.Run("nil header in the batched run falls back to the hash-keyed walk", func(t *testing.T) {
		withNil := make([]*model.BlockHeader, len(headers))
		copy(withNil, headers)
		withNil[7] = nil

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(withNil, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
	})

	t.Run("errors only when the batched fetch and the walk both fail", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)

		_, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.Error(t, err)
	})

	t.Run("walk stops cleanly at the chain start", func(t *testing.T) {
		// A 3-block chain whose oldest header points at the all-zero genesis
		// parent: the walk must return the short run, and the median over 3
		// headers is the middle timestamp.
		short := linkedHeadersFromGenesis(t, 3, baseTime)
		shortTip := short[0]

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)
		for _, h := range short {
			mockClient.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
		}

		got, err := newAssembler(mockClient).candidateTime(t.Context(), shortTip)
		require.NoError(t, err)
		require.Equal(t, int64(baseTime+1)+1, got)
	})
}

// linkedHeadersFromGenesis is linkedHeaders but the oldest header's parent is
// the all-zero hash, marking the start of the chain for walkParentChain.
func linkedHeadersFromGenesis(t *testing.T, count int, baseTime uint32) []*model.BlockHeader {
	t.Helper()

	nbits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	prev := &chainhash.Hash{}
	newestFirst := make([]*model.BlockHeader, count)

	for i := 0; i < count; i++ {
		header := &model.BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  prev,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      baseTime + uint32(i),
			Bits:           *nbits,
			Nonce:          uint32(i),
		}
		newestFirst[count-1-i] = header
		prev = header.Hash()
	}

	return newestFirst
}

// TestMiningCandidateTimeFlooredAtMedianTimePast drives the public
// GetMiningCandidate entry point with the best block on a future-stamped
// chain: the candidate's Time must come back strictly greater than the
// median-time-past of the previous 11 blocks. Before the floor it came back
// at the (lagging) wall clock, producing a block every peer rejects. This
// exercises whichever internal branch the assembler's state selects (busy or
// empty-block); the subtree-populated main path is covered by
// TestMiningCandidateMainPathTimeFloored below.
func TestMiningCandidateTimeFlooredAtMedianTimePast(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)
	server.blockAssembler.setBestBlockHeader(tipHeader, 11)

	candidate, _, err := server.blockAssembler.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.NotNil(t, candidate)

	medianTimePast := baseTime + 5
	require.Greater(t, candidate.Time, medianTimePast, "candidate time must be strictly greater than the median-time-past of the previous 11 blocks")
	require.Equal(t, medianTimePast+1, candidate.Time)
}

// TestMiningCandidateMainPathTimeFloored pins the floor on the main
// (subtree-populated) candidate path: the assembler holds transactions for
// the future-stamped tip, so GetMiningCandidate must return a candidate with
// those transactions AND the floored timestamp.
func TestMiningCandidateMainPathTimeFloored(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	ba := testItems.blockAssembler
	_, _, _ = setupBlockchainClient(t, testItems)

	ctx := t.Context()

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, ctx, ba.blockchainClient, baseTime)

	ba.setBestBlockHeader(tipHeader, 11)
	ba.subtreeProcessor.InitCurrentBlockHeader(tipHeader)

	for i := 0; i < 5; i++ {
		txHash := chainhash.HashH([]byte{byte(i), 0xBB})
		ba.AddTxBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(100), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{{}},
		)
	}

	medianTimePast := baseTime + 5

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		var err error
		candidate, _, err = ba.GetMiningCandidate(ctx)

		return err == nil && candidate != nil && candidate.NumTxs > 0
	}, 5*time.Second, 50*time.Millisecond, "expected a subtree-populated candidate for the future-stamped tip")

	require.Equal(t, medianTimePast+1, candidate.Time, "main-path candidate time must be floored at median-time-past+1")
}

// TestEmptyBlockCandidateTimeFlooredAtMedianTimePast covers the empty-block
// path directly, which previously had the same unfloored wall-clock gap.
func TestEmptyBlockCandidateTimeFlooredAtMedianTimePast(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)

	candidate, _, err := server.blockAssembler.generateEmptyBlockCandidate(t.Context(), tipHeader, 11)
	require.NoError(t, err)
	require.NotNil(t, candidate)

	medianTimePast := baseTime + 5
	require.Equal(t, medianTimePast+1, candidate.Time)
}
