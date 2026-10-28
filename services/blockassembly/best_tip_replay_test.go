package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// recordedMove is one Reorg call on the subtree processor: a catch-up has no moveBack.
type recordedMove struct {
	back    []string
	forward []string
}

// recordMoves replaces the subtree processor with a mock that accepts every Reorg and
// MoveForwardBlock and records them, in order, as moves.
func recordMoves(t *testing.T, items *baTestItems) *[]recordedMove {
	t.Helper()

	moves := &[]recordedMove{}

	hashes := func(blocks []*model.Block) []string {
		out := make([]string, len(blocks))
		for i, b := range blocks {
			out[i] = b.Header.Hash().String()
		}

		return out
	}

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Reorg", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*moves = append(*moves, recordedMove{
			back:    hashes(args.Get(0).([]*model.Block)),
			forward: hashes(args.Get(1).([]*model.Block)),
		})
	})
	mockStp.On("MoveForwardBlock", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*moves = append(*moves, recordedMove{forward: hashes([]*model.Block{args.Get(0).(*model.Block)})})
	})
	injectMockStp(t, items, mockStp)

	return moves
}

func headerHashes(headers ...*model.BlockHeader) []string {
	out := make([]string, len(headers))
	for i, h := range headers {
		out[i] = h.Hash().String()
	}

	return out
}

// startReplayAt puts block assembly on tip with the replay position at the store's latest
// number, as if it had applied every change so far.
func startReplayAt(t *testing.T, items *baTestItems, tip *model.BlockHeader, height uint32) uint64 {
	t.Helper()

	items.blockAssembler.setBestBlockHeader(tip, height)

	seq, err := items.blockchainClient.GetBestTipSeq(t.Context())
	require.NoError(t, err)

	items.blockAssembler.bestTipSeq.Store(seq)
	items.blockAssembler.bestTipSeqKnown.Store(true)

	return seq
}

func requireBlockAssemblyTip(t *testing.T, items *baTestItems, want *model.BlockHeader) {
	t.Helper()

	tip, _ := items.blockAssembler.CurrentBlock()
	require.Equal(t, want.Hash().String(), tip.Hash().String())
}

// TestReplayBestTipChanges_FlipFlop is the scenario of issue 2063: branch B becomes the best
// chain and loses it to branch A again before block assembly looks. Following the best header
// alone catches up from A1 to A3 and never touches B. The replay moves forward to B2 and then
// reorgs back to A3, so B's transactions go forward and back through the subtree processor.
func TestReplayBestTipChanges_FlipFlop(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 1000)[0]
	addChain(t, items, []*model.BlockHeader{a1})

	startSeq := startReplayAt(t, items, a1, 1)
	moves := recordMoves(t, items)

	// B1 ties A1 and loses the tiebreak; B2 makes B the best chain.
	b := buildChain(genesis, 2, 2000)
	addChain(t, items, b)

	// A2 ties B2 and loses; A3 makes A the best chain again.
	a := buildChain(a1, 2, 3000)
	addChain(t, items, a)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Equal(t, []recordedMove{
		{back: headerHashes(a1), forward: headerHashes(b[0], b[1])},
		{back: headerHashes(b[1], b[0]), forward: headerHashes(a1, a[0], a[1])},
	}, *moves)

	requireBlockAssemblyTip(t, items, a[1])

	latest, err := items.blockchainClient.GetBestTipSeq(t.Context())
	require.NoError(t, err)
	require.Equal(t, startSeq+2, latest, "B2 and A3 are the two tip changes")
	require.Equal(t, latest, items.blockAssembler.bestTipSeq.Load())

	saved, err := items.blockchainClient.GetState(t.Context(), BestTipSeqStateKey)
	require.NoError(t, err)

	savedSeq, err := DecodeBestTipSeq(saved)
	require.NoError(t, err)
	require.Equal(t, latest, savedSeq, "the position is persisted for a restart")
}

// TestReplayBestTipChanges_ExtensionsCollapse checks that a run of extensions is one catch-up,
// not one move per block.
func TestReplayBestTipChanges_ExtensionsCollapse(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	startReplayAt(t, items, genesis, 0)
	moves := recordMoves(t, items)

	chain := buildChain(genesis, 100, 4000)
	addChain(t, items, chain)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Len(t, *moves, 1)
	require.Empty(t, (*moves)[0].back)
	require.Len(t, (*moves)[0].forward, 100)
	requireBlockAssemblyTip(t, items, chain[99])
}

// TestReplayBestTipChanges_OwnLineageIsANoOp checks that a change block assembly already passed,
// for example through a reset that went straight to a later tip, moves nothing back, and that
// the position still advances past it.
func TestReplayBestTipChanges_OwnLineageIsANoOp(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	startSeq := startReplayAt(t, items, genesis, 0)

	chain := buildChain(genesis, 3, 5000)
	addChain(t, items, chain)

	// Block assembly is already on the latest block, but its position is before all three.
	items.blockAssembler.setBestBlockHeader(chain[2], 3)
	moves := recordMoves(t, items)

	// The oldest change, two blocks below the tip, goes through the ancestry check.
	_, meta, err := items.blockchainClient.GetBlockHeader(t.Context(), chain[0].Hash())
	require.NoError(t, err)
	require.NoError(t, items.blockAssembler.applyBestTipChange(t.Context(), chain[0], meta, false))
	require.Empty(t, *moves)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Empty(t, *moves)
	requireBlockAssemblyTip(t, items, chain[2])
	require.Equal(t, startSeq+3, items.blockAssembler.bestTipSeq.Load())
}

// TestReplayBestTipChanges_LatestChangeOnOwnChainMovesBack checks that when the latest change is
// an ancestor of block assembly's tip, the best tip moved back to it and block assembly follows.
// Invalidating block assembly's tip numbers its parent as the latest change.
func TestReplayBestTipChanges_LatestChangeOnOwnChainMovesBack(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 3, 5500)
	addChain(t, items, chain)

	startSeq := startReplayAt(t, items, chain[2], 3)
	moves := recordMoves(t, items)

	// A reorg off an invalid block is followed by a reset, which the mock lets succeed.
	mockStp := items.blockAssembler.subtreeProcessor.(*subtreeprocessor.MockSubtreeProcessor)
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)

	resets := 0

	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{}).Run(func(mock.Arguments) { resets++ })

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), chain[2].Hash())
	require.NoError(t, err)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Equal(t, []recordedMove{{back: headerHashes(chain[2]), forward: []string{}}}, *moves)
	require.Equal(t, 1, resets, "moving off an invalid block resets block assembly")
	requireBlockAssemblyTip(t, items, chain[1])
	require.Equal(t, startSeq+1, items.blockAssembler.bestTipSeq.Load())
}

// TestReplayBestTipChanges_NoOpWhenCaughtUp checks a wake-up with no new change moves nothing.
func TestReplayBestTipChanges_NoOpWhenCaughtUp(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 2, 6000)
	addChain(t, items, chain)

	seq := startReplayAt(t, items, chain[1], 2)
	moves := recordMoves(t, items)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Empty(t, *moves)
	require.Equal(t, seq, items.blockAssembler.bestTipSeq.Load())
}

// TestReplayBestTipChanges_InvalidNeverAppliedResets covers a branch that became the best tip and
// was invalidated before block assembly applied it. Its block-only transactions reach the pool
// only through a reset, which the replay runs once block validation has processed the block.
func TestReplayBestTipChanges_InvalidNeverAppliedResets(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 7000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)

	b := buildChain(genesis, 2, 8000)
	addChain(t, items, b)

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), b[0].Hash())
	require.NoError(t, err)

	// Block validation has processed the invalidation.
	require.NoError(t, items.blockchainClient.SetBlockMinedSet(t.Context(), b[1].Hash()))

	_, bMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), b[1].Hash())
	require.NoError(t, err)
	require.True(t, bMeta.Invalid)

	err = items.blockAssembler.applyBestTipChange(t.Context(), b[1], bMeta, true)
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset)
	requireBlockAssemblyTip(t, items, a1)
}

// TestReplayBestTipChanges_InvalidOnOwnChainIsLeftToTheNextChange checks that an invalid block
// block assembly's chain contains is not reset for: the change that follows its invalidation
// moves block assembly off it through the reorg's reset rule.
func TestReplayBestTipChanges_InvalidOnOwnChainIsLeftToTheNextChange(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 2, 9000)
	addChain(t, items, chain)
	startReplayAt(t, items, chain[1], 2)

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), chain[0].Hash())
	require.NoError(t, err)

	_, meta, err := items.blockchainClient.GetBlockHeader(t.Context(), chain[0].Hash())
	require.NoError(t, err)

	moves := recordMoves(t, items)

	require.NoError(t, items.blockAssembler.applyBestTipChange(t.Context(), chain[0], meta, false))
	require.Empty(t, *moves)
	requireBlockAssemblyTip(t, items, chain[1])
}

// TestReplayBestTipChanges_RestartResumesFromPersistedPosition checks that changes made while
// block assembly was down are replayed from the persisted position.
func TestReplayBestTipChanges_RestartResumesFromPersistedPosition(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 10000)[0]
	addChain(t, items, []*model.BlockHeader{a1})

	seq := startReplayAt(t, items, a1, 1)
	items.blockAssembler.bestTipSeqSaved.Store(0)
	items.blockAssembler.saveBestTipSeq(t.Context())

	// The flip happens while block assembly is down.
	b := buildChain(genesis, 2, 11000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 12000)
	addChain(t, items, a)

	// A restart reads the position back.
	items.blockAssembler.bestTipSeq.Store(0)
	items.blockAssembler.bestTipSeqKnown.Store(false)
	items.blockAssembler.loadBestTipSeq(t.Context())
	require.True(t, items.blockAssembler.bestTipSeqKnown.Load())
	require.Equal(t, seq, items.blockAssembler.bestTipSeq.Load())

	moves := recordMoves(t, items)
	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Len(t, *moves, 2, "the downtime's flip is replayed")
	requireBlockAssemblyTip(t, items, a[1])
}

// TestReplayBestTipChanges_MockWithoutNumbersFollowsBestHeader checks block assembly against a
// blockchain client that does not number its tip changes: it follows the best header as before.
func TestReplayBestTipChanges_MockWithoutNumbersFollowsBestHeader(t *testing.T) {
	m := &blockchain.Mock{}

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}}

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.False(t, ba.bestTipSeqKnown.Load())
}

// TestReplayBestTipChanges_RepeatedFailureFollowsBestHeader checks that a replay that keeps
// failing gives way to the best-header path after maxBestTipReplayFailures passes.
func TestReplayBestTipChanges_RepeatedFailureFollowsBestHeader(t *testing.T) {
	initPrometheusMetrics()

	m := &blockchain.Mock{}
	m.On("GetBestTipChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader(nil), []*model.BlockHeaderMeta(nil), []uint64(nil), errors.NewServiceError("down"))
	m.On("GetBestTipSeq", mock.Anything).Return(uint64(42), nil)

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}, settings: createTestSettings(t)}
	ba.bestTipSeqKnown.Store(true)
	ba.setBestBlockHeader(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}, 10)

	for i := 1; i < maxBestTipReplayFailures; i++ {
		require.False(t, ba.followBestTipChanges(t.Context()), "pass %d retries", i)
	}

	m.On("SetState", mock.Anything, BestTipSeqStateKey, mock.Anything).Return(nil)

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.Equal(t, uint64(42), ba.bestTipSeq.Load(), "the position jumps to the latest change")
}

// TestReplayBestTipChanges_ProbeIsRateLimited checks that against a blockchain service without
// the numbers, block assembly asks again only after bestTipSeqProbeInterval, so a mixed-version
// deploy does not cost an RPC and a warning on every wake-up.
func TestReplayBestTipChanges_ProbeIsRateLimited(t *testing.T) {
	m := &blockchain.Mock{}
	m.On("GetBestTipSeq", mock.Anything).Return(uint64(0), errors.NewServiceUnavailableError("not implemented"))

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}}

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.True(t, ba.followBestTipChanges(t.Context()))
	m.AssertNumberOfCalls(t, "GetBestTipSeq", 1)
	require.True(t, ba.bestTipSeqUnsupportedLogged.Load())

	ba.lastBestTipSeqProbe.Store(time.Now().Add(-bestTipSeqProbeInterval).UnixNano())
	require.True(t, ba.followBestTipChanges(t.Context()))
	m.AssertNumberOfCalls(t, "GetBestTipSeq", 2)
}

// TestLoadBestTipSeq_NoPersistedPosition checks the start without a persisted position: on
// the best tip, block assembly starts at the latest change, so it is not moved through the
// stale branches recorded before it; off the best tip, it replays the recent changes from 0.
func TestLoadBestTipSeq_NoPersistedPosition(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 13000)[0]
	addChain(t, items, []*model.BlockHeader{a1})

	// A stale branch that was the best chain for a while.
	b := buildChain(genesis, 2, 14000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 15000)
	addChain(t, items, a)

	latest, err := items.blockchainClient.GetBestTipSeq(t.Context())
	require.NoError(t, err)
	require.NotZero(t, latest)

	ba := items.blockAssembler

	ba.setBestBlockHeader(a[1], 3)
	ba.bestTipSeqKnown.Store(false)
	ba.loadBestTipSeq(t.Context())
	require.True(t, ba.bestTipSeqKnown.Load())
	require.Equal(t, latest, ba.bestTipSeq.Load(), "on the best tip there is nothing to replay")

	moves := recordMoves(t, items)
	ba.processNewBlockAnnouncement(t.Context())
	require.Empty(t, *moves, "the stale branch is not replayed")

	ba.setBestBlockHeader(a1, 1)
	ba.bestTipSeqKnown.Store(false)
	ba.loadBestTipSeq(t.Context())
	require.True(t, ba.bestTipSeqKnown.Load())
	require.Equal(t, uint64(0), ba.bestTipSeq.Load(), "off the best tip the recent changes are replayed")
}

// TestIdleReconcileNeeded checks that the idle tick starts a replay only when the tip moved:
// a check that finds nothing keeps block assembly out of StateReconciling, where a mining
// candidate is an empty one.
func TestIdleReconcileNeeded(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 2, 16000)
	addChain(t, items, chain[:1])

	ba := items.blockAssembler
	startReplayAt(t, items, chain[0], 1)

	ba.lastBestTipReplay.Store(0)
	require.False(t, ba.idleReconcileNeeded(t.Context()), "nothing moved")
	require.NotZero(t, ba.lastBestTipReplay.Load(), "the check waits a full interval before the next one")

	// A block whose notification was lost.
	addChain(t, items, chain[1:])
	require.True(t, ba.idleReconcileNeeded(t.Context()), "a new change of the best tip")

	// Against a blockchain service without the numbers, the best header decides.
	ba.bestTipSeqKnown.Store(false)
	require.True(t, ba.idleReconcileNeeded(t.Context()), "block assembly is behind the best header")

	ba.setBestBlockHeader(chain[1], 2)
	require.False(t, ba.idleReconcileNeeded(t.Context()), "block assembly is on the best header")
}

func TestReconcileDue(t *testing.T) {
	ba := &BlockAssembler{settings: createTestSettings(t)}
	ba.settings.BlockAssembly.ReconcileInterval = 30 * time.Second

	now := time.Now()
	ba.lastBestTipReplay.Store(now.Add(-29 * time.Second).UnixNano())
	require.False(t, ba.reconcileDue(now))

	ba.lastBestTipReplay.Store(now.Add(-30 * time.Second).UnixNano())
	require.True(t, ba.reconcileDue(now))

	ba.settings.BlockAssembly.ReconcileInterval = 0
	require.False(t, ba.reconcileDue(now), "0 turns the timer off")
}

func TestBestTipFloor(t *testing.T) {
	ba := &BlockAssembler{settings: createTestSettings(t)}
	retention := ba.settings.GetUtxoStoreBlockHeightRetention()
	require.NotZero(t, retention)

	require.Equal(t, uint32(0), ba.bestTipFloor(retention))
	require.Equal(t, uint32(10), ba.bestTipFloor(retention+10))
}

func TestBestTipSeqEncoding(t *testing.T) {
	seq, err := DecodeBestTipSeq(EncodeBestTipSeq(1234567))
	require.NoError(t, err)
	require.Equal(t, uint64(1234567), seq)

	_, err = DecodeBestTipSeq([]byte{1, 2, 3})
	require.Error(t, err)
}
