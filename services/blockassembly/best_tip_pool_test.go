package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func replayPoolTx(parent *chainhash.Hash, amount uint64) *bt.Tx {
	tx := bt.NewTx()
	input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: amount + 1, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x51})}
	_ = input.PreviousTxIDAdd(parent)
	tx.Inputs = []*bt.Input{input}
	tx.Outputs = []*bt.Output{{Satoshis: amount, LockingScript: bscript.NewFromBytes([]byte{0x51})}}
	return tx
}

func TestReplayBestTipChanges_ResetRecoversParentBeforeChild(t *testing.T) {
	t.Run("normal", func(t *testing.T) { testReplayResetPool(t, false, testCoinbaseMaturity) })
	t.Run("retry_after_target_finalized", func(t *testing.T) { testReplayResetPool(t, true, testCoinbaseMaturity) })
	// With maturity 1 the move from A1 to B2 would take A1 back although A3 builds on it,
	// and a spend of A1's coinbase could already be confirmed. The replay restamps B
	// where it lies instead, and the parent still reaches the pool ahead of its child.
	t.Run("restamped_below_coinbase_maturity", func(t *testing.T) { testReplayResetPool(t, false, 1) })
}

func testReplayResetPool(t *testing.T, lateFailure bool, coinbaseMaturity uint16) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.GlobalBlockHeightRetention = 2
		s.UtxoStore.BlockHeightRetentionAdjustment = 0
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	}, withCoinbaseMaturity(coinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(t.Context())
	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 19000)[0]
	addBlockWithMinedSet(t.Context(), t, items, a1)
	startReplayAt(t, items, a1, 1)
	stp.InitCurrentBlockHeader(a1)
	require.NoError(t, items.utxoStore.SetBlockHeight(3))
	funding := replayPoolTx(&chainhash.Hash{}, 100)
	parent := replayPoolTx(funding.TxIDChainHash(), 99)
	child := replayPoolTx(parent.TxIDChainHash(), 98)
	for _, tx := range []*bt.Tx{funding, parent, child} {
		_, err = items.utxoStore.(*utxosql.Store).Create(t.Context(), tx, 1)
		require.NoError(t, err)
	}
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(t.Context(), []chainhash.Hash{*funding.TxIDChainHash(), *parent.TxIDChainHash()}, true))
	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*parent.TxIDChainHash(), 1, uint64(len(parent.Bytes()))))
	bytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(t.Context(), tree.RootHash()[:], fileformat.FileTypeSubtree, bytes))
	b := buildChain(genesis, 2, 20000)
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(t.Context(), &model.Block{Header: b[0], CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	addBlockWithMinedSet(t.Context(), t, items, b[1])
	_, genMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), genesis.Hash())
	require.NoError(t, err)
	_, bMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), b[0].Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0})
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{parent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: bMeta.ID, BlockHeight: 1})
	require.NoError(t, err)

	a := buildChain(a1, 2, 21000)
	for _, h := range a {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	if lateFailure {
		failing := &iteratorDownStore{Store: items.utxoStore}
		failing.iteratorDown.Store(true)
		ba.utxoStore = failing
		seq := ba.bestTipSeq.Load()
		_, err = ba.replayBestTipChanges(t.Context())
		require.Error(t, err)
		requireBlockAssemblyTip(t, items, b[1])
		// One pass short of giving the history up: the retries keep the position.
		for i := 1; i < maxBestTipReplayFailures; i++ {
			require.False(t, ba.followBestTipChanges(t.Context()))
			require.Equal(t, seq, ba.bestTipSeq.Load(), "finalized target is not a successful reload")
		}
		failing.iteratorDown.Store(false)
	}
	_, err = ba.replayBestTipChanges(t.Context())
	require.NoError(t, err)
	requireBlockAssemblyTip(t, items, a[1])
	current := ba.subtreeProcessor.GetCurrentSubtree()
	var hashes []chainhash.Hash
	for _, node := range current.Nodes {
		if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
			hashes = append(hashes, node.Hash)
		}
	}
	require.Equal(t, []chainhash.Hash{*parent.TxIDChainHash(), *child.TxIDChainHash()}, hashes, "reset must recover block-only parent before its child")
	data, err := items.utxoStore.Get(t.Context(), parent.TxIDChainHash())
	require.NoError(t, err)
	require.NotZero(t, data.UnminedSince, "orphaned parent must survive restart/pruning")
}

// TestBranchOnlyTxs checks which transactions of a stale branch a restamp marks: one that a block
// of the best chain also contains stays mined, one only the stale branch contains is marked, and
// one the UTXO store has pruned is left out.
func TestBranchOnlyTxs(t *testing.T) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)
	a := buildChain(genesis, 2, 32000)
	b := buildChain(genesis, 1, 33000)
	for _, h := range append(a, b...) {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	_, aMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), a[0].Hash())
	require.NoError(t, err)
	_, bMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), b[0].Hash())
	require.NoError(t, err)

	both := replayPoolTx(&chainhash.Hash{}, 100)
	staleOnly := replayPoolTx(&chainhash.Hash{}, 99)
	pruned := replayPoolTx(&chainhash.Hash{}, 98)
	for _, tx := range []*bt.Tx{both, staleOnly} {
		_, err = items.utxoStore.(*utxosql.Store).Create(t.Context(), tx, 1)
		require.NoError(t, err)
	}
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{both.TxIDChainHash(), staleOnly.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: bMeta.ID, BlockHeight: 1})
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{both.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: aMeta.ID, BlockHeight: 1})
	require.NoError(t, err)

	got, err := items.blockAssembler.branchOnlyTxs(t.Context(),
		[]chainhash.Hash{*both.TxIDChainHash(), *staleOnly.TxIDChainHash(), *pruned.TxIDChainHash()},
		map[uint32]struct{}{bMeta.ID: {}}, a[1])
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*staleOnly.TxIDChainHash()}, got)
}
