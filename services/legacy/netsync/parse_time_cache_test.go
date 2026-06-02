package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// blockWithTxs builds a bsvutil.Block containing the given wire transactions.
func blockWithTxs(prev chainhash.Hash, txs ...*wire.MsgTx) *bsvutil.Block {
	merkle := chainhash.Hash{0xCC}
	hdr := wire.NewBlockHeader(1, &prev, &merkle, 0x1d00ffff, 0)
	mb := wire.NewMsgBlock(hdr)
	for _, tx := range txs {
		_ = mb.AddTransaction(tx)
	}
	return bsvutil.NewBlock(mb)
}

// txWithOutputs builds a wire.MsgTx with the given output satoshi values.
func txWithOutputs(sats ...int64) *wire.MsgTx {
	tx := wire.NewMsgTx(1)
	for _, s := range sats {
		tx.AddTxOut(wire.NewTxOut(s, []byte{0x51}))
	}
	return tx
}

// TestCacheBlockOutputSatoshis_PopulatesFromWireBlock verifies the consumer-side
// parse-time population: every output's satoshis become resolvable from the cache
// keyed by its outpoint, read straight from wire data without a bt.Tx conversion
// or a store touch.
func TestCacheBlockOutputSatoshis_PopulatesFromWireBlock(t *testing.T) {
	cache, err := newSatoshiCache(8 * 1024 * 1024)
	require.NoError(t, err)
	sm := &SyncManager{logger: ulogger.TestLogger{}, satoshiCache: cache}

	wtx := txWithOutputs(11_000, 22_000)
	block := blockWithTxs(chainhash.Hash{0xAA}, wtx)

	sm.cacheBlockOutputSatoshis(block)

	txHash := wtx.TxHash()
	var dst []byte

	s0, ok := cache.satoshis(&txHash, 0, &dst)
	require.True(t, ok)
	require.Equal(t, uint64(11_000), s0)

	s1, ok := cache.satoshis(&txHash, 1, &dst)
	require.True(t, ok)
	require.Equal(t, uint64(22_000), s1)
}

// TestCacheBlockOutputSatoshis_NilCacheSafe: with the cache disabled (nil), the
// consumer-side population is a no-op and must not panic.
func TestCacheBlockOutputSatoshis_NilCacheSafe(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}
	block := blockWithTxs(chainhash.Hash{0xAA}, txWithOutputs(1_000))
	require.NotPanics(t, func() { sm.cacheBlockOutputSatoshis(block) })
}

// TestResolveQuickFees_CrossBlockResolvedWithoutParentCreated is the create->fee
// dependency guard for the concurrent pipeline. A child block's fee for a tx
// spending a *previous block's* output must resolve from the parse-time cache
// alone — WITHOUT the parent's outputs existing in the store, because under
// concurrency the parent block's createUtxos may not have run yet. The store is
// wired with no expectations, so any read would fail the test.
func TestResolveQuickFees_CrossBlockResolvedWithoutParentCreated(t *testing.T) {
	cache, err := newSatoshiCache(8 * 1024 * 1024)
	require.NoError(t, err)

	store := &utxo.MockUtxostore{}

	sm := &SyncManager{logger: ulogger.TestLogger{}, satoshiCache: cache, utxoStore: store}

	// Parent block N: a tx with one 50_000 output. Populate the cache at parse time
	// (as the in-order consumer does) but never create it in the store.
	parentTx := txWithOutputs(50_000)
	parentBlock := blockWithTxs(chainhash.Hash{0xAA}, parentTx)
	sm.cacheBlockOutputSatoshis(parentBlock)

	// Child block N+1: spends parent's output, output 47_000 → fee 3_000.
	parentHash := parentTx.TxHash()
	child := feeTx([]parentOutpoint{{hash: parentHash, idx: 0}}, []uint64{47_000})

	txMapData := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](1)
	childHash := putWrapper(t, txMapData, child)

	fees, err := sm.resolveQuickFees(context.Background(), txMapData)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000), fees[childHash], "fee resolved from parse-time cache, parent never created")
	store.AssertNotCalled(t, "PreviousOutputsDecorate", mock.Anything, mock.Anything)
}
