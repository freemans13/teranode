package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// feeTx builds a bt.Tx with the given inputs (parent hash + vout) and output
// satoshis. Scripts are set so the tx is realistic, but resolveQuickFees must
// rely only on satoshis.
func feeTx(inputs []parentOutpoint, outSats []uint64) *bt.Tx {
	tx := bt.NewTx()

	for _, in := range inputs {
		ph := in.hash
		i := &bt.Input{PreviousTxOutIndex: in.idx, UnlockingScript: &bscript.Script{}}
		_ = i.PreviousTxIDAdd(&ph)
		tx.Inputs = append(tx.Inputs, i)
	}

	for _, s := range outSats {
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: s, LockingScript: &bscript.Script{0x51}})
	}

	return tx
}

func putWrapper(t *testing.T, m *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], tx *bt.Tx) chainhash.Hash {
	t.Helper()
	h := *tx.TxIDChainHash()
	m.Set(h, &TxMapWrapper{Tx: tx})

	return h
}

// TestResolveQuickFees_SameBlockAndCache covers the two store-free sources: a
// same-block parent (resolved from txMap) and a cross-block parent (resolved
// from the satoshi cache). No store read should occur. Crucially the fees are
// non-zero — this is the regression guard for the old fee-0 shortcut that made
// checkBlockRewardAndFees reject fee-bearing blocks.
func TestResolveQuickFees_SameBlockAndCache(t *testing.T) {
	// Grandparent G: only its output satoshis matter; cached, not in the block.
	grandparentHash := chainhash.Hash{0xaa}
	g := feeTx([]parentOutpoint{{hash: grandparentHash, idx: 0}}, []uint64{31_000})

	cache, err := newSatoshiCache(8 * 1024 * 1024)
	require.NoError(t, err)
	cache.putTx(g) // G:0 = 31_000 available cross-block

	sm := &SyncManager{satoshiCache: cache}

	txMapData := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](4)

	// P spends G:0 (cross-block, cached), outputs 10_000 + 20_000 → fee 1_000.
	gHash := *g.TxIDChainHash()
	p := feeTx([]parentOutpoint{{hash: gHash, idx: 0}}, []uint64{10_000, 20_000})
	pHash := putWrapper(t, txMapData, p)

	// C1 spends P:0 (same-block) → fee 10_000 - 7_000 = 3_000.
	c1 := feeTx([]parentOutpoint{{hash: pHash, idx: 0}}, []uint64{7_000})
	c1Hash := putWrapper(t, txMapData, c1)

	// C2 spends P:1 (same-block) → fee 20_000 - 15_000 = 5_000.
	c2 := feeTx([]parentOutpoint{{hash: pHash, idx: 1}}, []uint64{15_000})
	c2Hash := putWrapper(t, txMapData, c2)

	fees, err := sm.resolveQuickFees(context.Background(), txMapData)
	require.NoError(t, err)

	require.Equal(t, uint64(1_000), fees[pHash], "P fee from cached grandparent satoshis")
	require.Equal(t, uint64(3_000), fees[c1Hash], "C1 fee from same-block parent P:0")
	require.Equal(t, uint64(5_000), fees[c2Hash], "C2 fee from same-block parent P:1")

	// Every fee is non-zero: the fee-0 regression must not reappear.
	for h, f := range fees {
		require.NotZero(t, f, "fee for %s must not be zero", h)
	}
}

// TestResolveQuickFees_ColdMissBatchedStoreRead verifies the cold path: a parent
// neither in the block nor the cache is fetched via a single batched store read,
// and the real block transaction is NOT mutated (stays non-extended).
func TestResolveQuickFees_ColdMissBatchedStoreRead(t *testing.T) {
	coldParent := chainhash.Hash{0xcd}

	mockStore := &utxo.MockUtxostore{}
	// The shell tx (one input per cold outpoint) is decorated in place; set its
	// satoshis and assert the real child tx was never passed to the store.
	mockStore.On("PreviousOutputsDecorate", mock.Anything, mock.MatchedBy(func(tx *bt.Tx) bool {
		return len(tx.Inputs) == 1 && tx.Inputs[0].PreviousTxIDChainHash().IsEqual(&coldParent)
	})).Run(func(args mock.Arguments) {
		shell := args.Get(1).(*bt.Tx)
		shell.Inputs[0].PreviousTxSatoshis = 9_000
	}).Return(nil)

	sm := &SyncManager{utxoStore: mockStore}

	txMapData := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](1)

	// Child spends the cold parent → fee 9_000 - 6_000 = 3_000.
	child := feeTx([]parentOutpoint{{hash: coldParent, idx: 0}}, []uint64{6_000})
	childHash := putWrapper(t, txMapData, child)

	fees, err := sm.resolveQuickFees(context.Background(), txMapData)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000), fees[childHash])

	// The real child tx must remain non-extended — only the shell was decorated.
	require.Zero(t, child.Inputs[0].PreviousTxSatoshis, "real tx must not be mutated by the cold read")
	require.Nil(t, child.Inputs[0].PreviousTxScript, "real tx input script must stay nil")

	mockStore.AssertExpectations(t)
}

// TestResolveQuickFees_RejectsOverspend ensures a tx whose outputs exceed its
// resolved inputs (would be a negative fee) is rejected rather than silently
// wrapping to a huge uint64.
func TestResolveQuickFees_RejectsOverspend(t *testing.T) {
	parentHash := chainhash.Hash{0xbe}
	parent := feeTx([]parentOutpoint{{hash: parentHash, idx: 0}}, []uint64{5_000})

	cache, err := newSatoshiCache(8 * 1024 * 1024)
	require.NoError(t, err)
	cache.putTx(parent)

	sm := &SyncManager{satoshiCache: cache}

	txMapData := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](1)

	pHash := *parent.TxIDChainHash()
	// child outputs 9_000 > parent's 5_000 input → invalid.
	child := feeTx([]parentOutpoint{{hash: pHash, idx: 0}}, []uint64{9_000})
	putWrapper(t, txMapData, child)

	_, err = sm.resolveQuickFees(context.Background(), txMapData)
	require.Error(t, err, "overspend must be rejected, not wrapped to a huge fee")
}
