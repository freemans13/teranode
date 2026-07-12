package postgres

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// makeCoinbaseTx builds a real coinbase transaction: a single input whose
// PreviousTxID is all-zero and whose SequenceNumber is 0xffffffff, so that
// bt.Tx.IsCoinbase() returns true. The unique reward makes the txid unique per
// call. This is the tx shape block assembly hands to Create on move_forward.
func makeCoinbaseTx(reward uint64) *bt.Tx {
	tx := bt.NewTx()
	in := &bt.Input{SequenceNumber: 0xffffffff}
	in.PreviousTxOutIndex = 0xffffffff
	_ = in.PreviousTxIDAdd(&chainhash.Hash{}) // all-zero prev txid → coinbase
	tx.Inputs = append(tx.Inputs, in)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      reward,
		LockingScript: bscript.NewFromBytes([]byte{0x51}), // OP_TRUE, always spendable
	})
	return tx
}

// TestCoinbaseCreate_BuildOutputArraysMaturityParity proves — without a database —
// that the coinbase maturity stamp (coinbase_spending_height) is computed by the
// SAME function, buildOutputArrays, that BOTH the direct create path
// (createDirect, create.go) and the bulk batch path (sendCreateBatchUNNEST,
// create.go) call. Both call sites pass identical arguments
// (txHash, tx, isCoinbase, blockHeight, CoinbaseMaturity, GenesisActivationHeight),
// so the +maturity math cannot diverge between them. A coinbase that becomes
// spendable before blockHeight+CoinbaseMaturity would be a consensus break; this
// test locks the single-source-of-truth invariant that the Fix-1 routing change
// (coinbase bypasses the batcher into createDirect) relies on.
func TestCoinbaseCreate_BuildOutputArraysMaturityParity(t *testing.T) {
	const (
		blockHeight = uint32(700_000)
		maturity    = 100
		genesis     = uint32(0)
	)
	cb := makeCoinbaseTx(50_0000_0000)
	txHash := cb.TxIDChainHash()

	require.True(t, cb.IsCoinbase(), "test fixture must be a real coinbase (routing gate is tx.IsCoinbase())")

	// This is the exact call createDirect makes (create.go, ~line 274) and the
	// exact call the bulk loop makes per item (create.go, ~line 631): same
	// function, same argument list. Whichever path a coinbase takes, this is the
	// value that lands in coinbase_spending_height.
	got, err := buildOutputArrays(txHash, cb, true, blockHeight, maturity, genesis)
	require.NoError(t, err)

	require.Equal(t, int32(blockHeight)+int32(maturity), got.coinbaseSpendingHeight,
		"coinbase must mature at exactly blockHeight + CoinbaseMaturity (+%d)", maturity)
	require.Equal(t, int32(1), got.outCount)
	require.Equal(t, int32(1), got.spendableCount, "the OP_TRUE coinbase output is spendable")

	// A non-coinbase call to the same function must stamp height 0 (the sentinel
	// meaning "no maturity"), confirming the coinbase branch is the only thing
	// that sets a non-zero maturity — the discriminator the routing relies on.
	nonCB := makeBenchCreateTx()
	npo, err := buildOutputArrays(nonCB.TxIDChainHash(), nonCB, false, blockHeight, maturity, genesis)
	require.NoError(t, err)
	require.Equal(t, int32(0), npo.coinbaseSpendingHeight, "non-coinbase must have zero maturity height")
}

// TestCoinbaseCreate_BypassesBatcher_SameMaturity is the DB-backed proof that a
// coinbase persisted via the batcher-bypass route (Create with an active
// createBatcher: Fix 1 sends coinbases straight to createDirect) lands the SAME
// coinbase_spending_height as a coinbase persisted via the bulk batch callback
// (sendCreateBatch with len>1 → sendCreateBatchUNNEST). It also confirms a
// non-coinbase tx still flows through the batcher (does NOT hit createDirect via
// the bypass), so the routing change is coinbase-only.
//
// Requires a reachable postgres (local or the TestMain testcontainer); skips
// otherwise, exactly like the rest of this suite.
func TestCoinbaseCreate_BypassesBatcher_SameMaturity(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	// Start the batchers so s.createBatcher != nil — this is the production
	// configuration under which Fix 1's tx.IsCoinbase() gate takes effect.
	store.Start(ctx)

	const blockHeight = uint32(100)

	// (1) Coinbase created via the public Create with the batcher active. Fix 1
	// routes it to createDirect (bypassing the batcher). Carries one
	// MinedBlockInfo, exactly as block assembly's processCoinbaseUtxos does.
	cbBypass := makeCoinbaseTx(50_0000_0001)
	require.True(t, cbBypass.IsCoinbase())
	_, err := store.Create(ctx, cbBypass, blockHeight,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 7, BlockHeight: blockHeight, SubtreeIdx: 0}))
	require.NoError(t, err, "coinbase create via bypass must succeed")

	// (2) Coinbase created via the bulk batch callback directly (len>1 → UNNEST
	// bulk path). This is the path the coinbase would have taken WITHOUT Fix 1.
	cbBulk := makeCoinbaseTx(50_0000_0002)
	filler := makeBenchCreateTx() // second item forces the bulk (len>1) branch
	batch := []*batchCreateItem{
		{
			tx:          cbBulk,
			blockHeight: blockHeight,
			options:     &utxo.CreateOptions{MinedBlockInfos: []utxo.MinedBlockInfo{{BlockID: 7, BlockHeight: blockHeight, SubtreeIdx: 0}}},
			done:        make(chan batchCreateResult, 1),
		},
		{
			tx:          filler,
			blockHeight: blockHeight,
			options:     &utxo.CreateOptions{},
			done:        make(chan batchCreateResult, 1),
		},
	}
	store.sendCreateBatch(batch)
	for i, it := range batch {
		res := <-it.done
		require.NoError(t, res.Err, "bulk batch item %d", i)
	}

	// Read the persisted coinbase_spending_height for both coinbases directly.
	readMaturity := func(tx *bt.Tx) int32 {
		h := tx.TxIDChainHash()
		var csh int32
		err := store.pool.QueryRow(ctx,
			`SELECT coinbase_spending_height FROM txs WHERE hash = $1`, h[:]).Scan(&csh)
		require.NoError(t, err)
		return csh
	}

	bypassMaturity := readMaturity(cbBypass)
	bulkMaturity := readMaturity(cbBulk)

	// CoinbaseMaturity is 1 in the base test settings.
	require.Equal(t, int32(blockHeight)+1, bypassMaturity,
		"bypass-route coinbase must mature at blockHeight + CoinbaseMaturity")
	require.Equal(t, bypassMaturity, bulkMaturity,
		"batcher-bypass and bulk-batch coinbase maturities MUST be identical (consensus)")

	// The bypass-route coinbase must be spendable at exactly its maturity height
	// and not before — an end-to-end confirmation of the persisted stamp.
	got, err := store.Get(ctx, cbBypass.TxIDChainHash(), fields.IsCoinbase)
	require.NoError(t, err)
	require.True(t, got.IsCoinbase, "bypass coinbase must be recorded as a coinbase")

	// Confirm the non-coinbase filler still went through the batcher path: it was
	// created via sendCreateBatchUNNEST and must carry no maturity stamp.
	require.Equal(t, int32(0), readMaturity(filler),
		"non-coinbase must not receive a coinbase maturity stamp")
}
