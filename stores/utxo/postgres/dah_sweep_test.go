package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestDAHSchemaObjectsExist(t *testing.T) {
	store, ctx := setupTestStore(t)

	for _, q := range []struct{ name, sql string }{
		{"spends.spent_at_height", `SELECT 1 FROM information_schema.columns WHERE table_name='spends_p00' AND column_name='spent_at_height'`},
		{"txs.mined_at_height", `SELECT 1 FROM information_schema.columns WHERE table_name='txs_p00' AND column_name='mined_at_height'`},
		{"brin spends", `SELECT 1 FROM pg_indexes WHERE indexname='spends_p00_spent_at_height_brin'`},
		{"brin txs", `SELECT 1 FROM pg_indexes WHERE indexname='txs_p00_mined_at_height_brin'`},
		{"dah_watermark table", `SELECT 1 FROM information_schema.tables WHERE table_name='dah_watermark'`},
		{"dah_watermark seed row", `SELECT last_swept_height FROM dah_watermark WHERE id = 1`},
	} {
		var ok int
		err := store.pool.QueryRow(ctx, q.sql).Scan(&ok)
		require.NoError(t, err, "missing schema object: %s", q.name)
	}
}

// newMinedSingleOutputTx creates a transaction and stores it pre-mined via
// store.Create with utxo.WithMinedBlockInfo at the given height. The tx is the
// canonical testExtendedTx, which has exactly two spendable P2PKH outputs (no
// OP_RETURN / unspendable outputs). Paired with spendAllOutputs, the parent is
// left genuinely fully spent (count(spends) == count(outputs)), which is what
// Task 4's Worker 2 sweep relies on to stamp delete_at_height.
func newMinedSingleOutputTx(t *testing.T, store *Store, height uint32) *bt.Tx {
	t.Helper()
	ctx := context.Background()
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        uint32(height),
		BlockHeight:    height,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := store.Create(ctx, tx, height, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	return tx
}

// spendAllOutputs builds a child tx that spends every spendable output of
// parentTx, creates it in the store, then calls store.Spend at spendHeight.
// It self-checks that the parent is genuinely fully spent afterwards so that
// downstream DAH-sweep tests have a correct foundation.
func spendAllOutputs(t *testing.T, store *Store, parentTx *bt.Tx, spendHeight uint32) {
	t.Helper()
	ctx := context.Background()

	vouts := make([]uint32, 0, len(parentTx.Outputs))
	for i, out := range parentTx.Outputs {
		if out == nil {
			continue
		}
		vouts = append(vouts, uint32(i))
	}
	require.NotEmpty(t, vouts, "parent tx must have at least one spendable output")

	child := getSpendingTx(t, parentTx, vouts...)
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(t, err)

	// Self-check: the parent must now be fully spent.
	parentHash := parentTx.TxIDChainHash()[:]
	var spendCount, outputCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1`, parentHash).Scan(&spendCount))
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM outputs WHERE tx_hash=$1`, parentHash).Scan(&outputCount))
	require.Equal(t, outputCount, spendCount,
		"parent must be fully spent (count(spends) == count(outputs))")
}

func TestSpendTagsHeightAndDoesNotStampInline(t *testing.T) {
	store, ctx := setupTestStore(t)

	parent := newMinedSingleOutputTx(t, store, 100)
	spendAllOutputs(t, store, parent, 101)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.Nil(t, dah, "spend must not stamp delete_at_height inline")

	var h *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_at_height FROM spends WHERE prev_tx_hash=$1`, parent.TxIDChainHash()[:]).Scan(&h))
	require.NotNil(t, h)
	require.Equal(t, int64(101), *h)
}
