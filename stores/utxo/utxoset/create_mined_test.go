package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// minedRows counts the membership rows a transaction holds, across every live window.
func minedRows(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_mined WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n
}

// coinFacts reads the block facts off the transaction's first coin.
func coinFacts(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) (minedHeight, blockID int32) {
	t.Helper()

	lo, hi := Pack(hashBytes(tx), 0), Pack(hashBytes(tx), ^uint32(0))
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT mined_height, block_id FROM utxo
		 WHERE leaf = $1 AND ukey >= $2 AND ukey <= $3 AND txid = $4 ORDER BY ukey LIMIT 1`,
		LeafFor(hashBytes(tx)), lo, hi, hashBytes(tx)).Scan(&minedHeight, &blockID))

	return minedHeight, blockID
}

// TestBlockPathCreateWritesMembershipAndCoinFactsAndNoIdentityRow is the design in one test:
// a create carrying mined-block information writes a membership row and coins that know their
// block, and no identity row.
func TestBlockPathCreateWritesMembershipAndCoinFactsAndNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3, OnLongestChain: true}))
	require.NoError(t, err)

	require.False(t, identExists(t, s, ctx, tx), "a mined transaction has no identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(700_100), h)
	require.Equal(t, int32(42), b)
}

// TestMempoolCreateStillClaimsOnTheIdentityTable pins that stage 1 leaves the tip alone.
func TestMempoolCreateStillClaimsOnTheIdentityTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	h, b := coinFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "unconfirmed sentinel")
	require.Equal(t, int32(0), b)
}

// TestBlockPathCreateIsIdempotentForTheSameBlock: a re-applied block after a crash hits the
// membership key and gets ErrTxExists, writing no second coin.
func TestBlockPathCreateIsIdempotentForTheSameBlock(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	info := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})

	_, err := s.Create(ctx, tx, 700_100, info)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, info)
	require.True(t, errors.Is(err, errors.ErrTxExists))

	require.Equal(t, 1, coinCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesTheSameHeightUnderAnotherBlockId: block-id reuse failed on a
// retry, or a stale sibling block at the same height. The same-partition probe on
// (txid, height) refuses it; the caller's ErrTxExists branch stamps instead.
func TestBlockPathCreateRefusesTheSameHeightUnderAnotherBlockId(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesATransactionThatStillHasACoin is SV Node's own duplicate check
// and what catches the two historic duplicate coinbases: a re-offer at any height of a
// transaction with a live coin creates nothing.
func TestBlockPathCreateRefusesATransactionThatStillHasACoin(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// Its window retires and is dropped; the coin stays because nobody spent it.
	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	_, err = s.Create(ctx, tx, 5_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 5_000, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists), "a live coin proves the transaction exists")
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
}

// TestBlockPathCreateRefusesAMempoolStray: the same transaction already claimed on the
// identity table (a mempool arrival) must answer ErrTxExists to the block path, so the
// caller stamps it rather than creating its coins twice.
func TestBlockPathCreateRefusesAMempoolStray(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
}
