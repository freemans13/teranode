package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestMarkOnLongestChainClearsTheMempoolMarker is the repair block assembly runs at startup.
//
// It finds transactions that carry main-chain block membership while still marked as waiting
// to be mined, and this call is how it fixes them. Until it existed the node could not start
// at all once any such transaction was in the store.
func TestMarkOnLongestChainClearsTheMempoolMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 4)

	// Stored but not named, so a statement matching on the partition key alone would be caught.
	bystander := mkStoredTxs(t, s, 700_000, 8_000, 1)[0]

	hs := make([]chainhash.Hash, 0, len(txs))
	for _, h := range txHashes(txs) {
		hs = append(hs, *h)
	}

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, hs, true))

	for _, tx := range txs {
		h := tx.TxIDChainHash()
		require.Nil(t, readIdent(t, s, ctx, h[:]).offChainSince,
			"a transaction on the longest chain is no longer waiting to be mined")
	}

	bh := bystander.TxIDChainHash()
	require.NotNil(t, readIdent(t, s, ctx, bh[:]).offChainSince,
		"a transaction the call did not name must keep its marker")
}

// TestMarkOffLongestChainGivesAFreshClock is the reverse, and it pins the same rule the
// un-mine path follows: the clock comes from the CURRENT tip, not from the height the
// transaction was created at.
func TestMarkOffLongestChainGivesAFreshClock(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 100, 1_000, 3)

	hs := make([]chainhash.Hash, 0, len(txs))
	for _, h := range txHashes(txs) {
		hs = append(hs, *h)
	}

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, hs, true))
	require.NoError(t, s.SetBlockHeight(5_000))
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, hs, false))

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		r := readIdent(t, s, ctx, h[:])
		require.NotNil(t, r.offChainSince, "off the longest chain means waiting again")
		require.Equal(t, int32(5_000), *r.offChainSince,
			"with a clock from the current tip, not from created_height")
	}
}

// TestMarkOnLongestChainReportsATransactionItDoesNotHold. The sql store returns an error
// rather than a silent no-op, because the caller is repairing an inconsistency and a
// silently skipped row leaves the inconsistency in place.
func TestMarkOnLongestChainReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkStoredTxs(t, s, 700_000, 1_000, 1)[0]
	missing := mkTx(t, 1, 9_999)

	err := s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*known.TxIDChainHash(), *missing.TxIDChainHash()}, true)

	require.Error(t, err, "a hash the store does not hold must fail loudly")
	require.Contains(t, err.Error(), missing.TxIDChainHash().String())

	// The transactions it DOES hold are still repaired, so a partial input does not
	// strand the rest.
	kh := known.TxIDChainHash()
	require.Nil(t, readIdent(t, s, ctx, kh[:]).offChainSince)
}

// TestMarkOnLongestChainOnAnEmptyListIsANoOp, which is how block assembly calls it on the
// ordinary path where nothing needs repairing.
func TestMarkOnLongestChainOnAnEmptyListIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, nil, true))
	require.NoError(t, errors.Join())
}
