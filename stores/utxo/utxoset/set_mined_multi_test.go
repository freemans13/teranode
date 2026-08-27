package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// mkStoredTxs creates n distinct transactions and stores them. base separates one call's
// transactions from another's: the satoshi value is what makes them distinct, so two calls
// sharing a base would build the same transactions and the second Create would report them
// as already held.
func mkStoredTxs(t *testing.T, s *Store, height uint32, base uint64, n int) []*bt.Tx {
	t.Helper()

	txs := make([]*bt.Tx, 0, n)

	for i := 0; i < n; i++ {
		tx := mkTx(t, 1, base+uint64(i))

		_, err := s.Create(t.Context(), tx, height)
		require.NoError(t, err)

		txs = append(txs, tx)
	}

	return txs
}

func txHashes(txs []*bt.Tx) []*chainhash.Hash {
	out := make([]*chainhash.Hash, 0, len(txs))
	for _, tx := range txs {
		out = append(out, tx.TxIDChainHash())
	}

	return out
}

// TestSetMinedMultiStampsEveryTransactionInOneCall is the case a single statement built
// around array parameters can get wrong where a statement per transaction cannot: each
// transaction must gain the block on its OWN row, and on no other.
func TestSetMinedMultiStampsEveryTransactionInOneCall(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 5)

	// Stored but NOT named in the call. A join that matched on the leaf alone, or on the
	// 96-bit key prefix, would stamp this one too and no other assertion here would notice.
	bystander := mkStoredTxs(t, s, 700_000, 8_000, 1)[0]

	info := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true}

	got, err := s.SetMinedMulti(ctx, txHashes(txs), info)
	require.NoError(t, err)
	require.Len(t, got, 5, "every hash asked about must appear in the answer")

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		require.Contains(t, got[*h], uint32(77))

		r := readIdent(t, s, ctx, h[:])
		require.Equal(t, packTriples(t, [3]uint32{77, 700_005, 2}), r.membership,
			"exactly one block, on this transaction's own row")
		require.Nil(t, r.offChainSince, "mined on the longest chain means no longer waiting")
	}

	bh := bystander.TxIDChainHash()
	br := readIdent(t, s, ctx, bh[:])
	require.Empty(t, br.membership, "a transaction the call did not name must not be stamped")
	require.NotNil(t, br.offChainSince, "nor lose its mempool marker")
}

// TestSetMinedMultiReplaysOverAMixedBatch puts already-stamped and never-stamped
// transactions in ONE call, which is the shape a re-offered block actually arrives in.
//
// The transactions already carrying the block must not gain it twice, and the ones that do
// not yet carry it must gain it. A statement that got the per-row guard wrong would fail one
// of those two and pass the other.
func TestSetMinedMultiReplaysOverAMixedBatch(t *testing.T) {
	s, ctx := newTestStore(t)

	first := mkStoredTxs(t, s, 700_000, 1_000, 3)
	info := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true}

	_, err := s.SetMinedMulti(ctx, txHashes(first), info)
	require.NoError(t, err)

	later := mkStoredTxs(t, s, 700_001, 9_000, 2)
	all := append(txHashes(first), txHashes(later)...)

	got, err := s.SetMinedMulti(ctx, all, info)
	require.NoError(t, err, "a re-offered block must not report its transactions missing")
	require.Len(t, got, 5)

	want := packTriples(t, [3]uint32{77, 700_005, 2})

	for _, h := range all {
		require.Contains(t, got[*h], uint32(77))
		require.Equal(t, want, readIdent(t, s, ctx, h[:]).membership,
			"the block is recorded exactly once, whether or not this row already had it")
	}
}

// TestSetMinedMultiToleratesTheSameHashTwiceInOneCall. A caller may name a transaction twice
// in one block, and the two offers now travel in one statement rather than in two.
func TestSetMinedMultiToleratesTheSameHashTwiceInOneCall(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	got, err := s.SetMinedMulti(ctx, []*chainhash.Hash{h, h}, utxo.MinedBlockInfo{
		BlockID: 77, BlockHeight: 700_005, SubtreeIdx: 2, OnLongestChain: true,
	})
	require.NoError(t, err)
	require.Contains(t, got[*h], uint32(77))

	require.Equal(t, packTriples(t, [3]uint32{77, 700_005, 2}),
		readIdent(t, s, ctx, h[:]).membership, "named twice, recorded once")
}

// TestSetMinedMultiKeepsTheMempoolMarkerOffTheLongestChain pins the rule the stamp shares
// with the create gate: "mined into some block" and "on the main chain" are different facts,
// and only the second clears the marker.
func TestSetMinedMultiKeepsTheMempoolMarkerOffTheLongestChain(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 3)

	_, err := s.SetMinedMulti(ctx, txHashes(txs), utxo.MinedBlockInfo{
		BlockID: 88, BlockHeight: 700_006, SubtreeIdx: 1, OnLongestChain: false,
	})
	require.NoError(t, err)

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		r := readIdent(t, s, ctx, h[:])
		require.Equal(t, packTriples(t, [3]uint32{88, 700_006, 1}), r.membership)
		require.NotNil(t, r.offChainSince,
			"a fork-only block does not settle a transaction, so the marker stays")
	}
}

// TestUnsetMinedMultiUnstampsEveryTransactionInOneCall is the reorg path at batch width: each
// transaction loses the block and gets a clock from the CURRENT tip.
func TestUnsetMinedMultiUnstampsEveryTransactionInOneCall(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 100, 1_000, 4)
	info := utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true}

	_, err := s.SetMinedMulti(ctx, txHashes(txs), info)
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(5_000))

	info.UnsetMined = true

	_, err = s.SetMinedMulti(ctx, txHashes(txs), info)
	require.NoError(t, err)

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		r := readIdent(t, s, ctx, h[:])
		require.Empty(t, r.membership, "the block it was un-mined from is no longer claimed")
		require.NotNil(t, r.offChainSince, "an un-mined transaction is back in the mempool set")
		require.Equal(t, int32(5_000), *r.offChainSince, "with a clock from the current tip")
	}
}
