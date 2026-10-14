package utxoset

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// A parent in the list can hold coins the netting never wrote: it reached the store by a path
// that writes every output as a coin. Whatever that path, an output a transaction of the list
// spends must not stay a coin once the list has applied.

// The child's chunk commits and the parent's does not. Before the repeat, the parent arrives
// unmined through propagation, which writes all its outputs as coins. On the repeat the child
// already has its record and is skipped, so the parent's output it spends must still go.
func TestNettedBelowParentArrivingBetweenAttempts(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 970

	withNettedBelowChunkTxs(t, 1)

	w := tests.BuildMultiWorkload(t, 0x7c, 2, 2)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)
	parent := w.Txs[0]

	// The parent's chunk fails, but only once the child's chunk has committed: the chunks run
	// at the same time, and the first failure cancels the ones not yet started.
	childDone := make(chan struct{})
	crash := errors.NewProcessingError("injected crash")
	nettedBelowFault = func(chunk []int) error {
		switch chunk[0] {
		case 0:
			select {
			case <-childDone:
			case <-time.After(10 * time.Second):
			}

			return crash
		case 2:
			defer close(childDone)
		}

		return nil
	}

	t.Cleanup(func() { nettedBelowFault = nil })

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.Error(t, err)

	nettedBelowFault = nil

	var childMined int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1 AND mined_height = $2`,
		w.Txs[2].TxIDChainHash()[:], int32(height)).Scan(&childMined))
	require.Equal(t, 1, childMined, "the child's chunk committed before the crash")

	var parentMined int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1`, parent.TxIDChainHash()[:]).Scan(&parentMined))
	require.Zero(t, parentMined, "the parent's chunk did not")

	// The parent arrives unmined: its own spends made, every output a coin.
	_, _, err = s.SpendAndCreate(ctx, parent, height-1)
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins,
		"an output the list spends is not a coin")
}

// The build before version 2 wrote the list one transaction at a time through the block-path
// create, which writes every output as a coin. A restart onto version 2 after it wrote a parent
// and before it wrote the child leaves the parent with a mined record and the coin its child
// spends. On version 2 the parent exists and is skipped, and the child is new, so the coin must
// still go.
func TestNettedBelowParentWrittenByTheOldWriter(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 980

	w := tests.BuildMultiWorkload(t, 0x7d, 2, 2)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	// The old writer's first transaction: mined record, its spends, every output a coin.
	_, _, err := s.SpendAndCreate(ctx, list[0], height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height, belowCheckpointOptions(height)...)
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status, "the old writer wrote the parent")

	for i, r := range results[1:] {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i+1, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins,
		"an output the list spends is not a coin")
}

// The old writer can also have written a transaction that version 2 would write nothing for:
// every input netted and every output spent in the list. Its record and coins exist, so it is
// not such a transaction on version 2, and its coins the list spends must go.
func TestNettedBelowWriteNothingTransactionWrittenByTheOldWriter(t *testing.T) {
	s := newBodySkipStore(t, true)
	ctx := t.Context()

	const height = 990

	w := tests.BuildMultiWorkload(t, 0x7e, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := writeNothingList(t, w)

	// The old writer's first two transactions: list[1] is the one every input and output of
	// which the list nets.
	for _, tx := range list[:2] {
		_, _, err := s.SpendAndCreate(ctx, tx, height, belowCheckpointOptions(height)...)
		require.NoError(t, err)
	}

	results, err := s.SpendAndCreateMulti(ctx, writeNothingList(t, w), height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Contains(t, []utxo.SpendAndCreateMultiStatus{utxo.MultiTxCreated, utxo.MultiTxExisted}, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Equal(t, expectedNetState(w.Roots, list).coins, readNetState(t, s, ctx, w.Roots, list, height).coins,
		"an output the list spends is not a coin")
}
