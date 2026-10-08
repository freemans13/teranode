package utxoset

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// A chunk ends at nettedBelowMaxChunkOutputs outputs, not only at a transaction count. On mainnet
// at block 814,043 on 2026-10-08 the 16 parallel chunks of a 30,595-transaction block held about
// 28 million outputs, and the netted write used 13 GB until the OOM killer stopped the node. Each
// workload transaction has two outputs, so with a limit of three each chunk has one transaction.
func TestNettedBelowChunksEndAtTheOutputLimit(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 950

	old := nettedBelowMaxChunkOutputs
	nettedBelowMaxChunkOutputs = 3

	t.Cleanup(func() { nettedBelowMaxChunkOutputs = old; nettedBelowFault = nil })

	var (
		mu     sync.Mutex
		chunks [][]int
	)

	nettedBelowFault = func(chunk []int) error {
		mu.Lock()
		defer mu.Unlock()

		chunks = append(chunks, append([]int(nil), chunk...))

		return nil
	}

	w := tests.BuildMultiWorkload(t, 0x7a, 3, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Len(t, chunks, len(list), "one transaction for each chunk")

	for _, c := range chunks {
		require.Len(t, c, 1)
	}

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}

// The coins of a chunk are written in statements of at most nettedCoinsBatchRows rows, so one
// transaction with millions of outputs does not build one statement of all of them. The net
// effect is the same.
func TestNettedBelowWritesTheCoinsInBatches(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 960

	old := nettedCoinsBatchRows
	nettedCoinsBatchRows = 1

	t.Cleanup(func() { nettedCoinsBatchRows = old })

	w := tests.BuildMultiWorkload(t, 0x7b, 3, 4)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(w.Txs)

	_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	require.Equal(t, expectedNetState(w.Roots, list), readNetState(t, s, ctx, w.Roots, list, height))
}
