package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestPrunerReplayProtection exercises real pruning and the same-spender replay path.
func TestPrunerReplayProtection(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		paginated, markerFailure, ttl bool
	}{
		{name: "normal"},
		{name: "ttl", ttl: true},
		{name: "ttl_marker_failure", ttl: true, markerFailure: true},
		{name: "paginated_parent", paginated: true},
		{name: "marker_failure", markerFailure: true},
		{name: "paginated_marker_failure", paginated: true, markerFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPrunerReplayProtection(t, tc.paginated, tc.markerFailure, tc.ttl)
		})
	}
}

func testPrunerReplayProtection(t *testing.T, paginated, markerFailure, ttl bool) {
	t.Helper()
	logger := ulogger.New("pruner-replay-test")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false
	s.Pruner.UTXOSetTTL = ttl
	s.Aerospike.EnableSpendFilterExpressions = true
	var outputIndex uint32
	if paginated {
		s.UtxoStore.UtxoBatchSize = 2
		outputIndex = 3
	}
	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	if paginated {
		for i := 0; i < 3; i++ {
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		}
	}
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), outputIndex, parent.Outputs[outputIndex].LockingScript.String(), parent.Outputs[outputIndex].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	// Retry after a successful spend but before creation is still legitimate.
	_, err = store.Spend(ctx, child, 1000)
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)
	grandchild := bt.NewTx()
	require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
	require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
	_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
	require.NoError(t, err)
	childKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), child.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	before, err := client.Get(nil, childKey)
	require.NoError(t, err)
	require.NotEmpty(t, before.Bins[fields.BlockIDs.String()])
	require.Equal(t, 1, before.Bins[fields.SpentUtxos.String()])
	_, _, err = store.SpendAndCreate(ctx, child, 1200)
	require.ErrorIs(t, err, errors.ErrTxExists, "control: retained mined record prevents recreation")
	{
		astore.ResetPrunerServiceForTests()
		t.Cleanup(astore.ResetPrunerServiceForTests)
		require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
		require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))
		svc, err := store.GetPrunerService()
		require.NoError(t, err)
		require.NotNil(t, svc)
		parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(parent.TxIDChainHash(), outputIndex, s.UtxoStore.UtxoBatchSize))
		require.NoError(t, err)
		if markerFailure {
			// A malformed marker bin makes the real parent update fail on the
			// server. The cycle must SUCCEED and hold this one child back:
			// returning an error here would unwind into PruneWithPartitions,
			// which never retries a non-timeout error, so one poison record
			// would block pruning node-wide forever.
			require.NoError(t, client.Put(nil, parentKey, aerospike.BinMap{fields.DeletedChildren.String(): "invalid-map"}))
			pruned, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "failed-parent-update", 1)
			require.NoError(t, err, "a per-record marker failure must not fail the cycle")
			require.Equal(t, int64(0), pruned, "the held-back child must not be counted as pruned")
			retained, getErr := client.Get(nil, childKey)
			require.NoError(t, getErr, "failed marker write must retain the child")
			require.Equal(t, before.Expiration, retained.Expiration, "failed marker write must not schedule child expiry")
			require.Equal(t, before.Bins[fields.BlockIDs.String()], retained.Bins[fields.BlockIDs.String()])
			require.Equal(t, before.Bins[fields.SpentUtxos.String()], retained.Bins[fields.SpentUtxos.String()])
			require.NoError(t, client.Put(nil, parentKey, aerospike.BinMap{fields.DeletedChildren.String(): nil}))
		}
		n, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "isolated-replay-investigation", 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), n, "normal pruning must delete the fully spent child only")
		require.Eventually(t, func() bool { exists, err := client.Exists(nil, childKey); return err == nil && !exists }, 5*time.Second, 20*time.Millisecond)

		// The record the spend path reads is the page holding the spent output,
		// so that is the one that must carry the marker. With defensive mode off
		// (the deployed default, and what this test sets) the master copy is not
		// written when it differs: nothing reads it there, and it would grow
		// without bound on a high fan-out parent.
		pageRecord, err := client.Get(nil, parentKey)
		require.NoError(t, err)
		require.Contains(t, pageRecord.Bins[fields.DeletedChildren.String()], child.TxID(), "the spending page must carry replay protection")

		if paginated {
			masterKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
			require.NoError(t, err)
			masterRecord, err := client.Get(nil, masterKey)
			require.NoError(t, err)

			if markers := masterRecord.Bins[fields.DeletedChildren.String()]; markers != nil {
				require.NotContains(t, markers, child.TxID(),
					"with defensive mode off the master must not accumulate markers for outputs it does not hold")
			}
		}
	}
	_, _, err = store.SpendAndCreate(ctx, child, 1200)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "pruned confirmed child must not be recreated")
	require.Contains(t, err.Error(), "spending transaction was pruned")
	exists, err := client.Exists(nil, childKey)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestPrunerUnresolvableRecordDoesNotBlockCycle proves that a record whose
// external blob has vanished is retained on its own while the rest of the cycle
// still prunes. Before the fix, getTxInputsFromBins returned a ProcessingError
// that unwound the whole chunk into PruneWithPartitions, where a non-timeout
// error is never retried, so one such record blocked all pruning permanently.
func TestPrunerUnresolvableRecordDoesNotBlockCycle(t *testing.T) {
	logger := ulogger.New("pruner-unresolvable-test")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false
	s.Aerospike.EnableSpendFilterExpressions = true

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	// Parent keeps output 2 unspent so it survives this prune cycle and can carry
	// the replay markers we assert on.
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))

	for i := 0; i < 3; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	}

	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	// Two independent children of the same parent, both fully spent and mined,
	// so both are eligible for pruning in the same cycle.
	children := make([]*bt.Tx, 2)

	for i := range children {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), uint32(i), parent.Outputs[i].LockingScript.String(), parent.Outputs[i].Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
		_, _, err = store.SpendAndCreate(ctx, child, 1000)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
		require.NoError(t, err)

		grandchild := bt.NewTx()
		require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
		require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
		_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
		require.NoError(t, err)

		children[i] = child
	}

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	broken, healthy := children[0], children[1]

	brokenKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), broken.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	healthyKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), healthy.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// Mark one record external without writing any blob. Its input references are
	// now unrecoverable, which is exactly the anomaly the old code swallowed by
	// deleting the record and losing the parent's replay protection.
	require.NoError(t, client.Put(nil, brokenKey, aerospike.BinMap{fields.External.String(): true}))

	astore.ResetPrunerServiceForTests()
	t.Cleanup(astore.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	n, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "unresolvable-record", 1)
	require.NoError(t, err, "one unresolvable record must not fail the prune cycle")
	require.Equal(t, int64(1), n, "the healthy child must still be pruned")

	require.Eventually(t, func() bool {
		exists, err := client.Exists(nil, healthyKey)
		return err == nil && !exists
	}, 5*time.Second, 20*time.Millisecond)

	exists, err := client.Exists(nil, brokenKey)
	require.NoError(t, err)
	require.True(t, exists, "the unresolvable record must be retained, not deleted")

	parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	parentRecord, err := client.Get(nil, parentKey)
	require.NoError(t, err)
	require.Contains(t, parentRecord.Bins[fields.DeletedChildren.String()], healthy.TxID(), "the pruned child must leave replay protection on its parent")

	// A second cycle still makes progress rather than aborting, so a retained
	// record cannot wedge pruning.
	_, err = svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "unresolvable-record-retry", 1)
	require.NoError(t, err, "the retained record must not wedge later cycles")

	exists, err = client.Exists(nil, brokenKey)
	require.NoError(t, err)
	require.True(t, exists)
}
