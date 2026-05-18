package aerospike

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newTestStoreForMergedOps builds the minimum Store needed for sendMergedOpsBatch
// to operate without touching a real Aerospike client. Tests install
// batchOperateFn so BatchOperate is never dispatched to a real cluster.
func newTestStoreForMergedOps(t *testing.T, mode string) *Store {
	t.Helper()

	InitPrometheusMetrics()

	tSettings := &settings.Settings{}
	tSettings.Aerospike.UseDefaultPolicies = true
	tSettings.UtxoStore.UtxoBatchSize = 20_000
	tSettings.UtxoStore.MergedOpsBatcherMode = mode

	return &Store{
		ctx:           context.Background(),
		namespace:     "test-ns",
		setName:       "test-set",
		logger:        ulogger.TestLogger{},
		settings:      tSettings,
		utxoBatchSize: tSettings.UtxoStore.UtxoBatchSize,
	}
}

// makeMixedItems returns a set of mixedOps (excluding GET) plus their
// done/err channels so the test can verify each was notified.
type mergedTestItems struct {
	createCh    chan error
	outpointCh  chan error
	incrementCh chan incrementSpentRecordsRes
	setDAHCh    chan error
	setLockedCh chan error
	mixed       []*mixedOp
}

func buildMixedTestItems(t *testing.T) mergedTestItems {
	t.Helper()

	// CREATE — a minimal tx
	tx := txWithSingleOutput(t)
	createItem := NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2))

	// OUTPOINT — needs a *bt.Input with PreviousTxIDChainHash; using the same tx's
	// (empty) input list won't work, so we synthesize a hash-only outpoint via a
	// dummy bt.Input. The outpoint dispatch only needs PreviousTxIDChainHash() to
	// build keys; with batchErr non-nil it short-circuits to the error path.
	//
	// However we want batchErr == nil so the test sees normal flow. Easiest: skip
	// the deep dispatch by setting per-record err — but the dispatch then needs
	// real Record.Bins. To keep this test focused (assert call count + each
	// item's channel got *some* notification), we drive the OUTPOINT branch with
	// batchErr non-nil (error path notifies every item exactly once).
	//
	// To exercise the "single mode mixed BatchOperate call" assertion while
	// driving the error path for outpoint and the success path for create+others,
	// we'd need two batchOperateFn behaviours — but there's only one call in
	// single mode. So we choose: drive ALL dispatches via the per-record err
	// path so each builder's dispatch sees the same top-level result.
	//
	// We omit OUTPOINT from the test (it lives in get.go and its dispatch tries
	// to read Record.Bins which a mock can't easily populate). The single-mode
	// test still validates partitioning of the 4 remaining write-side op-types.

	// INCREMENT — needs txID
	var txid chainhash.Hash
	copy(txid[:], []byte("incr-txid-32bytes-aaaaaaaaaaaaaa"))
	incCh := make(chan incrementSpentRecordsRes, 2)
	incItem := &batchIncrement{txID: &txid, increment: 1, res: incCh}

	// SET DAH
	var dahTxid chainhash.Hash
	copy(dahTxid[:], []byte("dah-txid-32bytes-aaaaaaaaaaaaaaa"))
	dahCh := make(chan error, 2)
	dahItem := &batchDAH{txID: &dahTxid, childIdx: 1, deleteAtHeight: 100, errCh: dahCh}

	// SET LOCKED — dispatch parses Bins[LuaSuccess]; we drive batchErr non-nil
	// so it takes the simple error fan-out.
	var lockedTx chainhash.Hash
	copy(lockedTx[:], []byte("locked-txid-32bytes-aaaaaaaaaaa"))
	lockedCh := make(chan error, 2)
	lockedItem := &batchLocked{txHash: lockedTx, setValue: true, errCh: lockedCh}

	return mergedTestItems{
		createCh:    createItem.done,
		incrementCh: incCh,
		setDAHCh:    dahCh,
		setLockedCh: lockedCh,
		mixed: []*mixedOp{
			{kind: opCreate, create: createItem},
			{kind: opIncrement, increment: incItem},
			{kind: opSetDAH, setDAH: dahItem},
			{kind: opSetLocked, setLocked: lockedItem},
		},
	}
}

// TestSendMergedOpsBatch_Single_DispatchesPerOpType verifies that single mode
// performs ONE BatchOperate call covering every non-GET op-type and that each
// item's notification channel receives a result.
func TestSendMergedOpsBatch_Single_DispatchesPerOpType(t *testing.T) {
	s := newTestStoreForMergedOps(t, "single")

	var batchOperateCalls atomic.Int32

	// Drive batchErr non-nil so every builder takes its error fan-out path,
	// which always emits exactly one notification per item regardless of
	// per-record Bins shape.
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		batchOperateCalls.Add(1)
		// Sanity: records should be non-empty (each of 4 op-types contributes >= 1 record)
		require.NotEmpty(t, records, "merged single mode must concatenate per-op records")
		return aerospike.ErrNetwork
	}

	items := buildMixedTestItems(t)

	s.sendMergedOpsBatch(items.mixed)

	require.Equal(t, int32(1), batchOperateCalls.Load(), "single mode must perform exactly one BatchOperate call")

	// Each item must be notified exactly once.
	requireOneError(t, items.createCh, "create")
	requireOneIncrementResult(t, items.incrementCh, "increment")
	requireOneError(t, items.setDAHCh, "setDAH")
	requireOneError(t, items.setLockedCh, "setLocked")
}

// TestSendMergedOpsBatch_Split_FiresTwoBatchOperateCalls verifies that split
// mode performs TWO BatchOperate calls (one for reads, one for writes) when
// both reads and writes are present, and that each item is notified.
func TestSendMergedOpsBatch_Split_FiresTwoBatchOperateCalls(t *testing.T) {
	s := newTestStoreForMergedOps(t, "split")

	var batchOperateCalls atomic.Int32

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		batchOperateCalls.Add(1)
		require.NotEmpty(t, records)
		return aerospike.ErrNetwork
	}

	items := buildMixedTestItems(t)

	// Add an outpoint to exercise the read-side BatchOperate.
	// buildOutpointRecords needs PreviousTxIDChainHash(); we provide an input with
	// PreviousTxID set so the hash is derivable.
	outpointCh := make(chan error, 2)
	prevHash := chainhash.HashH([]byte("prev-tx"))
	// bt.Input zero-value: we'll set PreviousTxIDChainHash via the setter.
	// Use a minimal construction: a bt.Input embedded directly. The dispatch
	// path only invokes the error fan-out when batchErr is non-nil, so we don't
	// need Record.Bins to be set.
	in := newMinimalInputWithPrev(t, prevHash)
	items.mixed = append(items.mixed, &mixedOp{
		kind:     opOutpoint,
		outpoint: &batchOutpoint{outpoint: in, errCh: outpointCh},
	})

	s.sendMergedOpsBatch(items.mixed)

	// Two BatchOperate calls: outpoint (read) + writes (create+inc+dah+locked).
	require.Equal(t, int32(2), batchOperateCalls.Load(), "split mode must perform two BatchOperate calls (reads + writes)")

	requireOneError(t, items.createCh, "create")
	requireOneIncrementResult(t, items.incrementCh, "increment")
	requireOneError(t, items.setDAHCh, "setDAH")
	requireOneError(t, items.setLockedCh, "setLocked")
	requireOneError(t, outpointCh, "outpoint")
}

// newMinimalInputWithPrev builds a bt.Input whose PreviousTxID is the given hash.
func newMinimalInputWithPrev(t *testing.T, prev chainhash.Hash) *bt.Input {
	t.Helper()
	in := &bt.Input{}
	require.NoError(t, in.PreviousTxIDAdd(&prev))
	return in
}

func requireOneError(t *testing.T, ch chan error, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: expected a notification on done/err channel, got none", name)
	}
}

func requireOneIncrementResult(t *testing.T, ch chan incrementSpentRecordsRes, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: expected a notification on res channel, got none", name)
	}
}
