package netsync

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// createConflictingSpyValidator records the CreateConflicting validator option
// observed on each Validate call and, optionally, returns a fixed error to model
// a conflicting/double-spend outcome. It lets these tests assert both which option
// the netsync inline path passed AND how PreValidateTransactions treats the
// resulting error, without standing up a real validator + UTXO store.
type createConflictingSpyValidator struct {
	validator.MockValidator

	mu                  sync.Mutex
	sawCreateConflict   int // number of Validate calls that observed CreateConflicting=true
	sawNoCreateConflict int // number of Validate calls that observed CreateConflicting=false
	returnErr           error
	callCount           atomic.Int64
}

func (v *createConflictingSpyValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...validator.Option) (*meta.Data, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	v.callCount.Add(1)

	processed := validator.ProcessOptions(opts...)

	v.mu.Lock()
	if processed.CreateConflicting {
		v.sawCreateConflict++
	} else {
		v.sawNoCreateConflict++
	}
	returnErr := v.returnErr
	v.mu.Unlock()

	if returnErr != nil {
		return nil, returnErr
	}

	return &meta.Data{}, nil
}

func (v *createConflictingSpyValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *validator.Options) (*meta.Data, error) {
	return v.Validate(ctx, tx, blockHeight)
}

func (v *createConflictingSpyValidator) TriggerBatcher() {}

func (v *createConflictingSpyValidator) createConflictCounts() (with, without int) {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.sawCreateConflict, v.sawNoCreateConflict
}

// inlineBelowCheckpointHeight is the single hard-coded checkpoint used by the
// inline-path harness; block height 500 is below it.
const inlineBelowCheckpointHeight = int32(1000)

// newInlineBelowCheckpointSyncManager wires a SyncManager ready to drive
// PreValidateTransactions. The below-checkpoint fail-closed flag was retired, so the
// inline path always retains validator.WithCreateConflicting and swallows a resulting
// ErrTxConflicting — the behaviour these tests pin.
func newInlineBelowCheckpointSyncManager(t *testing.T, cv validator.Interface) *SyncManager {
	t.Helper()

	tSettings, params := newOutpointOnlySettings(t, true, inlineBelowCheckpointHeight)
	tSettings.Legacy.SpendBatcherSize = 2
	tSettings.Legacy.SpendBatcherConcurrency = 2

	return &SyncManager{
		settings:         tSettings,
		chainParams:      params,
		logger:           ulogger.TestLogger{},
		validationClient: cv,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}
}

// makeSameBlockParentChainTxMap builds n regular txs where tx[k] spends tx[k-1]'s
// output (a same-block parent chain). All spends succeed at the validator (no
// conflict), so this exercises the happy path.
func makeSameBlockParentChainTxMap(t *testing.T, n int) *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper] {
	t.Helper()

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](n)

	var prevHash *chainhash.Hash

	for i := 0; i < n; i++ {
		tx := bt.NewTx()
		tx.Version = 1

		if prevHash == nil {
			require.NoError(t, tx.From(chainhash.HashH([]byte(fmt.Sprintf("coinbase-%d", i))).String(), 0, "76a914", uint64(1_000_000)))
		} else {
			require.NoError(t, tx.From(prevHash.String(), 0, "76a914", uint64(1_000_000)))
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(900_000)))

		h := *tx.TxIDChainHash()
		txMap.Set(h, &TxMapWrapper{Tx: tx})
		prevHash = &h
	}

	return txMap
}

// TestPreValidateTransactions_SameBlockParentChain_Succeeds proves that a
// below-checkpoint block whose txs form a same-block parent chain validates: the
// validator sees WithCreateConflicting=true on every tx and PreValidateTransactions
// returns no error.
func TestPreValidateTransactions_SameBlockParentChain_Succeeds(t *testing.T) {
	initPrometheusMetrics()

	cv := &createConflictingSpyValidator{} // all spends succeed

	sm := newInlineBelowCheckpointSyncManager(t, cv)

	txMap := makeSameBlockParentChainTxMap(t, 5)

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 500, 0, 0, true)
	require.NoError(t, err, "same-block parent chain must validate with no spurious ErrTxNotFound")

	with, without := cv.createConflictCounts()
	require.Equal(t, 5, with, "the inline path always appends WithCreateConflicting(true)")
	require.Zero(t, without, "no tx should be validated without CreateConflicting on the inline path")
}

// TestPreValidateTransactions_ConflictSwallowed proves that an ErrTxConflicting
// result from the validator is swallowed on the inline below-checkpoint path: the
// conflict is reconciled downstream by ProcessConflicting during block acceptance, so
// PreValidateTransactions returns no error and the block proceeds. This is the
// permanent behaviour after the fail-closed lever was retired.
func TestPreValidateTransactions_ConflictSwallowed(t *testing.T) {
	initPrometheusMetrics()

	cv := &createConflictingSpyValidator{
		returnErr: errors.ErrTxConflicting,
	}

	sm := newInlineBelowCheckpointSyncManager(t, cv)

	txMap := makeTxMap(t, 3)

	err := sm.PreValidateTransactions(context.Background(), txMap, chainhash.Hash{}, 500, 0, 0, true)
	require.NoError(t, err, "inline path must swallow ErrTxConflicting and proceed")

	with, without := cv.createConflictCounts()
	require.Equal(t, 3, with, "the inline path always appends WithCreateConflicting(true) on every tx")
	require.Zero(t, without, "no tx should be validated without CreateConflicting on the inline path")
}
