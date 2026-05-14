package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/stretchr/testify/require"
)

// stubBatchStore implements batchUtxoStore for unit-testing the native path
// without touching real Aerospike. Each test populates the fields it needs.
type stubBatchStore struct {
	getCalls   int
	parents    map[[32]byte]*aerospike.ParentRecord
	missing    [][]byte
	parentsErr error
}

func (s *stubBatchStore) BatchGetParents(_ context.Context, hashes [][]byte) (map[[32]byte]*aerospike.ParentRecord, [][]byte, error) {
	s.getCalls++
	if s.parentsErr != nil {
		return nil, nil, s.parentsErr
	}
	return s.parents, s.missing, nil
}

// newNativeValidator returns a *Validator wired to a stub UTXO store so that
// validateBatchNative takes the native path (not the fallback).
// The UseBatchValidation flag is set to true so ValidateBatch routes to native.
func newNativeValidator(t *testing.T) (*Validator, *stubBatchStore) {
	t.Helper()
	v := newValidatorForTest(t)
	stub := &stubBatchStore{}
	v.setBatchStoreForTest(stub)
	v.settings.Validator.UseBatchValidation = true
	return v, stub
}

// mkParentMap builds a parents map for the stub, marking each hash as
// present with a minimal ParentRecord.
func mkParentMap(hashes ...*chainhash.Hash) map[[32]byte]*aerospike.ParentRecord {
	m := make(map[[32]byte]*aerospike.ParentRecord, len(hashes))
	for _, h := range hashes {
		var key [32]byte
		copy(key[:], h[:])
		m[key] = &aerospike.ParentRecord{BlockHeight: 1}
	}
	return m
}

// minimalTxWithParent builds a *bt.Tx with one input whose PreviousTxID is
// set to the given parent hash. We only care that PreviousTxIDChainHash()
// returns the right hash; actual script/satoshi fields are zeroed.
func minimalTxWithParent(t *testing.T, parent chainhash.Hash) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	input := &bt.Input{
		PreviousTxOutIndex: 0,
	}
	err := input.PreviousTxIDAdd(&parent)
	require.NoError(t, err)
	tx.Inputs = append(tx.Inputs, input)
	return tx
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestValidateBatchNative_PhaseA_OneGetCallRegardlessOfN asserts that exactly
// one BatchGetParents call is made for a batch of N txs.
func TestValidateBatchNative_PhaseA_OneGetCallRegardlessOfN(t *testing.T) {
	v, stub := newNativeValidator(t)

	pA, pB, pC := chainhash.Hash{0x01}, chainhash.Hash{0x02}, chainhash.Hash{0x03}
	stub.parents = mkParentMap(&pA, &pB, &pC)

	txs := []*bt.Tx{
		minimalTxWithParent(t, pA),
		minimalTxWithParent(t, pB),
		minimalTxWithParent(t, pC),
	}

	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Equal(t, 1, stub.getCalls, "Phase A must fire exactly one BatchGetParents call")
	require.Len(t, results, 3)
}

// TestValidateBatchNative_PhaseA_MissingParentTagged asserts that txs whose
// parent is absent from the Aerospike response are tagged with
// ErrTxMissingParent at PhaseGetParents, while txs with present parents
// remain error-free after Phase A.
func TestValidateBatchNative_PhaseA_MissingParentTagged(t *testing.T) {
	v, stub := newNativeValidator(t)

	pPresent := chainhash.Hash{0x10}
	pMissing := chainhash.Hash{0x20}
	stub.parents = mkParentMap(&pPresent)
	stub.missing = [][]byte{pMissing[:]}

	good := minimalTxWithParent(t, pPresent)
	bad := minimalTxWithParent(t, pMissing)

	results, err := v.ValidateBatch(context.Background(), []*bt.Tx{good, bad}, 0)
	require.NoError(t, err, "whole-batch err must be nil when only per-tx parents are missing")
	require.Len(t, results, 2)

	// First tx — parent was present; Phase A leaves it error-free.
	require.NoError(t, results[0].Err, "tx[0] has a present parent, must have no Phase A error")

	// Second tx — parent was missing; must be tagged.
	require.Equal(t, PhaseGetParents, results[1].Phase)
	require.ErrorIs(t, results[1].Err, terrors.ErrTxMissingParent)
}

// TestValidateBatchNative_PhaseA_WholeBatchAerospikeFailure asserts that a
// transport-level Aerospike error on BatchGetParents propagates as the
// whole-batch err return (not silently per-tx).
func TestValidateBatchNative_PhaseA_WholeBatchAerospikeFailure(t *testing.T) {
	v, stub := newNativeValidator(t)
	stub.parentsErr = terrors.NewProcessingError("aerospike unreachable")

	p := chainhash.Hash{0x01}
	tx := minimalTxWithParent(t, p)

	_, err := v.ValidateBatch(context.Background(), []*bt.Tx{tx}, 0)
	require.Error(t, err, "a transport-level Aerospike error must become the whole-batch error")
}

// TestValidateBatchNative_PhaseA_DeduplicatedParents asserts that when
// multiple txs share the same parent hash, collectUniqueParents deduplicates
// so BatchGetParents receives each hash only once.
func TestValidateBatchNative_PhaseA_DeduplicatedParents(t *testing.T) {
	v, stub := newNativeValidator(t)

	sharedParent := chainhash.Hash{0xAA}
	stub.parents = mkParentMap(&sharedParent)

	// Three txs all pointing at the same parent.
	txs := []*bt.Tx{
		minimalTxWithParent(t, sharedParent),
		minimalTxWithParent(t, sharedParent),
		minimalTxWithParent(t, sharedParent),
	}

	results, err := v.ValidateBatch(context.Background(), txs, 0)
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Equal(t, 1, stub.getCalls, "only one BatchGetParents call even with repeated parents")

	// All three txs should have no Phase A error (parent is present).
	for i, r := range results {
		require.NoError(t, r.Err, "tx[%d] should have no Phase A error", i)
	}
}
