package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// requestedFields captures the field set a Get call asked for.
func requestedFields(args mock.Arguments) []fields.FieldName {
	return args.Get(2).([]fields.FieldName)
}

// TestGetCounterConflictingTxHashesAsksForOutpointsNotTheWholeTx pins the field
// set, not just the behaviour. The walk reads parent hashes only, so asking for
// fields.Tx made the SQL store join the inputs and outputs tables and made
// aerospike fetch the external blob of a spilled transaction. A future edit that
// widens the request back to fields.Tx fails here.
func TestGetCounterConflictingTxHashesAsksForOutpointsNotTheWholeTx(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("test-tx")
	parentTxHash := createTestHash("parent-tx")
	testTx := createTestTransactionWithInputs(parentTxHash, 0)

	var askedFor []fields.FieldName

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Run(func(args mock.Arguments) {
			askedFor = requestedFields(args)
		}).
		Return(&meta.Data{Tx: testTx}, nil)

	// The parent lookup is a separate concern; fail it so the walk stops here.
	mockStore.On("Get", mock.Anything, &parentTxHash, mock.Anything).
		Return(nil, errors.NewProcessingError("stop here"))

	_, _ = GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

	require.Contains(t, askedFor, fields.TxInpoints)
	require.NotContains(t, askedFor, fields.Tx,
		"the counter-conflicting walk reads parent hashes only and must not pull the transaction body")
	require.NotContains(t, askedFor, fields.Inputs)
}

// TestGetCounterConflictingTxHashesErrorsOnMissingRecord covers the nil-record
// path. Aerospike returns (nil, nil) for a transaction it does not hold, which
// this function used to dereference straight into a panic.
func TestGetCounterConflictingTxHashesErrorsOnMissingRecord(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}

	txHash := createTestHash("missing-tx")

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).Return(nil, nil)

	result, err := GetCounterConflictingTxHashes(ctx, mockStore, txHash, 0)

	require.Nil(t, result)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
	mockStore.AssertExpectations(t)
}

// TestCandidateSpendsOutputMatchesOnOutpoint exercises the outpoint comparison
// directly, including the case that matters for a demoted transaction with
// several inputs against the same parent: the vout has to match too.
func TestCandidateSpendsOutputMatchesOnOutpoint(t *testing.T) {
	parent := createTestHash("parent")
	other := createTestHash("other-parent")

	// Two vouts of the same parent, plus one input from a different parent.
	tx := createTestTransactionWithInputs(parent, 1)

	extra := createTestTransactionWithInputs(parent, 4)
	tx.Inputs = append(tx.Inputs, extra.Inputs[0])

	fromOther := createTestTransactionWithInputs(other, 0)
	tx.Inputs = append(tx.Inputs, fromOther.Inputs[0])

	inpoints := inpointsFromTx(t, tx)

	require.True(t, candidateSpendsOutput(&inpoints, &parent, 1))
	require.True(t, candidateSpendsOutput(&inpoints, &parent, 4))
	require.True(t, candidateSpendsOutput(&inpoints, &other, 0))

	// Right parent, wrong vout.
	require.False(t, candidateSpendsOutput(&inpoints, &parent, 0))
	require.False(t, candidateSpendsOutput(&inpoints, &parent, 2))

	// Right vout, wrong parent.
	require.False(t, candidateSpendsOutput(&inpoints, &other, 1))

	// A parent that is not spent at all.
	unrelated := chainhash.HashH([]byte("unrelated"))
	require.False(t, candidateSpendsOutput(&inpoints, &unrelated, 0))
}

// inpointsFromTx derives the inpoints the way every real store does, so these
// tests exercise the same value production hands to candidateSpendsOutput.
func inpointsFromTx(t *testing.T, tx *bt.Tx) subtree.TxInpoints {
	t.Helper()

	inpoints, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
	require.NoError(t, err)

	return inpoints
}
