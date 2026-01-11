package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestPrefetchLevel0Parents_EmptyInput tests that prefetching with no transactions returns immediately
func TestPrefetchLevel0Parents_EmptyInput(t *testing.T) {
	ctx := context.Background()
	mockStore := new(utxo.MockUtxostore)
	settings := &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			Level0PrefetchConcurrency: 128,
		},
	}

	// Empty input should return immediately without calling UTXO store
	err := prefetchLevel0Parents(ctx, []missingTx{}, mockStore, settings)
	require.NoError(t, err)

	// Verify no UTXO store calls were made
	mockStore.AssertNotCalled(t, "PreviousOutputsDecorate")
}

// TestPrefetchLevel0Parents_NilTransaction tests that nil transactions are skipped
func TestPrefetchLevel0Parents_NilTransaction(t *testing.T) {
	ctx := context.Background()
	mockStore := new(utxo.MockUtxostore)
	settings := &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			Level0PrefetchConcurrency: 128,
		},
	}

	// Input with nil transaction
	input := []missingTx{
		{tx: nil, idx: 0},
	}

	err := prefetchLevel0Parents(ctx, input, mockStore, settings)
	require.NoError(t, err)

	// Verify no UTXO store calls were made for nil transaction
	mockStore.AssertNotCalled(t, "PreviousOutputsDecorate")
}

// TestPrefetchLevel0Parents_AlreadyExtended tests that already extended transactions are skipped
func TestPrefetchLevel0Parents_AlreadyExtended(t *testing.T) {
	ctx := context.Background()
	mockStore := new(utxo.MockUtxostore)
	settings := &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			Level0PrefetchConcurrency: 128,
		},
	}

	// Create a valid transaction
	tx, err := createTestTransaction("tx1")
	require.NoError(t, err)
	tx.SetExtended(true)

	input := []missingTx{
		{tx: tx, idx: 0},
	}

	err = prefetchLevel0Parents(ctx, input, mockStore, settings)
	require.NoError(t, err)

	// Verify no UTXO store calls were made for already extended transaction
	mockStore.AssertNotCalled(t, "PreviousOutputsDecorate")
}

// TestPrefetchLevel0Parents_Success tests successful prefetching of parent data
func TestPrefetchLevel0Parents_Success(t *testing.T) {
	ctx := context.Background()
	mockStore := new(utxo.MockUtxostore)
	settings := &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			Level0PrefetchConcurrency: 128,
		},
	}

	// Create test transactions
	tx1, err := createTestTransaction("tx1")
	require.NoError(t, err)

	tx2, err := createTestTransaction("tx2")
	require.NoError(t, err)

	input := []missingTx{
		{tx: tx1, idx: 0},
		{tx: tx2, idx: 1},
	}

	// Mock PreviousOutputsDecorate to succeed and mark transactions as extended
	mockStore.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		tx := args.Get(1).(*bt.Tx)
		tx.SetExtended(true)
	})

	err = prefetchLevel0Parents(ctx, input, mockStore, settings)
	require.NoError(t, err)

	// Verify both transactions were extended
	assert.True(t, tx1.IsExtended(), "Transaction 1 should be marked as extended")
	assert.True(t, tx2.IsExtended(), "Transaction 2 should be marked as extended")

	// Verify PreviousOutputsDecorate was called for both transactions
	mockStore.AssertNumberOfCalls(t, "PreviousOutputsDecorate", 2)
}

// TestPrefetchLevel0Parents_DefaultConcurrency tests that default concurrency is used when setting is 0
func TestPrefetchLevel0Parents_DefaultConcurrency(t *testing.T) {
	ctx := context.Background()
	mockStore := new(utxo.MockUtxostore)
	settings := &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			Level0PrefetchConcurrency: 0, // Should use default of 128
		},
	}

	// Create a test transaction
	tx, err := createTestTransaction("tx1")
	require.NoError(t, err)

	input := []missingTx{
		{tx: tx, idx: 0},
	}

	// Mock PreviousOutputsDecorate to succeed
	mockStore.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		tx := args.Get(1).(*bt.Tx)
		tx.SetExtended(true)
	})

	err = prefetchLevel0Parents(ctx, input, mockStore, settings)
	require.NoError(t, err)

	// Verify the function succeeded with default concurrency
	assert.True(t, tx.IsExtended())
	mockStore.AssertCalled(t, "PreviousOutputsDecorate", mock.Anything, tx)
}

// TestPrefetchLevel0Parents_ContextCancellation tests that context cancellation is handled
func TestPrefetchLevel0Parents_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	mockStore := new(utxo.MockUtxostore)
	settings := &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			Level0PrefetchConcurrency: 128,
		},
	}

	// Create a test transaction
	tx, err := createTestTransaction("tx1")
	require.NoError(t, err)

	input := []missingTx{
		{tx: tx, idx: 0},
	}

	// Should fail due to context cancellation
	err = prefetchLevel0Parents(ctx, input, mockStore, settings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}
