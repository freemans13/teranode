package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	pq "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_SetMinedMultiBulk_Success(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(3)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for successful case
	SetupSetMinedMultiBulkMocks(mock, hashes, minedInfo)

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result))

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_ContextCancelled_BeforeStart(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Execute the function with cancelled context
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, result)

	// No database operations should have been attempted
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_ContextCancelled_DuringExecution(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Create context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Setup mock to begin transaction but then context will be cancelled
	mock.ExpectBegin()
	mock.ExpectRollback()

	// Wait for context to be cancelled
	time.Sleep(2 * time.Millisecond)

	// Execute the function with cancelled context
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results - should fail due to context cancellation
	assert.Error(t, err)
	assert.True(t, err == context.DeadlineExceeded || err == context.Canceled)
	assert.Nil(t, result)
}

func TestStore_SetMinedMultiBulk_BeginTransactionError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for begin transaction error
	SetupSetMinedMultiBulkErrorMocks(mock, "begin_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, sql.ErrConnDone, err)
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_CheckExistsError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for check exists error
	SetupSetMinedMultiBulkErrorMocks(mock, "check_exists_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error checking transaction existence")
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_InsertBlockIDsError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for insert block_ids error
	SetupSetMinedMultiBulkErrorMocks(mock, "insert_block_ids_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error bulk inserting block_ids")
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_UpdateError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for update error
	SetupSetMinedMultiBulkErrorMocks(mock, "update_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error bulk updating transactions")
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_GetBlockIDsError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for get block IDs error
	SetupSetMinedMultiBulkErrorMocks(mock, "get_block_ids_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error bulk fetching block IDs")
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_CommitError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for commit error
	SetupSetMinedMultiBulkErrorMocks(mock, "commit_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error committing transaction")
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_RollbackError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test data
	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for rollback error (this tests the defer rollback logic)
	SetupSetMinedMultiBulkErrorMocks(mock, "rollback_error")

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results - original error should be returned, not rollback error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error checking transaction existence") // Original error from the query
	assert.Nil(t, result)

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiBulk_EmptyHashes(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	// Test with empty hashes slice
	hashes := []*chainhash.Hash{}
	minedInfo := CreateTestMinedBlockInfo()

	// Setup mock expectations for empty hashes - should do query and early commit since no existing transactions
	mock.ExpectBegin()

	// Empty hash array operations
	existsRows := sqlmock.NewRows([]string{"hash"})
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
		WithArgs(pq.Array([][]byte{})).
		WillReturnRows(existsRows)

	// Since no transactions exist, it should commit early
	mock.ExpectCommit()

	// Execute the function
	ctx := context.Background()
	result, err := store.setMinedMultiBulk(ctx, hashes, minedInfo)

	// Verify results - should succeed with empty result
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 0, len(result))

	// Verify all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateTestHashes(t *testing.T) {
	// Test the helper function
	hashes := CreateTestHashes(5)
	require.Equal(t, 5, len(hashes))

	// Verify all hashes are unique
	hashSet := make(map[chainhash.Hash]bool)
	for _, hash := range hashes {
		require.NotNil(t, hash)
		require.False(t, hashSet[*hash], "Hash should be unique")
		hashSet[*hash] = true
	}
}

func TestCreateTestMinedBlockInfo(t *testing.T) {
	// Test the helper function
	info := CreateTestMinedBlockInfo()
	require.Equal(t, uint32(1), info.BlockID)
	require.Equal(t, uint32(100), info.BlockHeight)
	require.Equal(t, 0, info.SubtreeIdx)
	require.Equal(t, false, info.UnsetMined)
}
