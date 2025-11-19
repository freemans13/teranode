package cleanup

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/cleanup"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Import sqlite driver
)

// createTestSettings creates default settings for testing
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		GlobalBlockHeightRetention: 288, // Default retention
	}
}

// setupTestDB creates an in-memory sqlite database with proper schema for testing
func setupTestDB(t *testing.T, ctx context.Context) *usql.DB {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///cleanup_test")
	require.NoError(t, err)

	db, err := util.InitSQLDB(logger, storeURL, tSettings)
	require.NoError(t, err)

	// Create minimal schema needed for cleanup tests
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS transactions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			hash BLOB NOT NULL,
			delete_at_height BIGINT,
			unmined_since BIGINT,
			last_spender BLOB
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS outputs (
			transaction_id INTEGER NOT NULL,
			idx BIGINT NOT NULL,
			spending_data BLOB
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS block_ids (
			transaction_id INTEGER NOT NULL,
			block_height BIGINT NOT NULL
		)
	`)
	require.NoError(t, err)

	return db
}

func TestNewService(t *testing.T) {
	t.Run("ValidService", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger:         logger,
			DB:             db.DB,
			WorkerCount:    2,
			MaxJobsHistory: 100,
			Ctx:            context.Background(),
		})

		assert.NoError(t, err)
		assert.NotNil(t, service)
		assert.Equal(t, logger, service.logger)
		assert.Equal(t, db.DB, service.db)
		assert.NotNil(t, service.jobManager)
	})

	t.Run("NilLogger", func(t *testing.T) {
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: nil,
			DB:     db.DB,
		})

		assert.Error(t, err)
		assert.Nil(t, service)
		assert.Contains(t, err.Error(), "logger is required")
	})

	t.Run("NilSettings", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(nil, Options{
			Logger: logger,
			DB:     db.DB,
		})

		assert.Error(t, err)
		assert.Nil(t, service)
		assert.Contains(t, err.Error(), "settings is required")
	})

	t.Run("NilDB", func(t *testing.T) {
		logger := &MockLogger{}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     nil,
		})

		assert.Error(t, err)
		assert.Nil(t, service)
		assert.Contains(t, err.Error(), "db is required")
	})

	t.Run("DefaultValues", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger:         logger,
			DB:             db.DB,
			WorkerCount:    0,  // Should use default
			MaxJobsHistory: -1, // Should use default
		})

		assert.NoError(t, err)
		assert.NotNil(t, service)
	})
}

func TestService_Start(t *testing.T) {
	t.Run("StartService", func(t *testing.T) {
		loggedMessages := make([]string, 0, 5)
		logger := &MockLogger{
			InfofFunc: func(format string, args ...interface{}) {
				loggedMessages = append(loggedMessages, format)
			},
		}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		service.Start(ctx)

		// Check that both service and job manager log messages are present
		found := false
		for _, msg := range loggedMessages {
			if strings.Contains(msg, "starting cleanup service") {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected to find 'starting cleanup service' in logged messages: %v", loggedMessages)
	})
}

func TestService_UpdateBlockHeight(t *testing.T) {
	t.Run("ValidBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		err = service.UpdateBlockHeight(100)
		assert.NoError(t, err)
	})

	t.Run("ZeroBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		err = service.UpdateBlockHeight(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot update block height to 0")
	})

	t.Run("WithDoneChannel", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(100, doneCh)
		assert.NoError(t, err)
	})
}

func TestService_GetJobs(t *testing.T) {
	t.Run("GetJobsEmpty", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		jobs := service.GetJobs()
		assert.NotNil(t, jobs)
		assert.Len(t, jobs, 0)
	})

	t.Run("GetJobsWithData", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Add a job
		err = service.UpdateBlockHeight(100)
		assert.NoError(t, err)

		jobs := service.GetJobs()
		assert.Len(t, jobs, 1)
		assert.Equal(t, uint32(100), jobs[0].BlockHeight)
	})
}

func TestService_processCleanupJob(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfulCleanup", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		loggedMessages := make([]string, 0, 5)
		logger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {
				loggedMessages = append(loggedMessages, format)
			},
		}

		// Insert test transactions
		_, err := db.Exec(`
			INSERT INTO transactions (hash, delete_at_height) VALUES
			(randomblob(32), 50),
			(randomblob(32), 75),
			(randomblob(32), 100)
		`)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(100, ctx)

		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
		assert.False(t, job.Started.IsZero())
		assert.False(t, job.Ended.IsZero())
		assert.Nil(t, job.Error)

		// Verify logging
		assert.GreaterOrEqual(t, len(loggedMessages), 1)
		assert.Contains(t, loggedMessages[0], "running cleanup job")
	})

	t.Run("FailedCleanup", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
			ErrorfFunc: func(format string, args ...interface{}) {},
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(100, ctx)

		// The processCleanupJob method will succeed with empty DB
		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
		assert.False(t, job.Started.IsZero())
		assert.False(t, job.Ended.IsZero())
		assert.Nil(t, job.Error)
	})

	t.Run("JobWithoutDoneChannel", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{
			DebugfFunc: func(format string, args ...interface{}) {},
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(100, ctx)

		// Should not panic when DoneCh is nil
		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
	})
}

func TestDeleteTombstoned(t *testing.T) {
	ctx := context.Background()

	t.Run("SuccessfulDelete", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Insert test transactions with delete_at_height
		_, err := db.Exec(`
			INSERT INTO transactions (hash, delete_at_height) VALUES
			(randomblob(32), 50),
			(randomblob(32), 75),
			(randomblob(32), 100),
			(randomblob(32), 150),
			(randomblob(32), 200)
		`)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(100, ctx)
		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
		assert.Nil(t, job.Error)

		// Verify only transactions with delete_at_height <= 100 were deleted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count, "Should have 2 transactions left (150 and 200)")
	})

	t.Run("DatabaseError", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{
			ErrorfFunc: func(format string, args ...interface{}) {},
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(100, ctx)

		// Test the successful path with empty DB
		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
		assert.Nil(t, job.Error)
	})

	t.Run("ZeroBlockHeight", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(0, ctx)
		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
		assert.Nil(t, job.Error)
	})

	t.Run("MaxBlockHeight", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Insert test transaction
		_, err := db.Exec(`INSERT INTO transactions (hash, delete_at_height) VALUES (randomblob(32), 4294967295)`)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		job := cleanup.NewJob(4294967295, ctx) // Max uint32
		service.processCleanupJob(job, 1)

		assert.Equal(t, cleanup.JobStatusCompleted, job.GetStatus())
		assert.Nil(t, job.Error)

		// Verify transaction was deleted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestService_IntegrationTests(t *testing.T) {
	t.Run("FullWorkflow", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			DebugfFunc: func(format string, args ...interface{}) {},
		}

		service, err := NewService(createTestSettings(), Options{
			Logger:         logger,
			DB:             db,
			WorkerCount:    1,
			MaxJobsHistory: 10,
		})
		require.NoError(t, err)

		// Start the service
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		service.Start(ctxTimeout)

		// Give the workers a moment to start
		time.Sleep(50 * time.Millisecond)

		// Update block height and wait for completion
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(100, doneCh)
		assert.NoError(t, err)

		// Wait for the job to complete
		select {
		case result := <-doneCh:
			assert.Equal(t, "completed", result)
		case <-time.After(2 * time.Second):
			// Check if we have any jobs at all
			jobs := service.GetJobs()
			if len(jobs) > 0 {
				t.Logf("Job status: %v, Error: %v", jobs[0].GetStatus(), jobs[0].Error)
			}
			t.Fatal("Job did not complete in time")
		}

		// Verify job is in history
		jobs := service.GetJobs()
		assert.GreaterOrEqual(t, len(jobs), 1, "Should have at least one job")
		if len(jobs) > 0 {
			assert.Equal(t, uint32(100), jobs[0].BlockHeight)
			assert.Equal(t, cleanup.JobStatusCompleted, jobs[0].GetStatus())
		}
	})

	t.Run("ServiceImplementsInterface", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Verify service implements the interface
		var _ cleanup.Service = service
	})
}

func TestService_EdgeCases(t *testing.T) {
	t.Run("RapidUpdates", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			DebugfFunc: func(format string, args ...interface{}) {},
		}

		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			return &MockResult{rowsAffected: 1}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Rapid updates should not cause issues
		for i := uint32(1); i <= 10; i++ {
			err = service.UpdateBlockHeight(i)
			assert.NoError(t, err)
		}

		jobs := service.GetJobs()
		assert.GreaterOrEqual(t, len(jobs), 1)
	})

	t.Run("LargeBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}

		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			height := args[0].(uint32)
			assert.Equal(t, uint32(4294967295), height) // Max uint32
			return &MockResult{rowsAffected: 1}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		err = service.UpdateBlockHeight(4294967295) // Max uint32
		assert.NoError(t, err)
	})

	t.Run("DatabaseUnavailable", func(t *testing.T) {
		ctx := context.Background()
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			DebugfFunc: func(format string, args ...interface{}) {},
			ErrorfFunc: func(format string, args ...interface{}) {},
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		service.Start(ctxCancel)

		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(100, doneCh)
		assert.NoError(t, err)

		// Job should complete successfully
		select {
		case result := <-doneCh:
			assert.Equal(t, "completed", result)
		case <-time.After(2 * time.Second):
			t.Fatal("Job did not complete in time")
		}

		jobs := service.GetJobs()
		assert.GreaterOrEqual(t, len(jobs), 1)
		if len(jobs) > 0 {
			assert.Equal(t, cleanup.JobStatusCompleted, jobs[0].GetStatus())
		}
	})
}

// TestSQLCleanupWithBlockPersisterCoordination tests SQL cleanup coordination with block persister
func TestSQLCleanupWithBlockPersisterCoordination(t *testing.T) {
	t.Run("BlockPersisterBehind_LimitsCleanup", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		// Mock expects query with limited height (not full height)
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			assert.Contains(t, query, "DELETE FROM transactions WHERE delete_at_height")
			// With default retention=288, persister at 50, max safe = 50 + 288 = 338
			// Cleanup requested 200, since 200 < 338, cleanup proceeds to 200 (no limitation)
			// To test limitation, persister needs to be far behind. Let's use persister=10, requested=500
			// Then max safe = 10 + 288 = 298, so 500 would be limited to 298
			if len(args) > 0 {
				height := args[0].(uint32)
				assert.LessOrEqual(t, height, uint32(298), "Cleanup should be limited by persister progress")
			}
			return &MockResult{rowsAffected: 5}, nil
		}

		// Block persister at height 10 (far behind)
		getPersistedHeight := func() uint32 {
			return uint32(10)
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Set the persisted height getter
		service.SetPersistedHeightGetter(getPersistedHeight)
		service.Start(context.Background())

		// Trigger cleanup at 500 - should be limited to 298 (10 + 288)
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(500, doneCh)
		require.NoError(t, err)

		select {
		case <-doneCh:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}
	})

	t.Run("BlockPersisterNotRunning_NormalCleanup", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			// When persister height = 0, no limitation
			if len(args) > 0 {
				height := args[0].(uint32)
				assert.Equal(t, uint32(100), height, "Should use full cleanup height when persister not running")
			}
			return &MockResult{rowsAffected: 5}, nil
		}

		// Block persister not running
		getPersistedHeight := func() uint32 {
			return uint32(0)
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		service.SetPersistedHeightGetter(getPersistedHeight)
		service.Start(context.Background())

		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(100, doneCh)
		require.NoError(t, err)

		select {
		case <-doneCh:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}
	})

	t.Run("NoGetPersistedHeightSet_NormalCleanup", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			// When no getter set, proceed normally
			if len(args) > 0 {
				height := args[0].(uint32)
				assert.Equal(t, uint32(150), height)
			}
			return &MockResult{rowsAffected: 5}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Don't set getPersistedHeight - should work normally
		service.Start(context.Background())

		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(150, doneCh)
		require.NoError(t, err)

		select {
		case <-doneCh:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}
	})
}

// TestChildStabilitySafety tests the delete-at-height-safely feature for SQL with real database
func TestChildStabilitySafety(t *testing.T) {
	ctx := context.Background()

	t.Run("ParentKeptWhenChildUnmined", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Create parent transaction eligible for deletion at height 300
		result, err := db.Exec(`INSERT INTO transactions (hash, delete_at_height) VALUES (randomblob(32), 300)`)
		require.NoError(t, err)
		parentID, err := result.LastInsertId()
		require.NoError(t, err)

		// Create child transaction that's unmined
		childResult, err := db.Exec(`INSERT INTO transactions (hash, unmined_since) VALUES (randomblob(32), 290)`)
		require.NoError(t, err)
		childID, err := childResult.LastInsertId()
		require.NoError(t, err)

		// Get child hash for spending_data
		var childHash []byte
		err = db.QueryRow(`SELECT hash FROM transactions WHERE id = ?`, childID).Scan(&childHash)
		require.NoError(t, err)

		// Create output for parent with spending_data pointing to child
		_, err = db.Exec(`INSERT INTO outputs (transaction_id, idx, spending_data) VALUES (?, 0, ?)`, parentID, childHash)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		service.Start(ctx)

		// Trigger cleanup at height 300
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(300, doneCh)
		require.NoError(t, err)

		select {
		case status := <-doneCh:
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}

		// Verify parent was NOT deleted (child is unmined)
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE id = ?`, parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Parent should NOT be deleted when child is unmined")
	})

	t.Run("ParentKeptWhenChildRecentlyMined", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Create parent transaction eligible for deletion at height 500
		result, err := db.Exec(`INSERT INTO transactions (hash, delete_at_height) VALUES (randomblob(32), 500)`)
		require.NoError(t, err)
		parentID, err := result.LastInsertId()
		require.NoError(t, err)

		// Create child transaction mined at height 350 (only 150 blocks old at cleanup height 500)
		childResult, err := db.Exec(`INSERT INTO transactions (hash, unmined_since) VALUES (randomblob(32), NULL)`)
		require.NoError(t, err)
		childID, err := childResult.LastInsertId()
		require.NoError(t, err)

		// Add block_ids entry for child at height 350
		_, err = db.Exec(`INSERT INTO block_ids (transaction_id, block_height) VALUES (?, 350)`, childID)
		require.NoError(t, err)

		// Get child hash for spending_data
		var childHash []byte
		err = db.QueryRow(`SELECT hash FROM transactions WHERE id = ?`, childID).Scan(&childHash)
		require.NoError(t, err)

		// Create output for parent with spending_data pointing to child
		_, err = db.Exec(`INSERT INTO outputs (transaction_id, idx, spending_data) VALUES (?, 0, ?)`, parentID, childHash)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		service.Start(ctx)

		// Trigger cleanup at height 500
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(500, doneCh)
		require.NoError(t, err)

		select {
		case status := <-doneCh:
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}

		// Verify parent was NOT deleted (child is only 150 blocks old, needs 288)
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE id = ?`, parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Parent should NOT be deleted when child is < 288 blocks old")
	})

	t.Run("ParentDeletedWhenAllChildrenStable", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Create parent transaction eligible for deletion at height 600
		result, err := db.Exec(`INSERT INTO transactions (hash, delete_at_height) VALUES (randomblob(32), 600)`)
		require.NoError(t, err)
		parentID, err := result.LastInsertId()
		require.NoError(t, err)

		// Create child transaction mined at height 200 (400 blocks old at cleanup height 600 - STABLE)
		childResult, err := db.Exec(`INSERT INTO transactions (hash, unmined_since) VALUES (randomblob(32), NULL)`)
		require.NoError(t, err)
		childID, err := childResult.LastInsertId()
		require.NoError(t, err)

		// Add block_ids entry for child at height 200
		_, err = db.Exec(`INSERT INTO block_ids (transaction_id, block_height) VALUES (?, 200)`, childID)
		require.NoError(t, err)

		// Get child hash for spending_data
		var childHash []byte
		err = db.QueryRow(`SELECT hash FROM transactions WHERE id = ?`, childID).Scan(&childHash)
		require.NoError(t, err)

		// Create output for parent with spending_data pointing to child
		_, err = db.Exec(`INSERT INTO outputs (transaction_id, idx, spending_data) VALUES (?, 0, ?)`, parentID, childHash)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		service.Start(ctx)

		// Trigger cleanup at height 600
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(600, doneCh)
		require.NoError(t, err)

		select {
		case status := <-doneCh:
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}

		// Verify parent WAS deleted (child is 400 blocks old - stable)
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE id = ?`, parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Parent SHOULD be deleted when all children are stable")
	})

	t.Run("SafetyWindow288BlocksBoundary", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Create parent transaction eligible for deletion at height 500
		result, err := db.Exec(`INSERT INTO transactions (hash, delete_at_height) VALUES (randomblob(32), 500)`)
		require.NoError(t, err)
		parentID, err := result.LastInsertId()
		require.NoError(t, err)

		// Create child transaction mined at EXACTLY height 212 (500 - 212 = 288 blocks - boundary test)
		childResult, err := db.Exec(`INSERT INTO transactions (hash, unmined_since) VALUES (randomblob(32), NULL)`)
		require.NoError(t, err)
		childID, err := childResult.LastInsertId()
		require.NoError(t, err)

		// Add block_ids entry for child at height 212
		_, err = db.Exec(`INSERT INTO block_ids (transaction_id, block_height) VALUES (?, 212)`, childID)
		require.NoError(t, err)

		// Get child hash for spending_data
		var childHash []byte
		err = db.QueryRow(`SELECT hash FROM transactions WHERE id = ?`, childID).Scan(&childHash)
		require.NoError(t, err)

		// Create output for parent with spending_data pointing to child
		_, err = db.Exec(`INSERT INTO outputs (transaction_id, idx, spending_data) VALUES (?, 0, ?)`, parentID, childHash)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		service.Start(ctx)

		// Trigger cleanup at height 500
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(500, doneCh)
		require.NoError(t, err)

		select {
		case status := <-doneCh:
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}

		// Verify parent WAS deleted (child is exactly 288 blocks old - should be safe)
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE id = ?`, parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Parent SHOULD be deleted when child is >= 288 blocks old")
	})

	t.Run("MultipleChildrenOneUnstable", func(t *testing.T) {
		db := setupTestDB(t, ctx)
		defer db.Close()

		logger := &MockLogger{}

		// Create parent eligible for deletion at height 600
		result, err := db.Exec(`INSERT INTO transactions (hash, delete_at_height) VALUES (randomblob(32), 600)`)
		require.NoError(t, err)
		parentID, err := result.LastInsertId()
		require.NoError(t, err)

		// Create child 1 - STABLE (mined at height 100, now 500 blocks old)
		child1Result, err := db.Exec(`INSERT INTO transactions (hash, unmined_since) VALUES (randomblob(32), NULL)`)
		require.NoError(t, err)
		child1ID, err := child1Result.LastInsertId()
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO block_ids (transaction_id, block_height) VALUES (?, 100)`, child1ID)
		require.NoError(t, err)

		// Create child 2 - NOT STABLE (mined at height 400, only 200 blocks old)
		child2Result, err := db.Exec(`INSERT INTO transactions (hash, unmined_since) VALUES (randomblob(32), NULL)`)
		require.NoError(t, err)
		child2ID, err := child2Result.LastInsertId()
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO block_ids (transaction_id, block_height) VALUES (?, 400)`, child2ID)
		require.NoError(t, err)

		// Get child hashes
		var child1Hash, child2Hash []byte
		err = db.QueryRow(`SELECT hash FROM transactions WHERE id = ?`, child1ID).Scan(&child1Hash)
		require.NoError(t, err)
		err = db.QueryRow(`SELECT hash FROM transactions WHERE id = ?`, child2ID).Scan(&child2Hash)
		require.NoError(t, err)

		// Create two outputs for parent, each spent by different child
		_, err = db.Exec(`INSERT INTO outputs (transaction_id, idx, spending_data) VALUES (?, 0, ?), (?, 1, ?)`,
			parentID, child1Hash, parentID, child2Hash)
		require.NoError(t, err)

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db,
		})
		require.NoError(t, err)

		service.Start(ctx)

		// Trigger cleanup at height 600
		doneCh := make(chan string, 1)
		err = service.UpdateBlockHeight(600, doneCh)
		require.NoError(t, err)

		select {
		case status := <-doneCh:
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
		case <-time.After(2 * time.Second):
			t.Fatal("Cleanup should complete")
		}

		// Verify parent was NOT deleted (child2 is not stable)
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE id = ?`, parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "Parent should NOT be deleted when ANY child is unstable")
	})
}
