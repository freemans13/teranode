package util_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// mockLogger implements ulogger.Logger for testing
type mockLogger struct{}

func (m *mockLogger) LogLevel() int                                { return 0 }
func (m *mockLogger) SetLogLevel(string)                           {}
func (m *mockLogger) Debugf(string, ...interface{})                {}
func (m *mockLogger) Infof(string, ...interface{})                 {}
func (m *mockLogger) Warnf(string, ...interface{})                 {}
func (m *mockLogger) Errorf(string, ...interface{})                {}
func (m *mockLogger) Fatalf(string, ...interface{})                {}
func (m *mockLogger) New(string, ...ulogger.Option) ulogger.Logger { return m }
func (m *mockLogger) Duplicate(...ulogger.Option) ulogger.Logger   { return m }

// createTestSettings creates a test settings object with default values
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		DataFolder: os.TempDir(),
		UtxoStore: settings.UtxoStoreSettings{
			PostgresMaxIdleConns: 10,
			PostgresMaxOpenConns: 80,
		},
	}
}

func TestInitSQLDB(t *testing.T) {
	logger := &mockLogger{}
	testSettings := createTestSettings()

	tests := []struct {
		name    string
		url     string
		wantErr bool
		errType string
	}{
		{
			name:    "valid postgres scheme",
			url:     "postgres://user:pass@localhost:5432/testdb",
			wantErr: true, // Will fail because we don't have a real postgres connection
			errType: "service error",
		},
		{
			name:    "valid sqlite scheme",
			url:     "sqlite:///testdb",
			wantErr: false,
		},
		{
			name:    "valid sqlitememory scheme",
			url:     "sqlitememory:///testdb",
			wantErr: false,
		},
		{
			name:    "unknown scheme",
			url:     "mysql://localhost:3306/testdb",
			wantErr: true,
			errType: "configuration error",
		},
		{
			name:    "invalid url",
			url:     "://invalid-url",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse(tt.url)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Failed to parse test URL: %v", err)
				}
				return
			}

			db, err := util.InitSQLDB(logger, storeURL, testSettings)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, db)

				if tt.errType == "configuration error" {
					var teranodeErr *errors.Error
					if assert.ErrorAs(t, err, &teranodeErr) {
						assert.Contains(t, err.Error(), "unknown scheme")
					}
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, db)
				if db != nil {
					err := db.Close()
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestInitPostgresDB(t *testing.T) {
	logger := &mockLogger{}
	testSettings := createTestSettings()

	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name:    "postgres with credentials and sslmode",
			url:     "postgres://testuser:testpass@localhost:5432/testdb?sslmode=require",
			wantErr: true, // Will fail without real postgres
		},
		{
			name:    "postgres without credentials",
			url:     "postgres://localhost:5432/testdb",
			wantErr: true, // Will fail without real postgres
		},
		{
			name:    "postgres with default sslmode",
			url:     "postgres://user:pass@localhost:5432/testdb",
			wantErr: true, // Will fail without real postgres
		},
		{
			name:    "postgres with multiple sslmode values",
			url:     "postgres://user:pass@localhost:5432/testdb?sslmode=disable&sslmode=require",
			wantErr: true, // Will fail without real postgres
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse(tt.url)
			require.NoError(t, err)

			db, err := util.InitPostgresDB(logger, storeURL, testSettings)

			// Since we don't have a real postgres instance, we expect connection errors
			// but we can still test the URL parsing logic
			assert.Error(t, err)
			assert.Nil(t, db)

			// Verify it's a service error (connection failure)
			var teranodeErr *errors.Error
			if assert.ErrorAs(t, err, &teranodeErr) {
				assert.Contains(t, err.Error(), "failed to open postgres DB")
			}
		})
	}
}

func TestInitSQLiteDB(t *testing.T) {
	logger := &mockLogger{}

	// Create a temporary directory for file-based sqlite tests
	tempDir := t.TempDir()
	testSettings := &settings.Settings{
		DataFolder: tempDir,
		UtxoStore: settings.UtxoStoreSettings{
			PostgresMaxIdleConns: 10,
			PostgresMaxOpenConns: 80,
		},
	}

	tests := []struct {
		name    string
		scheme  string
		path    string
		wantErr bool
	}{
		{
			name:    "sqlite memory database",
			scheme:  "sqlitememory",
			path:    "/testdb",
			wantErr: false,
		},
		{
			name:    "sqlite file database",
			scheme:  "sqlite",
			path:    "/testdb",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL := &url.URL{
				Scheme: tt.scheme,
				Path:   tt.path,
			}

			db, err := util.InitSQLiteDB(logger, storeURL, testSettings)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, db)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, db)

				if db != nil {
					// Test that foreign keys are enabled
					var foreignKeysEnabled int
					err = db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeysEnabled)
					assert.NoError(t, err)
					assert.Equal(t, 1, foreignKeysEnabled, "Foreign keys should be enabled")

					// Test that locking mode pragma was executed (value may vary by SQLite version)
					var lockingMode string
					err = db.QueryRow("PRAGMA locking_mode").Scan(&lockingMode)
					assert.NoError(t, err)
					assert.Contains(t, []string{"shared", "normal"}, lockingMode, "Locking mode should be set")

					// For file-based databases, check that the file was created
					if tt.scheme == "sqlite" {
						dbName := tt.path[1:] // Remove leading slash
						expectedPath := filepath.Join(tempDir, dbName+".db")
						_, err := os.Stat(expectedPath)
						assert.NoError(t, err, "Database file should be created")
					}

					err := db.Close()
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestInitSQLiteDBDataFolderCreation(t *testing.T) {
	logger := &mockLogger{}

	// Use a non-existent directory to test folder creation
	tempDir := filepath.Join(os.TempDir(), "teranode-test", "sql-test", time.Now().Format("20060102-150405"))
	testSettings := &settings.Settings{
		DataFolder: tempDir,
		UtxoStore: settings.UtxoStoreSettings{
			PostgresMaxIdleConns: 10,
			PostgresMaxOpenConns: 80,
		},
	}

	storeURL := &url.URL{
		Scheme: "sqlite",
		Path:   "/testdb",
	}

	// Ensure the directory doesn't exist initially
	_, err := os.Stat(tempDir)
	assert.True(t, os.IsNotExist(err), "Test directory should not exist initially")

	db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// Verify the directory was created
	stat, err := os.Stat(tempDir)
	assert.NoError(t, err)
	assert.True(t, stat.IsDir(), "Data folder should be created")

	if db != nil {
		err := db.Close()
		assert.NoError(t, err)
	}

	// Cleanup
	err = os.RemoveAll(filepath.Dir(tempDir))
	assert.NoError(t, err)
}

func TestInitSQLiteDBInvalidDataFolder(t *testing.T) {
	logger := &mockLogger{}

	// Try to create a database in a location where we can't create directories
	// This simulates a permission error scenario
	testSettings := &settings.Settings{
		DataFolder: "/root/invalid/path/that/should/not/exist",
		UtxoStore: settings.UtxoStoreSettings{
			PostgresMaxIdleConns: 10,
			PostgresMaxOpenConns: 80,
		},
	}

	storeURL := &url.URL{
		Scheme: "sqlite",
		Path:   "/testdb",
	}

	db, err := util.InitSQLiteDB(logger, storeURL, testSettings)

	// This should fail on most systems due to permissions
	if err != nil {
		assert.Error(t, err)
		assert.Nil(t, db)

		var teranodeErr *errors.Error
		if assert.ErrorAs(t, err, &teranodeErr) {
			assert.Contains(t, err.Error(), "failed to create data folder")
		}
	}
}

func TestSQLEngineConstants(t *testing.T) {
	// Test that the SQLEngine constants have expected values
	assert.Equal(t, "postgres", string(util.Postgres))
	assert.Equal(t, "sqlite", string(util.Sqlite))
	assert.Equal(t, "sqlitememory", string(util.SqliteMemory))
}

// TestSQLiteConnectionString tests various SQLite connection string formats
func TestSQLiteConnectionString(t *testing.T) {
	logger := &mockLogger{}
	tempDir := t.TempDir()

	testSettings := &settings.Settings{
		DataFolder: tempDir,
		UtxoStore: settings.UtxoStoreSettings{
			PostgresMaxIdleConns: 10,
			PostgresMaxOpenConns: 80,
		},
	}

	t.Run("memory database connection string format", func(t *testing.T) {
		storeURL := &url.URL{
			Scheme: "sqlitememory",
			Path:   "/testdb",
		}

		db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer func() {
			err := db.Close()
			assert.NoError(t, err)
		}()

		// Memory databases should work immediately
		_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
		assert.NoError(t, err)
	})

	t.Run("file database with WAL mode", func(t *testing.T) {
		storeURL := &url.URL{
			Scheme: "sqlite",
			Path:   "/waltest",
		}

		db, err := util.InitSQLiteDB(logger, storeURL, testSettings)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer func() {
			err := db.Close()
			assert.NoError(t, err)
		}()

		// Check that WAL mode is enabled
		var journalMode string
		err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
		assert.NoError(t, err)
		assert.Equal(t, "wal", journalMode, "Journal mode should be WAL")

		// Check busy timeout
		var busyTimeout int
		err = db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout)
		assert.NoError(t, err)
		assert.Equal(t, 5000, busyTimeout, "Busy timeout should be 5000ms")
	})
}
