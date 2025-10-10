package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock Exister
type mockExister struct {
	mu     sync.Mutex
	exists bool
	err    error
}

func newMockExister(exists bool) *mockExister {
	return &mockExister{exists: exists}
}

func (m *mockExister) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return false, m.err
	}
	// Simulate check based on hash string representation
	return m.exists, nil
}

func (m *mockExister) SetExists(exists bool) {
	m.mu.Lock()
	m.exists = exists
	m.mu.Unlock()
}

// Generate test hash
func GenerateTestHash(suffix string) *chainhash.Hash {
	// Ensure suffix length makes the total 64 chars
	baseLen := 64 - len(suffix)
	if baseLen < 0 {
		panic("suffix too long for GenerateTestHash")
	}

	base := "0000000000000000000000000000000000000000000000000000000000000000" // 64 zeros
	hashStr := base[:baseLen] + suffix

	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		// Panic is acceptable in test helpers if setup fails fundamentally
		panic(fmt.Sprintf("failed to generate test hash from string '%s': %v", hashStr, err))
	}

	return hash
}

const (
	quorumOpTimeout = 100 * time.Millisecond
	testLoopTimeout = 4000 * time.Millisecond
)

func TestTryLockIfNotExistsWithTimeout(t *testing.T) {
	testHash := GenerateTestHash("01")

	var logger ulogger.Logger = ulogger.TestLogger{}

	// Scenario 1: Acquire successfully
	t.Run("AcquireLockSuccessfully", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(quorumOpTimeout))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), testLoopTimeout)
		defer cancel()

		locked, exists, release, err := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)

		require.NoError(t, err)
		assert.True(t, locked)
		assert.False(t, exists)
		assert.NotNil(t, release)

		lockFilePath := filepath.Join(q.path, testHash.String()+".lock") // Add .lock suffix
		_, statErr := os.Stat(lockFilePath)
		assert.NoError(t, statErr, "Lock file should be created")
		release()

		_, statErr = os.Stat(lockFilePath)
		assert.True(t, os.IsNotExist(statErr), "Lock file should be removed")
	})

	// Scenario 2: Item Exists (via exister) -> returns immediately without error
	t.Run("ItemExistsViaExisterReturnsImmediately", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(true)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(quorumOpTimeout))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), testLoopTimeout)
		defer cancel()

		locked, exists, release, err := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)

		// Expect no error, exists=true, locked=false
		require.NoError(t, err)
		assert.False(t, locked)
		assert.True(t, exists, "Exists should be true as mock reported it")
		assert.NotNil(t, release) // Expect noopFunc, not nil
	})

	// Scenario 3: Acquire after release
	t.Run("AcquireAfterRelease", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(quorumOpTimeout))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), testLoopTimeout)

		defer cancel()

		// Acquire and release first
		exister.SetExists(false)

		locked1, _, release1, err1 := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)
		require.NoError(t, err1)
		require.True(t, locked1)
		release1()

		lockFilePath := filepath.Join(q.path, testHash.String()+".lock")
		_, statErr := os.Stat(lockFilePath)
		require.True(t, os.IsNotExist(statErr), "Lock file should be gone after release1")

		// Try acquire again
		exister.SetExists(false) // Mock still says false

		locked2, exists2, release2, err2 := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)
		require.NoError(t, err2)
		assert.True(t, locked2)
		assert.False(t, exists2)
		assert.NotNil(t, release2)

		defer release2()
	})

	// Scenario 4: Item Exists (via exister) -> returns immediately, ignores context cancellation
	t.Run("ItemExistsViaExisterIgnoresContextCancel", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(true)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(quorumOpTimeout))
		require.NoError(t, err)

		ctxCancelled, cancel := context.WithCancel(context.Background())
		defer cancel() // Cancel eventually, though it shouldn't matter

		locked, exists, release, lockErr := q.TryLockIfNotExistsWithTimeout(ctxCancelled, testHash, fileformat.FileTypeSubtree)

		// Expect no error, exists=true, locked=false
		require.NoError(t, lockErr)
		assert.False(t, locked)
		assert.True(t, exists, "Exists should be true as mock reported it")
		assert.NotNil(t, release) // Expect noopFunc, not nil
	})

	// Scenario 5: Lock file exists -> internal timeout
	t.Run("LockFileExistsInternalTimeout", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false) // Mock says item does NOT exist
		// Use a very short timeout for this test
		shortQuorumOpTimeout := 40 * time.Millisecond
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(shortQuorumOpTimeout))
		require.NoError(t, err)

		// Manually create the .lock file
		lockFilePath := filepath.Join(q.path, testHash.String()+".lock")
		require.NoError(t, os.WriteFile(lockFilePath, []byte("locked"), 0600))
		defer os.Remove(lockFilePath) // Ensure cleanup

		// Context with a timeout longer than the quorum op timeout, but short enough for the test
		ctx, cancel := context.WithTimeout(context.Background(), shortQuorumOpTimeout*10)
		defer cancel()

		locked, exists, release, err := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)

		// Expect the lock to be acquired successfully after the initial file becomes stale
		require.NoError(t, err, "Error should be nil as stale lock should be removed and lock acquired")
		assert.True(t, locked, "Lock should have been acquired")
		assert.False(t, exists, "Exists should be false as exister reported it so")
		assert.NotNil(t, release, "Release function should not be nil")
		// Call release to clean up the newly acquired lock
		if release != nil {
			release()
		}
	})

	// Scenario 6: Lock file exists -> external context cancelled
	t.Run("LockFileExistsExternalContextCancelled", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		// Use a longer timeout for the quorum itself to ensure staleness doesn't trigger first
		// while we wait to cancel the context externally.
		longQuorumOpTimeout := 1 * time.Second
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(longQuorumOpTimeout))
		require.NoError(t, err)

		// Manually create the .lock file
		lockFilePath := filepath.Join(q.path, testHash.String()+".lock")
		require.NoError(t, os.WriteFile(lockFilePath, []byte("locked"), 0600))
		defer os.Remove(lockFilePath) // Ensure cleanup

		ctxCancelled, cancel := context.WithCancel(context.Background())

		var (
			wg      sync.WaitGroup
			lockErr error
			locked  bool
			exists  bool
			release func()
		)

		wg.Add(1)

		go func() {
			defer wg.Done()
			// Use a longer timeout in the outer test context just to ensure the goroutine runs long enough
			ctxTest, cancelTest := context.WithTimeout(ctxCancelled, testLoopTimeout) // testLoopTimeout = 4000ms
			defer cancelTest()

			// Pass the cancellable context ctxTest here
			locked, exists, release, lockErr = q.TryLockIfNotExistsWithTimeout(ctxTest, testHash, fileformat.FileTypeSubtree)
		}()

		// Allow the goroutine to enter the wait loop, but cancel BEFORE the lock becomes stale.
		// longQuorumOpTimeout is 500ms, so sleeping 50ms is safeconversion.
		time.Sleep(500 * time.Millisecond) // Reduced sleep time
		cancel()                           // Cancel the context passed to the function (ctxTest derives from ctxCancelled)
		wg.Wait()                          // Wait for the goroutine to finish

		require.Error(t, lockErr, "An error is expected but got nil.")
		// Expecting the wrapper to detect the outer context cancellation
		assert.Contains(t, lockErr.Error(), "context done", "Error message should indicate outer context finished")
		assert.False(t, locked)
		assert.False(t, exists, "Exists should be false as exister reported it so")
		assert.NotNil(t, release) // Expect noopFunc from last loop iteration before context check
	})

	// Scenario 7: Already exists
	t.Run("AlreadyExists", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(true)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(quorumOpTimeout))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), testLoopTimeout)
		defer cancel()

		locked, exists, releaseFunc, err := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)

		// Expect no error, exists=true, locked=false
		require.NoError(t, err)
		assert.False(t, locked)
		assert.True(t, exists, "Exists should be true as mock reported it")
		assert.NotNil(t, releaseFunc) // Expect noopFunc, not nil

		// Attempt to lock again
		lockedAgain, existsAgain, releaseFuncAgain, errAgain := q.TryLockIfNotExistsWithTimeout(ctx, testHash, fileformat.FileTypeSubtree)
		assert.NoError(t, errAgain, "Should not error when lock already exists")
		assert.False(t, lockedAgain, "Should not acquire lock when it already exists")
		assert.True(t, existsAgain, "Exists should still be true on second check") // Add assertion for existsAgain
		assert.NotNil(t, releaseFuncAgain, "Release function should not be nil even if not acquired")
		// Function comparison in Go compares pointers. Check if the returned function is the specific noopFunc.
		assert.Equal(t, fmt.Sprintf("%p", noopFunc), fmt.Sprintf("%p", releaseFuncAgain), "Release function should be noopFunc when lock exists")

		// Release the original lock
		releaseFunc()
	})

	t.Run("RetryWarningWhenLockExpiresAfterMultipleAttempts", func(t *testing.T) {
		clogger := newCapturingLogger()
		tempDir := t.TempDir()
		exister := newMockExister(false) // We are testing lock contention, not existence via exister.
		localTestHash := GenerateTestHash("AABBCCDD01")

		lockFilePath := filepath.Join(tempDir, localTestHash.String()+".lock")

		// retryDelay in TryLockIfNotExistsWithTimeout is 10ms.
		// We want the lock to be considered stale after 2 retries (3rd attempt).
		// Attempt 1 (retryCount=0): age ~0ms. Need age < quorumTimeout. Fails.
		// After 10ms delay:
		// Attempt 2 (retryCount=1): age ~10ms. Need age < quorumTimeout. Fails.
		// After 10ms delay:
		// Attempt 3 (retryCount=2): age ~20ms. Need age > quorumTimeout. Succeeds.
		// So, 10ms < quorumTimeout < 20ms. Let quorumTimeout = 15ms.
		quorumTimeout := 15 * time.Millisecond

		q, err := NewQuorum(clogger, exister, tempDir, WithTimeout(quorumTimeout))
		require.NoError(t, err)

		// Pre-create the lock file to simulate it being held by another process.
		// Its ModTime will be checked by expireLockIfOld.
		initialLockFile, err := os.Create(lockFilePath)
		require.NoError(t, err, "Failed to pre-create lock file")
		require.NoError(t, initialLockFile.Close(), "Failed to close pre-created lock file")

		// Set a general timeout for the TryLockIfNotExistsWithTimeout operation itself.
		// This should be generous enough for the retries to occur.
		// The internal timeout 't' in TryLockIfNotExistsWithTimeout will be quorumTimeout + 1s.
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()

		locked, exists, release, err := q.TryLockIfNotExistsWithTimeout(ctx, localTestHash, fileformat.FileTypeSubtree)

		require.NoError(t, err, "TryLockIfNotExistsWithTimeout returned an error")
		assert.True(t, locked, "Lock should have been acquired after previous one expired")
		assert.False(t, exists, "Exists should be false as exister is configured to return false")
		require.NotNil(t, release, "Release function should not be nil")

		if release != nil {
			release()
		}

		assert.NoFileExists(t, lockFilePath, "Lock file should be removed after release")
	})
}

func TestNewQuorumWithOptions(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	path := t.TempDir()
	mockExister := &mockExister{}

	t.Run("Defaults", func(t *testing.T) {
		q, err := NewQuorum(logger, mockExister, path)
		require.NoError(t, err)
		assert.Equal(t, 10*time.Second, q.timeout, "Default timeout should be 10s")
		assert.Equal(t, time.Duration(0), q.absoluteTimeout, "Default absoluteTimeout should be 0")
		assert.Equal(t, "", q.fileType.String(), "Default extension should be empty")
	})

	t.Run("WithAbsoluteTimeout", func(t *testing.T) {
		absTimeout := 5 * time.Minute
		q, err := NewQuorum(logger, mockExister, path, WithAbsoluteTimeout(absTimeout))
		require.NoError(t, err)
		assert.Equal(t, absTimeout, q.absoluteTimeout)
	})

	t.Run("WithPathEmpty", func(t *testing.T) {
		_, err := NewQuorum(logger, mockExister, "")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrConfiguration), "Error should be the predefined terrors.ErrConfiguration")
	})
}

func TestAutoReleaseLockModes(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	tempDir := t.TempDir()
	baseHash := chainhash.HashH([]byte("test"))

	// --- Mode 1: absoluteTimeout == 0 --- //
	t.Run("AbsoluteTimeoutNotSet", func(t *testing.T) {
		quorumTimeout := 100 * time.Millisecond
		lockFileName := baseHash.String() + "_mode1.lock"
		lockFilePath := filepath.Join(tempDir, lockFileName)

		q := &Quorum{
			path:            tempDir,
			logger:          logger,
			timeout:         quorumTimeout, // Used for ticker
			absoluteTimeout: 0,             // Mode 1 trigger
			exister:         nil,
		}

		t.Run("ContextCancelledRemovesLock", func(t *testing.T) {
			// Create lock file
			file, err := os.Create(lockFilePath)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			require.FileExists(t, lockFilePath)

			parentCtx, parentCancel := context.WithCancel(context.Background())
			defer parentCancel()

			autoReleaseCtx, autoReleaseCancel := context.WithCancel(parentCtx)

			var wg sync.WaitGroup

			wg.Add(1)

			go func() {
				defer wg.Done()
				q.autoReleaseLock(autoReleaseCtx, autoReleaseCancel, lockFilePath)
			}()

			time.Sleep(quorumTimeout / 4) // Wait less than tick interval
			autoReleaseCancel()           // Cancel the context
			wg.Wait()                     // Wait for goroutine to remove lock and exit

			assert.NoFileExists(t, lockFilePath, "Lock file should be removed on context cancellation")
		})

		t.Run("TickerUpdatesModTime_CancelRemovesLock", func(t *testing.T) {
			// Create lock file
			file, err := os.Create(lockFilePath)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			require.FileExists(t, lockFilePath)

			initialStat, err := os.Stat(lockFilePath)
			require.NoError(t, err)

			initialModTime := initialStat.ModTime()

			parentCtx, parentCancel := context.WithCancel(context.Background())
			defer parentCancel()

			autoReleaseCtx, autoReleaseCancel := context.WithCancel(parentCtx)

			var wg sync.WaitGroup

			wg.Add(1)

			go func() {
				defer wg.Done()
				q.autoReleaseLock(autoReleaseCtx, autoReleaseCancel, lockFilePath)
			}()

			// Wait longer than one tick interval (timeout/2) but less than full timeout
			time.Sleep(quorumTimeout * 3 / 4)

			// Check lock still exists and mod time updated
			assert.FileExists(t, lockFilePath, "Lock file should still exist after ticker fires")
			currentStat, err := os.Stat(lockFilePath)
			require.NoError(t, err)
			assert.True(t, currentStat.ModTime().After(initialModTime), "ModTime should be updated by ticker")

			// Now cancel context to trigger removal
			autoReleaseCancel()
			wg.Wait()

			assert.NoFileExists(t, lockFilePath, "Lock file should be removed after context cancellation")
		})
	})

	// --- Mode 2: absoluteTimeout > 0 --- //
	t.Run("AbsoluteTimeoutSet", func(t *testing.T) {
		absTimeout := 50 * time.Millisecond
		lockFileName := baseHash.String() + "_mode2.lock"
		lockFilePath := filepath.Join(tempDir, lockFileName)

		q := &Quorum{
			path:            tempDir,
			logger:          logger,
			timeout:         0,          // Not used in this mode
			absoluteTimeout: absTimeout, // Mode 2 trigger
			exister:         nil,
		}

		t.Run("TimeoutRemovesLock", func(t *testing.T) {
			// Create lock file
			file, err := os.Create(lockFilePath)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			require.FileExists(t, lockFilePath)

			parentCtx, parentCancel := context.WithCancel(context.Background())
			defer parentCancel()

			autoReleaseCtx, autoReleaseCancel := context.WithCancel(parentCtx)

			var wg sync.WaitGroup

			wg.Add(1)

			go func() {
				defer wg.Done()
				q.autoReleaseLock(autoReleaseCtx, autoReleaseCancel, lockFilePath)
			}()

			time.Sleep(absTimeout + 30*time.Millisecond) // Wait longer than absTimeout
			wg.Wait()                                    // Wait for goroutine to remove lock and exit

			assert.NoFileExists(t, lockFilePath, "Lock file should be removed after absoluteTimeout")
			// Check context was NOT cancelled by the timeout path
			select {
			case <-autoReleaseCtx.Done():
				t.Fatal("autoReleaseCtx should not be cancelled when absoluteTimeout occurs")
			default: // expected
			}
		})

		t.Run("ContextCancelledRemovesLock", func(t *testing.T) {
			// Create lock file
			file, err := os.Create(lockFilePath)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			require.FileExists(t, lockFilePath)

			parentCtx, parentCancel := context.WithCancel(context.Background())
			defer parentCancel()

			autoReleaseCtx, autoReleaseCancel := context.WithCancel(parentCtx)

			var wg sync.WaitGroup

			wg.Add(1)

			go func() {
				defer wg.Done()
				q.autoReleaseLock(autoReleaseCtx, autoReleaseCancel, lockFilePath)
			}()

			time.Sleep(absTimeout / 2) // Wait less than absTimeout
			autoReleaseCancel()        // Cancel context before timeout
			wg.Wait()                  // Wait for goroutine to remove lock and exit

			assert.NoFileExists(t, lockFilePath, "Lock file should be removed on context cancellation")
			// Check context WAS cancelled
			select {
			case <-autoReleaseCtx.Done(): // expected
			default:
				t.Fatal("autoReleaseCtx should be cancelled")
			}
			// Clean up lock file just in case test failed before removal
			os.Remove(lockFilePath)
		})
	})
}

// capturingLogger captures log messages for testing
type capturingLogger struct {
	ulogger.TestLogger
	WarnBuf *bytes.Buffer
}

func newCapturingLogger() *capturingLogger {
	buf := &bytes.Buffer{}

	return &capturingLogger{
		TestLogger: ulogger.TestLogger{},
		WarnBuf:    buf,
	}
}

func (m *capturingLogger) Warnf(format string, args ...interface{}) {
	// Basic formatting to capture the essence
	fmt.Fprintf(m.WarnBuf, format, args...)
	m.WarnBuf.WriteString("\n") // Add newline for easier assertion
}

func Test_releaseLock(t *testing.T) {
	testDir := t.TempDir()

	t.Run("ErrNotExist_NoErrorLogged", func(t *testing.T) {
		mLogger := newCapturingLogger()
		lockFile := filepath.Join(testDir, "non_existent_lock")

		releaseLock(mLogger, lockFile)

		assert.Empty(t, mLogger.WarnBuf.String(), "No warning should be logged for ErrNotExist")
	})

	t.Run("OtherError_LogsWarning", func(t *testing.T) {
		mLogger := newCapturingLogger()
		// Simulate an error other than NotExist by trying to remove a directory
		lockDir := filepath.Join(testDir, "lock_as_dir")
		err := os.Mkdir(lockDir, 0755)
		require.NoError(t, err)

		// Create a file inside the directory to make it non-empty
		dummyFile, err := os.Create(filepath.Join(lockDir, "dummy"))
		require.NoError(t, err)
		require.NoError(t, dummyFile.Close())

		removeErr := os.Remove(lockDir)
		require.Error(t, removeErr, "os.Remove should have returned an error for a non-empty directory")

		// Replicate releaseLock logic for assertion
		if removeErr != nil {
			if !errors.Is(removeErr, os.ErrNotExist) {
				mLogger.Warnf("failed to remove lock file %q: %v", lockDir, removeErr)
			}
		}

		assert.NotEmpty(t, mLogger.WarnBuf.String(), "Warning should be logged for errors other than ErrNotExist")
		assert.Contains(t, mLogger.WarnBuf.String(), fmt.Sprintf("failed to remove lock file %q", lockDir), "Warning message should contain expected format")
	})
}

func TestAcquirePauseLock(t *testing.T) {
	var logger ulogger.Logger = ulogger.TestLogger{}

	t.Run("AcquirePauseLockSuccessfully", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()
		release, err := q.AcquirePauseLock(ctx)

		require.NoError(t, err)
		require.NotNil(t, release)

		pauseLockPath := filepath.Join(q.path, "__SUBTREE_PAUSE__.lock")
		_, statErr := os.Stat(pauseLockPath)
		assert.NoError(t, statErr, "Pause lock file should be created")

		assert.True(t, q.IsPauseActive(), "Pause should be active after acquiring lock")

		release()

		time.Sleep(50 * time.Millisecond)

		_, statErr = os.Stat(pauseLockPath)
		assert.True(t, os.IsNotExist(statErr), "Pause lock file should be removed after release")

		assert.False(t, q.IsPauseActive(), "Pause should not be active after release")
	})

	t.Run("AcquirePauseLockTwiceFails", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()
		release1, err1 := q.AcquirePauseLock(ctx)
		require.NoError(t, err1)
		require.NotNil(t, release1)
		defer release1()

		release2, err2 := q.AcquirePauseLock(ctx)
		assert.Error(t, err2, "Second lock acquisition should fail")
		assert.Contains(t, err2.Error(), "pause lock already held", "Error message should indicate lock is held")
		assert.NotNil(t, release2, "Release function should be returned even on error")
	})

	t.Run("AcquirePauseLockAfterRelease", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()

		release1, err1 := q.AcquirePauseLock(ctx)
		require.NoError(t, err1)
		require.NotNil(t, release1)

		release1()
		time.Sleep(50 * time.Millisecond)

		release2, err2 := q.AcquirePauseLock(ctx)
		require.NoError(t, err2, "Should be able to acquire lock after release")
		require.NotNil(t, release2)
		defer release2()

		assert.True(t, q.IsPauseActive(), "Pause should be active after second acquisition")
	})

	t.Run("PauseLockWithContextCancellation", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		release, err := q.AcquirePauseLock(ctx)
		require.NoError(t, err)
		require.NotNil(t, release)

		assert.True(t, q.IsPauseActive(), "Pause should be active")

		cancel()
		time.Sleep(150 * time.Millisecond)

		assert.False(t, q.IsPauseActive(), "Pause should be inactive after context cancellation")
	})

	t.Run("PauseLockWithAbsoluteTimeout", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir,
			WithTimeout(100*time.Millisecond),
			WithAbsoluteTimeout(200*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()
		release, err := q.AcquirePauseLock(ctx)
		require.NoError(t, err)
		require.NotNil(t, release)
		defer release()

		assert.True(t, q.IsPauseActive(), "Pause should be active")

		time.Sleep(250 * time.Millisecond)

		assert.False(t, q.IsPauseActive(), "Pause should expire after absolute timeout")
	})
}

func TestIsPauseActive(t *testing.T) {
	var logger ulogger.Logger = ulogger.TestLogger{}

	t.Run("NoPauseLock", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		assert.False(t, q.IsPauseActive(), "Should return false when no pause lock exists")
	})

	t.Run("ActivePauseLock", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()
		release, err := q.AcquirePauseLock(ctx)
		require.NoError(t, err)
		defer release()

		assert.True(t, q.IsPauseActive(), "Should return true when pause lock is active")
	})

	t.Run("StalePauseLockIsCleanedUp", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(50*time.Millisecond))
		require.NoError(t, err)

		pauseLockPath := filepath.Join(q.path, "__SUBTREE_PAUSE__.lock")

		file, err := os.Create(pauseLockPath)
		require.NoError(t, err)
		file.Close()

		oldTime := time.Now().Add(-100 * time.Millisecond)
		err = os.Chtimes(pauseLockPath, oldTime, oldTime)
		require.NoError(t, err)

		assert.False(t, q.IsPauseActive(), "Should return false and clean up stale lock")

		_, statErr := os.Stat(pauseLockPath)
		assert.True(t, os.IsNotExist(statErr), "Stale lock file should be removed")
	})
}

func TestPauseLockHeartbeat(t *testing.T) {
	var logger ulogger.Logger = ulogger.TestLogger{}

	t.Run("LockFileIsKeptFreshWithHeartbeat", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)
		q, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()
		release, err := q.AcquirePauseLock(ctx)
		require.NoError(t, err)
		defer release()

		pauseLockPath := filepath.Join(q.path, "__SUBTREE_PAUSE__.lock")

		info1, err := os.Stat(pauseLockPath)
		require.NoError(t, err)
		mtime1 := info1.ModTime()

		time.Sleep(80 * time.Millisecond)

		info2, err := os.Stat(pauseLockPath)
		require.NoError(t, err)
		mtime2 := info2.ModTime()

		assert.True(t, mtime2.After(mtime1), "Lock file should be updated by heartbeat")
		assert.True(t, q.IsPauseActive(), "Pause should still be active after heartbeat update")
	})
}

func TestMultiplePodsPauseCoordination(t *testing.T) {
	var logger ulogger.Logger = ulogger.TestLogger{}

	t.Run("TwoPodsCannotAcquirePauseLockSimultaneously", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)

		q1, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		q2, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()

		release1, err1 := q1.AcquirePauseLock(ctx)
		require.NoError(t, err1)
		require.NotNil(t, release1)
		defer release1()

		assert.True(t, q1.IsPauseActive(), "Pod 1 should see pause as active")
		assert.True(t, q2.IsPauseActive(), "Pod 2 should see pause as active")

		release2, err2 := q2.AcquirePauseLock(ctx)
		assert.Error(t, err2, "Pod 2 should fail to acquire lock")
		assert.NotNil(t, release2)

		assert.True(t, q1.IsPauseActive(), "Pod 1 should still see pause as active")
		assert.True(t, q2.IsPauseActive(), "Pod 2 should still see pause as active")
	})

	t.Run("SecondPodCanAcquireLockAfterFirstReleases", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)

		q1, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		q2, err := NewQuorum(logger, exister, tempDir, WithTimeout(100*time.Millisecond))
		require.NoError(t, err)

		ctx := context.Background()

		release1, err1 := q1.AcquirePauseLock(ctx)
		require.NoError(t, err1)
		defer release1()

		assert.True(t, q1.IsPauseActive())
		assert.True(t, q2.IsPauseActive())

		release1()
		time.Sleep(50 * time.Millisecond)

		assert.False(t, q1.IsPauseActive(), "Pod 1 should see pause as inactive")
		assert.False(t, q2.IsPauseActive(), "Pod 2 should see pause as inactive")

		release2, err2 := q2.AcquirePauseLock(ctx)
		require.NoError(t, err2, "Pod 2 should be able to acquire lock")
		defer release2()

		assert.True(t, q1.IsPauseActive(), "Pod 1 should see pause as active again")
		assert.True(t, q2.IsPauseActive(), "Pod 2 should see pause as active")
	})

	t.Run("PodCrashScenario", func(t *testing.T) {
		tempDir := t.TempDir()
		exister := newMockExister(false)

		q1, err := NewQuorum(logger, exister, tempDir, WithTimeout(1*time.Second))
		require.NoError(t, err)

		q2, err := NewQuorum(logger, exister, tempDir, WithTimeout(1*time.Second))
		require.NoError(t, err)

		ctx := context.Background()

		release1, err1 := q1.AcquirePauseLock(ctx)
		require.NoError(t, err1)

		assert.True(t, q1.IsPauseActive())
		assert.True(t, q2.IsPauseActive())

		release1()

		// Give some time for background goroutine to finish cleanup
		time.Sleep(10 * time.Millisecond)

		pauseLockPath := filepath.Join(q1.path, "__SUBTREE_PAUSE__.lock")
		file, err := os.Create(pauseLockPath)
		require.NoError(t, err)
		file.Close()

		oldTime := time.Now().Add(-2 * time.Second)
		err = os.Chtimes(pauseLockPath, oldTime, oldTime)
		require.NoError(t, err)

		assert.False(t, q2.IsPauseActive(), "Pod 2 should detect stale lock and clean it up")

		release2, err2 := q2.AcquirePauseLock(ctx)
		require.NoError(t, err2, "Pod 2 should be able to acquire lock after cleaning up stale lock")
		defer release2()

		assert.True(t, q2.IsPauseActive())
	})
}
