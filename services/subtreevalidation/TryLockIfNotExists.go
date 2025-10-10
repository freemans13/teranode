package subtreevalidation

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// existerIfc defines the interface for checking file existence in blob storage.
//
// This interface abstracts the existence checking functionality needed by the
// subtree validation service to determine whether files already exist in storage
// before attempting operations. It's primarily used in the locking mechanism
// to implement atomic "check-and-create" operations for preventing race conditions.
//
// The interface is typically implemented by blob storage systems that support
// efficient existence checks without requiring full file retrieval, which is
// essential for performance in high-throughput validation scenarios.
type existerIfc interface {
	// Exists checks whether a file exists in the blob storage system.
	//
	// Parameters:
	//   - ctx: Context for cancellation and request-scoped values
	//   - key: The storage key/path to check for existence
	//   - fileType: The type of file being checked (affects storage behavior)
	//   - opts: Optional file-specific options for the existence check
	//
	// Returns:
	//   - bool: true if the file exists, false otherwise
	//   - error: Error if the existence check operation fails
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
}

// QuorumOption defines a functional option for configuring Quorum behavior.
//
// This type follows the functional options pattern commonly used in Go for
// configurable struct initialization. It allows callers to customize Quorum
// instances with specific behaviors while maintaining backward compatibility
// and providing sensible defaults.
//
// QuorumOption functions are typically used with constructor functions or
// configuration methods to modify Quorum instances in a flexible and
// composable manner.
type QuorumOption func(*Quorum)

// WithTimeout configures the timeout duration for individual lock attempts.
//
// This option sets the maximum time to wait for a single lock operation to complete.
// It's used to prevent indefinite blocking when attempting to acquire locks in
// high-contention scenarios. The timeout applies to each individual attempt rather
// than the total operation time.
//
// Parameters:
//   - timeout: Maximum duration to wait for a single lock attempt
//
// Returns:
//   - QuorumOption: A function that applies the timeout configuration to a Quorum instance
//
// Example:
//
//	quorum := NewQuorum(logger, exister, "/locks/subtree", WithTimeout(5*time.Second))
func WithTimeout(timeout time.Duration) QuorumOption {
	return func(q *Quorum) {
		q.timeout = timeout
	}
}

// WithAbsoluteTimeout configures the absolute maximum timeout for the entire lock operation.
//
// This option sets the total maximum time allowed for the complete lock acquisition
// process, including all retry attempts. Once this timeout is reached, the operation
// will fail regardless of individual attempt timeouts. This provides a hard upper
// bound on lock acquisition time.
//
// Parameters:
//   - timeout: Maximum total duration for the entire lock operation
//
// Returns:
//   - QuorumOption: A function that applies the absolute timeout configuration to a Quorum instance
//
// Example:
//
//	quorum := NewQuorum(logger, exister, "/locks/subtree", WithAbsoluteTimeout(30*time.Second))
func WithAbsoluteTimeout(timeout time.Duration) QuorumOption {
	return func(q *Quorum) {
		q.absoluteTimeout = timeout
	}
}

var noopFunc = func() {
	// NOOP
}

// Quorum implements a distributed locking mechanism using file-based coordination.
//
// The Quorum struct provides a way to implement mutual exclusion across multiple
// processes or services by using file existence as a coordination primitive. It's
// particularly useful in distributed systems where multiple instances need to
// coordinate access to shared resources or ensure only one instance performs
// a specific operation at a time.
//
// The locking mechanism works by attempting to create files in a shared storage
// system (typically blob storage) and using the atomic nature of file creation
// to ensure mutual exclusion. The system supports both individual operation
// timeouts and absolute operation timeouts for flexible timeout management.
//
// Key Features:
//   - Distributed coordination using file-based locking
//   - Configurable timeout mechanisms (per-attempt and absolute)
//   - Integration with blob storage systems
//   - Comprehensive logging and error handling
//   - Support for different file types and storage options
//
// Thread Safety:
// Quorum instances are safe for concurrent use from multiple goroutines.
// The underlying file operations and timeout management are handled safely.
type Quorum struct {
	// logger provides structured logging for lock operations and debugging
	logger ulogger.Logger
	// path specifies the base path in storage where lock files are created
	path string
	// timeout defines the maximum duration for individual lock attempts
	timeout time.Duration
	// absoluteTimeout defines the maximum total duration for the entire lock operation
	absoluteTimeout time.Duration
	// fileType specifies the type of files created for locking (affects storage behavior)
	fileType fileformat.FileType
	// exister provides the interface for checking file existence in the storage system
	exister existerIfc
	// existerOpts contains additional options passed to existence check operations
	existerOpts []options.FileOption
}

// NewQuorum creates a new Quorum instance with the specified configuration.
//
// This constructor function initializes a Quorum with the provided parameters and
// applies any optional configuration through the functional options pattern. The
// function validates required parameters and sets sensible defaults for optional
// configuration values.
//
// The function creates a Quorum instance configured for distributed locking
// operations using the provided storage interface and path. Default timeout
// values are applied, but can be overridden using the provided options.
//
// Parameters:
//   - logger: Logger instance for structured logging of lock operations
//   - exister: Interface for checking file existence in the storage system
//   - path: Base path in storage where lock files will be created (must not be empty)
//   - quorumOpts: Optional configuration functions to customize the Quorum behavior
//
// Returns:
//   - *Quorum: Configured Quorum instance ready for locking operations
//   - error: Configuration error if required parameters are invalid
//
// Default Configuration:
//   - Individual timeout: 10 seconds
//   - Absolute timeout: Not set (no absolute limit)
//   - File type: Default file type from storage system
//
// Example Usage:
//
//	quorum, err := NewQuorum(
//	    logger,
//	    blobStore,
//	    "/locks/subtree-validation",
//	    WithTimeout(5*time.Second),
//	    WithAbsoluteTimeout(30*time.Second),
//	)
//	if err != nil {
//	    return fmt.Errorf("failed to create quorum: %w", err)
//	}
func NewQuorum(logger ulogger.Logger, exister existerIfc, path string, quorumOpts ...QuorumOption) (*Quorum, error) {
	if path == "" {
		return nil, errors.NewConfigurationError("Path is required")
	}

	q := &Quorum{
		logger:  logger,
		path:    path,
		exister: exister,
		timeout: 10 * time.Second,
	}

	for _, option := range quorumOpts {
		option(q)
	}

	logger.Infof("Creating subtree quorum path: %s", q.path)

	if err := os.MkdirAll(q.path, 0755); err != nil {
		return nil, errors.NewStorageError("Failed to create quorum path %s", q.path, err)
	}

	return q, nil
}

// TryLockIfFileNotExists attempts to acquire a lock for the given hash if the file does not already exist.
//
// This method checks if a file with the specified hash exists in the storage system.
// If the file does not exist, it attempts to create a lock file atomically.
// If the lock file is successfully created, it returns a release function to remove the lock.
// If the file already exists, it returns that the file exists without acquiring a lock.
//
// Parameters:
//   - ctx: Context for cancellation and request-scoped values
//   - hash: The hash of the file to check and potentially lock
//   - fileType: The type of file being checked (affects storage behavior)
//
// Returns:
//   - locked: true if the lock was successfully acquired, false otherwise
//   - exists: true if the file already exists, false otherwise
//   - release: A function to call to release the lock, or a no-op if no lock was acquired
//   - error: Error if the operation fails, nil if successful
func (q *Quorum) TryLockIfFileNotExists(ctx context.Context, hash *chainhash.Hash, fileType fileformat.FileType) (locked bool, exists bool, release func(), err error) {
	fileExists, err := q.exister.Exists(ctx, hash[:], fileType)
	if err != nil {
		return false, false, noopFunc, err
	}

	// return early if the file already exists, no need to acquire a lock
	if fileExists {
		return false, true, noopFunc, nil
	}

	lockFile := path.Join(q.path, hash.String()) + ".lock"

	q.expireLockIfOld(lockFile)

	// Attempt to acquire lock by atomically creating the lock file
	// The O_CREATE|O_EXCL|O_WRONLY flags ensure the file is created only if it does not already exist
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			q.logger.Debugf("[TryLockIfFileNotExists] lock file %s already exists", lockFile)

			return false, false, noopFunc, nil // Lock held by someone else
		}

		q.logger.Errorf("[TryLockIfFileNotExists] error creating lock file %s: %v", lockFile, err)

		return false, false, noopFunc, err
	}

	// Close the file immediately after creating it
	if err = file.Close(); err != nil {
		q.logger.Warnf("failed to close lock file %q: %v", lockFile, err)
	}

	// Set up automatic lock release
	ctx, cancel := context.WithCancel(ctx)

	go q.autoReleaseLock(ctx, cancel, lockFile)

	return true, false, func() {
		cancel()
		releaseLock(q.logger, lockFile)
	}, nil
}

// If the lock file already exists, the item is being processed by another node. However, the lock may be stale.
// If the lock file mtime is more than quorumTimeout old it is considered stale and can be removed.
func (q *Quorum) expireLockIfOld(lockFile string) {
	if info, err := os.Stat(lockFile); err == nil {
		t := q.timeout
		if q.absoluteTimeout > t {
			t = q.absoluteTimeout
		}

		if time.Since(info.ModTime()) > t {
			q.logger.Warnf("removing stale lock file %q", lockFile)

			releaseLock(q.logger, lockFile)
		}
	}
}

func releaseLock(logger ulogger.Logger, lockFile string) {
	if err := os.Remove(lockFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warnf("failed to remove lock file %q: %v", lockFile, err)
		}
	}
}

func (q *Quorum) autoReleaseLock(ctx context.Context, cancel context.CancelFunc, lockFile string) {
	if q.absoluteTimeout == 0 {
		// Initialize ticker to update the lock file twice every quorumTimeout
		ticker := time.NewTicker(q.timeout / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				releaseLock(q.logger, lockFile)

				return
			case <-ticker.C:
				// Touch the lock file by updating its access and modification times to the current time
				now := time.Now()
				if err := os.Chtimes(lockFile, now, now); err != nil {
					q.logger.Warnf("failed to update lock file %q: %v", lockFile, err)
				}
			}
		}
	} else {
		// If absoluteTimeout is set, we use a timer to release the lock after the timeout
		// Release the lock after timeout or when context is cancelled
		select {
		case <-ctx.Done():
		case <-time.After(q.absoluteTimeout):
		}
		releaseLock(q.logger, lockFile)
	}
}

// TryLockIfNotExistsWithTimeout attempts to acquire a lock for the given hash.
// Knowing a lock has a timeout, it keeps retrying just beyond the lock timeout or until the file exists.
// In theory it will always succeed in getting a lock or returning that the file exists.
func (q *Quorum) TryLockIfNotExistsWithTimeout(ctx context.Context, hash *chainhash.Hash, fileType fileformat.FileType) (locked bool, exists bool, release func(), err error) {
	t := q.timeout
	if q.absoluteTimeout > t {
		t = q.absoluteTimeout
	}

	t += 1 * time.Second

	cancelCtx, cancel := context.WithTimeout(ctx, t)
	defer cancel()

	const retryDelay = 10 * time.Millisecond // How long to wait between checks

	retryCount := 0

	for {
		locked, exists, release, err = q.TryLockIfFileNotExists(ctx, hash, fileType)
		if err != nil || exists {
			return locked, exists, release, err
		}

		if locked {
			q.logger.Debugf("[TryLockIfNotExistsWithTimeout][%s] Lock acquired after %d retries", hash, retryCount)
			return locked, exists, release, nil
		}

		retryCount++

		// Lock not acquired and item doesn't exist yet, wait briefly before retrying.
		// Use a context-aware delay to exit promptly if context is cancelled during the wait.
		select {
		case <-time.After(retryDelay):
			// Sleep finished normally, continue loop.
		case <-cancelCtx.Done(): // This channel will be closed if ctx is done OR if timeout t is reached
			// Check if the parent context (ctx) is the reason cancelCtx is done.
			// ctx.Err() is non-nil if ctx is cancelled.
			if ctx.Err() != nil {
				// Parent context was cancelled.
				return locked, exists, release, errors.NewStorageError("[TryLockIfNotExistsWithTimeout][%s] context done during retry delay", hash)
			}
			// If ctx.Err() is nil, it means cancelCtx timed out on its own (due to 't').
			return locked, exists, release, errors.NewStorageError("[TryLockIfNotExistsWithTimeout][%s] timeout waiting %s for lock to free up during retry delay", hash, t)
		}
	}
}

// AcquirePauseLock acquires a distributed pause lock for subtree processing.
//
// This method creates a lock file in the quorum path that signals all subtree validation
// pods to pause their processing. The lock is automatically kept alive through periodic
// heartbeat updates (via autoReleaseLock) and is automatically released when:
// - The context is cancelled
// - The absolute timeout expires (if configured)
// - The calling pod crashes (lock becomes stale and is cleaned up by other pods)
//
// This enables coordinated pausing across multiple pods during operations like block
// validation that require exclusive access to the UTXO state.
//
// Parameters:
//   - ctx: Context for cancellation and request-scoped values
//
// Returns:
//   - release: Function to call to explicitly release the pause lock
//   - error: Error if the lock cannot be acquired
func (q *Quorum) AcquirePauseLock(ctx context.Context) (release func(), err error) {
	pauseLockFile := path.Join(q.path, "__SUBTREE_PAUSE__.lock")

	q.expireLockIfOld(pauseLockFile)

	// Attempt to acquire lock by atomically creating the lock file
	file, err := os.OpenFile(pauseLockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			q.logger.Debugf("[AcquirePauseLock] pause lock file already exists - another pod holds the lock")
			return noopFunc, errors.NewStorageError("pause lock already held by another pod")
		}

		q.logger.Errorf("[AcquirePauseLock] error creating pause lock file %s: %v", pauseLockFile, err)
		return noopFunc, err
	}

	if err = file.Close(); err != nil {
		q.logger.Warnf("failed to close pause lock file %q: %v", pauseLockFile, err)
	}

	// Set up automatic lock release with heartbeat updates
	lockCtx, cancel := context.WithCancel(ctx)
	go q.autoReleaseLock(lockCtx, cancel, pauseLockFile)

	q.logger.Infof("[AcquirePauseLock] Successfully acquired distributed pause lock")

	return func() {
		cancel()
		releaseLock(q.logger, pauseLockFile)
		q.logger.Infof("[AcquirePauseLock] Released distributed pause lock")
	}, nil
}

// IsPauseActive checks if any pod currently holds a valid pause lock.
//
// This method checks for the existence of the pause lock file and verifies that
// it is not stale (i.e., the holding pod is still actively updating the heartbeat).
// If a stale lock is found, it is automatically cleaned up.
//
// This enables all pods to coordinate their pausing behavior by checking a shared
// lock file in the quorum path, ensuring that when one pod initiates a pause
// (e.g., during block validation), all other pods honor that pause.
//
// Returns:
//   - bool: true if a valid pause lock exists, false otherwise
func (q *Quorum) IsPauseActive() bool {
	pauseLockFile := path.Join(q.path, "__SUBTREE_PAUSE__.lock")

	// First clean up any stale locks
	q.expireLockIfOld(pauseLockFile)

	// Check if lock file still exists after cleanup
	_, err := os.Stat(pauseLockFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		q.logger.Warnf("[IsPauseActive] error checking pause lock file: %v", err)
		return false
	}

	return true
}
