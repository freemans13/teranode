package file

import (
	"context"
	"io"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestSeparateSemaphoresPreventDeadlock simulates the original deadlock scenario
// where write operations hold semaphore slots while blocked on pipe reads, and
// read operations need semaphore slots to provide data to those pipes.
//
// This test verifies that with separate read/write semaphores, the deadlock
// pattern is eliminated.
func TestSeparateSemaphoresPreventDeadlock(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "deadlock-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	ctx := context.Background()

	// Test parameters that simulate the original deadlock scenario
	const (
		numWriters       = 100 // Simulate many concurrent write operations
		numReaders       = 10  // Simulate a few read operations that writes depend on
		writeBlockTimeMs = 100 // How long writes block waiting for data
		testTimeoutSec   = 30  // Test should complete well within this time
	)

	// Track test progress
	var (
		writesStarted   atomic.Int32
		writesCompleted atomic.Int32
		readsCompleted  atomic.Int32
	)

	testCtx, cancel := context.WithTimeout(ctx, testTimeoutSec*time.Second)
	defer cancel()

	// errgroup for coordinating goroutines
	var wg sync.WaitGroup

	// Step 1: Start many write operations that will block on pipe reads
	// This simulates the SetFromReader operations that were holding semaphore slots
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			writesStarted.Add(1)

			// Create a pipe to simulate blocking write
			reader, writer := io.Pipe()

			// Start the write operation (this acquires writeSemaphore)
			writeKey := []byte{byte(id >> 8), byte(id)}
			writeErr := make(chan error, 1)
			go func() {
				err := store.SetFromReader(testCtx, writeKey, fileformat.FileTypeTesting, reader)
				writeErr <- err
			}()

			// Simulate the write blocking while waiting for data
			// In the real deadlock, this was the ProcessSubtree goroutine that
			// needed to do Exists checks (reads) before writing data
			time.Sleep(time.Duration(writeBlockTimeMs) * time.Millisecond)

			// Now write some data to the pipe
			data := []byte("test data for writer")
			_, err := writer.Write(data)
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			writer.Close()

			// Wait for write to complete
			select {
			case err := <-writeErr:
				if err != nil {
					t.Errorf("Write operation %d failed: %v", id, err)
				}
			case <-testCtx.Done():
				t.Errorf("Write operation %d timed out", id)
			}

			writesCompleted.Add(1)
		}(i)
	}

	// Step 2: While writes are blocking, start read operations
	// This simulates the Exists checks that ProcessSubtree needed to do
	// In the deadlock scenario, these couldn't get semaphore slots
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			// Give writes time to start and acquire semaphore slots
			time.Sleep(50 * time.Millisecond)

			// Perform multiple read operations (Exists checks)
			for j := 0; j < 5; j++ {
				readKey := []byte{byte(id), byte(j)}

				// This should NOT deadlock with separate semaphores
				// because reads use readSemaphore, not writeSemaphore
				exists, err := store.Exists(testCtx, readKey, fileformat.FileTypeTesting)
				if err != nil && err != io.EOF {
					t.Errorf("Read operation %d failed: %v", id, err)
					return
				}
				_ = exists // We don't care about the result, just that it doesn't deadlock
			}

			readsCompleted.Add(1)
		}(i)
	}

	// Wait for all operations to complete (or timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success! All operations completed
		t.Logf("Test completed successfully:")
		t.Logf("  Writes started: %d", writesStarted.Load())
		t.Logf("  Writes completed: %d", writesCompleted.Load())
		t.Logf("  Reads completed: %d", readsCompleted.Load())

		// Verify all operations completed
		require.Equal(t, int32(numWriters), writesStarted.Load(), "All writes should have started")
		require.Equal(t, int32(numWriters), writesCompleted.Load(), "All writes should have completed")
		require.Equal(t, int32(numReaders), readsCompleted.Load(), "All reads should have completed")

	case <-testCtx.Done():
		t.Fatalf("TEST DEADLOCK DETECTED: Operations did not complete within %d seconds\n"+
			"This indicates the separate semaphores are not working correctly.\n"+
			"Writes started: %d, Writes completed: %d, Reads completed: %d",
			testTimeoutSec, writesStarted.Load(), writesCompleted.Load(), readsCompleted.Load())
	}
}

// TestSemaphoreExhaustion verifies that exhausting write semaphore slots
// does not prevent read operations from proceeding.
func TestSemaphoreExhaustion(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "semaphore-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	ctx := context.Background()

	// We'll try to exhaust the write semaphore (256 slots)
	// and verify reads still work
	const numBlockingWrites = 200

	var wg sync.WaitGroup
	var readsSucceeded atomic.Bool

	// Start blocking write operations to consume writeSemaphore slots
	wg.Add(numBlockingWrites)
	for i := 0; i < numBlockingWrites; i++ {
		go func(id int) {
			defer wg.Done()

			reader, writer := io.Pipe()
			writeKey := []byte{byte(id >> 8), byte(id)}

			// Start write operation
			go func() {
				_ = store.SetFromReader(ctx, writeKey, fileformat.FileTypeTesting, reader)
			}()

			// Block for a bit to hold the semaphore slot
			time.Sleep(100 * time.Millisecond)

			// Then complete the write
			_, err := writer.Write([]byte("data"))
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			writer.Close()
		}(i)
	}

	// While writes are blocking, verify reads can still proceed
	go func() {
		time.Sleep(50 * time.Millisecond) // Let writes start

		// Perform a read operation - this should NOT block
		testKey := []byte{0xFF, 0xFF}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		_, err := store.Exists(ctx, testKey, fileformat.FileTypeTesting)
		if err == nil || err.Error() == "not found" {
			readsSucceeded.Store(true)
		}
	}()

	// Wait for writes to complete
	wg.Wait()

	// Verify reads succeeded even while writes were blocking
	require.True(t, readsSucceeded.Load(), "Reads should succeed even when write semaphore is exhausted")
}

// TestConcurrentReadWriteNoContention verifies that reads and writes
// can proceed concurrently without blocking each other.
func TestConcurrentReadWriteNoContention(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "concurrent-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	ctx := context.Background()

	// First, write some test files
	const numTestFiles = 10
	for i := 0; i < numTestFiles; i++ {
		key := []byte{byte(i)}
		data := []byte("test data")
		err := store.Set(ctx, key, fileformat.FileTypeTesting, data)
		require.NoError(t, err)
	}

	var (
		writeOps               atomic.Int32
		readOps                atomic.Int32
		wg                     sync.WaitGroup
		testCtx, testCtxCancel = context.WithTimeout(ctx, 10*time.Second)
	)

	defer testCtxCancel()

	// Start concurrent writers
	const numWriters = 50
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			key := []byte{byte(100 + id)}
			data := []byte("write data")
			err := store.Set(testCtx, key, fileformat.FileTypeTesting, data)
			if err == nil {
				writeOps.Add(1)
			}
		}(i)
	}

	// Start concurrent readers
	const numReaders = 100
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()
			key := []byte{byte(id % numTestFiles)}
			_, err := store.Exists(testCtx, key, fileformat.FileTypeTesting)
			if err == nil {
				readOps.Add(1)
			}
		}(i)
	}

	// Wait for all operations
	wg.Wait()

	// Verify operations completed successfully
	t.Logf("Concurrent operations completed: %d writes, %d reads", writeOps.Load(), readOps.Load())
	require.Equal(t, int32(numWriters), writeOps.Load(), "All writes should complete")
	require.Equal(t, int32(numReaders), readOps.Load(), "All reads should complete")
}
