// Package repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
package repository

import (
	"context"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// semaphoreReadCloser wraps an io.ReadCloser and releases a semaphore permit when closed.
type semaphoreReadCloser struct {
	io.ReadCloser
	sem  *semaphore.Weighted
	once sync.Once
}

func (sr *semaphoreReadCloser) Close() error {
	err := sr.ReadCloser.Close()
	sr.once.Do(func() {
		releaseSemaphorePermit(sr.sem)
	})
	return err
}

// GetSubtreeDataReader retrieves the subtree data associated with the given subtree hash.
// It returns a PipeReader that can be used to read the subtree data as it is being streamed.
// The data is either retrieved from the block store or the subtree store, depending on availability.
//
// Parameters:
// - ctx: The context for managing cancellation and timeouts.
// - subtreeHash: The hash of the subtree to retrieve.
//
// Returns:
// - *io.PipeReader: A PipeReader that can be used to read the subtree data.
// - error: An error if the retrieval fails, or nil if successful.
func (repo *Repository) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeDataReader, "GetSubtreeDataReader"); err != nil {
		return nil, err
	}
	// Note: semaphore will be released when the returned reader is closed

	subtreeDataExists, err := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err == nil && subtreeDataExists {
		reader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			releaseSemaphorePermit(repo.semGetSubtreeDataReader)
			return nil, err
		}
		// Wrap reader to release semaphore when closed
		return &semaphoreReadCloser{
			ReadCloser: reader,
			sem:        repo.semGetSubtreeDataReader,
		}, nil
	}

	// File doesn't exist - check if we should create it on-demand
	if !repo.settings.Asset.CreateSubtreeDataOnDemand {
		// Setting disabled - use dynamic streaming only (current behavior)
		return repo.dynamicStreamOnly(ctx, subtreeHash)
	}

	// Try to create the subtreeData file while streaming to HTTP response
	return repo.dualStreamWithFileCreation(ctx, subtreeHash)
}

// dynamicStreamOnly streams subtree data to HTTP response without creating a file (legacy behavior)
func (repo *Repository) dynamicStreamOnly(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	r, w := io.Pipe()
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		// Release semaphore when goroutine completes (after all Aerospike reads are done)
		defer releaseSemaphorePermit(repo.semGetSubtreeDataReader)

		// write all transactions of the subtree to the pipe writer in streaming chunks to minimize memory usage
		if err := repo.writeTransactionsViaSubtreeStoreStreaming(gCtx, w, nil, subtreeHash); err != nil {
			_ = w.CloseWithError(io.ErrClosedPipe)
			_ = r.CloseWithError(err)

			return err
		}

		// close the writer after all subtrees have been streamed
		_ = w.CloseWithError(io.ErrClosedPipe)

		return nil
	})

	return r, nil
}

// dualStreamWithFileCreation creates a subtreeData file while simultaneously streaming to HTTP response
func (repo *Repository) dualStreamWithFileCreation(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	// Initialize metrics (safe to call multiple times due to sync.Once)
	initPrometheusMetrics()

	// Get current block height to calculate DAH (Delete After Height)
	// DAH allows pruning of temporary subtreeData files created by asset service
	// Block persister will set DAH=0 when it processes the block, making the file permanent
	var dah uint32
	if repo.BlockchainClient != nil {
		// Use a deferred recover to gracefully handle mock clients in tests that don't set up GetBestBlockHeader
		func() {
			defer func() {
				if r := recover(); r != nil {
					repo.logger.Debugf("[GetSubtreeDataReader] Failed to get best block height (likely mock client): %v", r)
				}
			}()

			_, bestHeaderMeta, err := repo.BlockchainClient.GetBestBlockHeader(ctx)
			if err != nil {
				repo.logger.Debugf("[GetSubtreeDataReader] Failed to get best block height for DAH calculation: %v", err)
				// Continue without DAH - file will not have delete-at-height
			} else {
				// Set DAH to current height + global retention
				dah = bestHeaderMeta.Height + repo.settings.GlobalBlockHeightRetention
				repo.logger.Debugf("[GetSubtreeDataReader] Setting DAH=%d for subtreeData file (current=%d, retention=%d)",
					dah, bestHeaderMeta.Height, repo.settings.GlobalBlockHeightRetention)
			}
		}()
	}

	// Create FileStorer for blob storage with DAH option (will fail if file already exists - race protection)
	var fileOptions []bloboptions.FileOption
	if dah > 0 {
		fileOptions = append(fileOptions, bloboptions.WithDeleteAt(dah))
	}

	storer, err := filestorer.NewFileStorer(ctx, repo.logger, repo.settings,
		repo.SubtreeStore, subtreeHash[:], fileformat.FileTypeSubtreeData, fileOptions...)
	if err != nil {
		if errors.Is(err, errors.NewBlobAlreadyExistsError("")) {
			// Another process created the file - just read from it
			repo.logger.Debugf("[GetSubtreeDataReader] SubtreeData file for %s created by another process, reading from file", subtreeHash.String())
			prometheusAssetSubtreeDataCreated.WithLabelValues("success", "file_existed").Inc()

			reader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err != nil {
				releaseSemaphorePermit(repo.semGetSubtreeDataReader)
				return nil, err
			}
			return &semaphoreReadCloser{
				ReadCloser: reader,
				sem:        repo.semGetSubtreeDataReader,
			}, nil
		}
		// Other error - return it
		releaseSemaphorePermit(repo.semGetSubtreeDataReader)
		prometheusAssetSubtreeDataCreated.WithLabelValues("error", "creation_failed").Inc()
		return nil, err
	}

	// Create pipe for HTTP response
	httpReader, httpWriter := io.Pipe()

	// Use MultiWriter to write to both file storage and HTTP pipe simultaneously
	multiWriter := io.MultiWriter(storer, httpWriter)

	// Background goroutine: generate data and write to both destinations
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer releaseSemaphorePermit(repo.semGetSubtreeDataReader)

		// Write all transactions to both destinations
		err := repo.writeTransactionsViaSubtreeStoreStreaming(gCtx, multiWriter, nil, subtreeHash)

		// Close file storer
		if err != nil {
			repo.logger.Warnf("[GetSubtreeDataReader] Error writing subtreeData for %s: %v", subtreeHash.String(), err)
			storer.Abort(err)
			_ = httpWriter.CloseWithError(err)
			prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed").Inc()
			return err
		}

		// Close the file storer successfully
		if closeErr := storer.Close(ctx); closeErr != nil {
			repo.logger.Warnf("[GetSubtreeDataReader] Error closing subtreeData file for %s: %v", subtreeHash.String(), closeErr)
			_ = httpWriter.CloseWithError(closeErr)
			prometheusAssetSubtreeDataCreated.WithLabelValues("error", "close_failed").Inc()
			return closeErr
		}

		// Success - close HTTP pipe
		repo.logger.Infof("[GetSubtreeDataReader] Successfully created subtreeData file on-demand for %s", subtreeHash.String())
		_ = httpWriter.CloseWithError(io.ErrClosedPipe)
		prometheusAssetSubtreeDataCreated.WithLabelValues("success", "on_demand_created").Inc()
		return nil
	})

	return httpReader, nil
}
