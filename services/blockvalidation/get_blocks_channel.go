// This file contains channel-based block fetching for high-throughput catchup operations.
package blockvalidation

import (
	"context"
	"math/rand"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// blockRequest represents a request to fetch a block
type blockRequest struct {
	header *model.BlockHeader
	index  int // Position in original sequence for ordering
}

// channelBasedFetchAndDistribute implements channel-based block fetching with worker pool.
// This function provides:
// 1. Constant flow: Workers continuously pull from channel
// 2. Load balancing: Workers distribute across multiple peers
// 3. Automatic retry: Workers retry with different peers on failure
// 4. Simple architecture: Standard Go worker pool pattern
//
// Architecture:
//
//	[Feeder] → [Request Channel] → [Workers] → [Result Channel] → [Ordered Delivery]
//	   ↓            ↓ buffer           ↓ N workers      ↓ results         ↓ sequential
//	Headers      Backpressure    Fetch+Subtrees    With index        Validation
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Context containing block headers and peer pool
//   - resultQueue: Channel to send results for ordered delivery
//
// Returns:
//   - error: If fetching fails
func (u *Server) channelBasedFetchAndDistribute(ctx context.Context, catchupCtx *CatchupContext, resultQueue chan<- resultItem) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "channelBasedFetchAndDistribute",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:channelBasedFetchAndDistribute][%s] starting channel-based fetching for %d blocks", catchupCtx.blockUpTo.Hash().String(), len(catchupCtx.blockHeaders)),
	)
	defer deferFn()

	blockHeaders := catchupCtx.blockHeaders
	if len(blockHeaders) == 0 {
		return nil
	}

	// Configuration from settings
	numWorkers := u.settings.BlockValidation.FetchBlockWorkers
	bufferSize := u.settings.BlockValidation.FetchBufferSize

	u.logger.Infof("[catchup:channelBasedFetchAndDistribute][%s] launching %d workers for %d blocks", catchupCtx.blockUpTo.Hash().String(), numWorkers, len(blockHeaders))

	// Create request channel for worker pool
	requestChan := make(chan blockRequest, bufferSize)

	// Create error group for coordinating goroutines
	g, gCtx := errgroup.WithContext(ctx)

	// Start feeder goroutine - pushes block headers onto channel
	g.Go(func() error {
		defer close(requestChan)

		for i, header := range blockHeaders {
			select {
			case requestChan <- blockRequest{
				header: header,
				index:  i,
			}:
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}

		u.logger.Debugf("[catchup:feeder][%s] completed feeding %d block requests", catchupCtx.blockUpTo.Hash().String(), len(blockHeaders))
		return nil
	})

	// Start worker pool - each worker fetches blocks from channel
	for i := 0; i < numWorkers; i++ {
		workerID := i
		g.Go(func() error {
			return u.blockFetchWorker(gCtx, workerID, requestChan, resultQueue, catchupCtx)
		})
	}

	// Wait for all workers to complete
	return g.Wait()
}

// blockFetchWorker is a worker that fetches blocks from the request channel.
// Each worker:
//   - Pulls block requests from channel
//   - Fetches block + subtrees with retry and peer rotation
//   - Sends results to result queue
func (u *Server) blockFetchWorker(ctx context.Context, workerID int, requestChan <-chan blockRequest, resultQueue chan<- resultItem, catchupCtx *CatchupContext) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "blockFetchWorker",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:blockFetchWorker-%d][%s] starting worker", workerID, catchupCtx.blockUpTo.Hash().String()),
	)
	defer deferFn()

	blocksProcessed := 0
	for {
		select {
		case req, ok := <-requestChan:
			if !ok {
				u.logger.Debugf("[catchup:blockFetchWorker-%d][%s] request channel closed, processed %d blocks", workerID, catchupCtx.blockUpTo.Hash().String(), blocksProcessed)
				return nil
			}

			// Fetch block with retry logic
			block, err := u.fetchBlockWithRetry(ctx, req.header, catchupCtx)
			if err != nil {
				// Send error result
				result := resultItem{
					block: nil,
					index: req.index,
					err:   err,
				}

				select {
				case resultQueue <- result:
				case <-ctx.Done():
					return ctx.Err()
				}

				continue
			}

			// Fetch subtrees for this block
			if err = u.fetchSubtreeDataForBlock(ctx, block, catchupCtx.peerID, catchupCtx.baseURL); err != nil {
				// Send error result
				result := resultItem{
					block: block,
					index: req.index,
					err:   errors.NewProcessingError("[catchup:blockFetchWorker-%d][%s] failed to fetch subtrees for block %s", workerID, catchupCtx.blockUpTo.Hash().String(), req.header.Hash().String(), err),
				}

				select {
				case resultQueue <- result:
				case <-ctx.Done():
					return ctx.Err()
				}

				continue
			}

			// Send successful result
			result := resultItem{
				block: block,
				index: req.index,
				err:   nil,
			}

			select {
			case resultQueue <- result:
				blocksProcessed++
			case <-ctx.Done():
				return ctx.Err()
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// fetchBlockWithRetry fetches a single block with retry logic and peer rotation.
// On error/timeout, it tries different peers from the available peer pool.
func (u *Server) fetchBlockWithRetry(ctx context.Context, header *model.BlockHeader, catchupCtx *CatchupContext) (*model.Block, error) {
	const maxRetries = 3
	blockHash := header.Hash()

	// Use per-block timeout
	blockFetchTimeout := time.Duration(u.settings.BlockValidation.SubtreeFetchTimeoutPerBlock) * time.Second
	blockCtx, cancel := context.WithTimeout(ctx, blockFetchTimeout)
	defer cancel()

	// If we have a peer pool, use it for load distribution and retry
	if len(catchupCtx.availablePeers) > 0 {
		// Try multiple peers from the pool
		for attempt := 0; attempt < maxRetries; attempt++ {
			// Select peer from pool (round-robin or random)
			peerIdx := attempt % len(catchupCtx.availablePeers)
			if attempt > 0 {
				// On retry, use random selection for better distribution
				peerIdx = rand.Intn(len(catchupCtx.availablePeers))
			}

			peer := catchupCtx.availablePeers[peerIdx]
			peerID := peer.ID
			baseURL := peer.DataHubURL

			u.logger.Debugf("[catchup:fetchBlockWithRetry][%s] attempt %d/%d: trying peer %s (%s)", blockHash.String(), attempt+1, maxRetries, peerID, baseURL)

			// Use batch endpoint with n=1 (compatible with test mocks and existing infrastructure)
			blocks, err := u.fetchBlocksBatch(blockCtx, blockHash, 1, peerID, baseURL)
			if err == nil && len(blocks) >= 1 {
				// Take the first block (peer may return more than requested)
				block := blocks[0]
				// Success - verify block hash matches
				if block.Hash().IsEqual(blockHash) {
					return block, nil
				}

				u.logger.Warnf("[catchup:fetchBlockWithRetry][%s] block hash mismatch from peer %s: expected %s, got %s", blockHash.String(), peerID, blockHash.String(), block.Hash().String())
				continue
			} else if err == nil {
				u.logger.Warnf("[catchup:fetchBlockWithRetry][%s] received 0 blocks from peer %s", blockHash.String(), peerID)
				err = errors.NewProcessingError("[catchup:fetchBlockWithRetry][%s] received 0 blocks", blockHash.String())
			}

			u.logger.Warnf("[catchup:fetchBlockWithRetry][%s] attempt %d/%d failed with peer %s: %v", blockHash.String(), attempt+1, maxRetries, peerID, err)

			// On error, try next peer
			// Small delay before retry to avoid hammering peers
			if attempt < maxRetries-1 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-blockCtx.Done():
					return nil, blockCtx.Err()
				}
			}
		}

		return nil, errors.NewProcessingError("[catchup:fetchBlockWithRetry][%s] failed to fetch block after %d attempts across multiple peers", blockHash.String(), maxRetries)
	}

	// Fallback: Use single peer from catchupCtx with batch endpoint (n=1)
	blocks, err := u.fetchBlocksBatch(blockCtx, blockHash, 1, catchupCtx.peerID, catchupCtx.baseURL)
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchBlockWithRetry][%s] failed to fetch block from peer %s", blockHash.String(), catchupCtx.peerID, err)
	}

	if len(blocks) < 1 {
		return nil, errors.NewProcessingError("[catchup:fetchBlockWithRetry][%s] received 0 blocks from peer %s", blockHash.String(), catchupCtx.peerID)
	}

	// Take the first block (peer may return more than requested)
	block := blocks[0]

	// Verify block hash matches
	if !block.Hash().IsEqual(blockHash) {
		return nil, errors.NewProcessingError("[catchup:fetchBlockWithRetry][%s] block hash mismatch: expected %s, got %s", blockHash.String(), blockHash.String(), block.Hash().String())
	}

	return block, nil
}
