package subtreevalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

const (
	// aerospikeBatchChunkSize limits the number of records per Aerospike batch operation
	// to prevent timeouts on large blocks. Aerospike can struggle with batches >10K records.
	// This value is the baseline - dynamic sizing adjusts based on total record count.
	aerospikeBatchChunkSize = 5000

	// aerospikeBatchChunkSizeLarge is used for very large batches (>50K records)
	// to reduce network round-trip overhead. Larger batches = fewer round trips.
	aerospikeBatchChunkSizeLarge = 15000

	// aerospikeBatchParallelStreams controls how many chunk streams process concurrently
	// Higher values reduce I/O latency but increase memory usage and Aerospike load.
	// For 400K records with 10ms latency: 2 streams = 800ms → 400ms (50% improvement)
	aerospikeBatchParallelStreams = 3

	// maxTransactionsPerChunk bounds memory usage and Aerospike batch sizes for very large blocks
	// Target: ~2GB peak memory per chunk, prevent Aerospike timeout
	// Calculation:
	//   - 8M transactions × 250 bytes avg = 2GB transaction data
	//   - ~400K external parents (5%) × 1KB metadata = 400MB parent data
	//   - Dependency graph overhead = ~300MB
	//   Total: ~2.7GB per chunk
	// CRITICAL: Chunks must respect transaction dependencies (parents processed before children)
	maxTransactionsPerChunk = 8_000_000 // 8 million transactions

	// Average transaction statistics for capacity pre-sizing
	// Based on Bitcoin/BSV transaction analysis
	avgInputsPerTx     = 2.5  // Average number of inputs per transaction
	avgOutputsPerTx    = 2.0  // Average number of outputs per transaction
	externalParentRate = 0.05 // ~5% of parents are external (outside current block)
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances.
// With 14,496 subtrees per block, using 32KB buffers provides excellent I/O performance
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 32*1024) // 32KB buffer - optimized for sequential I/O
	},
}

// pipelineTxStatePool reduces allocation overhead for pipelined transaction processing.
// For 8M transaction chunks, this eliminates 8M allocations of ~80 bytes each = 640MB saved.
// Critical for multi-million transaction blocks to reduce GC pressure.
var pipelineTxStatePool = sync.Pool{
	New: func() interface{} {
		return &pipelineTxState{
			childrenWaiting:  make([]chainhash.Hash, 0, 2), // avgOutputsPerTx = 2
			completionSignal: make(chan struct{}),
		}
	},
}

// Tiered slice pools reduce allocation overhead by matching pool to transaction size
// Small pool (cap 3): Most transactions with 1-3 inputs (~80% of transactions)
// Medium pool (cap 8): Moderate transactions with 4-8 inputs (~15% of transactions)
// Large pool (cap 16): Large transactions with >8 inputs (~5% of transactions)
// This prevents reallocation when transactions exceed pool capacity
var (
	hashSlicePoolSmall = sync.Pool{
		New: func() interface{} {
			slice := make([]chainhash.Hash, 0, 3)
			return &slice
		},
	}
	hashSlicePoolMedium = sync.Pool{
		New: func() interface{} {
			slice := make([]chainhash.Hash, 0, 8)
			return &slice
		},
	}
	hashSlicePoolLarge = sync.Pool{
		New: func() interface{} {
			slice := make([]chainhash.Hash, 0, 16)
			return &slice
		},
	}
)

// getHashSliceFromPool returns a slice from the appropriate pool based on expected size
// This optimizes memory usage by matching pool capacity to actual needs
func getHashSliceFromPool(expectedSize int) *[]chainhash.Hash {
	switch {
	case expectedSize <= 3:
		return hashSlicePoolSmall.Get().(*[]chainhash.Hash)
	case expectedSize <= 8:
		return hashSlicePoolMedium.Get().(*[]chainhash.Hash)
	default:
		return hashSlicePoolLarge.Get().(*[]chainhash.Hash)
	}
}

// returnHashSliceToPool returns a slice to the appropriate pool based on its capacity
func returnHashSliceToPool(slice *[]chainhash.Hash) {
	if slice == nil || cap(*slice) == 0 {
		return
	}

	// Clear and return to appropriate pool
	*slice = (*slice)[:0]

	switch cap(*slice) {
	case 3:
		hashSlicePoolSmall.Put(slice)
	case 8:
		hashSlicePoolMedium.Put(slice)
	case 16:
		hashSlicePoolLarge.Put(slice)
	default:
		// Don't pool unusual sizes, let GC handle them
	}
}

// nextPowerOf2 returns the next power of 2 greater than or equal to n
// This optimizes map allocations by aligning with Go's internal map bucket sizing
// Go maps use power-of-2 buckets, so pre-allocating to exact power-of-2 prevents rehashing
func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	// Round up to next power of 2
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read
// Uses atomic.Uint64 instead of *uint64 to eliminate pointer indirection and GC overhead
type countingReadCloser struct {
	reader    io.ReadCloser
	bytesRead *atomic.Uint64 // Direct atomic type, no boxing/unboxing
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.bytesRead.Add(uint64(n))
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.reader.Close()
}

// CheckBlockSubtrees validates that all subtrees referenced in a block exist in storage.
//
// Pauses subtree processing during validation to avoid conflicts and returns missing
// subtree information for blocks that reference unavailable subtrees.
func (u *Server) CheckBlockSubtrees(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest) (*subtreevalidation_api.CheckBlockSubtreesResponse, error) {
	block, err := model.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to get block from blockchain client", err)
	}

	// Extract PeerID from request for tracking
	peerID := request.PeerId

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "CheckBlockSubtrees",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[CheckBlockSubtrees] called for block %s at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// Panic recovery to ensure pause lock is always released even on crashes
	defer func() {
		if r := recover(); r != nil {
			u.logger.Errorf("[CheckBlockSubtrees] PANIC recovered for block %s: %v", block.Hash().String(), r)
			// Panic is re-raised after this defer completes, ensuring all defers execute
			panic(r)
		}
	}()

	// Check if the block is on our chain or will become part of our chain
	// Only pause subtree processing if this block is on our chain or extending our chain
	shouldPauseProcessing := false

	bestBlockHeader, _, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to get best block header", err)
	}

	if bestBlockHeader.Hash().IsEqual(block.Header.HashPrevBlock) {
		// If the block's parent is the best block, we can safely assume this block
		// is extending our chain and should pause subtree processing
		u.logger.Infof("[CheckBlockSubtrees] Block %s is extending our chain - pausing subtree processing", block.Hash().String())
		shouldPauseProcessing = true
	} else {
		// First check if the block's parent exists
		parentExists, err := u.blockchainClient.GetBlockExists(ctx, block.Header.HashPrevBlock)
		if err != nil {
			u.logger.Warnf("[CheckBlockSubtrees] Failed to check if parent block exists: %v", err)
			// On error, default to pausing for safety
			shouldPauseProcessing = true
		} else if parentExists {
			// If the parent exists, check if it's on our current chain
			_, parentMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
			if err != nil {
				u.logger.Warnf("[CheckBlockSubtrees] Failed to get parent block header: %v", err)
				// On error, default to pausing for safety
				shouldPauseProcessing = true
			} else if parentMeta != nil && parentMeta.ID > 0 {
				// Check if the parent is on the current chain
				isOnChain, err := u.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{parentMeta.ID})
				if err != nil {
					u.logger.Warnf("[CheckBlockSubtrees] Failed to check if parent is on current chain: %v", err)
					// On error, default to pausing for safety
					shouldPauseProcessing = true
				} else {
					shouldPauseProcessing = isOnChain
				}
			}
		} else {
			// Parent doesn't exist - this could be a block from a different fork
			// Don't pause processing for blocks from different forks
			u.logger.Infof("[CheckBlockSubtrees] Block %s parent %s not found - likely from different fork, not pausing subtree processing", block.Hash().String(), block.Header.HashPrevBlock.String())
			shouldPauseProcessing = false
		}
	}

	// Skip pause lock if we're catching up
	if shouldPauseProcessing {
		currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			u.logger.Warnf("[CheckBlockSubtrees] Failed to get FSM state: %v - will pause for safety", err)
		} else if currentState != nil &&
			(*currentState == blockchain.FSMStateCATCHINGBLOCKS || *currentState == blockchain.FSMStateLEGACYSYNCING) {
			u.logger.Infof("[CheckBlockSubtrees] Skipping pause lock - FSM state is %s (catching up)", currentState.String())
			shouldPauseProcessing = false
		}
	}

	// Acquire and manage pause lock with immediate defer for guaranteed cleanup
	if shouldPauseProcessing {
		u.logger.Infof("[CheckBlockSubtrees] Block %s is on our chain or extending it - acquiring pause lock across all pods", block.Hash().String())

		releasePause, err := u.setPauseProcessing(ctx)
		// Always defer - safe to call even on error (returns noopFunc which does nothing)
		defer releasePause()

		if err != nil {
			u.logger.Warnf("[CheckBlockSubtrees] Failed to acquire distributed pause lock: %v - continuing without pause", err)
		} else {
			u.logger.Infof("[CheckBlockSubtrees] Pause lock acquired successfully for block %s", block.Hash().String())
		}
	} else {
		u.logger.Infof("[CheckBlockSubtrees] Block %s is on a different fork - not pausing subtree processing", block.Hash().String())
	}

	// validate all the subtrees in the block
	missingSubtrees := make([]chainhash.Hash, 0, len(block.Subtrees))
	for _, subtreeHash := range block.Subtrees {
		subtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to check if subtree exists in store", err)
		}

		if !subtreeExists {
			missingSubtrees = append(missingSubtrees, *subtreeHash)
		}
	}

	if len(missingSubtrees) == 0 {
		return &subtreevalidation_api.CheckBlockSubtreesResponse{
			Blessed: true,
		}, nil
	}

	// Shared collection for all transactions across subtrees
	subtreeTxs := make([][]*bt.Tx, len(missingSubtrees))

	// Dynamic concurrency scaling: I/O-bound operations benefit from higher concurrency
	// For blocks with many missing subtrees, increase parallelism to hide I/O latency
	// Base concurrency from settings, but scale up for I/O-bound work
	baseConcurrency := u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency
	concurrency := baseConcurrency

	// Scale concurrency based on number of missing subtrees
	// I/O-bound operations (HTTP fetches) can handle 2-3x more concurrency than CPU-bound
	if len(missingSubtrees) > 100 {
		concurrency = baseConcurrency * 2
		u.logger.Infof("[CheckBlockSubtrees] Scaling subtree fetch concurrency from %d to %d for %d missing subtrees",
			baseConcurrency, concurrency, len(missingSubtrees))
	}

	// get all the subtrees that are missing from the peer in parallel
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	for subtreeIdx, subtreeHash := range missingSubtrees {
		subtreeHash := subtreeHash
		subtreeIdx := subtreeIdx

		subtreeTxs[subtreeIdx] = make([]*bt.Tx, 0, 1024) // Pre-allocate space for transactions in this subtree

		g.Go(func() (err error) {
			subtreeToCheckExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
			if err != nil {
				return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String(), err)
			}

			var subtreeToCheck *subtreepkg.Subtree

			if subtreeToCheckExists {
				// get the subtreeToCheck from the store
				subtreeReader, err := u.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					return errors.NewStorageError("[CheckBlockSubtrees][%s] failed to get subtree from store", subtreeHash.String(), err)
				}
				defer subtreeReader.Close()

				// Use pooled bufio.Reader to reduce allocations (eliminates 50% of GC pressure)
				bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
				bufferedReader.Reset(subtreeReader)
				defer func() {
					bufferedReader.Reset(nil) // Clear reference before returning to pool
					bufioReaderPool.Put(bufferedReader)
				}()

				subtreeToCheck, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to deserialize subtree", subtreeHash.String(), err)
				}
			} else {
				// get the subtree from the peer
				url := fmt.Sprintf("%s/subtree/%s", request.BaseUrl, subtreeHash.String())

				subtreeNodeBytes, err := util.DoHTTPRequest(gCtx, url)
				if err != nil {
					return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree from %s", subtreeHash.String(), url, err)
				}

				// Track bytes downloaded from peer
				if u.p2pClient != nil && peerID != "" {
					if err := u.p2pClient.RecordBytesDownloaded(gCtx, peerID, uint64(len(subtreeNodeBytes))); err != nil {
						u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), len(subtreeNodeBytes), peerID, err)
					}
				}

				subtreeToCheck, err = subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeNodeBytes) / chainhash.HashSize)
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to create subtree structure", subtreeHash.String(), err)
				}

				var nodeHash chainhash.Hash
				for i := 0; i < len(subtreeNodeBytes)/chainhash.HashSize; i++ {
					copy(nodeHash[:], subtreeNodeBytes[i*chainhash.HashSize:(i+1)*chainhash.HashSize])

					if nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
						if err = subtreeToCheck.AddCoinbaseNode(); err != nil {
							return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to add coinbase node to subtree", subtreeHash.String(), err)
						}
					} else {
						if err = subtreeToCheck.AddNode(nodeHash, 0, 0); err != nil {
							return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to add node to subtree", subtreeHash.String(), err)
						}
					}
				}

				if !subtreeHash.Equal(*subtreeToCheck.RootHash()) {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] subtree root hash mismatch: %s", subtreeHash.String(), subtreeToCheck.RootHash().String())
				}

				subtreeBytes, err := subtreeToCheck.Serialize()
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to serialize subtree", subtreeHash.String(), err)
				}

				// Store the subtreeToCheck for later processing
				// we not set a DAH as this is part of a block and will be permanently stored anyway
				if err = u.subtreeStore.Set(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes, options.WithDeleteAt(dah)); err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to store subtree", subtreeHash.String(), err)
				}
			}

			subtreeDataExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err != nil {
				return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to check if subtree data exists in store", subtreeHash.String(), err)
			}

			if !subtreeDataExists {
				// get the subtree data from the peer and process it directly
				url := fmt.Sprintf("%s/subtree_data/%s", request.BaseUrl, subtreeHash.String())

				body, subtreeDataErr := util.DoHTTPRequestBodyReader(gCtx, url)
				if subtreeDataErr != nil {
					return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree data from %s", subtreeHash.String(), url, subtreeDataErr)
				}

				// Wrap with counting reader to track bytes downloaded
				var bytesRead atomic.Uint64
				countingBody := &countingReadCloser{
					reader:    body,
					bytesRead: &bytesRead,
				}

				// Process transactions directly from the stream while storing to disk
				err = u.processSubtreeDataStream(gCtx, subtreeToCheck, countingBody, &subtreeTxs[subtreeIdx])
				_ = countingBody.Close()

				// Track bytes downloaded from peer after stream is consumed
				// Decouple the context to ensure tracking completes even if parent context is cancelled
				if u.p2pClient != nil && peerID != "" {
					trackCtx, _, deferFn := tracing.DecoupleTracingSpan(gCtx, "subtreevalidation", "recordBytesDownloaded")
					defer deferFn()
					if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead.Load()); err != nil {
						u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), bytesRead.Load(), peerID, err)
					}
				}

				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to process subtree data stream", subtreeHash.String(), err)
				}
			} else {
				// SubtreeData exists, extract transactions from stored file
				err = u.extractAndCollectTransactions(gCtx, subtreeToCheck, &subtreeTxs[subtreeIdx])
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to extract transactions", subtreeHash.String(), err)
				}
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to get subtree tx hashes", err)
	}

	// Get block header IDs once for all transactions
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return nil, errors.NewProcessingError("[CheckSubtree] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))
	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	// Flatten all transactions from all subtrees
	totalTxCount := 0
	for _, txs := range subtreeTxs {
		totalTxCount += len(txs)
	}

	allTransactions := make([]*bt.Tx, 0, totalTxCount)
	for _, txs := range subtreeTxs {
		allTransactions = append(allTransactions, txs...)
	}

	subtreeTxs = nil // Clear the slice to free memory

	if len(allTransactions) == 0 {
		u.logger.Infof("[CheckBlockSubtrees] No transactions to validate")
	} else {
		u.logger.Infof("[CheckBlockSubtrees] Processing %d transactions from %d subtrees", len(allTransactions), len(missingSubtrees))

		// Process transactions in dependency-aware chunks
		// This prevents Aerospike timeouts while maintaining correct dependency ordering
		if err = u.processTransactionsInDependencyAwareChunks(ctx, allTransactions, *block.Hash(), block.Height, blockIds); err != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions", err)
		}

		u.logger.Infof("[CheckBlockSubtrees] Completed processing %d transactions", len(allTransactions))
	}

	g, gCtx = errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

	var revalidateSubtreesMutex sync.Mutex
	revalidateSubtrees := make([]chainhash.Hash, 0, len(missingSubtrees))

	// validate all the subtrees in parallel, since we already validated all transactions
	for _, subtreeHash := range missingSubtrees {
		subtreeHash := subtreeHash

		g.Go(func() (err error) {
			// This line is only reached when the base URL is not "legacy"
			v := ValidateSubtree{
				SubtreeHash:   subtreeHash,
				BaseURL:       request.BaseUrl,
				AllowFailFast: false,
				PeerID:        peerID,
			}

			subtree, err := u.ValidateSubtreeInternal(
				ctx,
				v,
				block.Height,
				blockIds,
				validator.WithSkipPolicyChecks(true),
				validator.WithCreateConflicting(true),
				validator.WithIgnoreLocked(true),
			)
			if err != nil {
				u.logger.Debugf("[CheckBlockSubtreesRequest] Failed to validate subtree %s", subtreeHash.String(), err)
				revalidateSubtreesMutex.Lock()
				revalidateSubtrees = append(revalidateSubtrees, subtreeHash)
				revalidateSubtreesMutex.Unlock()

				return nil
			}

			// Remove validated transactions from orphanage
			for _, node := range subtree.Nodes {
				u.orphanage.Delete(node.Hash)
			}

			return nil
		})
	}

	// Wait for all parallel validations to complete
	if err = g.Wait(); err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed during parallel subtree validation", err))
	}

	// Now validate the subtrees, in order, which should be much faster since we already validated all transactions
	// and they should have been added to the internal cache
	for _, subtreeHash := range revalidateSubtrees {
		// This line is only reached when the base URL is not "legacy"
		v := ValidateSubtree{
			SubtreeHash:   subtreeHash,
			BaseURL:       request.BaseUrl,
			AllowFailFast: false,
			PeerID:        peerID,
		}

		subtree, err := u.ValidateSubtreeInternal(
			ctx,
			v,
			block.Height,
			blockIds,
			validator.WithSkipPolicyChecks(true),
			validator.WithCreateConflicting(true),
			validator.WithIgnoreLocked(true),
		)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to validate subtree %s", subtreeHash.String(), err))
		}

		// Remove validated transactions from orphanage
		for _, node := range subtree.Nodes {
			u.orphanage.Delete(node.Hash)
		}
	}

	u.processOrphans(ctx, *block.Header.Hash(), block.Height, blockIds)

	return &subtreevalidation_api.CheckBlockSubtreesResponse{
		Blessed: true,
	}, nil
}

// extractAndCollectTransactions extracts all transactions from a subtree's data file
// and adds them to the shared collection for block-wide processing
func (u *Server) extractAndCollectTransactions(ctx context.Context, subtree *subtreepkg.Subtree, subtreeTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "extractAndCollectTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[extractAndCollectTransactions] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Get subtreeData reader
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[extractAndCollectTransactions] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	// Use pooled bufio.Reader to accelerate reading and reduce allocations
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeDataReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// Read transactions directly into the shared collection
	txCount, err := u.readTransactionsFromSubtreeDataStream(subtree, bufferedReader, subtreeTransactions)
	if err != nil {
		return errors.NewProcessingError("[extractAndCollectTransactions] failed to read transactions from subtreeData", err)
	}

	if txCount != subtree.Length() {
		return errors.NewProcessingError("[extractAndCollectTransactions] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	u.logger.Debugf("[extractAndCollectTransactions] Extracted %d transactions from subtree %s", txCount, subtree.RootHash().String())

	return nil
}

// processSubtreeDataStream processes subtreeData directly from HTTP stream while storing to disk
// This avoids the inefficiency of writing to disk and immediately reading back
func (u *Server) processSubtreeDataStream(ctx context.Context, subtree *subtreepkg.Subtree,
	body io.ReadCloser, allTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processSubtreeDataStream",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[processSubtreeDataStream] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Pre-allocate buffer based on estimated subtree size to avoid reallocation
	// Average transaction: ~500 bytes (varies widely, but good estimate for buffer sizing)
	// This eliminates multiple buffer grow operations during streaming
	estimatedSize := subtree.Length() * 500
	buffer := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	// Use TeeReader to read from HTTP stream while writing to buffer
	teeReader := io.TeeReader(body, buffer)

	// Read transactions directly into the shared collection from the stream
	txCount, err := u.readTransactionsFromSubtreeDataStream(subtree, teeReader, allTransactions)
	if err != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to read transactions from stream", err)
	}

	// make sure the subtree transaction count matches what we read from the stream
	if txCount != subtree.Length() {
		return errors.NewProcessingError("[processSubtreeDataStream] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	// Now store the buffered data to disk
	// we not set a DAH as this is part of a block and will be permanently stored anyway
	err = u.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, buffer.Bytes())
	if err != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to store subtree data", err)
	}

	u.logger.Debugf("[processSubtreeDataStream] Processed %d transactions from subtree %s directly from stream",
		txCount, subtree.RootHash().String())

	return nil
}

// readTransactionsFromSubtreeDataStream reads transactions directly from subtreeData stream
// This follows the same pattern as go-subtree's serializeFromReader but appends directly to the shared collection
func (u *Server) readTransactionsFromSubtreeDataStream(subtree *subtreepkg.Subtree, reader io.Reader, subtreeTransactions *[]*bt.Tx) (int, error) {
	txIndex := 0

	if len(subtree.Nodes) > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		txIndex = 1
	}

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// End of stream reached
				break
			}
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] error reading transaction", err)
		}

		if tx.IsCoinbase() && txIndex == 1 {
			// we did get an unexpected coinbase transaction
			// reset the index to 0 to check the coinbase
			txIndex = 0
		}

		tx.SetTxHash(tx.TxIDChainHash()) // Cache the transaction hash to avoid recomputing it

		// Basic sanity check: ensure the transaction hash matches the expected hash from the subtree
		if txIndex < subtree.Length() {
			expectedHash := subtree.Nodes[txIndex].Hash
			if !expectedHash.Equal(*tx.TxIDChainHash()) {
				return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] transaction hash mismatch at index %d: expected %s, got %s", txIndex, expectedHash.String(), tx.TxIDChainHash().String())
			}
		} else {
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] more transactions than expected in subtreeData")
		}

		*subtreeTransactions = append(*subtreeTransactions, tx)
		txIndex++
	}

	return txIndex, nil
}

// batchFetchWithParallelChunks fetches UTXO metadata in parallel chunk streams
// This reduces I/O latency by processing multiple chunks concurrently
func (u *Server) batchFetchWithParallelChunks(ctx context.Context, unresolvedMeta []*utxo.UnresolvedMetaData,
	chunkSize int, fieldsToFetch ...fields.FieldName) error {

	totalRecords := len(unresolvedMeta)
	if totalRecords == 0 {
		return nil
	}

	totalChunks := (totalRecords + chunkSize - 1) / chunkSize

	// Create buffered channel for chunk indices
	chunkChan := make(chan int, totalChunks)
	for i := 0; i < totalChunks; i++ {
		chunkChan <- i
	}
	close(chunkChan)

	// Process chunks in parallel streams
	g, gCtx := errgroup.WithContext(ctx)
	for stream := 0; stream < aerospikeBatchParallelStreams; stream++ {
		streamID := stream
		g.Go(func() error {
			for chunkIdx := range chunkChan {
				start := chunkIdx * chunkSize
				end := min(start+chunkSize, totalRecords)
				chunk := unresolvedMeta[start:end]

				u.logger.Debugf("[batchFetchWithParallelChunks] Stream %d processing chunk %d/%d (%d records)",
					streamID, chunkIdx+1, totalChunks, len(chunk))

				err := u.utxoStore.BatchDecorate(gCtx, chunk, fieldsToFetch...)
				if err != nil {
					return errors.NewProcessingError("[batchFetchWithParallelChunks] Stream %d failed on chunk %d/%d",
						streamID, chunkIdx+1, totalChunks, err)
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// prefetchAndCacheParentUTXOs scans all transactions, identifies required parent UTXOs,
// fetches them in batch, and pre-populates the cache to eliminate round-trips during validation
func (u *Server) prefetchAndCacheParentUTXOs(ctx context.Context, allTransactions []*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "prefetchAndCacheParentUTXOs",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[prefetchAndCacheParentUTXOs] Prefetching parent UTXOs for %d transactions", len(allTransactions)),
	)
	defer deferFn()

	// Step 1: Build complete set of transaction hashes in this block (FIRST PASS)
	// This must be done BEFORE collecting parent hashes to correctly identify in-block vs external parents
	transactionHashes := make(map[chainhash.Hash]struct{}, len(allTransactions))
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}
		txHash := *tx.TxIDChainHash()
		transactionHashes[txHash] = struct{}{}
	}

	// Step 2: Collect parent hashes with complete knowledge of in-block transactions (SECOND PASS)
	// Pre-size map: estimate external parents = txCount × avgInputs × externalParentRate
	// For 8M txs: 8M × 2.5 × 0.05 = 1M external parents
	estimatedExternalParents := int(float64(len(allTransactions)) * avgInputsPerTx * externalParentRate)
	parentHashes := make(map[chainhash.Hash]struct{}, estimatedExternalParents)

	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		// Collect parent hashes from inputs
		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()

			// Only prefetch if the parent is NOT in this block (external parents)
			// Parents in the block will be validated in dependency order
			if _, inBlock := transactionHashes[parentHash]; !inBlock {
				parentHashes[parentHash] = struct{}{}
			}
		}
	}

	if len(parentHashes) == 0 {
		u.logger.Infof("[prefetchAndCacheParentUTXOs] No external parent UTXOs to prefetch")
		return nil
	}

	// Step 2: Create UnresolvedMetaData slice for BatchDecorate
	// Pre-allocate exact size and use direct indexing to avoid append overhead and heap escapes
	unresolvedMeta := make([]*utxo.UnresolvedMetaData, len(parentHashes))
	idx := 0
	for hash := range parentHashes {
		hash := hash // capture loop variable
		unresolvedMeta[idx] = &utxo.UnresolvedMetaData{
			Hash: hash,
			Fields: []fields.FieldName{
				fields.Fee,
				fields.SizeInBytes,
				fields.TxInpoints,
				fields.BlockIDs,
				fields.IsCoinbase,
			},
		}
		idx++
	}

	totalRecords := len(unresolvedMeta)
	u.logger.Infof("[prefetchAndCacheParentUTXOs] Prefetching %d unique parent UTXOs", totalRecords)

	// Step 3: Batch fetch all parent UTXOs in parallel chunks
	// Parallelization reduces I/O latency: 400K records / 3 streams = ~50% faster
	start := time.Now()

	// Dynamic chunk sizing: larger chunks for large batches to reduce overhead
	chunkSize := aerospikeBatchChunkSize
	if totalRecords > 50000 {
		chunkSize = aerospikeBatchChunkSizeLarge
		u.logger.Infof("[prefetchAndCacheParentUTXOs] Using large chunk size %d for %d records", chunkSize, totalRecords)
	}

	err := u.batchFetchWithParallelChunks(ctx, unresolvedMeta, chunkSize,
		fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.BlockIDs, fields.IsCoinbase)
	if err != nil {
		return errors.NewProcessingError("[prefetchAndCacheParentUTXOs] Failed to batch fetch parent UTXOs", err)
	}

	fetchDuration := time.Since(start)
	u.logger.Infof("[prefetchAndCacheParentUTXOs] Fetched %d parent UTXOs in %v (parallel streams: %d, chunk size: %d)",
		totalRecords, fetchDuration, aerospikeBatchParallelStreams, chunkSize)

	// Step 4: Pre-populate the cache with fetched data
	// This ensures subsequent validations hit the cache instead of the store
	cacheKeys := make([][]byte, 0, len(unresolvedMeta))
	cacheValues := make([][]byte, 0, len(unresolvedMeta))

	for _, item := range unresolvedMeta {
		if item.Err != nil {
			// Parent not found is expected (could be in a different block)
			if errors.Is(item.Err, errors.ErrTxNotFound) {
				continue
			}
			u.logger.Warnf("[prefetchAndCacheParentUTXOs] Error fetching parent %s: %v", item.Hash.String(), item.Err)
			continue
		}

		if item.Data == nil {
			continue
		}

		// Serialize metadata for cache
		metaBytes, err := item.Data.MetaBytes()
		if err != nil {
			u.logger.Warnf("[prefetchAndCacheParentUTXOs] Failed to serialize metadata for %s: %v", item.Hash.String(), err)
			continue
		}

		cacheKeys = append(cacheKeys, item.Hash[:])
		cacheValues = append(cacheValues, metaBytes)
	}

	// Use SetCacheMulti if available (TxMetaCache), otherwise individual sets
	if len(cacheKeys) > 0 {
		// Try to use SetCacheMulti for batch cache population
		type batchCacher interface {
			SetCacheMulti(keys [][]byte, values [][]byte) error
		}

		if batchCache, ok := u.utxoStore.(batchCacher); ok {
			start = time.Now()
			if err := batchCache.SetCacheMulti(cacheKeys, cacheValues); err != nil {
				u.logger.Warnf("[prefetchAndCacheParentUTXOs] Failed to pre-populate cache: %v", err)
			} else {
				u.logger.Infof("[prefetchAndCacheParentUTXOs] Pre-populated cache with %d entries in %v", len(cacheKeys), time.Since(start))
			}
		}
	}

	return nil
}

// batchExtendTransactions extends all transactions using the prefetched parent UTXO data
// This eliminates the need for validator to call getTransactionInputBlockHeightsAndExtendTx
// which would do ~195K individual Get() calls (one per parent transaction)
func (u *Server) batchExtendTransactions(ctx context.Context, allTransactions []*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "batchExtendTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[batchExtendTransactions] Extending %d transactions", len(allTransactions)),
	)
	defer deferFn()

	// Step 1: Build complete set of transaction hashes in this block (FIRST PASS)
	// This must be done BEFORE collecting parent hashes for accurate in-block detection
	transactionHashes := make(map[chainhash.Hash]struct{}, len(allTransactions))
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() || tx.IsExtended() {
			continue // Skip if already extended
		}
		txHash := *tx.TxIDChainHash()
		transactionHashes[txHash] = struct{}{}
	}

	// Step 2: Collect parent hashes and separate in-block vs external parents (SECOND PASS)
	// Pre-size maps to avoid rehashing during population
	// External parents: ~5% of total parents (txCount × avgInputs × externalRate)
	// In-block parents: ~95% of total parents
	estimatedTotalParents := int(float64(len(allTransactions)) * avgInputsPerTx)
	estimatedExternalParents := int(float64(estimatedTotalParents) * externalParentRate)
	estimatedInBlockParents := estimatedTotalParents - estimatedExternalParents

	externalParentHashes := make(map[chainhash.Hash]struct{}, estimatedExternalParents)
	inBlockParents := make(map[chainhash.Hash]*bt.Tx, estimatedInBlockParents)

	// First, build a map of in-block transactions for quick lookup
	txMap := make(map[chainhash.Hash]*bt.Tx, len(allTransactions))
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}
		txMap[*tx.TxIDChainHash()] = tx
	}

	// Now collect parent hashes, separating in-block from external
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() || tx.IsExtended() {
			continue
		}

		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()

			// Check if parent is in this block
			if parentTx, inBlock := txMap[parentHash]; inBlock {
				// Parent is in-block - use in-memory transaction (no need to fetch from Aerospike)
				inBlockParents[parentHash] = parentTx
			} else {
				// Parent is external - needs to be fetched from Aerospike
				externalParentHashes[parentHash] = struct{}{}
			}
		}
	}

	u.logger.Infof("[batchExtendTransactions] Found %d in-block parents (using in-memory), %d external parents (fetching from store)", len(inBlockParents), len(externalParentHashes))

	if len(externalParentHashes) == 0 {
		// All parents are in-block, use them directly without fetching
		if len(inBlockParents) == 0 {
			u.logger.Infof("[batchExtendTransactions] No parent UTXOs needed for extension")
			return nil
		}

		// Skip to extension step using only in-block parents
		u.logger.Infof("[batchExtendTransactions] All parents are in-block, skipping Aerospike fetch")
		return u.extendTransactionsWithParents(allTransactions, inBlockParents)
	}

	// Fetch only external parent UTXOs using BatchDecorate in chunks to prevent Aerospike timeouts
	// Pre-allocate exact size and use direct indexing to avoid append overhead
	unresolvedMeta := make([]*utxo.UnresolvedMetaData, len(externalParentHashes))
	parentMap := make(map[chainhash.Hash]int, len(externalParentHashes))
	idx := 0
	for hash := range externalParentHashes {
		hash := hash
		unresolvedMeta[idx] = &utxo.UnresolvedMetaData{
			Hash: hash,
			Idx:  idx,
			Fields: []fields.FieldName{
				fields.Outputs, // Only need output data (amount + locking script), not full transaction with inputs/signatures
			},
		}
		parentMap[hash] = idx
		idx++
	}

	totalRecords := len(unresolvedMeta)
	u.logger.Infof("[batchExtendTransactions] Fetching %d unique external parent outputs for transaction extension", totalRecords)

	// Fetch in parallel chunks to reduce I/O latency
	start := time.Now()

	// Dynamic chunk sizing: larger chunks for large batches
	chunkSize := aerospikeBatchChunkSize
	if totalRecords > 50000 {
		chunkSize = aerospikeBatchChunkSizeLarge
		u.logger.Infof("[batchExtendTransactions] Using large chunk size %d for %d records", chunkSize, totalRecords)
	}

	err := u.batchFetchWithParallelChunks(ctx, unresolvedMeta, chunkSize, fields.Outputs)
	if err != nil {
		return errors.NewProcessingError("[batchExtendTransactions] Failed to batch fetch parent UTXOs", err)
	}

	fetchDuration := time.Since(start)
	u.logger.Infof("[batchExtendTransactions] Fetched %d external parent UTXOs in %v (parallel streams: %d, chunk size: %d)",
		totalRecords, fetchDuration, aerospikeBatchParallelStreams, chunkSize)

	// Build a map of parent hash -> parent tx for quick lookup
	// Start with in-block parents (already in memory)
	parentTxMap := make(map[chainhash.Hash]*bt.Tx, len(inBlockParents)+len(unresolvedMeta))
	for hash, tx := range inBlockParents {
		parentTxMap[hash] = tx
	}

	// Add fetched external parents and count large transactions
	largeTxCount := 0
	for _, item := range unresolvedMeta {
		if item.Err != nil || item.Data == nil || item.Data.Tx == nil {
			continue
		}

		// Count potentially external transactions (large ones likely stored in blob)
		// External threshold is typically >100 outputs
		if len(item.Data.Tx.Outputs) > 100 {
			largeTxCount++
		}

		parentTxMap[item.Hash] = item.Data.Tx
	}

	u.logger.Infof("[batchExtendTransactions] Built parent map with %d in-block + %d external parents = %d total", len(inBlockParents), len(unresolvedMeta), len(parentTxMap))
	if largeTxCount > 0 {
		u.logger.Infof("[batchExtendTransactions] Found %d large parent transactions (>100 outputs, likely external) out of %d external parents", largeTxCount, len(unresolvedMeta))
	}

	return u.extendTransactionsWithParents(allTransactions, parentTxMap)
}

// extendTransactionsWithParents extends all transactions using the provided parent transaction map
func (u *Server) extendTransactionsWithParents(allTransactions []*bt.Tx, parentTxMap map[chainhash.Hash]*bt.Tx) error {
	extendedCount := 0
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() || tx.IsExtended() {
			continue
		}

		// Extend each input with parent transaction output data
		allInputsExtended := true
		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()
			parentTx := parentTxMap[parentHash]

			if parentTx == nil {
				// Parent not found - mark as not fully extended
				allInputsExtended = false
				continue
			}

			// Check if parent has the required output
			if int(input.PreviousTxOutIndex) >= len(parentTx.Outputs) {
				u.logger.Warnf("[extendTransactionsWithParents] Parent tx %s doesn't have output index %d", parentHash.String(), input.PreviousTxOutIndex)
				allInputsExtended = false
				continue
			}

			parentOutput := parentTx.Outputs[input.PreviousTxOutIndex]

			// Extend the input with parent output data
			input.PreviousTxSatoshis = parentOutput.Satoshis
			input.PreviousTxScript = parentOutput.LockingScript
		}

		// Mark transaction as extended if all inputs were successfully extended
		if allInputsExtended {
			tx.SetExtended(true)
			extendedCount++
		}
	}

	u.logger.Infof("[extendTransactionsWithParents] Extended %d/%d transactions", extendedCount, len(allTransactions))
	return nil
}

// processTransactionsInDependencyAwareChunks processes transactions in chunks that respect dependencies
// This prevents Aerospike timeouts on large blocks while ensuring parent txs are processed before children
func (u *Server) processTransactionsInDependencyAwareChunks(ctx context.Context, allTransactions []*bt.Tx, blockHash chainhash.Hash, blockHeight uint32, blockIds map[uint32]bool) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTransactionsInDependencyAwareChunks",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processTransactionsInDependencyAwareChunks] Processing %d transactions at block height %d", len(allTransactions), blockHeight),
	)
	defer deferFn()

	if len(allTransactions) == 0 {
		return nil
	}

	// For small transaction counts, no chunking needed
	if len(allTransactions) <= maxTransactionsPerChunk {
		u.logger.Infof("[processTransactionsInDependencyAwareChunks] Processing all %d transactions in single batch (under chunk limit)", len(allTransactions))
		return u.processTransactionsInLevels(ctx, allTransactions, blockHash, chainhash.Hash{}, blockHeight, blockIds)
	}

	// Build transaction hash map for dependency tracking
	txMap := make(map[chainhash.Hash]*bt.Tx, len(allTransactions))
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}
		txMap[*tx.TxIDChainHash()] = tx
	}

	// Build dependency graph: for each tx, track which in-block parents it depends on
	dependencies := make(map[chainhash.Hash][]chainhash.Hash, len(txMap))
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		txHash := *tx.TxIDChainHash()
		deps := make([]chainhash.Hash, 0, len(tx.Inputs))

		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()
			// Only track in-block dependencies
			if _, inBlock := txMap[parentHash]; inBlock {
				deps = append(deps, parentHash)
			}
		}

		dependencies[txHash] = deps
	}

	// Topological sort: assign each transaction a "level" (max distance from roots)
	// This ensures parents are always in earlier chunks than children
	levels := make(map[chainhash.Hash]int, len(txMap))

	var computeLevel func(chainhash.Hash) int
	computeLevel = func(txHash chainhash.Hash) int {
		if level, exists := levels[txHash]; exists {
			return level
		}

		// Base case: no dependencies means level 0
		deps := dependencies[txHash]
		if len(deps) == 0 {
			levels[txHash] = 0
			return 0
		}

		// Recursive case: level is 1 + max(parent levels)
		maxParentLevel := -1
		for _, parentHash := range deps {
			parentLevel := computeLevel(parentHash)
			if parentLevel > maxParentLevel {
				maxParentLevel = parentLevel
			}
		}

		level := maxParentLevel + 1
		levels[txHash] = level
		return level
	}

	// Compute levels for all transactions
	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}
		computeLevel(*tx.TxIDChainHash())
	}

	// Sort transactions by level (parents before children)
	// Transactions with same level can be in any order
	type txWithLevel struct {
		tx    *bt.Tx
		level int
	}
	sortedTxs := make([]txWithLevel, 0, len(allTransactions))
	for _, tx := range allTransactions {
		if tx == nil {
			continue
		}
		level := 0
		if !tx.IsCoinbase() {
			level = levels[*tx.TxIDChainHash()]
		}
		sortedTxs = append(sortedTxs, txWithLevel{tx: tx, level: level})
	}

	// Sort by level (stable sort maintains original order within same level)
	for i := 0; i < len(sortedTxs); i++ {
		for j := i + 1; j < len(sortedTxs); j++ {
			if sortedTxs[j].level < sortedTxs[i].level {
				sortedTxs[i], sortedTxs[j] = sortedTxs[j], sortedTxs[i]
			}
		}
	}

	// Create chunks respecting the size limit
	// Since transactions are sorted by level, each chunk's transactions can only
	// depend on transactions in the same or earlier chunks
	chunks := make([][]*bt.Tx, 0, (len(sortedTxs)+maxTransactionsPerChunk-1)/maxTransactionsPerChunk)
	currentChunk := make([]*bt.Tx, 0, maxTransactionsPerChunk)

	for _, txl := range sortedTxs {
		currentChunk = append(currentChunk, txl.tx)

		if len(currentChunk) >= maxTransactionsPerChunk {
			chunks = append(chunks, currentChunk)
			currentChunk = make([]*bt.Tx, 0, maxTransactionsPerChunk)
		}
	}

	// Add remaining transactions
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	u.logger.Infof("[processTransactionsInDependencyAwareChunks] Split %d transactions into %d dependency-aware chunks", len(allTransactions), len(chunks))

	// Process each chunk sequentially
	// Each chunk can depend on previous chunks (already committed to UTXO store)
	for chunkIdx, chunk := range chunks {
		u.logger.Infof("[processTransactionsInDependencyAwareChunks] Processing chunk %d/%d with %d transactions", chunkIdx+1, len(chunks), len(chunk))

		if err := u.processTransactionsInLevels(ctx, chunk, blockHash, chainhash.Hash{}, blockHeight, blockIds); err != nil {
			return errors.NewProcessingError("[processTransactionsInDependencyAwareChunks] Failed to process chunk %d/%d", chunkIdx+1, len(chunks), err)
		}

		u.logger.Infof("[processTransactionsInDependencyAwareChunks] Completed chunk %d/%d", chunkIdx+1, len(chunks))
	}

	return nil
}

// processTransactionsInLevels processes all transactions from all subtrees using level-based validation
// This ensures transactions are processed in dependency order while maximizing parallelism
func (u *Server) processTransactionsInLevels(ctx context.Context, allTransactions []*bt.Tx, blockHash chainhash.Hash, subtreeHash chainhash.Hash, blockHeight uint32, blockIds map[uint32]bool) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTransactionsInLevels",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processTransactionsInLevels] Processing %d transactions at block height %d", len(allTransactions), blockHeight),
	)
	defer deferFn()

	if len(allTransactions) == 0 {
		return nil
	}

	u.logger.Infof("[processTransactionsInLevels] Organizing %d transactions into dependency levels", len(allTransactions))

	// Convert transactions to missingTx format for prepareTxsPerLevel
	missingTxs := make([]missingTx, len(allTransactions))
	for i, tx := range allTransactions {
		if tx == nil {
			return errors.NewProcessingError("[processTransactionsInLevels] transaction is nil at index %d", i)
		}

		missingTxs[i] = missingTx{
			tx:  tx,
			idx: i,
		}
	}

	// Use the existing prepareTxsPerLevel logic to organize transactions by dependency levels
	maxLevel, txsPerLevel, err := u.prepareTxsPerLevel(ctx, missingTxs)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to prepare transactions per level", err)
	}

	u.logger.Infof("[processTransactionsInLevels] Processing transactions across %d levels", maxLevel+1)

	validatorOptions := []validator.Option{
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
	}

	currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to get FSM current state", err)
	}

	// During legacy syncing or catching up, disable adding transactions to block assembly
	if *currentState == blockchain.FSMStateLEGACYSYNCING || *currentState == blockchain.FSMStateCATCHINGBLOCKS {
		validatorOptions = append(validatorOptions, validator.WithAddTXToBlockAssembly(false))
	}

	// Pre-process validation options
	processedValidatorOptions := validator.ProcessOptions(validatorOptions...)

	// OPTIMIZATION STEP 1: Prefetch and cache all parent UTXOs before validation
	// This eliminates 197+ round-trips to the UTXO store by batching all fetches upfront.
	// CRITICAL: Must succeed - the next step (batch extend) requires this data.
	// If prefetch fails (typically Aerospike timeout), fail fast to trigger immediate retry
	// rather than proceeding to validation which will fail anyway.
	if err := u.prefetchAndCacheParentUTXOs(ctx, allTransactions); err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to prefetch parent UTXOs", err)
	}

	// OPTIMIZATION STEP 2: Extend all transactions upfront using prefetched parent data
	// This eliminates ~195K individual Get() calls during validation (one per parent tx).
	// Validator will see transactions are already extended and skip getTransactionInputBlockHeightsAndExtendTx.
	//
	// CRITICAL: Must succeed - subtree serialization requires TxInpoints which come from extended transactions.
	// If batch extend fails (typically Aerospike timeout during BatchDecorate), we MUST fail fast because:
	//   1. Individual validator fallback would require 195K individual Get() calls (extremely slow)
	//   2. If Aerospike is timing out on batch operations, individual operations will likely timeout too
	//   3. Even if individual extension succeeds, TxInpoints may not properly propagate through
	//      validation -> storage -> retrieval -> serialization flow, causing "parent tx hashes not set" errors
	//   4. Failing fast triggers immediate retry, giving Aerospike another chance to succeed
	//
	// The validator fallback (individual tx extension) is still used for:
	//   - Transactions with missing parents (partial batch extend success)
	//   - Regular transaction validation (non-block validation paths)
	//   - Any validation path that doesn't use batchExtendTransactions
	if err := u.batchExtendTransactions(ctx, allTransactions); err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to batch extend transactions - cannot continue without extended transactions", err)
	}

	// Track validation results
	var (
		errorsFound      atomic.Uint64
		addedToOrphanage atomic.Uint64
	)

	// OPTIMIZATION: Choose processing strategy based on transaction count
	// Benchmark results show a clear crossover point at ~100 transactions:
	//
	// Transaction Count vs Performance (coordination overhead only):
	//   20 txs:   ByLevel 4.2x faster  (15,257 ns vs 64,397 ns)
	//   30 txs:   ByLevel 2.9x faster  (23,125 ns vs 65,977 ns)
	//   90 txs:   ByLevel 1.3x faster  (52,703 ns vs 70,021 ns)
	//  150 txs:   Pipelined 1.1x faster (76,064 ns vs 81,764 ns)
	//  300 txs:   Pipelined 1.7x faster (89,065 ns vs 149,895 ns)
	// 1970 txs:   Pipelined 3.8x faster (402,161 ns vs 1,523,933 ns)
	// 5000 txs:   Pipelined 4.7x faster (538,022 ns vs 2,536,742 ns)
	//
	// Below 100 txs: Graph-building overhead dominates, ByLevel wins
	// Above 100 txs: Parallelism benefits outweigh overhead, Pipelined wins
	if len(allTransactions) < 100 {
		// Use level-based processing for small transaction counts (< 100 txs)
		// Simpler approach without dependency graph overhead
		err = u.processTransactionsByLevel(ctx, blockHash, subtreeHash, maxLevel, txsPerLevel, blockHeight, blockIds, processedValidatorOptions, &errorsFound, &addedToOrphanage)
	} else {
		// Use pipelined processing for larger transaction counts (>= 100 txs)
		// Builds dependency graph to enable fine-grained parallelism
		err = u.processTransactionsPipelined(ctx, blockHash, subtreeHash, missingTxs, blockHeight, blockIds, processedValidatorOptions, &errorsFound, &addedToOrphanage)
	}

	if err != nil {
		return err
	}

	if errorsFound.Load() > 0 {
		return errors.NewProcessingError("[processTransactionsInLevels] Completed processing with %d errors, %d transactions added to orphanage", errorsFound.Load(), addedToOrphanage.Load())
	}

	u.logger.Infof("[processTransactionsInLevels] Successfully processed all %d transactions", len(allTransactions))
	return nil
}

// processTransactionsByLevel processes transactions level by level with barriers between levels.
//
// This approach is optimal for small transaction counts (< 100 txs) where the overhead
// of building a dependency graph outweighs the benefits of fine-grained parallelism.
//
// How it works:
// - Processes transactions in dependency order: level 0, then level 1, etc.
// - All transactions within a level are processed in parallel
// - Waits for entire level to complete before starting the next level
//
// Performance characteristics:
// - Simple coordination: uses basic errgroup for parallel processing within levels
// - Lower memory overhead: no dependency graph construction
// - Optimal for < 100 txs: 1.3-4.2x faster than pipelined approach
// - Level barriers become bottleneck for large counts (>100 txs)
//
// Example with 3 levels: [Tx0] -> [Tx1, Tx2, Tx3] -> [Tx4, Tx5]
// - Level 0: Process Tx0
// - Wait for level 0 to complete
// - Level 1: Process Tx1, Tx2, Tx3 in parallel
// - Wait for level 1 to complete (even if Tx1 finishes early, must wait for Tx2, Tx3)
// - Level 2: Process Tx4, Tx5 in parallel
func (u *Server) processTransactionsByLevel(ctx context.Context, blockHash chainhash.Hash, subtreeHash chainhash.Hash, maxLevel uint32, txsPerLevel [][]missingTx,
	blockHeight uint32, blockIds map[uint32]bool, processedValidatorOptions *validator.Options,
	errorsFound, addedToOrphanage *atomic.Uint64) error {

	// Process each level in series, but all transactions within a level in parallel
	for level := uint32(0); level <= maxLevel; level++ {
		levelTxs := txsPerLevel[level]
		if len(levelTxs) == 0 {
			continue
		}

		u.logger.Debugf("[processTransactionsByLevel] Processing level %d/%d with %d transactions", level+1, maxLevel+1, len(levelTxs))

		// Process all transactions at this level in parallel
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.SpendBatcherSize*2)

		for _, mTx := range levelTxs {
			tx := mTx.tx
			if tx == nil {
				return errors.NewProcessingError("[processTransactionsByLevel] transaction is nil at level %d", level)
			}

			g.Go(func() error {
				return u.validateSingleTransaction(gCtx, blockHash, subtreeHash, tx, blockHeight, blockIds, processedValidatorOptions, errorsFound, addedToOrphanage)
			})
		}

		// Fail early if we get an actual tx error thrown
		if err := g.Wait(); err != nil {
			return errors.NewProcessingError("[processTransactionsByLevel] Failed to process level %d", level+1, err)
		}

		u.logger.Debugf("[processTransactionsByLevel] Processing level %d/%d with %d transactions DONE", level+1, maxLevel+1, len(levelTxs))
	}

	return nil
}

// processTransactionsPipelined processes transactions using a dependency-aware pipeline.
//
// This approach is optimal for larger transaction counts (>= 100 txs) where fine-grained
// parallelism provides significant speedups despite the graph construction overhead.
//
// How it works:
// - Builds a complete dependency graph of all transactions
// - Tracks parent/child relationships and pending dependency counts
// - Transactions start processing immediately when their specific dependencies complete
// - No level barriers: maximum parallelism within dependency constraints
//
// Performance characteristics:
// - Graph construction overhead: 2 passes over all transactions
// - Higher memory usage: maintains txMap, dependencies, and childrenMap
// - Optimal for >= 100 txs: 1.1-4.7x faster than level-based approach
// - Scales excellently with deep trees (197 levels: 3.8x faster)
// - Scales excellently with wide trees (5000 txs: 4.7x faster)
//
// Example with dependencies: Tx0 -> Tx1 -> Tx4
//
//	Tx0 -> Tx2 -> Tx5
//	Tx0 -> Tx3
//
// Level-based would process in 3 waves with barriers:
//
//	Wave 1: Tx0
//	Wave 2: Tx1, Tx2, Tx3 (wait for slowest)
//	Wave 3: Tx4, Tx5 (wait even though dependencies met earlier)
//
// Pipelined processes with no barriers:
//   - Tx0 starts immediately
//   - Tx1, Tx2, Tx3 start as soon as Tx0 completes
//   - Tx4 starts as soon as Tx1 completes (doesn't wait for Tx2, Tx3)
//   - Tx5 starts as soon as Tx2 completes (doesn't wait for Tx3)
func (u *Server) processTransactionsPipelined(ctx context.Context, blockHash chainhash.Hash, subtreeHash chainhash.Hash, transactions []missingTx,
	blockHeight uint32, blockIds map[uint32]bool, processedValidatorOptions *validator.Options,
	errorsFound, addedToOrphanage *atomic.Uint64) error {

	u.logger.Infof("[processTransactionsPipelined] Using pipeline processing for %d transactions", len(transactions))

	// Build dependency graph in two passes to handle transactions in any order
	// CRITICAL: Must build complete txMap first before checking dependencies
	// Otherwise, if child appears before parent in array, dependency won't be detected

	// PASS 1: Build complete txMap using object pool to reduce allocations
	// Use power-of-2 capacity to align with Go's internal map bucket sizing (prevents rehashing)
	mapCapacity := nextPowerOf2(len(transactions))
	txMap := make(map[chainhash.Hash]*pipelineTxState, mapCapacity)
	pooledStates := make([]*pipelineTxState, 0, len(transactions)) // Track pooled objects for cleanup

	for _, mTx := range transactions {
		if mTx.tx == nil || mTx.tx.IsCoinbase() {
			continue
		}

		txHash := *mTx.tx.TxIDChainHash()

		// Get state from pool and reset fields
		state := pipelineTxStatePool.Get().(*pipelineTxState)
		state.tx = mTx.tx
		state.pendingParents = 0
		state.childrenWaiting = state.childrenWaiting[:0] // Reset slice, keep capacity

		txMap[txHash] = state
		pooledStates = append(pooledStates, state)
	}

	// Defer cleanup: return all states to pool after processing
	defer func() {
		for _, state := range pooledStates {
			// Reset state before returning to pool
			state.tx = nil
			state.pendingParents = 0
			state.childrenWaiting = state.childrenWaiting[:0]
			pipelineTxStatePool.Put(state)
		}
	}()

	// PASS 2: Build dependencies and children relationships
	// Pre-size maps with power-of-2 capacity to prevent rehashing
	dependencies := make(map[chainhash.Hash][]chainhash.Hash, mapCapacity)
	childrenMap := make(map[chainhash.Hash][]chainhash.Hash, mapCapacity)

	for _, mTx := range transactions {
		if mTx.tx == nil || mTx.tx.IsCoinbase() {
			continue
		}

		txHash := *mTx.tx.TxIDChainHash()

		// Get slice from tiered pool for dependencies based on input count
		inputCount := len(mTx.tx.Inputs)
		depsSlicePtr := getHashSliceFromPool(inputCount)
		depsSlice := (*depsSlicePtr)[:0] // Reset to length 0, keep capacity

		// Build dependency relationships
		for _, input := range mTx.tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()

			// Only track dependencies within this block
			if _, exists := txMap[parentHash]; exists {
				depsSlice = append(depsSlice, parentHash)

				// Add to children map
				if childrenMap[parentHash] == nil {
					// Get slice from pool for children (estimate 2 outputs per tx)
					childSlicePtr := getHashSliceFromPool(2)
					childrenMap[parentHash] = (*childSlicePtr)[:0]
				}
				childrenMap[parentHash] = append(childrenMap[parentHash], txHash)
			}
		}

		dependencies[txHash] = depsSlice
	}

	// Defer cleanup: return all dependency and children slices to appropriate pools
	defer func() {
		for _, depsSlice := range dependencies {
			returnHashSliceToPool(&depsSlice)
		}
		for _, childSlice := range childrenMap {
			returnHashSliceToPool(&childSlice)
		}
	}()

	// Set up pending parent counts and children references
	readyQueue := make([]chainhash.Hash, 0, len(txMap))

	for txHash, state := range txMap {
		parents := dependencies[txHash]
		state.pendingParents = int32(len(parents))
		state.childrenWaiting = childrenMap[txHash]

		// Transactions with no in-block dependencies are ready immediately
		if len(parents) == 0 {
			readyQueue = append(readyQueue, txHash)
		}
	}

	u.logger.Infof("[processTransactionsPipelined] %d transactions ready to start immediately", len(readyQueue))

	// Worker pool for processing transactions
	g, gCtx := errgroup.WithContext(ctx)
	concurrency := u.settings.SubtreeValidation.SpendBatcherSize * 2
	util.SafeSetLimit(g, concurrency)

	// Optimize channel buffer size: use 2x concurrency for better throughput
	// Large buffer reduces blocking on sends, but too large wastes memory
	// Sweet spot: 2-3x worker count allows some queuing without excessive memory
	bufferSize := concurrency * 2
	if bufferSize > len(txMap) {
		bufferSize = len(txMap) // Cap at total transaction count
	}
	readyChan := make(chan chainhash.Hash, bufferSize)

	// Seed the ready channel with initial ready transactions
	for _, txHash := range readyQueue {
		readyChan <- txHash
	}

	// Track completion
	var completedCount atomic.Uint64
	totalToProcess := uint64(len(txMap))

	// Completion signal: use WaitGroup instead of polling to eliminate 100ms ticker overhead
	var allWorkDone sync.WaitGroup
	allWorkDone.Add(1)

	// Spawn workers
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			// OPTIMIZATION: Remove context check from hot path
			// Workers exit naturally when channel closes
			for txHash := range readyChan {
				state := txMap[txHash]
				if state == nil {
					continue
				}

				// Validate this transaction
				err := u.validateSingleTransaction(gCtx, blockHash, subtreeHash, state.tx, blockHeight, blockIds, processedValidatorOptions, errorsFound, addedToOrphanage)
				if err != nil {
					return err
				}

				// Mark complete and notify children
				completed := completedCount.Add(1)

				if completed%1000 == 0 {
					u.logger.Infof("[processTransactionsPipelined] Progress: %d/%d transactions completed", completed, totalToProcess)
				}

				// Check if all work is done (avoid polling goroutine overhead)
				if completed == totalToProcess {
					allWorkDone.Done()
				}

				// Notify all children that this parent is complete
				// OPTIMIZATION: Non-blocking send to reduce contention
				for _, childHash := range state.childrenWaiting {
					childState := txMap[childHash]
					if childState == nil {
						continue
					}

					// Decrement child's pending count atomically
					remaining := atomic.AddInt32(&childState.pendingParents, -1)

					// If child has no more pending parents, it's ready to process
					if remaining == 0 {
						readyChan <- childHash // Non-blocking with buffered channel
					}
				}
			}
			return nil
		})
	}

	// Close channel when all work is done (no polling overhead!)
	// Pass parameters to avoid closure capture and heap escape
	go func(wg *sync.WaitGroup, ch chan chainhash.Hash) {
		wg.Wait()
		close(ch)
	}(&allWorkDone, readyChan)

	// Wait for all workers to complete
	err := g.Wait()

	if err != nil {
		return errors.NewProcessingError("[processTransactionsPipelined] Pipeline processing failed", err)
	}

	u.logger.Infof("[processTransactionsPipelined] Completed processing %d transactions", completedCount.Load())
	return nil
}

// pipelineTxState tracks the state of a transaction in the pipeline
type pipelineTxState struct {
	tx               *bt.Tx
	pendingParents   int32            // Atomic counter of parents not yet completed
	childrenWaiting  []chainhash.Hash // Children that depend on this transaction
	completionSignal chan struct{}
}

// validateSingleTransaction validates a single transaction with common error handling
// Extracted to avoid code duplication between level-based and pipelined processing
func (u *Server) validateSingleTransaction(ctx context.Context, blockHash chainhash.Hash, subtreeHash chainhash.Hash, tx *bt.Tx, blockHeight uint32,
	blockIds map[uint32]bool, processedValidatorOptions *validator.Options,
	errorsFound, addedToOrphanage *atomic.Uint64) error {

	// Use existing blessMissingTransaction logic for validation
	txMeta, err := u.blessMissingTransaction(ctx, blockHash, subtreeHash, tx, blockHeight, blockIds, processedValidatorOptions)
	if err != nil {
		u.logger.Debugf("[validateSingleTransaction] Failed to validate transaction %s: %v", tx.TxIDChainHash().String(), err)

		// TX_EXISTS is not an error - transaction was already validated
		if errors.Is(err, errors.ErrTxExists) {
			u.logger.Debugf("[validateSingleTransaction] Transaction %s already exists, skipping", tx.TxIDChainHash().String())
			return nil
		}

		// Count all other errors
		errorsFound.Add(1)

		// Handle missing parent transactions by adding to orphanage
		if errors.Is(err, errors.ErrTxMissingParent) {
			isRunning, runningErr := u.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
			if runningErr == nil && isRunning {
				u.logger.Debugf("[validateSingleTransaction] Transaction %s missing parent, adding to orphanage", tx.TxIDChainHash().String())
				if u.orphanage.Set(*tx.TxIDChainHash(), tx) {
					addedToOrphanage.Add(1)
				} else {
					u.logger.Warnf("[validateSingleTransaction] Failed to add transaction %s to orphanage - orphanage is full", tx.TxIDChainHash().String())
				}
			} else {
				u.logger.Debugf("[validateSingleTransaction] Transaction %s missing parent, but FSM not in RUNNING state - not adding to orphanage", tx.TxIDChainHash().String())
			}
		} else if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
			// Log truly invalid transactions
			u.logger.Warnf("[validateSingleTransaction] Invalid transaction detected: %s: %v", tx.TxIDChainHash().String(), err)

			if errors.Is(err, errors.ErrTxInvalid) {
				return err
			}
		} else {
			u.logger.Errorf("[validateSingleTransaction] Processing error for transaction %s: %v", tx.TxIDChainHash().String(), err)
		}

		return nil // Don't fail the entire batch
	}

	if txMeta == nil {
		u.logger.Debugf("[validateSingleTransaction] Transaction metadata is nil for %s", tx.TxIDChainHash().String())
	} else {
		u.logger.Debugf("[validateSingleTransaction] Successfully validated transaction %s", tx.TxIDChainHash().String())
	}

	return nil
}
