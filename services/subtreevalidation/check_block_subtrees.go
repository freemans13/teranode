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
	// This value balances performance (fewer round trips) with reliability (avoiding timeouts).
	aerospikeBatchChunkSize = 5000

	// maxTransactionsPerChunk bounds memory usage for very large blocks (600M+ transactions)
	// Target: ~2GB peak memory per chunk
	// Calculation:
	//   - 8M transactions × 250 bytes avg = 2GB transaction data
	//   - ~400K external parents (5%) × 1KB metadata = 400MB parent data
	//   - Dependency graph overhead = ~300MB
	//   Total: ~2.7GB per chunk
	// For 600M tx block: 75 chunks × 125ms = ~10 seconds, 2.7GB peak memory
	maxTransactionsPerChunk = 8_000_000 // 8 million transactions
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances.
// With 14,496 subtrees per block, using 32KB buffers provides excellent I/O performance
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 32*1024) // 32KB buffer - optimized for sequential I/O
	},
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read
type countingReadCloser struct {
	reader    io.ReadCloser
	bytesRead *uint64 // Pointer to allow external access to count
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	atomic.AddUint64(c.bytesRead, uint64(n))
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

	// get all the subtrees that are missing from the peer in parallel
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

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
				var bytesRead uint64
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
					if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
						u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), bytesRead, peerID, err)
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

	// Get block header IDs once for all chunks
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return nil, errors.NewProcessingError("[CheckSubtree] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))
	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	// Process transactions in chunks to bound memory usage for very large blocks
	// Each chunk processes up to maxTransactionsPerChunk (8M) transactions, limiting memory to ~2GB
	currentChunk := make([]*bt.Tx, 0, maxTransactionsPerChunk)
	chunkNum := 0
	totalProcessed := 0

	for subtreeIdx, txs := range subtreeTxs {
		if len(txs) == 0 {
			continue
		}

		currentChunk = append(currentChunk, txs...)

		// Process chunk when it reaches size limit or is the last subtree
		isLastSubtree := subtreeIdx == len(subtreeTxs)-1
		shouldProcessChunk := len(currentChunk) >= maxTransactionsPerChunk || isLastSubtree

		if shouldProcessChunk && len(currentChunk) > 0 {
			chunkNum++
			u.logger.Infof("[CheckBlockSubtrees] Processing chunk %d with %d transactions (total processed: %d)", chunkNum, len(currentChunk), totalProcessed)

			// Process this chunk with existing optimized pipeline
			if err = u.processTransactionsInLevels(ctx, currentChunk, *block.Hash(), chainhash.Hash{}, block.Height, blockIds); err != nil {
				return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in chunk %d", chunkNum, err)
			}

			totalProcessed += len(currentChunk)
			u.logger.Infof("[CheckBlockSubtrees] Completed chunk %d, total processed: %d", chunkNum, totalProcessed)

			// Free memory before next chunk (only if not last subtree)
			if !isLastSubtree {
				currentChunk = make([]*bt.Tx, 0, maxTransactionsPerChunk)
			}
		}
	}

	subtreeTxs = nil // Clear the slice to free memory

	if totalProcessed == 0 {
		u.logger.Infof("[CheckBlockSubtrees] No transactions to validate")
	} else {
		u.logger.Infof("[CheckBlockSubtrees] Completed processing %d transactions in %d chunks from %d subtrees", totalProcessed, chunkNum, len(missingSubtrees))
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

	// Create a buffer to capture the data for storage
	var buffer bytes.Buffer

	// Use TeeReader to read from HTTP stream while writing to buffer
	teeReader := io.TeeReader(body, &buffer)

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

// prefetchAndCacheParentUTXOs scans all transactions, identifies required parent UTXOs,
// fetches them in batch, and pre-populates the cache to eliminate round-trips during validation
func (u *Server) prefetchAndCacheParentUTXOs(ctx context.Context, allTransactions []*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "prefetchAndCacheParentUTXOs",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[prefetchAndCacheParentUTXOs] Prefetching parent UTXOs for %d transactions", len(allTransactions)),
	)
	defer deferFn()

	// Step 1: Collect all unique parent UTXO hashes from all transactions
	parentHashes := make(map[chainhash.Hash]struct{})
	transactionHashes := make(map[chainhash.Hash]struct{}, len(allTransactions))

	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		// Track this transaction hash
		txHash := *tx.TxIDChainHash()
		transactionHashes[txHash] = struct{}{}

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
	unresolvedMeta := make([]*utxo.UnresolvedMetaData, 0, len(parentHashes))
	for hash := range parentHashes {
		hash := hash // capture loop variable
		unresolvedMeta = append(unresolvedMeta, &utxo.UnresolvedMetaData{
			Hash: hash,
			Fields: []fields.FieldName{
				fields.Fee,
				fields.SizeInBytes,
				fields.TxInpoints,
				fields.BlockIDs,
				fields.IsCoinbase,
			},
		})
	}

	totalRecords := len(unresolvedMeta)
	u.logger.Infof("[prefetchAndCacheParentUTXOs] Prefetching %d unique parent UTXOs", totalRecords)

	// Step 3: Batch fetch all parent UTXOs in chunks to prevent Aerospike timeouts
	// Large batches (>10K records) can cause network errors and timeouts
	start := time.Now()
	totalChunks := (totalRecords + aerospikeBatchChunkSize - 1) / aerospikeBatchChunkSize

	for i := 0; i < totalRecords; i += aerospikeBatchChunkSize {
		end := min(i+aerospikeBatchChunkSize, totalRecords)
		chunk := unresolvedMeta[i:end]
		chunkNum := i/aerospikeBatchChunkSize + 1

		u.logger.Infof("[prefetchAndCacheParentUTXOs] Processing chunk %d/%d (%d records)", chunkNum, totalChunks, len(chunk))

		err := u.utxoStore.BatchDecorate(ctx, chunk, fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.BlockIDs, fields.IsCoinbase)
		if err != nil {
			return errors.NewProcessingError("[prefetchAndCacheParentUTXOs] Failed to batch fetch parent UTXOs (chunk %d/%d)", chunkNum, totalChunks, err)
		}
	}

	fetchDuration := time.Since(start)
	u.logger.Infof("[prefetchAndCacheParentUTXOs] Fetched %d parent UTXOs in %d chunks in %v", totalRecords, totalChunks, fetchDuration)

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

	// Collect all unique parent transaction hashes across all transactions
	parentHashes := make(map[chainhash.Hash]struct{})
	transactionHashes := make(map[chainhash.Hash]struct{}, len(allTransactions))

	for _, tx := range allTransactions {
		if tx == nil || tx.IsCoinbase() || tx.IsExtended() {
			continue // Skip if already extended
		}

		txHash := *tx.TxIDChainHash()
		transactionHashes[txHash] = struct{}{}

		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()
			// Collect both in-block and external parents
			parentHashes[parentHash] = struct{}{}
		}
	}

	if len(parentHashes) == 0 {
		u.logger.Infof("[batchExtendTransactions] No parent UTXOs to fetch for extension")
		return nil
	}

	// Fetch all parent UTXOs using BatchDecorate in chunks to prevent Aerospike timeouts
	unresolvedMeta := make([]*utxo.UnresolvedMetaData, 0, len(parentHashes))
	parentMap := make(map[chainhash.Hash]int, len(parentHashes))
	idx := 0
	for hash := range parentHashes {
		hash := hash
		unresolvedMeta = append(unresolvedMeta, &utxo.UnresolvedMetaData{
			Hash: hash,
			Idx:  idx,
			Fields: []fields.FieldName{
				fields.Outputs, // Only need output data (amount + locking script), not full transaction with inputs/signatures
			},
		})
		parentMap[hash] = idx
		idx++
	}

	totalRecords := len(unresolvedMeta)
	u.logger.Infof("[batchExtendTransactions] Fetching %d unique parent outputs for transaction extension", totalRecords)

	// Fetch in chunks to prevent Aerospike timeouts on large batches
	start := time.Now()
	totalChunks := (totalRecords + aerospikeBatchChunkSize - 1) / aerospikeBatchChunkSize

	for i := 0; i < totalRecords; i += aerospikeBatchChunkSize {
		end := min(i+aerospikeBatchChunkSize, totalRecords)
		chunk := unresolvedMeta[i:end]
		chunkNum := i/aerospikeBatchChunkSize + 1

		u.logger.Infof("[batchExtendTransactions] Processing chunk %d/%d (%d records)", chunkNum, totalChunks, len(chunk))

		err := u.utxoStore.BatchDecorate(ctx, chunk, fields.Outputs)
		if err != nil {
			return errors.NewProcessingError("[batchExtendTransactions] Failed to batch fetch parent UTXOs (chunk %d/%d)", chunkNum, totalChunks, err)
		}
	}

	fetchDuration := time.Since(start)
	u.logger.Infof("[batchExtendTransactions] Fetched %d parent UTXOs in %d chunks in %v", totalRecords, totalChunks, fetchDuration)

	// Build a map of parent hash -> parent tx for quick lookup
	// Also count large transactions that may be stored externally
	parentTxMap := make(map[chainhash.Hash]*bt.Tx, len(unresolvedMeta))
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

	if largeTxCount > 0 {
		u.logger.Infof("[batchExtendTransactions] Found %d large parent transactions (>100 outputs, likely external) out of %d parents", largeTxCount, len(unresolvedMeta))
	}

	// Now extend all transactions using the parent data
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
				u.logger.Warnf("[batchExtendTransactions] Parent tx %s doesn't have output index %d", parentHash.String(), input.PreviousTxOutIndex)
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

	u.logger.Infof("[batchExtendTransactions] Extended %d/%d transactions", extendedCount, len(allTransactions))
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

	// Build dependency graph
	txMap := make(map[chainhash.Hash]*pipelineTxState, len(transactions))
	dependencies := make(map[chainhash.Hash][]chainhash.Hash) // child -> parents
	childrenMap := make(map[chainhash.Hash][]chainhash.Hash)  // parent -> children

	for _, mTx := range transactions {
		if mTx.tx == nil || mTx.tx.IsCoinbase() {
			continue
		}

		txHash := *mTx.tx.TxIDChainHash()
		txMap[txHash] = &pipelineTxState{
			tx:               mTx.tx,
			pendingParents:   0,
			childrenWaiting:  make([]chainhash.Hash, 0),
			completionSignal: make(chan struct{}),
		}

		dependencies[txHash] = make([]chainhash.Hash, 0)

		// Build dependency relationships
		for _, input := range mTx.tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()

			// Only track dependencies within this block
			if _, exists := txMap[parentHash]; exists {
				dependencies[txHash] = append(dependencies[txHash], parentHash)

				if childrenMap[parentHash] == nil {
					childrenMap[parentHash] = make([]chainhash.Hash, 0)
				}
				childrenMap[parentHash] = append(childrenMap[parentHash], txHash)
			}
		}
	}

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

	// Channel for transactions that become ready
	readyChan := make(chan chainhash.Hash, len(txMap))

	// Seed the ready channel with initial ready transactions
	for _, txHash := range readyQueue {
		readyChan <- txHash
	}

	// Track completion
	var completedCount atomic.Uint64
	totalToProcess := uint64(len(txMap))

	// Spawn workers
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case txHash, ok := <-readyChan:
					if !ok {
						return nil // Channel closed, worker done
					}

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

					// Notify all children that this parent is complete
					for _, childHash := range state.childrenWaiting {
						childState := txMap[childHash]
						if childState == nil {
							continue
						}

						// Decrement child's pending count atomically
						remaining := atomic.AddInt32(&childState.pendingParents, -1)

						// If child has no more pending parents, it's ready to process
						if remaining == 0 {
							select {
							case readyChan <- childHash:
							case <-gCtx.Done():
								return gCtx.Err()
							}
						}
					}
				}
			}
		})
	}

	// Separate goroutine to close channel when all work is done
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for completedCount.Load() < totalToProcess {
			select {
			case <-ctx.Done():
				// Context cancelled, exit goroutine
				close(readyChan)
				return
			case <-ticker.C:
				// Continue polling
			}
		}
		close(readyChan)
	}()

	// Wait for all workers to complete
	if err := g.Wait(); err != nil {
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
