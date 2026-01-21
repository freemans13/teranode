package subtreevalidation

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

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
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances.
// With 14,496 subtrees per block, using 32KB buffers provides excellent I/O performance
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 1024*1024) // Temp changed to 1MB buffer for scaling env - 32KB buffer - optimized for sequential I/O
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

	// Check which subtrees are missing first to avoid unnecessary pause logic
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

	// Early return if all subtrees already exist - no need for pause logic
	if len(missingSubtrees) == 0 {
		return &subtreevalidation_api.CheckBlockSubtreesResponse{
			Blessed: true,
		}, nil
	}

	u.logger.Infof("[CheckBlockSubtrees] Found %d missing subtrees for block %s, proceeding with validation", len(missingSubtrees), block.Hash().String())

	// BATCHED SUBTREE LOADING: Get blockIds once before batching
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return nil, errors.NewProcessingError("[CheckSubtree] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))
	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Calculate batch size dynamically based on configured transaction batch size
	totalSubtrees := len(missingSubtrees)
	totalProcessedTxs := 0
	var subtreesBatchSize int

	txBatchSize := u.settings.SubtreeValidation.TxBatchSize

	if txBatchSize == 0 {
		// No batching - process all subtrees at once
		subtreesBatchSize = totalSubtrees
	} else if block.TransactionCount > 0 && len(block.Subtrees) > 0 {
		// Calculate exact txs per subtree using block metadata
		txsPerSubtree := int(block.TransactionCount / uint64(len(block.Subtrees)))
		subtreesBatchSize = txBatchSize / txsPerSubtree
		if subtreesBatchSize == 0 {
			subtreesBatchSize = 1 // Minimum 1 subtree per batch
		}
	} else {
		// Fallback if metadata not available (shouldn't happen)
		subtreesBatchSize = 1
		u.logger.Warnf("[CheckBlockSubtrees] Block metadata incomplete (txs=%d, subtrees=%d), using 1 subtree per batch",
			block.TransactionCount, len(block.Subtrees))
	}

	// Process subtrees in batches to limit memory usage
	// Each batch loads subtree data, processes transactions, then GCs before next batch
	for batchStart := 0; batchStart < totalSubtrees; batchStart += subtreesBatchSize {
		batchEnd := batchStart + subtreesBatchSize
		if batchEnd > totalSubtrees {
			batchEnd = totalSubtrees
		}

		batchNum := (batchStart / subtreesBatchSize) + 1
		batchSubtrees := missingSubtrees[batchStart:batchEnd]
		u.logger.Infof("[CheckBlockSubtrees] Processing subtree batch %d/%d with %d subtrees for block %s", batchNum, (totalSubtrees+subtreesBatchSize-1)/subtreesBatchSize, len(batchSubtrees), block.Hash().String())

		// Load transactions for this batch of subtrees in parallel
		subtreeTxs := make([][]*bt.Tx, len(batchSubtrees))
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

		for subtreeIdx, subtreeHash := range batchSubtrees {
			subtreeHash := subtreeHash
			subtreeIdx := subtreeIdx

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

				// PHASE 2: Exact pre-allocation
				subtreeTxs[subtreeIdx] = make([]*bt.Tx, 0, subtreeToCheck.Length())

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
			return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to get subtree tx hashes for batch %d", batchNum, err)
		}

		// Collect all transactions from this batch of subtrees
		// Calculate exact capacity needed across all subtrees in this batch to avoid reallocations
		totalTxCapacity := 0
		for _, txs := range subtreeTxs {
			totalTxCapacity += len(txs)
		}
		allTransactions := make([]*bt.Tx, 0, totalTxCapacity)
		for _, txs := range subtreeTxs {
			if len(txs) > 0 {
				allTransactions = append(allTransactions, txs...)
			}
		}

		// Release 2D subtree transaction slice after consolidation
		// All transactions now in allTransactions, original 2D structure no longer needed
		subtreeTxs = nil //nolint:ineffassign // Intentional early GC hint

		batchTxCount := len(allTransactions)
		totalBatches := (totalSubtrees + subtreesBatchSize - 1) / subtreesBatchSize
		u.logger.Infof("[CheckBlockSubtrees] Batch %d/%d loaded %d transactions for block %s, now processing", batchNum, totalBatches, batchTxCount, block.Hash().String())

		// Process transactions for this batch
		if batchTxCount > 0 {
			if err = u.processTransactionsInLevels(ctx, allTransactions, *block.Hash(), chainhash.Hash{}, block.Height, blockIds); err != nil {
				errStr := err.Error()
				// During fork processing it's expected that some transactions will either:
				// - be marked as conflicting/spent, or
				// - be temporarily missing parents and placed into the orphanage.
				// In these cases we must not fail the whole block.
				if strings.Contains(errStr, "[processTransactionsInLevels] Completed processing with") &&
					(strings.Contains(errStr, "UTXO_SPENT") ||
						strings.Contains(errStr, "TX_CONFLICTING") ||
						!strings.Contains(errStr, ", 0 transactions added to orphanage")) {
					u.logger.Warnf("[CheckBlockSubtrees] Non-fatal transaction processing errors for block %s: %v", block.Hash().String(), err)
				} else {
					return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in batch %d", batchNum, err)
				}
			}
			totalProcessedTxs += batchTxCount

			// Release transaction slice after processing completes
			// Transactions are now in UTXO store and validator cache, original slice no longer needed
			allTransactions = nil //nolint:ineffassign // Intentional early GC hint
		}

		batchSubtrees = nil //nolint:ineffassign // Intentional early GC hint for batch slice view
		u.logger.Infof("[CheckBlockSubtrees] Batch %d/%d complete for block %s (%d txs processed, %d total), memory reclaimed", batchNum, totalBatches, block.Hash().String(), batchTxCount, totalProcessedTxs)
	}

	u.logger.Infof("[CheckBlockSubtrees] Completed processing %d transactions across %d subtree batches", totalProcessedTxs, (totalSubtrees+subtreesBatchSize-1)/subtreesBatchSize)

	// Subtree validation continues regardless of whether we processed transactions
	g, gCtx := errgroup.WithContext(ctx)
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
				gCtx,
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

// processSubtreeDataStream downloads subtreeData and simultaneously stores to disk while parsing transactions.
// PHASE 1: Concurrent streaming - eliminates storage read-back by writing to disk while parsing.
func (u *Server) processSubtreeDataStream(ctx context.Context, subtree *subtreepkg.Subtree,
	body io.ReadCloser, allTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processSubtreeDataStream",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[processSubtreeDataStream] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Create a pipe for concurrent storage write
	pr, pw := io.Pipe()

	// Channel to capture storage errors
	storeDone := make(chan error, 1)

	// Goroutine to write to storage concurrently
	go func() {
		err := u.subtreeStore.SetFromReader(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, pr)
		storeDone <- err
		// If storage failed, close pipe writer to unblock any pending writes
		// This prevents deadlock when SetFromReader returns an error without fully draining the pipe reader
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	// Use TeeReader to split network stream to storage and parser simultaneously
	teeReader := io.TeeReader(body, pw)

	// Use pooled bufio.Reader for parsing
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(teeReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// Parse transactions while writing to storage
	txCount, parseErr := u.readTransactionsFromSubtreeDataStream(subtree, bufferedReader, allTransactions)

	// Close the pipe writer to signal completion to storage goroutine
	// Use CloseWithError if parsing failed to properly signal the storage goroutine
	if parseErr != nil {
		pw.CloseWithError(parseErr)
	} else {
		pw.Close()
	}

	// Wait for storage operation to complete
	storeErr := <-storeDone

	// Check for errors from both operations
	if storeErr != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to store subtree data", storeErr)
	}

	if parseErr != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to parse transactions", parseErr)
	}

	// Verify transaction count
	if txCount != subtree.Length() {
		return errors.NewProcessingError("[processSubtreeDataStream] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	u.logger.Debugf("[processSubtreeDataStream] Processed %d transactions from subtree %s (single-pass streaming)",
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

	txHashes := make([]chainhash.Hash, len(allTransactions))

	for i, tx := range allTransactions {
		if tx == nil {
			return errors.NewProcessingError("[processTransactionsInLevels] transaction is nil at index %d", i)
		}

		txHashes[i] = *tx.TxIDChainHash()
	}

	// Pre-check: identify transactions that are already validated in cache or UTXO store
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := u.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to check txMeta cache", err)
	}

	if missed > 0 {
		u.logger.Infof("[processTransactionsInLevels] Pre-check: %d/%d transactions missed in cache, checking UTXO store", missed, len(txHashes))

		batched := u.settings.SubtreeValidation.BatchMissingTransactions
		missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, blockIds, batched, false)
		if err != nil {
			return errors.NewProcessingError("[processTransactionsInLevels] Failed to check txMeta store", err)
		}
	}

	alreadyValidated := len(txHashes) - missed

	if missed == 0 {
		u.logger.Infof("[processTransactionsInLevels] All transactions already validated, skipping processing")
		return nil
	} else if alreadyValidated > 0 {
		u.logger.Infof("[processTransactionsInLevels] Pre-check: %d/%d transactions already validated, %d need validation", alreadyValidated, len(txHashes), missed)
	}

	// Convert transactions to missingTx format for prepareTxsPerLevel
	missingTxs := make([]missingTx, len(allTransactions))

	for i, tx := range allTransactions {
		if txMetaSlice[i].isSet {
			// Transaction already validated, skip
			continue
		}

		missingTxs[i] = missingTx{
			tx:  tx,
			idx: i,
		}
	}

	u.logger.Infof("[processTransactionsInLevels] Preparing to validate %d transactions using Validator.ValidateMulti", len(allTransactions))

	// Get FSM state to determine block assembly flag
	currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to get FSM current state", err)
	}

	// Build validator options for ValidateMulti
	opts := &validator.Options{
		AutoExtendTransactions: true, // Enable automatic transaction extension
		SkipPolicyChecks:       true,
		CreateConflicting:      true,
		IgnoreLocked:           true,
		ParentBlockHeights:     make(map[chainhash.Hash]uint32),
		AddTXToBlockAssembly:   true,
		ChunkSize:              75,
		MaxConcurrentChunks:    2, // VERY conservative: 2 chunks to prevent Aerospike overload
	}

	// During legacy syncing or catching up, disable adding transactions to block assembly
	if *currentState == blockchain.FSMStateLEGACYSYNCING || *currentState == blockchain.FSMStateCATCHINGBLOCKS {
		opts.AddTXToBlockAssembly = false

		// Skip CPU-intensive script verification during catchup if setting is enabled
		if u.settings.Validator.SkipScriptVerificationDuringCatchup {
			opts.SkipScriptVerification = true
			u.logger.Infof("[processTransactionsInLevels] Skipping script verification during catchup for block %s (setting enabled)", blockHash.String())
		}
	}

	// ⭐ NEW: Use ValidateMulti for batch validation with automatic level organization
	multiResult, err := u.validatorClient.ValidateMulti(ctx, allTransactions, blockHeight, opts)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] ValidateMulti failed: %v", err)
	}

	// Track validation results
	var (
		successCount     int
		errorsFound      int
		addedToOrphanage int
	)

	// Process results from ValidateMulti
	for txHash, txResult := range multiResult.Results {
		if txResult.Success {
			successCount++
			u.logger.Debugf("[processTransactionsInLevels] Successfully validated transaction %s", txHash.String())
		} else {
			// Handle validation errors
			err := txResult.Err
			u.logger.Debugf("[processTransactionsInLevels] Failed to validate transaction %s: %v", txHash.String(), err)

			// TX_EXISTS is not an error - transaction was already validated
			if errors.Is(err, errors.ErrTxExists) {
				u.logger.Debugf("[processTransactionsInLevels] Transaction %s already exists, skipping", txHash.String())
				continue
			}

			// Conflicting/Spent are expected outcomes when CreateConflicting is enabled.
			// The validator records these transactions as conflicting; block processing must continue.
			if opts.CreateConflicting {
				if errors.Is(err, errors.ErrSpent) || errors.Is(err, errors.ErrTxConflicting) {
					u.logger.Debugf("[processTransactionsInLevels] Transaction %s marked as conflicting: %v", txHash.String(), err)
					continue
				}

				// Handle cases where we only have a teranode error code available.
				// In fork processing we expect some spends to fail due to conflicts.
				var tErr *errors.Error
				if errors.As(err, &tErr) {
					switch tErr.Code() {
					case errors.ERR_TX_CONFLICTING, errors.ERR_UTXO_SPENT:
						u.logger.Debugf("[processTransactionsInLevels] Transaction %s marked as conflicting (code): %v", txHash.String(), err)
						continue
					case errors.ERR_UTXO_ERROR:
						// This error is used as an aggregate for spend failures. When it is the
						// standard 'could not be spent' case, treat it as a conflict outcome.
						if strings.Contains(strings.ToLower(tErr.Message()), "could not be spent") {
							u.logger.Debugf("[processTransactionsInLevels] Transaction %s marked as conflicting (utxo_error): %v", txHash.String(), err)
							continue
						}
					}
				}

				// Some UTXO backends (or gRPC rehydration) return spent/conflict failures where
				// the underlying code doesn't survive as a wrapped error chain. In those cases,
				// the canonical code is still present in the formatted error string.
				errStr := err.Error()
				if strings.Contains(errStr, "UTXO_SPENT") || strings.Contains(errStr, "TX_CONFLICTING") {
					u.logger.Debugf("[processTransactionsInLevels] Transaction %s marked as conflicting (string-match): %v", txHash.String(), err)
					continue
				}
			}

			// Handle missing parent transactions by adding to orphanage.
			// Missing parents are expected during parallel subtree processing, but we still
			// report them as errors via the aggregate errorsFound return so callers can decide.
			if errors.Is(err, errors.ErrTxMissingParent) {
				isRunning, runningErr := u.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
				if runningErr == nil && isRunning {
					u.logger.Debugf("[processTransactionsInLevels] Transaction %s missing parent, adding to orphanage", txHash.String())
					// Find the transaction in allTransactions to add to orphanage
					for _, tx := range allTransactions {
						if tx != nil && *tx.TxIDChainHash() == txHash {
							if u.orphanage.Set(txHash, tx) {
								addedToOrphanage++
							} else {
								u.logger.Warnf("[processTransactionsInLevels] Failed to add transaction %s to orphanage - orphanage is full", txHash.String())
							}
							break
						}
					}
				} else {
					u.logger.Debugf("[processTransactionsInLevels] Transaction %s missing parent, but FSM not in RUNNING state - not adding to orphanage", txHash.String())
				}
				errorsFound++
				continue
			}

			// Count all other errors
			errorsFound++

			if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
				// Log truly invalid transactions and fail
				u.logger.Warnf("[processTransactionsInLevels] Invalid transaction detected: %s: %v", txHash.String(), err)
				return err
			} else {
				u.logger.Errorf("[processTransactionsInLevels] Processing error for transaction %s: %v", txHash.String(), err)
			}
		}
	}

	if errorsFound > 0 {
		return errors.NewProcessingError("[processTransactionsInLevels] Completed processing with %d errors, %d transactions added to orphanage", errorsFound, addedToOrphanage)
	}

	u.logger.Infof("[processTransactionsInLevels] Successfully processed all %d transactions (validated: %d)", len(allTransactions), successCount)

	txMetaSlice = nil //nolint:ineffassign // Intentional early GC hint

	return nil
}

// NOTE: buildParentMapFromLevel and extendTxWithInBlockParents functions have been moved
// to services/validator/tx_extender.go as part of the ValidateMulti refactoring.
// These optimizations are now handled automatically by Validator.ValidateMulti when
// AutoExtendTransactions option is enabled.
