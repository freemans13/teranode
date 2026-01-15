package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// txWithIndex holds a transaction and its original index in the input array
type txWithIndex struct {
	tx  *bt.Tx
	idx int
}

// txLevelInfo holds level calculation information for a transaction
type txLevelInfo struct {
	tx                 *bt.Tx
	idx                int
	level              uint32
	someParentsInBlock bool
}

// organizeTxsByLevel organizes transactions by dependency levels using topological sort.
// This function handles transactions that may not be in topological order.
//
// The algorithm performs a complete dependency graph analysis:
// 1. Build parent-child dependency maps
// 2. Calculate levels using iterative topological sort
// 3. Detect circular dependencies
// 4. Group transactions by level
//
// Complexity: O(V*E + V²) where V=transactions, E=dependencies
// Use organizeTxsByLevelOrdered for O(V*I) complexity when inputs are pre-ordered.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txs: Slice of transactions to organize (may be in any order)
//
// Returns:
//   - [][]txWithIndex: Slice of dependency levels, each containing transactions at that level
//   - error: Error if circular dependencies detected
func organizeTxsByLevel(ctx context.Context, txs []*bt.Tx) ([][]txWithIndex, error) {
	_, _, deferFn := tracing.Tracer("validator").Start(ctx, "organizeTxsByLevel")
	defer deferFn()

	if len(txs) == 0 {
		return [][]txWithIndex{}, nil
	}

	// Build dependency graph with adjacency lists for efficient lookups
	txMap := make(map[chainhash.Hash]*txLevelInfo, len(txs))
	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]int)

	// First pass: create all nodes and initialize structures
	for i, tx := range txs {
		if tx != nil && !tx.IsCoinbase() {
			hash := *tx.TxIDChainHash()
			txMap[hash] = &txLevelInfo{
				tx:                 tx,
				idx:                i,
				level:              0,
				someParentsInBlock: false,
			}
		}
	}

	// Second pass: calculate dependency levels using topological approach
	// Build dependency graph first
	dependencies := make(map[chainhash.Hash][]chainhash.Hash) // child -> parents

	for i, tx := range txs {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		txHash := *tx.TxIDChainHash()
		dependencies[txHash] = make([]chainhash.Hash, 0)

		// Check each input of the transaction to find its parents
		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()

			// check if parentHash exists in the map, which means it is part of the batch
			if _, exists := txMap[parentHash]; exists {
				dependencies[txHash] = append(dependencies[txHash], parentHash)
			}
		}

		// Update txMap entry
		if info, exists := txMap[txHash]; exists {
			info.idx = i
		}
	}

	// Calculate levels using iterative topological sort to avoid stack overflow
	// and detect circular dependencies
	levelCache := make(map[chainhash.Hash]uint32)

	// Find all transactions with no dependencies (level 0)
	for txHash, parents := range dependencies {
		if len(parents) == 0 {
			levelCache[txHash] = 0
		}
	}

	// Process remaining transactions level by level
	// Maximum iterations is len(dependencies) + 1 to handle all possible levels
	maxIterations := len(dependencies) + 1
	for iteration := 0; iteration < maxIterations; iteration++ {
		progress := false

		for txHash, parents := range dependencies {
			if _, exists := levelCache[txHash]; exists {
				continue
			}

			// Check if all parents have computed levels
			allParentsComputed := true
			maxParentLevel := uint32(0)
			for _, parentHash := range parents {
				parentLevel, exists := levelCache[parentHash]
				if !exists {
					allParentsComputed = false
					break
				}
				if parentLevel > maxParentLevel {
					maxParentLevel = parentLevel
				}
			}

			if allParentsComputed {
				levelCache[txHash] = maxParentLevel + 1
				progress = true
			}
		}

		if !progress {
			// No progress made - check if we're done or have a cycle
			if len(levelCache) < len(dependencies) {
				return nil, errors.NewProcessingError("Circular dependency detected in transaction graph")
			}
			break
		}
	}

	// Update level info with calculated levels
	for _, tx := range txs {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		txHash := *tx.TxIDChainHash()
		info := txMap[txHash]
		if info == nil {
			continue
		}

		level, exists := levelCache[txHash]
		if !exists {
			// This shouldn't happen if the algorithm is correct
			return nil, errors.NewProcessingError("Failed to calculate level for transaction")
		}

		info.level = level
		info.someParentsInBlock = len(dependencies[txHash]) > 0

		sizePerLevel[level]++
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Build result slices with pre-allocated capacity
	txsPerLevelSlice := make([][]txWithIndex, maxLevel+1)
	for level := uint32(0); level <= maxLevel; level++ {
		txsPerLevelSlice[level] = make([]txWithIndex, 0, sizePerLevel[level])
	}

	// Populate result slices
	for _, info := range txMap {
		level := info.level
		txsPerLevelSlice[level] = append(txsPerLevelSlice[level], txWithIndex{
			tx:  info.tx,
			idx: info.idx,
		})
	}

	return txsPerLevelSlice, nil
}

// organizeTxsByLevelOrdered is an optimized version of organizeTxsByLevel that assumes transactions
// are already in topological order (parents before children), as guaranteed by the Bitcoin protocol.
//
// ORDERING GUARANTEE: The Bitcoin protocol mandates that transactions within a block must be ordered
// such that parent transactions appear before their children. This is enforced during block construction
// and validated during block processing.
//
// This optimization reduces complexity from O(V*E + V²) to O(V*I) where:
//   - V = number of transactions
//   - E = number of dependencies
//   - I = average inputs per transaction
//
// SINGLE-PASS OPTIMIZATION: Calculates levels AND groups transactions simultaneously in ONE iteration.
// Eliminates: second pass, redundant hash calculations, and extra map lookups.
// Optimized for 1M+ transaction batches.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txs: Slice of transactions in topological order (parents before children)
//
// Returns:
//   - [][]txWithIndex: Slice of dependency levels containing transactions at each level
//   - error: Any error encountered during processing
func organizeTxsByLevelOrdered(ctx context.Context, txs []*bt.Tx) ([][]txWithIndex, error) {
	_, _, deferFn := tracing.Tracer("validator").Start(ctx, "organizeTxsByLevelOrdered")
	defer deferFn()

	if len(txs) == 0 {
		return [][]txWithIndex{}, nil
	}

	// GC OPTIMIZATION: Use index-based approach to minimize heap allocations
	// Map stores hash -> transaction index (int is smaller than pointer + reduces map overhead)
	// Levels stored in slice for fast array access instead of map lookups
	txIndex := make(map[chainhash.Hash]int, len(txs))
	levels := make([]uint32, len(txs))

	// Pre-allocate result slices with reasonable initial capacity
	// Most transactions are level 0 (no parents in block), so optimize for that case
	txsPerLevel := make([][]txWithIndex, 1, 16)                  // Start with level 0, capacity for 16 levels
	txsPerLevel[0] = make([]txWithIndex, 0, len(txs)/2)          // Level 0: assume ~50% of txs

	maxLevel := uint32(0)
	validTxCount := 0 // Track valid transactions for index mapping

	// SINGLE PASS: calculate levels AND append to result slices simultaneously
	for i, tx := range txs {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		// GC OPTIMIZATION: Get hash pointer once and reuse it
		// This avoids copying the 32-byte hash multiple times
		txHashPtr := tx.TxIDChainHash()
		txHash := *txHashPtr // Single dereference for map operations

		maxParentLevel := uint32(0)
		hasParentInBlock := false

		// Check each input to find the maximum parent level
		// GC OPTIMIZATION: Look up parent level in array instead of map
		for _, input := range tx.Inputs {
			parentHashPtr := input.PreviousTxIDChainHash()
			parentHash := *parentHashPtr // Single dereference

			// If parent exists in txIndex, it's part of this batch
			if parentIdx, exists := txIndex[parentHash]; exists {
				hasParentInBlock = true
				// Array lookup is faster and more GC-friendly than map lookup
				parentLevel := levels[parentIdx]
				if parentLevel > maxParentLevel {
					maxParentLevel = parentLevel
				}
			}
		}

		// Calculate this transaction's level
		level := uint32(0)
		if hasParentInBlock {
			level = maxParentLevel + 1
		}

		// Store index mapping for children to reference
		// GC OPTIMIZATION: Store index (int) in map, level in array
		txIndex[txHash] = i
		levels[i] = level

		// Track max level and grow result slice if needed
		if level > maxLevel {
			maxLevel = level
			// Grow txsPerLevel slice to accommodate new level
			for uint32(len(txsPerLevel)) <= level {
				// GC OPTIMIZATION: Use more realistic capacity hints based on distribution
				// Level 0 is large, higher levels are progressively smaller
				capacity := 64
				if level == maxLevel && validTxCount > 1000 {
					// For new max level, estimate based on transaction count
					capacity = validTxCount / 100 // Heuristic: ~1% of txs at higher levels
					if capacity < 64 {
						capacity = 64
					}
				}
				txsPerLevel = append(txsPerLevel, make([]txWithIndex, 0, capacity))
			}
		}

		// Append directly to result slice (NO second pass!)
		txsPerLevel[level] = append(txsPerLevel[level], txWithIndex{
			tx:  tx,
			idx: i,
		})
		validTxCount++
	}

	return txsPerLevel, nil
}
