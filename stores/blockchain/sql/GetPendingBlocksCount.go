// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetPendingBlocksCount method, which returns the count
// of blocks that have been stored in the blockchain database but have not yet had
// their mining status properly recorded. This is used by WaitForPendingBlocks to
// efficiently wait for all blocks to be processed before loading unmined transactions.
package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetPendingBlocksCount returns the count of blocks whose mining status has not been properly recorded,
// regardless of subtree processing status.
//
// This method differs from GetBlocksMinedNotSet by not filtering on subtrees_set=true and
// returning only a count instead of full blocks. This is more efficient for WaitForPendingBlocks
// which only needs to know if there are any pending blocks, not their full data.
//
// The method returns all blocks with mined_set=false, including blocks that are still waiting
// for subtree processing. This ensures WaitForPendingBlocks waits for ALL blocks to be processed
// before loading unmined transactions, preventing race conditions.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//
// Returns:
//   - int: The count of blocks whose mining status needs to be set
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database errors or processing failures
func (s *SQL) GetPendingBlocksCount(ctx context.Context) (int, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetPendingBlocksCount")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Count all blocks where mined_set = false, regardless of subtrees_set status.
	// This ensures WaitForPendingBlocks waits for ALL blocks, including those still
	// processing subtrees, before loading unmined transactions.
	var count int
	q := `SELECT COUNT(*) FROM blocks WHERE mined_set = false`

	err := s.db.QueryRowContext(ctx, q).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
