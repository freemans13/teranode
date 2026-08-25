package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// claimBlockSQL takes the ledger row for a block, or reports that someone already has it.
//
// ON CONFLICT DO NOTHING is genuinely idempotent here, unlike on the UTXO table: this
// table has a real PRIMARY KEY for it to act on. It is also the concurrency gate. A
// second caller offering the same block blocks on the conflicting row until the first
// transaction resolves, then inserts nothing if that transaction committed, or takes the
// claim if it rolled back. So a caller can never skip a block whose application is still
// in flight, and a rolled-back application never leaves the block marked as done.
const claimBlockSQL = `
INSERT INTO applied_block (height, block_hash, chunk_size, chunk_count, completed)
VALUES ($1, $2, $3, $4, TRUE)
ON CONFLICT (block_hash) DO NOTHING`

// ApplyBlock runs fn as one atomic, replay-safe unit of block application.
//
// It returns (true, nil) when fn ran and committed, and (false, nil) when the block had
// already been applied and was therefore skipped.
//
// This gate is not an optimisation, it is what keeps the store from inflating the money
// supply. The UTXO table's ukey is a 96-bit prefix and deliberately NON-UNIQUE, so
// createSQL has no ON CONFLICT that could make an insert idempotent, and re-applying a
// block would insert every output a second time as independently spendable rows. Replay
// is routine rather than exotic: catchup, a restart mid-window, and the documented
// post-restart unrequested-block storm all re-offer blocks. The ledger is therefore
// ground truth written in the SAME transaction as the work it describes, so the claim
// and the rows it authorises commit or roll back together.
//
// Running fn inside that one transaction is the second half of the guard, and it is what
// makes the coinbase safe. A replayed ordinary transaction would fail its spends anyway,
// because the parents it consumes are gone, and take the chunk down with it. A coinbase
// has no inputs and so has no failing spend to trigger that rollback; only the ledger
// stops it.
func (s *Store) ApplyBlock(ctx context.Context, blockHash *chainhash.Hash, height uint32,
	fn func(q querier) error) (bool, error) {
	if blockHash == nil {
		return false, errors.NewProcessingError("[utxoset][ApplyBlock] nil block hash")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, errors.NewStorageError("[utxoset][ApplyBlock] begin %s", blockHash.String(), err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, claimBlockSQL, int32(height), blockHash[:], 0, 1)
	if err != nil {
		return false, errors.NewStorageError("[utxoset][ApplyBlock] claim %s", blockHash.String(), err)
	}

	if tag.RowsAffected() == 0 {
		return false, nil // already applied by us or by a committed peer
	}

	if err = fn(tx); err != nil {
		return false, err
	}

	if err = tx.Commit(ctx); err != nil {
		return false, errors.NewStorageError("[utxoset][ApplyBlock] commit %s", blockHash.String(), err)
	}

	return true, nil
}
