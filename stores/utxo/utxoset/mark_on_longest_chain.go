package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// markOnLongestChainSQL moves transactions into or out of the mempool set, and reports which
// rows it actually reached.
//
// One statement of arrays, so repairing a thousand transactions costs the same round trip as
// repairing one. RETURNING is not decoration here: the caller is fixing an inconsistency, and
// a row that was silently skipped leaves that inconsistency in place, so it has to know which
// rows moved.
//
// off_chain_since is the only column this touches. Block membership is NOT rewritten, because
// this call does not claim to know which blocks a transaction is in. It answers one narrower
// question, which is whether the chain the node currently believes in contains it.
const markOnLongestChainSQL = `
UPDATE tx_ident i
   SET off_chain_since = $3::int
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
 WHERE i.leaf = k.leaf AND i.txid = k.txid
RETURNING i.txid`

// MarkTransactionsOnLongestChain records whether the node's current chain contains these
// transactions.
//
// Block assembly calls this at startup, as a repair. It walks the transactions still marked
// as waiting to be mined, finds any that already carry membership of a block on the main
// chain, and hands them here to have the marker cleared. Until this existed the node could
// not start at all once one such transaction was in the store, which is exactly the state an
// ordinary sync produces.
//
// onLongestChain true clears the marker. False sets it to the CURRENT tip rather than to the
// transaction's creation height, which is the same rule the un-mine path follows: a
// transaction created at height 100 and put back in the mempool while the tip is 5,000 must
// wait from 5,000, or the preservation pass fires on it immediately.
//
// A hash the store does not hold is an error rather than a silent skip, matching the sql
// store. The rows it DID reach are still updated, so a partial input repairs what it can.
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash,
	onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(txHashes))
	txids := make([][]byte, 0, len(txHashes))

	for i := range txHashes {
		leaves = append(leaves, LeafFor(txHashes[i][:]))
		txids = append(txids, txHashes[i][:])
	}

	// NULL when the chain contains it, the current tip when it does not.
	var marker any
	if !onLongestChain {
		marker = int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32
	}

	rows, err := s.pool.Query(ctx, markOnLongestChainSQL, leaves, txids, marker)
	if err != nil {
		return errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] update", err)
	}

	reached := make(map[chainhash.Hash]struct{}, len(txHashes))

	for rows.Next() {
		var txid []byte

		if err := rows.Scan(&txid); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] scan", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)
		reached[h] = struct{}{}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][MarkTransactionsOnLongestChain] rows", err)
	}

	if len(reached) == len(txHashes) {
		return nil
	}

	// Named but absent. Reported rather than swallowed, because the caller asked for a repair
	// and a row it could not reach is a repair that did not happen.
	missing := make([]error, 0, 4)

	for i := range txHashes {
		if _, ok := reached[txHashes[i]]; ok {
			continue
		}

		// Bounded, so one bad batch cannot produce an error the size of a block.
		if len(missing) < 10 {
			missing = append(missing,
				errors.NewTxNotFoundError("[utxoset][MarkTransactionsOnLongestChain] %s", txHashes[i].String()))
		}
	}

	return errors.Join(missing...)
}
