package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// stampSQL records a block against every transaction that does not already claim it, and
// clears the mempool marker when the block is on the longest chain.
//
// The append is guarded by position_of_the_block rather than by a plain concatenation, so
// replaying a block does not record it twice.
//
// The marker is cleared only when the caller states the block is on the longest chain. That
// is the same rule the create gate applies, and for the same reason: "mined into some block"
// and "on the main chain" are different facts, and a transaction whose only block later
// loses must stay in the mempool set.
const stampSQL = `
UPDATE tx_ident
   SET membership = CASE
           WHEN position($3::bytea in coalesce(membership, '\x'::bytea)) > 0 THEN membership
           ELSE coalesce(membership, '\x'::bytea) || $3::bytea
       END,
       off_chain_since = CASE WHEN $4::boolean THEN NULL ELSE off_chain_since END
 WHERE leaf = $1 AND txid = $2`

// unstampSQL removes one block from a transaction's membership and puts it back in the
// mempool set with a FRESH clock.
//
// The clock is the store's current tip, NOT the transaction's creation height, and that is
// the fact that decides whether these two columns are one concept or two. A transaction
// created at height 100 and un-mined while the tip is 5,000 must wait from 5,000, or the
// preservation pass fires on it immediately. Both reference stores do the same.
const unstampSQL = `
UPDATE tx_ident
   SET membership = CASE
           WHEN position($3::bytea in coalesce(membership, '\x'::bytea)) > 0
           THEN overlay(membership placing ''::bytea
                        from position($3::bytea in membership) for 12)
           ELSE membership
       END,
       off_chain_since = $4
 WHERE leaf = $1 AND txid = $2`

// provePresentSQL is the SECOND statement, and splitting it out is not tidiness.
//
// The interface says every hash asked about must appear in the answer, and an implementation
// that cannot prove it must return an error. A single UPDATE ... RETURNING cannot: it returns
// nothing for a transaction that is already correctly mined, which is indistinguishable from
// the row not existing, so a replayed block would report every transaction in it as missing.
// Asking separately whether the row is there answers the question the contract actually poses.
const provePresentSQL = `
SELECT i.txid, i.membership
  FROM unnest($1::smallint[], $2::bytea[]) AS k(leaf, txid)
  JOIN tx_ident i ON i.leaf = k.leaf AND i.txid = k.txid`

// SetMinedMulti marks transactions as mined in the block described by info.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash,
	info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return map[chainhash.Hash][]uint32{}, nil
	}

	entry := packMembership([]utxo.MinedBlockInfo{info})

	stmt := stampSQL
	marker := any(nil)

	if info.UnsetMined {
		stmt = unstampSQL
		// A fresh clock from the current tip. See unstampSQL.
		marker = int32(s.GetBlockHeight()) //nolint:gosec // a chain height fits int32
	}

	batch := &pgxBatch{}
	for _, h := range hashes {
		if h == nil {
			continue
		}

		if info.UnsetMined {
			batch.queue(stmt, LeafFor(h[:]), h[:], entry, marker)
		} else {
			batch.queue(stmt, LeafFor(h[:]), h[:], entry, info.OnLongestChain)
		}
	}

	if err := batch.send(ctx, s.pool); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] stamp", err)
	}

	leaves := make([]int16, 0, len(hashes))
	txids := make([][]byte, 0, len(hashes))

	for _, h := range hashes {
		if h == nil {
			continue
		}

		leaves = append(leaves, LeafFor(h[:]))
		txids = append(txids, h[:])
	}

	rows, err := s.pool.Query(ctx, provePresentSQL, leaves, txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] prove", err)
	}

	out := make(map[chainhash.Hash][]uint32, len(hashes))

	for rows.Next() {
		var (
			txid       []byte
			membership []byte
		)

		if err := rows.Scan(&txid, &membership); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("[utxoset][SetMinedMulti] scan", err)
		}

		var h chainhash.Hash
		copy(h[:], txid)

		ids, _, _ := unpackMembership(membership)
		out[h] = ids
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][SetMinedMulti] rows", err)
	}

	// The postcondition, checked rather than assumed. A partial map would leave the caller
	// believing transactions were mined that this store has never heard of.
	//
	// Un-mining is exempt, because the interface says missing entries are tolerated there:
	// a reorg may un-mine a transaction the store has already discarded. Tolerated means it
	// does not error, NOT that the answer is empty. Transactions that DO still exist must
	// still appear, which the conformance suite checks.
	if info.UnsetMined {
		return out, nil
	}

	for _, h := range hashes {
		if h == nil {
			continue
		}

		if _, ok := out[*h]; !ok {
			return nil, errors.NewTxNotFoundError("[utxoset][SetMinedMulti] %s", h.String())
		}
	}

	return out, nil
}
