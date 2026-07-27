package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// spendSQL is the whole design in one statement.
//
// It does four jobs at once. It is the DOUBLE-SPEND ARBITER: an outpoint that is absent
// deletes zero rows, and absence is the rejection — there is no spent-set to consult.
// It is the DECORATE FETCH: RETURNING hands back satoshis and the locking script, so
// script validation never fetches or deserialises a parent transaction. It is the
// RECLAIM: the row is gone, so there is no sweep to fold it and no pruner to chase it.
// And it is the write.
//
// Two predicates are load-bearing rather than defensive:
//
//	u.txid = k.txid     The ukey is a 96-bit txid prefix and is NON-UNIQUE. It locates
//	                    candidate rows; it can never authorise one. Identity is the full
//	                    32 bytes, carried on the row and rechecked here. A prefix
//	                    collision therefore costs one extra heap visit and can never
//	                    spend the wrong coin.
//
//	u.spendable_from    Coinbase maturity and the ReAssignUTXO delay, folded into one
//	  <= $5             precomputed height so the hot path never branches on "is this a
//	                    coinbase".
//
// Frozen and conflicting outputs are excluded by flag mask, so a frozen coin fails the
// DELETE itself rather than being caught by a separate lookup that could race it.
const spendSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[])
        AS t(leaf, ukey, txid, vin)
)
DELETE FROM utxo u USING k
 WHERE u.leaf           = k.leaf
   AND u.ukey           = k.ukey
   AND u.txid           = k.txid
   AND (u.flags & 1)    = 0          -- not frozen
   AND (u.flags & 4)    = 0          -- not conflicting
   AND u.spendable_from <= $5
RETURNING k.vin, u.satoshis, u.script`

// classifySQL explains a miss. Reached only when the DELETE affected fewer rows than
// inputs offered, which is the uncommon path.
//
// The distinction matters and cannot be skipped: absence alone cannot tell "already
// spent" from "never existed" from "exists but is not yet spendable", and the validator
// behaves differently for each. Note the deliberate absence of the flag and maturity
// predicates here — this asks "does the row exist at all", precisely so a row excluded
// by the DELETE's eligibility tests surfaces as frozen or immature rather than as spent.
const classifySQL = `
SELECT k.vin, u.flags, u.spendable_from
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, vin)
  JOIN utxo u ON u.leaf = k.leaf AND u.ukey = k.ukey AND u.txid = k.txid`

// Spend consumes every input of tx.
//
// Errors are reported per-input on the returned Spend records rather than as a single
// error, matching the postgres store's contract: the caller needs to know WHICH input
// failed and why.
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return s.spendIn(ctx, s.pool, tx, blockHeight, ignoreFlags...)
}

// spendIn is Spend against an arbitrary querier.
func (s *Store) spendIn(ctx context.Context, q querier, tx *bt.Tx, blockHeight uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if tx == nil || tx.IsCoinbase() {
		return nil, nil
	}

	var err error

	n := len(tx.Inputs)
	leaves := make([]int16, n)
	ukeys := make([][16]byte, n)
	txids := make([][]byte, n)
	vins := make([]int32, n)
	spends := make([]*utxo.Spend, n)

	for i, in := range tx.Inputs {
		parent := in.PreviousTxIDChainHash()
		leaves[i] = LeafFor(parent[:])
		ukeys[i] = Pack(parent[:], in.PreviousTxOutIndex)
		txids[i] = parent[:]
		vins[i] = int32(i)
		spends[i] = &utxo.Spend{TxID: parent, Vout: in.PreviousTxOutIndex}
	}

	spendingTxID := tx.TxIDChainHash()

	var rows pgx.Rows

	if s.journal.Load() {
		// The journal leaf must exist before the insert, and creating it is idempotent.
		if err = s.ensureUndoPartition(ctx, blockHeight); err != nil {
			return nil, err
		}

		rows, err = q.Query(ctx, spendJournalSQL, leaves, ukeys, txids, vins,
			int32(blockHeight), spendingTxID[:])
	} else {
		rows, err = q.Query(ctx, spendSQL, leaves, ukeys, txids, vins, int32(blockHeight))
	}

	if err != nil {
		return nil, errors.NewStorageError("[utxoset][Spend] delete", err)
	}

	done := make(map[int32]struct{}, n)

	for rows.Next() {
		var (
			vin      int32
			satoshis int64
			script   []byte
		)

		if err := rows.Scan(&vin, &satoshis, &script); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("[utxoset][Spend] scan", err)
		}

		done[vin] = struct{}{}

		// The decorate fetch, free: the input now carries what script validation needs,
		// so PreviousOutputsDecorate has nothing left to do for this input.
		if in := tx.Inputs[vin]; in != nil {
			in.PreviousTxSatoshis = uint64(satoshis)
			in.PreviousTxScript = bscript.NewFromBytes(script)
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][Spend] rows", err)
	}

	if len(done) == n {
		return spends, nil
	}

	return spends, s.classifyMisses(ctx, q, leaves, ukeys, txids, vins, blockHeight, done, spends)
}

// classifyMisses turns "the DELETE did not take this row" into a specific error.
func (s *Store) classifyMisses(ctx context.Context, q querier, leaves []int16, ukeys [][16]byte, txids [][]byte,
	vins []int32, blockHeight uint32, done map[int32]struct{}, spends []*utxo.Spend) error {
	rows, err := q.Query(ctx, classifySQL, leaves, ukeys, txids, vins)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] classify", err)
	}
	defer rows.Close()

	present := make(map[int32]struct{})

	for rows.Next() {
		var (
			vin           int32
			flags         int16
			spendableFrom int32
		)

		if err := rows.Scan(&vin, &flags, &spendableFrom); err != nil {
			return errors.NewStorageError("[utxoset][Spend] classify scan", err)
		}

		present[vin] = struct{}{}

		switch {
		case flags&FlagFrozen != 0:
			spends[vin].Err = errors.ErrFrozen
		case flags&FlagConflicting != 0:
			spends[vin].Err = errors.ErrTxConflicting
		case uint32(spendableFrom) > blockHeight:
			// Exists, but immature — a coinbase before maturity, or a reassigned output
			// still inside its delay. NOT a double-spend, and reporting it as one would
			// be wrong.
			spends[vin].Err = errors.NewProcessingError("[utxoset] utxo not spendable until height %d (current %d)", spendableFrom, blockHeight)
		}
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] classify rows", err)
	}

	// Anything neither deleted nor present is genuinely gone: already spent, or it never
	// existed. A delete-on-spend store cannot distinguish those two without the undo
	// journal, which is why ErrSpent is the honest answer here and why spender identity
	// is explicitly a capability this store gives up beyond the journal's retention.
	for _, vin := range vins {
		if _, ok := done[vin]; ok {
			continue
		}

		if _, ok := present[vin]; !ok {
			spends[vin].Err = errors.ErrSpent
		}
	}

	return nil
}
