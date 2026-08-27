package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// The DELETE is the whole design, and it lives in spendJournalSQL because the journal is
// not optional. That one statement does five jobs. It ARBITRATES DOUBLE-SPENDS: an
// outpoint that is absent deletes zero rows, and absence is the rejection -- there is no
// spent-set to consult. It is the DECORATE FETCH: RETURNING hands back satoshis and the
// locking script, so script validation never fetches or deserialises a parent
// transaction. It CAPTURES THE UNDO PAYLOAD, in the same statement rather than merely the
// same transaction. It is the RECLAIM: the coin row is gone. And it is the write.
//
// There used to be a second, journal-free variant of it here for below-checkpoint sync.
// It is gone rather than kept behind a flag, because two copies of a consensus predicate
// that "must stay identical" is a defect waiting for one of them to be edited.

// classifySQL explains a miss. Reached only when the DELETE affected fewer rows than
// inputs offered, which is the uncommon path.
//
// The distinction matters and cannot be skipped: absence alone cannot tell "already
// spent" from "never existed" from "exists but is not yet spendable", and the validator
// behaves differently for each. Note the deliberate absence of the flag and maturity
// predicates here — this asks "does the row exist at all", precisely so a row excluded
// by the DELETE's eligibility tests surfaces as frozen or immature rather than as spent.
// spenderSQL asks the journal WHO took a coin that is no longer there.
//
// The coin row is destroyed by the spend, so absence is how a double spend is rejected. That
// answers "no" but not "who", and the caller needs "who": it marks the losing transaction
// conflicting and walks its descendants.
//
// The journal already recorded the spending transaction against every coin it destroyed, so
// a reorg could match the spender that actually took it. The same row answers this question.
//
// Matched on the full 32-byte parent txid as well as the ukey, for the same reason every
// other predicate here is: the ukey is a non-unique 96-bit prefix, so it can locate a row but
// never authorise one.
//
// Bounded by the journal's retention. Beyond it the store genuinely cannot say who took a
// coin, and that is a stated limit of delete-on-spend rather than a gap to paper over.
const spenderSQL = `
SELECT k.vin, j.spending_txid
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[]) AS k(leaf, ukey, txid, vin)
  JOIN spend_journal j ON j.ukey = k.ukey AND j.txid = k.txid`

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
	if err := s.ensureSpendJournalPartition(ctx, blockHeight); err != nil {
		return nil, err
	}

	return s.spendIn(ctx, s.pool, tx, blockHeight, ignoreFlags...)
}

// spendIn is Spend against an arbitrary querier.
func (s *Store) spendIn(ctx context.Context, q querier, tx *bt.Tx, blockHeight uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if tx == nil || tx.IsCoinbase() {
		return nil, nil
	}

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

	// The partition must already exist. Creating it HERE would be a nested acquire from
	// the same pool that q is borrowed from when q is a transaction, and once the number
	// of concurrent spenders reaches pool_max_conns every connection is held by a
	// transaction waiting for a connection. That deadlock has no timeout. Callers ensure
	// the partition before they open their transaction.
	rows, err := q.Query(ctx, spendJournalSQL, leaves, ukeys, txids, vins,
		int32(blockHeight), spendingTxID[:])
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
	// existed. The coin table cannot distinguish those two, which is why ErrSpent is the
	// honest answer here.
	missing := false

	for _, vin := range vins {
		if _, ok := done[vin]; ok {
			continue
		}

		if _, ok := present[vin]; !ok {
			spends[vin].Err = errors.ErrSpent
			missing = true
		}
	}

	if !missing {
		return nil
	}

	return s.nameSpenders(ctx, q, leaves, ukeys, txids, vins, spends)
}

// nameSpenders fills in which transaction took each coin that is no longer there.
//
// Reached only after a spend has already failed, so its cost falls on the uncommon path.
// Beyond the journal's retention it finds nothing and leaves the field nil, which is the
// honest answer rather than a wrong one.
func (s *Store) nameSpenders(ctx context.Context, q querier, leaves []int16, ukeys [][16]byte,
	txids [][]byte, vins []int32, spends []*utxo.Spend) error {
	rows, err := q.Query(ctx, spenderSQL, leaves, ukeys, txids, vins)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] find spender", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			vin     int32
			spender []byte
		)

		if err := rows.Scan(&vin, &spender); err != nil {
			return errors.NewStorageError("[utxoset][Spend] spender scan", err)
		}

		if spends[vin] == nil || !errors.Is(spends[vin].Err, errors.ErrSpent) {
			continue
		}

		h, herr := chainhash.NewHash(spender)
		if herr != nil {
			return errors.NewStorageError("[utxoset][Spend] spender hash", herr)
		}

		spends[vin].ConflictingTxID = h
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] spender rows", err)
	}

	return nil
}
