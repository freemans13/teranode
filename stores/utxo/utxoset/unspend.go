package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// unspendSQL restores coins from the journal and CONSUMES the journal rows doing it.
//
// The journal row IS the authorisation. Deleting it as part of the restore makes the
// operation single-use: a second Unspend of the same outpoint matches nothing and does
// nothing. That is what keeps invariant 5 intact -- the restore is authorised by the
// presence of a durable row, not by a counter that could drift. A counter drift is
// exactly what once stamped a live transaction and cascade-deleted it into a
// TX_NOT_FOUND wedge.
//
// Three predicates, each load-bearing:
//
//	j.ukey = i.ukey            locates candidates -- and is the journal's only index
//	j.txid = i.ptxid           full 32 bytes: the ukey is a non-unique 96-bit prefix and
//	                           can never establish identity on its own
//	j.spending_txid = i.stxid  THE OWNERSHIP TOKEN. A restore must name the spender that
//	                           actually took the coin. A stale reorg record whose output
//	                           has since been re-spent by a DIFFERENT transaction matches
//	                           nothing and is a no-op, rather than resurrecting a coin
//	                           that now belongs to someone else.
//
// The NOT EXISTS guard is belt-and-braces: with a non-unique key a concurrent re-create
// could otherwise produce a duplicate live row for one outpoint, which is counterfeit.
//
// $4 carries the flags to OR in, so a caller asking for flagAsLocked gets the coin back
// already locked rather than briefly spendable.
const unspendSQL = `
WITH items AS (
    SELECT * FROM unnest($1::uuid[], $2::bytea[], $3::bytea[]) AS t(ukey, ptxid, stxid)
),
taken AS (
    DELETE FROM utxo_undo j USING items i
     WHERE j.ukey          = i.ukey
       AND j.txid          = i.ptxid
       AND j.spending_txid = i.stxid
    RETURNING j.ukey, j.txid, j.satoshis, j.script, j.created_height,
              j.spendable_from, j.flags, j.hash_override
),
restored AS (
    INSERT INTO utxo (leaf, txid, ukey, satoshis, script, created_height,
                      spendable_from, flags, hash_override)
    SELECT (get_byte(t.txid, 0) & 7)::smallint, t.txid, t.ukey, t.satoshis, t.script,
           t.created_height, t.spendable_from, t.flags | $4::smallint, t.hash_override
      FROM taken t
     WHERE NOT EXISTS (
           SELECT 1 FROM utxo u
            WHERE u.leaf = (get_byte(t.txid, 0) & 7)::smallint
              AND u.ukey = t.ukey
              AND u.txid = t.txid)
    RETURNING ukey
)
SELECT (SELECT count(*) FROM restored), (SELECT count(*) FROM items)`

// Unspend restores previously spent UTXOs from the undo journal.
//
// Used by reorg handling and by conflicting-transaction resolution. Note that a plain
// block disconnect does NOT come through here: a transaction in a disconnected block is
// still valid and returns to assembly unmined, so its spends must STAY -- restoring them
// would create a coin that a still-live transaction is already spending.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if len(spends) == 0 {
		return nil
	}

	var extraFlags int16
	if len(flagAsLocked) > 0 && flagAsLocked[0] {
		extraFlags |= FlagLocked
	}

	ukeys := make([][16]byte, 0, len(spends))
	ptxids := make([][]byte, 0, len(spends))
	stxids := make([][]byte, 0, len(spends))

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil {
			continue
		}

		if sp.SpendingData == nil || sp.SpendingData.TxID == nil {
			// Without the spender there is no ownership token, and restoring on the
			// outpoint alone could resurrect a coin a different transaction now owns.
			// Refuse rather than guess.
			return errors.NewProcessingError("[utxoset][Unspend] %s:%d has no SpendingData; the spender is required as the restore ownership token",
				sp.TxID.String(), sp.Vout)
		}

		ukeys = append(ukeys, Pack(sp.TxID[:], sp.Vout))
		ptxids = append(ptxids, sp.TxID[:])
		stxids = append(stxids, sp.SpendingData.TxID[:])
	}

	if len(ukeys) == 0 {
		return nil
	}

	var restored, requested int

	if err := s.pool.QueryRow(ctx, unspendSQL, ukeys, ptxids, stxids, extraFlags).
		Scan(&restored, &requested); err != nil {
		return errors.NewStorageError("[utxoset][Unspend] restore", err)
	}

	if restored != requested {
		// Silence here would be the dangerous outcome: a reorg that believes it has
		// restored coins which are in fact still missing leaves the UTXO set wrong and
		// consensus-divergent, with nothing to indicate it. Either every requested coin
		// came back or the caller must know it did not.
		//
		// The usual causes are a journal partition already reclaimed (the spend is older
		// than retention), or a spender mismatch meaning the coin was re-spent by a
		// different transaction in the meantime.
		return errors.NewProcessingError("[utxoset][Unspend] restored %d of %d requested; the rest are beyond journal retention or were re-spent by a different transaction",
			restored, requested)
	}

	return nil
}
