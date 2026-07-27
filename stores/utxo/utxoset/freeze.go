package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// setFlagsSQL ORs or ANDs a flag mask onto matching arbiter rows.
//
// $4 is OR-ed in, $5 is AND-ed, so one statement serves both freeze and unfreeze. The
// full 32-byte txid recheck is present for the same reason it is everywhere else: the
// ukey is a non-unique 96-bit prefix and can locate a row but never authorise acting on
// it.
const setFlagsSQL = `
UPDATE utxo u
   SET flags = (u.flags | $4::smallint) & $5::smallint
  FROM unnest($1::smallint[], $2::uuid[], $3::bytea[]) AS k(leaf, ukey, txid)
 WHERE u.leaf = k.leaf AND u.ukey = k.ukey AND u.txid = k.txid`

// FreezeUTXOs marks outputs unspendable.
//
// The flag lives on the arbiter row, so the spend path enforces it directly: a frozen
// coin fails the DELETE's own predicate rather than being caught by a separate lookup
// that could race the spend. There is no window in which a freeze has been recorded but
// a concurrent spend still succeeds.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	return s.setFlags(ctx, spends, FlagFrozen, ^int16(0), "FreezeUTXOs")
}

// UnFreezeUTXOs clears the frozen flag.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	return s.setFlags(ctx, spends, 0, ^FlagFrozen, "UnFreezeUTXOs")
}

func (s *Store) setFlags(ctx context.Context, spends []*utxo.Spend, orMask, andMask int16, op string) error {
	if len(spends) == 0 {
		return nil
	}

	leaves := make([]int16, 0, len(spends))
	ukeys := make([][16]byte, 0, len(spends))
	txids := make([][]byte, 0, len(spends))

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil {
			continue
		}

		leaves = append(leaves, LeafFor(sp.TxID[:]))
		ukeys = append(ukeys, Pack(sp.TxID[:], sp.Vout))
		txids = append(txids, sp.TxID[:])
	}

	if len(ukeys) == 0 {
		return nil
	}

	tag, err := s.pool.Exec(ctx, setFlagsSQL, leaves, ukeys, txids, orMask, andMask)
	if err != nil {
		return errors.NewStorageError("[utxoset][%s]", op, err)
	}

	if tag.RowsAffected() != int64(len(ukeys)) {
		// A freeze that silently missed is a freeze that did not happen, and the caller
		// would carry on believing the coin is immobilised. The usual cause is that the
		// output is already spent -- delete-on-spend leaves no row to flag.
		return errors.NewProcessingError("[utxoset][%s] affected %d of %d outputs; the rest are already spent or were never created",
			op, tag.RowsAffected(), len(ukeys))
	}

	return nil
}

// getSpendSQL answers "what happened to this outpoint" from the arbiter first and the
// journal second.
//
// The journal is doing double duty here. It exists so a reorg can restore a coin, but
// because it records spending_txid it is also the ONLY place this store can recover
// SPENDER IDENTITY -- the arbiter row is gone, and absence alone cannot distinguish
// "spent by X" from "never existed". That identity is therefore available exactly as far
// back as journal retention, and no further: beyond it the honest answer is NOT_FOUND.
const getSpendSQL = `
SELECT 'live'::text, u.flags, NULL::bytea
  FROM utxo u
 WHERE u.leaf = $1 AND u.ukey = $2 AND u.txid = $3
UNION ALL
SELECT 'spent'::text, j.flags, j.spending_txid
  FROM spend_journal j
 WHERE j.ukey = $2 AND j.txid = $3
 LIMIT 1`

// GetSpend reports whether an outpoint is unspent, frozen, or spent.
func (s *Store) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	if sp == nil || sp.TxID == nil {
		return nil, errors.NewProcessingError("[utxoset][GetSpend] nil spend")
	}

	ukey := Pack(sp.TxID[:], sp.Vout)

	rows, err := s.pool.Query(ctx, getSpendSQL, LeafFor(sp.TxID[:]), ukey, sp.TxID[:])
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][GetSpend]", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			where   string
			flags   int16
			spender []byte
		)

		if err := rows.Scan(&where, &flags, &spender); err != nil {
			return nil, errors.NewStorageError("[utxoset][GetSpend] scan", err)
		}

		if where == "live" {
			if flags&FlagFrozen != 0 {
				// A frozen coin reports a sentinel spender rather than a real one, which
				// is how every store signals "immobilised" through this interface.
				return &utxo.SpendResponse{
					Status:       int(utxo.Status_FROZEN),
					SpendingData: spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, 0),
				}, nil
			}

			return &utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil
		}

		hash, hErr := chainhash.NewHash(spender)
		if hErr != nil {
			return nil, errors.NewProcessingError("[utxoset][GetSpend] malformed spender in journal", hErr)
		}

		return &utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			SpendingData: spendpkg.NewSpendingData(hash, 0),
		}, nil
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][GetSpend] rows", err)
	}

	// Absent from both. Either it never existed, or it was spent longer ago than the
	// journal retains. This store cannot tell those apart, and says NOT_FOUND rather
	// than guessing.
	return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
}
