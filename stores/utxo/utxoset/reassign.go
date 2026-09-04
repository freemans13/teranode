package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// reassignSQL moves ONE frozen coin to a new output, in one statement.
//
// The frozen bit is a predicate rather than a prior read, and that is the whole safety
// property. A read-then-write would leave a window in which an UnFreezeUTXOs, or a spend
// that becomes legal the moment the freeze lifts, lands between the two; here the coin is
// either frozen at the instant of the update or the update takes nothing and the caller is
// told. It is the same reason FreezeUTXOs enforces on the row rather than beside it.
//
// The full 32-byte txid recheck is present for the reason it is present everywhere in this
// store: the ukey is a non-unique 96-bit prefix, so it can locate a row but never authorise
// acting on one. Reassigning a coin is handing someone else's money to a new owner, which is
// the last place to trust a prefix.
//
// hash_override is the only thing written about the new output, because it is the only thing
// the interface carries: ReAssignUTXO is handed a utxo.Spend, which names an outpoint and a
// UTXO hash and has no room for a locking script or an amount. So the satoshis and the script
// on the row stay as they were, and the spend path stops trusting them the moment
// hash_override is non-NULL -- see claimMismatch.
//
// The frozen bit is cleared in the same statement. Leaving it set would make the coin
// permanently unspendable by its new owner, since the spend's own predicate excludes a frozen
// row, and clearing it separately would expose a moment in which the old owner's claim is
// live again against a coin that is no longer theirs.
const reassignSQL = `
UPDATE utxo u
   SET hash_override  = $4,
       flags          = u.flags & ~$5::smallint,
       spendable_from = $6
 WHERE u.leaf = $1 AND u.ukey = $2 AND u.txid = $3
   AND (u.flags & $5::smallint) > 0`

// reassignProbeSQL asks whether the coin is there at all, and is reached only when the
// update took nothing.
//
// Zero rows has two causes and the caller must be told which. A coin that is present but not
// frozen is a procedural error: the alert system is required to freeze before it reassigns,
// and answering "not found" would send an operator looking for a missing coin. A coin that is
// absent was already spent or never existed, and saying "not frozen" about it would be a
// claim this store cannot support. Neither may be silence.
const reassignProbeSQL = `
SELECT u.flags FROM utxo u
 WHERE u.leaf = $1 AND u.ukey = $2 AND u.txid = $3`

// ReAssignUTXO hands a frozen coin to a new output.
//
// This is the alert system's confiscation path: a court order freezes an output and then
// reassigns it to a new owner, who may spend it once the delay has passed. The delay is not
// cosmetic -- it is the window in which the reassignment can itself be challenged and undone
// before the coin moves again -- so it is enforced on the row by spendable_from, the same
// column coinbase maturity uses, which means the spend statement already honours it and no
// second check can drift out of step with the first.
//
// The interface gives this store a hash of the new output and nothing else. Every other store
// keeps a UTXO hash as the coin's identity and simply overwrites it. This one keeps the
// satoshis and the locking script themselves, because its spend is also its decorate fetch, so
// there is nothing here to overwrite with: the new script is not in the argument. What it can
// do is record what the new output must hash to and refuse anything else, which is what
// hash_override is for and what the spend path enforces.
func (s *Store) ReAssignUTXO(ctx context.Context, old *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	if old == nil || old.TxID == nil {
		return errors.NewProcessingError("[utxoset][ReAssignUTXO] nil utxo")
	}

	if newUtxo == nil || newUtxo.UTXOHash == nil {
		return errors.NewProcessingError("[utxoset][ReAssignUTXO] %s:%d has no new utxo hash to reassign to",
			old.TxID, old.Vout)
	}

	reassignBlocks := uint32(utxo.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}

	spendableFrom := s.GetBlockHeight() + reassignBlocks

	leaf := LeafFor(old.TxID[:])
	ukey := Pack(old.TxID[:], old.Vout)

	tag, err := s.pool.Exec(ctx, reassignSQL, leaf, ukey, old.TxID[:], newUtxo.UTXOHash[:], FlagFrozen,
		int32(spendableFrom)) //nolint:gosec // block heights are far below 2^31
	if err != nil {
		return errors.NewStorageError("[utxoset][ReAssignUTXO] %s:%d", old.TxID, old.Vout, err)
	}

	if tag.RowsAffected() == 1 {
		return nil
	}

	return s.explainReassignMiss(ctx, old, leaf, ukey)
}

// explainReassignMiss turns "the update took nothing" into the specific refusal.
func (s *Store) explainReassignMiss(ctx context.Context, old *utxo.Spend, leaf int16, ukey [16]byte) error {
	var flags int16

	if err := s.pool.QueryRow(ctx, reassignProbeSQL, leaf, ukey, old.TxID[:]).Scan(&flags); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NewTxNotFoundError("[utxoset][ReAssignUTXO] %s:%d does not exist; it was already spent or never created",
				old.TxID, old.Vout)
		}

		return errors.NewStorageError("[utxoset][ReAssignUTXO] classify %s:%d", old.TxID, old.Vout, err)
	}

	return errors.NewUtxoFrozenError("[utxoset][ReAssignUTXO] transaction %s:%d is not frozen", old.TxID, old.Vout)
}
