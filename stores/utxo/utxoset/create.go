package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// createSQL inserts one row per SPENDABLE output.
//
// There is deliberately no ON CONFLICT clause. The ukey is a 96-bit prefix and is
// non-unique, so there is no constraint for ON CONFLICT to act on — idempotence cannot
// come from the key here and must come from the applied_block ledger instead. Writing
// ON CONFLICT DO NOTHING against a non-unique index would not merely fail to protect
// anything, it would read as protection that does not exist.
const createSQL = `
INSERT INTO utxo (satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
SELECT * FROM unnest($1::bigint[], $2::int[], $3::int[], $4::smallint[], $5::smallint[],
                     $6::uuid[], $7::bytea[], $8::bytea[])`

// Create records a transaction's spendable outputs in the arbiter.
//
// Only SPENDABLE outputs get a row. A provably-unspendable output — an OP_RETURN data
// carrier — creates nothing, because a row that can never be deleted would sit in the
// arbiter forever and the arbiter's size is the entire budget. This mirrors the
// postgres store's spendable_count and the aerospike store's ShouldStoreOutputAsUTXO
// gate, so all three agree on what "spendable" means.
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return s.createIn(ctx, s.pool, tx, blockHeight, opts...)
}

// createIn is Create against an arbitrary querier, so SpendAndCreate can run it inside
// the same transaction as the spend.
func (s *Store) createIn(ctx context.Context, q querier, tx *bt.Tx, blockHeight uint32, _ ...utxo.CreateOption) (*meta.Data, error) {
	if tx == nil {
		return nil, errors.NewProcessingError("[utxoset][Create] nil tx")
	}

	txHash := tx.TxIDChainHash()
	leaf := LeafFor(txHash[:])
	isCoinbase := tx.IsCoinbase()

	// Coinbase maturity and the ReAssignUTXO delay are folded into one precomputed
	// height so the spend hot path never branches on "is this a coinbase" — it just
	// compares spendable_from against the current height.
	//
	// Zero for an ordinary output, NOT the creation height. There is no consensus rule
	// keeping a normal output from being spent at a height below the one it was created
	// at, and encoding one here would reject perfectly valid spends -- during a reorg,
	// or whenever a caller passes a height that is not strictly increasing. Only coinbase
	// maturity and an explicit reassignment delay may hold an output back.
	var spendableFrom int32
	if isCoinbase {
		spendableFrom = int32(blockHeight) + int32(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	var flags int16
	if isCoinbase {
		flags |= FlagCoinbase
	}

	genesisHeight := s.settings.ChainCfgParams.GenesisActivationHeight

	n := len(tx.Outputs)
	satoshis := make([]int64, 0, n)
	createdAt := make([]int32, 0, n)
	spendable := make([]int32, 0, n)
	leaves := make([]int16, 0, n)
	flagArr := make([]int16, 0, n)
	ukeys := make([][16]byte, 0, n)
	txids := make([][]byte, 0, n)
	scripts := make([][]byte, 0, n)

	for vout, out := range tx.Outputs {
		if out == nil {
			continue
		}

		if out.LockingScript != nil && !utxo.ShouldStoreOutputAsUTXO(out, blockHeight, genesisHeight) {
			continue // provably unspendable: no arbiter row, ever
		}

		var script []byte
		if out.LockingScript != nil {
			script = *out.LockingScript
		}

		satoshis = append(satoshis, int64(out.Satoshis))
		createdAt = append(createdAt, int32(blockHeight))
		spendable = append(spendable, spendableFrom)
		leaves = append(leaves, leaf)
		flagArr = append(flagArr, flags)
		ukeys = append(ukeys, Pack(txHash[:], uint32(vout)))
		txids = append(txids, txHash[:])
		scripts = append(scripts, script)
	}

	if len(ukeys) > 0 {
		if _, err := q.Exec(ctx, createSQL, satoshis, createdAt, spendable,
			leaves, flagArr, ukeys, txids, scripts); err != nil {
			return nil, errors.NewStorageError("[utxoset][Create] insert %s", txHash.String(), err)
		}
	}

	return &meta.Data{
		Tx:          tx,
		Fee:         0,
		SizeInBytes: uint64(tx.Size()),
		IsCoinbase:  isCoinbase,
	}, nil
}
