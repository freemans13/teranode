package utxoset

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/jackc/pgx/v5"
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

// claimSQL inserts the identity row and reports whether THIS caller inserted it.
//
// ON CONFLICT names (leaf, txid) and not (txid): a partitioned parent rejects the shorter
// form with "there is no unique or exclusion constraint matching the ON CONFLICT
// specification". And the answer comes back through RETURNING rather than by letting the
// uniqueness violation raise, because inside a pgx batch a raised error aborts every later
// statement in the batch -- see commit d648732a9 in this repository.
//
// created_height is NOT updated on conflict. The first sighting wins, because it is
// tx_body's filing address and moving it would strand the body.
const claimSQL = `
INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since, membership,
                      fee, size_in_bytes, tx_inpoints, locktime, created_at, flags)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (leaf, txid) DO NOTHING
RETURNING 1`

// bodySQL files the serialized bytes in the window tx_ident's created_height names, so the
// two always agree about where to look.
const bodySQL = `
INSERT INTO tx_body (created_height, txid, raw_tx) VALUES ($1, $2, $3)`

// packMembership renders mined-block information into the packed form tx_ident carries:
// 12-byte triples of block id, block height and subtree index, big-endian, in the order the
// caller supplied. Insertion order is load-bearing -- the conformance suite requires subtree
// indexes to come back in the order they were written rather than sorted.
func packMembership(infos []utxo.MinedBlockInfo) []byte {
	if len(infos) == 0 {
		return nil
	}

	b := make([]byte, 0, len(infos)*12)

	for _, mi := range infos {
		var e [12]byte

		binary.BigEndian.PutUint32(e[0:4], mi.BlockID)
		binary.BigEndian.PutUint32(e[4:8], mi.BlockHeight)
		binary.BigEndian.PutUint32(e[8:12], uint32(mi.SubtreeIdx)) //nolint:gosec // subtree index is never negative

		b = append(b, e[:]...)
	}

	return b
}

// offChainSinceAt decides whether a newly created transaction belongs in the mempool set.
//
// The rule is NOT "was mined-block information supplied". It is "has anyone told us a
// MAIN-CHAIN block contains this". The distinction is the whole point, and getting it wrong
// is a live bug in the reference stores.
//
// A block-application create passes the block's mined info but leaves OnLongestChain false,
// because at create time the block is still being validated and its chain status is
// genuinely unknown. Clearing the marker on that basis gives a transaction from a block that
// later loses fork-only membership and no marker, so it reads as mined, and once that block
// is 288 deep it reads as SETTLED despite never having been on the main chain. Block
// assembly repairs that at reset today; a reclaimer would delete the parent first and make
// it unrepairable.
//
// Marking it and letting the mined stamp clear it fails in the safe direction: a transaction
// wrongly in the mempool set is narrowed out on the next reload by machinery that already
// runs.
func offChainSinceAt(infos []utxo.MinedBlockInfo, blockHeight uint32) *int32 {
	for _, mi := range infos {
		if mi.OnLongestChain && !mi.UnsetMined {
			return nil
		}
	}

	h := int32(blockHeight) //nolint:gosec // block height fits int32 for any reachable chain

	return &h
}

// Create records a transaction's spendable outputs in the UTXO table.
//
// Only SPENDABLE outputs get a row. A provably-unspendable output — an OP_RETURN data
// carrier — creates nothing, because a row that can never be deleted would sit in the
// table forever and the UTXO table's size is the entire budget. This mirrors the
// postgres store's spendable_count and the aerospike store's ShouldStoreOutputAsUTXO
// gate, so all three agree on what "spendable" means.
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
		return nil, err
	}

	return s.createIn(ctx, s.pool, tx, blockHeight, opts...)
}

// createIn is Create against an arbitrary querier, so SpendAndCreate can run it inside
// the same transaction as the spend.
func (s *Store) createIn(ctx context.Context, q querier, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
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

	// The caller's state options MUST reach the row. schema.go defines these bits and
	// spend.go checks all three when deciding whether a spend may proceed, so dropping
	// them here does not fail loudly, it creates an ordinary spendable output and lets
	// every downstream guard pass quietly on a zero bit. That is a silently wrong answer
	// rather than a missing feature, which is the worse of the two.
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var flags int16
	if isCoinbase {
		flags |= FlagCoinbase
	}

	if options.Frozen {
		flags |= FlagFrozen
	}

	if options.Conflicting {
		flags |= FlagConflicting
	}

	if options.Locked {
		flags |= FlagLocked
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
			continue // provably unspendable: no UTXO row, ever
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

	// THE CLAIM, and it must come before the coin rows.
	//
	// The UTXO table's key is a non-unique 96-bit prefix, so it can never reject a
	// duplicate. Identity does that, and it has to do it BEFORE any output is written or
	// the duplicate has already been applied. This is what retires the applied_block
	// ledger: a block arriving twice becomes a no-op transaction by transaction, and a
	// duplicate mempool submission is covered by the same mechanism rather than being
	// left unimplemented.
	var claimed int

	err := q.QueryRow(ctx, claimSQL,
		leaf, txHash[:], int32(blockHeight), offChainSinceAt(options.MinedBlockInfos, blockHeight),
		packMembership(options.MinedBlockInfos),
		nil, int32(tx.Size()), nil, int32(tx.LockTime), time.Now().UnixMilli(), flags,
	).Scan(&claimed)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Someone already holds this txid. Nothing of ours was written, so there is
		// nothing to undo.
		return nil, errors.NewTxExistsError("[utxoset][Create] %s", txHash.String())
	case err != nil:
		return nil, errors.NewStorageError("[utxoset][Create] claim %s", txHash.String(), err)
	}

	// The bytes, filed in the window created_height names. Gated by the claim like the coin
	// rows: a re-applied block must not write the body again, at a second height.
	if _, err := q.Exec(ctx, bodySQL, int32(blockHeight), txHash[:], tx.Bytes()); err != nil {
		return nil, errors.NewStorageError("[utxoset][Create] body %s", txHash.String(), err)
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
