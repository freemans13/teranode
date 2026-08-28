package utxoset

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// noteConflictSQL records a contesting transaction on the parent whose coin it wants.
//
// A transaction that loses a double-spend race is stored as conflicting rather than
// discarded, because resolving the conflict later has to find it. Finding it means asking the
// PARENT whose coin was contested, so the parent carries the list. Without it, conflict
// resolution has no route from a contested coin to the transactions competing for it.
//
// Appended only when the parent does not already name it, so re-offering the same losing
// transaction does not grow the list. Matched on the full 32-byte txid, never on the ukey
// prefix alone.
//
// Membership is tested on a 32-byte BOUNDARY. The column is a concatenation of 32-byte ids and
// the reader unpacks it that way, rejecting any length that is not a multiple of 32. This used
// to be a plain substring search, which can match bytes STRADDLING two neighbouring entries,
// read that as already-present, and silently skip a real append: the parent then never names
// one of the transactions contesting its coin, and conflict resolution has no route to it.
// setConflictingSQL's own note statement in conflicting.go carries the identical test, and the
// two must stay identical, because they are two writers of one column.
const noteConflictSQL = `
UPDATE tx_ident
   SET conflicting_children = CASE
           WHEN EXISTS (
                SELECT 1
                  FROM generate_series(0, coalesce(length(conflicting_children), 0) / 32 - 1) g
                 WHERE substring(conflicting_children from g * 32 + 1 for 32) = $3::bytea)
           THEN conflicting_children
           ELSE coalesce(conflicting_children, '\x'::bytea) || $3::bytea
       END
 WHERE leaf = $1 AND txid = $2`

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
	// Batched when configured, which is the normal path. The batcher collects calls arriving
	// from many goroutines and sends them as one round trip, which is what the other two
	// implementations of this interface do and what this store was missing.
	//
	// The conflicting case takes the direct path. It writes to the PARENTS of the incoming
	// transaction rather than only to the transaction itself, so two items in one batch can
	// touch the same row, which is exactly the overlap the batched path assumes away.
	if s.createBatcher != nil {
		options := &utxo.CreateOptions{}
		for _, opt := range opts {
			opt(options)
		}

		if !options.Conflicting {
			done := make(chan createResult, 1)

			s.createBatcher.PutCtx(ctx, &createItem{
				tx:          tx,
				blockHeight: blockHeight,
				options:     options,
				done:        done,
			})

			select {
			case res := <-done:
				return res.data, res.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	if err := s.ensureTxBodyPartition(ctx, blockHeight); err != nil {
		return nil, err
	}

	return s.createIn(ctx, s.pool, tx, blockHeight, opts...)
}

// appendCreate adds one transaction to the plan: its identity row, its serialized bytes, and
// one coin row per spendable output.
//
// Shared by the single and the batched path so the two cannot drift apart on what they store.
// Nothing is appended until every failure is behind us, so a transaction this rejects leaves
// no half-written row in the arrays.
func (s *Store) appendCreate(p *createPlan, item int, tx *bt.Tx, blockHeight uint32,
	options *utxo.CreateOptions) (*meta.Data, error) {
	if options == nil {
		options = &utxo.CreateOptions{}
	}

	txHash := tx.TxIDChainHash()
	leaf := LeafFor(txHash[:])
	isCoinbase := tx.IsCoinbase()

	// Coinbase maturity and the ReAssignUTXO delay fold into one precomputed height, so the
	// spend hot path never branches on "is this a coinbase".
	//
	// Zero for an ordinary output, NOT the creation height. No consensus rule stops a normal
	// output being spent below the height it was created at, and encoding one would reject
	// valid spends during a reorg or whenever a caller passes a height that is not strictly
	// increasing.
	var spendableFrom int32
	if isCoinbase {
		spendableFrom = int32(blockHeight) + int32(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	// The caller's state options MUST reach the row. spend.go checks all three flags when
	// deciding whether a spend may proceed, so dropping them here does not fail loudly, it
	// creates an ordinary spendable output and lets every downstream guard pass quietly on a
	// zero bit.
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

	// The inputs, stored rather than re-derived. Block assembly rebuilds a mining candidate
	// from the fee, the size and these, never from the serialized transaction, which is what
	// lets the body age out of its window while the transaction stays mineable.
	var inpoints []byte

	if !isCoinbase {
		ip, ierr := subtree.NewTxInpointsFromTx(tx)
		if ierr != nil {
			return nil, errors.NewProcessingError("[utxoset][Create] inpoints %s", txHash.String(), ierr)
		}

		if inpoints, ierr = ip.Serialize(); ierr != nil {
			return nil, errors.NewProcessingError("[utxoset][Create] serialise inpoints %s", txHash.String(), ierr)
		}
	}

	genesisHeight := s.settings.ChainCfgParams.GenesisActivationHeight

	// The identity row. k is this transaction's position in the statement, and the result
	// comes back keyed on it, so a caller learns whether its OWN claim took rather than
	// whether the batch as a whole wrote anything.
	p.idx = append(p.idx, int32(len(p.owner))) //nolint:gosec // bounded by batch size
	p.owner = append(p.owner, item)
	p.txs = append(p.txs, tx)
	p.leaves = append(p.leaves, leaf)
	p.txids = append(p.txids, txHash[:])
	p.heights = append(p.heights, int32(blockHeight))
	p.offChain = append(p.offChain, offChainSinceAt(options.MinedBlockInfos, blockHeight))
	p.membership = append(p.membership, packMembership(options.MinedBlockInfos))
	p.sizes = append(p.sizes, int32(tx.Size()))
	p.inpoints = append(p.inpoints, inpoints)
	p.locktimes = append(p.locktimes, int32(tx.LockTime))
	p.createdAt = append(p.createdAt, time.Now().UnixMilli())
	p.txFlags = append(p.txFlags, flags)
	p.bodies = append(p.bodies, tx.Bytes())

	for vout, out := range tx.Outputs {
		if out == nil {
			continue
		}

		if out.LockingScript != nil && !utxo.ShouldStoreOutputAsUTXO(out, blockHeight, genesisHeight) {
			continue // provably unspendable: no coin row, ever
		}

		var script []byte
		if out.LockingScript != nil {
			script = *out.LockingScript
		}

		p.coinSats = append(p.coinSats, int64(out.Satoshis))
		p.coinHeights = append(p.coinHeights, int32(blockHeight))
		p.coinSpendable = append(p.coinSpendable, spendableFrom)
		p.coinLeaves = append(p.coinLeaves, leaf)
		p.coinFlags = append(p.coinFlags, flags)
		p.coinUkeys = append(p.coinUkeys, Pack(txHash[:], uint32(vout)))
		p.coinTxids = append(p.coinTxids, txHash[:])
		p.coinScripts = append(p.coinScripts, script)
	}

	return &meta.Data{
		Tx:          tx,
		Fee:         0,
		SizeInBytes: uint64(tx.Size()),
		IsCoinbase:  isCoinbase,
	}, nil
}

// createIn is Create against an arbitrary querier, so SpendAndCreate can run it inside the
// same database transaction as the spend.
//
// It is a plan of one. There is deliberately no second statement for the single-transaction
// case: what a create writes, and the claim that decides whether it writes at all, is this
// store's whole idempotence rule, and two copies of it is a defect waiting for one to be
// edited alone.
func (s *Store) createIn(ctx context.Context, q querier, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	plan := s.planCreates([]*createItem{{tx: tx, blockHeight: blockHeight, options: options}})
	if plan.errs[0] != nil {
		return nil, plan.errs[0]
	}

	if options.Conflicting {
		txHash := tx.TxIDChainHash()

		if cerr := s.noteConflictOnParents(ctx, q, tx, txHash[:]); cerr != nil {
			return nil, cerr
		}
	}

	if err := s.runCreatePlan(ctx, q, plan); err != nil {
		return nil, err
	}

	return plan.perItem[0], plan.errs[0]
}

// noteConflictOnParents tells every parent of a losing transaction that it is being contested.
func (s *Store) noteConflictOnParents(ctx context.Context, q querier, tx *bt.Tx, txid []byte) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, in := range tx.Inputs {
		if in == nil {
			continue
		}

		parent := in.PreviousTxIDChainHash()
		if parent == nil {
			continue
		}

		// One note per parent, however many of its outputs this transaction reaches for.
		if _, dup := seen[*parent]; dup {
			continue
		}

		seen[*parent] = struct{}{}

		if _, err := q.Exec(ctx, noteConflictSQL, LeafFor(parent[:]), parent[:], txid); err != nil {
			return errors.NewStorageError("[utxoset][Create] note conflict on %s", parent.String(), err)
		}
	}

	return nil
}
