package utxoset

import (
	"bytes"
	"context"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// spendResult is what one queued Spend gets back.
type spendResult struct {
	spends []*utxo.Spend
	err    error
}

// spendItem is a single Spend waiting for its batch to flush.
type spendItem struct {
	tx          *bt.Tx
	blockHeight uint32
	done        chan spendResult
}

// newSpendBatcher wires the spend path through the shared batcher.
//
// background is FALSE, matching the sql store's spend batcher and for its stated reason:
// batch callbacks have to be serialised or two database transactions can lock overlapping
// rows in different orders and deadlock. That applies here in full. Two blocks can spend
// coins of the same parent transaction, so unlike the create path, batches genuinely do
// overlap.
func newSpendBatcher(s *Store, size int, duration time.Duration) *batcher.Batcher[spendItem] {
	return batcher.NewWithPool(size, duration, s.sendSpendBatch, false)
}

// spendPlan is the argument set for one call of the spend statement, however many
// transactions went into it, plus the mapping needed to give each caller its own answers.
//
// Building it in one place is what stops the batched and unbatched paths carrying separate
// copies of the same predicates. The single-transaction path is a batch of one.
type spendPlan struct {
	leaves   []int16
	ukeys    [][16]byte
	txids    [][]byte
	idx      []int32
	heights  []int32
	spenders [][]byte
	owner    []int // global index -> which item in the batch
	ownerVin []int // global index -> which input of that item
	perItem  [][]*utxo.Spend
	itemTxs  []*bt.Tx
}

// planSpends flattens a batch of transactions into one set of arrays.
func planSpends(items []*spendItem) *spendPlan {
	total := 0
	for _, it := range items {
		if it.tx != nil && !it.tx.IsCoinbase() {
			total += len(it.tx.Inputs)
		}
	}

	p := &spendPlan{
		leaves:   make([]int16, 0, total),
		ukeys:    make([][16]byte, 0, total),
		txids:    make([][]byte, 0, total),
		idx:      make([]int32, 0, total),
		heights:  make([]int32, 0, total),
		spenders: make([][]byte, 0, total),
		owner:    make([]int, 0, total),
		ownerVin: make([]int, 0, total),
		perItem:  make([][]*utxo.Spend, len(items)),
		itemTxs:  make([]*bt.Tx, len(items)),
	}

	for i, it := range items {
		p.itemTxs[i] = it.tx

		if it.tx == nil || it.tx.IsCoinbase() {
			continue
		}

		spends := make([]*utxo.Spend, len(it.tx.Inputs))
		spendingTxID := it.tx.TxIDChainHash()

		for vin, in := range it.tx.Inputs {
			parent := in.PreviousTxIDChainHash()

			p.leaves = append(p.leaves, LeafFor(parent[:]))
			p.ukeys = append(p.ukeys, Pack(parent[:], in.PreviousTxOutIndex))
			p.txids = append(p.txids, parent[:])
			p.idx = append(p.idx, int32(len(p.owner))) //nolint:gosec // bounded by batch size
			p.heights = append(p.heights, int32(it.blockHeight))
			p.spenders = append(p.spenders, spendingTxID[:])
			p.owner = append(p.owner, i)
			p.ownerVin = append(p.ownerVin, vin)

			spends[vin] = &utxo.Spend{TxID: parent, Vout: in.PreviousTxOutIndex}
		}

		p.perItem[i] = spends
	}

	return p
}

// sendSpendBatch flushes a batch of Spends as one statement.
func (s *Store) sendSpendBatch(batch []*spendItem) {
	s.spendInFlight.Add(1)
	defer s.spendInFlight.Done()

	ctx := context.Background()

	// The journal partitions must exist before the statement runs, and the DDL needs its own
	// connection, so it happens here rather than inside anything holding one. A batch can
	// span heights, so every distinct partition is prepared.
	seen := make(map[uint32]struct{}, 4)

	for _, it := range batch {
		h := it.blockHeight / SpendJournalPartitionBlocks
		if _, dup := seen[h]; dup {
			continue
		}

		seen[h] = struct{}{}

		if err := s.ensureSpendJournalPartition(ctx, it.blockHeight); err != nil {
			for _, item := range batch {
				item.done <- spendResult{err: err}
			}

			return
		}
	}

	plan := planSpends(batch)

	if err := s.runSpendPlan(ctx, s.pool, plan); err != nil {
		for _, item := range batch {
			item.done <- spendResult{err: err}
		}

		return
	}

	for i, item := range batch {
		item.done <- spendResult{spends: plan.perItem[i]}
	}
}

// runSpendPlan issues the statement and fills in each caller's answers, including the
// per-input errors that conflict detection reads.
func (s *Store) runSpendPlan(ctx context.Context, q querier, p *spendPlan) error {
	if len(p.owner) == 0 {
		return nil
	}

	rows, err := q.Query(ctx, spendJournalSQL, p.leaves, p.ukeys, p.txids, p.idx,
		p.heights, p.spenders)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] delete", err)
	}

	done := make(map[int32]struct{}, len(p.owner))

	for rows.Next() {
		var (
			k        int32
			satoshis int64
			script   []byte
		)

		if err := rows.Scan(&k, &satoshis, &script); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Spend] scan", err)
		}

		done[k] = struct{}{}

		// The decorate fetch, free: the input now carries what script validation needs, so
		// nothing has to read the parent transaction for it.
		item := p.owner[k]
		vin := p.ownerVin[k]

		if in := p.itemTxs[item].Inputs[vin]; in != nil {
			in.PreviousTxSatoshis = uint64(satoshis) //nolint:gosec // satoshis are never negative
			in.PreviousTxScript = bscript.NewFromBytes(script)
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] rows", err)
	}

	if len(done) == len(p.owner) {
		return nil
	}

	return s.classifyPlanMisses(ctx, q, p, done)
}

// classifyPlanMisses turns "the DELETE did not take this row" into a specific error, per
// input, for every caller in the batch.
//
// The distinction cannot be skipped: absence alone cannot tell "already spent" from "never
// existed" from "exists but is not yet spendable", and the validator behaves differently for
// each.
func (s *Store) classifyPlanMisses(ctx context.Context, q querier, p *spendPlan, done map[int32]struct{}) error {
	rows, err := q.Query(ctx, classifySQL, p.leaves, p.ukeys, p.txids, p.idx)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] classify", err)
	}

	present := make(map[int32]struct{})

	for rows.Next() {
		var (
			k             int32
			flags         int16
			spendableFrom int32
		)

		if err := rows.Scan(&k, &flags, &spendableFrom); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Spend] classify scan", err)
		}

		present[k] = struct{}{}

		sp := p.perItem[p.owner[k]][p.ownerVin[k]]

		switch {
		case flags&FlagFrozen != 0:
			sp.Err = errors.ErrFrozen
		case flags&FlagConflicting != 0:
			sp.Err = errors.ErrTxConflicting
		case spendableFrom > p.heights[k]:
			// Exists, but immature: a coinbase before maturity, or a reassigned output still
			// inside its delay. NOT a double spend, and reporting it as one would be wrong.
			sp.Err = errors.NewProcessingError("[utxoset] utxo not spendable until height %d (current %d)",
				spendableFrom, p.heights[k])
		}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] classify rows", err)
	}

	// Anything neither deleted nor present is genuinely gone: already spent, or it never
	// existed. The coin table cannot tell those apart, which is why ErrSpent is the honest
	// answer, and why the journal is asked next for who took it.
	missing := false

	for _, k := range p.idx {
		if _, ok := done[k]; ok {
			continue
		}

		if _, ok := present[k]; !ok {
			p.perItem[p.owner[k]][p.ownerVin[k]].Err = errors.ErrSpent
			missing = true
		}
	}

	if !missing {
		return nil
	}

	return s.namePlanSpenders(ctx, q, p)
}

// namePlanSpenders reads the journal for every coin that is no longer there, and does two
// different jobs with the answer.
//
// If the transaction the journal names is the one now spending, this is a REPLAY of our own
// earlier work rather than a competing spend, and it must succeed. Delete-on-spend destroys
// the coin row, so a block interrupted part-way through application leaves its coins already
// gone; re-offering that block asks the store to take them again. Calling that a double spend
// is not merely unhelpful, it is fatal: the block can never be applied, the tip never
// advances, and no restart helps. Mainnet wedged at height 97389 exactly this way, with all
// 200 inputs of one transaction reported as already spent by that same transaction.
//
// The replay still has to decorate the input, because the spend is also the decorate fetch and
// script validation has no other source for the satoshis and the locking script. The journal
// row captured both at the moment of the delete, so it can serve them unchanged.
//
// If the journal names a DIFFERENT transaction, this is a real double spend and the caller
// needs to know who won, so it can mark the loser conflicting and walk its descendants.
//
// Bounded by the journal's retention. Beyond that the store genuinely cannot say, and a replay
// that old cannot be recognised either, which is a stated limit of delete-on-spend rather than
// a gap to paper over.
func (s *Store) namePlanSpenders(ctx context.Context, q querier, p *spendPlan) error {
	rows, err := q.Query(ctx, spenderSQL, p.leaves, p.ukeys, p.txids, p.idx)
	if err != nil {
		return errors.NewStorageError("[utxoset][Spend] find spender", err)
	}

	defer rows.Close()

	// A coin can appear in the journal more than once, having been spent, restored by an
	// unspend, and spent again. One matching row is enough to make this a replay, so once an
	// input is settled that way no later row may unsettle it.
	replayed := make(map[int32]struct{})

	for rows.Next() {
		var (
			k        int32
			spender  []byte
			satoshis int64
			script   []byte
		)

		if err := rows.Scan(&k, &spender, &satoshis, &script); err != nil {
			return errors.NewStorageError("[utxoset][Spend] spender scan", err)
		}

		sp := p.perItem[p.owner[k]][p.ownerVin[k]]
		if sp == nil {
			continue
		}

		if _, done := replayed[k]; done {
			continue
		}

		if bytes.Equal(spender, p.spenders[k]) {
			replayed[k] = struct{}{}

			sp.Err = nil
			sp.ConflictingTxID = nil

			if in := p.itemTxs[p.owner[k]].Inputs[p.ownerVin[k]]; in != nil {
				in.PreviousTxSatoshis = uint64(satoshis) //nolint:gosec // satoshis are never negative
				in.PreviousTxScript = bscript.NewFromBytes(script)
			}

			continue
		}

		if !errors.Is(sp.Err, errors.ErrSpent) {
			continue
		}

		h, herr := chainhash.NewHash(spender)
		if herr != nil {
			return errors.NewStorageError("[utxoset][Spend] spender hash", herr)
		}

		sp.ConflictingTxID = h
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Spend] spender rows", err)
	}

	return nil
}
