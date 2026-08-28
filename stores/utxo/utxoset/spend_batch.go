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
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
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
	ignoreFlags utxo.IgnoreFlags
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
	// skipClaim[i] suppresses the previous-output comparison for item i. Set only by the
	// gated below-checkpoint outpoint-only path, which is the one caller entitled to it.
	skipClaim []bool
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

		skipClaim: make([]bool, len(items)),
	}

	for i, it := range items {
		p.itemTxs[i] = it.tx
		p.skipClaim[i] = it.ignoreFlags.SkipUTXOHashCheck

		if it.tx == nil || it.tx.IsCoinbase() {
			continue
		}

		spends := make([]*utxo.Spend, len(it.tx.Inputs))
		spendingTxID := it.tx.TxIDChainHash()

		// One backing array for the records and one for the spenders, rather than a fresh
		// allocation per input. This loop runs once per input of every transaction in every
		// block, so the whole set costs three allocations regardless of input count where the
		// obvious form would cost two more per input.
		records := make([]utxo.Spend, len(it.tx.Inputs))
		spenders := make([]spendpkg.SpendingData, len(it.tx.Inputs))

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

			// The spender belongs on the record from the moment the record exists. Conflict
			// resolution hands these same records straight back to this store's Unspend,
			// which restores on the spender and REFUSES a record that cannot name the
			// transaction that took the coin. Handing out records without one turned every
			// conflict-resolution failure into the manual-intervention escalation, whatever
			// had actually gone wrong, because the rollback itself could never succeed.
			//
			// The coin hash is deliberately left unset. Computing it is a double hash per
			// input, which is a real cost on this path, and nothing that reads these records
			// uses it: this store's Unspend restores on the outpoint and the spender.
			spenders[vin] = spendpkg.SpendingData{TxID: spendingTxID, Vin: vin}
			records[vin] = utxo.Spend{
				TxID:         parent,
				Vout:         in.PreviousTxOutIndex,
				SpendingData: &spenders[vin],
			}
			spends[vin] = &records[vin]
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

// claimMismatch compares what a spending transaction CLAIMED about the coin against what the
// store has just handed back for it, and returns the rejection when they differ.
//
// This is the control the other two stores get from the UTXO hash, and the reason that hash
// has content. A transaction may be submitted in EXTENDED FORMAT, meaning it carries its own
// copy of every coin it spends -- the coin's value in satoshis and its locking script, the
// rules for who may move it. The validator deliberately does not re-derive those when they
// arrive; it validates against whatever the transaction brought. So the no-inflation check
// sums the submitter's satoshis and script verification runs against the submitter's script.
// The hash the other two stores compare at the spend is computed from those carried copies,
// which is what makes it an authentication of the SUBMITTER rather than a consistency check on
// the store.
//
// This store needs no hash to do the same job. Its DELETE returns the coin's real satoshis and
// script in the same round trip, because the spend is also the decorate fetch, so it holds the
// truth at exactly the moment the other two are comparing digests. Comparing the values
// directly is cheaper -- a byte comparison instead of a double hash per input, on the hottest
// path in the store -- and strictly stronger, because it compares the values themselves rather
// than a digest of them.
//
// A nil PreviousTxScript means the input carried no claim at all, which is the un-decorated
// below-checkpoint outpoint-only path. There is nothing to authenticate, and that path
// switches script validation off in the same breath, so nothing acts on a claim either.
//
// The error is deliberately the one the other two stores already raise for this condition.
// Their needsSpendRollback reads it as a genuine invalidity rather than a transient failure,
// and conflict resolution reads it as "this transaction is invalid" rather than "this
// transaction lost a race" -- so a false claim is rejected outright instead of being kept as a
// conflicting transaction whose descendants get walked.
//
// Rejection is all-or-nothing on every path a caller can reach in production: SpendAndCreate
// runs the spend inside one database transaction and rolls it back on any per-input error, and
// SpendAndCreate is the only entry point the validator, block validation and conflict
// resolution use. The batched Spend path has no enclosing transaction, so a rejected
// transaction there leaves the rows the DELETE already took. That is a pre-existing property
// of that path for every error class, not something this comparison introduces, and no caller
// outside tests reaches it.
func claimMismatch(in *bt.Input, sp *utxo.Spend, satoshis int64, script []byte) error {
	if in == nil || in.PreviousTxScript == nil {
		return nil
	}

	if in.PreviousTxSatoshis == uint64(satoshis) && //nolint:gosec // satoshis are never negative
		bytes.Equal(*in.PreviousTxScript, script) {
		return nil
	}

	// The scripts are reported by length rather than by content: one of them is attacker
	// supplied and unbounded, and this string reaches the logs.
	return errors.NewUtxoHashMismatchError(
		"[utxoset][Spend] %s:%d was offered as %d satoshis with a %d-byte script, the store holds %d satoshis with a %d-byte script",
		sp.TxID, sp.Vout, in.PreviousTxSatoshis, len(*in.PreviousTxScript), satoshis, len(script))
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
			// Before the overwrite, and it has to be: the overwrite is what destroys the
			// claim this is checking.
			if !p.skipClaim[item] {
				if err := claimMismatch(in, p.perItem[item][vin], satoshis, script); err != nil {
					p.perItem[item][vin].Err = err
				}
			}

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

			// The replay decorates from the journal rather than from the coin row, which
			// makes this the SECOND place the store hands a caller's claim back
			// unexamined. It gets the same comparison as the first, against the payload
			// the journal captured at the moment of the delete.
			in := p.itemTxs[p.owner[k]].Inputs[p.ownerVin[k]]

			sp.Err = nil
			sp.ConflictingTxID = nil

			if !p.skipClaim[p.owner[k]] {
				sp.Err = claimMismatch(in, sp, satoshis, script)
			}

			if in != nil {
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
