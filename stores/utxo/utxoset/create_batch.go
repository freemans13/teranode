package utxoset

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// createPlanSQL stores a whole BATCH of transactions in ONE statement.
//
// Every parameter is an array, so one statement serves one transaction or a thousand, exactly
// as spendJournalSQL does on the other side. That is what lets the single-transaction path be
// a plan of one rather than a second copy of these predicates living somewhere else.
//
// The three writes a create makes used to be three separate statements on the pool. That cost
// three commits each and was not atomic. A failure between them left an identity row with no
// serialized bytes, which is indistinguishable from a transaction whose bytes have aged out
// of their window, so it read as normal forever. Worse, the retry was refused, because the
// identity claim reported the transaction as already present, so the bytes were never
// written.
//
// Folding them into one statement fixes both at once. Data-modifying common table expressions
// run in a single snapshot, so either all three land or none do, and both the body and the
// coin inserts are gated on the claim having actually inserted that transaction's row. A
// transaction the store already holds therefore writes nothing at all and is absent from the
// result.
//
// The gate is a JOIN against claim rather than an EXISTS, and that is the whole difference
// between this and the per-transaction statement it replaces. EXISTS asks "did anything in
// this statement claim", which is the right question only when the statement carries one
// transaction. The join asks it per transaction, so a batch mixing fresh transactions with
// ones the store already holds writes bodies and coins for exactly the fresh ones.
//
// claim RETURNING gives back leaf and txid rather than the caller's index k, because
// PostgreSQL only lets INSERT ... RETURNING name columns of the target table. The join back
// onto (leaf, txid) recovers k, and it is exact rather than approximate: txid is the full 32
// bytes, and (leaf, txid) is tx_ident's primary key.
//
// The coin insert has no conflict clause and needs none. The coin key is a non-unique 96-bit
// prefix by design and has nothing to conflict on, so idempotence here comes from the claim
// gate. Without that gate a re-applied block would create every output a second time, which
// is the failure this mechanism exists to prevent.
//
// fee is written as NULL deliberately. The store does not compute it, and block assembly
// rebuilds a mining candidate from size and inpoints instead.
//
// births is gated on the claim like body and coins, so a duplicate offer writes nothing, and
// it fires only for a transaction that wrote no coin row. See the tx_birth DDL comment for
// why such a transaction needs a work list of its own.
const createPlanSQL = `
WITH t AS (
    SELECT * FROM unnest($1::int[], $2::smallint[], $3::bytea[], $4::int[], $5::int[],
                         $6::bytea[], $7::int[], $8::bytea[], $9::int[], $10::bigint[],
                         $11::smallint[], $12::bytea[], $21::boolean[])
        AS t(k, leaf, txid, created_height, off_chain_since, membership, size_in_bytes,
             tx_inpoints, locktime, created_at, flags, raw_tx, no_coins)
),
claim AS (
    INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since, membership,
                          fee, size_in_bytes, tx_inpoints, locktime, created_at, flags)
    SELECT t.leaf, t.txid, t.created_height, t.off_chain_since, t.membership,
           NULL::bigint, t.size_in_bytes, t.tx_inpoints, t.locktime, t.created_at, t.flags
      FROM t
    ON CONFLICT (leaf, txid) DO NOTHING
    RETURNING leaf, txid
),
body AS (
    INSERT INTO tx_body (created_height, txid, raw_tx)
    SELECT t.created_height, t.txid, t.raw_tx
      FROM t
      JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid
),
births AS (
    INSERT INTO tx_birth (created_height, txid)
    SELECT t.created_height, t.txid
      FROM t
      JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid
     WHERE t.no_coins
),
coins AS (
    INSERT INTO utxo (satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
    SELECT o.satoshis, o.created_height, o.spendable_from, o.leaf, o.flags,
           o.ukey, o.txid, o.script
      FROM unnest($13::bigint[], $14::int[], $15::int[], $16::smallint[], $17::smallint[],
                  $18::uuid[], $19::bytea[], $20::bytea[])
        AS o(satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
      JOIN claim c ON c.leaf = o.leaf AND c.txid = o.txid
)
SELECT t.k
  FROM t
  JOIN claim c ON c.leaf = t.leaf AND c.txid = t.txid`

// createResult is what one queued Create gets back.
type createResult struct {
	data *meta.Data
	err  error
}

// createItem is a single Create waiting for its batch to flush.
type createItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	done        chan createResult
}

// createPlan is the argument set for one call of the create statement, however many
// transactions went into it, plus the mapping needed to give each caller its own answer.
//
// Building it in one place is what stops the batched and unbatched paths carrying separate
// copies of the same statement. The single-transaction path is a plan of one.
//
// Two array widths live here. The identity fields carry one element per transaction. The coin
// fields carry one element per SPENDABLE output, flattened across every transaction in the
// batch, and they are tied back to their transaction by the leaf and txid they already carry
// rather than by a separate mapping.
type createPlan struct {
	// One element per transaction that made it into the statement.
	idx        []int32
	leaves     []int16
	txids      [][]byte
	heights    []int32
	offChain   []*int32
	membership [][]byte
	sizes      []int32
	inpoints   [][]byte
	locktimes  []int32
	createdAt  []int64
	txFlags    []int16
	bodies     [][]byte
	// noCoins is true for a transaction that contributed no coin rows, which is what puts
	// it in the birth ledger.
	noCoins []bool

	// One element per spendable output, across the whole batch.
	coinSats      []int64
	coinHeights   []int32
	coinSpendable []int32
	coinLeaves    []int16
	coinFlags     []int16
	coinUkeys     [][16]byte
	coinTxids     [][]byte
	coinScripts   [][]byte

	owner   []int        // plan row -> which item in the batch
	txs     []*bt.Tx     // plan row -> its transaction, for error messages
	perItem []*meta.Data // batch item -> the record its caller gets back
	errs    []error      // batch item -> its own error, if it has one
}

// planCreates flattens a batch of transactions into one set of arrays.
func (s *Store) planCreates(items []*createItem) *createPlan {
	p := &createPlan{
		perItem: make([]*meta.Data, len(items)),
		errs:    make([]error, len(items)),
	}

	// One claim per txid per statement, deduplicated HERE rather than left to the database.
	//
	// This is not tidiness. ON CONFLICT DO NOTHING tolerates a repeated key within a single
	// command, but it returns the winning row ONCE, so a second plan row for the same
	// transaction would join that one claim and insert its body a second time. tx_body's
	// primary key then raises a unique violation and takes the whole batch down with it.
	//
	// Reporting the repeat as already-held is also what the caller would have seen had the
	// two offers landed in separate batches, so batch composition does not change the answer.
	seen := make(map[chainhash.Hash]struct{}, len(items))

	for i, it := range items {
		if it.tx == nil {
			p.errs[i] = errors.NewProcessingError("[utxoset][Create] nil tx")
			continue
		}

		txHash := it.tx.TxIDChainHash()

		if _, dup := seen[*txHash]; dup {
			p.errs[i] = errors.NewTxExistsError("[utxoset][Create] %s", txHash.String())
			continue
		}

		data, err := s.appendCreate(p, i, it.tx, it.blockHeight, it.options)
		if err != nil {
			p.errs[i] = err
			continue
		}

		seen[*txHash] = struct{}{}
		p.perItem[i] = data
	}

	p.sortRows()

	return p
}

// sortRows puts the identity rows in one global order, by leaf and txid, for the same reason
// spendPlan.sortRows does: two batches claiming the same transactions in opposite orders would
// wait on each other's speculative inserts and deadlock, and in one order they cannot. The coin
// rows are left as built; nothing about them is unique, so nothing about them can wait.
func (p *createPlan) sortRows() {
	n := len(p.owner)
	if n < 2 {
		return
	}

	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool {
		x, y := order[a], order[b]
		if p.leaves[x] != p.leaves[y] {
			return p.leaves[x] < p.leaves[y]
		}

		return bytes.Compare(p.txids[x], p.txids[y]) < 0
	})

	p.leaves = permute(p.leaves, order)
	p.txids = permute(p.txids, order)
	p.heights = permute(p.heights, order)
	p.noCoins = permute(p.noCoins, order)
	p.offChain = permute(p.offChain, order)
	p.membership = permute(p.membership, order)
	p.sizes = permute(p.sizes, order)
	p.inpoints = permute(p.inpoints, order)
	p.locktimes = permute(p.locktimes, order)
	p.createdAt = permute(p.createdAt, order)
	p.txFlags = permute(p.txFlags, order)
	p.bodies = permute(p.bodies, order)
	p.owner = permute(p.owner, order)
	p.txs = permute(p.txs, order)

	for k := range p.idx {
		p.idx[k] = int32(k) //nolint:gosec // bounded by batch size
	}
}

// runCreatePlan issues the statement and tells each caller whether its own claim took.
func (s *Store) runCreatePlan(ctx context.Context, q querier, p *createPlan) error {
	if len(p.owner) == 0 {
		return nil
	}

	rows, err := q.Query(ctx, createPlanSQL,
		p.idx, p.leaves, p.txids, p.heights, p.offChain, p.membership, p.sizes,
		p.inpoints, p.locktimes, p.createdAt, p.txFlags, p.bodies,
		p.coinSats, p.coinHeights, p.coinSpendable, p.coinLeaves, p.coinFlags,
		p.coinUkeys, p.coinTxids, p.coinScripts, p.noCoins)
	if err != nil {
		return errors.NewStorageError("[utxoset][Create] store", err)
	}

	claimed := make(map[int32]struct{}, len(p.owner))

	for rows.Next() {
		var k int32

		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset][Create] scan", err)
		}

		claimed[k] = struct{}{}
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][Create] rows", err)
	}

	if len(claimed) == len(p.owner) {
		return nil
	}

	// A transaction the claim did not insert is one the store already holds. The statement
	// wrote nothing at all for it, because its body and its coins were gated on that same
	// claim, so there is nothing to undo.
	for k := range p.owner {
		if _, ok := claimed[int32(k)]; ok { //nolint:gosec // bounded by batch size
			continue
		}

		item := p.owner[k]
		p.perItem[item] = nil
		p.errs[item] = errors.NewTxExistsError("[utxoset][Create] %s", p.txs[k].TxIDChainHash().String())
	}

	return nil
}

// newCreateBatcher wires the create path through the shared batcher, exactly as the sql and
// aerospike stores do.
//
// background is FALSE, which differs from the sql store's create batcher, and the reason is
// specific to this store rather than a disagreement about deadlocks.
//
// The sql store's create callback issues statements and nothing else, so two batches running
// at once cannot interfere. This one may also run DDL: a create at a height whose body window
// does not exist yet has to make the window first, and a batch can span several. Concurrent
// batches doing DDL against the same parent table, coordinated through a one-entry cache, is
// far more machinery than the win justifies.
//
// It costs nothing that matters here. The batching win is turning N round trips into one, and
// that is unaffected. What background dispatch adds on top is overlapping one batch's database
// work with the next batch's, and block application is a single writer, so there is no second
// batch to overlap with.
//
// It also makes Close honest. The batcher guarantees only that queued items have been HANDED
// TO the callback; with background dispatch the work was still landing afterwards, which
// showed up as one test's batch inserting into tables a later test had already replaced.
func newCreateBatcher(s *Store, size int, duration time.Duration) *batcher.Batcher[createItem] {
	return batcher.NewWithPool(size, duration, s.sendCreateBatch, false)
}

// sendCreateBatch flushes a batch of Creates as one statement.
func (s *Store) sendCreateBatch(batch []*createItem) {
	// Marks this batch as in flight so Close waits for the database work, not merely for the
	// hand-off. See createInFlight on Store.
	s.createInFlight.Add(1)
	defer s.createInFlight.Done()

	ctx := context.Background()

	// Planned outside any connection: this is processor work, and holding one while doing it
	// would be the nested-acquire hazard in a different disguise.
	plan := s.planCreates(batch)

	// The body window has to exist before the statement runs, for the same reason the spend
	// journal's does: the DDL needs its own connection, and taking one while holding a
	// transaction from the same pool deadlocks with no timeout once writers reach the pool
	// limit. A batch can span windows, so every distinct one is prepared.
	seen := make(map[int32]struct{}, 4)

	for _, h := range plan.heights {
		win := h / TxBodyPartitionBlocks
		if _, dup := seen[win]; dup {
			continue
		}

		seen[win] = struct{}{}

		if err := s.ensureTxBodyPartition(ctx, uint32(h)); err != nil { //nolint:gosec // height is non-negative
			s.failBatch(batch, err)
			return
		}
	}

	if err := s.runCreatePlan(ctx, s.pool, plan); err != nil {
		s.failBatch(batch, err)
		return
	}

	for i, item := range batch {
		item.done <- createResult{data: plan.perItem[i], err: plan.errs[i]}
	}
}

// failBatch reports one error to every waiter, so a batch-level failure never leaves a
// caller blocked on a channel nobody will write to.
func (s *Store) failBatch(items []*createItem, err error) {
	for _, item := range items {
		item.done <- createResult{err: err}
	}
}
