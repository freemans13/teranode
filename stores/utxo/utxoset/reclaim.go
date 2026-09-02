package utxoset

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
)

// SettledDepthBlocks is how deep a transaction's block must be before the store treats it as
// permanent.
//
// 288, and this store did not pick the number. It is global_blockHeightRetention, the point
// at which the subtree files a reorg would need are deleted, after which the un-mine path
// warns and skips a missing file. So past 288 the node physically cannot take a block back.
// 100 is coinbase maturity, which refuses deep honest reorgs during catchup but is not a
// ceiling, because block invalidation has no depth bound beyond refusing genesis and is
// reachable over the network. 144 is the unmined retention, which is when parent preservation
// starts firing and not a maturity bound at all.
const SettledDepthBlocks = 288

// DefaultReclaimChunkParents bounds how many parent transactions one reclaim pass holds.
//
// 20,000 parents is roughly ten megabytes of transaction ids and map overhead, against a leaf
// that can hold over a million spend records at fat-band rates. Small enough that the spike is
// irrelevant next to the 5 GiB heap ceiling, large enough that every query still gets the wide
// array parameter this store is fastest at.
const DefaultReclaimChunkParents = 20_000

// settledSQL answers "which of these transactions can never be un-mined" for a batch.
//
// Three clauses, and all three are load-bearing. The marker must be NULL, meaning a main-chain
// block contains it; a transaction whose only block lost is still waiting and its parent's
// coins may still have to come back. Its membership must name at least one block. And its
// DEEPEST block must be at or below the cutoff. Deepest, not first: a transaction can name a
// block that lost and the block that actually mined it, and taking the convenient one would
// call it settled while the real one is still shallow. The deepest is safe in the only
// direction that matters, because it can delay reclaim but never rush it.
//
// The candidate list arrives as an array parameter rather than as a query built over the
// journal partition. That is deliberate: a data-modifying expression over the partition
// carries no size estimate, and the planner then throws away the per-key probes and reads
// both the identity table and the coin table whole. Measured at 174,186 page fetches for a
// 7,917 row chunk.
//
// It asks the deepest-block question WITHOUT calling mh_max, and that is the whole cost of this
// statement rather than a tidy-up. Measured on the mainnet box, mh_max costs about 52 microseconds per
// call against an 8.4 microsecond index probe underneath it, and this one statement is roughly
// three quarters of a retiring leaf's time with 93% of that inside the helper. PostgreSQL
// cannot inline a SQL function whose body is an aggregate over a set-returning function, and
// this one is both, so every row paid the full call.
//
// "Is the deepest block at or below the cutoff" and "is no block above the cutoff" are the same
// question, and the second needs no maximum. Written as NOT EXISTS the planner stops at the
// first triple that disqualifies a row instead of reducing over all of them, and there is no
// function call left to pay.
//
// The length guard is NOT the same as "membership IS NOT NULL", and picking the wrong one
// settles transactions that must never be settled. mh_max returns NULL for BOTH spellings of
// "no block": a NULL membership, and the empty value that unstampSQL leaves behind when it
// removes the last triple with overlay (see set_mined.go). NOT EXISTS over an empty membership
// is TRUE, so a bare NULL test would flip that empty residue from refused to settled and delete
// the identity row of a transaction no block contains. octet_length refuses both, because NULL
// compares to nothing and 0 is below 12.
//
// The casts MUST be bigint, for the reason mh_max's own comment gives: in PostgreSQL
// 255::int << 24 wraps to a negative number, silently, so an int4 version would read a high
// height as negative and pass every cutoff test.
const settledSQL = `
SELECT i.txid
  FROM unnest($1::bytea[]) AS k(txid)
  JOIN tx_ident i ON i.leaf = (get_byte(k.txid, 0) & 7)::smallint AND i.txid = k.txid
 WHERE i.off_chain_since IS NULL
   AND octet_length(i.membership) >= 12
   AND NOT EXISTS (
       SELECT 1
         FROM generate_series(0, octet_length(i.membership) / 12 - 1) g
        WHERE ( (get_byte(i.membership, g * 12 + 4)::bigint << 24)
              | (get_byte(i.membership, g * 12 + 5)::bigint << 16)
              | (get_byte(i.membership, g * 12 + 6)::bigint <<  8)
              |  get_byte(i.membership, g * 12 + 7)::bigint ) > $2)`

// settled returns the subset of txids that can never be un-mined at this tip, keyed by the
// raw txid bytes as a string so callers can test membership cheaply.
func (s *Store) settled(ctx context.Context, txids [][]byte, tip uint32) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(txids))

	if len(txids) == 0 {
		return out, nil
	}

	if tip < SettledDepthBlocks {
		// Nothing can be deep enough yet, and an underflow here would settle everything.
		return out, nil
	}

	cutoff := int64(tip - SettledDepthBlocks)

	rows, err := s.pool.Query(ctx, settledSQL, txids, cutoff)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][settled]", err)
	}

	defer rows.Close()

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			return nil, errors.NewStorageError("[utxoset][settled] scan", err)
		}

		out[string(txid)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][settled] rows", err)
	}

	return out, nil
}

// candidatesSQL takes the distinct parents named by one retiring journal partition.
//
// The partition IS the work list, and it was written for free: every spend already recorded
// which parent it consumed, and the rows are grouped by height, so the set of transactions
// that had an output spent in a given window is already on disk and arrives on a schedule.
// Nothing else in the store can produce that list without a counter on every spend or a scan
// that races.
// The ORDER BY is load-bearing rather than cosmetic. The reclaimer reads this in bounded
// batches and may only cut between parents, so every row for one parent has to arrive
// together. DISTINCT already forces a sort or hash aggregate over the whole partition, so
// asking for the order costs close to nothing on top.
//
// The slice predicate is what lets one pruner run do a fraction of a partition instead of all
// of it. A partition covers 48 block heights, so one run does one of those heights, and over the
// 48 runs leading up to the partition's due date every height is done exactly once. The block
// height being cleaned says which one, so nothing has to be written down.
//
// spent_height is not indexed on a journal partition, so this is a sequential read. That is
// fine and it is not the cost: reading a whole partition takes about 28 ms, while the DISTINCT
// over 182,000 rows spills 13 MB to disk and takes tens of seconds. One height's worth is a few
// thousand rows and sorts in memory.
// One limitation, stated because it is real rather than because it blocks this. A parent whose
// outputs were taken in DIFFERENT blocks of the same partition is seen by more than one height,
// and each of those runs judges it on the spenders it can see. Measured at 16.2% of parents on a
// real mainnet partition. That is the same shape as the store's existing known gap, where a
// parent is judged from one partition while a later attached partition holds the spend that took
// its last coin, and it wants the same fix: a guard that asks whether any undo record for that
// parent survives anywhere. It is not made better here and it is worth sizing on its own.
const candidatesSQL = `SELECT DISTINCT txid, spending_txid FROM %s
 WHERE spent_height = $1 ORDER BY txid`

const candidatesAllSQL = `SELECT DISTINCT txid, spending_txid FROM %s ORDER BY txid`

// hasLiveCoinSQL asks whether any of a transaction's outputs is still unspent. A parent with
// a live coin is needed by whoever eventually spends it, however settled its other spends are.
//
// The packed-key range bound is mandatory, not an optimisation. The coin table carries exactly
// one index, on the packed key, and the schema says in its own words that any query filtering
// on the transaction id without a packed-key range bound is a review failure. This statement
// was that failure: it read all eight partitions whole, built a hash over the entire live
// unspent set and probed it with a few hundred keys. The pruner runs it once per retiring
// journal partition, so its cost grew with the unspent set and with how far behind the pruner
// was, at the same time, and that compounds.
//
// The bound alone does not fix it, which is the part worth knowing. Written as a plain EXISTS
// with the range added, the planner still chooses the hash join over every partition and
// demotes the range to a filter it applies after reading everything. The lateral join with
// LIMIT 1 is what stops it flattening the subquery, and OFFSET 0 holds the fence if a future
// planner learns to pull up a limited lateral. Measured on the real schema at ten million
// rows, the same shape of change took five hundred keys from 1,883 ms to 4.8 ms.
//
// TestWithLiveCoinsDoesNotScanTheCoinTable asserts the plan. That test is load-bearing: the
// fence is long-established behaviour rather than a documented guarantee, so without it a
// future PostgreSQL could quietly go back to reading the table whole and nothing else here
// would notice.
//
// The range can admit a collision but can never exclude a genuine match, because the packed
// key is built from the first twelve bytes of the same transaction id. Identity is still
// settled by the full 32-byte comparison on the row.
// The ORDER BY is what picks the plan, and it is worth more than it looks. Without it the
// planner chooses a bitmap heap scan, which materialises every matching entry in the packed-key
// range before the LIMIT 1 can stop it, so the cost of asking grows with how many outputs the
// parent still has rather than stopping at the first. Asking for packed-key order gives the
// planner a reason to walk the index directly, and a plain index scan can stop after one row.
//
// Measured on the mainnet box against 20,000 parents, twice, on two different journal
// partitions: 435 ms and 520 ms as a bitmap scan, against 207 ms and 240 ms as an index scan.
// Per probe that is about 19 microseconds down to about 9. It changes no answer, because a
// LIMIT 1 over a set asks only whether the set is non-empty and the order within it is
// irrelevant.
//
// Keep the LIMIT and the OFFSET 0 as well. They are a different fence, stopping the planner
// flattening the lateral back into the outer join, and removing either has previously put this
// statement back to reading every partition whole.
const hasLiveCoinSQL = `
SELECT k.txid
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS k(leaf, txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT 1
     FROM utxo u
    WHERE u.leaf  = k.leaf
      AND u.ukey >= k.lo
      AND u.ukey <= k.hi
      AND u.txid  = k.txid
    ORDER BY u.ukey
    LIMIT 1
   OFFSET 0
 ) AS hit`

// deleteIdentSQL removes finished identity rows.
//
// This table cannot be reclaimed by dropping a window, because a row dies when its
// transaction's last output is spent and that spend settles, which happens at any age. So it
// needs row-level deletion forever, and that is the store's one remaining unsolved cost.
const deleteIdentSQL = `
DELETE FROM tx_ident i
 USING unnest($1::bytea[]) AS k(txid)
 WHERE i.leaf = (get_byte(k.txid, 0) & 7)::smallint AND i.txid = k.txid`

// A negative atHeight means no height predicate at all: read the whole partition. That is what
// an overdue partition gets, because it has no window left to spread the work across.
//
// reclaimFromPartition uses one retiring journal partition as a work list and deletes the
// identity rows that are genuinely finished.
//
// The ordering constraint is absolute: this must run BEFORE the partition is dropped,
// because dropping it destroys the record of which transactions to examine.
//
// A candidate is deleted only when all three hold. It holds no live coin. It is itself on the
// main chain and buried past the point the node could un-mine it. And every transaction that
// spent it is likewise buried, so no reorg can ask for its coins back.
//
// A candidate that fails any of those is FORGOTTEN rather than queued, and that is the whole
// trick. The partition holding a transaction's LAST spend always names it, and by the time
// that partition retires, every earlier spend of it sits in a partition that already retired.
// So a rejected candidate comes back on its own, at exactly the right moment, with no state
// carried between sessions.
func (s *Store) reclaimFromPartition(ctx context.Context, partition string, tip uint32, atHeight int64) (int, int, error) {
	limit := s.reclaimChunkParents
	if limit <= 0 {
		limit = DefaultReclaimChunkParents
	}

	var (
		rows pgx.Rows
		err  error
	)

	if atHeight < 0 {
		rows, err = s.pool.Query(ctx, fmt.Sprintf(candidatesAllSQL, partition))
	} else {
		rows, err = s.pool.Query(ctx, fmt.Sprintf(candidatesSQL, partition), atHeight)
	}

	if err != nil {
		return 0, 0, errors.NewStorageError("[utxoset][reclaim] candidates from %s at height %d", partition, atHeight, err)
	}

	var (
		reclaimed  int
		chunks     int
		batch      = newReclaimBatch()
		lastParent []byte
		scanErr    error
	)

	for rows.Next() {
		var parent, spender []byte
		if err := rows.Scan(&parent, &spender); err != nil {
			scanErr = errors.NewStorageError("[utxoset][reclaim] scan %s", partition, err)
			break
		}

		// Cut only when this row starts a NEW parent. The statement orders by parent, so
		// every row for one parent arrives together, and cutting mid-parent would judge it
		// on a subset of its spenders.
		if len(batch.parents) >= limit && !bytes.Equal(parent, lastParent) {
			n, cerr := s.reclaimBatch(ctx, batch, tip)
			if cerr != nil {
				scanErr = cerr
				break
			}

			reclaimed += n
			chunks++

			batch.reset()
		}

		batch.add(parent, spender)
		lastParent = parent
	}

	rows.Close()

	if scanErr != nil {
		return reclaimed, chunks, scanErr
	}

	if err := rows.Err(); err != nil {
		return reclaimed, chunks, errors.NewStorageError("[utxoset][reclaim] rows %s", partition, err)
	}

	if len(batch.parents) > 0 {
		n, err := s.reclaimBatch(ctx, batch, tip)
		if err != nil {
			return reclaimed, chunks, err
		}

		reclaimed += n
		chunks++
	}

	return reclaimed, chunks, nil
}

// reclaimBatch is one bounded slice of a partition's work list: the decision, and the delete.
//
// Splitting this out is what makes the memory bound possible. It is also the unit a failure
// stops at: an earlier batch's delete has already committed, which is safe because every batch
// is decided from ground truth read at that moment and nothing is carried between them.
func (s *Store) reclaimBatch(ctx context.Context, b *reclaimBatch, tip uint32) (int, error) {
	if len(b.parents) == 0 {
		return 0, nil
	}

	settledSpenders, err := s.settled(ctx, b.spenders, tip)
	if err != nil {
		return 0, err
	}

	live, err := s.withLiveCoins(ctx, b.parents)
	if err != nil {
		return 0, err
	}

	// The parent must itself be on the main chain. It does NOT separately need to be buried,
	// because a transaction cannot be mined before the one it spends, so a settled spender
	// implies at least that much depth on its parent.
	onChain, err := s.onMainChain(ctx, b.parents)
	if err != nil {
		return 0, err
	}

	var doomed [][]byte

	for _, parent := range b.parents {
		key := string(parent)

		if _, hasCoin := live[key]; hasCoin {
			continue
		}

		if _, ok := onChain[key]; !ok {
			continue
		}

		allSpendsSettled := true

		for _, spender := range b.spentBy[key] {
			if _, ok := settledSpenders[string(spender)]; !ok {
				allSpendsSettled = false
				break
			}
		}

		if allSpendsSettled {
			doomed = append(doomed, parent)
		}
	}

	if len(doomed) == 0 {
		return 0, nil
	}

	tag, err := s.pool.Exec(ctx, deleteIdentSQL, doomed)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][reclaim] delete", err)
	}

	return int(tag.RowsAffected()), nil
}

// reclaimBatch holds one bounded slice of a partition's work list.
type reclaimBatch struct {
	parents  [][]byte
	spenders [][]byte
	spentBy  map[string][][]byte

	// seenSpender deduplicates the array handed to the settled check.
	//
	// The work list arrives as one row per (parent, spender) pair, so a transaction that
	// consumed several parents appeared once per parent and was probed once per appearance.
	// The parents beside it were already deduplicated and the spenders were not. On a measured
	// mainnet leaf that was 207,500 entries for 103,492 distinct spenders.
	//
	// The saving is real but smaller than the leaf-wide ratio suggests, and the difference is
	// worth knowing before anyone quotes it. Deduplication can only happen INSIDE a batch of
	// reclaimChunkParents, and a spender's other parents scatter across the whole leaf because
	// the cut is on parent order, so the measured within-batch factor is about 1.40 against
	// 2.01 leaf-wide.
	seenSpender map[string]struct{}
}

func newReclaimBatch() *reclaimBatch {
	return &reclaimBatch{
		spentBy:     map[string][][]byte{},
		seenSpender: map[string]struct{}{},
	}
}

func (b *reclaimBatch) add(parent, spender []byte) {
	key := string(parent)

	if _, seen := b.spentBy[key]; !seen {
		b.parents = append(b.parents, parent)
	}

	// EVERY spender goes on the per-parent list, deduplicated or not. That list decides whether
	// all of a parent's spends have settled, and dropping a repeat from it would be harmless
	// only by luck; the query array below is the only place a repeat costs anything.
	b.spentBy[key] = append(b.spentBy[key], spender)

	sk := string(spender)
	if _, seen := b.seenSpender[sk]; seen {
		return
	}

	b.seenSpender[sk] = struct{}{}
	b.spenders = append(b.spenders, spender)
}

// reset drops the batch's contents without keeping the backing arrays.
//
// Reusing them would defeat the point: the bound exists so the reclaimer's footprint does not
// track the largest leaf it has ever seen.
func (b *reclaimBatch) reset() {
	b.parents = nil
	b.spenders = nil
	b.spentBy = map[string][][]byte{}
	b.seenSpender = map[string]struct{}{}
}

// liveCoinArgs expands transaction ids into the four parallel arrays hasLiveCoinSQL takes:
// the partition key, the identity, and the packed-key range covering every output the
// transaction could have created.
//
// It is a named function rather than four lines inline so the plan test can build exactly the
// arguments the production path builds. A test that explained a hand-written variant would be
// pinning the plan of a statement nothing runs.
func liveCoinArgs(txids [][]byte) (leaves []int16, ids [][]byte, los, his [][16]byte) {
	leaves = make([]int16, 0, len(txids))
	ids = make([][]byte, 0, len(txids))
	los = make([][16]byte, 0, len(txids))
	his = make([][16]byte, 0, len(txids))

	for _, id := range txids {
		leaves = append(leaves, LeafFor(id))
		ids = append(ids, id)
		los = append(los, Pack(id, 0))
		his = append(his, Pack(id, ^uint32(0)))
	}

	return leaves, ids, los, his
}

// withLiveCoins returns the subset of txids that still have at least one unspent output.
func (s *Store) withLiveCoins(ctx context.Context, txids [][]byte) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(txids))

	if len(txids) == 0 {
		return out, nil
	}

	leaves, ids, los, his := liveCoinArgs(txids)

	rows, err := s.pool.Query(ctx, hasLiveCoinSQL, leaves, ids, los, his)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][reclaim] live coins", err)
	}

	defer rows.Close()

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			return nil, errors.NewStorageError("[utxoset][reclaim] live coin scan", err)
		}

		out[string(txid)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][reclaim] live coin rows", err)
	}

	return out, nil
}

// onMainChainSQL asks which of these transactions a main-chain block contains, which is
// exactly the marker being absent.
const onMainChainSQL = `
SELECT i.txid
  FROM unnest($1::bytea[]) AS k(txid)
  JOIN tx_ident i ON i.leaf = (get_byte(k.txid, 0) & 7)::smallint AND i.txid = k.txid
 WHERE i.off_chain_since IS NULL`

// onMainChain returns the subset of txids that a main-chain block contains.
func (s *Store) onMainChain(ctx context.Context, txids [][]byte) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(txids))

	rows, err := s.pool.Query(ctx, onMainChainSQL, txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][reclaim] on main chain", err)
	}

	defer rows.Close()

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			return nil, errors.NewStorageError("[utxoset][reclaim] on main chain scan", err)
		}

		out[string(txid)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][reclaim] on main chain rows", err)
	}

	return out, nil
}
