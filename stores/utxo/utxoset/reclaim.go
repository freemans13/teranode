package utxoset

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
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
// The third column is whether EVERY journal row for that (parent, spender) pair carries the
// applied mark. A pair's rows are written by one statement, so in practice they agree, and
// bool_and is the conservative reading if they ever did not. The GROUP BY replaces the
// DISTINCT at the same cost.
const candidatesSQL = `SELECT txid, spending_txid, bool_and(applied) FROM %s GROUP BY txid, spending_txid ORDER BY txid`

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

// unmarkedRecentSpendsSQL asks whether ANY spend recorded above the settled depth lacks the
// applied mark. Below the checkpoint every spend is block-applied and marked, so this is false
// for the whole of initial sync and the recent-spend probe below never runs. The moment a
// mempool spend is journaled it is true, and stays true, because the tip is where the probe
// is needed. Partition pruning on spent_height bounds it to the newest few partitions, and
// the block-range index on the mark (see ensureSpendJournalPartition) answers the no-match
// case from a few kilobytes of page summaries instead of a sequential read. The cast matches
// the index expression; NOT applied would not.
const unmarkedRecentSpendsSQL = `
SELECT EXISTS (SELECT 1 FROM spend_journal WHERE spent_height > $1 AND (applied::int) = 0)`

// recentSpendsSQL returns the subset of parents that have a spend recorded above the settled
// depth, in any attached partition. Such a spend's spender cannot be buried yet, so the parent
// is not finished whatever the retiring partition says about it.
//
// The shape is hasLiveCoinSQL's: a lateral probe of the journal's packed-key index per
// partition with ORDER BY and LIMIT 1 OFFSET 0 as the fence, and spent_height in the predicate
// so only the partitions above the depth are visited.
const recentSpendsSQL = `
SELECT k.txid
  FROM unnest($1::bytea[], $2::uuid[], $3::uuid[]) AS k(txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT 1
     FROM spend_journal j
    WHERE j.spent_height > $4
      AND j.ukey >= k.lo
      AND j.ukey <= k.hi
      AND j.txid  = k.txid
    ORDER BY j.ukey
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
//
// A parent is judged from the spenders the retiring partition names, and that alone is not
// enough at the tip. A later spend of the same parent sits in a newer partition the reclaimer
// has not read, and the live-coin check stops guarding the parent the moment that later spend
// took the last coin. So a parent whose first output was spent long ago by a settled child
// and whose last output was spent recently by a mempool child would be deleted with that child
// unmined; when the child is mined, block validation looks the parent up, finds nothing, and
// in the RUNNING state stores a valid block as invalid. Below the checkpoint there is no
// mempool and every spend is settled by construction, so the check is skipped there, decided
// once per retiring partition by unmarkedRecentSpendsSQL. Above it, every doomed parent is
// first probed for a spend above the settled depth in any attached partition.
// TestReclaimKeepsAParentWhoseLaterSpendIsStillUnsettled pins this in both reclaim paths.
//
// The whole partition is JUDGED before anything is DELETED, and the delete is one database
// transaction. Both halves are load-bearing. The settled check asks whether each spender's
// identity row is on the main chain and buried, so it needs the spender's row to still exist.
// Inside one partition most parents are also spenders (89 percent on a measured mainnet
// partition), and the work list is ordered by transaction id, which is a hash, so about half
// the time a spender sorts into an earlier chunk than its parent. Deleting per chunk removed
// the spender's row before its parent was judged; the parent was refused, and because this
// partition held the parent's last spend, nothing ever named it again. Measured on mainnet on
// 3 September 2026: 48 percent of parents deleted per chunk, and about 83 percent of the
// 196 million identity rows were residue no work list could reach. One transaction for the
// delete matters for the same reason: a crash between chunked deletes would leave some
// spenders gone when the next session re-judged the partition, reopening the leak once per
// crash. TestReclaimDoesNotRefuseAParentWhoseSpenderSortsIntoAnEarlierChunk pins this.
func (s *Store) reclaimFromPartition(ctx context.Context, partition string, tip uint32) (int, int, error) {
	limit := s.reclaimChunkParents
	if limit <= 0 {
		limit = DefaultReclaimChunkParents
	}

	guardRecent, err := s.hasUnmarkedRecentSpends(ctx, tip)
	if err != nil {
		return 0, 0, err
	}

	rows, err := s.pool.Query(ctx, fmt.Sprintf(candidatesSQL, partition))
	if err != nil {
		return 0, 0, errors.NewStorageError("[utxoset][reclaim] candidates from %s", partition, err)
	}

	var (
		doomed     [][]byte
		chunks     int
		batch      = newReclaimBatch()
		lastParent []byte
		scanErr    error
	)

	for rows.Next() {
		var (
			parent, spender []byte
			applied         bool
		)

		if err := rows.Scan(&parent, &spender, &applied); err != nil {
			scanErr = errors.NewStorageError("[utxoset][reclaim] scan %s", partition, err)
			break
		}

		// Cut only when this row starts a NEW parent. The statement orders by parent, so
		// every row for one parent arrives together, and cutting mid-parent would judge it
		// on a subset of its spenders.
		if len(batch.parents) >= limit && !bytes.Equal(parent, lastParent) {
			d, cerr := s.judgeBatch(ctx, batch, tip, guardRecent)
			if cerr != nil {
				scanErr = cerr
				break
			}

			doomed = append(doomed, d...)
			chunks++

			batch.reset()
		}

		batch.add(parent, spender, applied)
		lastParent = parent
	}

	rows.Close()

	if scanErr != nil {
		return 0, chunks, scanErr
	}

	if err := rows.Err(); err != nil {
		return 0, chunks, errors.NewStorageError("[utxoset][reclaim] rows %s", partition, err)
	}

	if len(batch.parents) > 0 {
		d, err := s.judgeBatch(ctx, batch, tip, guardRecent)
		if err != nil {
			return 0, chunks, err
		}

		doomed = append(doomed, d...)
		chunks++
	}

	reclaimed, err := s.deleteIdents(ctx, doomed, limit)
	if err != nil {
		return 0, chunks, err
	}

	return reclaimed, chunks, nil
}

// deleteIdents removes the identity rows of every doomed parent in ONE transaction.
//
// The statement still takes the parents in bounded arrays, because a single array of several
// hundred thousand ids is a needlessly large parameter, but every array runs inside the same
// transaction, so either the whole partition's verdict lands or none of it does. A crash
// mid-way rolls all of it back and the next session judges the partition again with every
// spender row still in place.
func (s *Store) deleteIdents(ctx context.Context, doomed [][]byte, chunk int) (int, error) {
	if len(doomed) == 0 {
		return 0, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][reclaim] begin delete", err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	reclaimed := 0

	for lo := 0; lo < len(doomed); lo += chunk {
		hi := lo + chunk
		if hi > len(doomed) {
			hi = len(doomed)
		}

		tag, err := tx.Exec(ctx, deleteIdentSQL, doomed[lo:hi])
		if err != nil {
			return 0, errors.NewStorageError("[utxoset][reclaim] delete", err)
		}

		reclaimed += int(tag.RowsAffected())
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, errors.NewStorageError("[utxoset][reclaim] commit delete", err)
	}

	return reclaimed, nil
}

// judgeBatch is one bounded slice of a partition's work list: the decision, and only the
// decision. It returns the parents that may go; reclaimFromPartition deletes them once the
// whole partition has been judged.
//
// Splitting this out is what makes the memory bound possible: the probes run on at most
// reclaimChunkParents parents at a time. What is carried between chunks is only the list of
// doomed transaction ids, about 32 bytes each, so the fattest partition measured (366,000
// rows) holds under 20 MB.
//
// The live-coin check runs first and on every parent, because it is the one rule no mark
// can answer: a parent with an unspent output is needed by whoever eventually spends it. A
// coin-free parent whose EVERY spend carries the applied mark is then finished without
// another question. The mark says each spend was recorded by the block path below the
// hardcoded checkpoint, where a block cannot be un-mined by rule, so the spender is in a
// main-chain block that will never be taken back and the parent, mined at or below it on
// the same chain, is too. Those parents skip the settled probe (which would have visited
// each spender's identity row) and the on-chain probe (which would have visited the
// parent's). Only parents with an unmarked spend, which at the tip is every mempool spend,
// take the two probes, and only their spenders are asked about.
//
// The mark also closes a leak the settled probe cannot see past. That probe is an inner join
// on the identity table, so a spender with no row is simply absent and the parent reads as
// unsettled. A spender can lack a row legitimately: it was never stored because it has no
// spendable outputs, or its own row was already reclaimed. Below the checkpoint the mark
// answers for it. At the tip the fail-safe stays, because an unmarked spend by a spender
// with no row proves nothing about depth.
func (s *Store) judgeBatch(ctx context.Context, b *reclaimBatch, tip uint32, guardRecent bool) ([][]byte, error) {
	if len(b.parents) == 0 {
		return nil, nil
	}

	doomed, err := s.judgeBatchLocal(ctx, b, tip)
	if err != nil || !guardRecent || len(doomed) == 0 {
		return doomed, err
	}

	recent, err := s.withRecentSpends(ctx, doomed, tip)
	if err != nil {
		return nil, err
	}

	if len(recent) == 0 {
		return doomed, nil
	}

	kept := make([][]byte, 0, len(doomed))

	for _, parent := range doomed {
		if _, ok := recent[string(parent)]; !ok {
			kept = append(kept, parent)
		}
	}

	return kept, nil
}

// judgeBatchLocal decides a batch on the retiring partition's own evidence: live coins, the
// applied mark, and the settled and on-chain probes for unmarked parents.
func (s *Store) judgeBatchLocal(ctx context.Context, b *reclaimBatch, tip uint32) ([][]byte, error) {

	live, err := s.withLiveCoins(ctx, b.parents)
	if err != nil {
		return nil, err
	}

	var (
		doomed   [][]byte
		unmarked [][]byte
		spenders [][]byte
		seen     = map[string]struct{}{}
	)

	for _, parent := range b.parents {
		key := string(parent)

		if _, hasCoin := live[key]; hasCoin {
			continue
		}

		if b.allApplied[key] {
			doomed = append(doomed, parent)
			continue
		}

		unmarked = append(unmarked, parent)

		for _, spender := range b.spentBy[key] {
			sk := string(spender)
			if _, dup := seen[sk]; dup {
				continue
			}

			seen[sk] = struct{}{}
			spenders = append(spenders, spender)
		}
	}

	if len(unmarked) == 0 {
		return doomed, nil
	}

	settledSpenders, err := s.settled(ctx, spenders, tip)
	if err != nil {
		return nil, err
	}

	// The parent must itself be on the main chain. It does NOT separately need to be buried,
	// because a transaction cannot be mined before the one it spends, so a settled spender
	// implies at least that much depth on its parent.
	onChain, err := s.onMainChain(ctx, unmarked)
	if err != nil {
		return nil, err
	}

	for _, parent := range unmarked {
		key := string(parent)

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

	return doomed, nil
}

// hasUnmarkedRecentSpends reports whether any spend above the settled depth lacks the applied
// mark, which is the condition under which the recent-spend probe is needed at all.
func (s *Store) hasUnmarkedRecentSpends(ctx context.Context, tip uint32) (bool, error) {
	if tip < SettledDepthBlocks {
		return true, nil
	}

	var any bool
	if err := s.pool.QueryRow(ctx, unmarkedRecentSpendsSQL, int64(tip-SettledDepthBlocks)).Scan(&any); err != nil {
		return false, errors.NewStorageError("[utxoset][reclaim] unmarked recent spends", err)
	}

	return any, nil
}

// withRecentSpends returns the subset of parents with a spend recorded above the settled depth.
func (s *Store) withRecentSpends(ctx context.Context, parents [][]byte, tip uint32) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(parents))

	if len(parents) == 0 || tip < SettledDepthBlocks {
		return out, nil
	}

	_, ids, los, his := liveCoinArgs(parents)

	rows, err := s.pool.Query(ctx, recentSpendsSQL, ids, los, his, int64(tip-SettledDepthBlocks))
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][reclaim] recent spends", err)
	}

	defer rows.Close()

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			return nil, errors.NewStorageError("[utxoset][reclaim] recent spends scan", err)
		}

		out[string(txid)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][reclaim] recent spends rows", err)
	}

	return out, nil
}

// reclaimBatch holds one bounded slice of a partition's work list.
//
// Spenders are deduplicated at judgement time, per parent that still needs them, rather than
// here. The work list arrives as one row per (parent, spender) pair, so a transaction that
// consumed several parents appears once per parent; on a measured mainnet leaf that was
// 207,500 entries for 103,492 distinct spenders. Only the spenders of parents with an
// unmarked spend are ever asked about, so the set is built once those parents are known.
type reclaimBatch struct {
	parents [][]byte
	spentBy map[string][][]byte

	// allApplied[parent] is true while every (parent, spender) pair seen so far carried the
	// applied mark. One unmarked pair makes it false for good.
	allApplied map[string]bool
}

func newReclaimBatch() *reclaimBatch {
	return &reclaimBatch{
		spentBy:    map[string][][]byte{},
		allApplied: map[string]bool{},
	}
}

func (b *reclaimBatch) add(parent, spender []byte, applied bool) {
	key := string(parent)

	if _, seen := b.spentBy[key]; !seen {
		b.parents = append(b.parents, parent)
		b.allApplied[key] = true
	}

	// EVERY spender goes on the per-parent list, deduplicated or not. That list decides whether
	// all of a parent's spends have settled, and dropping a repeat from it would be harmless
	// only by luck.
	b.spentBy[key] = append(b.spentBy[key], spender)

	if !applied {
		b.allApplied[key] = false
	}
}

// reset drops the batch's contents without keeping the backing arrays.
//
// Reusing them would defeat the point: the bound exists so the reclaimer's footprint does not
// track the largest leaf it has ever seen.
func (b *reclaimBatch) reset() {
	b.parents = nil
	b.spentBy = map[string][][]byte{}
	b.allApplied = map[string]bool{}
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
