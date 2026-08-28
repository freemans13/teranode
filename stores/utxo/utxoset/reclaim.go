package utxoset

import (
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

// settledSQL answers "which of these transactions can never be un-mined" for a batch.
//
// Two clauses, and both are load-bearing. The marker must be NULL, meaning a main-chain block
// contains it; a transaction whose only block lost is still waiting and its parent's coins
// may still have to come back. And its DEEPEST block must be at or below the cutoff. Deepest,
// not first: a transaction can name a block that lost and the block that actually mined it,
// and taking the convenient one would call it settled while the real one is still shallow.
// The deepest is safe in the only direction that matters, because it can delay reclaim but
// never rush it.
//
// The candidate list arrives as an array parameter rather than as a query built over the
// journal partition. That is deliberate: a data-modifying expression over the partition
// carries no size estimate, and the planner then throws away the per-key probes and reads
// both the identity table and the coin table whole. Measured at 174,186 page fetches for a
// 7,917 row chunk.
const settledSQL = `
SELECT i.txid
  FROM unnest($1::bytea[]) AS k(txid)
  JOIN tx_ident i ON i.leaf = (get_byte(k.txid, 0) & 7)::smallint AND i.txid = k.txid
 WHERE i.off_chain_since IS NULL
   AND mh_max(i.membership) IS NOT NULL
   AND mh_max(i.membership) <= $2`

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
const candidatesSQL = `SELECT DISTINCT txid, spending_txid FROM %s`

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
func (s *Store) reclaimFromPartition(ctx context.Context, partition string, tip uint32) (int, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(candidatesSQL, partition))
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][reclaim] candidates from %s", partition, err)
	}

	var (
		parents  [][]byte
		spenders [][]byte
		spentBy  = map[string][][]byte{}
	)

	for rows.Next() {
		var parent, spender []byte
		if err := rows.Scan(&parent, &spender); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("[utxoset][reclaim] scan %s", partition, err)
		}

		if _, seen := spentBy[string(parent)]; !seen {
			parents = append(parents, parent)
		}

		spentBy[string(parent)] = append(spentBy[string(parent)], spender)
		spenders = append(spenders, spender)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset][reclaim] rows %s", partition, err)
	}

	if len(parents) == 0 {
		return 0, nil
	}

	settledSpenders, err := s.settled(ctx, spenders, tip)
	if err != nil {
		return 0, err
	}

	live, err := s.withLiveCoins(ctx, parents)
	if err != nil {
		return 0, err
	}

	// The parent must itself be on the main chain. It does NOT separately need to be buried,
	// because a transaction cannot be mined before the one it spends, so a settled spender
	// implies at least that much depth on its parent.
	onChain, err := s.onMainChain(ctx, parents)
	if err != nil {
		return 0, err
	}

	var doomed [][]byte

	for _, parent := range parents {
		key := string(parent)

		if _, hasCoin := live[key]; hasCoin {
			continue
		}

		if _, ok := onChain[key]; !ok {
			continue
		}

		allSpendsSettled := true

		for _, spender := range spentBy[key] {
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
