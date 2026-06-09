package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
)

// bucketHeights is the generation width: every bucketHeights block heights the
// store rolls over to a new bucket, i.e. bucket = blockHeight / bucketHeights.
// Each (hash partition × bucket) pair is a concrete leaf table
// (txs_pNN_bBBBB / spends_pNN_bBBBB). Row reclaim is still the row-deleting
// pruner; once it has drained an aged generation, the EMPTY leaves are
// detached+dropped by dropEmptyAgedBucketLeaves (called from Prune) so the
// attached-leaf count stays bounded.
// INCREMENT 2: replace row-DELETE reclaim entirely with DETACH PARTITION +
// DROP TABLE of bucket leaves whose entire height range has aged past
// retention (with survivor relocation for stragglers).
const bucketHeights = 64

// bucketFor returns the generation bucket for a block height.
func bucketFor(height uint32) int32 {
	return int32(height / bucketHeights)
}

// bucketDDLLockClass is the pg_advisory_xact_lock classid used to serialise
// bucket-leaf DDL across processes (multiple pods share one database; two
// concurrent CREATE TABLE IF NOT EXISTS ... PARTITION OF of the same leaf can
// otherwise fail with a duplicate-key error on the catalogs).
const bucketDDLLockClass = int32(0x7e8a0001)

// bucketManager tracks which generation buckets have had their leaf partitions
// created, so the hot create path pays only a sync.Map hit per item once a
// bucket's leaves exist.
type bucketManager struct {
	created sync.Map   // int32 (bucket) -> struct{}
	mu      sync.Mutex // serialises in-process DDL on cache miss

	// lastReclaimCutoff is the cutoff bucket of the last completed
	// dropEmptyAgedBucketLeaves pass. The cutoff only advances once every
	// bucketHeights block heights, so gating on it makes the per-Prune-call
	// reclaim check a single atomic load instead of numPartitions catalog
	// queries (the pruner can run in a tight back-to-back loop under load).
	lastReclaimCutoff atomic.Int64
}

// EnsureBucket creates the txs/spends leaf partitions for the given bucket if
// they do not exist yet (idempotent), and caches the result. It is called
// lazily from the create path on a cheap cached check; for this increment we
// deliberately accept the rare leaf-creation DDL on the hot path rather than
// running a background goroutine (simplicity first).
// INCREMENT 2: move leaf creation off the hot path (pre-create the next bucket
// ahead of time from a background task). Aged-leaf cleanup of EMPTY leaves is
// handled by dropEmptyAgedBucketLeaves below.
func (s *Store) EnsureBucket(ctx context.Context, bucket int32) error {
	if _, ok := s.buckets.created.Load(bucket); ok {
		return nil
	}

	s.buckets.mu.Lock()
	defer s.buckets.mu.Unlock()

	// Double-check under the lock: a concurrent caller may have created it.
	if _, ok := s.buckets.created.Load(bucket); ok {
		return nil
	}

	if err := createBucketLeaves(ctx, s, bucket); err != nil {
		return err
	}

	s.buckets.created.Store(bucket, struct{}{})

	return nil
}

// createBucketLeaves creates one RANGE leaf per hash partition per table for
// the given bucket, with the same per-leaf fillfactor/autovacuum settings the
// single-level leaves carried before two-level partitioning.
//
// Locking shape matters: CREATE TABLE ... PARTITION OF takes ACCESS EXCLUSIVE
// on the mid-level parent (txs_pNN). Creating all 16 leaves inside ONE
// transaction would hold a growing set of AE locks while concurrent
// multi-partition INSERTs acquire partition locks in data-dependent order — a
// classic lock-order-inversion deadlock. So each leaf's DDL runs as its own
// implicit (autocommit) transaction — at most one AE lock held at a time, so
// no cycle — and cross-process catalog races on the SAME leaf are serialised
// by a per-bucket session advisory lock held on the dedicated connection for
// the duration (released via defer; dropped by the server if the conn dies).
func createBucketLeaves(ctx context.Context, s *Store, bucket int32) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return errors.NewStorageError("[EnsureBucket] acquire for bucket %d: %v", bucket, err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, bucketDDLLockClass, bucket); err != nil {
		return errors.NewStorageError("[EnsureBucket] advisory lock for bucket %d: %v", bucket, err)
	}
	defer func() {
		// Always release on the SAME connection before it goes back to the pool;
		// a failed unlock means the conn is broken and the server drops the lock
		// when the pool destroys it.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1, $2)`, bucketDDLLockClass, bucket)
	}()

	for _, spec := range leafSpecs {
		for p := 0; p < numPartitions; p++ {
			leaf := fmt.Sprintf("%s_p%02d_b%04d", spec.name, p, bucket)

			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s_p%02d FOR VALUES FROM (%d) TO (%d) WITH (fillfactor = %d)",
				leaf, spec.name, p, bucket, bucket+1, spec.fillfactor,
			)
			if _, err := conn.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("[EnsureBucket] leaf creation failed for %s: %v", leaf, err)
			}

			// Set on the leaf — autovacuum ignores parent-level reloptions.
			av := fmt.Sprintf("ALTER TABLE %s SET (%s)", leaf, spec.autovacuum)
			if _, err := conn.Exec(ctx, av); err != nil {
				return errors.NewStorageError("[EnsureBucket] autovacuum tuning failed for %s: %v", leaf, err)
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Aged-leaf reclamation (increment 2, part 1: EMPTY leaves only)
// ---------------------------------------------------------------------------

// leafBoundFromRe extracts the lower bucket bound from a leaf's partition-bound
// expression as printed by pg_get_expr(relpartbound, oid), e.g.
// "FOR VALUES FROM (12) TO (13)" (optional quotes tolerated for robustness).
// createBucketLeaves only ever creates single-bucket-wide leaves, so the upper
// bound is FROM+1; a DEFAULT partition or any unparseable bound simply does not
// match and is skipped.
var leafBoundFromRe = regexp.MustCompile(`FROM \('?(-?\d+)'?\)`)

// dropEmptyAgedBucketLeaves detaches and drops bucket leaf pairs
// (txs_pNN_bBBBB / spends_pNN_bBBBB) that are BOTH aged out and EMPTY, so the
// steady-state attached-leaf count per hash partition stays bounded at roughly
// the live bucket span plus small slack — instead of every hash-only statement
// (Get = ANY(hashes), SetLocked, SetMinedMulti, ...) fanning out across an
// ever-growing tail of empty leaves the row-deleting pruner has drained.
//
// Aged means the leaf's bucket range upper bound is below
// bucketFor(blockHeight - 2*retention) — the same retention the DAH machinery
// uses, doubled, so the dropped range is comfortably below anything still being
// written or read: creates always target bucketFor(current height), Unspend
// only re-inserts spend markers for parents that still have live txs rows, and
// a reorg rewind is bounded by retention. That is also why the
// emptiness-check-then-DETACH sequence is not racy: nothing writes rows into a
// bucket that far behind the tip.
//
// Non-empty aged leaves are skipped — the row-deleting pruner keeps draining
// them and they are retried on a later pass (increment 2 part 2: survivor
// relocation will handle long-lived stragglers such as preserved txs).
//
// Returns the number of leaf pairs dropped. Errors on individual pairs are
// collected (first error returned) but do not stop the sweep — reclamation is
// best-effort and retried on a later Prune call.
func (s *Store) dropEmptyAgedBucketLeaves(ctx context.Context, blockHeight uint32) (int, error) {
	retention := uint64(s.settings.GetUtxoStoreBlockHeightRetention())
	if retention == 0 {
		return 0, nil
	}
	horizon := 2 * retention
	if uint64(blockHeight) <= horizon {
		return 0, nil
	}
	cutoff := bucketFor(blockHeight - uint32(horizon))

	// The cutoff only advances every bucketHeights heights; between advances a
	// full pass would just re-discover the same skipped (non-empty) leaves, so
	// gate the catalog scan on cutoff movement. On error the gate is NOT
	// updated, so the next Prune call retries.
	if s.buckets.lastReclaimCutoff.Load() == int64(cutoff) {
		return 0, nil
	}

	dropped := 0
	var firstErr error
	for p := 0; p < numPartitions; p++ {
		agedBuckets, err := s.agedLeafBuckets(ctx, p, cutoff)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, b := range agedBuckets {
			ok, err := s.dropBucketLeafPairIfEmpty(ctx, p, b)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !ok {
				continue
			}
			dropped++
			// Keep the EnsureBucket cache coherent: if anything ever routes a
			// write back to this bucket (it should not — see ageing rationale
			// above), EnsureBucket must recreate the leaves via its idempotent
			// CREATE TABLE IF NOT EXISTS rather than trust a stale cache hit.
			s.buckets.created.Delete(b)
			s.logger.Infof("[pruner] dropped empty bucket leaves txs_p%02d_b%04d/spends_p%02d_b%04d", p, b, p, b)
		}
	}

	if firstErr == nil {
		s.buckets.lastReclaimCutoff.Store(int64(cutoff))
	}

	return dropped, firstErr
}

// agedLeafBuckets lists the buckets of the attached txs_pNN bucket leaves whose
// range upper bound is below cutoffBucket, by reading the partition bounds from
// the catalog (pg_inherits → pg_class.relpartbound). Reading the catalog rather
// than trusting the in-process cache makes the sweep correct across restarts
// and across multiple store instances sharing one database.
func (s *Store) agedLeafBuckets(ctx context.Context, partIdx int, cutoffBucket int32) ([]int32, error) {
	parent := fmt.Sprintf("txs_p%02d", partIdx)

	rows, err := s.pool.Query(ctx, `
		SELECT pg_get_expr(c.relpartbound, c.oid)
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = to_regclass($1)`, parent)
	if err != nil {
		return nil, errors.NewStorageError("[pruner] list bucket leaves of %s: %v", parent, err)
	}
	defer rows.Close()

	var aged []int32
	for rows.Next() {
		var bound string
		if err := rows.Scan(&bound); err != nil {
			return nil, errors.NewStorageError("[pruner] scan bucket leaf bound of %s: %v", parent, err)
		}
		m := leafBoundFromRe.FindStringSubmatch(bound)
		if m == nil {
			continue // DEFAULT partition or unexpected bound shape — never ours, skip
		}
		from, err := strconv.ParseInt(m[1], 10, 32)
		if err != nil {
			continue
		}
		// Leaves are one bucket wide (FROM b TO b+1), so upper bound = from+1.
		if int32(from)+1 < cutoffBucket {
			aged = append(aged, int32(from))
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[pruner] iterate bucket leaves of %s: %v", parent, err)
	}

	return aged, nil
}

// dropBucketLeafPairIfEmpty verifies that BOTH leaves of one (hash partition,
// bucket) pair are empty and, if so, detaches and drops them. Returns true only
// when the pair was dropped.
//
// Locking shape:
//
//   - The per-bucket session advisory lock (same discipline as
//     createBucketLeaves) serialises this DDL against a concurrent EnsureBucket
//     and against another pruner instance working the same bucket.
//
//   - Plain (non-CONCURRENT) DETACH PARTITION takes ACCESS EXCLUSIVE on the
//     mid-level parent (txs_pNN/spends_pNN) and on the leaf: it waits for
//     in-flight queries that touch the parent and briefly blocks new ones. The
//     leaves are empty and the detach is metadata-only, so the AE hold is
//     momentary; a lock_timeout caps the WAIT so a long-running query on the
//     parent can never stall the pruner behind a queued AE lock (which would
//     itself block all new queries on the parent). On timeout the pair is
//     skipped and retried later. INCREMENT 2: consider DETACH CONCURRENTLY
//     (SHARE UPDATE EXCLUSIVE, but cannot run inside a transaction, so the
//     orphan-on-crash window between DETACH and DROP returns).
//
//   - Both detach+drops run in ONE short transaction so a crash never leaves an
//     orphaned detached table, and both leaves go atomically (never a txs leaf
//     without its aligned spends leaf or vice versa). Inside the transaction
//     the txs parent is locked BEFORE the spends parent — the same order every
//     writer touches the tables (create: txs then spends; pruner cascade CTE:
//     txs scan then spends delete) — so no lock-order inversion.
func (s *Store) dropBucketLeafPairIfEmpty(ctx context.Context, partIdx int, bucket int32) (bool, error) {
	txsParent := fmt.Sprintf("txs_p%02d", partIdx)
	spendsParent := fmt.Sprintf("spends_p%02d", partIdx)
	txsLeaf := fmt.Sprintf("%s_b%04d", txsParent, bucket)
	spendsLeaf := fmt.Sprintf("%s_b%04d", spendsParent, bucket)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, errors.NewStorageError("[pruner] acquire for leaf drop %s: %v", txsLeaf, err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, bucketDDLLockClass, bucket); err != nil {
		return false, errors.NewStorageError("[pruner] advisory lock for bucket %d: %v", bucket, err)
	}
	defer func() {
		// Always release on the SAME connection before it goes back to the pool;
		// a failed unlock means the conn is broken and the server drops the lock
		// when the pool destroys it.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1, $2)`, bucketDDLLockClass, bucket)
	}()

	if _, err := conn.Exec(ctx, `SET lock_timeout = '5s'`); err != nil {
		return false, errors.NewStorageError("[pruner] set lock_timeout for %s: %v", txsLeaf, err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `RESET lock_timeout`)
	}()

	// Re-resolve existence under the advisory lock — another instance may have
	// dropped the pair between enumeration and here.
	var txsExists, spendsExists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL`,
		txsLeaf, spendsLeaf).Scan(&txsExists, &spendsExists); err != nil {
		return false, errors.NewStorageError("[pruner] resolve leaf pair %s: %v", txsLeaf, err)
	}
	if !txsExists && !spendsExists {
		return false, nil
	}

	// Verify BOTH leaves are empty before touching either. Safe as
	// check-then-act because nothing writes rows into a bucket this far behind
	// the tip (see dropEmptyAgedBucketLeaves).
	for leaf, exists := range map[string]bool{txsLeaf: txsExists, spendsLeaf: spendsExists} {
		if !exists {
			continue
		}
		var hasRows bool
		if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)`, leaf)).Scan(&hasRows); err != nil {
			return false, errors.NewStorageError("[pruner] emptiness check %s: %v", leaf, err)
		}
		if hasRows {
			return false, nil // leave it to the row-deleter; retried on a later pass
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, errors.NewStorageError("[pruner] begin leaf drop %s: %v", txsLeaf, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// txs before spends — writer lock order, see locking shape above.
	if txsExists {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s`, txsParent, txsLeaf)); err != nil {
			return false, errors.NewStorageError("[pruner] detach %s: %v", txsLeaf, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, txsLeaf)); err != nil {
			return false, errors.NewStorageError("[pruner] drop %s: %v", txsLeaf, err)
		}
	}
	if spendsExists {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s`, spendsParent, spendsLeaf)); err != nil {
			return false, errors.NewStorageError("[pruner] detach %s: %v", spendsLeaf, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, spendsLeaf)); err != nil {
			return false, errors.NewStorageError("[pruner] drop %s: %v", spendsLeaf, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, errors.NewStorageError("[pruner] commit leaf drop %s/%s: %v", txsLeaf, spendsLeaf, err)
	}

	return true, nil
}
