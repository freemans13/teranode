package utxoset

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// DefaultCoinIndexRebuildBytesPerRow is when a coin-table index is bloated enough to rebuild,
// measured in index bytes per live row.
//
// The coin index is the one structure in this store that vacuum cannot keep in shape, and the
// reason is worth writing down because it looks like neglect and is not. VACUUM frees a dead
// index entry for reuse, but it returns a btree leaf page to the free list only when that page
// becomes COMPLETELY empty. The key here is the packed key, derived from the transaction id, so
// it is effectively random and deletes scatter across every page in the index. Pages almost
// never empty completely, so they sit half-full for the life of the database and the file never
// shrinks. Nothing about autovacuum settings changes that. Only a rebuild does.
//
// The numbers from the mainnet soak box on 2026-09-02, at height 655,000. Before a rebuild the
// eight coin indexes held 5,553 MB at 22 to 24 percent leaf density, which is 119 bytes of index
// per live row. After, 1,372 MB at 90 to 92 percent, which is 30 bytes per live row. So 4.08 GB
// of a 24 GB buffer pool was being spent holding empty pages.
//
// 80 is between those two, closer to the bloated end so a rebuild only fires when it will
// actually recover something. The floor is set by the key: a 16-byte packed key plus a 6-byte
// heap pointer plus item overhead cannot come in under about 24 bytes even at a perfect fill,
// so 80 is a little over three times the best case and cannot be reached by a healthy index.
//
// Bytes per live row was chosen over leaf density on purpose. Density is the honest measure but
// reading it means pgstatindex(), which scans the whole index, so it is far too expensive to
// consult every block. Both numbers behind this ratio come from the catalog for free. It is also
// self-scaling: it says nothing about block sizes or transaction rates, so it needs no retuning
// as the chain gets denser, which a fixed "every N blocks" schedule would.
const DefaultCoinIndexRebuildBytesPerRow = 80

// DefaultCoinIndexMinRows is how many live rows a partition needs before its ratio means
// anything.
//
// Bytes per row is meaningless on a nearly empty index, where a page or two of fixed metadata
// divided by a handful of rows exceeds any threshold. A fresh node would rebuild its indexes on
// the first few blocks, over and over, recovering nothing.
//
// It is a store field rather than a bare constant because it decides whether the ratio is
// consulted at all, and a test that cannot lower it cannot reach the code it is testing. The
// first version of this was a constant at this value, and it silently made a test asserting
// "a healthy index is left alone" pass by skipping the check entirely.
const DefaultCoinIndexMinRows = 100_000

// coinIndexBloatSQL reports index bytes and live rows per coin partition, from the catalog only.
//
// n_live_tup is an estimate maintained by autovacuum rather than an exact count, which is fine
// at this threshold: the trigger is a three-fold difference, not a percentage point. The
// alternative, counting rows, would cost a full scan per partition per block.
const coinIndexBloatSQL = `
SELECT i.indexrelname,
       pg_relation_size(i.indexrelid),
       GREATEST(t.n_live_tup, 0)
  FROM pg_stat_user_indexes i
  JOIN pg_stat_user_tables t ON t.relid = i.relid
 WHERE i.indexrelname ~ '^utxo_p[0-9]+_ukey$'`

// invalidCoinIndexSQL finds indexes a previous rebuild left behind.
//
// A REINDEX CONCURRENTLY that fails part way leaves an invalid index in the catalog, and
// PostgreSQL will not retry over it. So this runs BEFORE any rebuild rather than in a recovery
// path nothing calls: the first thing the next session does is clear the wreckage of the last
// one. Without this a single failure stops every future rebuild silently, which is the failure
// mode that makes background maintenance untrustworthy.
const invalidCoinIndexSQL = `
SELECT c.relname
  FROM pg_index x
  JOIN pg_class c ON c.oid = x.indexrelid
 WHERE NOT x.indisvalid
   AND c.relname ~ '^utxo_p[0-9]+_ukey'`

// rebuildBloatedCoinIndex rebuilds at most ONE bloated coin index, and returns which one.
//
// One per call, worst first, is the whole cost control. A rebuild is cheap but not free: on the
// mainnet box, under full sync load, each of the eight took between 11.8 and 18.2 seconds. Doing
// the set in one session would stall a pruning session for two minutes; doing one leaves the
// next seven to the next seven blocks, and once a leaf is rebuilt it drops below the threshold
// and stops being chosen. So the work spreads itself and then stops, with no schedule to tune
// and no counter to keep.
//
// An empty name with a nil error means nothing needed doing, which is the normal case.
func (s *Store) rebuildBloatedCoinIndex(ctx context.Context) (string, error) {
	threshold := s.coinIndexRebuildBytesPerRow

	switch {
	case threshold < 0:
		// Negative disables the whole step, for an operator who would rather run pg_repack
		// or their own maintenance window than have the node take locks on their database.
		return "", nil
	case threshold == 0:
		threshold = DefaultCoinIndexRebuildBytesPerRow
	}

	if err := s.dropInvalidCoinIndexes(ctx); err != nil {
		return "", err
	}

	worst, ratio, err := s.worstCoinIndex(ctx, threshold)
	if err != nil || worst == "" {
		return "", err
	}

	// REINDEX CONCURRENTLY cannot run inside a transaction block, so this is a bare Exec on
	// the pool and never inside one. It takes SHARE UPDATE EXCLUSIVE on the index and its
	// table, which permits every read and write this store issues and blocks only schema
	// changes. The coin table's partitions are created once at schema install and never
	// altered afterwards, so there is nothing for it to contend with. The journal and
	// transaction-body partitions ARE created and dropped constantly, and they are different
	// tables.
	//
	// The name is interpolated because an identifier cannot be a bind parameter. It comes from
	// the catalog, matched against ^utxo_p[0-9]+_ukey$, so it cannot be anything else.
	if _, err = s.pool.Exec(ctx, fmt.Sprintf(`REINDEX INDEX CONCURRENTLY %s`, worst)); err != nil {
		return "", errors.NewStorageError("[utxoset] rebuild coin index %s", worst, err)
	}

	s.logger.Infof("[utxoset] rebuilt coin index %s, which held %d bytes per live row against a %d threshold",
		worst, ratio, threshold)

	return worst, nil
}

// worstCoinIndex returns the most bloated coin index above the threshold, or an empty name.
func (s *Store) worstCoinIndex(ctx context.Context, threshold int) (string, int64, error) {
	rows, err := s.pool.Query(ctx, coinIndexBloatSQL)
	if err != nil {
		return "", 0, errors.NewStorageError("[utxoset] read coin index sizes", err)
	}

	defer rows.Close()

	var (
		worst string
		ratio int64
	)

	for rows.Next() {
		var (
			name  string
			bytes int64
			live  int64
		)

		if err = rows.Scan(&name, &bytes, &live); err != nil {
			return "", 0, errors.NewStorageError("[utxoset] scan coin index sizes", err)
		}

		minRows := s.coinIndexMinRows
		if minRows <= 0 {
			minRows = DefaultCoinIndexMinRows
		}

		if live < int64(minRows) {
			continue
		}

		if perRow := bytes / live; perRow >= int64(threshold) && perRow > ratio {
			worst, ratio = name, perRow
		}
	}

	if err = rows.Err(); err != nil {
		return "", 0, errors.NewStorageError("[utxoset] coin index sizes", err)
	}

	return worst, ratio, nil
}

// dropInvalidCoinIndexes clears what a failed rebuild left behind.
//
// DROP INDEX CONCURRENTLY, not a plain drop, because the plain form takes ACCESS EXCLUSIVE on
// the coin partition and would stall every spend for its duration. Like REINDEX CONCURRENTLY it
// cannot run inside a transaction block.
func (s *Store) dropInvalidCoinIndexes(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, invalidCoinIndexSQL)
	if err != nil {
		return errors.NewStorageError("[utxoset] find invalid coin indexes", err)
	}

	var stale []string

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return errors.NewStorageError("[utxoset] scan invalid coin indexes", err)
		}

		stale = append(stale, name)
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset] invalid coin indexes", err)
	}

	for _, name := range stale {
		if _, err = s.pool.Exec(ctx, fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s`, name)); err != nil {
			return errors.NewStorageError("[utxoset] drop invalid coin index %s", name, err)
		}

		s.logger.Warnf("[utxoset] dropped %s, left invalid by an interrupted index rebuild", name)
	}

	return nil
}
