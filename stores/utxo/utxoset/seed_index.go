package utxoset

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// seedIndexBuildConcurrency is how many partitions build their ukey index at once after a
// deferred-index seed. Each build sorts through temporary files about the size of its index,
// so it is bounded rather than all eight together, to keep the scratch space at the end of a
// seed, when the disks are fullest, to a few indexes' worth.
const seedIndexBuildConcurrency = 4

// seedIndexBuildMemory is each build's maintenance_work_mem. Four builds at once take four
// times this out of the container's memory.
const seedIndexBuildMemory = "1GB"

// dropUTXOIndexesForSeed drops every partition's ukey index. Only a seedDeferIndex store calls
// it, on a table that held no coins when the store opened, so there is nothing to lose: the
// index is rebuilt from the loaded rows by buildUTXOIndexesAfterSeed.
func (s *Store) dropUTXOIndexesForSeed(ctx context.Context) error {
	for leaf := 0; leaf < NumLeaves; leaf++ {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS utxo_p%d_ukey`, leaf)); err != nil {
			return errors.NewStorageError("[utxoset] drop utxo_p%d_ukey for the seed", leaf, err)
		}
	}

	s.logger.Infof("[utxoset] seeding with the ukey indexes deferred: the load runs with none, and they are built once the import is done")

	return nil
}

// buildUTXOIndexesAfterSeed builds every partition's ukey index, each in its table's tablespace
// and with its sort's temporary files there too, so a partition on the second drive neither
// builds its index on the root drive nor spills its sort there. It is what CreateSchema's index
// would be, built once from the loaded rows. IF NOT EXISTS makes a second call a no-op.
func (s *Store) buildUTXOIndexesAfterSeed(ctx context.Context) error {
	start := time.Now()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(seedIndexBuildConcurrency)

	for leaf := 0; leaf < NumLeaves; leaf++ {
		g.Go(func() error {
			return s.buildOneUTXOIndexAfterSeed(gCtx, leaf)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	s.logger.Infof("[utxoset] built the %d ukey indexes deferred by the seed in %s", NumLeaves, time.Since(start).Round(time.Second))

	return nil
}

func (s *Store) buildOneUTXOIndexAfterSeed(ctx context.Context, leaf int) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset] acquire a connection to build utxo_p%d_ukey", leaf, err)
	}
	defer conn.Release()

	var tablespace string
	if err := conn.QueryRow(ctx, `SELECT coalesce(t.spcname, 'pg_default')
  FROM pg_class c LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
 WHERE c.relname = $1`, fmt.Sprintf("utxo_p%d", leaf)).Scan(&tablespace); err != nil {
		return errors.NewStorageError("[utxoset] read the tablespace of utxo_p%d", leaf, err)
	}

	// Session settings on this connection only, reset before it goes back to the pool.
	stmts := []string{
		fmt.Sprintf(`SET maintenance_work_mem = '%s'`, seedIndexBuildMemory),
		fmt.Sprintf(`SET temp_tablespaces = '%s'`, tablespace),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS utxo_p%[1]d_ukey ON utxo_p%[1]d (ukey) TABLESPACE %[2]s`, leaf, tablespace),
	}

	defer func() {
		_, _ = conn.Exec(context.Background(), `RESET maintenance_work_mem; RESET temp_tablespaces`)
	}()

	started := time.Now()

	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return errors.NewStorageError("[utxoset] build utxo_p%d_ukey after the seed", leaf, err)
		}
	}

	s.logger.Infof("[utxoset] built utxo_p%d_ukey in %s, tablespace %s", leaf, time.Since(started).Round(time.Second), tablespace)

	return nil
}
