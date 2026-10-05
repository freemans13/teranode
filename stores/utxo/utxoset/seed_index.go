package utxoset

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// seedIndexBuildConcurrency is how many partitions build their ukey index at once per
// tablespace after a deferred-index seed. Each build holds temporary sort files about the size
// of the index it is writing until it finishes, at the end of a seed when the disks are
// fullest. Bounding it per tablespace rather than overall matters: on mainnet five partitions
// share the second drive, and four of them building together there would leave it about 20 GB
// free at the peak, where two at a time leaves about 55 GB.
const seedIndexBuildConcurrency = 2

// seedIndexBuildMemory is each build's maintenance_work_mem. With two builds per tablespace and
// two tablespaces on mainnet, four at once take four times this out of the container's memory.
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

	byTablespace := make(map[string][]int, 2)

	for leaf := 0; leaf < NumLeaves; leaf++ {
		ts, err := s.partitionTablespace(ctx, leaf)
		if err != nil {
			return err
		}

		byTablespace[ts] = append(byTablespace[ts], leaf)
	}

	g, gCtx := errgroup.WithContext(ctx)

	for ts, leaves := range byTablespace {
		g.Go(func() error {
			tg, tCtx := errgroup.WithContext(gCtx)
			tg.SetLimit(seedIndexBuildConcurrency)

			for _, leaf := range leaves {
				tg.Go(func() error {
					return s.buildOneUTXOIndexAfterSeed(tCtx, leaf, ts)
				})
			}

			return tg.Wait()
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	s.logger.Infof("[utxoset] built the %d ukey indexes deferred by the seed in %s", NumLeaves, time.Since(start).Round(time.Second))

	return nil
}

// partitionTablespace is the tablespace utxo_pN lives in, pg_default when it has none of its own.
func (s *Store) partitionTablespace(ctx context.Context, leaf int) (string, error) {
	var tablespace string
	if err := s.pool.QueryRow(ctx, `SELECT coalesce(t.spcname, 'pg_default')
  FROM pg_class c LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
 WHERE c.relname = $1`, fmt.Sprintf("utxo_p%d", leaf)).Scan(&tablespace); err != nil {
		return "", errors.NewStorageError("[utxoset] read the tablespace of utxo_p%d", leaf, err)
	}

	return tablespace, nil
}

func (s *Store) buildOneUTXOIndexAfterSeed(ctx context.Context, leaf int, tablespace string) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset] acquire a connection to build utxo_p%d_ukey", leaf, err)
	}
	defer conn.Release()

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
