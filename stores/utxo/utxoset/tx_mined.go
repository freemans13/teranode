package utxoset

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// TxMinedPartitionBlocks is the width of one membership window.
//
// 288, not the journal's 48. A lookup by transaction id with no height probes every live
// window, and six probes at 288 cost about 50 microseconds against 31 at 48 costing 260.
// Nothing needs 48-block drop granularity here.
const TxMinedPartitionBlocks = 288

// ensureTxMinedPartition creates the membership window covering height, if absent.
//
// It MUST be called before the caller opens its transaction, for the reason
// ensureTxBodyPartition must: the DDL needs its own pool connection.
//
// It REFUSES a window at or below the floor. The floor is the highest window ever dropped
// plus one, and a create below it can only be a block re-offered after its window retired.
// Recreating the window would claim every transaction in that block afresh and double every
// coin that is still live. Failing loudly here is the guard.
func (s *Store) ensureTxMinedPartition(ctx context.Context, height uint32) error {
	window := height / TxMinedPartitionBlocks

	if s.minedWindow.Load() == window+1 {
		return nil
	}

	s.minedDDL.Lock()
	defer s.minedDDL.Unlock()

	if s.minedWindow.Load() == window+1 {
		return nil
	}

	floor, err := s.txMinedFloor(ctx)
	if err != nil {
		return err
	}

	if window < floor {
		return errors.NewProcessingError("[utxoset] refusing to recreate dropped membership window %d for height %d (floor %d)", window, height, floor)
	}

	lo := window * TxMinedPartitionBlocks
	hi := lo + TxMinedPartitionBlocks

	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS tx_mined_w%[1]d PARTITION OF tx_mined
  FOR VALUES FROM (%[2]d) TO (%[3]d);
ALTER TABLE tx_mined_w%[1]d ALTER COLUMN tx_inpoints SET STORAGE EXTERNAL;`, window, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return errors.NewStorageError("[utxoset] create tx_mined window %d", window, err)
	}

	s.minedWindow.Store(window + 1)

	return nil
}

// txMinedFloor returns the highest dropped window index plus one; 0 when nothing was dropped.
func (s *Store) txMinedFloor(ctx context.Context) (uint32, error) {
	var floor int32
	if err := s.pool.QueryRow(ctx, `SELECT floor FROM tx_mined_floor WHERE id = 0`).Scan(&floor); err != nil {
		return 0, errors.NewStorageError("[utxoset] read tx_mined floor", err)
	}

	return uint32(floor), nil //nolint:gosec // a window index is never negative
}

// txMinedWindowSQL lists the membership windows in whichever of the three crash states they
// are in; see txBodyWindowSQL for the states and why the join is LEFT.
const txMinedWindowSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'tx_mined'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'tx_mined'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^tx_mined_w[0-9]+$'`

// dropTxMinedWindowsBelow drops every membership window whose upper bound is below
// cutoffHeight, oldest first, and advances the floor past each. Returns the count dropped.
//
// This IS identity reclaim in this design: no work list, no probes, no row deletes. The
// coins of transactions in a retiring window are stamped from the window's list first in
// stage 2; in stage 1 every coin was written with its block facts at create.
func (s *Store) dropTxMinedWindowsBelow(ctx context.Context, cutoffHeight uint32) (int, error) {
	cutoff := cutoffHeight / TxMinedPartitionBlocks

	rows, err := s.pool.Query(ctx, txMinedWindowSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	type windowState struct {
		name          string
		window        uint32
		attached      bool
		detachPending bool
	}

	var windows []windowState

	for rows.Next() {
		var w windowState
		if err := rows.Scan(&w.name, &w.attached, &w.detachPending); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("[utxoset] scan tx_mined window", err)
		}

		if _, err := fmt.Sscanf(w.name, "tx_mined_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	dropped := 0

	for _, w := range windows {
		if w.window >= cutoff {
			continue
		}

		switch {
		case w.detachPending:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of tx_mined window %s", w.name, err)
			}

		case w.attached:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s CONCURRENTLY`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach tx_mined window %s", w.name, err)
			}

		default:
			// Already standalone after an interrupted session: finish the job.
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, w.name)); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop tx_mined window %s", w.name, err)
		}

		// The floor moves BEFORE the next window is touched, so a crash between two drops
		// still leaves every dropped window below it.
		if _, err := s.pool.Exec(ctx,
			`UPDATE tx_mined_floor SET floor = GREATEST(floor, $1) WHERE id = 0`, int32(w.window+1)); err != nil { //nolint:gosec // a window index fits
			return dropped, errors.NewStorageError("[utxoset] advance tx_mined floor", err)
		}

		dropped++
	}

	return dropped, nil
}
