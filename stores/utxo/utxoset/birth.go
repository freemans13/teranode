package utxoset

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// txBirthWindowSQL lists the birth windows this store owns, in whichever of the three states
// a crash can leave them (see txBodyWindowSQL for the states and why the join is LEFT).
const txBirthWindowSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'tx_birth'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'tx_birth'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^tx_birth_w[0-9]+$'`

// identExistsSQL reports which of these transactions still have an identity row at all. A
// birth whose transaction was deleted outright (the rewind tool) has nothing left to judge
// and must not be re-queued forever.
const identExistsSQL = `
SELECT i.txid
  FROM unnest($1::bytea[]) AS k(txid)
  JOIN tx_ident i ON i.leaf = (get_byte(k.txid, 0) & 7)::smallint AND i.txid = k.txid`

// requeueBirthSQL puts a transaction that is not yet finished into the window of the height
// it was judged at, so it comes back once that window retires.
const requeueBirthSQL = `INSERT INTO tx_birth (created_height, txid) SELECT $1, unnest($2::bytea[])`

// reclaimBirthWindowsBelow retires every birth window whose upper bound is below cutoff.
//
// Each window is read whole as a work list before it is dropped, in bounded chunks. A
// transaction is finished when it is mined on the main chain and every block naming it is at
// or below tip minus SettledDepthBlocks, which is the same settled rule the journal reclaim
// applies to spenders. It has no coins by construction, and withLiveCoins is run anyway as
// the fail-safe against a birth row written for a transaction that did get a coin. A
// transaction that is not finished is re-queued into the current window; one that no longer
// has an identity row is simply forgotten.
//
// Returns the identity rows deleted and the windows dropped.
func (s *Store) reclaimBirthWindowsBelow(ctx context.Context, cutoff, tip uint32) (int, int, error) {
	rows, err := s.pool.Query(ctx, txBirthWindowSQL)
	if err != nil {
		return 0, 0, errors.NewStorageError("[utxoset] list tx_birth windows", err)
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
			return 0, 0, errors.NewStorageError("[utxoset] scan tx_birth window", err)
		}

		if _, err := fmt.Sscanf(w.name, "tx_birth_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, 0, errors.NewStorageError("[utxoset] list tx_birth windows", err)
	}

	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	cutoffWindow := cutoff / TxBodyPartitionBlocks

	var reclaimed, dropped int

	for _, w := range windows {
		if w.window >= cutoffWindow {
			continue
		}

		// Read before drop: the window IS the work list, in whichever state a crash left it.
		n, err := s.reclaimBirths(ctx, w.name, tip)
		if err != nil {
			return reclaimed, dropped, err
		}

		reclaimed += n

		switch {
		case w.detachPending:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_birth DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return reclaimed, dropped, errors.NewStorageError("[utxoset] finalize detach of tx_birth window %s", w.name, err)
			}

		case w.attached:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_birth DETACH PARTITION %s CONCURRENTLY`, w.name)); err != nil {
				return reclaimed, dropped, errors.NewStorageError("[utxoset] detach tx_birth window %s", w.name, err)
			}

		default:
			// Already standalone after an interrupted session: just finish the job.
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, w.name)); err != nil {
			return reclaimed, dropped, errors.NewStorageError("[utxoset] drop tx_birth window %s", w.name, err)
		}

		dropped++
	}

	return reclaimed, dropped, nil
}

// reclaimBirths judges one birth window and deletes the identity rows of the transactions in
// it that are finished. Unfinished ones are re-queued into the window covering tip.
func (s *Store) reclaimBirths(ctx context.Context, window string, tip uint32) (int, error) {
	limit := s.reclaimChunkParents
	if limit <= 0 {
		limit = DefaultReclaimChunkParents
	}

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT txid FROM %s`, window))
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][births] read %s", window, err)
	}

	var (
		doomed  [][]byte
		requeue [][]byte
		chunk   = make([][]byte, 0, limit)
		scanErr error
	)

	judge := func(txids [][]byte) error {
		d, r, err := s.judgeBirths(ctx, txids, tip)
		if err != nil {
			return err
		}

		doomed = append(doomed, d...)
		requeue = append(requeue, r...)

		return nil
	}

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			scanErr = errors.NewStorageError("[utxoset][births] scan %s", window, err)
			break
		}

		chunk = append(chunk, txid)

		if len(chunk) >= limit {
			if err := judge(chunk); err != nil {
				scanErr = err
				break
			}

			chunk = make([][]byte, 0, limit)
		}
	}

	rows.Close()

	if scanErr != nil {
		return 0, scanErr
	}

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset][births] rows %s", window, err)
	}

	if len(chunk) > 0 {
		if err := judge(chunk); err != nil {
			return 0, err
		}
	}

	if len(requeue) > 0 {
		// The current window may not exist yet if nothing has been created at this height.
		if err := s.ensureTxBodyPartition(ctx, tip); err != nil {
			return 0, err
		}

		if _, err := s.pool.Exec(ctx, requeueBirthSQL, int32(tip), requeue); err != nil { //nolint:gosec // a height fits
			return 0, errors.NewStorageError("[utxoset][births] requeue", err)
		}
	}

	return s.deleteIdents(ctx, doomed, limit)
}

// judgeBirths splits one chunk of birth rows into those to delete and those to re-queue.
func (s *Store) judgeBirths(ctx context.Context, txids [][]byte, tip uint32) (doomed, requeue [][]byte, err error) {
	present, err := s.identsPresent(ctx, txids)
	if err != nil {
		return nil, nil, err
	}

	settled, err := s.settled(ctx, txids, tip)
	if err != nil {
		return nil, nil, err
	}

	live, err := s.withLiveCoins(ctx, txids)
	if err != nil {
		return nil, nil, err
	}

	for _, txid := range txids {
		key := string(txid)

		if _, ok := present[key]; !ok {
			continue // deleted outright; nothing to judge and nothing to wait for
		}

		if _, hasCoin := live[key]; hasCoin {
			// A birth row for a transaction with a coin is a bug upstream; the spend of that
			// coin will name it, so the ledger has no further business with it.
			continue
		}

		if _, ok := settled[key]; ok {
			doomed = append(doomed, txid)
		} else {
			requeue = append(requeue, txid)
		}
	}

	return doomed, requeue, nil
}

// identsPresent returns the subset of txids that still have an identity row.
func (s *Store) identsPresent(ctx context.Context, txids [][]byte) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(txids))

	rows, err := s.pool.Query(ctx, identExistsSQL, txids)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][births] present", err)
	}

	defer rows.Close()

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			return nil, errors.NewStorageError("[utxoset][births] present scan", err)
		}

		out[string(txid)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][births] present rows", err)
	}

	return out, nil
}
