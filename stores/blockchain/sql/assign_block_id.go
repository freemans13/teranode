package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jellydator/ttlcache/v3"
)

// AssignBlockID returns a stable block ID for the given block hash. Unlike
// GetNextBlockID (which always burns a fresh nextval), repeated calls for the
// same hash return the SAME id, and concurrent callers converge on one id. This
// is the single authority both ingestion paths use so a block's UTXO mined-info
// and its committed blocks row can never reference different ids.
//
// Resolution order (idempotent per hash):
//  1. If the block is already committed, return its authoritative id.
//  2. If an id is reserved for this hash in the in-memory cache (L1), return it.
//  3. If an id is reserved in the durable block_id_reservations table (L2),
//     return it (and re-populate L1).
//  4. Otherwise reserve a fresh nextval id, persist it durably, and remember it.
//
// The durable L2 table backs the in-memory cache for the three windows the cache
// alone cannot cover: the 10-minute ttlcache TTL expiring while a large block is
// still being processed, a process restart, and a second blockchain instance
// (replicas>1). Without L2, a re-entering caller in any of those windows would
// burn a second nextval and re-open the phantom-id divergence #1043 closed.
func (s *SQL) AssignBlockID(ctx context.Context, blockHash *chainhash.Hash) (uint64, error) {
	if id, ok, err := s.blockIDByHash(ctx, blockHash); err != nil {
		return 0, err
	} else if ok {
		return id, nil
	}

	// The mutex is deliberately held across the cache lookup, the committed
	// re-check below (a DB query) and the reservation, so that concurrent callers
	// for the same hash within THIS process serialize and exactly one allocates.
	// Re-ordering the DB call out of the lock would reopen the race this method
	// exists to close. Cross-process races are handled by the durable table's
	// INSERT ... ON CONFLICT below. The lock is only contended when callers race on
	// the same block — the intended case — and each holder releases after the
	// reservation completes.
	s.blockIDReservationMu.Lock()
	defer s.blockIDReservationMu.Unlock()

	if item := s.blockIDReservations.Get(*blockHash); item != nil {
		return item.Value(), nil
	}

	// Re-check committed under the lock: a concurrent StoreBlock could have
	// committed this hash between the unlocked check above and acquiring the lock.
	if id, ok, err := s.blockIDByHash(ctx, blockHash); err != nil {
		return 0, err
	} else if ok {
		return id, nil
	}

	// Durable L2 reservation: survives ttlcache TTL expiry, restart, and a second
	// instance. Check it before burning a fresh nextval.
	if id, ok, err := s.durableReservationID(ctx, blockHash); err != nil {
		return 0, err
	} else if ok {
		s.blockIDReservations.Set(*blockHash, id, ttlcache.DefaultTTL)
		return id, nil
	}

	id, err := s.reserveDurableBlockID(ctx, blockHash)
	if err != nil {
		return 0, err
	}

	// ttlcache.DefaultTTL applies the cache-wide default set in New (blockIDReservationTTL).
	s.blockIDReservations.Set(*blockHash, id, ttlcache.DefaultTTL)

	return id, nil
}

// durableReservationID returns the block id reserved for a hash in the durable
// block_id_reservations table, or ok=false if none is reserved.
func (s *SQL) durableReservationID(ctx context.Context, blockHash *chainhash.Hash) (uint64, bool, error) {
	var id uint64
	err := s.db.QueryRowContext(ctx, `SELECT block_id FROM block_id_reservations WHERE hash = $1`, blockHash[:]).Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, errors.NewStorageError("failed to look up durable block-id reservation", err)
	}
	return id, true, nil
}

// reserveDurableBlockID allocates a fresh nextval id and persists it for the hash
// in the durable table. The INSERT ... ON CONFLICT DO NOTHING makes the
// reservation idempotent across processes: if a concurrent instance inserted a row
// for this hash first, our INSERT is a no-op and we read back its id — discarding
// our just-allocated nextval. A discarded nextval is a harmless sequence gap (no
// UTXO references it), never a phantom. Called under blockIDReservationMu.
func (s *SQL) reserveDurableBlockID(ctx context.Context, blockHash *chainhash.Hash) (uint64, error) {
	id, err := s.GetNextBlockID(ctx)
	if err != nil {
		return 0, err
	}

	insertQ := `INSERT INTO block_id_reservations (hash, block_id) VALUES ($1, $2) ON CONFLICT (hash) DO NOTHING`
	if s.engine != util.Postgres {
		insertQ = `INSERT OR IGNORE INTO block_id_reservations (hash, block_id) VALUES ($1, $2)`
	}

	if _, err := s.db.ExecContext(ctx, insertQ, blockHash[:], id); err != nil {
		return 0, errors.NewStorageError("failed to persist durable block-id reservation", err)
	}

	// Re-read: on a cross-process conflict our INSERT was a no-op and the winning
	// instance's id is the authority; in the common (no-conflict) case this returns
	// the id we just inserted.
	stored, ok, err := s.durableReservationID(ctx, blockHash)
	if err != nil {
		return 0, err
	}

	// Final committed-check (resolution priority 1). A concurrent instance can
	// commit this hash (under a different id) AND have StoreBlock delete its
	// reservation row during our reserve — after AssignBlockID's under-lock
	// committed-check passed but before/around our L2 read. That surfaces two ways,
	// both of which would otherwise return a divergent (phantom) id for an
	// already-committed hash:
	//   - the delete lands after our INSERT  → our re-read finds no row (!ok);
	//   - the delete lands before our INSERT → our own nextval wins the INSERT and
	//     the re-read returns it (ok, stored == id).
	// In both cases the committed blocks row is the authority, so re-check it before
	// trusting either result. (One extra SELECT, only on the reserve slow path.)
	if committedID, committed, cErr := s.blockIDByHash(ctx, blockHash); cErr != nil {
		return 0, cErr
	} else if committed {
		return committedID, nil
	}

	if !ok {
		// Unreachable in practice (we just inserted, or someone else did, and the
		// block isn't committed) — never mint a bare nextval that wasn't persisted.
		return id, nil
	}

	return stored, nil
}

// blockIDByHash returns the committed id for a block hash, or ok=false if no
// such row exists yet.
func (s *SQL) blockIDByHash(ctx context.Context, blockHash *chainhash.Hash) (uint64, bool, error) {
	var id uint64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM blocks WHERE hash = $1`, blockHash[:]).Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, errors.NewStorageError("failed to look up block id by hash", err)
	}
	return id, true, nil
}

// checkCallerSuppliedBlockID decides whether StoreBlock may write a blocks row
// under an id the caller chose (options.WithID) rather than one the INSERT
// allocates. Quick validation reserves an id with AssignBlockID, stamps the
// block's transactions with it in the UTXO store, and only then asks for the
// blocks row. The UTXO store is a separate database, so nothing but this check
// stops a row landing under an id that another block's transactions already
// carry, or under an id the sequence will hand out later.
//
// The rules, in order:
//   - The hash already has a blocks row: pass, so the INSERT fails with the
//     same "block already exists" error it returned before this check existed.
//     Callers that retry an AddBlock whose first attempt landed rely on that.
//   - Another hash already has a blocks row under the id: refuse.
//   - The hash has a reservation: the id must equal it.
//   - The hash has no reservation: the id must not be reserved by another hash,
//     and the sequence must already have issued it.
//
// The last rule exists because sweepStaleReservations deletes reservations older
// than staleReservationSweepAge. A block retried after a long outage reads its id
// back from its own transactions in the UTXO store and arrives with no
// reservation row. Refusing it would leave that block unable to commit at all.
//
// The check runs under slowPathMu but outside the INSERT's transaction. That is
// enough: a reservation is written once per hash and only ever deleted, and ids
// come from a sequence, so nothing can make a refused id acceptable or an accepted
// id taken between the check and the INSERT, other than the INSERT's own unique
// constraints, which still apply.
func (s *SQL) checkCallerSuppliedBlockID(ctx context.Context, blockHash *chainhash.Hash, id uint64) error {
	if _, committed, err := s.blockIDByHash(ctx, blockHash); err != nil {
		return err
	} else if committed {
		return nil
	}

	// Another block already committed under this id. The INSERT would fail on
	// the primary key, but parseSQLError reports any unique violation as "block
	// already exists", which legacy sync treats as success, so the block would be
	// dropped with no row. Refuse it here with an error that says what happened.
	var owner []byte

	err := s.db.QueryRowContext(ctx, `SELECT hash FROM blocks WHERE id = $1`, id).Scan(&owner)
	if err == nil {
		return errors.NewStorageError("[StoreBlock][%s] refusing caller-supplied block id %d: block %s is already stored under it", blockHash.String(), id, hashString(owner))
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return errors.NewStorageError("[StoreBlock][%s] failed to look up the block stored under id %d", blockHash.String(), id, err)
	}

	reserved, ok, err := s.durableReservationID(ctx, blockHash)
	if err != nil {
		return err
	}

	if ok {
		if reserved != id {
			return errors.NewStorageError("[StoreBlock][%s] refusing caller-supplied block id %d: the id reserved for this block is %d", blockHash.String(), id, reserved)
		}

		return nil
	}

	var holder []byte

	err = s.db.QueryRowContext(ctx, `SELECT hash FROM block_id_reservations WHERE block_id = $1 LIMIT 1`, id).Scan(&holder)
	if err == nil {
		return errors.NewStorageError("[StoreBlock][%s] refusing caller-supplied block id %d: it is reserved for block %s", blockHash.String(), id, hashString(holder))
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return errors.NewStorageError("[StoreBlock][%s] failed to look up the reservation holding block id %d", blockHash.String(), id, err)
	}

	highest, err := s.highestIssuedBlockID(ctx)
	if err != nil {
		return err
	}

	if id > highest {
		return errors.NewStorageError("[StoreBlock][%s] refusing caller-supplied block id %d: the id sequence has only issued up to %d and this block has no reservation", blockHash.String(), id, highest)
	}

	return nil
}

// hashString renders a hash column for an error message, falling back to hex
// when the bytes are not a 32-byte hash.
func hashString(b []byte) string {
	h, err := chainhash.NewHash(b)
	if err != nil {
		return fmt.Sprintf("%x", b)
	}

	return h.String()
}

// highestIssuedBlockID returns the largest block id the id sequence has handed
// out so far, whether through GetNextBlockID, AssignBlockID or an auto-increment
// INSERT. Every honest caller-supplied id came from that sequence, so an id above
// this value was never issued. It reads the sequence without advancing it.
func (s *SQL) highestIssuedBlockID(ctx context.Context) (uint64, error) {
	var (
		q       string
		highest sql.NullInt64
	)

	if s.engine == util.Postgres {
		// pg_sequence_last_value returns NULL until the first nextval.
		q = `SELECT pg_sequence_last_value(pg_get_serial_sequence('blocks', 'id')::regclass)`
	} else {
		// AUTOINCREMENT keeps seq at the largest rowid ever used, and
		// getNextBlockIdFromSQLite advances it directly.
		q = `SELECT seq FROM sqlite_sequence WHERE name = 'blocks'`
	}

	if err := s.db.QueryRowContext(ctx, q).Scan(&highest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}

		return 0, errors.NewStorageError("failed to read the highest issued block id", err)
	}

	if !highest.Valid || highest.Int64 < 0 {
		return 0, nil
	}

	return uint64(highest.Int64), nil
}
