package utxoset

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so the create and spend
// statements run unchanged whether they are issued standalone or composed inside one
// transaction by SpendAndCreate. That composition is the whole reason this abstraction
// exists: it is what lets a failed create undo its spends with a ROLLBACK instead of
// compensating logic.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Store is the delete-on-spend UTXO-set store.
//
// It satisfies the same utxo.Store contract as the postgres, aerospike and sql stores,
// so the store-agnostic conformance suite in stores/utxo/tests applies to it unchanged.
// That suite is the specification: M1 is complete when it passes.
type Store struct {
	logger   ulogger.Logger
	settings *settings.Settings
	pool     *pgxpool.Pool

	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32

	// journal controls whether a spend captures its undo payload.
	//
	// It defaults to TRUE and must be turned off deliberately. Without a journal a
	// spend is irreversible: nothing can restore the coin on a reorg, and
	// ProcessConflicting cannot unspend. The only condition under which that is safe
	// is applying blocks below the hardcoded checkpoint, where a reorg is impossible
	// by rule and there is no mempool to produce conflicts -- which is exactly what
	// SetSyncMode asserts. Defaulting to off would make an irreversible store the
	// accident rather than the choice.
	journal atomic.Bool

	// journalLeaf is the spend-journal leaf the last spend landed in, so the catalog is only
	// touched when it changes.
	journalLeaf atomic.Uint32

	// journalDDL serialises spend-journal partition creation within this process.
	// CREATE TABLE IF NOT EXISTS is not concurrency-safe in PostgreSQL.
	journalDDL sync.Mutex

	// journalRetention is how far back spends stay undoable, in blocks.
	journalRetention uint32
}

// New opens the store and installs the schema.
//
// The store is reached by its own URL scheme, utxoset://, so that an operator selecting it
// has to mean it rather than flipping a query parameter on a postgres:// URL that points at
// an incompatible schema. pgx knows nothing about that scheme, and given it will not fail
// cleanly: it folds the entire URL into a config parameter and falls back to a unix socket,
// which surfaces as a baffling "unrecognized configuration parameter" against the wrong
// host. So normalise it here, at the one place that owns the connection.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	dsn := *storeURL
	if dsn.Scheme == "utxoset" {
		dsn.Scheme = "postgres"
	}

	pool, err := pgxpool.New(ctx, dsn.String())
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] open pool", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.NewStorageError("[utxoset] ping", err)
	}

	s := &Store{logger: logger, settings: tSettings, pool: pool,
		journalRetention: DefaultSpendJournalRetentionBlocks}
	s.journal.Store(true)

	if err := CreateSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, errors.NewStorageError("[utxoset] create schema", err)
	}

	return s, nil
}

// SupportsOutpointOnlySpend reports that this store can spend by outpoint alone.
//
// This gates the below-checkpoint fast path (model.OutpointOnlyEligible). It is true
// here for a structural reason rather than as an optimisation: the UTXO table IS the
// outpoint set, so a spend needs nothing but the outpoint to be authorised.
func (s *Store) SupportsOutpointOnlySpend() bool { return true }

func (s *Store) SetBlockHeight(height uint32) error {
	s.blockHeight.Store(height)
	return nil
}

func (s *Store) GetBlockHeight() uint32 { return s.blockHeight.Load() }

func (s *Store) SetMedianBlockTime(t uint32) error {
	s.medianBlockTime.Store(t)
	return nil
}

func (s *Store) GetMedianBlockTime() uint32 { return s.medianBlockTime.Load() }

// SetSyncMode disables the spend journal for below-checkpoint block application, where a
// reorg is impossible by rule and there is no mempool. It must be turned back on before
// the node crosses the checkpoint or starts accepting unmined transactions.
func (s *Store) SetSyncMode(on bool) { s.journal.Store(!on) }

// JournalEnabled reports whether spends are currently reversible.
func (s *Store) JournalEnabled() bool { return s.journal.Load() }

func (s *Store) PoolMaxConns() int { return int(s.pool.Config().MaxConns) }

func (s *Store) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}

func (s *Store) Health(ctx context.Context, _ bool) (int, string, error) {
	if err := s.pool.Ping(ctx); err != nil {
		return 503, "utxoset: unreachable", err
	}

	return 200, "utxoset: ok", nil
}

// errM1 marks a method that is deliberately out of M1 scope.
//
// M1 is the UTXO table alone, in sync mode below the hardcoded checkpoint. The undo
// journal and the tx_meta window arrive in M3, and everything that depends on them
// fails loudly here rather than silently returning a wrong answer — a store that
// quietly answers "not found" for a question it cannot answer is how consensus bugs
// start.
func errM1(method string) error {
	return errors.NewProcessingError("[utxoset] %s is not implemented in M1 (UTXO-table-only, sync mode); it depends on the spend journal or tx_meta window landing in M3", method)
}
