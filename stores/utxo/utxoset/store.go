package utxoset

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
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

	// utxo.BlockStateFields supplies the chain-tip pair — block height and median
	// block time — and the six Store methods that read and write it, over a single
	// atomic snapshot. Embedding the shared implementation rather than carrying two
	// independent atomics here is what keeps GetBlockHeight and GetBlockState
	// reading the same memory, so they cannot disagree (issue 1443).
	utxo.BlockStateFields

	// journalLeaf is the spend-journal leaf the last spend landed in, so the catalog is only
	// touched when it changes.
	journalLeaf atomic.Uint32

	// journalDDL serialises spend-journal partition creation within this process.
	// CREATE TABLE IF NOT EXISTS is not concurrency-safe in PostgreSQL.
	journalDDL sync.Mutex

	// journalRetention is how far back spends stay undoable, in blocks.
	journalRetention uint32

	// bodyWindow is the tx_body window the last create landed in, so the catalog is only
	// touched when it changes.
	bodyWindow atomic.Uint32

	// bodyDDL serialises tx_body partition creation within this process, for the same
	// reason journalDDL does: CREATE TABLE IF NOT EXISTS is not concurrency-safe.
	bodyDDL sync.Mutex

	// bodyRetention is how long the serialized transaction bytes are kept, in blocks.
	bodyRetention uint32
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
		journalRetention: DefaultSpendJournalRetentionBlocks,
		bodyRetention:    DefaultTxBodyRetentionBlocks}
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

// SetBlockHeight records the chain height.
//
// It deliberately does NOT decide whether spends are reversible. An earlier version of
// this drove sync mode from model.BelowCheckpoint here, switching the journal off for
// the whole initial sync on the reasoning that a reorg is impossible below the
// checkpoint. That reasoning is sound and still irrelevant, because the journal's
// second job outweighs its first: see the spend_journal comment in schema.go for why it
// has no off-switch.
func (s *Store) SetBlockHeight(height uint32) error {
	return s.BlockStateFields.SetBlockHeight(height)
}

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
// M1 is the UTXO table, the spend journal and the block ledger. What is missing is the
// transaction window: tx_bounded and tx_mined. Everything that depends on those fails
// loudly here rather than silently returning a wrong answer — a store that quietly
// answers "not found" for a question it cannot answer is how consensus bugs start.
func errM1(method string) error {
	return errors.NewProcessingError("[utxoset] %s is not implemented yet; it depends on the tx_bounded and tx_mined tables", method)
}
