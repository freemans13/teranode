package utxoset

import (
	"context"
	"net/url"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
}

// New opens the store and installs the schema.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	pool, err := pgxpool.New(ctx, storeURL.String())
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] open pool", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.NewStorageError("[utxoset] ping", err)
	}

	s := &Store{logger: logger, settings: tSettings, pool: pool}

	if err := CreateSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, errors.NewStorageError("[utxoset] create schema", err)
	}

	return s, nil
}

// SupportsOutpointOnlySpend reports that this store can spend by outpoint alone.
//
// This gates the below-checkpoint fast path (model.OutpointOnlyEligible). It is true
// here for a structural reason rather than as an optimisation: the arbiter IS the
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
// M1 is the arbiter alone, in sync mode below the hardcoded checkpoint. The undo
// journal and the tx_meta window arrive in M3, and everything that depends on them
// fails loudly here rather than silently returning a wrong answer — a store that
// quietly answers "not found" for a question it cannot answer is how consensus bugs
// start.
func errM1(method string) error {
	return errors.NewProcessingError("[utxoset] %s is not implemented in M1 (arbiter-only, sync mode); it depends on the undo journal or tx_meta window landing in M3", method)
}
