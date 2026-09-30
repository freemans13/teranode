package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
)

// SpendAndCreate implements utxo.Store. It delegates to the shared sequential
// implementation; an atomic implementation using a database transaction is a
// followup.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	return utxo.SequentialSpendAndCreate(ctx, s.logger, s, tx, blockHeight, opts...)
}

// SpendAndCreateMulti implements utxo.Store through the shared
// DefaultSpendAndCreateMulti. On Postgres it writes each dependency level's
// transactions concurrently, as wide as subtree validation writes a level today.
// SQLite has one writer, and overlapping write transactions abort each other's
// spend batches with a table-lock error, so there a level is written one
// transaction at a time.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	concurrency := 1
	if s.engine == string(util.Postgres) {
		concurrency = utxo.SpendAndCreateMultiConcurrency(s.settings)
	}

	return utxo.DefaultSpendAndCreateMulti(ctx, s, concurrency, txs, blockHeight, opts...)
}
