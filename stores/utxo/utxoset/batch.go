package utxoset

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxBatch is a thin wrapper over pgx.Batch that sends every queued statement in one round
// trip and fails on the first error.
//
// It exists so a per-transaction stamp does not become a per-transaction round trip. A block
// carries tens of thousands of transactions, and at one round trip each the stamp would
// dominate block application.
type pgxBatch struct {
	b pgx.Batch
}

func (p *pgxBatch) queue(sql string, args ...any) {
	p.b.Queue(sql, args...)
}

func (p *pgxBatch) send(ctx context.Context, pool *pgxpool.Pool) error {
	if p.b.Len() == 0 {
		return nil
	}

	res := pool.SendBatch(ctx, &p.b)
	defer func() { _ = res.Close() }()

	for i := 0; i < p.b.Len(); i++ {
		if _, err := res.Exec(); err != nil {
			return err
		}
	}

	return nil
}
