package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
)

func (s *SQL) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	return nil, nil
}
