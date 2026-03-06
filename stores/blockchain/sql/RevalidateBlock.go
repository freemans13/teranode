package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

func (s *SQL) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("RevalidateBlock %s", blockHash.String())

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return errors.NewStorageError("error checking block exists", err)
	}

	if !exists {
		return errors.NewStorageError("block %s does not exist", blockHash.String())
	}

	// Go defers execute LIFO, so register in reverse order of desired execution.
	// Desired: ResetResponseCache → resetChainWalkCache → rebuildOffChainSet
	// (rebuildOffChainSet calls GetBestBlockHeader which reads the response cache,
	// so the cache must be reset before the rebuild runs.)
	// Use context.Background() because the caller's ctx may have been
	// cancelled after the DB update succeeded — the rebuild must still run
	// to keep the in-memory membership state consistent.
	defer func() {
		if rebuildErr := s.rebuildOffChainSet(context.Background()); rebuildErr != nil {
			s.logger.Errorf("RevalidateBlock: %v", rebuildErr)
		}
	}()
	defer s.resetChainWalkCache()
	defer s.ResetResponseCache()

	// recursively update all children blocks to invalid in 1 query
	q := `
		UPDATE blocks
		SET invalid = false, mined_set = false
		WHERE hash = $1
	`
	if _, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return errors.NewStorageError("error updating block to invalid", err)
	}

	return nil
}
