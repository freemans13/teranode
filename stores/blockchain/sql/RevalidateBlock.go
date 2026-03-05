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

	// Invalidate caches to ensure cached blocks reflect updated invalid field
	defer s.ResetResponseCache()
	defer s.resetChainWalkCache()
	// Rebuild the off-chain set and membership cache after revalidation
	defer func() {
		if rebuildErr := s.rebuildOffChainSet(ctx); rebuildErr != nil {
			s.logger.Errorf("RevalidateBlock: %v", rebuildErr)
		}
	}()

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
