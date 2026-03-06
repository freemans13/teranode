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

	// Update the block to valid (not invalid) and clear the mined_set flag.
	q := `
		UPDATE blocks
		SET invalid = false, mined_set = false
		WHERE hash = $1
	`
	if _, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return errors.NewStorageError("error updating block to valid", err)
	}

	// Invalidate caches and rebuild the off-chain set only after the UPDATE
	// succeeds. Order matters: ResetResponseCache must run before
	// rebuildOffChainSet because the rebuild calls GetBestBlockHeader which
	// reads the response cache.
	// Use a non-cancelable context with a timeout because the caller's ctx may
	// have been cancelled after the DB update succeeded — the rebuild must still
	// run to keep the in-memory membership state consistent, but should not
	// block indefinitely if the DB is unhealthy.
	s.ResetResponseCache()
	s.resetChainWalkCache()
	rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), rebuildOffChainSetTimeout)
	defer rebuildCancel()
	if rebuildErr := s.rebuildOffChainSet(rebuildCtx); rebuildErr != nil {
		s.logger.Errorf("RevalidateBlock: %v", rebuildErr)
	}

	return nil
}
