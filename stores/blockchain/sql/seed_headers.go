package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
)

// StoreSeedHeaders stores a run of headers that extends the current best block, in order, in one
// transaction. It writes exactly the rows StoreBlock writes, through the same storeBlock, but
// pays StoreBlock's per-call costs once per run instead of once per header: one commit, and
// therefore one WAL flush, instead of one per block, the median time past read from the
// timestamp cache it fills as it goes, and no durable reservation delete, since a seed reserves
// no block ids. It is for the seeder, which reads a trusted, linear header file into an empty or
// matching store.
//
// It returns false, with nothing written and no error, for a run that does not start at the
// current best block or is not itself a chain; the caller stores such a run through StoreBlock.
func (s *SQL) StoreSeedHeaders(ctx context.Context, blocks []*model.Block, peerID string, opts ...options.StoreBlockOption) (bool, error) {
	if len(blocks) == 0 {
		return true, nil
	}

	storeBlockOptions := options.StoreBlockOptions{}
	for _, opt := range opts {
		opt(&storeBlockOptions)
	}

	// Seeding is single-writer, but StoreBlock's invariants hold only under this lock, so take it.
	s.slowPathMu.Lock()
	defer s.slowPathMu.Unlock()

	_, bestHash, err := s.getBestBlockID(ctx)
	if err != nil {
		return false, errors.NewStorageError("[StoreSeedHeaders] read the best block", err)
	}

	prev := bestHash
	for _, b := range blocks {
		if prev == nil || b.Header.HashPrevBlock == nil || *b.Header.HashPrevBlock != *prev {
			return false, nil
		}

		prev = b.Hash()
	}

	if s.useInMemoryChainCheck {
		s.mainChainRebuilding.Add(1)
		defer s.mainChainRebuilding.Add(-1)
	}

	var lastID uint64

	err = s.db.RetryTx(ctx, nil, func(tx *sql.Tx) error {
		for _, b := range blocks {
			id, height, _, storedInvalid, storeErr := s.storeBlockWith(ctx, tx, tx, b, peerID, storeBlockOptions, true)
			if storeErr != nil {
				return storeErr
			}

			if storedInvalid {
				return errors.NewProcessingError("[StoreSeedHeaders] block %s at height %d would be stored invalid", b.Hash().String(), height)
			}

			// The next block's median time past reads this from the cache rather than walking
			// the parent chain. It is the value the row was written with, and on a rollback the
			// retry writes the same block at the same height again.
			s.blockTimestampCache.Add(height, b.Header.Timestamp)

			lastID = id
		}

		return nil
	})
	if err != nil {
		s.blockTimestampCache.Clear()
		return false, s.typedStoreBlockError(err, blocks[0])
	}

	s.ResetResponseCache()

	for _, b := range blocks {
		s.blockIDReservations.Delete(*b.Hash())
	}

	s.updateMaxBlockID(lastID)

	last := blocks[len(blocks)-1]
	cacheOp := s.responseCache.NewOp(chainhash.HashH([]byte("getBestBlockID")))
	cacheOp.Set(bestBlockIDResult{id: uint32(lastID), hash: last.Hash()}, s.cacheTTL) //nolint:gosec // block ids fit uint32, as StoreBlock assumes

	return true, nil
}
