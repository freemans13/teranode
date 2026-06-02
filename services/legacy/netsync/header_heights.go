package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
)

// setHeaderHeight records the height of a downloaded header, keyed by its hash.
// Called by handleHeadersMsg as each header is linked into the in-memory header
// list. Lazily allocates the map so a zero-value SyncManager (used in unit
// tests) is safe.
func (sm *SyncManager) setHeaderHeight(hash chainhash.Hash, height int32) {
	sm.headerHeightsMu.Lock()
	defer sm.headerHeightsMu.Unlock()

	if sm.headerHeights == nil {
		sm.headerHeights = make(map[chainhash.Hash]int32)
	}

	sm.headerHeights[hash] = height
}

// headerHeight returns the height previously recorded for the given block hash
// from the in-memory header list, and whether it was known.
func (sm *SyncManager) headerHeight(hash chainhash.Hash) (int32, bool) {
	sm.headerHeightsMu.RLock()
	defer sm.headerHeightsMu.RUnlock()

	h, ok := sm.headerHeights[hash]

	return h, ok
}

// deleteHeaderHeight prunes a single entry once its block has been ingested, so
// the map stays bounded to the in-flight window rather than growing for the
// whole sync.
func (sm *SyncManager) deleteHeaderHeight(hash chainhash.Hash) {
	sm.headerHeightsMu.Lock()
	defer sm.headerHeightsMu.Unlock()

	delete(sm.headerHeights, hash)
}

// resetHeaderHeights clears all recorded heights. Called whenever the header
// list itself is reset (new sync peer, or switching out of headers-first mode)
// so stale entries from an abandoned header chain cannot leak.
func (sm *SyncManager) resetHeaderHeights() {
	sm.headerHeightsMu.Lock()
	defer sm.headerHeightsMu.Unlock()

	sm.headerHeights = make(map[chainhash.Hash]int32)
}

// deriveBlockHeight determines the height of an incoming block.
//
// On the quick-validation pipeline path (Legacy.BlockFinalizationPipeline on and
// the height known from the in-memory header list) it returns that height
// WITHOUT a GetBlockHeader(prevBlock) round-trip. That blockchain read needs the
// parent block already ADDED — a finalization-pipeline output — which is exactly
// what forced an earlier pipeline attempt to serialise block N's tx-work behind
// block N-1 finalizing. The header list is checkpoint-anchored and was verified
// to link together in handleHeadersMsg, so its heights are trusted below the
// checkpoint.
//
// Otherwise (flag off / height not recorded / above the final checkpoint) it
// falls back to the original GetBlockHeader(prevBlock) lookup, preserving the
// default path's behaviour byte-for-byte. In both cases the block's height is
// set via SetHeight as a side effect.
func (sm *SyncManager) deriveBlockHeight(ctx context.Context, block *bsvutil.Block, blockHash chainhash.Hash) (uint32, error) {
	if sm.settings.Legacy.BlockFinalizationPipeline {
		if h, ok := sm.headerHeight(blockHash); ok && h > 0 {
			if block.Height() > 0 && block.Height() != h {
				return 0, errors.NewBlockInvalidError("block height %d does not match header-list height %d for block %s", block.Height(), h, blockHash)
			}

			block.SetHeight(h)

			height, err := safeconversion.Int32ToUint32(h)
			if err != nil {
				return 0, errors.NewProcessingError("failed to convert block height to uint32", err)
			}

			return height, nil
		}
	}

	// Fallback: look up the previous block's height from the blockchain store.
	_, previousBlockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(ctx, &block.MsgBlock().Header.PrevBlock)
	if err != nil {
		sm.logger.Errorf("[HandleBlockDirect][%s] failed to get block header for previous block %s: %s", blockHash.String(), block.MsgBlock().Header.PrevBlock, err)
		return 0, errors.NewProcessingError("failed to get block header for previous block %s", block.MsgBlock().Header.PrevBlock, err)
	}

	if block.Height() <= 0 {
		// block height was not set in the msgBlock, set it from our lookup
		blockHeight := previousBlockHeaderMeta.Height + 1

		blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeight)
		if err != nil {
			return 0, errors.NewProcessingError("failed to convert block height to int32", err)
		}

		block.SetHeight(blockHeightInt32)

		return blockHeight, nil
	}

	// check whether the block height being reported is the correct block height
	previousBlockHeightInt32, err := safeconversion.Uint32ToInt32(previousBlockHeaderMeta.Height + 1)
	if err != nil {
		return 0, errors.NewProcessingError("failed to convert block height to int32", err)
	}

	if block.Height() != previousBlockHeightInt32 {
		return 0, errors.NewBlockInvalidError("block height %d is not the correct height for block %s, expected %d", block.Height(), blockHash, previousBlockHeaderMeta.Height+1)
	}

	blockHeight, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		return 0, errors.NewProcessingError("failed to convert block height to uint32", err)
	}

	return blockHeight, nil
}
