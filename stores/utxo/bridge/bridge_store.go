// Package bridge provides a utxo.Store wrapper that transparently merges
// in-memory bridge blockIDs into every read, closing the window between
// SetMinedMulti writes and full DB persistence.
package bridge

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// BridgeStore wraps a utxo.Store and merges blockIDs from the MinedTxBridge
// into every read so that callers see mined state immediately, before DB
// persistence has completed.
type BridgeStore struct {
	inner   utxo.Store
	bridge  *MinedTxBridge
	enabled bool
}

// NewBridgeStore returns a utxo.Store that transparently merges bridge blockIDs
// into reads. When enabled is false all methods are pure passthroughs with zero
// bridge overhead. If bridge is nil and enabled is true, enabled is forced to
// false to prevent nil-pointer panics.
func NewBridgeStore(inner utxo.Store, bridge *MinedTxBridge, enabled bool) utxo.Store {
	if bridge == nil {
		enabled = false
	}
	return &BridgeStore{
		inner:   inner,
		bridge:  bridge,
		enabled: enabled,
	}
}

// wantsBlockIDs reports whether fields.BlockIDs is in the requested field list.
// An empty list means "all fields", so it returns true in that case too.
func wantsBlockIDs(f []fields.FieldName) bool {
	if len(f) == 0 {
		return true
	}

	for _, name := range f {
		if name == fields.BlockIDs {
			return true
		}
	}

	return false
}

// mergeBlockRefs appends bridge block references into data's BlockIDs, BlockHeights,
// and SubtreeIdxs slices, keeping all three aligned. Deduplicates by blockID.
func mergeBlockRefs(data *meta.Data, refs []BlockRef) {
	if len(refs) == 0 || data == nil {
		return
	}

	// Build a set of existing blockIDs for deduplication
	seen := make(map[uint32]struct{}, len(data.BlockIDs)+len(refs))
	for _, id := range data.BlockIDs {
		seen[id] = struct{}{}
	}

	for _, ref := range refs {
		if _, exists := seen[ref.BlockID]; exists {
			continue
		}
		seen[ref.BlockID] = struct{}{}
		data.BlockIDs = append(data.BlockIDs, ref.BlockID)
		data.BlockHeights = append(data.BlockHeights, ref.BlockHeight)
		data.SubtreeIdxs = append(data.SubtreeIdxs, ref.SubtreeIdx)
	}
}

// ---- intercepted methods ---------------------------------------------------

// Get delegates to the inner store and, when enabled and BlockIDs is requested,
// merges bridge blockIDs into the returned meta.Data.
func (s *BridgeStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	data, err := s.inner.Get(ctx, hash, f...)
	if err != nil || !s.enabled || !wantsBlockIDs(f) {
		return data, err
	}

	if data != nil {
		mergeBlockRefs(data, s.bridge.GetBlockRefsForTx(hash))
	}

	return data, nil
}

// GetMeta delegates to the inner store and, when enabled, merges bridge
// blockIDs into the caller-supplied meta.Data.
func (s *BridgeStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	err := s.inner.GetMeta(ctx, hash, data)
	if err != nil || !s.enabled {
		return err
	}

	mergeBlockRefs(data, s.bridge.GetBlockRefsForTx(hash))

	return nil
}

// BatchDecorate delegates to the inner store and, when enabled and BlockIDs is
// requested, merges bridge blockIDs into each item's Data.
func (s *BridgeStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	err := s.inner.BatchDecorate(ctx, items, f...)
	if err != nil || !s.enabled || !wantsBlockIDs(f) {
		return err
	}

	for _, item := range items {
		if item.Data == nil {
			continue
		}

		mergeBlockRefs(item.Data, s.bridge.GetBlockRefsForTx(&item.Hash))
	}

	return nil
}

// SetMinedMulti delegates to the inner store and, when enabled, merges bridge
// blockIDs into the returned blockID map.
func (s *BridgeStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	blockIDsMap, err := s.inner.SetMinedMulti(ctx, hashes, info)
	if err != nil || !s.enabled {
		return blockIDsMap, err
	}

	if blockIDsMap == nil {
		blockIDsMap = make(map[chainhash.Hash][]uint32, len(hashes))
	}

	for _, hash := range hashes {
		refs := s.bridge.GetBlockRefsForTx(hash)
		if len(refs) == 0 {
			continue
		}

		existing := blockIDsMap[*hash]
		// SetMinedMulti returns only blockIDs (no heights/subtrees), so extract IDs from refs
		bridgeIDs := make([]uint32, 0, len(refs))
		for _, ref := range refs {
			bridgeIDs = append(bridgeIDs, ref.BlockID)
		}
		// Deduplicate against existing
		seen := make(map[uint32]struct{}, len(existing)+len(bridgeIDs))
		merged := make([]uint32, 0, len(existing)+len(bridgeIDs))
		for _, id := range existing {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				merged = append(merged, id)
			}
		}
		for _, id := range bridgeIDs {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				merged = append(merged, id)
			}
		}
		blockIDsMap[*hash] = merged
	}

	return blockIDsMap, nil
}

// ---- passthrough methods ---------------------------------------------------

func (s *BridgeStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return s.inner.Health(ctx, checkLiveness)
}

func (s *BridgeStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return s.inner.Create(ctx, tx, blockHeight, opts...)
}

func (s *BridgeStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	return s.inner.Delete(ctx, hash)
}

func (s *BridgeStore) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return s.inner.GetSpend(ctx, spend)
}

func (s *BridgeStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return s.inner.Spend(ctx, tx, blockHeight, ignoreFlags...)
}

func (s *BridgeStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	return s.inner.Unspend(ctx, spends, flagAsLocked...)
}

func (s *BridgeStore) GetUnminedTxIterator(fullScan bool) (utxo.UnminedTxIterator, error) {
	return s.inner.GetUnminedTxIterator(fullScan)
}

func (s *BridgeStore) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return s.inner.GetPrunableUnminedTxIterator(cutoffBlockHeight)
}

func (s *BridgeStore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	return s.inner.QueryOldUnminedTransactions(ctx, cutoffBlockHeight)
}

func (s *BridgeStore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	return s.inner.PreserveTransactions(ctx, txIDs, preserveUntilHeight)
}

func (s *BridgeStore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	return s.inner.ProcessExpiredPreservations(ctx, currentHeight)
}

func (s *BridgeStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return s.inner.PreviousOutputsDecorate(ctx, tx)
}

func (s *BridgeStore) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	return s.inner.BatchPreviousOutputsDecorate(ctx, txs)
}

func (s *BridgeStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return s.inner.FreezeUTXOs(ctx, spends, tSettings)
}

func (s *BridgeStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return s.inner.UnFreezeUTXOs(ctx, spends, tSettings)
}

func (s *BridgeStore) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	return s.inner.ReAssignUTXO(ctx, utxoSpend, newUtxo, tSettings)
}

func (s *BridgeStore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return s.inner.GetCounterConflicting(ctx, txHash)
}

func (s *BridgeStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return s.inner.GetConflictingChildren(ctx, txHash)
}

func (s *BridgeStore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return s.inner.SetConflicting(ctx, txHashes, value)
}

func (s *BridgeStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	return s.inner.SetLocked(ctx, txHashes, value)
}

func (s *BridgeStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	return s.inner.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
}

func (s *BridgeStore) SetBlockHeight(height uint32) error {
	return s.inner.SetBlockHeight(height)
}

func (s *BridgeStore) GetBlockHeight() uint32 {
	return s.inner.GetBlockHeight()
}

func (s *BridgeStore) SetMedianBlockTime(height uint32) error {
	return s.inner.SetMedianBlockTime(height)
}

func (s *BridgeStore) GetMedianBlockTime() uint32 {
	return s.inner.GetMedianBlockTime()
}

func (s *BridgeStore) GetBlockState() utxo.BlockState {
	return s.inner.GetBlockState()
}
