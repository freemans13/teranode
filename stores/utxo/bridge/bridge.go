package bridge

import (
	"encoding/binary"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/dolthub/swiss"
)

// BlockTxSet holds the set of truncated tx hashes for a single block.
type BlockTxSet struct {
	blockHash   chainhash.Hash
	blockHeight uint32
	blockID     uint32
	txHashes    *swiss.Map[uint64, struct{}]
}

// MinedTxBridge is an in-memory bridge that holds per-block tx hash sets so block
// validation can check whether a tx is mined without waiting for slow DB writes.
type MinedTxBridge struct {
	mu        sync.RWMutex
	blocks    map[chainhash.Hash]*BlockTxSet
	maxBlocks int
}

// NewMinedTxBridge creates a new MinedTxBridge. maxBlocks is a soft capacity hint.
func NewMinedTxBridge(maxBlocks int) *MinedTxBridge {
	return &MinedTxBridge{
		blocks:    make(map[chainhash.Hash]*BlockTxSet, maxBlocks),
		maxBlocks: maxBlocks,
	}
}

// truncateHash returns the first 8 bytes of a chainhash.Hash as a uint64 using little-endian byte order.
func truncateHash(hash *chainhash.Hash) uint64 {
	return binary.LittleEndian.Uint64(hash[:8])
}

// AddBlock builds a Swiss table from the given tx hashes and stores it in the bridge keyed by blockHash.
func (b *MinedTxBridge) AddBlock(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, txHashes []*chainhash.Hash) {
	m := swiss.NewMap[uint64, struct{}](uint32(len(txHashes)))
	for _, h := range txHashes {
		m.Put(truncateHash(h), struct{}{})
	}

	set := &BlockTxSet{
		blockHash:   blockHash,
		blockHeight: blockHeight,
		blockID:     blockID,
		txHashes:    m,
	}

	b.mu.Lock()
	b.blocks[blockHash] = set
	b.mu.Unlock()
}

// RemoveBlock removes the block entry for the given blockHash from the bridge.
func (b *MinedTxBridge) RemoveBlock(blockHash chainhash.Hash) {
	b.mu.Lock()
	delete(b.blocks, blockHash)
	b.mu.Unlock()
}

// HasBlock reports whether the bridge contains an entry for the given blockHash.
func (b *MinedTxBridge) HasBlock(blockHash chainhash.Hash) bool {
	b.mu.RLock()
	_, ok := b.blocks[blockHash]
	b.mu.RUnlock()
	return ok
}

// BlockCount returns the number of blocks currently held in the bridge.
func (b *MinedTxBridge) BlockCount() int {
	b.mu.RLock()
	n := len(b.blocks)
	b.mu.RUnlock()
	return n
}

// GetBlockIDsForTx scans all block sets and returns the blockIDs of any blocks whose tx
// set contains the given tx hash (by 8-byte truncated key). Returns nil if not found.
func (b *MinedTxBridge) GetBlockIDsForTx(txHash *chainhash.Hash) []uint32 {
	key := truncateHash(txHash)

	b.mu.RLock()
	defer b.mu.RUnlock()

	var ids []uint32
	for _, set := range b.blocks {
		if _, found := set.txHashes.Get(key); found {
			ids = append(ids, set.blockID)
		}
	}
	return ids
}
