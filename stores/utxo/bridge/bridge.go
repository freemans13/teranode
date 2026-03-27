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
	logger    Logger
}

// Logger is a minimal logging interface for bridge warnings.
type Logger interface {
	Warnf(format string, args ...interface{})
}

// NewMinedTxBridge creates a new MinedTxBridge. maxBlocks is a soft capacity hint;
// when exceeded a warning is logged but processing continues.
func NewMinedTxBridge(maxBlocks int, logger ...Logger) *MinedTxBridge {
	b := &MinedTxBridge{
		blocks:    make(map[chainhash.Hash]*BlockTxSet, maxBlocks),
		maxBlocks: maxBlocks,
	}
	if len(logger) > 0 {
		b.logger = logger[0]
	}
	return b
}

// truncateHash returns the first 8 bytes of a chainhash.Hash as a uint64 using little-endian byte order.
// Using 64-bit keys instead of full 32-byte hashes saves ~24 bytes per entry in the Swiss table.
// False positives from birthday-paradox collisions are harmless: they only add an extra blockID
// to the merge set, which the authoritative UTXO store data will correct once the background
// DB write completes and the bridge entry is removed.
func truncateHash(hash *chainhash.Hash) uint64 {
	return binary.LittleEndian.Uint64(hash[:8])
}

// AddBlock builds a Swiss table from the given tx hashes and stores it in the bridge keyed by blockHash.
func (b *MinedTxBridge) AddBlock(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, txHashes []*chainhash.Hash) {
	m := swiss.NewMap[uint64, struct{}](uint32(len(txHashes)))
	for _, h := range txHashes {
		m.Put(truncateHash(h), struct{}{})
	}

	b.storeBlock(blockHash, blockID, blockHeight, m)
}

// AddBlockFromIterator builds a Swiss table by iterating over tx hashes via a callback,
// avoiding intermediate slice allocations. The callback calls visit for each tx hash.
func (b *MinedTxBridge) AddBlockFromIterator(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, count int, iter func(visit func(*chainhash.Hash))) {
	m := swiss.NewMap[uint64, struct{}](uint32(count))
	iter(func(h *chainhash.Hash) {
		m.Put(truncateHash(h), struct{}{})
	})

	b.storeBlock(blockHash, blockID, blockHeight, m)
}

// storeBlock inserts a block set into the bridge and logs a warning if the soft limit is exceeded.
func (b *MinedTxBridge) storeBlock(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, m *swiss.Map[uint64, struct{}]) {
	set := &BlockTxSet{
		blockHash:   blockHash,
		blockHeight: blockHeight,
		blockID:     blockID,
		txHashes:    m,
	}

	b.mu.Lock()
	b.blocks[blockHash] = set
	count := len(b.blocks)
	b.mu.Unlock()

	if count > b.maxBlocks && b.logger != nil {
		b.logger.Warnf("[MinedTxBridge] bridge holds %d blocks, exceeds soft limit of %d — background SetTxMined may be falling behind", count, b.maxBlocks)
	}
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
