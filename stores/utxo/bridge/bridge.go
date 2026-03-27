package bridge

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/dolthub/swiss"
)

// BlockTxSet holds the set of tx hashes for a single block.
type BlockTxSet struct {
	blockHash   chainhash.Hash
	blockHeight uint32
	blockID     uint32
	txHashes    *swiss.Map[chainhash.Hash, struct{}]
}

// MinedTxBridge is an in-memory bridge that holds per-block tx hash sets so block
// validation can check whether a tx is mined without waiting for slow DB writes.
type MinedTxBridge struct {
	mu               sync.RWMutex
	blocks           map[chainhash.Hash]*BlockTxSet
	warningThreshold int
	logger           Logger
}

// Logger is a minimal logging interface for bridge warnings.
type Logger interface {
	Warnf(format string, args ...interface{})
}

// NewMinedTxBridge creates a new MinedTxBridge. warningThreshold controls when a
// warning is logged if the bridge holds too many blocks — it is NOT a hard limit.
// The bridge never evicts blocks or rejects new ones.
func NewMinedTxBridge(warningThreshold int, logger ...Logger) *MinedTxBridge {
	b := &MinedTxBridge{
		blocks:           make(map[chainhash.Hash]*BlockTxSet),
		warningThreshold: warningThreshold,
	}
	if len(logger) > 0 {
		b.logger = logger[0]
	}
	return b
}

// AddBlock builds a Swiss table from the given tx hashes and stores it in the bridge keyed by blockHash.
func (b *MinedTxBridge) AddBlock(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, txHashes []*chainhash.Hash) {
	m := swiss.NewMap[chainhash.Hash, struct{}](uint32(len(txHashes)))
	for _, h := range txHashes {
		m.Put(*h, struct{}{})
	}

	b.storeBlock(blockHash, blockID, blockHeight, m)
}

// AddBlockFromIterator builds a Swiss table by iterating over tx hashes via a callback,
// avoiding intermediate slice allocations. The callback calls visit for each tx hash.
func (b *MinedTxBridge) AddBlockFromIterator(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, count int, iter func(visit func(*chainhash.Hash))) {
	if count < 0 {
		count = 0
	}
	m := swiss.NewMap[chainhash.Hash, struct{}](uint32(count))
	iter(func(h *chainhash.Hash) {
		m.Put(*h, struct{}{})
	})

	b.storeBlock(blockHash, blockID, blockHeight, m)
}

// storeBlock inserts a block set into the bridge and logs a warning if the soft limit is exceeded.
func (b *MinedTxBridge) storeBlock(blockHash chainhash.Hash, blockID uint32, blockHeight uint32, m *swiss.Map[chainhash.Hash, struct{}]) {
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

	if count > b.warningThreshold && b.logger != nil {
		b.logger.Warnf("[MinedTxBridge] bridge holds %d blocks, exceeds warning threshold of %d — background SetTxMined may be falling behind", count, b.warningThreshold)
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
// set contains the given tx hash. Returns nil if not found.
func (b *MinedTxBridge) GetBlockIDsForTx(txHash *chainhash.Hash) []uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var ids []uint32
	for _, set := range b.blocks {
		if _, found := set.txHashes.Get(*txHash); found {
			ids = append(ids, set.blockID)
		}
	}
	return ids
}
