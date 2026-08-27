package utxoset

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
)

// TxBodyPartitionBlocks is the width of one body window. Reclaim drops whole windows, so
// retention is granular to this. It matches the spend journal's width deliberately: the two
// are reclaimed by the same pruner session and there is no reason for them to disagree.
const TxBodyPartitionBlocks = 48

// DefaultTxBodyRetentionBlocks is how long the serialized bytes are kept.
//
// 288, which is global_blockHeightRetention, and it is not a number this store chose. It is
// the horizon at which subtree files are deleted, past which the node physically cannot
// un-mine a block because the un-mine path warns and skips a missing subtree file. Choosing
// 144 instead would not create a new failure class, it would narrow a range that already
// works and make depths 145 to 287 fail quietly inside a band where the rest of the node
// still functions.
const DefaultTxBodyRetentionBlocks = 288

// ensureTxBodyPartition creates the body window covering height, if absent.
//
// It MUST be called before the caller opens its transaction, for the same reason
// ensureSpendJournalPartition must: the DDL needs its own pool connection, and taking one
// while holding a transaction borrowed from the same pool is a nested acquire. At
// pool_max_conns concurrent writers every connection ends up held by a transaction waiting
// for a connection, and that deadlock has no timeout.
func (s *Store) ensureTxBodyPartition(ctx context.Context, height uint32) error {
	window := height / TxBodyPartitionBlocks

	// Only touch the catalog when the window actually changes. The cache holds window+1 so
	// its zero value means "nothing cached yet" rather than "window 0 is already created",
	// which would make window 0 permanently unreachable on a fresh store.
	if s.bodyWindow.Load() == window+1 {
		return nil
	}

	s.bodyDDL.Lock()
	defer s.bodyDDL.Unlock()

	if s.bodyWindow.Load() == window+1 {
		return nil
	}

	lo := window * TxBodyPartitionBlocks
	hi := lo + TxBodyPartitionBlocks

	// raw_tx is forced out of line on each partition. Keep it for the genuinely large tail,
	// but NOT for the reason the design doc gave: an inline body is not rewritten by an
	// update of another column, because postgres writes only what changed working in from
	// both ends of the row. Do not set toast_tuple_target to force more of it out -- at 128
	// the toaster does not stop at raw_tx, it keeps going and externalises txid too.
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS tx_body_w%[1]d PARTITION OF tx_body
  FOR VALUES FROM (%[2]d) TO (%[3]d);
ALTER TABLE tx_body_w%[1]d ALTER COLUMN raw_tx SET STORAGE EXTERNAL;`, window, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return errors.NewStorageError("[utxoset] create tx_body window %d", window, err)
	}

	// Only cache once the DDL has actually succeeded, or a transient failure becomes
	// permanent: every later write would see a hit, skip the retry, and fail on a partition
	// that was never created.
	s.bodyWindow.Store(window + 1)

	return nil
}
