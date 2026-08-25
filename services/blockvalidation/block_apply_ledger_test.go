package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// ledgerStore is a nullstore that also keeps a block-apply ledger.
type ledgerStore struct {
	utxo.Store

	completed map[chainhash.Hash]bool
	claimed   map[chainhash.Hash]bool
	failClaim bool
}

func newLedgerStore() *ledgerStore {
	return &ledgerStore{
		Store:     &nullstore.NullStore{},
		completed: map[chainhash.Hash]bool{},
		claimed:   map[chainhash.Hash]bool{},
	}
}

func (l *ledgerStore) BeginBlockApply(_ context.Context, h *chainhash.Hash, _ uint32) (bool, error) {
	if l.failClaim {
		return false, errors.NewStorageError("claim exploded")
	}

	if l.completed[*h] {
		return true, nil
	}

	l.claimed[*h] = true

	return false, nil
}

func (l *ledgerStore) CompleteBlockApply(_ context.Context, h *chainhash.Hash) error {
	l.completed[*h] = true
	return nil
}

func testBlock(t *testing.T, height uint32) *model.Block {
	t.Helper()

	b := &model.Block{Height: height, Header: &model.BlockHeader{
		Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{},
		Timestamp: 1, Bits: model.NBit{}, Nonce: uint32(height),
	}}

	return b
}

// TestClaimBlockApplySkipsAReplayedBlock is the point of the whole ledger.
//
// The utxoset store's create has no ON CONFLICT, because its key is a deliberately
// non-unique 96-bit prefix, so re-applying a block inserts every output a second time as
// an independently spendable row. Replay is routine rather than exotic: catchup, a
// restart mid-window, and the post-restart unrequested-block storm all re-offer blocks.
func TestClaimBlockApplySkipsAReplayedBlock(t *testing.T) {
	store := newLedgerStore()
	u := &BlockValidation{logger: ulogger.TestLogger{}, utxoStore: store}
	ctx := context.Background()
	block := testBlock(t, 500)

	skip, err := u.claimBlockApply(ctx, block)
	require.NoError(t, err)
	require.False(t, skip, "a block never seen before must be applied")

	require.NoError(t, u.completeBlockApply(ctx, block))

	skip, err = u.claimBlockApply(ctx, block)
	require.NoError(t, err)
	require.True(t, skip, "a completed block must not be applied a second time")
}

// TestClaimBlockApplyReappliesAnUnfinishedBlock guards the failure that would be worse
// than the one the ledger fixes. A block that died part-way must come back, or its
// outputs never exist and every later block spending them fails forever.
func TestClaimBlockApplyReappliesAnUnfinishedBlock(t *testing.T) {
	store := newLedgerStore()
	u := &BlockValidation{logger: ulogger.TestLogger{}, utxoStore: store}
	ctx := context.Background()
	block := testBlock(t, 501)

	skip, err := u.claimBlockApply(ctx, block)
	require.NoError(t, err)
	require.False(t, skip)

	// no completeBlockApply: simulate a crash part-way through
	skip, err = u.claimBlockApply(ctx, block)
	require.NoError(t, err)
	require.False(t, skip, "an unfinished block must re-apply, not be silently skipped")
}

// TestClaimBlockApplyIsANoOpForStoresWithoutALedger keeps aerospike and the generic SQL
// store completely unaffected. Their create IS idempotent, so they need no ledger, and a
// change to shared block-validation code must not alter what they do.
func TestClaimBlockApplyIsANoOpForStoresWithoutALedger(t *testing.T) {
	u := &BlockValidation{logger: ulogger.TestLogger{}, utxoStore: &nullstore.NullStore{}}
	ctx := context.Background()
	block := testBlock(t, 502)

	skip, err := u.claimBlockApply(ctx, block)
	require.NoError(t, err)
	require.False(t, skip, "a store without a ledger never skips")
	require.NoError(t, u.completeBlockApply(ctx, block), "and completing is a no-op")
}

// TestClaimBlockApplyFailsClosed: if the ledger cannot be consulted we must NOT guess.
// Applying anyway risks duplicating outputs; the block should fail and be retried.
func TestClaimBlockApplyFailsClosed(t *testing.T) {
	store := newLedgerStore()
	store.failClaim = true
	u := &BlockValidation{logger: ulogger.TestLogger{}, utxoStore: store}

	_, err := u.claimBlockApply(context.Background(), testBlock(t, 503))
	require.Error(t, err, "an unreadable ledger must stop the block, not be assumed empty")
}
