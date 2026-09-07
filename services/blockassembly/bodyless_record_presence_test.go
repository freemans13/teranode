package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// bodylessUtxoStore is a UTXO store that holds every record but hands back none
// of their serialized bodies, which is the steady state of a store running with
// utxostore_skipTxBodyBelowCheckpoint on (and of any store whose body window has
// aged out). It answers a missing transaction exactly as the wrapped store does,
// so the two cases stay distinguishable.
type bodylessUtxoStore struct {
	utxoStore.Store
}

func (s *bodylessUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	data, err := s.Store.Get(ctx, hash, f...)
	if err != nil || data == nil {
		return data, err
	}

	// Copy before clearing: the wrapped store may hand out a record other
	// readers hold.
	stripped := *data
	stripped.Tx = nil

	return &stripped, nil
}

// TestCanonicalCoinbaseAt_BodylessRecordIsPresent pins the fix for the Hetzner
// mainnet incident. canonicalCoinbaseAt decided presence on txMeta.Tx, so the
// first restart after utxostore_skipTxBodyBelowCheckpoint was enabled read every
// coinbase created below the checkpoint as missing, the walk-back never found a
// good floor, and startup escalated MANUAL INTERVENTION REQUIRED against a UTXO
// set that was entirely intact.
func TestCanonicalCoinbaseAt_BodylessRecordIsPresent(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	// height 1: canonical block carries cb1, and the store holds cb1's record.
	cb1 := coinbaseTxForHeader(t, blockHeader1)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader1, cb1)

	_, _, err := items.utxoStore.SpendAndCreate(ctx, cb1, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	// height 2: canonical block carries cb2, whose record was never created.
	cb2 := coinbaseTxForHeader(t, blockHeader2)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader2, cb2)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore}

	// The record exists with no body at all: present.
	present, blk, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 1)
	require.NoError(t, err)
	require.True(t, present, "a coinbase whose body was never written is still present")
	require.NotNil(t, blk)

	// Proof the double really is body-less, so the assertion above is not
	// passing for the wrong reason.
	stripped, err := items.blockAssembler.utxoStore.Get(ctx, cb1.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, stripped)
	require.Nil(t, stripped.Tx)

	// No record at all: absent, and still no error.
	absent, blk2, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 2)
	require.NoError(t, err)
	require.False(t, absent, "a coinbase with no record is missing")
	require.NotNil(t, blk2)

	// And the store really did answer that one with a not-found.
	_, err = items.blockAssembler.utxoStore.Get(ctx, cb2.TxIDChainHash(), fields.Tx)
	require.True(t, errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound))
}

// TestStartupCoinbaseDivergenceCheck_BodylessCoinbasesRaiseNoAlarm is the same
// fix seen from the startup scan that fired on mainnet: a chain whose coinbases
// are all present but all body-less must boot silently, with no recovery run and
// no MANUAL INTERVENTION line.
func TestStartupCoinbaseDivergenceCheck_BodylessCoinbasesRaiseNoAlarm(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

	headers := buildCanonicalChain(ctx, t, items, 4)
	for h := uint32(1); h <= 4; h++ {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[3], 4)

	logger := &capturingLogger{}
	items.blockAssembler.logger = logger
	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore}

	items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx)

	require.False(t, logger.sawWarn("canonical coinbase missing"), "no coinbase is missing")
	require.False(t, logger.sawError("MANUAL INTERVENTION REQUIRED"))
	require.False(t, logger.sawError("startup recovery failed"))
}

// TestUnlockConflictParents_BodylessWinnerStillUnlocksItsParents covers the
// second reader that decided on the body: healStaleConflictIntent's step-5
// unlock read the winner's parents out of txMeta.Tx.Inputs, so a body-less
// winner contributed no parents and was skipped in silence, leaving them locked
// with nothing left to unlock them. The parents live on the identity record as
// stored inpoints, which is what it reads now.
func TestUnlockConflictParents_BodylessWinnerStillUnlocksItsParents(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	parent := coinbaseTxForHeader(t, blockHeader1)

	_, _, err := items.utxoStore.SpendAndCreate(ctx, parent, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	child.AddOutput(&bt.Output{Satoshis: parent.Outputs[0].Satoshis - 1, LockingScript: parent.Outputs[0].LockingScript})

	_, _, err = items.utxoStore.SpendAndCreate(ctx, child, 2, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	// Lock the parent, the state a forward ProcessConflicting leaves at step 2.
	require.NoError(t, items.utxoStore.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	locked, err := items.utxoStore.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, locked.Locked)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore}

	require.NoError(t, items.blockAssembler.unlockConflictParents(ctx, []chainhash.Hash{*child.TxIDChainHash()}))

	unlocked, err := items.utxoStore.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.False(t, unlocked.Locked, "the winner's parent must be unlocked even when the winner has no body")
}

// TestUnlockConflictParents_MissingWinnerIsSkipped keeps the other half of the
// contract: a winner whose record is gone entirely contributes no parents and is
// not an error, so the heal still completes.
func TestUnlockConflictParents_MissingWinnerIsSkipped(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	never := coinbaseTxForHeader(t, blockHeader2)

	_, err := items.utxoStore.Get(ctx, never.TxIDChainHash())
	require.Error(t, err)

	require.NoError(t, items.blockAssembler.unlockConflictParents(ctx, []chainhash.Hash{*never.TxIDChainHash()}))
}
