//go:build aerospike

package aerospike_test

import (
	"context"
	crand "crypto/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	utxotesthelper "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// newStoreForBatchTests spins up a testcontainers Aerospike instance and returns
// a ready *aerostore.Store plus a cleanup function. It reuses initAerospike from
// container_helper_test.go which is compiled into this test binary.
//
// The returned store has an in-memory external store and all test-safe defaults.
func newStoreForBatchTests(t *testing.T) (*aerostore.Store, func()) {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, cleanup := initAerospike(t, tSettings, logger)

	return store, cleanup
}

// seedParentRecord creates a minimal tx with the given number of outputs,
// stores it via s.Create, and returns the tx's hash. The record is stored
// as an unmined transaction (blockHeight=0, no block info) so that the test
// is not sensitive to block-height assignment logic.
func seedParentRecord(t *testing.T, s *aerostore.Store, numOutputs int) *chainhash.Hash {
	t.Helper()

	tx, err := utxotesthelper.CreateTransaction(uint64(numOutputs)) //nolint:gosec
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.Create(ctx, tx, 0)
	require.NoError(t, err)

	return tx.TxIDChainHash()
}

// seedParentTx creates a minimal tx with the given number of outputs, stores
// it via s.Create, and returns the full *bt.Tx so callers can compute
// UTXOHashes and build *utxo.Spend entries.
func seedParentTx(t *testing.T, s *aerostore.Store, numOutputs int) *bt.Tx {
	t.Helper()

	tx, err := utxotesthelper.CreateTransaction(uint64(numOutputs)) //nolint:gosec
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.Create(ctx, tx, 0)
	require.NoError(t, err)

	return tx
}

// randSpendingHash returns a random 32-byte chainhash suitable for use as a
// spending-tx hash in test SpendingData.
func randSpendingHash(t *testing.T) *chainhash.Hash {
	t.Helper()
	b := make([]byte, 32)
	_, err := crand.Read(b)
	require.NoError(t, err)
	h, err := chainhash.NewHash(b)
	require.NoError(t, err)
	return h
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestBatchGetParents_AllPresent(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	parents := []*chainhash.Hash{
		seedParentRecord(t, s, 1),
		seedParentRecord(t, s, 2),
		seedParentRecord(t, s, 3),
	}

	hashes := make([][]byte, len(parents))
	for i, p := range parents {
		b := p.CloneBytes()
		hashes[i] = b
	}

	got, missing, err := s.BatchGetParents(ctx, hashes)
	require.NoError(t, err)
	require.Empty(t, missing)
	require.Len(t, got, 3)

	for _, p := range parents {
		var key [32]byte
		copy(key[:], p[:])
		_, ok := got[key]
		require.True(t, ok, "expected parent %x to be present in result", p[:])
	}
}

func TestBatchGetParents_SomeMissing(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	present := seedParentRecord(t, s, 1)
	var missingHash chainhash.Hash
	missingHash[0] = 0xff
	missingHash[1] = 0xee

	got, missing, err := s.BatchGetParents(ctx, [][]byte{present[:], missingHash[:]})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, missing, 1)
	require.Equal(t, missingHash[:], missing[0])
}

func TestBatchGetParents_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	got, missing, err := s.BatchGetParents(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, missing)
}

// ---------------------------------------------------------------------------
// BatchSpend tests
// ---------------------------------------------------------------------------

func TestBatchSpend_AllSucceed(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// Seed a parent with 2 outputs and build proper spend entries.
	parentTx := seedParentTx(t, s, 2)
	parentHash := parentTx.TxIDChainHash()
	spendingHash := randSpendingHash(t)

	sp0Hash, err := util.UTXOHashFromOutput(parentHash, parentTx.Outputs[0], 0)
	require.NoError(t, err)
	sp1Hash, err := util.UTXOHashFromOutput(parentHash, parentTx.Outputs[1], 1)
	require.NoError(t, err)

	spends := []*utxo.Spend{
		{
			TxID:         parentHash,
			Vout:         0,
			UTXOHash:     sp0Hash,
			SpendingData: spendpkg.NewSpendingData(spendingHash, 0),
		},
		{
			TxID:         parentHash,
			Vout:         1,
			UTXOHash:     sp1Hash,
			SpendingData: spendpkg.NewSpendingData(spendingHash, 1),
		},
	}

	results, err := s.BatchSpend(ctx, spends, 1)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err)
	require.NoError(t, results[1].Err)
}

func TestBatchSpend_PerRecordErrorMapsToInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	// Seed a parent with 1 output, then spend it with a wrong UTXOHash — the
	// Lua will return UTXO_HASH_MISMATCH which surfaces as results[0].Err.
	parentTx := seedParentTx(t, s, 1)
	parentHash := parentTx.TxIDChainHash()
	spendingHash := randSpendingHash(t)

	// Build a deliberately wrong UTXOHash (all zeros).
	wrongHash, err := chainhash.NewHash(make([]byte, 32))
	require.NoError(t, err)

	spends := []*utxo.Spend{
		{
			TxID:         parentHash,
			Vout:         0,
			UTXOHash:     wrongHash,
			SpendingData: spendpkg.NewSpendingData(spendingHash, 0),
		},
	}

	results, err := s.BatchSpend(ctx, spends, 1)
	require.NoError(t, err, "whole-call err must be nil for per-record Lua failures")
	require.Len(t, results, 1)
	require.Error(t, results[0].Err, "expected per-record error for hash mismatch")
}

func TestBatchSpend_EmptyInput(t *testing.T) {
	ctx := context.Background()
	s, cleanup := newStoreForBatchTests(t)
	defer cleanup()

	got, err := s.BatchSpend(ctx, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got)
}
