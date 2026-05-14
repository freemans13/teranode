//go:build aerospike

package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
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
