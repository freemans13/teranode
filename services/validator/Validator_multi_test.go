package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidateMulti_EmptySlice tests ValidateMulti with empty transaction slice
func TestValidateMulti_EmptySlice(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	mockUtxoStore := &utxo.MockUtxostore{}

	v, err := New(context.Background(), logger, tSettings, mockUtxoStore, nil, nil, nil, nil)
	require.NoError(t, err)

	result, err := v.ValidateMulti(context.Background(), []*bt.Tx{}, 100, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 0, len(result.Results))
}

// TestOrganizeTxsByLevel_EmptySlice tests level organization with empty slice
func TestOrganizeTxsByLevel_EmptySlice(t *testing.T) {
	levels, err := organizeTxsByLevelOrdered(context.Background(), []*bt.Tx{})
	require.NoError(t, err)
	require.NotNil(t, levels)
	require.Equal(t, 0, len(levels))
}

// TestBuildParentMap_EmptySlice tests parent map construction with empty slice
func TestBuildParentMap_EmptySlice(t *testing.T) {
	parentMap := buildParentMap([]txWithIndex{})
	require.Nil(t, parentMap)
}
