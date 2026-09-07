package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newChunkTestService(t *testing.T) *Service {
	t.Helper()
	ensurePrometheusMetrics()

	return &Service{
		logger: ulogger.NewVerboseTestLogger(t),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipDeletions: true,
			},
		},
		namespace:            "test",
		set:                  "test",
		utxoBatchSize:        128,
		defensiveEnabled:     false,
		fieldTxID:            fields.TxID.String(),
		fieldUtxos:           fields.Utxos.String(),
		fieldInputs:          fields.Inputs.String(),
		fieldDeletedChildren: fields.DeletedChildren.String(),
		fieldExternal:        fields.External.String(),
		fieldDeleteAtHeight:  fields.DeleteAtHeight.String(),
		fieldTotalExtraRecs:  fields.TotalExtraRecs.String(),
		fieldUnminedSince:    fields.UnminedSince.String(),
		fieldBlockHeights:    fields.BlockHeights.String(),
	}
}

func TestProcessRecordChunk_EmptyInputs(t *testing.T) {
	ctx := context.Background()
	svc := newChunkTestService(t)

	var childTxID chainhash.Hash
	for i := range childTxID {
		childTxID[i] = 0x77
	}

	key, keyErr := aerospike.NewKey(svc.namespace, svc.set, childTxID[:])
	require.NoError(t, keyErr)

	// Empty inputs (e.g. coinbase) — getTxInputsFromBins returns an empty
	// slice, so no parent loop runs.
	chunk := []*aerospike.Result{
		{
			Record: &aerospike.Record{
				Key: key,
				Bins: aerospike.BinMap{
					svc.fieldTxID:     childTxID.CloneBytes(),
					svc.fieldInputs:   []interface{}{},
					svc.fieldExternal: false,
				},
			},
		},
	}

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 0, skipped)

}
