package utxoset

import (
	"context"
	"testing"

	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// With logging=true every caller holds the utxoset store through stores/utxo/logger. The stamp
// worker, the pruner service and the rewind tool's preflight all find this store's extra
// capabilities by type assertion, so the wrapper has to offer them, answered by this store.
func TestLoggingWrapperOffersTheStampAndPrunerCapabilities(t *testing.T) {
	s, ctx := newTestStore(t)

	wrapped := storelogger.New(ctx, ulogger.TestLogger{}, s)

	// The rewind preflight's interface, copied because cmd/rewindblockchain keeps it unexported.
	preflight, ok := wrapped.(interface {
		Floors(ctx context.Context) (pruner.StampFloors, error)
		StampDepth() uint32
	})
	require.True(t, ok, "the logging wrapper hid the stamp boundary the rewind preflight reads")

	want, err := s.Floors(ctx)
	require.NoError(t, err)

	got, err := preflight.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, s.StampDepth(), preflight.StampDepth())

	st, ok := wrapped.(pruner.Stamper)
	require.True(t, ok, "the logging wrapper hid pruner.Stamper, so the stamp worker cannot drive this store")
	require.Equal(t, s.WindowBlocks(), st.WindowBlocks())

	provider, ok := wrapped.(pruner.PrunerServiceProvider)
	require.True(t, ok, "the logging wrapper hid the pruner provider, so the pruner service never prunes this store")

	svc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)
}
