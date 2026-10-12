package logger

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A wrapped sql store keeps the one optional capability it has, the pruner provider, and does
// not gain the stamp interface it lacks: a wrapper that claimed Stamper for it would hand the
// stamp worker a store with no stamp.
func TestNewOffersTheWrappedStoresCapabilitiesOnly(t *testing.T) {
	s, base := newLoggedSQLiteStore(t)

	_, baseProvides := base.(pruner.PrunerServiceProvider)
	require.True(t, baseProvides, "the sql store is a pruner provider")

	provider, ok := s.(pruner.PrunerServiceProvider)
	require.True(t, ok, "the logging wrapper hid the sql store's pruner provider")

	svc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)

	_, isStamper := s.(pruner.Stamper)
	require.False(t, isStamper, "the logging wrapper claimed a stamp interface the sql store does not have")

	// A store with neither capability gets the plain wrapper.
	require.IsType(t, &Store{}, New(t.Context(), ulogger.TestLogger{}, &MockStore{}))
}
