package adaptivefetch

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics_ModeGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics("test-service", reg)

	m.setMode(ModePessimistic)
	require.InDelta(t, 0.0, testutil.ToFloat64(m.modeGauge.WithLabelValues("test-service")), 0.0001)

	m.setMode(ModeOptimistic)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.modeGauge.WithLabelValues("test-service")), 0.0001)
}

func TestMetrics_TransitionsCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics("test-service", reg)

	m.recordTransition(ModePessimistic, ModeOptimistic)
	m.recordTransition(ModePessimistic, ModeOptimistic)
	m.recordTransition(ModeOptimistic, ModePessimistic)

	require.InDelta(t, 2.0, testutil.ToFloat64(
		m.transitions.WithLabelValues("test-service", "pessimistic", "optimistic")), 0.0001)
	require.InDelta(t, 1.0, testutil.ToFloat64(
		m.transitions.WithLabelValues("test-service", "optimistic", "pessimistic")), 0.0001)
}

func TestMetrics_RegisteredNamesMatchSpec(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics("test-service", reg)
	// Seed one real observation so the histogram appears in Gather() output.
	// (Histograms only show up after at least one Observe call — no pre-seeding in production.)
	m.hitRate.WithLabelValues("test-service").Observe(0.5)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	joined := strings.Join(names, ",")

	require.Contains(t, joined, "teranode_adaptive_fetch_mode")
	require.Contains(t, joined, "teranode_adaptive_fetch_hit_rate")
	require.Contains(t, joined, "teranode_adaptive_fetch_missing_fetches_total")
	require.Contains(t, joined, "teranode_adaptive_fetch_mode_transitions_total")
}
