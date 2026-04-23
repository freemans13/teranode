package adaptivefetch

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metrics groups the prometheus collectors used by State.
//
// All collectors are registered with the supplied registry at construction
// time so the caller chooses between the global promauto registry (via
// prometheus.DefaultRegisterer) and a private one (for tests).
type metrics struct {
	modeGauge   *prometheus.GaugeVec
	hitRate     *prometheus.HistogramVec
	missesTotal *prometheus.CounterVec
	transitions *prometheus.CounterVec
}

func newMetrics(serviceName string, reg prometheus.Registerer) *metrics {
	m := &metrics{
		modeGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "mode",
				Help:      "Current adaptive fetch mode (0=pessimistic, 1=optimistic), by service.",
			},
			[]string{"service"},
		),
		hitRate: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "hit_rate",
				Help:      "Local-UTXO-store hit rate per observation (LocalHits/TotalTxs), by service.",
				Buckets:   []float64{0.0, 0.5, 0.9, 0.95, 0.99, 0.995, 1.0},
			},
			[]string{"service"},
		),
		missesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "missing_fetches_total",
				Help:      "Running total of transactions recovered via processMissingTransactions, by service.",
			},
			[]string{"service"},
		),
		transitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "mode_transitions_total",
				Help:      "Count of mode transitions, by service and direction.",
			},
			[]string{"service", "from", "to"},
		),
	}
	reg.MustRegister(m.modeGauge, m.hitRate, m.missesTotal, m.transitions)
	// Initialise all series for this service so dashboards show a line even before first Record
	// and so registry.Gather returns all metric families immediately.
	m.modeGauge.WithLabelValues(serviceName).Set(0)
	m.hitRate.WithLabelValues(serviceName).Observe(0)
	m.missesTotal.WithLabelValues(serviceName).Add(0)
	m.transitions.WithLabelValues(serviceName, ModePessimistic.String(), ModeOptimistic.String()).Add(0)
	m.transitions.WithLabelValues(serviceName, ModeOptimistic.String(), ModePessimistic.String()).Add(0)
	return m
}

// setMode is a test-only helper that sets the gauge for "test-service"
// without going through State. Production code uses State.emitMode which
// bakes in the serviceName the State was constructed with.
func (m *metrics) setMode(mode Mode) {
	val := 0.0
	if mode == ModeOptimistic {
		val = 1.0
	}
	m.modeGauge.WithLabelValues("test-service").Set(val)
}

// recordTransition is a test-only helper matching setMode — asserts the
// transitions counter is wired correctly from a test's point of view.
func (m *metrics) recordTransition(from, to Mode) {
	m.transitions.WithLabelValues("test-service", from.String(), to.String()).Inc()
}
