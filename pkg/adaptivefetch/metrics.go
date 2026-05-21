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

// registerOrReuse registers c with reg. If the metric is already registered,
// it returns the previously-registered collector of the same type. This allows
// production code to call New() more than once against prometheus.DefaultRegisterer
// (e.g. in tests that call server.New() in multiple subtests) without panicking.
func registerOrReuse[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	if err := reg.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(C) //nolint:forcetypeassert // same type was registered
		}
		panic(err) // unexpected error — surface it
	}
	return c
}

func newMetrics(serviceName string, reg prometheus.Registerer) *metrics {
	m := &metrics{
		modeGauge: registerOrReuse(reg, prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "mode",
				Help:      "Current adaptive fetch mode (0=pessimistic, 1=optimistic), by service.",
			},
			[]string{"service"},
		)),
		hitRate: registerOrReuse(reg, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "hit_rate",
				Help:      "Local-UTXO-store hit rate per observation (LocalHits/TotalTxs), by service.",
				Buckets:   []float64{0.0, 0.5, 0.9, 0.95, 0.99, 0.995, 1.0},
			},
			[]string{"service"},
		)),
		missesTotal: registerOrReuse(reg, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "missing_fetches_total",
				Help:      "Running total of transactions recovered/fetched individually after an optimistic-mode skip, by service.",
			},
			[]string{"service"},
		)),
		transitions: registerOrReuse(reg, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "mode_transitions_total",
				Help:      "Count of mode transitions, by service and direction.",
			},
			[]string{"service", "from", "to"},
		)),
	}
	// Initialise all series for this service so dashboards show a line even before first Record.
	// Note: hitRate (histogram) is intentionally NOT pre-seeded — Observe() always records a real
	// data point, so pre-seeding would poison the histogram with a fake 0% hit-rate entry.
	// Gauges (Set) and counters (Add) are genuine no-ops at zero, so they are safe to initialise.
	m.modeGauge.WithLabelValues(serviceName).Set(0)
	m.missesTotal.WithLabelValues(serviceName).Add(0)
	m.transitions.WithLabelValues(serviceName, ModePessimistic.String(), ModeOptimistic.String()).Add(0)
	m.transitions.WithLabelValues(serviceName, ModeOptimistic.String(), ModePessimistic.String()).Add(0)
	return m
}
