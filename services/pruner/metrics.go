package pruner

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prunerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pruner_duration_seconds",
			Help:    "Duration of pruner operations in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~17 minutes
		},
		[]string{"operation"}, // "preserve_parents" or "dah_pruner"
	)

	prunerSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_skipped_total",
			Help: "Number of pruner operations skipped",
		},
		[]string{"reason"}, // "not_running", "no_new_height", "already_in_progress"
	)

	prunerProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pruner_processed_total",
			Help: "Total number of successful pruner operations",
		},
	)

	prunerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_errors_total",
			Help: "Total number of pruner errors",
		},
		[]string{"operation"}, // "preserve_parents", "dah_pruner", "poll"
	)
)

func initPrometheusMetrics() {
	// Metrics are auto-registered via promauto
	// This function exists for consistency with other services
}
