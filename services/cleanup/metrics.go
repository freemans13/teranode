package cleanup

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	cleanupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cleanup_duration_seconds",
			Help:    "Duration of cleanup operations in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~17 minutes
		},
		[]string{"operation"}, // "preserve_parents" or "dah_cleanup"
	)

	cleanupSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cleanup_skipped_total",
			Help: "Number of cleanup operations skipped",
		},
		[]string{"reason"}, // "not_running", "no_new_height", "already_in_progress"
	)

	cleanupProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cleanup_processed_total",
			Help: "Total number of successful cleanup operations",
		},
	)

	cleanupErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cleanup_errors_total",
			Help: "Total number of cleanup errors",
		},
		[]string{"operation"}, // "preserve_parents", "dah_cleanup", "poll"
	)
)

func initPrometheusMetrics() {
	// Metrics are auto-registered via promauto
	// This function exists for consistency with other services
}
