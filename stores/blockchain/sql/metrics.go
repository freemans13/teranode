package sql

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// prometheusOnMainChainWriteFailures counts on_main_chain writes whose failure the
	// store swallowed, by the path that swallowed it. Each one leaves the column
	// inconsistent until the background repair lands.
	prometheusOnMainChainWriteFailures *prometheus.CounterVec
	// prometheusOnMainChainRepairPending is 1 while the store owes an on_main_chain
	// repair and every flag reader is on its flag-free walk, and 0 otherwise.
	prometheusOnMainChainRepairPending prometheus.Gauge

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusOnMainChainWriteFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockchain_sql",
			Name:      "on_main_chain_write_failures",
			Help:      "on_main_chain writes whose failure was swallowed, by path (invalidate_block|revalidate_block|startup_rebuild|store_block_fork_clear|store_block_best_lookup); each one is repaired in the background",
		},
		[]string{"path"},
	)

	prometheusOnMainChainRepairPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockchain_sql",
			Name:      "on_main_chain_repair_pending",
			Help:      "1 while a failed on_main_chain write still owes a repair and flag readers use the flag-free walk, 0 otherwise",
		},
	)
}
