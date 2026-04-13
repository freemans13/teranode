package postgres

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusDirectCreateDuration prometheus.Histogram
	prometheusDirectSpendDuration  prometheus.Histogram
	prometheusDirectMinedDuration  prometheus.Histogram
	prometheusDirectConflicts      prometheus.Counter
	prometheusDirectCreate         prometheus.Counter
	prometheusDirectSpend          prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(doInitPrometheusMetrics)
}

func doInitPrometheusMetrics() {
	prometheusDirectCreateDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "queue_utxo",
		Name:      "create_duration_seconds",
		Help:      "Duration of Create() calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectSpendDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "queue_utxo",
		Name:      "spend_duration_seconds",
		Help:      "Duration of Spend() per-input calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectMinedDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "queue_utxo",
		Name:      "mined_duration_seconds",
		Help:      "Duration of SetMinedMulti() calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "queue_utxo",
		Name:      "spend_conflicts_total",
		Help:      "Total number of spend conflicts detected",
	})

	prometheusDirectCreate = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "queue_utxo",
		Name:      "create_total",
		Help:      "Total number of Create calls",
	})

	prometheusDirectSpend = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "queue_utxo",
		Name:      "spend_total",
		Help:      "Total number of Spend calls",
	})
}
