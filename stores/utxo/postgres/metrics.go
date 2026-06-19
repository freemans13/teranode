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

	// proc-mode DAH sweep (runDAHCursorProc)
	prometheusDAHSweepCallDuration prometheus.Histogram
	prometheusDAHSweepRowsStamped  prometheus.Counter
	prometheusDAHSweepWatermarkLag prometheus.Gauge
	prometheusDAHSweepErrors       prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(doInitPrometheusMetrics)
}

func doInitPrometheusMetrics() {
	prometheusDirectCreateDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "create_duration_seconds",
		Help:      "Duration of Create() calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectSpendDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "spend_duration_seconds",
		Help:      "Duration of Spend() per-input calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectMinedDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "mined_duration_seconds",
		Help:      "Duration of SetMinedMulti() calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "spend_conflicts_total",
		Help:      "Total number of spend conflicts detected",
	})

	prometheusDirectCreate = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "create_total",
		Help:      "Total number of Create calls",
	})

	prometheusDirectSpend = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "spend_total",
		Help:      "Total number of Spend calls",
	})

	prometheusDAHSweepCallDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_call_duration_seconds",
		Help:      "Duration of a proc-mode dah_sweep_batch() CALL in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDAHSweepRowsStamped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_rows_stamped_total",
		Help:      "Total rows stamped by the proc-mode DAH sweep",
	})

	prometheusDAHSweepWatermarkLag = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_watermark_lag",
		Help:      "Heights between the DAH watermark and the safe tip (backlog)",
	})

	prometheusDAHSweepErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_errors_total",
		Help:      "Total proc-mode DAH sweep CALL errors",
	})
}
