// Package repository provides access to blockchain data storage and retrieval operations.
package repository

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for tracking repository operations.
var (
	// prometheusAssetSubtreeDataCreated tracks on-demand subtreeData file creation events
	prometheusAssetSubtreeDataCreated *prometheus.CounterVec
)

// prometheusMetricsInitOnce ensures metrics are initialized exactly once
var prometheusMetricsInitOnce sync.Once

// initPrometheusMetrics safely initializes all Prometheus metrics using sync.Once
// to ensure thread-safe single initialization.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics creates and registers all Prometheus metrics.
func _initPrometheusMetrics() {
	prometheusAssetSubtreeDataCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset_repository",
			Name:      "subtree_data_created_total",
			Help:      "Number of on-demand subtreeData file creation events",
		},
		[]string{
			"result", // success or error
			"source", // on_demand_created, file_existed, creation_failed
		},
	)
}
