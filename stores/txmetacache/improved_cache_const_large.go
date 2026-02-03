//go:build !smalltxmetacache && !testtxmetacache

// Package txmetacache provides large-scale cache configuration for production environments.
// Uses build tags to select appropriate cache size for high-throughput production deployments.
package txmetacache

import "github.com/bsv-blockchain/teranode/ulogger"

// BucketsCount defines the number of cache buckets (8,192 for production environments).
const BucketsCount = 8 * 1024

// ChunkSize defines the memory chunk size (maxValueSizeKB * 2 * 1024 = ~4MB per chunk).
const ChunkSize = maxValueSizeKB * 2 * 1024

// MaxGenWindow defines the multi-generation retention window for unallocated buckets.
// Higher values improve retention (entries survive more wrap cycles) but increase map heap memory.
// Production setting: 10 generations = ~95% retention, ~2.4GB map heap per instance.
const MaxGenWindow = 10

// LogCacheSize logs which cache configuration is active for diagnostics.
func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_large.go")
}
