package txmetacache

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
)

// BenchmarkCacheComparison compares Clock vs Unallocated with truly concurrent Set/Get operations
// Dedicated reader and writer goroutines run simultaneously
func BenchmarkCacheComparison(b *testing.B) {
	cacheSize := 1 * 1024 * 1024 * 1024 // 1GB
	entrySize := 177

	bucketTypes := []struct {
		name       string
		bucketType BucketType
	}{
		{"Unallocated", Unallocated},
		{"Clock", Clock},
	}

	// Test scenarios: different read/write ratios
	scenarios := []struct {
		name       string
		writeRatio float64 // 0.0 = all reads, 1.0 = all writes
	}{
		{"100_Writes_0_Reads", 1.0},
		{"75_Writes_25_Reads", 0.75},
		{"50_Writes_50_Reads", 0.5},
		{"25_Writes_75_Reads", 0.25},
		{"0_Writes_100_Reads", 0.0},
	}

	for _, bt := range bucketTypes {
		for _, scenario := range scenarios {
			name := fmt.Sprintf("%s_%s", bt.name, scenario.name)
			b.Run(name, func(b *testing.B) {
				cache, err := New(cacheSize, bt.bucketType)
				if err != nil {
					b.Fatal(err)
				}
				defer cache.Reset()

				// Pre-populate cache with some entries for read tests
				capacity := cacheSize / entrySize
				prepopulate := capacity / 2
				for i := 0; i < prepopulate; i++ {
					key := []byte(fmt.Sprintf("key_%09d", i))
					value := make([]byte, entrySize-len(key)-4)
					binary.BigEndian.PutUint64(value[0:8], uint64(i))
					_ = cache.Set(key, value)
				}

				b.ResetTimer()

				// Calculate how many ops should be writes vs reads
				totalOps := b.N
				writeOps := int(float64(totalOps) * scenario.writeRatio)
				readOps := totalOps - writeOps

				// Use 8 total workers, split between readers and writers
				totalWorkers := 8
				writeWorkers := int(float64(totalWorkers) * scenario.writeRatio)
				if writeWorkers == 0 && writeOps > 0 {
					writeWorkers = 1
				}
				readWorkers := totalWorkers - writeWorkers
				if readWorkers == 0 && readOps > 0 {
					readWorkers = 1
				}

				var wg sync.WaitGroup

				// Start writer goroutines (truly concurrent writes)
				if writeWorkers > 0 {
					opsPerWriter := writeOps / writeWorkers
					wg.Add(writeWorkers)
					for w := 0; w < writeWorkers; w++ {
						go func(workerID int) {
							defer wg.Done()
							for i := 0; i < opsPerWriter; i++ {
								opID := workerID*opsPerWriter + i
								key := []byte(fmt.Sprintf("key_%09d", prepopulate+opID))
								value := make([]byte, entrySize-len(key)-4)
								binary.BigEndian.PutUint64(value[0:8], uint64(opID))
								_ = cache.Set(key, value)
							}
						}(w)
					}
				}

				// Start reader goroutines (truly concurrent reads, running simultaneously with writes)
				if readWorkers > 0 {
					opsPerReader := readOps / readWorkers
					wg.Add(readWorkers)
					for r := 0; r < readWorkers; r++ {
						go func(workerID int) {
							defer wg.Done()
							for i := 0; i < opsPerReader; i++ {
								opID := workerID*opsPerReader + i
								readID := opID % prepopulate
								key := []byte(fmt.Sprintf("key_%09d", readID))
								var dst []byte
								_ = cache.Get(&dst, key)
							}
						}(r)
					}
				}

				wg.Wait()

				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
			})
		}
	}
}

// BenchmarkCacheSetOnly compares pure write performance with concurrent writers
func BenchmarkCacheSetOnly(b *testing.B) {
	cacheSize := 1 * 1024 * 1024 * 1024 // 1GB
	entrySize := 177

	bucketTypes := []struct {
		name       string
		bucketType BucketType
	}{
		{"Unallocated", Unallocated},
		{"Clock", Clock},
	}

	for _, bt := range bucketTypes {
		b.Run(bt.name, func(b *testing.B) {
			cache, err := New(cacheSize, bt.bucketType)
			if err != nil {
				b.Fatal(err)
			}
			defer cache.Reset()

			b.ResetTimer()

			// Use 8 concurrent writers
			numWorkers := 8
			opsPerWorker := b.N / numWorkers

			var wg sync.WaitGroup
			wg.Add(numWorkers)

			for w := 0; w < numWorkers; w++ {
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						opID := workerID*opsPerWorker + i
						key := []byte(fmt.Sprintf("key_%09d", opID))
						value := make([]byte, entrySize-len(key)-4)
						_ = cache.Set(key, value)
					}
				}(w)
			}

			wg.Wait()

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
		})
	}
}

// BenchmarkCacheGetOnly compares pure read performance with concurrent readers
func BenchmarkCacheGetOnly(b *testing.B) {
	cacheSize := 1 * 1024 * 1024 * 1024 // 1GB
	entrySize := 177

	bucketTypes := []struct {
		name       string
		bucketType BucketType
	}{
		{"Unallocated", Unallocated},
		{"Clock", Clock},
	}

	for _, bt := range bucketTypes {
		b.Run(bt.name, func(b *testing.B) {
			cache, err := New(cacheSize, bt.bucketType)
			if err != nil {
				b.Fatal(err)
			}
			defer cache.Reset()

			// Pre-populate with entries
			capacity := cacheSize / entrySize
			prepopulate := capacity / 2
			for i := 0; i < prepopulate; i++ {
				key := []byte(fmt.Sprintf("key_%09d", i))
				value := make([]byte, entrySize-len(key)-4)
				_ = cache.Set(key, value)
			}

			b.ResetTimer()

			// Use 8 concurrent readers
			numWorkers := 8
			opsPerWorker := b.N / numWorkers

			var wg sync.WaitGroup
			wg.Add(numWorkers)

			for w := 0; w < numWorkers; w++ {
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						opID := workerID*opsPerWorker + i
						readID := opID % prepopulate
						key := []byte(fmt.Sprintf("key_%09d", readID))
						var dst []byte
						_ = cache.Get(&dst, key)
					}
				}(w)
			}

			wg.Wait()

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
		})
	}
}

// BenchmarkCacheEviction compares eviction performance when cache is full with concurrent writers
func BenchmarkCacheEviction(b *testing.B) {
	cacheSize := 256 * 1024 * 1024 // 256MB for faster testing
	entrySize := 177

	bucketTypes := []struct {
		name       string
		bucketType BucketType
	}{
		{"Unallocated", Unallocated},
		{"Clock", Clock},
	}

	for _, bt := range bucketTypes {
		b.Run(bt.name, func(b *testing.B) {
			cache, err := New(cacheSize, bt.bucketType)
			if err != nil {
				b.Fatal(err)
			}
			defer cache.Reset()

			// Fill cache to capacity to trigger eviction
			capacity := cacheSize / entrySize
			for i := 0; i < capacity; i++ {
				key := []byte(fmt.Sprintf("key_%09d", i))
				value := make([]byte, entrySize-len(key)-4)
				_ = cache.Set(key, value)
			}

			b.ResetTimer()

			// Use 8 concurrent writers to benchmark insertions that trigger eviction
			numWorkers := 8
			opsPerWorker := b.N / numWorkers

			var wg sync.WaitGroup
			wg.Add(numWorkers)

			for w := 0; w < numWorkers; w++ {
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						opID := workerID*opsPerWorker + i
						key := []byte(fmt.Sprintf("key_%09d", capacity+opID))
						value := make([]byte, entrySize-len(key)-4)
						_ = cache.Set(key, value)
					}
				}(w)
			}

			wg.Wait()

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
		})
	}
}

// BenchmarkCacheConcurrency tests concurrent access scalability with dedicated reader/writer goroutines
func BenchmarkCacheConcurrency(b *testing.B) {
	cacheSize := 1 * 1024 * 1024 * 1024 // 1GB
	entrySize := 177

	bucketTypes := []struct {
		name       string
		bucketType BucketType
	}{
		{"Unallocated", Unallocated},
		{"Clock", Clock},
	}

	workerCounts := []int{1, 2, 4, 8, 16, 32}

	for _, bt := range bucketTypes {
		for _, totalWorkers := range workerCounts {
			name := fmt.Sprintf("%s_%dWorkers", bt.name, totalWorkers)
			b.Run(name, func(b *testing.B) {
				cache, err := New(cacheSize, bt.bucketType)
				if err != nil {
					b.Fatal(err)
				}
				defer cache.Reset()

				// Pre-populate
				capacity := cacheSize / entrySize
				prepopulate := capacity / 4
				for i := 0; i < prepopulate; i++ {
					key := []byte(fmt.Sprintf("key_%09d", i))
					value := make([]byte, entrySize-len(key)-4)
					_ = cache.Set(key, value)
				}

				b.ResetTimer()

				// Split workers 50/50 between readers and writers
				writeWorkers := totalWorkers / 2
				readWorkers := totalWorkers - writeWorkers
				if writeWorkers == 0 {
					writeWorkers = 1
					readWorkers = totalWorkers - 1
				}

				writeOps := b.N / 2
				readOps := b.N - writeOps

				var wg sync.WaitGroup

				// Start writer goroutines (concurrent writes)
				if writeWorkers > 0 && writeOps > 0 {
					opsPerWriter := writeOps / writeWorkers
					wg.Add(writeWorkers)
					for w := 0; w < writeWorkers; w++ {
						go func(workerID int) {
							defer wg.Done()
							for i := 0; i < opsPerWriter; i++ {
								opID := workerID*opsPerWriter + i
								key := []byte(fmt.Sprintf("key_%09d", prepopulate+opID))
								value := make([]byte, entrySize-len(key)-4)
								_ = cache.Set(key, value)
							}
						}(w)
					}
				}

				// Start reader goroutines (concurrent reads, running simultaneously with writes)
				if readWorkers > 0 && readOps > 0 {
					opsPerReader := readOps / readWorkers
					wg.Add(readWorkers)
					for r := 0; r < readWorkers; r++ {
						go func(workerID int) {
							defer wg.Done()
							for i := 0; i < opsPerReader; i++ {
								opID := workerID*opsPerReader + i
								readID := opID % prepopulate
								key := []byte(fmt.Sprintf("key_%09d", readID))
								var dst []byte
								_ = cache.Get(&dst, key)
							}
						}(r)
					}
				}

				wg.Wait()

				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
			})
		}
	}
}

// BenchmarkCacheSweepThroughput validates Clock performance during active sweeping
// Tests concurrent reads/writes while cache oscillates 90-100% to verify:
// - Throughput remains consistent during sweep cycles
// - No performance degradation from Clock hand advancement
// - System can handle get, set, and sweep simultaneously
func BenchmarkCacheSweepThroughput(b *testing.B) {
	cacheSize := 256 * 1024 * 1024 // 256MB for reasonable benchmark time
	entrySize := 177
	capacity := cacheSize / entrySize

	bucketTypes := []struct {
		name       string
		bucketType BucketType
	}{
		{"Unallocated", Unallocated},
		{"Clock", Clock},
	}

	for _, bt := range bucketTypes {
		b.Run(bt.name, func(b *testing.B) {
			cache, err := New(cacheSize, bt.bucketType)
			if err != nil {
				b.Fatal(err)
			}
			defer cache.Reset()

			// Phase 1: Fill to 90% capacity (no eviction yet)
			fillTo90 := int(float64(capacity) * 0.90)
			for i := 0; i < fillTo90; i++ {
				key := []byte(fmt.Sprintf("key_%09d", i))
				value := make([]byte, entrySize-len(key)-4)
				binary.BigEndian.PutUint64(value[0:8], uint64(i))
				_ = cache.Set(key, value)
			}

			// Verify we're at 90%
			var stats Stats
			cache.UpdateStats(&stats)
			utilizationBefore := float64(stats.EntriesCount) / float64(capacity)
			b.Logf("Initial utilization: %.1f%% (%d entries)", utilizationBefore*100, stats.EntriesCount)

			b.ResetTimer()

			// Phase 2: Concurrent read/write workload that pushes to 100% and oscillates
			// This triggers Clock sweeping (or ring buffer wrap for Unallocated)
			const numReaders = 4
			const numWriters = 4
			const opsPerWorker = 100000

			var wg sync.WaitGroup

			// Start readers (reading existing entries)
			wg.Add(numReaders)
			for r := 0; r < numReaders; r++ {
				go func(readerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						readID := (readerID*opsPerWorker + i) % fillTo90
						key := []byte(fmt.Sprintf("key_%09d", readID))
						var dst []byte
						_ = cache.Get(&dst, key)
					}
				}(r)
			}

			// Start writers (inserting new entries to trigger sweep)
			wg.Add(numWriters)
			for w := 0; w < numWriters; w++ {
				go func(writerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						writeID := fillTo90 + writerID*opsPerWorker + i
						key := []byte(fmt.Sprintf("key_%09d", writeID))
						value := make([]byte, entrySize-len(key)-4)
						binary.BigEndian.PutUint64(value[0:8], uint64(writeID))
						_ = cache.Set(key, value)
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()

			// Measure final state
			cache.UpdateStats(&stats)
			utilizationAfter := float64(stats.EntriesCount) / float64(capacity)

			totalOps := (numReaders + numWriters) * opsPerWorker
			throughput := float64(totalOps) / b.Elapsed().Seconds() / 1e6

			b.Logf("Final utilization: %.1f%% (%d entries)", utilizationAfter*100, stats.EntriesCount)
			b.Logf("Total ops: %d (reads: %d, writes: %d)", totalOps, numReaders*opsPerWorker, numWriters*opsPerWorker)
			b.Logf("Throughput during sweep: %.2f Mops/s", throughput)

			// Report metrics
			b.ReportMetric(throughput, "Mops/s")
			b.ReportMetric(utilizationAfter*100, "utilization_%")
		})
	}
}
