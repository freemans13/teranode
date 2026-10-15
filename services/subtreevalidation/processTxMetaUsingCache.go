// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// probeTaskMinKeys is the smallest number of lookups a probe task is given:
// adjacent bucket ranges are merged until they reach it, so a small subtree is
// one task rather than one goroutine and pool hand-off per range. A full-size
// subtree (~1M txs, ~4k per range) still gets one task per range.
const probeTaskMinKeys = 2048

// probePool is a server-wide pool of cache-probe worker slots, sized by
// subtreevalidation_processTxMetaUsingCache_Concurrency and created on first
// use so struct-literal Servers (tests) get it too.
type probePool struct {
	once     sync.Once
	slots    chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32 // highest inFlight seen; reported by tests
}

func (p *probePool) slotsFor(u *Server) chan struct{} {
	p.once.Do(func() {
		n := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency
		if n < 1 {
			// Same fallback as util.SafeSetLimit for the per-call limit this replaced.
			u.logger.Warnf("[processTxMetaUsingCache] subtreevalidation_processTxMetaUsingCache_Concurrency=%d, using %d (NumCPU)", n, runtime.NumCPU())
			n = runtime.NumCPU()
		}

		p.slots = make(chan struct{}, n)
	})

	return p.slots
}

func (p *probePool) enter() {
	n := p.inFlight.Add(1)

	for {
		peak := p.peak.Load()
		if n <= peak || p.peak.CompareAndSwap(peak, n) {
			return
		}
	}
}

func (p *probePool) leave() {
	p.inFlight.Add(-1)
}

// processTxMetaUsingCache looks up a peer-announced subtree's transactions in
// the txmeta cache. See probeTxMetaCache; peer subtrees share one pool of
// probe workers.
func (u *Server) processTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem, failFast bool) (int, error) {
	return u.probeTxMetaCache(ctx, &u.peerProbePool, txHashes, txMetaSlice, failFast)
}

// processBlockTxMetaUsingCache is processTxMetaUsingCache for block validation's
// cache pre-check. Block validation, here and in ValidateSubtreeInternal for a
// block's subtrees (ValidateSubtree.FromBlock), uses a separate pool of the same
// size, so a burst of peer subtrees cannot delay a block, while the total number
// of probe workers stays bounded.
func (u *Server) processBlockTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem) (int, error) {
	return u.probeTxMetaCache(ctx, &u.blockProbePool, txHashes, txMetaSlice, false)
}

// probeTxMetaCache attempts to retrieve transaction metadata from the cache for a batch
// of transactions.
//
// The lookups are grouped by cache bucket range (txmetacache.ProbeRangeOf) and each task
// (one or more adjacent ranges) is probed with one GetMetaCachedMulti call, so every bucket's
// read lock is taken once per task rather than once per transaction. Tasks run on probe workers
// whose number is limited across ALL concurrent calls drawing from the same pool
// (subtreevalidation_processTxMetaUsingCache_Concurrency): when hundreds of subtrees arrive at
// once they share the workers instead of each starting its own, which kept thousands of
// goroutines contending with the Kafka txmeta handler for the cache's bucket locks and the CPU.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - pool: The server-wide worker pool to draw from
//   - txHashes: Slice of transaction hashes to process
//   - txMetaSlice: Pre-allocated slice to store retrieved metadata
//   - failFast: If true, fails quickly when missing transaction threshold is exceeded
//
// Returns:
//   - int: Number of transactions missing from cache
//   - error: Any error encountered during processing
//
// The function will return a ThresholdExceededError if failFast is true and
// the number of missing transactions exceeds the configured threshold.
func (u *Server) probeTxMetaCache(ctx context.Context, pool *probePool, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem, failFast bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewProcessingError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTxMetaUsingCache")
	defer deferFn()

	cache, ok := u.utxoStore.(*txmetacache.TxMetaCache)
	if !ok {
		u.logger.Warnf("[processTxMetaUsingCache] txMetaStore is not a cached implementation")
		return len(txHashes), nil // As there was no cache, we "missed" all the txHashes
	}

	missingTxThreshold := uint32(u.settings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold) // nolint: gosec // G115: configured threshold is non-negative

	// Hash each lookup once (the cache reuses it) and group the indices still
	// to look up by bucket range with a counting sort.
	keyHashes := make([]uint64, len(txHashes))
	pending := make([]bool, len(txHashes))

	var counts [txmetacache.ProbeRanges]int

	for i := range txHashes {
		if txMetaSlice[i].isSet || txHashes[i].Equal(*subtree.CoinbasePlaceholderHash) {
			continue
		}

		pending[i] = true
		keyHashes[i] = txmetacache.KeyHash(&txHashes[i])
		counts[txmetacache.ProbeRangeOf(keyHashes[i])]++
	}

	var next [txmetacache.ProbeRanges]int

	offset := 0
	for r, n := range counts {
		next[r] = offset
		offset += n
	}

	starts := next
	order := make([]int32, offset)

	for i := range txHashes {
		if !pending[i] {
			continue
		}

		r := txmetacache.ProbeRangeOf(keyHashes[i])
		order[next[r]] = int32(i) // nolint: gosec // G115: a subtree batch is far below 2^31 transactions
		next[r]++
	}

	var (
		missed  atomic.Uint32
		aborted bool // not every task was started
	)

	g, gCtx := errgroup.WithContext(ctx)
	slots := pool.slotsFor(u)

dispatch:
	for _, task := range probeTasks(counts[:], probeTaskMinKeys) {
		// Ranges task[0]..task[1]-1 are contiguous in order.
		indices := order[starts[task[0]]:next[task[1]-1]]

		// Take a shared probe slot before starting the goroutine, so the number
		// of probe goroutines is bounded across every concurrent call.
		select {
		case slots <- struct{}{}:
		case <-gCtx.Done():
			aborted = true
			break dispatch
		}

		g.Go(func() error {
			pool.enter()

			defer func() {
				pool.leave()
				<-slots
			}()

			return probeCacheRange(gCtx, cache, txHashes, keyHashes, txMetaSlice, indices, &missed, failFast, missingTxThreshold)
		})
	}

	err := g.Wait()
	if err == nil && aborted {
		// Cancelled while waiting for a slot: the unstarted tasks were neither found
		// nor counted as missed.
		err = errors.NewContextCanceledError("[processTxMetaUsingCache context cancelled]", ctx.Err())
	}

	if err != nil {
		return int(missed.Load()), errors.NewProcessingError("error processing txMeta using cache", err)
	}

	return int(missed.Load()), nil
}

// probeTasks splits the per-range lookup counts into tasks of adjacent ranges,
// [first, last+1), each with at least minKeys lookups where possible (the last
// task takes whatever is left). Empty ranges are absorbed into a neighbour.
func probeTasks(counts []int, minKeys int) [][2]int {
	var (
		tasks [][2]int
		first = -1
		keys  int
	)

	for r, n := range counts {
		if first < 0 {
			first = r
		}

		keys += n

		if keys >= minKeys {
			tasks = append(tasks, [2]int{first, r + 1})
			first, keys = -1, 0
		}
	}

	if first >= 0 && keys > 0 {
		tasks = append(tasks, [2]int{first, len(counts)})
	}

	return tasks
}

// probeCacheRange looks up the given indices, which fall into one task's
// bucket ranges, with a single batched cache read and fills txMetaSlice.
func probeCacheRange(ctx context.Context, cache *txmetacache.TxMetaCache, txHashes []chainhash.Hash, keyHashes []uint64,
	txMetaSlice []metaSliceItem, indices []int32, missed *atomic.Uint32, failFast bool, missingTxThreshold uint32) error {
	if ctx.Err() != nil {
		return errors.NewContextCanceledError("[processTxMetaUsingCache context cancelled]", ctx.Err())
	}

	hashes := make([]chainhash.Hash, len(indices))
	hashKeys := make([]uint64, len(indices))

	for j, i := range indices {
		hashes[j] = txHashes[i]
		hashKeys[j] = keyHashes[i]
	}

	found := make([]*meta.Data, len(indices))
	cache.GetMetaCachedMulti(ctx, hashes, hashKeys, found)

	for j, i := range indices {
		txMeta := found[j]

		// Defensively reject a partial cache entry: a non-coinbase tx with no
		// parent tx hashes is incomplete (TxInpoints was never populated) and
		// would later fail subtree-meta serialization and wedge the block. Treat
		// it as a miss so the store fallback — which can reconstruct inpoints from
		// the tx inputs — resolves it and self-heals the cache.
		if txMeta != nil && (txMeta.IsCoinbase || len(txMeta.TxInpoints.ParentTxHashes) > 0) {
			txMetaSlice[i] = metaSliceItem{
				fee:         txMeta.Fee,
				sizeInBytes: txMeta.SizeInBytes,
				coinbase:    txMeta.IsCoinbase,
				conflicting: txMeta.Conflicting,
				creating:    txMeta.Creating,
				isSet:       true,
				txInpoints:  txMeta.TxInpoints,
			}

			continue
		}

		newMissed := missed.Add(1)

		if failFast && missingTxThreshold > 0 && newMissed > missingTxThreshold {
			return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
		}
	}

	return nil
}
