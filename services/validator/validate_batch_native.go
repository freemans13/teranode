package validator

import (
	"context"
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// batchUtxoStore is the minimal UTXO store surface ValidateBatch needs.
// The concrete *aerospike.Store satisfies it; tests use a stub.
type batchUtxoStore interface {
	BatchGetParents(ctx context.Context, parentHashes [][]byte) (map[[32]byte]*aerospike.ParentRecord, [][]byte, error)
	BatchSpend(ctx context.Context, spends []*utxo.Spend, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]aerospike.SpendResult, error)
	BatchCreate(ctx context.Context, txs []*bt.Tx, blockHeight uint32, lockedTrue bool) ([]aerospike.CreateResult, error)
	BatchSetLocked(ctx context.Context, txHashes [][]byte, locked bool) ([]aerospike.SetLockedResult, error)
}

// validateBatchNative runs the six-phase native pipeline. Phase wiring is
// added incrementally — this version implements Phase A only.
// Phases B–F are stubs that leave results untouched until Tasks 12–16.
func (v *Validator) validateBatchNative(
	ctx context.Context,
	txs []*bt.Tx,
	blockHeight uint32,
	opts ...Option,
) ([]ValidationResult, error) {
	results := make([]ValidationResult, len(txs))
	for i, tx := range txs {
		results[i].TxHash = *tx.TxIDChainHash()
	}

	store, ok := v.getBatchUtxoStore()
	if !ok {
		// Configured UTXO store does not implement the batch methods.
		// Fall back to per-tx fan-out so the flag-on path stays additive.
		return v.validateBatchFallback(ctx, txs, blockHeight, opts...)
	}

	// Phase A: fetch all unique parent hashes in a single BatchGetParents call.
	parentHashes := collectUniqueParents(txs)
	parents, _, err := store.BatchGetParents(ctx, parentHashes)
	if err != nil {
		return results, err
	}

	// Both an explicit "missing" hash and a hash absent from the parents map
	// are treated identically — any absent parent marks the tx as failed.
	// The missing slice is intentionally discarded here; both cases are
	// detected by the absence of the hash from the parents map below.

	alive := make([]bool, len(txs))
	for i, tx := range txs {
		parentMissing := false
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])

			if _, present := parents[key]; present {
				continue
			}

			// Parent is absent from the result map — either explicitly
			// listed as missing by Aerospike, or simply not returned.
			// Both cases map to ErrTxMissingParent.
			results[i].Err = terrors.ErrTxMissingParent
			results[i].Phase = PhaseGetParents
			parentMissing = true
			break
		}
		alive[i] = !parentMissing
	}

	// Phase A hydration: populate input.PreviousTxScript and PreviousTxSatoshis
	// from the UTXO store for every alive tx whose inputs are not already
	// extended, and collect per-input UTXO heights for Phase B script
	// verification. This mirrors what getTransactionInputBlockHeightsAndExtendTx
	// does for the per-tx Validate path, so that Validate(tx) and
	// ValidateBatch([tx]) operate on identical hydrated tx state.
	//
	// NOTE: This issues one v.utxoStore.Get per unique parent hash (N+1 relative
	// to BatchGetParents). Parents are already confirmed present by the alive
	// check above; we fetch the Tx bin to read output satoshis and scripts.
	// A follow-up task should fold this data into BatchGetParents to eliminate
	// the extra round-trip (tracked as a known suboptimal step).
	utxoHeightsByTx, hydrateErr := v.hydrateInputsForBatch(ctx, txs, parents, alive, results)
	if hydrateErr != nil {
		return results, hydrateErr
	}

	// Phase B — per-tx CPU validation (format + scripts).
	// Runs in an errgroup bounded by NumCPU so the goroutine count does not
	// grow unboundedly with batch size.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())
	for i := range txs {
		if !alive[i] {
			continue
		}
		i := i // capture loop var
		g.Go(func() error {
			if err := v.runCPUValidation(gCtx, txs[i], blockHeight, utxoHeightsByTx[i], opts...); err != nil {
				results[i].Err = err
				results[i].Phase = PhaseCPU
				alive[i] = false
			}
			return nil // never bubble per-tx errors as whole-batch errors
		})
	}
	_ = g.Wait() // errgroup never returns a non-nil error here

	// Phase C — BatchSpend. One BatchOperate carries all surviving inputs.
	// Per-input SpendResult.Err is attributed back to every tx that referenced
	// the failed parent.
	spends, spendToTxIdx := buildSpendsForBatch(txs, alive, results)
	if len(spends) > 0 {
		spendResults, spendErr := store.BatchSpend(ctx, spends, blockHeight)
		if spendErr != nil {
			// Whole-batch transport failure — mark all survivors as PhaseSpend.
			for i, a := range alive {
				if a {
					results[i].Err = spendErr
					results[i].Phase = PhaseSpend
					alive[i] = false
				}
			}
		} else {
			// Build a map: failed parent hash → first error seen.
			type parentKey [32]byte
			failedParents := make(map[parentKey]error)
			for j, sr := range spendResults {
				if sr.Err == nil {
					continue
				}
				sp := spends[j]
				var key parentKey
				copy(key[:], sp.TxID[:])
				if _, already := failedParents[key]; !already {
					failedParents[key] = sr.Err
				}
			}
			// Attribute parent failures to every tx that referenced them.
			for i, tx := range txs {
				if !alive[i] {
					continue
				}
				for _, in := range tx.Inputs {
					ph := in.PreviousTxIDChainHash()
					var key parentKey
					copy(key[:], ph[:])
					if e, bad := failedParents[key]; bad {
						results[i].Err = e
						results[i].Phase = PhaseSpend
						alive[i] = false
						break
					}
				}
			}
		}
	}
	_ = spendToTxIdx // reserved for Phase F metadata path

	// Phase D — BatchCreate (locked=true). One BatchOperate carries all surviving txs.
	// Per-tx errors (CREATE_ONLY collisions, large-tx-needs-external-storage) are tagged
	// PhaseCreate. Surviving txs have results[i].Meta populated so Phase F (Kafka) and
	// single-tx callers can use it.
	aliveTxs, aliveIdx := compactAlive(txs, alive)
	if len(aliveTxs) > 0 {
		createResults, cErr := store.BatchCreate(ctx, aliveTxs, blockHeight, true)
		if cErr != nil {
			// Whole-batch transport failure — mark all survivors as PhaseCreate.
			for _, i := range aliveIdx {
				results[i].Err = cErr
				results[i].Phase = PhaseCreate
				alive[i] = false
			}
		} else {
			for j, cr := range createResults {
				i := aliveIdx[j]
				if cr.Err != nil {
					results[i].Err = cr.Err
					results[i].Phase = PhaseCreate
					alive[i] = false
					continue
				}
				// Populate Meta for the surviving tx. Mirrors what
				// ValidateWithOptions / CreateInUtxoStore builds via
				// util.TxMetaDataFromTx after a successful Create call,
				// with Locked set to true (matching lockedTrue=true above).
				m, metaErr := buildMetaFromTx(aliveTxs[j], blockHeight)
				if metaErr != nil {
					results[i].Err = metaErr
					results[i].Phase = PhaseCreate
					alive[i] = false
					continue
				}
				results[i].Meta = m
			}
		}
	}

	// Phase E — submit surviving txs to BlockAssembly, then unlock the
	// BA-acknowledged subset via a single BatchSetLocked(false) call.
	// BA-rejected txs are left locked and tagged PhaseBlockAssembly so the
	// existing locked-tx reconciler can pick them up via the normal retry path.
	aliveTxs, aliveIdx = compactAlive(txs, alive)
	if len(aliveTxs) > 0 {
		baErrs := v.submitToBlockAssemblyBatch(ctx, aliveTxs)
		unlockHashes := make([][]byte, 0, len(aliveTxs))
		unlockIdx := make([]int, 0, len(aliveTxs))
		for j, tx := range aliveTxs {
			i := aliveIdx[j]
			h := tx.TxIDChainHash()
			if e, rejected := baErrs[*h]; rejected {
				results[i].Err = e
				results[i].Phase = PhaseBlockAssembly
				alive[i] = false
				continue
			}
			unlockHashes = append(unlockHashes, h[:])
			unlockIdx = append(unlockIdx, i)
		}
		if len(unlockHashes) > 0 {
			ulResults, uErr := store.BatchSetLocked(ctx, unlockHashes, false)
			if uErr != nil {
				for _, i := range unlockIdx {
					results[i].Err = uErr
					results[i].Phase = PhaseSetLocked
					alive[i] = false
				}
			} else {
				for j, ur := range ulResults {
					if ur.Err != nil {
						i := unlockIdx[j]
						results[i].Err = ur.Err
						results[i].Phase = PhaseSetLocked
						alive[i] = false
					}
				}
			}
		}
	}

	// Phase F — TxMeta Kafka publish for all surviving tx. Fire-and-forget;
	// uses the existing v.txmetaKafkaBatcher, which is a go-batcher we
	// deliberately keep in place because Kafka batching is a different concern
	// from the UTXO hot path. Errors are logged but do not fail the batch —
	// mirroring the existing single-tx path (Validator.go line ~783).
	// When SkipTxMetaPublishing is set in the opts, the publish is skipped
	// entirely (e.g. legacy catchup / quickValidationMode).
	processedOpts := ProcessOptions(opts...)
	skipPublish := processedOpts.SkipTxMetaPublishing || (v.txmetaKafkaProducerClient == nil && v.txMetaPublishOverride == nil)
	if !skipPublish {
		for i, tx := range txs {
			if !alive[i] {
				continue
			}
			if v.txMetaPublishOverride != nil {
				v.txMetaPublishOverride(tx, results[i].Meta)
				continue
			}
			if txMetaErr := v.sendTxMetaToKafka(results[i].Meta, tx.TxIDChainHash()); txMetaErr != nil {
				v.logger.Errorf("[ValidateBatch][%s] failed to serialize/enqueue txmeta for kafka: %v",
					tx.TxIDChainHash().String(), txMetaErr)
			}
		}
	}

	return results, nil
}

// compactAlive returns the subset of txs where alive[i] is true, together with
// their original indexes. Used by Phase D to build the BatchCreate input slice
// and map results back to the full results slice.
func compactAlive(txs []*bt.Tx, alive []bool) ([]*bt.Tx, []int) {
	out := make([]*bt.Tx, 0, len(txs))
	idx := make([]int, 0, len(txs))
	for i, tx := range txs {
		if alive[i] {
			out = append(out, tx)
			idx = append(idx, i)
		}
	}
	return out, idx
}

// buildMetaFromTx constructs the *meta.Data returned for a tx that was
// successfully created in Phase D. It mirrors the construction performed by
// util.TxMetaDataFromTx (called by Store.Create → CreateInUtxoStore) and sets
// Locked=true to match the lockedTrue=true flag passed to BatchCreate.
func buildMetaFromTx(tx *bt.Tx, blockHeight uint32) (*meta.Data, error) {
	m, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}
	m.Locked = true
	// blockHeight is used by the caller for fee/UTXO-hash computation; the
	// meta.Data struct does not have a plain BlockHeight field — unmined txs
	// use UnminedSince to record when they entered the mempool.
	m.UnminedSince = blockHeight
	return m, nil
}

// runCPUValidation runs the format + script checks for a single transaction
// without touching the UTXO store. It calls TxValidatorI methods directly
// (bypassing the v.validateTransaction / v.validateTransactionScripts wrappers
// which would call extendTransaction → UTXO store). Phase A already confirmed
// parents are present and hydrated the tx to extended form; utxoHeights holds
// one block height per input derived from the ParentRecord.BlockHeight values
// fetched in Phase A.
//
// In test code, v.cpuOverride intercepts the call so tests can inject
// controlled failures without needing a fully-extended signed transaction.
func (v *Validator) runCPUValidation(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, opts ...Option) error {
	if v.cpuOverride != nil {
		return v.cpuOverride(tx)
	}
	processedOpts := ProcessOptions(opts...)
	// ValidateTransaction uses utxoHeights only in checkFees → isConsolidationTx.
	// ValidateTransactionScripts passes them through to the BDK script engine,
	// which requires the slice to have exactly one entry per input.
	if err := v.txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, processedOpts); err != nil {
		return err
	}
	return v.txValidator.ValidateTransactionScripts(tx, blockHeight, utxoHeights, processedOpts)
}

// getBatchUtxoStore returns the validator's UTXO store as a batchUtxoStore if
// it satisfies the interface (i.e. is an *aerospike.Store or a test stub).
// Returns false when the store is e.g. a sql.Store that does not implement
// the batch methods.
func (v *Validator) getBatchUtxoStore() (batchUtxoStore, bool) {
	if v.batchStoreOverride != nil {
		return v.batchStoreOverride, true
	}
	s, ok := v.utxoStore.(batchUtxoStore)
	return s, ok
}

// collectUniqueParents walks all tx inputs and returns the deduplicated set of
// parent tx hashes as a [][]byte (preserves first-seen order).
func collectUniqueParents(txs []*bt.Tx) [][]byte {
	seen := make(map[[32]byte]struct{})
	out := make([][]byte, 0, len(txs))
	for _, tx := range txs {
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			b := make([]byte, 32)
			copy(b, ph[:])
			out = append(out, b)
		}
	}
	return out
}

// byteSliceSet converts a [][]byte into a map keyed by [32]byte for O(1) lookup.
func byteSliceSet(hashes [][]byte) map[[32]byte]struct{} {
	set := make(map[[32]byte]struct{}, len(hashes))
	for _, h := range hashes {
		var key [32]byte
		copy(key[:], h)
		set[key] = struct{}{}
	}
	return set
}

// buildSpendsForBatch flattens all surviving tx inputs into a single
// []*utxo.Spend slice suitable for one BatchSpend call. It mirrors the
// existing single-tx spend construction in utxo.GetSpends (stores/utxo/utils.go).
//
// spendToTxIdx[j] is the index in txs that contributed spends[j]. This allows
// downstream phases to attribute per-spend metadata back to the originating tx.
//
// If GetSpends fails for a tx (e.g. missing extended input data), the tx is
// marked failed at PhaseSpend via results/alive and excluded from the returned
// slice so BatchSpend never receives a nil or invalid entry.
func buildSpendsForBatch(txs []*bt.Tx, alive []bool, results []ValidationResult) (spends []*utxo.Spend, spendToTxIdx []int) {
	// Pre-size to the total number of inputs across surviving txs.
	total := 0
	for i, tx := range txs {
		if alive[i] {
			total += len(tx.Inputs)
		}
	}
	spends = make([]*utxo.Spend, 0, total)
	spendToTxIdx = make([]int, 0, total)

	for i, tx := range txs {
		if !alive[i] {
			continue
		}
		txSpends, err := utxo.GetSpends(tx)
		if err != nil {
			// Extended input data was unavailable — mark as PhaseSpend failure.
			results[i].Err = err
			results[i].Phase = PhaseSpend
			alive[i] = false
			continue
		}
		for _, sp := range txSpends {
			spends = append(spends, sp)
			spendToTxIdx = append(spendToTxIdx, i)
		}
	}
	return spends, spendToTxIdx
}

// submitToBlockAssemblyBatch submits a batch of txs to BlockAssembly.
// Returns a map keyed by tx hash of per-tx errors (only present for rejected
// txs; an absent entry means accepted). Uses the test override if set;
// otherwise calls v.blockAssembler.Store per-tx with bounded parallelism —
// the BA client internally batches via go-batcher so this is efficient.
//
// Service-level errors (e.g. blockAssembler is nil or Store returns a
// non-nil error) are treated as per-tx rejections tagged in the result map.
// The rationale: a transient BA unavailability should surface as
// PhaseBlockAssembly (leaving the tx locked for reconciler pickup) rather
// than as a whole-batch failure that would incorrectly un-spend and un-create
// txs that were successfully written to Aerospike in Phase D.
func (v *Validator) submitToBlockAssemblyBatch(ctx context.Context, txs []*bt.Tx) map[chainhash.Hash]error {
	if v.blockAssemblySubmitOverride != nil {
		return v.blockAssemblySubmitOverride(ctx, txs)
	}

	// If BlockAssembly is disabled (blockAssembler is nil), treat every tx as
	// accepted (no error) — mirroring the existing single-tx path which skips
	// the sendToBlockAssembler call entirely when addToBlockAssembly is false.
	if v.blockAssembler == nil {
		return map[chainhash.Hash]error{}
	}

	out := make(map[chainhash.Hash]error)
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())
	for _, tx := range txs {
		tx := tx
		g.Go(func() error {
			h := tx.TxIDChainHash()
			txInpoints, err := subtree.NewTxInpointsFromTx(tx)
			if err != nil {
				mu.Lock()
				out[*h] = err
				mu.Unlock()
				return nil
			}
			fee, err := util.GetFees(tx)
			if err != nil {
				mu.Lock()
				out[*h] = err
				mu.Unlock()
				return nil
			}
			if _, storeErr := v.blockAssembler.Store(gCtx, h, fee, uint64(tx.Size()), txInpoints); storeErr != nil { //nolint:gosec
				mu.Lock()
				out[*h] = storeErr
				mu.Unlock()
			}
			return nil // never bubble per-tx errors as whole-batch errors
		})
	}
	_ = g.Wait()
	return out
}

// hydrateInputsForBatch populates input.PreviousTxScript and
// input.PreviousTxSatoshis from the UTXO store for every alive tx that is not
// already extended, and derives per-input UTXO block heights from the
// ParentRecord.BlockHeight values returned by Phase A.
//
// Returns utxoHeightsByTx: one []uint32 per tx (parallel to txs), where each
// entry holds one block height per input. This is used by Phase B
// (runCPUValidation) when calling the BDK script engine, which requires the
// heights slice length to equal the number of inputs.
//
// This must produce the same tx state that
// getTransactionInputBlockHeightsAndExtendTx produces for the per-tx Validate
// path, so both paths operate on identical hydrated tx state.
//
// One v.utxoStore.Get call is made per unique parent hash (confirmed present by
// Phase A). Concurrent fetches are bounded by runtime.NumCPU. Per-tx errors
// mark the tx dead at PhaseGetParents and are written into results/alive.
//
// NOTE: This is a known suboptimal step — one Get per unique parent versus a
// single BatchGetParents call. A follow-up task should fold the output-bin data
// into BatchGetParents to eliminate these extra round-trips.
func (v *Validator) hydrateInputsForBatch(
	ctx context.Context,
	txs []*bt.Tx,
	parents map[[32]byte]*aerospike.ParentRecord,
	alive []bool,
	results []ValidationResult,
) (utxoHeightsByTx [][]uint32, err error) {
	// Always return a slice of the same length as txs so Phase B can index it
	// safely regardless of whether hydration is needed.
	utxoHeightsByTx = make([][]uint32, len(txs))

	if v.utxoStore == nil {
		return utxoHeightsByTx, nil
	}

	// Collect the unique parent hashes needed by at least one alive tx that is
	// not already extended. Already-extended txs have their input scripts and
	// satoshis populated; we only need the UTXO heights (from ParentRecord)
	// for them, which we derive below without a store lookup.
	needed := make(map[[32]byte]struct{})
	for i, tx := range txs {
		if !alive[i] || tx.IsExtended() {
			continue
		}
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])
			if _, ok := parents[key]; ok {
				needed[key] = struct{}{}
			}
		}
	}

	// Fetch full tx data for each needed parent. Store results in a map
	// protected by a mutex so goroutines can write concurrently.
	var fetchedMu sync.Mutex
	fetched := make(map[[32]byte]*bt.Tx, len(needed))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())
	for key := range needed {
		key := key // capture
		hash := chainhash.Hash(key)
		g.Go(func() error {
			txMeta, getErr := v.utxoStore.Get(gCtx, &hash, fields.Tx)
			if getErr != nil {
				// Parent was confirmed present in Phase A; treat a Get failure as
				// a whole-batch transport error (not a per-tx error) since we
				// cannot distinguish which tx is at fault.
				return getErr
			}
			if txMeta == nil || txMeta.Tx == nil {
				// Record exists but has no Tx bin — treat as a whole-batch error.
				return terrors.NewProcessingError(
					"[ValidateBatch] parent %s found in Phase A but Tx bin is nil during hydration",
					hash.String(),
				)
			}
			fetchedMu.Lock()
			fetched[key] = txMeta.Tx
			fetchedMu.Unlock()
			return nil
		})
	}
	if waitErr := g.Wait(); waitErr != nil {
		return utxoHeightsByTx, waitErr
	}

	// Populate inputs for each alive tx and build utxoHeights from the
	// ParentRecord.BlockHeight values collected in Phase A.
	for i, tx := range txs {
		if !alive[i] {
			continue
		}
		heights := make([]uint32, len(tx.Inputs))
		hydrationFailed := false

		for j, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])

			pr, inParents := parents[key]
			if !inParents {
				// Should have been caught by the alive check. Skip.
				continue
			}

			// Derive the UTXO height for this input. Mirrors the logic in
			// getUtxoBlockHeightAndExtendForParentTx: unmined parents use
			// blockState.Height+1 as a sentinel; for the batch path we do
			// not have the blockState readily available so we use the
			// ParentRecord.BlockHeight, which is 0 for unmined txs.
			heights[j] = pr.BlockHeight

			// Hydrate script + satoshis for non-extended inputs.
			if tx.IsExtended() {
				continue
			}
			parentTx, ok := fetched[key]
			if !ok {
				// Parent fetched map is missing an entry — only possible when
				// utxoStore.Get was skipped (e.g. store is nil). Should not
				// happen when needed was populated correctly.
				continue
			}
			outIdx := in.PreviousTxOutIndex
			if parentTx.Outputs == nil || int(outIdx) >= len(parentTx.Outputs) || parentTx.Outputs[outIdx] == nil { //nolint:gosec
				results[i].Err = terrors.NewProcessingError(
					"[ValidateBatch] parent tx output index %d out of range or nil for input",
					outIdx,
				)
				results[i].Phase = PhaseGetParents
				alive[i] = false
				hydrationFailed = true
				break
			}
			in.PreviousTxSatoshis = parentTx.Outputs[outIdx].Satoshis
			in.PreviousTxScript = parentTx.Outputs[outIdx].LockingScript
		}

		if !hydrationFailed && !tx.IsExtended() {
			tx.SetExtended(true)
		}
		utxoHeightsByTx[i] = heights
	}
	return utxoHeightsByTx, nil
}
