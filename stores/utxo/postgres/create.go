package postgres

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgxExecutor is the subset of the pgx API shared by *pgxpool.Conn and pgx.Tx,
// letting insertConflictingChildrenDirect run either directly on a pooled
// connection or inside the conflicting-create transaction.
type pgxExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ---------------------------------------------------------------------------
// Batch types
// ---------------------------------------------------------------------------

// batchCreateItem represents a single Create() request queued into the batcher.
type batchCreateItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	done        chan batchCreateResult
}

// batchCreateResult holds the result routed back to a Create() caller.
type batchCreateResult struct {
	Data *meta.Data
	Err  error
}

// ---------------------------------------------------------------------------
// Array packing — pack per-output data into flat byte forms for the txs row
// ---------------------------------------------------------------------------

// outputArrayParams holds the PACKED per-output columns for one txs row.
// locking_script and satoshis are recovered from raw_tx at read time (see
// batchDecorateOutputs), so they are not stored separately here.
//
// utxoHashes is a flat concatenation: output i at byte offset i*32 (read in
// SQL via substr(utxo_hashes, i*32+1, 32) — substr is 1-based). spendableBits
// is a bitmap whose encoding matches PostgreSQL get_bit(): bit i lives in byte
// i/8 at the (i%8)-th LEAST significant position. out_frozens is NOT built at
// create time — no output is frozen on create, and NULL means "none frozen".
type outputArrayParams struct {
	utxoHashes             []byte // nil when no outputs
	spendableBits          []byte // nil when no outputs
	outCount               int32
	spendableCount         int32
	coinbaseSpendingHeight int32 // 0 = non-coinbase; same height for every output (column is INT4)
}

// setPackedBit sets bit i in buf using the PostgreSQL get_bit()/set_bit()
// encoding: byte i/8, least-significant-bit-first within the byte.
func setPackedBit(buf []byte, i int) {
	buf[i/8] |= 1 << (i % 8)
}

// getPackedBit reads bit i from buf using the same encoding (i must be within
// len(buf)*8).
func getPackedBit(buf []byte, i int) bool {
	return buf[i/8]&(1<<(i%8)) != 0
}

// unpackBitmap expands a packed bitmap into count per-output booleans (the
// read-time reconstruction of the old BOOLEAN[] form — the wire contract for
// consumers is unchanged). A nil bitmap returns nil ("no flag set" common
// case, e.g. out_frozens before any freeze). Bits beyond len(bitmap)*8 read
// false, mirroring the old OOB-subscript-is-NULL behaviour.
func unpackBitmap(bitmap []byte, count int) []bool {
	if bitmap == nil || count <= 0 {
		return nil
	}
	out := make([]bool, count)
	for i := 0; i < count && i < len(bitmap)*8; i++ {
		out[i] = getPackedBit(bitmap, i)
	}
	return out
}

// buildOutputArrays packs transaction outputs into the flat per-output columns.
// Outputs are packed at their TRUE index i (a nil output — never produced by a
// valid tx — leaves a zero 32-byte slot with spendable bit clear).
func buildOutputArrays(txHash *chainhash.Hash, btTx *bt.Tx, isCoinbase bool, blockHeight uint32, coinbaseMaturity int) (outputArrayParams, error) {
	if countNonNilOutputs(btTx) == 0 {
		return outputArrayParams{}, nil
	}
	count := len(btTx.Outputs)

	p := outputArrayParams{
		utxoHashes:    make([]byte, count*32),
		spendableBits: make([]byte, (count+7)/8),
		outCount:      int32(count),
	}
	if isCoinbase {
		// coinbase_spending_height is INT4: heights stay far below 2^31, but
		// guard the widened sum before narrowing so a corrupt input can never
		// silently wrap.
		h := int64(blockHeight) + int64(coinbaseMaturity)
		if h > math.MaxInt32 {
			return outputArrayParams{}, errors.NewProcessingError("coinbase spending height %d exceeds int32 range", h)
		}
		p.coinbaseSpendingHeight = int32(h)
	}

	for i, output := range btTx.Outputs {
		if output == nil {
			continue
		}
		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return outputArrayParams{}, err
		}
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return outputArrayParams{}, err
		}
		copy(p.utxoHashes[i*32:], utxoHash[:])
		// A zero-value OP_RETURN / data-carrier output can never be spent, so it
		// must not count toward the "fully spent" pruning check. Mirror the
		// aerospike store's ShouldStoreOutputAsUTXO gate. (nil script → no deref,
		// treated as spendable; non-nil zero-value scripts are inspected.)
		if output.LockingScript == nil || utxo.ShouldStoreOutputAsUTXO(isCoinbase, output, blockHeight) {
			setPackedBit(p.spendableBits, i)
			p.spendableCount++
		}
	}
	return p, nil
}

// preparedCreate holds pre-computed data for one item in a create batch.
type preparedCreate struct {
	txHash     *chainhash.Hash
	txMeta     *meta.Data
	isCoinbase bool
	outArrs    outputArrayParams
}

// ---------------------------------------------------------------------------
// Create — public API
// ---------------------------------------------------------------------------

// Create stores a new transaction's outputs as UTXOs. When a createBatcher is
// active (after Start()), requests are enqueued for batch processing. Otherwise,
// the INSERT is executed directly for single-item operation (used in tests).
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	startTime := time.Now()
	defer func() {
		if prometheusDirectCreate != nil {
			prometheusDirectCreate.Inc()
		}
		if prometheusDirectCreateDuration != nil {
			prometheusDirectCreateDuration.Observe(time.Since(startTime).Seconds())
		}
	}()

	if _, err := blockHeightToInt32(blockHeight); err != nil {
		return nil, err
	}

	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// If batcher is active, enqueue and wait.
	if s.createBatcher != nil {
		return s.createBatched(ctx, tx, blockHeight, options)
	}

	// No batcher — execute INSERT directly (single-item path).
	return s.createDirect(ctx, tx, blockHeight, options)
}

// createBatched enqueues a Create request into the batcher for bulk processing.
func (s *Store) createBatched(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	done := make(chan batchCreateResult, 1)
	s.createBatcher.Put(&batchCreateItem{
		tx:          tx,
		blockHeight: blockHeight,
		options:     options,
		done:        done,
	})

	select {
	case result := <-done:
		return result.Data, result.Err
	case <-ctx.Done():
		s.logger.Warnf("[createBatched] context cancelled while waiting for batcher result")
		return nil, ctx.Err()
	}
}

// createDirect executes the INSERT directly for a single transaction.
func (s *Store) createDirect(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	if options.Conflicting {
		txMeta.Conflicting = true
	}
	if options.Locked {
		txMeta.Locked = true
	}

	var txHash *chainhash.Hash
	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	var unminedSince interface{}
	if len(options.MinedBlockInfos) == 0 {
		unminedSince = int32(blockHeight) // unmined_since is INT4; heights < 2^31
	}

	rawTx := tx.ExtendedBytes()

	// Build block_id arrays.
	var blockIDs, blkHeights, subtreeIdxs []int32
	// minedAtHeight feeds Worker 2's deferred-DAH mine-arm: a tx created already
	// mined (legacy catch-up) must record the height it was mined at, exactly as
	// SetMinedMulti does, otherwise the sweep can never enumerate the
	// fully-spent-then-mined case and it falls to the keyspace backstop. Use the
	// lowest mined block height (earliest mining). INT4 column; heights < 2^31.
	var minedAtHeight *int32
	if len(options.MinedBlockInfos) > 0 {
		blockIDs = make([]int32, len(options.MinedBlockInfos))
		blkHeights = make([]int32, len(options.MinedBlockInfos))
		subtreeIdxs = make([]int32, len(options.MinedBlockInfos))
		minH := int32(options.MinedBlockInfos[0].BlockHeight)
		for i, info := range options.MinedBlockInfos {
			blockIDs[i] = int32(info.BlockID)
			blkHeights[i] = int32(info.BlockHeight)
			subtreeIdxs[i] = int32(info.SubtreeIdx)
			if int32(info.BlockHeight) < minH {
				minH = int32(info.BlockHeight)
			}
		}
		minedAtHeight = &minH
	}

	outArrs, err := buildOutputArrays(txHash, tx, isCoinbase, blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
	if err != nil {
		return nil, err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to acquire connection", err)
	}
	defer conn.Release()

	const insertSQL = `
		INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			locked, conflicting, frozen, unmined_since,
			block_ids, block_heights, subtree_idxs, conflicting_children, mined_at_height,
			utxo_hashes, out_count, spendable_count, out_spendables, coinbase_spending_height)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
		        $17, $18, $19, $20, $21)
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`
	insertArgs := []interface{}{
		txHash[:], int64(tx.Version), int64(tx.LockTime),
		int64(txMeta.Fee), int64(txMeta.SizeInBytes), isCoinbase, rawTx,
		options.Locked, options.Conflicting, options.Frozen, unminedSince,
		blockIDs, blkHeights, subtreeIdxs, [][]byte(nil), minedAtHeight,
		outArrs.utxoHashes, outArrs.outCount, outArrs.spendableCount,
		outArrs.spendableBits, outArrs.coinbaseSpendingHeight,
	}

	// Conflicting txs need the txs INSERT AND the N parent conflicting_children
	// UPDATEs to be atomic: a failure (or disconnect) after the child row commits
	// but before the parents are stamped would leave a torn write the early
	// TxExistsError return on retry can never repair (ON CONFLICT DO NOTHING →
	// pgx.ErrNoRows → return before the child UPDATEs re-run). Wrap both in one
	// transaction for this rare path. The common, non-conflicting path stays a
	// single auto-committing INSERT (no extra round-trip).
	if txMeta.Conflicting {
		txn, beginErr := conn.Begin(ctx)
		if beginErr != nil {
			return nil, errors.NewStorageError("failed to begin conflicting-create tx", beginErr)
		}
		defer func() { _ = txn.Rollback(ctx) }()

		var insertedHash []byte
		if err = txn.QueryRow(ctx, insertSQL, insertArgs...).Scan(&insertedHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
			}
			return nil, errors.NewStorageError("failed to create UTXO", err)
		}

		// insertConflictingChildrenDirect is idempotent (the @> guard), so a retry
		// after a partial failure re-applies the missing parent stamps cleanly.
		if err = s.insertConflictingChildrenDirect(ctx, txn, txHash, tx); err != nil {
			return nil, err
		}

		if err = txn.Commit(ctx); err != nil {
			return nil, errors.NewStorageError("failed to commit conflicting-create tx", err)
		}

		result := s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight)
		return result, nil
	}

	// Insert into txs (metadata + state + raw_tx + packed output columns).
	// out_frozens is NULL on create (no output frozen — freeze materialises the
	// bitmap on demand); spendable_ins is NULL on create (set by ReAssignUTXO).
	var insertedHash []byte
	if err = conn.QueryRow(ctx, insertSQL, insertArgs...).Scan(&insertedHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxExistsError("transaction already exists (coinbase=%v):", isCoinbase)
		}
		return nil, errors.NewStorageError("failed to create UTXO", err)
	}

	result := s.buildCreateMeta(txMeta, options, isCoinbase, blockHeight)
	return result, nil
}

// ---------------------------------------------------------------------------
// sendCreateBatch — batch callback for the go-batcher
// ---------------------------------------------------------------------------

func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	s.batchStats.createItems.Add(int64(len(batch)))
	s.batchStats.createBatches.Add(1)
	ctx := context.Background()

	// Single-item: the direct INSERT path handles every option (mined,
	// conflicting, locked, frozen) in a single round-trip.
	if len(batch) == 1 {
		item := batch[0]
		result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
		item.done <- batchCreateResult{Data: result, Err: err}
		return
	}

	// Bulk path: a single UNNEST'd INSERT handles the common cases — plain
	// validator-path creates and single-block mined creates (legacy catch-up).
	// The two rare exceptions fall back to the proven per-item createDirect:
	//   - Conflicting txs need a separate conflicting_children insert.
	//   - A tx mined in >1 block at once needs per-row variable-length array
	//     columns, which a flat UNNEST cannot express.
	// (UNNEST benchmarked faster than the old COPY+staging+merge path across
	// the entire configured batch-size range, so there is no large-batch case
	// where staging would win — see BenchmarkCreatePaths.)
	bulk := make([]*batchCreateItem, 0, len(batch))
	for _, item := range batch {
		if item.options.Conflicting || len(item.options.MinedBlockInfos) > 1 {
			result, err := s.createDirect(ctx, item.tx, item.blockHeight, item.options)
			item.done <- batchCreateResult{Data: result, Err: err}
			continue
		}
		bulk = append(bulk, item)
	}
	if len(bulk) > 0 {
		s.sendCreateBatchUNNEST(ctx, bulk)
	}
}

// ---------------------------------------------------------------------------
// sendCreateBatchUNNEST — direct INSERT…SELECT FROM UNNEST. The single bulk
// create path: no staging tables, no COPY protocol, no merge step. Handles
// plain validator-path creates and single-block mined creates (block_ids etc.
// populated when the item carries exactly one MinedBlockInfo). Conflicting and
// multi-block items are filtered out by sendCreateBatch and never reach here.
// It issues a SINGLE auto-committing INSERT for the whole batch, which is
// implicitly atomic on its own — there is no second statement to coordinate (the
// conflicting path, which does need a multi-statement transaction, runs in
// createDirect, not here).
// ---------------------------------------------------------------------------
func (s *Store) sendCreateBatchUNNEST(ctx context.Context, batch []*batchCreateItem) {
	n := len(batch)
	hashes := make([][]byte, 0, n)
	versions := make([]int64, 0, n)
	lockTimes := make([]int64, 0, n)
	fees := make([]int64, 0, n)
	sizes := make([]int64, 0, n)
	coinbases := make([]bool, 0, n)
	rawTxs := make([][]byte, 0, n)
	lockeds := make([]bool, 0, n)
	conflictings := make([]bool, 0, n)
	frozens := make([]bool, 0, n)
	unminedSinces := make([]int32, 0, n) // unmined_since is INT4
	// Mined-block columns. mined[i] discriminates: when true the row is mined in
	// exactly one block and block_ids/heights/subtree_idxs are written as a
	// single-element array with unmined_since NULL; when false the tx is unmined
	// (block columns NULL, unmined_since = blockHeight). Matches createDirect.
	// All three feed INT columns (and block_height also feeds the INT4
	// mined_at_height), so they are bound as int4[].
	mineds := make([]bool, 0, n)
	blockIDs := make([]int32, 0, n)
	blockHeights := make([]int32, 0, n)
	subtreeIdxs := make([]int32, 0, n)

	prepared := make([]preparedCreate, n)
	valid := make([]bool, n)

	// Packed per-output columns — one element per tx row, carried directly in
	// the per-tx UNNEST arrays (no flat per-output fan-out, no re-aggregation
	// CTE). nil elements become SQL NULL (txs with no outputs).
	packedUtxoHashes := make([][]byte, 0, n)
	packedSpendableBits := make([][]byte, 0, n)
	outCounts := make([]int32, 0, n)
	spendableCounts := make([]int32, 0, n)
	coinbaseSpendingHeights := make([]int32, 0, n) // coinbase_spending_height is INT4

	for i, item := range batch {
		txMeta, err := util.TxMetaDataFromTx(item.tx)
		if err != nil {
			item.done <- batchCreateResult{Err: errors.NewProcessingError("failed to get tx meta data", err)}
			continue
		}
		if item.options.Locked {
			txMeta.Locked = true
		}

		var txHash *chainhash.Hash
		if item.options.TxID != nil {
			txHash = item.options.TxID
		} else {
			txHash = item.tx.TxIDChainHash()
		}

		isCoinbase := item.tx.IsCoinbase()
		if item.options.IsCoinbase != nil {
			isCoinbase = *item.options.IsCoinbase
		}

		outArrs, err := buildOutputArrays(txHash, item.tx, isCoinbase, item.blockHeight, int(s.settings.ChainCfgParams.CoinbaseMaturity))
		if err != nil {
			item.done <- batchCreateResult{Err: err}
			continue
		}

		prepared[i] = preparedCreate{
			txHash:     txHash,
			txMeta:     txMeta,
			isCoinbase: isCoinbase,
			outArrs:    outArrs,
		}
		valid[i] = true

		hashes = append(hashes, txHash[:])
		versions = append(versions, int64(item.tx.Version))
		lockTimes = append(lockTimes, int64(item.tx.LockTime))
		fees = append(fees, int64(txMeta.Fee))
		sizes = append(sizes, int64(txMeta.SizeInBytes))
		coinbases = append(coinbases, isCoinbase)
		rawTxs = append(rawTxs, item.tx.ExtendedBytes())
		lockeds = append(lockeds, item.options.Locked)
		conflictings = append(conflictings, false) // conflicting items routed to createDirect
		frozens = append(frozens, item.options.Frozen)
		unminedSinces = append(unminedSinces, int32(item.blockHeight))

		// sendCreateBatch guarantees len(MinedBlockInfos) <= 1 here.
		mined := len(item.options.MinedBlockInfos) == 1
		mineds = append(mineds, mined)
		var bID, bHeight, sIdx int32
		if mined {
			info := item.options.MinedBlockInfos[0]
			bID, bHeight, sIdx = int32(info.BlockID), int32(info.BlockHeight), int32(info.SubtreeIdx)
		}
		blockIDs = append(blockIDs, bID)
		blockHeights = append(blockHeights, bHeight)
		subtreeIdxs = append(subtreeIdxs, sIdx)

		packedUtxoHashes = append(packedUtxoHashes, outArrs.utxoHashes)
		packedSpendableBits = append(packedSpendableBits, outArrs.spendableBits)
		outCounts = append(outCounts, outArrs.outCount)
		spendableCounts = append(spendableCounts, outArrs.spendableCount)
		coinbaseSpendingHeights = append(coinbaseSpendingHeights, outArrs.coinbaseSpendingHeight)
	}

	if len(hashes) == 0 {
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("failed to acquire connection", err)}
			}
		}
		return
	}
	defer conn.Release()

	// Single autocommit UNNEST INSERT. Each tx row carries its packed per-output
	// values directly in the per-tx arrays — no per-output row fan-out and no
	// re-aggregation CTE (the old out_arr GROUP BY was a top-4 server-CPU
	// consumer). out_frozens and spendable_ins are NULL on create (freeze /
	// ReAssignUTXO materialise them later).
	rows, err := conn.Query(ctx, `
		INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
			locked, conflicting, frozen, unmined_since, block_ids, block_heights, subtree_idxs, mined_at_height,
			utxo_hashes, out_count, spendable_count, out_spendables, coinbase_spending_height)
		SELECT u.hash, u.version, u.lock_time, u.fee, u.size_in_bytes, u.coinbase, u.raw_tx,
		       u.locked, u.conflicting, u.frozen,
		       CASE WHEN u.mined THEN NULL::int ELSE u.unmined_since END,
		       CASE WHEN u.mined THEN ARRAY[u.block_id] ELSE NULL::int[] END,
		       CASE WHEN u.mined THEN ARRAY[u.block_height] ELSE NULL::int[] END,
		       CASE WHEN u.mined THEN ARRAY[u.subtree_idx] ELSE NULL::int[] END,
		       CASE WHEN u.mined THEN u.block_height ELSE NULL::int END,
		       u.utxo_hashes, u.out_count, u.spendable_count, u.out_spendables, u.coinbase_spending_height
		FROM UNNEST($1::bytea[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[],
		            $6::boolean[], $7::bytea[], $8::boolean[], $9::boolean[], $10::boolean[],
		            $11::int[], $12::boolean[], $13::int[], $14::int[], $15::int[],
		            $16::bytea[], $17::int[], $18::int[], $19::bytea[], $20::int[])
		     AS u(hash, version, lock_time, fee, size_in_bytes, coinbase, raw_tx,
		          locked, conflicting, frozen, unmined_since, mined, block_id, block_height, subtree_idx,
		          utxo_hashes, out_count, spendable_count, out_spendables, coinbase_spending_height)
		ON CONFLICT (hash) DO NOTHING
		RETURNING hash`,
		hashes, versions, lockTimes, fees, sizes, coinbases, rawTxs,
		lockeds, conflictings, frozens, unminedSinces,
		mineds, blockIDs, blockHeights, subtreeIdxs,
		packedUtxoHashes, outCounts, spendableCounts, packedSpendableBits, coinbaseSpendingHeights,
	)
	if err != nil {
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("failed to INSERT create batch via CTE", err)}
			}
		}
		return
	}

	newHashSet := make(map[chainhash.Hash]struct{})
	for rows.Next() {
		var hashBytes []byte
		if scanErr := rows.Scan(&hashBytes); scanErr != nil {
			rows.Close()
			for i, item := range batch {
				if valid[i] {
					item.done <- batchCreateResult{Err: errors.NewStorageError("failed to scan inserted hash", scanErr)}
				}
			}
			return
		}
		var h chainhash.Hash
		copy(h[:], hashBytes)
		newHashSet[h] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		for i, item := range batch {
			if valid[i] {
				item.done <- batchCreateResult{Err: errors.NewStorageError("create batch rows error", err)}
			}
		}
		return
	}
	rows.Close()

	for i, item := range batch {
		if !valid[i] {
			continue
		}
		p := &prepared[i]
		if _, wasNew := newHashSet[*p.txHash]; wasNew {
			// Consume the hash so only the first item with this hash in the
			// batch is treated as newly created. Concurrent duplicate creates
			// of the same tx coalesce into a single bulk INSERT whose ON CONFLICT
			// DO NOTHING inserts exactly one row (RETURNING it once); without
			// consuming it here, every same-hash item would be marked created and
			// each caller would notify block assembly. Subsequent same-hash items
			// must return TxExists, matching createDirect's ON CONFLICT semantics.
			delete(newHashSet, *p.txHash)
			result := s.buildCreateMeta(p.txMeta, item.options, p.isCoinbase, item.blockHeight)
			item.done <- batchCreateResult{Data: result}
		} else {
			item.done <- batchCreateResult{
				Err: errors.NewTxExistsError("transaction already exists (coinbase=%v):", p.isCoinbase),
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildCreateMeta populates the meta.Data from computed values.
func (s *Store) buildCreateMeta(txMeta *meta.Data, options *utxo.CreateOptions, isCoinbase bool, blockHeight uint32) *meta.Data {
	txMeta.IsCoinbase = isCoinbase
	txMeta.Frozen = options.Frozen
	if len(options.MinedBlockInfos) == 0 {
		txMeta.UnminedSince = blockHeight
	}
	if len(options.MinedBlockInfos) > 0 {
		txMeta.BlockIDs = make([]uint32, len(options.MinedBlockInfos))
		txMeta.BlockHeights = make([]uint32, len(options.MinedBlockInfos))
		txMeta.SubtreeIdxs = make([]int, len(options.MinedBlockInfos))
		for i, info := range options.MinedBlockInfos {
			txMeta.BlockIDs[i] = info.BlockID
			txMeta.BlockHeights[i] = info.BlockHeight
			txMeta.SubtreeIdxs[i] = info.SubtreeIdx
		}
	}
	return txMeta
}

// insertConflictingChildrenDirect updates the parent txs rows to append this child
// to their conflicting_children array.
func (s *Store) insertConflictingChildrenDirect(ctx context.Context, exec pgxExecutor, childTxHash *chainhash.Hash, tx *bt.Tx) error {
	seen := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := *input.PreviousTxIDChainHash()
		if _, ok := seen[parentHash]; ok {
			continue
		}
		seen[parentHash] = struct{}{}

		_, err := exec.Exec(ctx, `
			UPDATE txs SET conflicting_children = COALESCE(conflicting_children, '{}') || $2::bytea[]
			WHERE hash = $1
			  AND NOT (COALESCE(conflicting_children, '{}') @> $2::bytea[])`,
			parentHash[:], [][]byte{childTxHash[:]},
		)
		if err != nil {
			return errors.NewStorageError("failed to insert conflicting_children", err)
		}
	}

	return nil
}

// countNonNilOutputs counts non-nil outputs in a transaction.
func countNonNilOutputs(tx *bt.Tx) int {
	count := 0
	for _, o := range tx.Outputs {
		if o != nil {
			count++
		}
	}
	return count
}

// buildINClauseLocal generates a SQL IN clause placeholder string and args.
// startIdx is the 1-based parameter index ($startIdx, $startIdx+1, ...).
func buildINClauseLocal(hashes [][]byte, startIdx int) (string, []interface{}) {
	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = fmt.Sprintf("$%d", startIdx+i)
		args[i] = h
	}
	return "(" + strings.Join(placeholders, ",") + ")", args
}
