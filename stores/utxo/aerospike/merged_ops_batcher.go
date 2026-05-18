package aerospike

import (
	"context"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/ordishs/gocore"
)

// opKind identifies which underlying op a mixedOp carries.
type opKind uint8

const (
	opGet opKind = iota
	opSpend
	opCreate
	opOutpoint
	opIncrement
	opSetDAH
	opSetLocked
)

// mixedOp is a sum-type item queued into the merged ops batcher.
// Exactly one of the pointer fields is non-nil, indicated by kind.
type mixedOp struct {
	kind      opKind
	get       *batchGetItem
	spend     *batchSpend
	create    *BatchStoreItem
	outpoint  *batchOutpoint
	increment *batchIncrement
	setDAH    *batchDAH
	setLocked *batchLocked
}

// sendMergedOpsBatch is the flush handler for the merged ops batcher. It
// partitions the queued items by kind, then either dispatches them through a
// single mixed BatchOperate (single mode) or splits reads vs writes into two
// parallel BatchOperate calls (split mode). In all cases, GET items are
// dispatched through sendGetBatch (which uses BatchDecorate + retries) in
// parallel with the BatchOperate path(s).
func (s *Store) sendMergedOpsBatch(items []*mixedOp) {
	if len(items) == 0 {
		return
	}

	var (
		gets       []*batchGetItem
		spends     []*batchSpend
		creates    []*BatchStoreItem
		outpoints  []*batchOutpoint
		increments []*batchIncrement
		setDAHs    []*batchDAH
		setLockeds []*batchLocked
	)

	for _, it := range items {
		switch it.kind {
		case opGet:
			if it.get != nil {
				gets = append(gets, it.get)
			}
		case opSpend:
			if it.spend != nil {
				spends = append(spends, it.spend)
			}
		case opCreate:
			if it.create != nil {
				creates = append(creates, it.create)
			}
		case opOutpoint:
			if it.outpoint != nil {
				outpoints = append(outpoints, it.outpoint)
			}
		case opIncrement:
			if it.increment != nil {
				increments = append(increments, it.increment)
			}
		case opSetDAH:
			if it.setDAH != nil {
				setDAHs = append(setDAHs, it.setDAH)
			}
		case opSetLocked:
			if it.setLocked != nil {
				setLockeds = append(setLockeds, it.setLocked)
			}
		}
	}

	mode := s.settings.UtxoStore.MergedOpsBatcherMode

	if mode == "split" {
		s.sendMergedOpsBatchSplit(s.ctx, gets, spends, creates, outpoints, increments, setDAHs, setLockeds)
		return
	}

	// single mode (default): GET in parallel with one mixed BatchOperate over
	// spend + create + outpoint + increment + setDAH + setLocked.
	var wg sync.WaitGroup

	if len(gets) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.sendGetBatch(gets)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.dispatchMixedBatchOperate(spends, creates, outpoints, increments, setDAHs, setLockeds)
	}()

	wg.Wait()
}

// sendMergedOpsBatchSplit fires reads (GET via sendGetBatch + outpoint via
// BatchOperate) in parallel with writes (spend + create + increment + setDAH +
// setLocked via a single BatchOperate). Each branch is independent.
func (s *Store) sendMergedOpsBatchSplit(
	_ context.Context,
	gets []*batchGetItem,
	spends []*batchSpend,
	creates []*BatchStoreItem,
	outpoints []*batchOutpoint,
	increments []*batchIncrement,
	setDAHs []*batchDAH,
	setLockeds []*batchLocked,
) {
	var wg sync.WaitGroup

	if len(gets) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.sendGetBatch(gets)
		}()
	}

	if len(outpoints) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Outpoint is a pure read — keep it isolated from writes.
			s.dispatchMixedBatchOperate(nil, nil, outpoints, nil, nil, nil)
		}()
	}

	if len(spends)+len(creates)+len(increments)+len(setDAHs)+len(setLockeds) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.dispatchMixedBatchOperate(spends, creates, nil, increments, setDAHs, setLockeds)
		}()
	}

	wg.Wait()
}

// dispatchMixedBatchOperate concatenates records from each non-GET builder,
// performs ONE BatchOperate, then routes the sub-slices back to each
// builder's dispatch closure. Empty op-type slices are skipped entirely.
//
// Builder dispatch closures all accept (records []BatchRecordIfc, batchErr
// aerospike.Error) except buildStoreRecords which takes a trailing batchID.
// We allocate a batchID up front and apply it only to the store dispatch.
func (s *Store) dispatchMixedBatchOperate(
	spends []*batchSpend,
	creates []*BatchStoreItem,
	outpoints []*batchOutpoint,
	increments []*batchIncrement,
	setDAHs []*batchDAH,
	setLockeds []*batchLocked,
) {
	type segment struct {
		offset   int
		length   int
		dispatch func([]aerospike.BatchRecordIfc, aerospike.Error)
	}

	var (
		all      []aerospike.BatchRecordIfc
		segments []segment
	)

	ctx := s.ctx

	// SPEND
	if len(spends) > 0 {
		batchID := s.batchID.Add(1)
		recs, disp := s.buildSpendRecords(ctx, spends, batchID)
		if disp != nil && len(recs) > 0 {
			segments = append(segments, segment{offset: len(all), length: len(recs), dispatch: disp})
			all = append(all, recs...)
		}
	}

	// CREATE
	if len(creates) > 0 {
		stat := gocore.NewStat("sendMergedOpsBatch.create")
		start := time.Now()
		recs, disp := s.buildStoreRecords(ctx, creates, stat, &start)
		if disp != nil && len(recs) > 0 {
			batchID := s.batchID.Add(1)
			adapter := func(rs []aerospike.BatchRecordIfc, be aerospike.Error) {
				disp(rs, be, batchID)
			}
			segments = append(segments, segment{offset: len(all), length: len(recs), dispatch: adapter})
			all = append(all, recs...)
		}
	}

	// OUTPOINT
	if len(outpoints) > 0 {
		recs, disp := s.buildOutpointRecords(ctx, outpoints)
		if disp != nil && len(recs) > 0 {
			segments = append(segments, segment{offset: len(all), length: len(recs), dispatch: disp})
			all = append(all, recs...)
		}
	}

	// INCREMENT
	if len(increments) > 0 {
		recs, disp := s.buildIncrementRecords(ctx, increments)
		if disp != nil && len(recs) > 0 {
			segments = append(segments, segment{offset: len(all), length: len(recs), dispatch: disp})
			all = append(all, recs...)
		}
	}

	// SET DAH
	if len(setDAHs) > 0 {
		recs, disp := s.buildSetDAHRecords(ctx, setDAHs)
		if disp != nil && len(recs) > 0 {
			segments = append(segments, segment{offset: len(all), length: len(recs), dispatch: disp})
			all = append(all, recs...)
		}
	}

	// SET LOCKED
	if len(setLockeds) > 0 {
		recs, disp := s.buildSetLockedRecords(ctx, setLockeds)
		if disp != nil && len(recs) > 0 {
			segments = append(segments, segment{offset: len(all), length: len(recs), dispatch: disp})
			all = append(all, recs...)
		}
	}

	if len(all) == 0 {
		return
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	var batchErr aerospike.Error
	if s.batchOperateFn != nil {
		batchErr = s.batchOperateFn(batchPolicy, all)
	} else {
		batchErr = s.client.BatchOperate(batchPolicy, all)
	}

	for _, seg := range segments {
		sub := all[seg.offset : seg.offset+seg.length]
		seg.dispatch(sub, batchErr)
	}
}

// sendMergedOpsReadBatch handles the read-intake batcher (GET + Outpoint).
// GET goes through sendGetBatch (BatchDecorate with retries); Outpoint goes
// through its own BatchOperate. The two run in parallel.
func (s *Store) sendMergedOpsReadBatch(items []*mixedOp) {
	if len(items) == 0 {
		return
	}
	var gets []*batchGetItem
	var outpoints []*batchOutpoint
	for _, it := range items {
		switch it.kind {
		case opGet:
			if it.get != nil {
				gets = append(gets, it.get)
			}
		case opOutpoint:
			if it.outpoint != nil {
				outpoints = append(outpoints, it.outpoint)
			}
		}
	}

	var wg sync.WaitGroup
	if len(gets) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.sendGetBatch(gets)
		}()
	}
	if len(outpoints) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.dispatchMixedBatchOperate(nil, nil, outpoints, nil, nil, nil)
		}()
	}
	wg.Wait()
}

// sendMergedOpsWriteBatch handles the write-intake batcher
// (SPEND + CREATE + Increment + SetDAH + SetLocked) via one mixed BatchOperate.
func (s *Store) sendMergedOpsWriteBatch(items []*mixedOp) {
	if len(items) == 0 {
		return
	}
	var spends []*batchSpend
	var creates []*BatchStoreItem
	var increments []*batchIncrement
	var setDAHs []*batchDAH
	var setLockeds []*batchLocked
	for _, it := range items {
		switch it.kind {
		case opSpend:
			if it.spend != nil {
				spends = append(spends, it.spend)
			}
		case opCreate:
			if it.create != nil {
				creates = append(creates, it.create)
			}
		case opIncrement:
			if it.increment != nil {
				increments = append(increments, it.increment)
			}
		case opSetDAH:
			if it.setDAH != nil {
				setDAHs = append(setDAHs, it.setDAH)
			}
		case opSetLocked:
			if it.setLocked != nil {
				setLockeds = append(setLockeds, it.setLocked)
			}
		}
	}
	s.dispatchMixedBatchOperate(spends, creates, nil, increments, setDAHs, setLockeds)
}

// submitOp routes an op to either the merged batcher (when configured) or the
// legacy per-op batcher.
func (s *Store) submitOp(ctx context.Context, op *mixedOp) {
	// Split-intake path: route by op-kind to read or write batcher.
	if s.mergedOpsReadBatcher != nil {
		switch op.kind {
		case opGet, opOutpoint:
			s.mergedOpsReadBatcher.PutCtx(ctx, op)
		default:
			s.mergedOpsWriteBatcher.PutCtx(ctx, op)
		}
		return
	}
	if s.mergedOpsBatcher != nil {
		s.mergedOpsBatcher.PutCtx(ctx, op)
		return
	}
	switch op.kind {
	case opGet:
		s.getBatcher.PutCtx(ctx, op.get)
	case opSpend:
		s.spendBatcher.PutCtx(ctx, op.spend)
	case opCreate:
		s.storeBatcher.PutCtx(ctx, op.create)
	case opOutpoint:
		s.outpointBatcher.PutCtx(ctx, op.outpoint)
	case opIncrement:
		s.incrementBatcher.PutCtx(ctx, op.increment)
	case opSetDAH:
		s.setDAHBatcher.PutCtx(ctx, op.setDAH)
	case opSetLocked:
		s.lockedBatcher.PutCtx(ctx, op.setLocked)
	}
}
