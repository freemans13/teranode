package blockassembly

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
)

// candidateTime returns the timestamp to stamp on a mining candidate built on
// parentHeader: the local wall clock, floored at median-time-past+1 of the
// parent chain. Block validation rejects any block whose timestamp is not
// strictly greater than the median timestamp of the previous 11 blocks
// (model.Block CheckHeaderContextual), so without the floor a lagging local
// clock — or a run of forward-stamped blocks dragging the median above
// wall-clock time — makes the assembler hand miners a candidate that the
// network refuses once mined. The submit path itself does not catch it:
// SubmitMiningSolution calls block.Valid with no currentChain, which skips
// the median-time rule, so the bad block is accepted locally and then
// rejected by every peer that validates it. SV Node applies the same floor
// in UpdateTime (max of the parent's median-time-past+1 and adjusted time).
//
// Headers are fetched by parent hash rather than height so a candidate built
// on a side chain gets the median of its own chain. The batched fetch is
// verified to be anchored at the parent and correctly linked; on any mismatch
// (the store's batched lookup ranges over main-chain flags and can race a
// reorg) candidateTime falls back to a hash-keyed walk of the parent chain,
// whose per-hash reads are immutable and immune to that race — the same
// two-step strategy as the candidate-parent MTP helpers in subtreevalidation
// and legacy/netsync. Only when both attempts fail does the candidate request
// error, failing that one miner poll.
func (b *BlockAssembler) candidateTime(ctx context.Context, parentHeader *model.BlockHeader) (int64, error) {
	if parentHeader == nil {
		return 0, errors.NewProcessingError("[candidateTime] nil parent header")
	}

	parentHash := parentHeader.Hash()

	run, batchedErr := b.batchedParentChain(ctx, parentHash)
	if batchedErr != nil {
		walked, walkErr := b.walkParentChain(ctx, parentHash, blockchain.MedianTimeBlocks)
		if walkErr != nil {
			return 0, errors.NewProcessingError("[candidateTime] batched header fetch failed (%v); hash-keyed parent-chain walk also failed for %s", batchedErr, parentHash.String(), walkErr)
		}

		run = walked
	}

	timestamps := make([]time.Time, len(run))
	for i, header := range run {
		timestamps[i] = time.Unix(int64(header.Timestamp), 0)
	}

	medianTimestamp, err := model.CalculateMedianTimestamp(timestamps)
	if err != nil {
		return 0, errors.NewProcessingError("[candidateTime] failed to calculate median timestamp for parent %s", parentHash.String(), err)
	}

	timeNow := time.Now().Unix()

	if minTime := medianTimestamp.Unix() + 1; timeNow < minTime {
		// Warn once per tip rather than once per call: miners poll several
		// times a second, and while a clock-lag condition lasts every poll
		// floors, which would flood the log with identical lines.
		if prev := b.mtpFloorWarnedTip.Swap(parentHash); prev == nil || !prev.IsEqual(parentHash) {
			b.logger.Warnf("[candidateTime] local clock %d is at or below the parent chain's median-time-past %d, flooring candidate time to %d for blocks on parent %s", timeNow, medianTimestamp.Unix(), minTime, parentHash.String())
		}

		timeNow = minTime
	}

	return timeNow, nil
}

// batchedParentChain fetches up to MedianTimeBlocks headers ending at
// parentHash in one batched call and verifies the run is anchored at the
// parent and correctly linked. A mismatch means the batched lookup raced a
// chain reorganisation (its fast path is a height-range scan over main-chain
// flags, not an atomic parent-chain walk); the caller falls back to
// walkParentChain when that happens.
func (b *BlockAssembler) batchedParentChain(ctx context.Context, parentHash *chainhash.Hash) ([]*model.BlockHeader, error) {
	headers, _, err := b.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return nil, errors.NewProcessingError("failed to fetch parent-chain headers for %s", parentHash.String(), err)
	}

	if len(headers) == 0 || headers[0] == nil || !headers[0].Hash().IsEqual(parentHash) {
		return nil, errors.NewProcessingError("returned chain head does not match requested parent %s", parentHash.String())
	}

	for i := 1; i < len(headers); i++ {
		if headers[i] == nil {
			return nil, errors.NewProcessingError("nil header at depth %d below parent %s", i, parentHash.String())
		}

		if !headers[i-1].HashPrevBlock.IsEqual(headers[i].Hash()) {
			return nil, errors.NewProcessingError("parent-chain link broken at depth %d below parent %s", i, parentHash.String())
		}
	}

	return headers, nil
}

// walkParentChain fetches up to depth headers ending at startHash by following
// HashPrevBlock one header at a time. Each read is keyed by hash — immutable
// once stored — so the result cannot be poisoned by a reorg happening between
// reads, at the cost of one round-trip per header. Unlike the equivalents in
// subtreevalidation and legacy/netsync it tolerates reaching the start of the
// chain (returning fewer than depth headers), because block assembly runs from
// genesis on fresh networks; the validator applies the median rule over
// however many headers exist in the same way.
func (b *BlockAssembler) walkParentChain(ctx context.Context, startHash *chainhash.Hash, depth uint64) ([]*model.BlockHeader, error) {
	headers := make([]*model.BlockHeader, 0, depth)
	cur := startHash

	for i := uint64(0); i < depth; i++ {
		header, _, err := b.blockchainClient.GetBlockHeader(ctx, cur)
		if err != nil {
			return nil, errors.NewProcessingError("failed to fetch header %s at depth %d", cur.String(), i, err)
		}

		if header == nil {
			return nil, errors.NewProcessingError("nil header for %s at depth %d", cur.String(), i)
		}

		headers = append(headers, header)

		// The genesis block's HashPrevBlock is all zeroes: the chain start.
		if header.HashPrevBlock == nil || header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
			break
		}

		cur = header.HashPrevBlock
	}

	return headers, nil
}
