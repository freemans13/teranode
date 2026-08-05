package blockassembly

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
)

// candidateTime returns the timestamp to stamp on a mining candidate built on
// parentHeader: the local wall clock, floored at median-time-past+1 of the
// parent chain. Block validation (model.Block CheckHeaderContextual) rejects
// any block whose timestamp is not strictly greater than the median timestamp
// of the previous 11 blocks, so without the floor a lagging local clock — or a
// run of forward-stamped blocks dragging the median above wall-clock time —
// makes the assembler hand miners a candidate the node itself rejects once
// mined. SV Node applies the same floor in UpdateTime (max of the parent's
// median-time-past+1 and adjusted time).
//
// Headers are fetched by parent hash rather than height so a candidate built
// on a side chain gets the median of its own chain. The returned run is
// verified to be anchored at the parent and correctly linked, mirroring the
// candidate-parent MTP helpers in subtreevalidation and legacy/netsync; a
// mismatch means the chain reorganised between fetch and use, and the error
// simply fails this poll — the miner's next GetMiningCandidate retries.
func (b *BlockAssembler) candidateTime(ctx context.Context, parentHeader *model.BlockHeader) (int64, error) {
	parentHash := parentHeader.Hash()

	headers, _, err := b.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return 0, errors.NewProcessingError("[candidateTime] failed to fetch parent-chain headers for %s", parentHash.String(), err)
	}

	if len(headers) == 0 || headers[0] == nil || !headers[0].Hash().IsEqual(parentHash) {
		return 0, errors.NewProcessingError("[candidateTime] returned chain head does not match requested parent %s (possible reorg between fetch and use)", parentHash.String())
	}

	timestamps := make([]time.Time, 0, len(headers))

	for i, header := range headers {
		if header == nil {
			return 0, errors.NewProcessingError("[candidateTime] nil header at depth %d below parent %s", i, parentHash.String())
		}

		if i > 0 && !headers[i-1].HashPrevBlock.IsEqual(header.Hash()) {
			return 0, errors.NewProcessingError("[candidateTime] parent-chain link broken at depth %d below parent %s (possible reorg between fetch and use)", i, parentHash.String())
		}

		timestamps = append(timestamps, time.Unix(int64(header.Timestamp), 0))
	}

	medianTimestamp, err := model.CalculateMedianTimestamp(timestamps)
	if err != nil {
		return 0, errors.NewProcessingError("[candidateTime] failed to calculate median timestamp for parent %s", parentHash.String(), err)
	}

	timeNow := time.Now().Unix()

	if minTime := medianTimestamp.Unix() + 1; timeNow < minTime {
		b.logger.Warnf("[candidateTime] local clock %d is at or below the parent chain's median-time-past %d, flooring candidate time to %d", timeNow, medianTimestamp.Unix(), minTime)
		timeNow = minTime
	}

	return timeNow, nil
}
