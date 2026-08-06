package blockassembly

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
)

// maxFutureBlockTime mirrors the two-hours-in-the-future ceiling that
// model.Block CheckHeaderContextual applies to every header unconditionally —
// unlike the median-time rule, that check needs no currentChain, so the local
// submit path does enforce it (model/Block.go).
const maxFutureBlockTime = 2 * time.Hour

// mtpFloorEntry is the median-time-past floor for one parent block. The median
// of the 11 headers ending at a given hash can never change — headers are
// immutable once stored — so it is computed once per parent and reused for
// every miner poll on that tip. warned latches the "floor engaged" log line so
// it fires once per parent rather than once per poll.
type mtpFloorEntry struct {
	parent  chainhash.Hash
	minTime int64
	warned  atomic.Bool
}

// candidateTime returns the timestamp to stamp on a mining candidate built on
// parentHeader: the local wall clock, floored at median-time-past+1 of the
// parent chain. Block validation rejects any block whose timestamp is not
// strictly greater than the median timestamp of the previous 11 blocks
// (model.Block CheckHeaderContextual), so without the floor a lagging local
// clock — or a run of forward-stamped blocks dragging the median above
// wall-clock time — makes the assembler hand miners a candidate that the
// network refuses once mined. SV Node applies the same floor in UpdateTime
// (max of the parent's median-time-past+1 and adjusted time).
//
// The two consensus bounds on a block timestamp are a pair, and this function
// honours both. The median rule is the floor; the two-hours-in-the-future rule
// is the ceiling. A local clock lagging far enough behind the network puts
// median-time-past+1 above now+2h, leaving no timestamp that satisfies both.
// Flooring anyway would hand miners a candidate whose every solution fails
// block.Valid on our own submit path, and that failure is treated as a
// subtree-processor fault: it deletes the job and calls Reset, so each solution
// would trigger a full block-assembly reset. Rather than emit unmineable work,
// candidateTime fails the poll and names the clock skew. Chains with
// GenerateSupported downgrade the median rule to a warning, so there the
// wall-clock candidate is still valid and is served instead of failing.
//
// Availability is preserved everywhere else. The floor only changes the outcome
// when the wall clock is at or below median-time-past, which is rare, whereas
// the lookup it needs is a blockchain round-trip that can fail at any time. A
// lookup failure therefore degrades to the bare wall clock with an error log —
// exactly the pre-PR behaviour — rather than failing the poll. That matters
// most for generateEmptyBlockCandidate, whose whole purpose is to keep handing
// miners work while a block is being processed.
//
// Headers are fetched by parent hash rather than height so a candidate built on
// a side chain gets the median of its own chain.
func (b *BlockAssembler) candidateTime(ctx context.Context, parentHeader *model.BlockHeader) (int64, error) {
	if parentHeader == nil {
		return 0, errors.NewProcessingError("[candidateTime] nil parent header")
	}

	parentHash := parentHeader.Hash()
	timeNow := time.Now().Unix()

	entry := b.mtpFloor(ctx, parentHash)
	if entry == nil {
		// mtpFloor has already logged the cause. Serve the wall clock rather
		// than stopping mining on a blockchain hiccup.
		return timeNow, nil
	}

	// Check the ceiling before applying the floor: when the two cross there is
	// no valid timestamp at all and flooring makes things worse, not better.
	if maxTime := timeNow + int64(maxFutureBlockTime/time.Second); entry.minTime > maxTime {
		if b.generateSupported() {
			b.logger.Warnf("[candidateTime] parent %s median-time-past floor %d exceeds the two-hour future bound %d; serving the wall clock %d because this chain treats the median-time rule as advisory", parentHash.String(), entry.minTime, maxTime, timeNow)

			return timeNow, nil
		}

		return 0, errors.NewProcessingError("[candidateTime] parent %s median-time-past floor %d exceeds the two-hour future bound %d, so no timestamp satisfies both consensus rules; the local clock is skewed by at least %d seconds", parentHash.String(), entry.minTime, maxTime, entry.minTime-maxTime)
	}

	if timeNow < entry.minTime {
		if entry.warned.CompareAndSwap(false, true) {
			b.logger.Warnf("[candidateTime] local clock %d is at or below the parent chain's median-time-past; flooring candidate time to %d for blocks on parent %s", timeNow, entry.minTime, parentHash.String())
		}

		timeNow = entry.minTime
	}

	return timeNow, nil
}

// MinCandidateTime returns the median-time-past floor candidateTime computed for
// parentHash, and whether it is known. It is a pure memo read: the entry is warm
// whenever a candidate was just built on that parent, so the submit path can
// reject a miner-supplied nTime below the consensus floor without a round-trip.
// A false second return means no floor was established for that parent (the
// candidate was served on the degraded wall-clock path, or the tip has since
// moved twice), in which case the submit path has nothing to enforce and behaves
// as it did before the floor existed.
func (b *BlockAssembler) MinCandidateTime(parentHash *chainhash.Hash) (int64, bool) {
	if parentHash == nil {
		return 0, false
	}

	for i := range b.mtpFloorMemo {
		if entry := b.mtpFloorMemo[i].Load(); entry != nil && entry.parent.IsEqual(parentHash) {
			return entry.minTime, true
		}
	}

	return 0, false
}

// mtpFloor returns the memoized median-time-past floor for parentHash,
// computing it on a miss. It returns nil — having logged the cause — when the
// floor cannot be established, leaving the caller to decide how to degrade.
//
// The memo has two slots because the two candidate paths key on different
// parents: GetMiningCandidate's busy branch uses the blockchain service's tip
// while the main path uses the subtree processor's precomputed parent. A single
// slot would thrash whenever those two alternate, refetching on every poll and
// re-firing the floor warning each time.
func (b *BlockAssembler) mtpFloor(ctx context.Context, parentHash *chainhash.Hash) *mtpFloorEntry {
	for i := range b.mtpFloorMemo {
		if entry := b.mtpFloorMemo[i].Load(); entry != nil && entry.parent.IsEqual(parentHash) {
			return entry
		}
	}

	run, batchedErr := b.batchedParentChain(ctx, parentHash)
	if batchedErr != nil {
		walked, walkErr := b.walkParentChain(ctx, parentHash, blockchain.MedianTimeBlocks)
		if walkErr != nil {
			b.logger.Errorf("[candidateTime] cannot establish the median-time-past floor for parent %s: batched header fetch failed (%v) and the hash-keyed parent-chain walk also failed (%v); serving an unfloored candidate time", parentHash.String(), batchedErr, walkErr)

			return nil
		}

		// Name the cause of the slower path rather than discarding the batched
		// error silently: the walk costs one round-trip per header, so an
		// operator seeing the extra latency needs to know why it engaged.
		b.logger.Warnf("[candidateTime] batched header fetch for parent %s was unusable (%v); fell back to the hash-keyed parent-chain walk", parentHash.String(), batchedErr)

		run = walked
	}

	timestamps := make([]time.Time, len(run))
	for i, header := range run {
		timestamps[i] = time.Unix(int64(header.Timestamp), 0)
	}

	// Defensive only: CalculateMedianTimestamp errors on an empty slice, and both
	// producers above guarantee at least one header.
	medianTimestamp, err := model.CalculateMedianTimestamp(timestamps)
	if err != nil {
		b.logger.Errorf("[candidateTime] failed to calculate the median timestamp for parent %s over %d headers: %v; serving an unfloored candidate time", parentHash.String(), len(run), err)

		return nil
	}

	entry := &mtpFloorEntry{parent: *parentHash, minTime: medianTimestamp.Unix() + 1}

	slot := b.mtpFloorMemoNext.Add(1) % uint64(len(b.mtpFloorMemo))
	b.mtpFloorMemo[slot].Store(entry)

	return entry
}

// generateSupported reports whether this chain lets blocks be generated quickly,
// in which case model.Block CheckHeaderContextual downgrades a median-time
// violation from an error to a warning. Guarded because a nil Settings deref
// past the struct's guard page is an unrecoverable fault.
func (b *BlockAssembler) generateSupported() bool {
	return b.settings != nil && b.settings.ChainCfgParams != nil && b.settings.ChainCfgParams.GenerateSupported
}

// batchedParentChain fetches the MedianTimeBlocks headers ending at parentHash
// in one batched call and verifies the run is complete, anchored at the parent
// and correctly linked. Any mismatch means the batched lookup cannot be trusted
// — its fast path is a height-range scan over main-chain flags and its CTE
// stops early on an unresolved parent_id, neither of which is an atomic
// parent-chain walk — and the caller falls back to walkParentChain.
func (b *BlockAssembler) batchedParentChain(ctx context.Context, parentHash *chainhash.Hash) ([]*model.BlockHeader, error) {
	headers, _, err := b.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return nil, errors.NewProcessingError("failed to fetch parent-chain headers for %s", parentHash.String(), err)
	}

	if len(headers) == 0 || headers[0] == nil || !headers[0].Hash().IsEqual(parentHash) {
		return nil, errors.NewProcessingError("returned chain head does not match requested parent %s", parentHash.String())
	}

	if uint64(len(headers)) > blockchain.MedianTimeBlocks {
		return nil, errors.NewProcessingError("batched run below parent %s returned %d headers, more than the %d requested", parentHash.String(), len(headers), blockchain.MedianTimeBlocks)
	}

	for i := 1; i < len(headers); i++ {
		if headers[i] == nil {
			return nil, errors.NewProcessingError("nil header at depth %d below parent %s", i, parentHash.String())
		}

		if !headers[i-1].HashPrevBlock.IsEqual(headers[i].Hash()) {
			return nil, errors.NewProcessingError("parent-chain link broken at depth %d below parent %s", i, parentHash.String())
		}
	}

	// The anchor and link checks cannot see a run truncated at its oldest end:
	// such a run is still anchored at the parent and still correctly linked, but
	// its median is taken over a narrower window and is biased high, which feeds
	// straight into the two-hour ceiling above. A short run is only legitimate
	// when it reaches the start of the chain.
	if uint64(len(headers)) < blockchain.MedianTimeBlocks && !isChainStart(headers[len(headers)-1]) {
		return nil, errors.NewProcessingError("short parent-chain run (%d of %d) below parent %s does not reach the chain start", len(headers), blockchain.MedianTimeBlocks, parentHash.String())
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
		if cur == nil {
			return nil, errors.NewProcessingError("nil parent hash at depth %d below %s", i, startHash.String())
		}

		header, _, err := b.blockchainClient.GetBlockHeader(ctx, cur)
		if err != nil {
			return nil, errors.NewProcessingError("failed to fetch header %s at depth %d", cur.String(), i, err)
		}

		if header == nil {
			return nil, errors.NewProcessingError("nil header for %s at depth %d", cur.String(), i)
		}

		headers = append(headers, header)

		if isChainStart(header) {
			break
		}

		cur = header.HashPrevBlock
	}

	return headers, nil
}

// isChainStart reports whether header is the start of the chain: the genesis
// block's HashPrevBlock is all zeroes, so nothing links below it.
func isChainStart(header *model.BlockHeader) bool {
	return header.HashPrevBlock == nil || header.HashPrevBlock.IsEqual(&chainhash.Hash{})
}
