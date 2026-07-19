// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package netsync provides network synchronization functionality for the legacy Bitcoin protocol.
// It handles peer coordination, block synchronization, and transaction relay operations.
package netsync

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	teranodeblockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
)

const (
	// defaultMaxInFlightBlocks is the default maximum number of blocks that
	// should be in the request queue for headers-first mode. This is the
	// starting value for small blocks, and will be dynamically adjusted down
	// based on observed block sizes to avoid memory issues with large blocks.
	defaultMaxInFlightBlocks = 20

	// minInFlightBlockWeight is the minimum prefetch-budget weight charged for an
	// admitted block, regardless of how small it serializes. Each in-flight block
	// costs a fixed overhead beyond its bytes — an awaitBlockResult goroutine
	// (stack), a reply channel, and the decoded block wrapper. Charging only the
	// serialized size would let a flood of minimal (e.g. ~81-byte, zero-tx) blocks
	// admit a huge number of concurrent goroutines within the byte budget; the
	// floor bounds the in-flight block count (≈ budget/minInFlightBlockWeight) and
	// therefore the goroutine count. It is well below any real small-block size,
	// so it never reduces prefetch depth for legitimate traffic.
	minInFlightBlockWeight = 64 * 1024

	// maxBlockQueueSlots caps the block-queue channel capacity so a misconfigured
	// (e.g. multi-TB) prefetch budget cannot size an enormous channel backing
	// array. 65536 slots covers budgets up to 4 GiB at the weight floor before the
	// clamp binds; the channel holds pointers, so this is ~512 KiB.
	maxBlockQueueSlots = 65536

	// defaultBlockProcessingStallTimeout bounds how long localReadBackpressured
	// keeps suppressing the sync-peer stall check while the block backlog is
	// non-empty but not advancing (see lastBacklogProgress). It is only the
	// fallback for settings.Legacy.PeerProcessingTimeout — the pre-prefetch
	// per-message watchdog whose coverage this progress-aware rule restores —
	// used when a SyncManager has no settings wired (unit tests) or the setting
	// is unset. Kept equal to that setting's own default so behaviour matches
	// production when it is configured.
	defaultBlockProcessingStallTimeout = 3 * time.Minute

	// maxNetworkViolations is the max number of network violations a
	// sync peer can have before a new sync peer is found.
	maxNetworkViolations = 3

	// maxRejectedTxns is the maximum number of rejected transactions
	// hashes to store in memory.
	maxRejectedTxns = 10_000

	// maxRequestedBlocks is the maximum number of requested block
	// hashes to store in memory.
	maxRequestedBlocks = wire.MaxInvPerMsg

	// maxRequestedTxns is the maximum number of requested transactions
	// hashes to store in memory.
	maxRequestedTxns = wire.MaxInvPerMsg

	// maxLastBlockTime is the longest time in seconds that we will
	// stay with a sync peer while below the current blockchain height.
	// Set to 3 minutes.
	maxLastBlockTime = 60 * 3 * time.Second

	// maxMsgQueuePerPeer is the maximum number of messages that can be
	// queued for a peer. This is the size if the msgChan buffer.
	maxMsgQueueSize = 10_000

	// syncPeerTickerInterval is how often we check the current
	// syncPeer. Set to 30 seconds.
	syncPeerTickerInterval = 30 * time.Second

	// blockAssemblyHeightPollInterval is how often the background poller
	// refreshes the cached block-assembly height. The per-block coinbase-maturity
	// check reads this cache atomically instead of doing a gRPC round-trip on the
	// serial drain path. 250ms is far tighter than the 100-block maturity window,
	// so the cache can never fall stale enough to matter.
	blockAssemblyHeightPollInterval = 250 * time.Millisecond

	// windowMaturityRecheckInterval is how often the below-checkpoint maturity
	// wait re-reads the poller-refreshed cached block-assembly height while it is
	// behind the bound. A short fixed interval (no gRPC in the loop — the
	// blockAssemblyHeightPoller does that in the background) lets the parallel
	// window re-engage within one interval of block assembly advancing, instead
	// of the coarse 20ms->80ms->...->5s exponential backoff of the fresh-gRPC
	// WaitForBlockAssemblyReady, which produced a bursty "+100 blocks every ~5s"
	// lockstep rather than a smooth release at block-assembly's rate.
	windowMaturityRecheckInterval = 200 * time.Millisecond

	// windowMaturityMaxWait bounds the below-checkpoint cache-poll recheck loop so
	// a GENUINE block-assembly stall (the cache never advances) is still detected
	// and escalated instead of looping forever. On expiry the wait returns an
	// error so the caller's existing recover/escalation path fires, preserving the
	// stall-detection semantics of the old exponential path (which capped at 100
	// retries). 30s is far longer than any legitimate block-assembly catch-up
	// within the 100-block maturity window, yet escalates a true stall promptly.
	windowMaturityMaxWait = 30 * time.Second
)

// zeroHash is the zero-value hash (all zeros).  It is defined as a convenience.
var zeroHash chainhash.Hash

// ErrDuplicateBlockInFlight is the benign sentinel AcquireBlockPrefetch returns
// when the requested block hash is already admitted (or parked waiting for
// budget): a duplicate is dropped at admission rather than reserving a second
// slice of budget. The single production caller (OnBlock) matches it with
// errors.Is and drops the duplicate without disconnecting — it is the only
// ServiceError AcquireBlockPrefetch ever returns, so the code-based match is
// unambiguous there.
var ErrDuplicateBlockInFlight = errors.NewServiceError("duplicate block already in flight")

// BlockProcessingErrorIsPeerFault reports whether a block-processing error
// proves the DELIVERING PEER sent invalid bytes — the ONLY class of failure
// that justifies disconnecting it. Consensus-invalid blocks (bad proof-of-work,
// bad merkle root, the CVE-2012-2459 duplicate-transaction mutation, height /
// checkpoint mismatch) all surface as errors.ErrBlockInvalid, so that single
// check is the allowlist.
//
// Everything else — the block-assembly maturity gate timing out
// (waitForBlockAssemblyCachePoll, a ProcessingError), storage/service hiccups,
// transient parent-not-yet-committed retries — is a LOCAL condition. The peer
// did nothing wrong, so the correct response is to KEEP the peer and re-fetch
// the block (see requeueFailedBlock), never to execute the peer. Blaming the
// peer for local backpressure is exactly the failure that froze mainnet IBD for
// ~3.5h on 2026-07-15: a false gate timeout disconnected the sync peer, rotation
// found no replacement, and the node starved to zero peers.
//
// This predicate is the SINGLE SOURCE for that classification, shared by the
// disconnect decision (legacy.shouldDisconnectOnBlockErr) and the requeue
// decision (the block-handler drain loop), so the two can never drift.
func BlockProcessingErrorIsPeerFault(err error) bool {
	return err != nil && errors.Is(err, errors.ErrBlockInvalid)
}

// requeueFailedBlock puts a block hash back into the re-fetch set after a
// TOLERATED (non-peer-fault) processing failure, so the download pipeline
// re-requests it instead of silently dropping it. handleBlockPreamble removes
// an arriving block from ALL in-flight tracking (requestedBlocks, assignedTo,
// assignedAt, refetchBlocks) BEFORE processing, and the monotonic startHeader
// cursor never rewinds, so without this requeue a tolerated failure leaves the
// block permanently un-fetched — a silent wedge of the strictly-ascending
// commit pipeline. Safe from any goroutine: refetchBlocks is guarded by
// assignedMu, and drainRefetchBlocks consumes the set on the next fetch pass.
func (sm *SyncManager) requeueFailedBlock(blockHash chainhash.Hash) {
	sm.assignedMu.Lock()
	defer sm.assignedMu.Unlock()
	if sm.refetchBlocks == nil {
		sm.refetchBlocks = make(map[chainhash.Hash]struct{})
	}
	sm.refetchBlocks[blockHash] = struct{}{}
}

// newPeerMsg signifies a newly connected peer to the block handler.
type newPeerMsg struct {
	peer  *peerpkg.Peer
	reply chan struct{}
}

// blockMsg packages a bitcoin block message and the peer it came from together
// so the block handler has access to that information.
type blockMsg struct {
	block *bsvutil.Block
	peer  *peerpkg.Peer
	reply chan error
}

// headersMsg packages a bitcoin headers message and the peer it came from
// together so the block handler has access to that information.
type headersMsg struct {
	headers *wire.MsgHeaders
	peer    *peerpkg.Peer
}

// donePeerMsg signifies a newly disconnected peer to the block handler.
type donePeerMsg struct {
	peer  *peerpkg.Peer
	reply chan struct{}
}

// txMsg packages a bitcoin tx message and the peer it came from together
// so the block handler has access to that information.
type txMsg struct {
	tx    *bsvutil.Tx
	peer  *peerpkg.Peer
	reply chan struct{}
}

// getSyncPeerMsg is a message type to be sent across the message channel for
// retrieving the current sync peer.
type getSyncPeerMsg struct {
	reply chan int32
}

// isCurrentMsg is a message type to be sent across the message channel for
// requesting whether or not the sync manager believes it is synced with the
// currently connected peers.
type isCurrentMsg struct {
	reply chan bool
}

// pauseMsg is a message type to be sent across the message channel for
// pausing the sync manager.  This effectively provides the caller with
// exclusive access over the manager until a receive is performed on the
// unpause channel.
type pauseMsg struct {
	unpause <-chan struct{}
}

// headerNode is used as a node in a list of headers that are linked together
// between checkpoints.
type headerNode struct {
	height int32
	hash   *chainhash.Hash
}

// peerSyncState stores additional information that the SyncManager tracks
// about a peer.
type peerSyncState struct {
	syncCandidate   bool
	requestQueue    *txmap.SyncedSlice[wire.InvVect]
	requestedTxns   *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	requestedBlocks *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
}

// syncPeerState stores additional info about the sync peer.
type syncPeerState struct {
	mu                sync.RWMutex // Protects all fields
	recvBytes         uint64
	recvBytesLastTick uint64
	// assocReadBytes tracks byte-granular read progress across the sync peer's
	// whole association (GENERAL + DATA1). Unlike recvBytes (the GENERAL peer's
	// message-granular total) it advances while a large block is still
	// streaming in on DATA1, so it can tell an active fat-block download apart
	// from a stalled peer.
	assocReadBytes         uint64
	assocReadBytesLastTick uint64
	lastBlockTime          time.Time
	violations             int
	ticks                  uint64
	// silentTicks counts consecutive 30s samples in which the association
	// moved ZERO bytes. During headers-first IBD the sync peer always owes us
	// data (headers or blocks), so sustained total silence means it is stalled
	// — but the only detector was the 3-minute last-block-time window, which
	// is the dominant dead-air cost at checkpoint-block stalls (the direct
	// path's sole recovery is rotation). Reset on any byte movement, so a peer
	// streaming the post-rotation header replay (~160KB/batch) or a fat block
	// can never accrue silent ticks — thrash-proof by construction.
	silentTicks int
}

// validNetworkSpeed checks if the peer is slow and
// returns an integer representing the number of network
// violations the sync peer has.
func (sps *syncPeerState) validNetworkSpeed(minSyncPeerNetworkSpeed uint64) int {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	// Fresh sync peer. We need another tick.
	if sps.ticks == 0 {
		return 0
	}

	// Number of bytes received in the last tick.
	recvDiff := sps.recvBytes - sps.recvBytesLastTick

	// If the peer was below the threshold, mark a violation and return.
	if recvDiff/uint64(syncPeerTickerInterval.Seconds()) < minSyncPeerNetworkSpeed {
		sps.violations++
		return sps.violations
	}

	// No violation found, reset the violation counter.
	sps.violations = 0

	return sps.violations
}

type orphanTxAndParents struct {
	tx      *bt.Tx
	parents *txmap.SyncedMap[chainhash.Hash, struct{}] // map of parent tx hashes
	addedAt time.Time
}

// updateNetwork updates the received bytes. Just tracks 2 ticks
// worth of network bandwidth.
func (sps *syncPeerState) updateNetwork(syncPeer *peerpkg.Peer) {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	sps.ticks++
	sps.recvBytesLastTick = sps.recvBytes
	sps.recvBytes = syncPeer.BytesReceived()

	sps.assocReadBytesLastTick = sps.assocReadBytes
	sps.assocReadBytes = syncPeer.AssociationReadBytes()

	// Silence accounting: a tick with zero forward byte movement on the whole
	// association (a shrink means a stream died — also not progress). Needs one
	// prior sample so a fresh sync peer is never counted silent on its first tick.
	if sps.ticks > 1 && sps.assocReadBytes <= sps.assocReadBytesLastTick {
		sps.silentTicks++
	} else {
		sps.silentTicks = 0
	}
}

// silentTickCount returns the current consecutive-silent-tick count.
func (sps *syncPeerState) silentTickCount() int {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	return sps.silentTicks
}

// resetSilentTicks clears silence accounting. Used while the node is
// self-backpressured: zero throughput then measures our own validation speed,
// not the peer's health, and must not accrue toward rotation.
func (sps *syncPeerState) resetSilentTicks() {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	sps.silentTicks = 0
}

// hasHealthyDownloadThroughput reports whether the sync peer's association
// pulled in data over the last tick at or above minSyncPeerNetworkSpeed. It is
// used to keep a sync peer that is actively downloading a large block — which
// streams in on DATA1 and so completes no block within maxLastBlockTime — from
// being rotated as if it were stalled. It does not mutate violation state.
func (sps *syncPeerState) hasHealthyDownloadThroughput(minSyncPeerNetworkSpeed uint64) bool {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	// Need at least one prior sample to compute a delta.
	if sps.ticks == 0 {
		return false
	}

	// Association.ReadBytes sums over the streams present at sample time. If a
	// stream (e.g. DATA1) was removed between samples the sum drops, so guard
	// the unsigned subtraction: a decrease means a stream just died, which is
	// the opposite of healthy progress — treat it as no throughput.
	if sps.assocReadBytes < sps.assocReadBytesLastTick {
		return false
	}

	recvDiff := sps.assocReadBytes - sps.assocReadBytesLastTick

	// Require actual bytes to have moved: a peer that delivered nothing is not
	// "downloading", regardless of how the speed threshold is configured (it may
	// be 0, which would otherwise make any rate pass).
	if recvDiff == 0 {
		return false
	}

	return recvDiff/uint64(syncPeerTickerInterval.Seconds()) >= minSyncPeerNetworkSpeed
}

// updateLastBlockTime updates the last block time
func (sps *syncPeerState) updateLastBlockTime() {
	sps.mu.Lock()
	defer sps.mu.Unlock()
	sps.lastBlockTime = time.Now()
}

// getLastBlockTime returns the last block time
func (sps *syncPeerState) getLastBlockTime() time.Time {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	return sps.lastBlockTime
}

// getViolations returns the current violation count
func (sps *syncPeerState) getViolations() int {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	return sps.violations
}

// setViolations sets the violation count
func (sps *syncPeerState) setViolations(v int) {
	sps.mu.Lock()
	defer sps.mu.Unlock()
	sps.violations = v
}

type TxHashAndFee struct {
	TxHash chainhash.Hash
	Fee    uint64
	Size   uint64
}

// Default in-flight fetch budgets used when the tracker is constructed without
// explicit budgets (e.g. via newBlockSizeTracker in tests). Production wires
// these from settings via newBlockSizeTrackerWithBudgets.
const (
	// defaultInFlightTxBudget bounds in-flight WORK by transaction count. Sized
	// in the tens-of-thousands so tiny (1-2 tx) blocks stream many-in-flight and
	// keep the sync peer's feed busy, while fat (~2500 tx) blocks resolve to a
	// small handful in flight. 50k tx ≈ 20 blocks of ~2500 tx.
	defaultInFlightTxBudget = 50000

	// defaultInFlightByteBudgetFraction is the fraction of GOMEMLIMIT allowed as
	// in-flight fetch bytes. 0.25 keeps in-flight memory to a quarter of the
	// process budget even when tx counts under-represent memory (few huge txs),
	// leaving headroom for validation, the window path, and the runtime.
	defaultInFlightByteBudgetFraction = 0.25

	// maxInFlightBlocksCap is the hard upper bound on in-flight blocks. It stops
	// tiny (1-tx) blocks from producing an absurd request count regardless of
	// the tx budget.
	maxInFlightBlocksCap = 1024

	// noSampleInFlightDefault is the safe fallback used before any block sample
	// has been observed (avgTxCount == 0). Matches the previous "small blocks"
	// aggressive default so cold-start behaviour is unchanged.
	noSampleInFlightDefault = 20
)

// blockSizeTracker tracks recent block sizes and transaction counts, and
// dynamically bounds the number of in-flight block fetches by a transaction
// WORK budget (with a byte-budget safety clamp), rather than a fixed block
// count. This lets tiny blocks stream many-in-flight (eliminating feed-idle
// gaps) while fat blocks stay few-in-flight to bound memory.
type blockSizeTracker struct {
	mu             sync.RWMutex
	recentSizes    []int64 // last N block sizes in bytes
	recentTxCounts []int64 // last N block transaction counts
	avgSize        int64   // rolling average block size (bytes)
	avgTxCount     int64   // rolling average transaction count per block
	maxSamples     int     // number of samples to track

	// inFlightTxBudget bounds in-flight blocks by total average transactions.
	inFlightTxBudget int
	// inFlightByteBudget bounds in-flight blocks by total average bytes.
	inFlightByteBudget int64
}

// newBlockSizeTracker creates a new block size tracker with default budgets.
// Used by tests and any caller that does not need settings-derived budgets.
func newBlockSizeTracker(maxSamples int) *blockSizeTracker {
	return newBlockSizeTrackerWithBudgets(maxSamples, defaultInFlightTxBudget,
		int64(float64(effectiveGOMEMLIMIT())*defaultInFlightByteBudgetFraction))
}

// newBlockSizeTrackerWithBudgets creates a tracker with explicit in-flight
// budgets. A non-positive budget falls back to its default so a mis-set value
// can never disable the bound entirely.
func newBlockSizeTrackerWithBudgets(maxSamples, txBudget int, byteBudget int64) *blockSizeTracker {
	if txBudget <= 0 {
		txBudget = defaultInFlightTxBudget
	}

	if byteBudget <= 0 {
		byteBudget = int64(float64(effectiveGOMEMLIMIT()) * defaultInFlightByteBudgetFraction)
	}

	return &blockSizeTracker{
		recentSizes:        make([]int64, 0, maxSamples),
		recentTxCounts:     make([]int64, 0, maxSamples),
		maxSamples:         maxSamples,
		avgSize:            0,
		avgTxCount:         0,
		inFlightTxBudget:   txBudget,
		inFlightByteBudget: byteBudget,
	}
}

// addBlockStats records a new block's size (bytes) and transaction count,
// updating both rolling averages. Called on the serial block-drain goroutine.
func (bst *blockSizeTracker) addBlockStats(size, txCount int64) {
	bst.mu.Lock()
	defer bst.mu.Unlock()

	bst.recentSizes = append(bst.recentSizes, size)
	bst.recentTxCounts = append(bst.recentTxCounts, txCount)

	if len(bst.recentSizes) > bst.maxSamples {
		bst.recentSizes = bst.recentSizes[1:] // keep last maxSamples
	}

	if len(bst.recentTxCounts) > bst.maxSamples {
		bst.recentTxCounts = bst.recentTxCounts[1:] // keep last maxSamples
	}

	// Rolling average block size.
	var sizeSum int64
	for _, s := range bst.recentSizes {
		sizeSum += s
	}

	if len(bst.recentSizes) > 0 {
		bst.avgSize = sizeSum / int64(len(bst.recentSizes))
	}

	// Rolling average transaction count.
	var txSum int64
	for _, c := range bst.recentTxCounts {
		txSum += c
	}

	if len(bst.recentTxCounts) > 0 {
		bst.avgTxCount = txSum / int64(len(bst.recentTxCounts))
	}
}

// getAverageSize returns the current rolling average block size in bytes.
func (bst *blockSizeTracker) getAverageSize() int64 {
	bst.mu.RLock()
	defer bst.mu.RUnlock()
	return bst.avgSize
}

// getAverageTxCount returns the current rolling average transactions per block.
func (bst *blockSizeTracker) getAverageTxCount() int64 {
	bst.mu.RLock()
	defer bst.mu.RUnlock()
	return bst.avgTxCount
}

// calculateMaxInFlightBlocks returns the recommended max in-flight block
// fetches, bounding by a transaction WORK budget with a byte-budget safety
// clamp:
//
//   - txBound   = inFlightTxBudget / max(avgTxCount, 1)
//   - byteBound = inFlightByteBudget / max(avgSize, 1)
//   - result    = clamp(min(txBound, byteBound), 1, maxInFlightBlocksCap)
//
// The byte clamp keeps in-flight memory bounded even when the tx count is
// misleading (few but huge transactions). Before any sample exists
// (avgTxCount == 0) it returns a safe default and never divides by zero.
func (bst *blockSizeTracker) calculateMaxInFlightBlocks() int {
	bst.mu.RLock()
	avgTxCount := bst.avgTxCount
	avgSize := bst.avgSize
	txBudget := bst.inFlightTxBudget
	byteBudget := bst.inFlightByteBudget
	bst.mu.RUnlock()

	// No samples yet: fall back to a safe default (no divide-by-zero).
	if avgTxCount <= 0 {
		return noSampleInFlightDefault
	}

	// Transaction WORK budget: how many average-sized (in txs) blocks fit.
	txBound := txBudget / int(avgTxCount)

	// Byte safety clamp: also cap so in-flight bytes stay within the budget,
	// even when tx count under-represents memory. Only binds when we have a
	// size sample.
	result := txBound

	if avgSize > 0 {
		byteBound := int(byteBudget / avgSize)
		if byteBound < result {
			result = byteBound
		}
	}

	// Always at least 1 in flight.
	if result < 1 {
		result = 1
	}

	// Hard cap so tiny (1-tx) blocks don't produce an absurd count.
	if result > maxInFlightBlocksCap {
		result = maxInFlightBlocksCap
	}

	return result
}

// calculateWindowK returns the number of blocks to admit in one window batch.
// windowBudget is derived from effectiveGOMEMLIMIT × fraction.
// Admit-one floor: result is always >= 1.
// Clamped to maxBlocks (MaxBlocksBehindBlockAssembly).
func (bst *blockSizeTracker) calculateWindowK(windowBudget int64, maxBlocks int) int {
	avg := bst.getAverageSize()
	if avg <= 0 {
		return 1
	}

	k := int(windowBudget / avg)
	if k < 1 {
		k = 1
	}

	if maxBlocks > 0 && k > maxBlocks {
		k = maxBlocks
	}

	return k
}

// SyncManager is used to communicate block related messages with peers. The
// SyncManager is started as by executing Start() in a goroutine. Once started,
// it selects peers to sync from and starts the initial block download. Once the
// chain is in sync, the SyncManager handles incoming block and header
// notifications and relays announcements of new blocks to peers.
type SyncManager struct {
	ctx          context.Context
	logger       ulogger.Logger
	settings     *settings.Settings
	peerNotifier PeerNotifier
	started      int32
	shutdown     int32
	orphanTxs    *expiringmap.ExpiringMap[chainhash.Hash, *orphanTxAndParents]
	chainParams  *chaincfg.Params
	msgChan      chan interface{}
	handlerDone  chan struct{}
	quit         chan struct{}

	// TERANODE services
	blockchainClient  teranodeblockchain.ClientI
	validationClient  validator.Interface
	utxoStore         utxostore.Store
	subtreeStore      blob.Store
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
	blockAssembly     blockassembly.ClientI
	// cachedBlockAssemblyHeight holds the block-assembly CurrentHeight most
	// recently observed by the background poller (blockAssemblyHeightPoller).
	// The per-block coinbase-maturity check reads it atomically on the serial
	// drain path so the common case needs no gRPC round-trip. Zero means "not
	// yet polled" and forces the slow (fresh-gRPC) path. Because block-assembly
	// height is monotonic below the checkpoint (no reorg), this cached value is
	// always a stale-LOW-or-equal lower bound on the true height, so a fast-path
	// pass on the cache implies the true bound also holds.
	cachedBlockAssemblyHeight atomic.Uint32
	// baHeightPolled records that blockAssemblyHeightPoller has successfully
	// reported at least once. It exists because a cached height of ZERO is
	// ambiguous: it is both the unpolled zero value AND block assembly's real
	// height on a fresh node. Consumers must use THIS flag (not cached > 0) to
	// decide whether the cache is trustworthy — treating 0 as "unpolled" on a
	// from-scratch sync disabled parking entirely, so an out-of-order far-ahead
	// delivery put the drain goroutine into the blocking maturity wait and the
	// node wedged at exactly the gate width above genesis (mainnet, height 100).
	baHeightPolled atomic.Bool
	// parkAheadActive is set once, when the drain loop creates the park store
	// (park-ahead enabled + refill tick present). It gates the headers-first fetch
	// runway cap (fetchRunwayHorizon): only when parking is live must the fetch
	// frontier be bounded to the parkable horizon so the park cannot saturate.
	// Off (the zero value) leaves the forward walk uncapped — byte-identical to the
	// pre-park behaviour.
	parkAheadActive atomic.Bool
	// parkRef publishes the drain goroutine's park store to the drain-only fetch
	// scheduler (fetchRunwayHorizon / drainRefetchBlocks) so the runway can shrink to
	// the maturity gate while the park is count-full — stopping both the forward walk
	// and the refetch drain from pulling un-parkable far-ahead blocks that would only
	// be park-rejected and requeued, churning the budget and starving the in-gate
	// tip+1. Stored once at drain-loop setup; nil when parking is off. Only the drain
	// goroutine reads/writes park, so this pointer is atomic solely for safe setup
	// publication (no cross-goroutine park mutation).
	parkRef atomic.Pointer[parkStore]
	// windowOwnedBlocks is the hash-keyed ownership ledger for blocks the window
	// pipeline holds between admission and commit: parked (parkStore), accumulated
	// (windowAccumulator), or inside an in-flight windowFlushJob. Such a block is
	// invisible to GetBlockExists (not committed yet) and to requestedBlocks
	// (wiped by handleBlockPreamble when the block first ARRIVES), so without this
	// ledger rotation-driven re-walks re-request it and the re-delivery is fully
	// re-prepared and parked as a TWIN of the same height — releaseParkedBlocks
	// then splits the twins across two successive flush jobs and the FIFO worker
	// commits the same block twice (the mainnet blocks_pkey duplicate storm).
	// Claimed on park.add/wa.add (drain goroutine); consulted by the admission
	// guard (drain goroutine) and the multi-peer walk; released on EVERY job exit
	// (commitWindowJob defer — success, fatal and panic unwind — plus the
	// poisoned-discard and ctx-drain branches of flushWorker, the shutdown
	// abandon, and the park drop-arm/shutdown discards). A leaked claim would
	// make its block unfetchable until restart, so every exit must release.
	// Value is the block height (diagnostics only). SyncedMap: the flushWorker
	// releases concurrently with the drain goroutine's claims/reads.
	windowOwnedBlocks *txmap.SyncedMap[chainhash.Hash, uint32]
	// lastHandedWindowEnd is the height of the last block handed to the window
	// committer (gateContiguousWindow). Drain-goroutine only — read and written
	// exclusively inside flushWindow/flushWindowSync, so no synchronization.
	// Zero means "nothing handed yet" (seeds from the first flushed job). A job
	// starting at/below this value re-seeds it (idempotent re-sync after a
	// fatal rotation); a job starting more than one above it sits beyond a lost
	// range and is parked instead of handed (the stall-burst defect).
	lastHandedWindowEnd uint32
	// deferredCheckpoint is the one-slot holding cell for a checkpoint block
	// whose first delivery arrived before its parent committed (the fresh-sync
	// norm: the fetch frontier runs ~1000+ blocks ahead, so checkpoint-1 is
	// still parked behind the BA gate — measured 11-31s behind in every live
	// episode). Previously that delivery was DROPPED: the ErrBlockNotFound arm
	// returned nil (no requeue), the preamble had already wiped every fetch
	// ledger, the cursor never rewinds, and headers-first mode discards the
	// getblocks fallback's inv — so nothing could ever re-request the block and
	// the tip froze until the 3-minute sync-peer rotation re-walked the
	// interval. Deferring the delivery and retrying on the refill tick commits
	// it within ~one tick of the parent landing. Drain-goroutine only.
	deferredCheckpoint *deferredCheckpointBlock
	// deferBarredCheckpoint is the one-shot cap: after a deferral hits its
	// deadline (parent never committed — a double-fault), that hash may not
	// re-defer; its next delivery takes the old arm verbatim, so the rotation
	// backstop can never be indefinitely re-armed away. Drain-goroutine only.
	deferBarredCheckpoint chainhash.Hash
	legacyKafkaInvCh      chan *kafka.Message
	// legacyKafkaInvProducer is retained (DC11) so SyncManager.Stop() can stop it
	// synchronously; without a field there is no handle to flush it on shutdown.
	legacyKafkaInvProducer kafka.KafkaAsyncProducerI
	txAnnounceBatcher      *batcher.BatcherWithDedup[TxHashAndFee]
	// txAnnounceMu / txAnnounceClosed guard txAnnounceBatcher.Put against the
	// batcher's Close in Stop(). go-batcher v2.0.4 PANICS on Put-after-Close, and
	// the txmeta Kafka listener (which Puts into the batcher) is a fire-and-forget
	// goroutine not joined by Stop(). The RLock/RWLock pairing guarantees no Put
	// runs concurrently with or after the drain: Stop takes the write lock (which
	// waits for any in-flight Put holding the read lock), sets closed, then drains;
	// subsequent Puts see closed and become no-ops. (DC15 / review C1.)
	txAnnounceMu     sync.RWMutex
	txAnnounceClosed bool

	// These fields should only be accessed from the blockHandler thread
	// (except syncPeer/syncPeerState which are protected by syncPeerMu).
	rejectedTxns    *txmap.SyncedMap[chainhash.Hash, struct{}]
	requestedTxns   *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	requestedBlocks *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	// assignedTo/assignedAt track, per outstanding block, which peer it was
	// assigned to and when (Task 2.4, the head-of-line stalling timeout). They
	// are populated in assignBlocksAcrossPeers alongside the requestedBlocks
	// record (post-send), deleted when the block ARRIVES (handleBlockPreamble,
	// the same site the requestedBlocks entries are removed) and when the
	// assigned peer disconnects (handleDonePeerMsg). They are therefore kept in
	// exact one-to-one correspondence with the global requestedBlocks ledger so
	// they cannot leak.
	//
	// Almost all of that traffic (assign, arrival-delete, checkHeadStall) is on
	// the single drain goroutine, but handleDonePeerMsg runs on the OTHER
	// (outer) blockHandler goroutine — so a plain map would race. assignedMu
	// guards both maps; it is a leaf lock, never held across a peer send, gRPC,
	// or headerMu.
	assignedMu sync.Mutex
	assignedTo map[chainhash.Hash]*peerpkg.Peer
	assignedAt map[chainhash.Hash]time.Time
	// refetchBlocks holds hashes that were assigned, had their shared-cursor
	// (startHeader) position advanced past them, and then lost their in-flight
	// status WITHOUT being received — either freed by the head-of-line stall
	// detector (freePeerAssignments) or dropped when a peer's send queue was
	// full. Because startHeader is monotonic-forward and never rewound, the
	// forward walk in assignBlocksAcrossPeers can never re-reach a block below
	// the cursor, so without this set such a block would be orphaned forever:
	// outstanding to nobody, blocking the strictly-ascending window commit and
	// wedging the whole download (height frozen, drain idle). assignBlocksAcrossPeers
	// drains this set FIRST each pass (orphans are the lowest, commit-blocking
	// blocks), re-requesting each from an eligible peer via the normal getdata
	// path. Entries are removed on successful re-send, on receipt, and on a fresh
	// sync generation; blocks whose requestedBlocks ledger entry expires (60s TTL)
	// while still assigned are re-enqueued here by reconcileLostAssignments, so no
	// orphan trigger is left unhandled. Guarded by assignedMu (same leaf lock as
	// assignedTo/assignedAt); bounded by the total in-flight cap, so it cannot
	// grow unbounded.
	refetchBlocks map[chainhash.Hash]struct{}
	syncPeerMu    sync.RWMutex // protects syncPeer and syncPeerState
	syncPeer      *peerpkg.Peer
	syncPeerState *syncPeerState
	peerStates    *txmap.SyncedMap[*peerpkg.Peer, *peerSyncState]

	// blockBacklog counts blocks sitting in the local processing pipeline:
	// queued in blockHandler's blockQueue plus the one inside handleBlockMsg.
	// While it is non-zero the node is backpressuring its own network reads
	// (OnBlock blocks until the previous block is processed), so the stall
	// detector must not hold the resulting zero throughput against the sync
	// peer. Written by the blockHandler goroutines, read by handleCheckSyncPeer.
	blockBacklog atomic.Int64

	// lastBacklogProgress is the UnixNano time the block backlog last advanced:
	// the 0->1 enqueue that opened the current backpressure window, or the most
	// recent block completion. localReadBackpressured suppresses the sync-peer
	// stall check only while this stays fresh — a backlog that stops advancing
	// for longer than blockProcessingStallTimeout is a genuine processing hang
	// (store/validator deadlock, Aerospike overload), not slow-but-progressing
	// validation, and must be allowed to rotate the peer. This restores the
	// liveness coverage lost when the per-message watchdog was disarmed for
	// prefetched blocks, without the false rotation of a merely-slow block.
	// Written by the blockHandler goroutines (the 0->1 enqueue and every
	// completion, via noteBacklogProgress), read by handleCheckSyncPeer.
	lastBacklogProgress atomic.Int64

	// blockPrefetchBudget bounds, by total serialized bytes, the blocks that
	// have been received from peers but not yet finished processing. It lets
	// OnBlock admit a block and return (so the read-loop downloads the next
	// block while this one validates) instead of blocking on per-block
	// completion, while capping the memory pinned by buffered blocks across ALL
	// peers and streams. nil when prefetch is disabled (budget <= 0), in which
	// case OnBlock keeps its original synchronous, one-block-in-flight behaviour.
	// A block larger than the whole budget is admitted alone (weight clamped to
	// the budget), preserving full backpressure for huge blocks.
	blockPrefetchBudget      *semaphore.Weighted
	blockPrefetchBudgetBytes int64

	// inFlightBlocks is the dedup half of the same block-admission gate whose
	// byte half is blockPrefetchBudget. It holds the hash of every block that is
	// currently admitted (has reserved budget) OR parked waiting for budget, so
	// at most one copy of any given block hash is ever in flight at a time.
	// AcquireBlockPrefetch inserts the hash BEFORE the (possibly blocking) budget
	// Acquire and ReleaseBlockPrefetch deletes it alongside the budget release, so
	// the two halves share exactly one lifetime and can never drift. Without it,
	// N duplicates of a single requested, near-budget-sized block would each
	// reserve budget, fill the whole budget, and park every legacy peer's
	// read-loop in Acquire — the very "a malicious peer cannot outrun the budget"
	// property this gate exists to guarantee. nil (alongside a nil
	// blockPrefetchBudget) when prefetch is disabled, so the synchronous/regtest
	// path skips dedup entirely. inFlightBlocksMu guards the map.
	inFlightBlocks   map[chainhash.Hash]struct{}
	inFlightBlocksMu sync.Mutex

	// blockPrefetchWaiters counts read-loops currently blocked acquiring
	// prefetch budget (i.e. local processing cannot keep up). While > 0 the node
	// is backpressuring its own network reads, so the stall detector must not
	// hold the resulting zero throughput against the sync peer — the prefetch
	// analogue of the blockBacklog guard. Read by handleCheckSyncPeer.
	blockPrefetchWaiters atomic.Int64

	// The following fields are used for headers-first mode.
	headersFirstMode atomic.Bool // accessed from multiple goroutines, must be atomic
	// headerMu guards the five headers-first fields below (headerList,
	// startHeader, headerListSeed, nextCheckpoint, headerCheckpoint) AND
	// headerGen and headerHeightIndex. It serialises the outer blockHandler loop
	// (updateSyncPeer / resetHeaderState / startSync, reached from the
	// peer-lifecycle handlers) against the drain goroutine (handleHeadersMsg /
	// handleBlockPreamble / handleBlockMsg / fetchHeaderBlocks). It is NEVER held
	// across a peer send, a gRPC, or block validation — fetchHeaderBlocks
	// snapshots headerGen under the lock, releases it for the I/O, then re-takes
	// it and aborts the walk if the generation changed.
	headerMu  sync.Mutex
	headerGen uint64 // bumped by resetHeaderState; guarded by headerMu
	// recentlyNeededUntil carries block hashes across resetHeaderState (guarded
	// by headerMu, TTL-bounded). A rotation clears headerHeightIndex and every
	// fetch ledger, so blocks legitimately requested BEFORE the rotation arrive
	// afterwards looking "unrequested" — the old response was to disconnect the
	// delivering peer, which cascaded (measured live: 8 peers executed within
	// one second, 4s after a rotation) and produced the multi-minute fetch
	// wedges behind the bursty sync. Entries are folded in from the outgoing
	// index at reset, expire after recentlyNeededTTL, and are deleted at the
	// same commit/prune sites that delete headerHeightIndex entries.
	recentlyNeededUntil map[chainhash.Hash]time.Time
	// lastHeaderResetAt (guarded by headerMu) stamps resetHeaderState so the
	// header-linkage check can give a short grace after a reset: a successor
	// sync peer's first batch legitimately fails to connect to the rebuilt
	// list's seed and was being chain-killed for it (19 kills measured).
	lastHeaderResetAt time.Time
	// headStallSuppressedAt (drain goroutine only — checkHeadStall runs from the
	// refill tick via maintainInFlightWindow) records when the head-stall check
	// last declined to fire because the node was throttling its own reads. The
	// 2s BlockStallTimeout must not execute a peer for OUR backpressure.
	headStallSuppressedAt time.Time
	// frontierGapHash / frontierGapSince (drain goroutine only — reconcileFrontierGap
	// runs from the refill tick) debounce the frontier-orphan re-request. The
	// frontier can be briefly untracked by the ledgers reconcileFrontierGap checks
	// while it is legitimately mid-flight (e.g. its 60s global requestedBlocks entry
	// lapsed but the block is still coming from a slow peer). Re-requesting on every
	// 20ms tick would churn (harmless to correctness but a log/CPU storm), so we only
	// act once the SAME frontier has stayed orphaned longer than BlockInFlightTimeout,
	// then re-arm — matching how reconcileLostAssignments gates on the same timeout.
	frontierGapHash  chainhash.Hash
	frontierGapSince time.Time
	// headerHeightIndex maps each headerNode's block hash to its authoritative
	// PoW-verified height. It allows handleBlockPreamble to resolve the height of
	// blocks that arrive out of order (not at headerList.Front()), which happens
	// under multi-peer parallel download. Guarded by headerMu; populated wherever
	// headerList.PushBack is called; cleared in resetHeaderState; entry deleted
	// when the corresponding headerList node is removed in handleBlockPreamble.
	headerHeightIndex map[chainhash.Hash]int32
	headerList        *list.List
	startHeader       *list.Element
	// headerListSeed is the current leading "seed" node at the front of the
	// header list: a node whose block is NOT pending a fetch and is kept only so
	// the next interval's first header can prove it links onto the chain. There
	// are exactly two kinds:
	//   1. The DB-best node pushed by resetHeaderState (the block already in our
	//      chain that the very first downloaded header links onto). No block
	//      message ever arrives for it.
	//   2. A checkpoint node whose block has already been processed. The block
	//      handler keeps it after its block commits so the NEXT interval's
	//      headers can link onto it, then it becomes the new seed.
	// A seed has no pending block, so the block-commit front-removal never
	// matches it directly and it would otherwise sit at Front() forever, blocking
	// removal (and header-height sourcing) of the next interval's real blocks.
	// handleBlockPreamble therefore drops the leading seed the moment the block
	// that follows it commits — removal tied to block-fetch progress, never to a
	// header-download event, so no un-fetched node is ever stranded.
	headerListSeed *list.Element
	// nextCheckpoint is the BLOCK-level checkpoint tracker: the next checkpoint
	// whose full block we still expect to process. handleBlockPreamble uses its
	// Hash to recognise (and keep in the header list) the checkpoint block, and
	// the block handler advances it as each checkpoint block is committed.
	nextCheckpoint *chaincfg.Checkpoint
	// checkpointsDisabled records config.DisableCheckpoints so that
	// realignCheckpointCursor can never resurrect a cursor on a node started
	// with checkpoints off (chainParams still lists them). Written once in
	// New() before any goroutine starts; read-only afterwards.
	checkpointsDisabled bool
	// headerCheckpoint is the HEADER-request look-ahead cursor: the checkpoint
	// the currently-outstanding getheaders request is heading toward. It is
	// decoupled from nextCheckpoint so header download can run ahead of block
	// fetching. handleHeadersMsg verifies each checkpoint-height header against
	// it and, on reaching it, advances the cursor and requests the next
	// interval's headers immediately — eliminating the checkpoint-boundary
	// stall where block fetching idles while the next headers download. Both
	// cursors start equal (set together wherever nextCheckpoint is initialised)
	// and headerCheckpoint only ever runs ahead of nextCheckpoint.
	headerCheckpoint *chaincfg.Checkpoint
	blockSizeTracker *blockSizeTracker // tracks block sizes for dynamic in-flight adjustment

	// An optional fee estimator.
	// feeEstimator *mempool.FeeEstimator
	currentFeeFilter atomic.Uint64

	// minSyncPeerNetworkSpeed is the minimum speed allowed for
	// a sync peer.
	minSyncPeerNetworkSpeed uint64
}

// loadSyncPeer returns the current sync peer, safe for concurrent access.
func (sm *SyncManager) loadSyncPeer() *peerpkg.Peer {
	sm.syncPeerMu.RLock()
	defer sm.syncPeerMu.RUnlock()
	return sm.syncPeer
}

// loadSyncPeerAndState returns the current sync peer and its state, safe for concurrent access.
func (sm *SyncManager) loadSyncPeerAndState() (*peerpkg.Peer, *syncPeerState) {
	sm.syncPeerMu.RLock()
	defer sm.syncPeerMu.RUnlock()
	return sm.syncPeer, sm.syncPeerState
}

// syncPeerStateFor returns the sync peer's state if p is the current sync peer
// or another stream of its association, and whether it matched. Under the
// BlockPriority policy a block is delivered on the DATA1 stream — a different
// Peer from the GENERAL sync peer — so a plain `p == syncPeer` check misses it
// and the sync peer's lastBlockTime is never refreshed during multistream sync.
func (sm *SyncManager) syncPeerStateFor(p *peerpkg.Peer) (*syncPeerState, bool) {
	sp, sps := sm.loadSyncPeerAndState()
	if sp == nil || sps == nil || p == nil {
		return nil, false
	}

	if p == sp {
		return sps, true
	}

	if a := p.AssociationRef(); a != nil && a == sp.AssociationRef() {
		return sps, true
	}

	return nil, false
}

// noteSyncPeerBlockDelivery refreshes the sync peer's last-block time when a
// windowed block ARRIVES from the peer but we do not commit it now — either we
// park it ahead of block assembly, or the park buffer is full so we requeue it for
// re-fetch. A block landing that we defer or refuse is proof the peer is alive and
// feeding us data at capacity; the refusal is our OWN backpressure, not a stalled
// peer. Only the real accept paths stamp lastBlockTime, so without this a park
// refusal storm (fast IBD, tiny blocks fill the park's count cap at sub-MB memory)
// leaves lastBlockTime frozen while blocks keep arriving. handleCheckSyncPeer's
// last-block-time violation then fires and rotates our best data source, which
// clears requestedBlocks and resets header state — serialising ALL fetch behind a
// fresh getheaders round-trip (the observed multi-minute all-peer silence). A
// genuinely dead peer delivers no bytes and is still rotated: that is covered by
// the bytes-based silence detector and the backlog-stale checks, which this does
// not touch.
func (sm *SyncManager) noteSyncPeerBlockDelivery(peer *peerpkg.Peer) {
	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}
}

// storeSyncPeer sets the sync peer and its state, safe for concurrent access.
func (sm *SyncManager) storeSyncPeer(peer *peerpkg.Peer, state *syncPeerState) {
	sm.syncPeerMu.Lock()
	defer sm.syncPeerMu.Unlock()
	sm.syncPeer = peer
	sm.syncPeerState = state
}

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer. It takes headerMu for its whole body and bumps
// headerGen so any concurrent fetchHeaderBlocks walk aborts. Callers must NOT
// already hold headerMu (the lock is non-reentrant).
func (sm *SyncManager) resetHeaderState(newestHash *chainhash.Hash, newestHeight int32) {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	sm.headerGen++
	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.startHeader = nil
	sm.headerListSeed = nil

	// Fold the outgoing index into the TTL carryover BEFORE clearing, so blocks
	// requested under the old generation can still be recognised as needed when
	// they arrive after the reset (see recentlyNeededUntil). Expired entries are
	// swept in the same pass to bound the map.
	if sm.recentlyNeededUntil == nil {
		sm.recentlyNeededUntil = make(map[chainhash.Hash]time.Time)
	}

	now := time.Now()
	for h, until := range sm.recentlyNeededUntil {
		if now.After(until) {
			delete(sm.recentlyNeededUntil, h)
		}
	}

	deadline := now.Add(recentlyNeededTTL)
	for h := range sm.headerHeightIndex {
		sm.recentlyNeededUntil[h] = deadline
	}

	sm.lastHeaderResetAt = now

	// Clear the index before rebuilding; this bounds its memory across resets.
	clear(sm.headerHeightIndex)

	// A fresh sync generation invalidates any pending re-fetch requests: their
	// hashes belong to the header list we are discarding. headerMu -> assignedMu
	// is the established lock order (see checkHeadStall). clear on nil is a no-op.
	sm.assignedMu.Lock()
	clear(sm.refetchBlocks)
	sm.assignedMu.Unlock()

	// Re-align the header-request look-ahead cursor with the block-level
	// checkpoint tracker on a fresh sync/recovery. From here they may diverge
	// again as handleHeadersMsg pipelines headers ahead of block fetching.
	sm.headerCheckpoint = sm.nextCheckpoint

	// When there is a next checkpoint, add an entry for the latest known
	// block into the header pool.  This allows the next downloaded header
	// to prove it links to the chain properly. Track it as the leading seed so
	// it is removed exactly once when the first real block commits (it has no
	// block of its own to fetch); see headerListSeed.
	if sm.nextCheckpoint != nil {
		node := headerNode{height: newestHeight, hash: newestHash}
		sm.headerListSeed = sm.headerList.PushBack(&node)
		sm.headerHeightIndex[*node.hash] = node.height
	}
}

// recentlyNeededTTL bounds how long a hash carried across resetHeaderState is
// still treated as needed. Long enough to cover post-rotation redelivery of
// everything that was in flight (60s requestedBlocks TTL + delivery latency),
// short enough that the unrequested-block spam guard re-arms promptly.
const recentlyNeededTTL = 90 * time.Second

// blockStillNeeded reports whether a delivered block is one the node genuinely
// wants right now: present in the live header index, or carried across a recent
// resetHeaderState and not yet expired. Used to stop the unrequested-block
// disconnect punishing peers for deliveries WE requested before a rotation
// cleared the ledgers (the measured disconnect-cascade trigger). Expired
// carryover entries are deleted on read.
func (sm *SyncManager) blockStillNeeded(h chainhash.Hash) bool {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	if _, ok := sm.headerHeightIndex[h]; ok {
		return true
	}

	until, ok := sm.recentlyNeededUntil[h]
	if !ok {
		return false
	}

	if time.Now().After(until) {
		delete(sm.recentlyNeededUntil, h)
		return false
	}

	return true
}

// findNextHeaderCheckpoint returns the next checkpoint after the passed height.
// It returns nil when there is not one either because the height is already
// later than the final checkpoint or some other reason such as disabled
// checkpoints.
func (sm *SyncManager) findNextHeaderCheckpoint(height int32) *chaincfg.Checkpoint {
	checkpoints := sm.chainParams.Checkpoints
	if len(checkpoints) == 0 {
		return nil
	}

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= finalCheckpoint.Height {
		return nil
	}

	// Find the next checkpoint.
	nextCheckpoint := finalCheckpoint

	for i := len(checkpoints) - 2; i >= 0; i-- {
		if height >= checkpoints[i].Height {
			break
		}

		nextCheckpoint = &checkpoints[i]
	}

	return nextCheckpoint
}

// realignCheckpointCursor re-derives nextCheckpoint from the committed best
// height and heals it when the stored value has gone stale, returning whether
// it did. It exists because the cursor is a cache of "next checkpoint after
// tip" that only the direct block path maintains (handleBlockMsg /
// runPostBlockProcessing): the window pipeline commits checkpoint blocks
// without advancing it — recognition matches only the current cursor hash, so
// a checkpoint block fetched by the park-ahead before the cursor reaches it is
// admitted as an ordinary block, and the re-delivery that WOULD be recognised
// is discarded by the window-ownership guard before checkpoint handling runs.
// Once stranded, the startSync headers-first gate (tip < cursor height)
// evaluates false forever, and the first header-state reset past that point
// latches the node into getblocks mode where no delivered block can resolve a
// height: the mainnet wedge of 2026-07-17 (one committed block per 120-second
// sync-peer rotation, ~1400 BLOCK_NOT_FOUND rejections per cycle, healed only
// by restart — because New() runs exactly this derivation). Re-deriving at
// every sync start removes the gate's dependency on incremental bookkeeping
// entirely, whatever desynchronised it.
//
// Callers must NOT hold headerMu: on the stale path this calls
// resetHeaderState, which takes headerMu for its whole body. startSync is
// only ever invoked from the blockHandler event loop, so the brief window
// between releasing headerMu and resetHeaderState re-taking it cannot race
// another startSync.
func (sm *SyncManager) realignCheckpointCursor(bestHash *chainhash.Hash, bestHeight int32) bool {
	if sm.checkpointsDisabled {
		return false
	}

	recomputed := sm.findNextHeaderCheckpoint(bestHeight)

	sm.headerMu.Lock()

	stored := sm.nextCheckpoint
	stale := (stored == nil) != (recomputed == nil) ||
		(stored != nil && recomputed != nil && stored.Height != recomputed.Height)

	if stale {
		sm.nextCheckpoint = recomputed
	}

	sm.headerMu.Unlock()

	if !stale {
		return false
	}

	formatCheckpoint := func(c *chaincfg.Checkpoint) string {
		if c == nil {
			return "nil"
		}

		return fmt.Sprintf("%d", c.Height)
	}

	sm.logger.Warnf("[startSync] checkpoint cursor stale (stored height %s, tip %d): realigned to %s, resetting header state",
		formatCheckpoint(stored), bestHeight, formatCheckpoint(recomputed))

	// Realign headerCheckpoint to the fresh cursor and reseed the header list
	// from the committed tip so the next getheaders can link onto the chain.
	sm.resetHeaderState(bestHash, bestHeight)

	return true
}

// startSync will choose the best peer among the available candidate peers to
// download/sync the blockchain from.  When syncing is already running, it
// simply returns.  It also examines the candidates for any which are no longer
// candidates and removes them as needed.
func (sm *SyncManager) startSync() {
	// Return now if we're already syncing.
	if sm.loadSyncPeer() != nil {
		return
	}

	sm.logger.Debugf("startSync - Syncing from %v", sm.loadSyncPeer())

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf("Failed to get best block header: %v", err)
		return
	}

	bestPeers := make([]*peerpkg.Peer, 0)

	okPeers := make([]*peerpkg.Peer, 0)

	sm.logger.Debugf("[startSync] selecting sync peer from %d candidates", sm.peerStates.Length())

	for peer, state := range sm.peerStates.Range() {
		if !state.syncCandidate {
			sm.logger.Debugf("[startSync] peer %v is not a sync candidate", peer.String())

			continue
		}

		// Defence-in-depth: never elect a peer whose socket has already been
		// torn down. If one slips into peerStates (e.g. a future regression in
		// the new-peer registration path), picking it here would push
		// getheaders into a closed connection and stall sync for the duration
		// of maxLastBlockTime before rotating.
		if !peer.Connected() {
			sm.logger.Debugf("[startSync] peer %v is not connected, skipping", peer.String())

			continue
		}

		// Add any peers on the same block to okPeers. These should
		// only be used as a last resort.

		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf("[startSync] failed to convert block height to int32: %v", err)

			continue
		}

		if peer.LastBlock() == bestBlockHeightInt32 {
			okPeers = append(okPeers, peer)
			sm.logger.Debugf("[startSync][%v] peer is at the same height %d as us (%d), added to okPeers", peer.String(), peer.LastBlock(), bestBlockHeaderMeta.Height)

			continue
		}

		// Skip sync candidate peers that are no longer candidates due
		// to passing their latest known block.
		if peer.LastBlock() < bestBlockHeightInt32 {
			sm.logger.Debugf("[startSync][%v] peer is behind us at height %d (us: %d), skipping", peer.String(), peer.LastBlock(), bestBlockHeaderMeta.Height)

			continue
		}

		// Append each good peer to bestPeers for selection later.
		sm.logger.Debugf("[startSync][%v] peer is a sync candidate at height %d (us: %d), adding to bestPeers", peer.String(), peer.LastBlock(), bestBlockHeaderMeta.Height)
		bestPeers = append(bestPeers, peer)
	}

	var bestPeer *peerpkg.Peer

	// Try to select a random peer that is at a higher block height,
	// if that is not available, then use a random peer at the same
	// height and hope they find blocks.
	if len(bestPeers) > 0 {
		// #nosec G404
		bestPeer = bestPeers[rand.IntN(len(bestPeers))]
		sm.logger.Debugf("[startSync] selected best peer %s from %d peers ahead of us", bestPeer.String(), len(bestPeers))
	} else if len(okPeers) > 0 {
		// #nosec G404
		bestPeer = okPeers[rand.IntN(len(okPeers))]
		sm.logger.Debugf("[startSync] no peers ahead, selected ok peer %s from %d peers at same height", bestPeer.String(), len(okPeers))
	}

	// Start syncing from the best peer if one was selected.
	if bestPeer == nil {
		sm.logger.Warnf("[startSync] No sync peer candidates available after evaluating %d total peers (%d ahead, %d at same height)", sm.peerStates.Length(), len(bestPeers), len(okPeers))

		return
	}

	sm.logger.Debugf("[startSync] best peer selected: %s", bestPeer.String())

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("[startSync] failed to convert block height to int32: %v", err)

		return
	}

	// check whether we are in sync with this peer and send RUNNING FSM state
	if bestPeer.LastBlock() == bestBlockHeightInt32 {
		sm.logger.Debugf("[startSync] peer %v is at the same height %d as us, sending RUNNING", bestPeer.String(), bestPeer.LastBlock())

		if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/startSync"); err != nil {
			sm.logger.Errorf("[startSync] failed to set blockchain state to running: %v", err)
		}

		sm.resetFeeFilterToDefault()

		return
	}

	// Clear the requestedBlocks if the sync peer changes, otherwise
	// we may ignore blocks we need that the last sync peer failed
	// to send.
	sm.requestedBlocks.Clear()

	locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("[startSync] Failed to get block locator for the latest block: %v", err)

		return
	}

	sm.logger.Infof("[startSync] Syncing from block height %d to block height %d using peer %v", bestBlockHeaderMeta.Height, bestPeer.LastBlock(), bestPeer.String())

	// If we are behind the peer more than 10 blocks, move to CATCHING BLOCKS
	if bestPeer.LastBlock()-bestBlockHeightInt32 > 10 {
		// move FSM state to CATCHING BLOCKS, we are behind the peer more than 10 blocks
		if err = sm.blockchainClient.CatchUpBlocks(sm.ctx); err != nil {
			sm.logger.Errorf("[startSync] failed to set blockchain state to catching blocks: %v", err)
		}
	}

	// When the current height is less than a known checkpoint we
	// can use block headers to learn about which blocks comprise
	// the chain up to the checkpoint and perform less validation
	// for them.  This is possible since each header contains the
	// hash of the previous header and a merkle root.  Therefore, if
	// we validate all of the received headers linked together
	// properly and the checkpoint hashes match, we can be sure the
	// hashes for the blocks in between are accurate.  Further, once
	// the full blocks are downloaded, the merkle root is computed
	// and compared against the value in the header which proves the
	// full block hasn't been tampered with.
	//
	// Once we have passed the final checkpoint, or checkpoints are
	// disabled, use standard inv messages learn about the blocks
	// and fully validate them.  Finally, regression test mode does
	// not support the headers-first approach so do normal block
	// downloads when in regression test mode.
	// Heal a stale checkpoint cursor before the gate below reads it — see
	// realignCheckpointCursor for the strand mechanism this defends against.
	sm.realignCheckpointCursor(bestBlockHeader.Hash(), bestBlockHeightInt32)

	// Read the checkpoint cursors under headerMu and, in the headers-first
	// branch, realign headerCheckpoint before releasing the lock. The
	// PushGetHeadersMsg peer send must NOT be held under headerMu, so capture the
	// stop-hash while locked and send after releasing.
	sm.headerMu.Lock()
	nextCheckpoint := sm.nextCheckpoint
	headersFirst := nextCheckpoint != nil &&
		bestBlockHeightInt32 < nextCheckpoint.Height &&
		sm.chainParams != &chaincfg.RegressionNetParams

	var headerCheckpointHash *chainhash.Hash

	var headerCheckpointHeight int32

	if headersFirst {
		// The header-request cursor starts aligned with the block-level
		// checkpoint tracker; the first getheaders heads toward it.
		sm.headerCheckpoint = nextCheckpoint
		headerCheckpointHash = nextCheckpoint.Hash
		headerCheckpointHeight = nextCheckpoint.Height
	}
	sm.headerMu.Unlock()

	if headersFirst {
		if err = bestPeer.PushGetHeadersMsg(locator, headerCheckpointHash); err != nil {
			sm.logger.Warnf("[startSync] Failed to send getheaders message to peer %s: %v", bestPeer.String(), err)

			return
		}

		sm.headersFirstMode.Store(true)

		sm.logger.Infof("[startSync] Downloading headers for blocks %d to %d from peer %s", bestBlockHeaderMeta.Height+1, headerCheckpointHeight, bestPeer.String())
	} else {
		if err = bestPeer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
			sm.logger.Warnf("[startSync] Failed to send getblocks message to peer %s: %v", bestPeer.String(), err)

			return
		}
	}

	bestPeer.SetSyncPeer(true)
	sm.storeSyncPeer(bestPeer, &syncPeerState{
		lastBlockTime:     time.Now(),
		recvBytes:         bestPeer.BytesReceived(),
		recvBytesLastTick: uint64(0),
	})
}

func (sm *SyncManager) resetFeeFilterToDefault() {
	if sm.currentFeeFilter.Load() != uint64(bsvutil.SatoshiPerBitcoin*sm.settings.Policy.MinMiningTxFee) {
		feeFilter := wire.NewMsgFeeFilter(int64(sm.settings.Policy.MinMiningTxFee)) // nolint:gosec

		for p := range sm.peerStates.Range() {
			if p == nil {
				continue
			}

			if !p.Connected() {
				continue
			}

			p.QueueMessage(feeFilter, nil)
		}

		sm.currentFeeFilter.Store(uint64(bsvutil.SatoshiPerBitcoin * sm.settings.Policy.MinMiningTxFee))
	}
}

// SyncHeight returns latest known block being synced to.
func (sm *SyncManager) SyncHeight() uint64 {
	if sm.loadSyncPeer() == nil {
		return 0
	}

	return uint64(sm.topBlock())
}

// IsHeadersFirstMode returns whether the sync manager is currently in headers-first mode.
// This is used to avoid serving headers to other peers during checkpoint sync, which
// can cause significant delays (18s+ per batch) due to database query contention.
func (sm *SyncManager) IsHeadersFirstMode() bool {
	return sm.headersFirstMode.Load()
}

// isRegtest reports whether the active chain params are regression net by
// network magic rather than pointer identity with chaincfg.RegressionNetParams,
// so a copied Params value (as some tests construct) is still recognized, and a
// nil chainParams is safely not-regtest. It exists to give BlockRequested the
// SAME value semantics as peerpkg.UseBlockPrefetchIngestion (.Net != RegTestNet)
// so those two prefetch-path siblings cannot drift on a copied-params manager.
//
// It deliberately does NOT replace the pointer-equality regtest checks
// elsewhere in this file (startSync's headers-first gate, isSyncCandidate,
// handleBlockMsg's unrequested-block disconnect). Those run on the synchronous
// (non-prefetch) path that regtest always takes, and the E2E harness builds
// chainParams as a *copy* of RegressionNetParams — so switching them to value
// semantics flips real behavior (e.g. isSyncCandidate would apply the regtest
// localhost restriction, and startSync would drop headers-first) and breaks
// legacy-sync/smoketest. Pointer equality there is load-bearing; leave it.
func (sm *SyncManager) isRegtest() bool {
	return sm.chainParams != nil && sm.chainParams.Net == wire.RegTestNet
}

// isSyncCandidate returns whether or not the peer is a candidate to consider
// syncing from.
func (sm *SyncManager) isSyncCandidate(peer *peerpkg.Peer) bool {
	// Typically a peer is not a candidate for sync if it's not a full node,
	// however regression test is special in that the regression tool is
	// not a full node and still needs to be considered a sync candidate.
	if sm.chainParams == &chaincfg.RegressionNetParams {
		// The peer is not a candidate if it's not coming from localhost
		// or the hostname can't be determined for some reason.
		// If we need to allow the peer with different host to be a sync candidate
		if !sm.settings.Legacy.AllowSyncCandidateFromLocalPeers {
			host, _, err := net.SplitHostPort(peer.String())
			if err != nil {
				return false
			}

			if host != "127.0.0.1" && host != "localhost" {
				return false
			}
		}
	} else {
		// The peer is not a candidate for sync if it's not a full
		// node.
		nodeServices := peer.Services()

		sm.logger.Debugf("Checking sync candidate %s: Services=%v, Required=%v", peer.String(), nodeServices, wire.SFNodeNetwork)

		if nodeServices&wire.SFNodeNetwork != wire.SFNodeNetwork {
			sm.logger.Debugf("Peer %s rejected as sync candidate: Missing SFNodeNetwork flag", peer.String())

			return false
		}
	}

	sm.logger.Debugf("Peer %s accepted as sync candidate", peer.String())
	// Candidate if all checks passed.
	return true
}

// handleNewPeerMsg deals with new peers that have signalled they may
// be considered as a sync peer (they have already successfully negotiated).  It
// also starts syncing if needed.  It is invoked from the syncHandler goroutine.
func (sm *SyncManager) handleNewPeerMsg(peer *peerpkg.Peer) {
	// Ignore if in the process of shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	// If the peer's socket was already torn down by the time this newPeerMsg
	// drained from msgChan, don't insert it into peerStates at all. Pairs
	// with the Connected() guard in startSync to close the window during
	// which a dead pointer can sit in the map waiting for a donePeerMsg.
	if !peer.Connected() {
		sm.logger.Debugf("[handleNewPeerMsg] peer %s already disconnected before registration, skipping", peer.String())
		return
	}

	sm.logger.Infof("New valid peer %s (%s)", peer, peer.UserAgent())

	// Initialize the peer state
	isSyncCandidate := sm.isSyncCandidate(peer)

	// While catching up, ask every newly-connected peer to hold back
	// transaction announcements to reduce load during sync. The raise is queued
	// per-peer; the global currentFeeFilter is only the marker the reset path
	// (resetFeeFilterToDefault) checks, so it must NOT gate the per-peer queue —
	// otherwise only the first peer to connect during catch-up would be told.
	// The filter is restored to the policy default once we reach RUNNING.
	if state, ferr := sm.blockchainClient.GetFSMCurrentState(sm.ctx); ferr != nil {
		sm.logger.Errorf("[handleNewPeerMsg] failed to get current FSM state: %v", ferr)
	} else if state != nil && *state == teranodeblockchain.FSMStateCATCHINGBLOCKS {
		feeFilter := wire.NewMsgFeeFilter(bsvutil.SatoshiPerBitcoin)
		peer.QueueMessage(feeFilter, nil)
		sm.currentFeeFilter.Store(bsvutil.SatoshiPerBitcoin)
	}

	sm.peerStates.Set(peer, &peerSyncState{
		syncCandidate:   isSyncCandidate,
		requestQueue:    txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second), // allow the node 10 seconds to respond to the tx request
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](60 * time.Minute), // allow the node 1 hour to respond to the requested blocks, needed for legacy sync/checkpoints
	})

	// Start syncing by choosing the best candidate if needed.
	if isSyncCandidate && sm.loadSyncPeer() == nil {
		sm.startSync()
	}
}

// handleCheckSyncPeer selects a new sync peer.
func (sm *SyncManager) handleCheckSyncPeer() {
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	sp, sps := sm.loadSyncPeerAndState()

	// If we don't have a sync peer, select a new one and return.
	if sp == nil {
		sm.startSync()

		return
	}

	// Update network stats at the end of this tick.
	defer sps.updateNetwork(sp)

	// While the node is throttling its own network reads because local block
	// processing cannot keep up, zero throughput and a stale last-block-time
	// measure our own validation speed, not the peer's health. Skip stall checks
	// until that self-backpressure clears — a genuinely stalled peer keeps
	// failing them afterwards. The deferred updateNetwork still runs, keeping
	// throughput samples fresh for the next tick.
	//
	// Any queued/mid-validation backlog suppresses the check (see
	// localReadBackpressured): a stale last-block-time then measures our
	// validation speed, not the peer. A genuinely stalled peer stops feeding the
	// queue, the backlog drains, and the check resumes — so this delays, but does
	// not prevent, rotation of a truly stalled peer.
	if sm.localReadBackpressured() {
		// Self-backpressure: silence here measures our validation speed, not the
		// peer. Clear the silence counter so it cannot fire spuriously the moment
		// backpressure lifts (the deferred updateNetwork may still add one tick
		// after this reset, which stays safely below any limit >= 2).
		sps.resetSilentTicks()
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check skipped: read-loop backpressured by local block processing", sp.String())

		return
	}

	headersFirst := sm.headersFirstMode.Load()
	lastBlockSince := time.Since(sps.getLastBlockTime())

	// During headers-first mode, only suppress network speed checks since
	// downloading 80-byte headers makes the peer appear slow. Still check
	// last-block-time so stalled peers get rotated even during headers-first.
	var isNetworkSpeedViolation bool
	if !headersFirst {
		validNetworkSpeed := sps.validNetworkSpeed(sm.minSyncPeerNetworkSpeed)
		isNetworkSpeedViolation = validNetworkSpeed >= maxNetworkViolations
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check, network violations: %v (limit %v), time since last block: %v (limit %v)", sp.String(), validNetworkSpeed, maxNetworkViolations, lastBlockSince, maxLastBlockTime)
	} else {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check (headers-first mode, speed check skipped), time since last block: %v (limit %v)", sp.String(), lastBlockSince, maxLastBlockTime)
	}
	isLastBlockTimeViolation := lastBlockSince > maxLastBlockTime

	// A multi-GB block can take longer than maxLastBlockTime to arrive. Under
	// the BlockPriority stream policy it streams in on the DATA1 stream, so no
	// block "completes" (lastBlockTime stays put) even though bytes are
	// actively flowing across the association. Don't rotate a sync peer that is
	// still pulling data at a healthy rate — it is making progress on a large
	// block, not stalled. A genuinely stalled peer delivers no throughput and
	// is still rotated.
	//
	// This suppression is itself capped at peer.MaxBlockDownloadTime: past that
	// wall-clock window the peer is rotated regardless of throughput, so a
	// malicious peer cannot dribble bytes just above the threshold forever to
	// hold the single sync-peer slot and stall IBD.
	if isLastBlockTimeViolation &&
		lastBlockSince < peerpkg.MaxBlockDownloadTime &&
		sps.hasHealthyDownloadThroughput(sm.minSyncPeerNetworkSpeed) {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s exceeded last-block-time but association still downloading at a healthy rate (%.0fs in, cap %s); not rotating", sp.String(), lastBlockSince.Seconds(), peerpkg.MaxBlockDownloadTime)
		isLastBlockTimeViolation = false
	}

	// Silent sync-peer detector (headers-first only). During IBD the sync peer
	// always owes us data — headers below the final checkpoint or requested
	// blocks — so N consecutive 30s samples of literally ZERO association bytes
	// mean it is stalled. This fires in ~N*30s instead of the 3-minute
	// last-block-time window, which was the dominant dead-air cost at
	// checkpoint-block stalls (the direct path's only recovery is rotation).
	// Restricted to headers-first mode: post-IBD, quiet is normal (blocks are
	// minutes apart) and the network-speed check above is active instead. A
	// peer moving ANY bytes (header replay, fat-block streaming) never accrues
	// silent ticks, so this cannot churn a slow-but-alive peer.
	isSilenceViolation := false

	if limit := sm.syncPeerSilentTickLimit(); headersFirst && limit > 0 {
		if st := sps.silentTickCount(); st >= limit {
			isSilenceViolation = true

			sm.logger.Infof("[CheckSyncPeer] sync peer %s silent for %d consecutive ticks during headers-first sync (limit %d)", sp.String(), st, limit)
		}
	}

	// If no violations detected, the sync peer is healthy — nothing to do.
	if !isNetworkSpeedViolation && !isLastBlockTimeViolation && !isSilenceViolation {
		return
	}

	var reason string

	switch {
	case isNetworkSpeedViolation:
		reason = "network speed violation"
	case isLastBlockTimeViolation:
		reason = "last block time out of range"
	case isSilenceViolation:
		reason = "silent sync peer during headers-first sync"
	}
	sm.logger.Debugf("[CheckSyncPeer] sync peer %s is stalled due to %s, updating sync peer", sp.String(), reason)

	state, exists := sm.peerStates.Get(sp)
	if !exists {
		return
	}

	sm.logger.Debugf("[CheckSyncPeer] removing sync peer %s", sp.String())

	sm.clearRequestedState(state)
	sm.updateSyncPeer(state, reason)
}

// syncPeerSilentTickLimit resolves the silent-tick rotation threshold:
// 0 disables the detector entirely (byte-identical rollback lever); any other
// configured value is clamped to a minimum of 2, because the first silent
// sample after backpressure or a fresh peer is legitimate settling time.
// A SyncManager without settings (unit-test constructions) is disabled.
func (sm *SyncManager) syncPeerSilentTickLimit() int {
	if sm.settings == nil {
		return 0
	}

	limit := sm.settings.Legacy.SyncPeerSilentTicks
	if limit <= 0 {
		return 0
	}

	if limit < 2 {
		return 2
	}

	return limit
}

// topBlock returns the best chains top block height
func (sm *SyncManager) topBlock() int32 {
	sp := sm.loadSyncPeer()
	if sp == nil {
		return 0
	}

	if sp.LastBlock() > sp.StartingHeight() {
		return sp.LastBlock()
	}

	return sp.StartingHeight()
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It
// removes the peer as a candidate for syncing and in the case where it was
// the current sync peer, attempts to select a new best peer to sync from.  It
// is invoked from the syncHandler goroutine.
func (sm *SyncManager) handleDonePeerMsg(peer *peerpkg.Peer) {
	sm.logger.Debugf("Received done peer message from peer %s", peer)

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Debugf("Received done peer message for unknown peer %s", peer)
		return
	}

	// Remove the peer from the list of candidate peers.
	sm.peerStates.Delete(peer)

	sm.logger.Infof("Lost peer %s (removed from peerStates)", peer)

	// Cleanup state of requested items.
	sm.clearRequestedState(state)

	// Drop this peer's entries from the stall-detector tracking maps (Task 2.4)
	// so they cannot outlive the peer. clearRequestedState stops the peer's own
	// requestedBlocks map above; the global requestedBlocks entries for those
	// hashes are left for handleBlockPreamble / re-request to clear, but the
	// tracking maps key on hash + peer, so a departed peer's stamps must go now.
	sm.freePeerAssignments(peer)

	// Fetch a new sync peer if this is the sync peer.
	if peer == sm.loadSyncPeer() {
		sm.updateSyncPeer(state, "sync peer disconnected")
	}
}

// clearRequestedState removes requested transactions
// and blocks from the global map.
func (sm *SyncManager) clearRequestedState(state *peerSyncState) {
	// Remove requested transactions from the global map so that they will
	// be fetched from elsewhere next time we get an inv.
	state.requestedTxns.Stop()

	// Remove requested blocks from the global map so that they will be
	// fetched from elsewhere next time we get an inv.
	state.requestedBlocks.Stop()
}

// updateSyncPeer picks a new peer to sync from. reason is logged so rotation
// storms are attributable from the INFO log (previously the trigger was only
// visible at debug level, which made live cascade forensics needlessly hard).
func (sm *SyncManager) updateSyncPeer(_ *peerSyncState, reason string) {
	sp, sps := sm.loadSyncPeerAndState()
	sm.logger.Infof("Updating sync peer (%s), last block: %v, violations: %v, headers-first mode: %v",
		reason,
		sps.getLastBlockTime(),
		sps.getViolations(),
		sm.headersFirstMode.Load())

	// Only disconnect if we have a valid sync peer
	if sp != nil {
		// Log current sync state before disconnecting. Read the two header-state
		// fields under headerMu (never hold it across DisconnectWithInfo below).
		if sm.headersFirstMode.Load() {
			sm.headerMu.Lock()
			headerListLen := sm.headerList.Len()
			hasStartHeader := sm.startHeader != nil
			sm.headerMu.Unlock()

			sm.logger.Debugf("Current header sync state - headerList length: %d, startHeader exists: %v",
				headerListLen, hasStartHeader)
		}

		sp.SetSyncPeer(false)
		sp.DisconnectWithInfo("updateSyncPeer - disconnect old sync peer")
	}

	// Reset sync peer state
	sm.storeSyncPeer(nil, nil)

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		// TODO we should return an error here to the caller
		sm.logger.Errorf("Failed to get best block header: %v", err)
		return
	}

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("failed to convert block height to int32: %v", err)
		return // add return to prevent continuing with invalid height
	}

	if sm.headersFirstMode.Load() {
		sm.logger.Infof("Resetting header sync state at height %d with hash %v",
			bestBlockHeightInt32, bestBlockHeader.Hash())

		sm.resetHeaderState(bestBlockHeader.Hash(), bestBlockHeightInt32)
	}

	sm.startSync()
}

// handleTxMsg handles transaction messages from all peers.
func (sm *SyncManager) handleTxMsg(tmsg *txMsg) {
	ctx, _, _ := tracing.Tracer("SyncManager").Start(sm.ctx, "handleTxMsg",
		tracing.WithHistogram(prometheusLegacyNetsyncHandleTxMsg),
		tracing.WithDebugLogMessage(sm.logger, "handling transaction message for %s from %s", tmsg.tx.Hash(), tmsg.peer),
	)

	peer := tmsg.peer

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Warnf("Received tx message from unknown peer %s", peer)
		return
	}

	// NOTE: BitcoinJ, and possibly other wallets, don't follow the spec of
	// sending an inventory message and allowing the remote peer to decide
	// whether or not they want to request the transaction via a getdata
	// message.  Unfortunately, the reference implementation permits
	// unrequested data, so it has allowed wallets that don't follow the
	// spec to proliferate.  While this is not ideal, there is no check here
	// to disconnect peers for sending unsolicited transactions to provide
	// interoperability.
	txHash := tmsg.tx.Hash()

	// Ignore transactions that we have already rejected.  Do not
	// send a reject message here because if the transaction was already
	// rejected, the transaction was unsolicited.
	if _, exists = sm.rejectedTxns.Get(*txHash); exists {
		sm.logger.Debugf("Ignoring unsolicited previously rejected transaction %v from %s", txHash, peer)
		return
	}

	// Validate the transaction using the validation service
	buf := bytes.NewBuffer(make([]byte, 0, tmsg.tx.MsgTx().SerializeSize()))
	_ = tmsg.tx.MsgTx().Serialize(buf)

	// Single inbound tx per call, passed downstream to the validator. Stays
	// on the standard heap path — no arena amortisation possible for a
	// one-shot decode where the tx must outlive this function frame.
	btTx, err := bt.NewTxFromBytes(buf.Bytes())
	if err != nil {
		sm.logger.Errorf("Failed to create transaction from bytes: %v", err)
		return
	}

	var txMeta *meta.Data

	timeStart := time.Now()
	// passing in block height 0, which will default to utxo store block height in validator
	txMeta, err = sm.validationClient.Validate(ctx, btTx, 0)

	prometheusLegacyNetsyncHandleTxMsgValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	// Remove transaction from request maps. Either the mempool/chain
	// already knows about it and as such we shouldn't have any more
	// instances of trying to fetch it, or we failed to insert and thus
	// we'll retry next time we get an inv.
	state.requestedTxns.Delete(*txHash)
	sm.requestedTxns.Delete(*txHash)

	if err != nil {
		if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) {
			// this is an orphan transaction, we will accept it when the parent comes in
			// first check if the transaction already exists in the orphan pool, otherwise add it
			if _, orphanTxExists := sm.orphanTxs.Get(*txHash); !orphanTxExists {
				sm.logger.Debugf("orphan transaction %v added from %s", txHash, peer)

				// create a map of the parents of the transaction for faster lookups
				txParents := txmap.NewSyncedMap[chainhash.Hash, struct{}]()
				for _, input := range tmsg.tx.MsgTx().TxIn {
					txParents.Set(input.PreviousOutPoint.Hash, struct{}{})
				}

				sm.orphanTxs.Set(*txHash, &orphanTxAndParents{
					tx:      btTx,
					parents: txParents,
					addedAt: time.Now(),
				})
			}

			return
		} else {
			// Do not request this transaction again until a new block
			// has been processed.
			sm.rejectedTxns.Set(*txHash, struct{}{})

			// When the error is a rule error, it means the transaction was
			// simply rejected as opposed to something actually going wrong,
			// so log it as such.  Otherwise, something really did go wrong,
			// so log it as an actual error.
			sm.logger.Errorf("Failed to process transaction %v: %v", txHash, err)

			// Convert the error into an appropriate reject message and send it.
			// TODO better rejection code and message from the error
			peer.PushRejectMsg(wire.CmdTx, wire.RejectInvalid, "rejected", txHash, false)

			return
		}
	}

	// acceptedTxs also should contain any orphan transactions that were accepted when this transaction was processed
	acceptedTxs := []*TxHashAndFee{{
		TxHash: *btTx.TxIDChainHash(),
		Fee:    txMeta.Fee,
	}}

	// process any orphan transactions that were waiting for this transaction to be accepted
	// this is a recursive call, but the orphan pool should be limited in size
	sm.processOrphanTransactions(ctx, btTx.TxIDChainHash(), &acceptedTxs)

	if len(acceptedTxs) > 0 {
		sm.peerNotifier.AnnounceNewTransactions(acceptedTxs)
	}
}

// processOrphanTransactions recursively processes orphan transactions that were waiting for a transaction to be accepted
func (sm *SyncManager) processOrphanTransactions(ctx context.Context, txHash *chainhash.Hash, acceptedTxs *[]*TxHashAndFee) {
	// check whether any transaction in the orphan pool has this transaction as a parent
	ctx, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "processOrphanTransactions",
		tracing.WithHistogram(prometheusLegacyNetsyncProcessOrphanTransactions),
	)
	defer deferFn()

	// remove the transaction from the orphan pool
	sm.orphanTxs.Delete(*txHash)

	// first we get all the orphan transactions, this will not block the orphan tx pool while processing
	orphanTxs := sm.orphanTxs.Items()

	for _, orphanTx := range orphanTxs {
		// check if the orphan transaction has this transaction as a parent
		if _, ok := orphanTx.parents.Get(*txHash); !ok {
			continue
		}

		// validate the orphan transaction
		// passing in block height 0, which will default to utxo store block height in validator
		txMeta, err := sm.validationClient.Validate(ctx, orphanTx.tx, 0)
		if err != nil {
			if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) {
				// silently exit, we will accept this transaction when the other parent(s) comes in
				// or when the transaction is spendable again
				continue
			}

			if errors.Is(err, errors.ErrTxConflicting) {
				// remove the tx from the orphan pool, it is a double spend
				sm.orphanTxs.Delete(*txHash)
				continue
			}

			// if the transaction was rejected, we will not process any of the orphan transactions that were waiting for it
			sm.logger.Errorf("Failed to process orphan transaction %v: %v", txHash, err)

			continue
		}

		// add the orphan transaction to the list of accepted transactions
		*acceptedTxs = append(*acceptedTxs, &TxHashAndFee{
			TxHash: *orphanTx.tx.TxIDChainHash(),
			Fee:    txMeta.Fee,
			Size:   txMeta.SizeInBytes,
		})

		// add the time it took to process the orphan transaction to the histogram
		prometheusLegacyNetsyncOrphanTime.Observe(float64(time.Since(orphanTx.addedAt).Microseconds()) / 1_000_000)

		// process any orphan transactions that were waiting for this transaction to be accepted
		sm.processOrphanTransactions(ctx, orphanTx.tx.TxIDChainHash(), acceptedTxs)
	}
}

// isCurrent returns whether the sync manager believes it is synced with the chain.
// this function is a rewrite of the function in the original bsvd blockchain package
func (sm *SyncManager) isCurrent(bestBlockHeaderMeta *model.BlockHeaderMeta) bool {
	// Not current if the latest main (best) chain height is before the
	// latest known good checkpoint (when checkpoints are enabled).
	if len(sm.chainParams.Checkpoints) > 0 {
		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf("failed to convert block height to int32: %v", err)
		}

		checkpoint := &sm.chainParams.Checkpoints[len(sm.chainParams.Checkpoints)-1]
		if bestBlockHeightInt32 < checkpoint.Height {
			return false
		}
	}

	// Not current if the latest best block has a timestamp before 24 hours ago.
	//
	// The chain appears to be current if none of the checks reported otherwise.
	// minus24Hours := b.timeSource.AdjustedTime().Add(-24 * time.Hour).Unix()
	minus24Hours := time.Now().Add(-24 * time.Hour).Unix()

	current := int64(bestBlockHeaderMeta.BlockTime) >= minus24Hours

	return current
}

// current returns true if we believe we are synced with our peers, false if we
// still have blocks to check
func (sm *SyncManager) current() bool {
	_, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf("[current] failed to get best block header: %v", err)
		return false
	}

	if !sm.isCurrent(bestBlockHeaderMeta) {
		return false
	}

	// if blockChain thinks we are current, and we have no syncPeer, it is probably right.
	sp := sm.loadSyncPeer()
	if sp == nil {
		return true
	}

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("failed to convert block height to int32: %v", err)
	}

	// No matter what the chain thinks, if we are below the block we are syncing to we are not current.
	if bestBlockHeightInt32 < sp.LastBlock() {
		return false
	}

	return true
}

// peerStateResolvingPrimary returns the sync state for peer, resolving a stream
// sub-peer (e.g. a BlockPriority DATA1 stream, not itself registered in
// peerStates) to its association's primary peer. It returns the resolved peer
// (the primary when a stream peer resolved, otherwise the input peer) and
// whether a state was found. Centralizes the stream→primary walk previously
// inlined in handleBlockMsg/handleHeadersMsg/handleInvMsg/BlockRequested; call
// sites that log the resolution or reassign to the primary compare the returned
// peer against their input (resolved != input means a stream peer resolved).
func (sm *SyncManager) peerStateResolvingPrimary(peer *peerpkg.Peer) (*peerSyncState, *peerpkg.Peer, bool) {
	if state, exists := sm.peerStates.Get(peer); exists {
		return state, peer, true
	}

	if assoc := peer.AssociationRef(); assoc != nil {
		if primary := assoc.PrimaryPeer(); primary != nil {
			if state, exists := sm.peerStates.Get(primary); exists {
				return state, primary, true
			}
		}
	}

	return nil, peer, false
}

// handleBlockPreamble is the shared serial preamble that BOTH handleBlockMsg
// and handleBlockMsgWithWindow run before any block-processing work.
//
// It must execute on the single blockHandler drain goroutine. All state it
// touches (peerStates, headerList, requestedBlocks, blockSizeTracker,
// startHeader) is drain-goroutine-only; the only atomic load is
// headersFirstMode (safe from any goroutine). NOTE: handleHeadersMsg mutates
// the same header state (headerList/startHeader/nextCheckpoint/headerCheckpoint)
// and is therefore ALSO dispatched onto this one drain goroutine (via
// headersQueue in blockHandler) — never `go handleHeadersMsg`. Do not
// reintroduce a separate goroutine for it: with the headers-first pipeline the
// two overlap in time and would race this state.
//
// On success it returns the resolved peer, its peerSyncState, the
// catchingBlocks flag, the isCheckpointBlock flag, and the block's
// headers-first chain height (headerHeight, or -1 when not resolvable from
// the header list). The header-list height is the authoritative,
// PoW-verified, parent-independent height and is the correct source for
// windowed blocks, which are streamed ahead of commit (so a parent lookup
// would race the commit).
// On failure it returns a non-nil error; the caller must propagate it.
func (sm *SyncManager) handleBlockPreamble(caller string, bmsg *blockQueueMsg) (
	resolvedPeer *peerpkg.Peer,
	state *peerSyncState,
	catchingBlocks bool,
	isCheckpointBlock bool,
	headerHeight int32,
	err error,
) {
	resolvedPeer = bmsg.peer
	headerHeight = -1

	state, resolved, exists := sm.peerStateResolvingPrimary(resolvedPeer)
	if !exists {
		sm.logger.Errorf("[%s][%s] Received block message from unknown peer %s", caller, bmsg.blockHash, resolvedPeer)
		err = errors.NewServiceError("[%s] Received block message from unknown peer %s", caller, resolvedPeer)
		return
	}
	if resolved != resolvedPeer {
		// Stream peers (e.g. BlockPriority) are not registered in peerStates
		// directly - resolved via their association's primary peer instead.
		sm.logger.Debugf("[%s][%s] resolved stream peer %s to primary peer %s", caller, bmsg.blockHash, resolvedPeer, resolved)
		resolvedPeer = resolved
	}

	// Under async prefetch, awaitBlockResult disconnects the source peer on its
	// first validation failure, but blocks it already admitted keep draining the
	// queue FIFO until handleDonePeerMsg evicts peerStates — a racy window in
	// which we would validate the whole tail of a peer that has already proven it
	// serves bad blocks. Peer.Disconnect* flips the connected flag synchronously
	// (atomic), so skipping here as soon as that flag drops deterministically
	// stops the tail, bounding wasted validation to the block already in flight
	// (disconnect on first validation failure before draining the rest). We test
	// bmsg.peer — the exact peer OnBlock queued and awaitBlockResult disconnects
	// (sp.Peer) — NOT the resolved primary: for a BlockPriority stream the failure
	// disconnects only the stream sub-peer while its association primary stays
	// connected, so the resolved `peer` would miss precisely the streaming case
	// this guards. The ServiceError is benign to shouldDisconnectOnBlockErr, so
	// it only makes awaitBlockResult release budget and log — no second
	// disconnect. Gated on UsePrefetchIngestion so the regtest/synchronous path,
	// where block-acceptance tooling feeds blocks in ways this must not disturb,
	// is completely untouched.
	// chainParams nil-check: UsePrefetchIngestion dereferences chainParams.Net,
	// and window unit tests construct minimal SyncManagers without chain params.
	if sm.chainParams != nil && sm.UsePrefetchIngestion() && !bmsg.peer.Connected() {
		sm.logger.Debugf("[%s][%s] skipping block from disconnected peer %s", caller, bmsg.blockHash, bmsg.peer)
		err = errors.NewServiceError("[%s] skipping block %s from disconnected peer %s", caller, bmsg.blockHash, bmsg.peer)
		return
	}

	fsmState, fsmErr := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if fsmErr != nil {
		err = errors.NewProcessingError("[%s] failed to get current FSM state", caller, fsmErr)
		return
	}

	if fsmState != nil && *fsmState == teranodeblockchain.FSMStateCATCHINGBLOCKS {
		catchingBlocks = true
	}

	// If we didn't ask for this block then the peer is misbehaving.
	if _, reqExists := state.requestedBlocks.Get(bmsg.blockHash); !reqExists {
		// The regression test intentionally sends some blocks twice
		// to test duplicate block insertion fails.  Don't disconnect
		// the peer or ignore the block when we're in regression test
		// mode, in this case, so the chain code is actually fed the
		// duplicate blocks.
		if sm.chainParams != &chaincfg.RegressionNetParams {
			// Tolerate deliveries of blocks the node still NEEDS: a rotation's
			// clearRequestedState/resetHeaderState (and the 60s ledger TTL)
			// orphan in-flight deliveries we genuinely asked for, and executing
			// the deliverers cascaded into peer-set collapse (measured: 8 peers
			// disconnected within one second, 4s after a rotation). Only a hash
			// that is neither in the live header index nor in the reset
			// carryover is treated as spam and disconnects.
			if sm.blockStillNeeded(bmsg.blockHash) {
				sm.logger.Debugf("[%s] block %v not in the per-peer ledger but still needed (post-rotation redelivery); processing", caller, bmsg.blockHash)
			} else {
				reason := fmt.Sprintf("Got unrequested block %v", bmsg.blockHash)
				resolvedPeer.DisconnectWithWarning(reason)
				err = errors.NewServiceError("Got unrequested block %v", bmsg.blockHash)
				return
			}
		}
	}

	// When in headers-first mode, recognise the checkpoint block by its HASH
	// directly, independent of the header list's front position (the C-NEW fix).
	// With continuous pipelining the checkpoint block can arrive while a leading
	// seed node still sits ahead of it at the front, so a plain Front()-position
	// check is not a reliable proxy. Recognising by hash decouples checkpoint
	// recognition from list position, so nextCheckpoint still advances (and the
	// final checkpoint still clears headers-first mode) regardless of where the
	// checkpoint node currently sits. The checkpoint hash is unique, so this is
	// equivalent-or-better than the old position proxy.
	//
	// Independently of checkpoint recognition, maintain the header list by
	// consuming it from the OLDEST (front) end as blocks commit — the only place
	// nodes are removed, so removal is always tied to block-fetch progress and
	// never to a header-download event. Two node kinds sit at the front:
	//   - a leading SEED node (headerListSeed): the DB-best node, or a previous
	//     checkpoint node kept for the next interval's linkage. Its block is
	//     already accounted for, so no block message matches it; it must be
	//     dropped the moment the block that follows it commits, otherwise it
	//     wedges the front and starves the next block's height sourcing.
	//   - real block nodes, which arrive and commit in header (front) order.
	// The loop below drops a leading seed if the arriving block is not it, then
	// matches the arriving block at the (new) front, sources its authoritative
	// PoW-verified height, and removes it — unless it is the checkpoint, which is
	// retained as the new seed so the next interval's headers can link onto it.
	if sm.headersFirstMode.Load() {
		// All header-state mutation below is pure (no I/O), so hold headerMu for
		// the whole block to serialise it against the outer-loop reset path.
		sm.headerMu.Lock()

		if sm.nextCheckpoint != nil && bmsg.blockHash.IsEqual(sm.nextCheckpoint.Hash) {
			isCheckpointBlock = true
			// The checkpoint carries the same authoritative, PoW-verified,
			// parent-independent height as its header-list node. Surface it here
			// so the window path keeps the correct height even if the checkpoint
			// node is not the current front (e.g. a leading seed still ahead of
			// it, dropped by the loop below).
			headerHeight = sm.nextCheckpoint.Height
		}

		for {
			firstNodeEl := sm.headerList.Front()
			if firstNodeEl == nil {
				break
			}

			firstNode := firstNodeEl.Value.(*headerNode)

			if bmsg.blockHash.IsEqual(firstNode.hash) {
				// The header-list node carries the authoritative, PoW-verified,
				// parent-independent height.
				headerHeight = firstNode.height

				if isCheckpointBlock {
					// Retain the checkpoint node as the new leading seed: the next
					// interval's first header must prove it links onto it. It is
					// dropped later, when the block that follows it commits.
					sm.headerListSeed = firstNodeEl
					// Keep the index entry: the seed stays in the list and its
					// hash must remain resolvable until the seed itself is removed.
				} else {
					sm.headerList.Remove(firstNodeEl)
					delete(sm.headerHeightIndex, *firstNode.hash)
					delete(sm.recentlyNeededUntil, *firstNode.hash)
				}

				break
			}

			// The arriving block is not the front node. If the front is the stale
			// leading seed (its block is already accounted for), drop it and
			// re-check the new front — this is where a committed checkpoint/DB-best
			// seed is finally removed, tied to the commit of the block after it.
			// Otherwise the front is a real node whose block has not committed yet
			// (blocks commit in front order, so this should not normally happen);
			// leave the list untouched.
			if firstNodeEl == sm.headerListSeed {
				sm.headerList.Remove(firstNodeEl)
				delete(sm.headerHeightIndex, *firstNode.hash)
				delete(sm.recentlyNeededUntil, *firstNode.hash)
				sm.headerListSeed = nil

				continue
			}

			break
		}

		// For blocks that arrive out of order (not at Front() — the multi-peer
		// parallel download case), the loop above exits without setting headerHeight.
		// Fall back to the index, which maps every known hash to its authoritative
		// PoW-verified height regardless of list position.
		if headerHeight == -1 {
			if h, ok := sm.headerHeightIndex[bmsg.blockHash]; ok {
				headerHeight = h
			}
		}

		sm.headerMu.Unlock()
	}

	// Remove block from request maps. Either chain will know about it, and
	// so we shouldn't have any more instances of trying to fetch it, or we
	// will fail the insert, and thus we'll retry next time we get an inv.
	state.requestedBlocks.Delete(bmsg.blockHash)
	sm.requestedBlocks.Delete(bmsg.blockHash)
	// Keep the stall-detector tracking maps (Task 2.4) in lock-step with the
	// global requestedBlocks ledger: the block has arrived, so its assignment is
	// no longer outstanding.
	sm.assignedMu.Lock()
	delete(sm.assignedTo, bmsg.blockHash)
	delete(sm.assignedAt, bmsg.blockHash)
	// The block has arrived; it is no longer an orphan awaiting re-fetch.
	delete(sm.refetchBlocks, bmsg.blockHash)
	sm.assignedMu.Unlock()

	// Count this accepted block against the delivering peer for observability.
	// The delivering peer is bmsg.peer (before any stream-peer resolution) for
	// address labelling, because resolvedPeer may have been remapped to the
	// primary in the stream-peer case above.
	prometheusLegacyNetsyncBlocksReceived.WithLabelValues(bmsg.peer.Addr()).Inc()

	// Track block size AND transaction count for dynamic in-flight adjustment
	// during headers-first mode. The in-flight bound is now a transaction WORK
	// budget (with a byte-budget safety clamp): tiny blocks stream
	// many-in-flight to keep the peer busy, fat blocks stay few-in-flight to
	// bound memory. See calculateMaxInFlightBlocks.
	if sm.headersFirstMode.Load() && bmsg.block != nil {
		blockSize := int64(bmsg.block.SerializeSize())
		txCount := int64(len(bmsg.block.Transactions))
		sm.blockSizeTracker.addBlockStats(blockSize, txCount)

		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()
		avgSize := sm.blockSizeTracker.getAverageSize()
		avgTxCount := sm.blockSizeTracker.getAverageTxCount()
		sm.logger.Debugf("[%s][%s] Block size: %d bytes (%d txs), avg: %d bytes / %d txs, dynamic max in-flight: %d",
			caller, bmsg.blockHash, blockSize, txCount, avgSize, avgTxCount, dynamicMax)
	}

	return
}

// handleBlockMsg handles block messages from all peers.
func (sm *SyncManager) handleBlockMsg(bmsg *blockQueueMsg) error {
	sm.logger.Debugf("[handleBlockMsg][%s] received block height %d from %s", bmsg.blockHash, bmsg.blockHeight, bmsg.peer)

	// The direct path derives height inside HandleBlockDirect, so the
	// header-list height from the preamble is unused here.
	peer, state, catchingBlocks, isCheckpointBlock, _, err := sm.handleBlockPreamble("handleBlockMsg", bmsg)
	if err != nil {
		return err
	}

	sm.logger.Debugf("[handleBlockMsg][%s] calling HandleBlockDirect", bmsg.blockHash)

	// Hand sole ownership of the decoded block to HandleBlockDirect. The
	// blockHandler goroutine keeps *bmsg alive until the reply is sent, so
	// leaving the field set would pin the multi-GB wire block (and its decode
	// arena) for the whole minutes-long processing of a big block. Copy the
	// parent hash first — the missing-parent error path below needs it.
	msgBlock := bmsg.block
	if msgBlock == nil {
		return errors.NewProcessingError("[handleBlockMsg][%s] block message carries no block", bmsg.blockHash)
	}

	prevBlockHash := msgBlock.Header.PrevBlock
	bmsg.block = nil

	// Process the block directly. A missing-parent error (ErrBlockNotFound)
	// always triggers a getblocks request from our best block so block
	// validation can proceed in order — see the orphan-continuation note below.
	if err = sm.HandleBlockDirect(sm.ctx, bmsg.peer, bmsg.blockHash, msgBlock); err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			// We don't have the parent of this block. While catching blocks
			// this is typically the peer announcing its tip while we are
			// still behind — and in the legacy sync protocol that orphan tip
			// doubles as the batch-continuation signal: the peer pushes its
			// tip inv after delivering a getblocks batch and waits for the
			// next getblocks before sending more. Swallowing the orphan here
			// stalls the sync until the stall detector rotates the peer, so
			// always answer with a getblocks from our best block.
			// PushGetBlocksMsg filters duplicate requests and the peer only
			// invs blocks past the locator fork point, so a redundant
			// request costs one inv message at most.
			sm.logger.Infof("Block %v has missing parent %v, requesting missing blocks",
				bmsg.blockHash, prevBlockHash)

			bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
			if err != nil {
				sm.logger.Errorf("Failed to get best block header: %v", err)
				return nil
			}
			// Create a block locator starting from the parent hash
			locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf("Failed to get block locator for the block hash %s: %v",
					bmsg.blockHash, err)
				return nil
			}

			// Send a getblocks message to request missing blocks
			zeroHash := chainhash.Hash{}
			if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
				sm.logger.Errorf("Failed to send getblocks message: %v", err)

				return nil
			}

			return nil
		} else {
			if errors.Is(err, context.Canceled) || errors.IsContextError(err) {
				return nil
			}

			serviceError := errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError)
			if !catchingBlocks && !serviceError {
				peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
			}

			sm.logger.Errorf("Failed to process new block in service blockQueueMsg %v: %v", bmsg.blockHash, err)

			// Never panic in sync processing goroutines; bubble error to caller.
			return err
		}
	}

	// Meta-data about the new block this peer is reporting. We use this
	// below to update this peer's latest block height and the heights of
	// other peers based on their last announced block hash. This allows us
	// to dynamically update the block heights of peers, avoiding stale
	// heights when looking for a new sync peer. Upon acceptance of a block
	// or recognition of an orphan, we also use this information to update
	// the block heights over other peers who's invs may have been ignored
	// if we are actively syncing while the chain is not yet current or
	// who may have lost the lock announcement race.
	var (
		heightUpdate  int32
		blkHashUpdate *chainhash.Hash
	)

	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	// When the block is not an orphan, log information about it and update the chain state.

	// Update this peer's latest block height, for future potential sync node candidacy.
	// bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	// if err != nil {
	//	return errors.NewServiceError("failed to get best block header", err)
	// }

	heightUpdate = bmsg.blockHeight
	blkHashUpdate = &bmsg.blockHash

	if heightUpdate <= 0 {
		// get the height of the new block from the blockchain store
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &bmsg.blockHash)
		if err != nil {
			sm.logger.Errorf("Failed to get block header for block %v: %v", bmsg.blockHash, err)
		} else {
			blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf("failed to convert block height to int32: %v", err)
			}

			heightUpdate = blockHeightInt32
		}
	}

	sm.logger.Infof("accepted block %v at height %d", bmsg.blockHash, heightUpdate)

	// Clear the rejected transactions.
	sm.rejectedTxns.Clear()

	// Update the block height for this peer. But only send a message to
	// the server for updating peer heights if this is an orphan or our
	// chain is "current". This avoids sending a spammy amount of messages
	// if we're syncing the chain from scratch.
	if heightUpdate != 0 {
		peer.UpdateLastBlockHeight(heightUpdate)
		sm.logger.Debugf("peer %s reports new best height %d, current %v", peer.String(), peer.LastBlock(), sm.current())

		if sm.current() { // used to check for isOrphan || sm.current()
			go sm.peerNotifier.UpdatePeerHeights(blkHashUpdate, heightUpdate, peer)

			// Since we are current, we can tell FSM to transition to RUN
			// Blockchain client will check if miner is registered, if so it will send Mine event, and FSM will transition to Mine
			if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/handleBlockMsg"); err != nil {
				sm.logger.Errorf("[Sync Manager] failed to send FSM RUN event %v", err)
			}

			sm.resetFeeFilterToDefault()
		}
	}

	// This is headers-first mode, so if the block is not a checkpoint
	// request more blocks using the header list to maintain the pipeline
	// at the dynamic max limit (adjusts based on block size).
	if !isCheckpointBlock {
		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()

		// Snapshot startHeader under headerMu; fetchHeaderBlocks takes headerMu
		// itself, so it must be called after releasing the lock.
		sm.headerMu.Lock()
		hasStartHeader := sm.startHeader != nil
		sm.headerMu.Unlock()

		if hasStartHeader && state.requestedBlocks.Len() < dynamicMax {
			sm.topUpBlockFetch()
		} else if !sm.current() && state.requestedBlocks.Len() == 0 {
			sm.logger.Debugf("Not current, and no headers to sync to, fetching more headers")

			latestBlockHeader, _, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
			if err != nil {
				return errors.NewServiceError("Failed to get best block header", err)
			}

			locator := blockchain.BlockLocator([]*chainhash.Hash{latestBlockHeader.Hash()})
			if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
				return errors.NewServiceError("Failed to send getblocks message to peer %s", peer.String(), err)
			}
		}

		return nil
	}

	// This is headers-first mode and the block is a checkpoint. Advance the
	// block-level checkpoint tracker only. The next interval's headers are NOT
	// requested here anymore — handleHeadersMsg already pipelined them ahead
	// when it reached this checkpoint's headers, so requesting again would be a
	// redundant, duplicate getheaders. When there is no further checkpoint we
	// switch to normal mode below.
	sm.headerMu.Lock()
	prevHeight := sm.nextCheckpoint.Height
	sm.nextCheckpoint = sm.findNextHeaderCheckpoint(prevHeight)

	if sm.nextCheckpoint != nil {
		sm.headerMu.Unlock()
		return nil
	}

	// This is headers-first mode, the block is a checkpoint, and there are
	// no more checkpoints, so switch to normal mode by requesting blocks
	// from the block after this one up to the end of the chain (zero hash).
	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.headerListSeed = nil
	sm.headerMu.Unlock()

	sm.logger.Infof("Reached the final checkpoint -- switching to normal mode")

	locator := blockchain.BlockLocator([]*chainhash.Hash{&bmsg.blockHash})
	if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
		return errors.NewServiceError("Failed to send getblocks message to peer %s", peer.String(), err)
	}

	return nil
}

// blockAdmitOutcome is the result of handleBlockMsgWithWindow: whether the block
// was processed directly / not added (Direct — the legacy !added semantics),
// added to the window accumulator (Windowed — the legacy added semantics), or
// parked ahead of the block-assembly maturity gate (Parked). The three outcomes
// drive distinct backlog/ack/requeue accounting in the drain loop.
type blockAdmitOutcome int

const (
	blockAdmitDirect   blockAdmitOutcome = iota // processed directly or not added
	blockAdmitWindowed                          // added to the window accumulator
	blockAdmitParked                            // parked ahead of block assembly; early-acked, released later
)

// handleBlockMsgWithWindow is the window-enabled variant of handleBlockMsg.
// It delegates the shared serial preamble to handleBlockPreamble, then — when
// the block is eligible for the window path (legacyUnified returns true) —
// prepares the block via prepareBlockForWindow and adds it to the accumulator
// instead of calling HandleBlockDirect.
//
// Returns (addedToWindow bool, err error).
// When addedToWindow=true the block was added to the window; the caller (the
// drain goroutine) sends the accept-time ack via ackWindowedBlock.
// When addedToWindow=false the block was processed directly (or failed); err
// carries the outcome and the caller sends it as the reply.
//
// Post-processing (peer height update, FSM RUN, header pipeline advance) only
// runs on the direct path (addedToWindow=false) because ProcessBlockWindow
// does it server-side for windowed blocks. The peer-height-update and FSM-RUN
// branches inside runPostBlockProcessing are guarded by sm.current(). Windowed
// blocks are below the hardcoded checkpoint, so !sm.current() is guaranteed
// (the sync peer's LastBlock() far exceeds our best height); those branches
// would also be skipped on the normal handleBlockMsg path for the same reason.
// There is no behaviour divergence.
func (sm *SyncManager) handleBlockMsgWithWindow(bmsg *blockQueueMsg, wa *windowAccumulator, flushWindow, flushWindowSync func(), park *parkStore) (outcome blockAdmitOutcome, retErr error) {
	sm.logger.Debugf("[handleBlockMsgWithWindow][%s] received block height %d from %s", bmsg.blockHash, bmsg.blockHeight, bmsg.peer)

	peer, state, catchingBlocks, isCheckpointBlock, headerHeight, preambleErr := sm.handleBlockPreamble("handleBlockMsgWithWindow", bmsg)
	if preambleErr != nil {
		return blockAdmitDirect, preambleErr
	}

	// headerHeightIndex lifecycle across a tolerated-failure requeue.
	// handleBlockPreamble CONSUMES this block's index entry at early-ack admission
	// (front-removal, see the delete near "headerList.Remove" above) BEFORE this
	// function decides the block's fate. On a tolerated (non-peer-fault) error the
	// drain loop requeues the block (requeueFailedBlock); with the entry already
	// gone, the re-fetched OUT-OF-ORDER arrival resolves headerHeight=-1, falls into
	// the default parent-lookup arm, fails BLOCK_NOT_FOUND for a not-yet-committed
	// parent — itself a tolerated error — and requeues again, spinning forever.
	// Restore the authoritative PoW-verified height on exactly the drain loop's
	// requeue condition (tolerated error) so the re-fetch stays height-resolvable
	// and re-enters the normal window/park path; delete it on any other exit
	// (committed, parked, or peer-fault — not requeued). This bounds the index to
	// the header-list frontier plus the live refetch set. Guarded by
	// headersFirstMode so it is a no-op post-checkpoint and on the single-peer /
	// pre-park direct path. No return point below holds headerMu (handleBlockPreamble
	// releases it; pumpBlockRequests takes/releases it internally), so this cannot
	// deadlock.
	defer func() {
		if !sm.headersFirstMode.Load() || headerHeight <= 0 {
			return
		}

		sm.headerMu.Lock()
		// Production always initialises headerHeightIndex in New(); guard the write
		// so a minimally-constructed test SyncManager with a nil map is a no-op,
		// matching the nil-safe read/delete used elsewhere in the header path.
		if sm.headerHeightIndex != nil {
			if retErr != nil && !BlockProcessingErrorIsPeerFault(retErr) {
				sm.headerHeightIndex[bmsg.blockHash] = headerHeight
			} else {
				delete(sm.headerHeightIndex, bmsg.blockHash)
				delete(sm.recentlyNeededUntil, bmsg.blockHash)
			}
		}
		sm.headerMu.Unlock()
	}()

	// Already-committed guard (mirrors HandleBlockDirect, handle_block.go). A peer
	// can re-deliver a block that is already committed to our chain — observed on
	// mainnet when an old block (hundreds below the tip) is re-sent. Without this
	// check the window path re-runs prepareBlockForWindow/createBlockUTXOs, which
	// re-reads the block's subtree data from the blob store; but an old block's
	// subtree has been DAH-pruned (retention ≪ the gap), so the read fails
	// "subtree not found locally", bounded recovery loops forever, and the window
	// never advances — deadlocking block assembly behind it.
	//
	// Placed before any prepare/window-admission/subtree work and before the
	// legacyUnified/checkpoint branch, so it guards both the window-add and the
	// direct-fallback branches whether or not the window is enabled-and-below-
	// checkpoint. On skip we return (false, nil): the caller (blockHandler) then
	// decrements blockBacklog and acks the peer via reply <- nil exactly as for a
	// normal not-added block — no double-ack, no backlog leak.
	// Window-ownership guard (the parked-twin double-commit). A block the window
	// pipeline already owns — parked, accumulated, or inside an in-flight flush
	// job — is not yet committed, so the GetBlockExists guard below cannot see
	// it, and it is no longer in requestedBlocks (wiped by the preamble on its
	// FIRST arrival), so rotation-driven re-walks re-request it and the copy is
	// re-delivered here minutes later. Without this check the re-delivery pays
	// the full prepare pass again and parks a TWIN of the same height, which
	// releaseParkedBlocks then splits across two successive flush jobs — the
	// FIFO worker commits the same block twice, one job apart. Skip BEFORE the
	// GetBlockExists round-trip; pump exactly like the already-committed skip
	// below so a skipped re-delivery still advances sync.
	if sm.windowBlockOwned(bmsg.blockHash) {
		sm.logger.Infof("[handleBlockMsgWithWindow][%s] block already owned by the window pipeline (parked or committing), skipping re-delivery", bmsg.blockHash)
		sm.pumpBlockRequests(peer, state, isCheckpointBlock, bmsg.blockHash)

		return blockAdmitDirect, nil
	}

	blockExists, existsErr := sm.blockchainClient.GetBlockExists(sm.ctx, &bmsg.blockHash)
	if existsErr != nil {
		sm.logger.Errorf("[handleBlockMsgWithWindow][%s] failed to check if block exists: %s", bmsg.blockHash, existsErr)
		return blockAdmitDirect, errors.NewProcessingError("failed to check if block exists", existsErr)
	}

	if blockExists {
		sm.logger.Warnf("[handleBlockMsgWithWindow][%s] block already exists, skipping", bmsg.blockHash)

		// Skipping is not standing still. The preamble already deleted this block
		// from state.requestedBlocks, so if we returned here without pumping, a peer
		// re-delivering an already-committed range (e.g. after a mid-chain restart)
		// would drain the in-flight count to zero with no new request ever issued —
		// the peer keeps re-sending the same committed range forever, the node never
		// reaches blocks past our tip, and block assembly stays wedged. Run the same
		// next-block-request pump the accept path runs so a skipped block advances
		// sync exactly like an accepted one (mirroring the direct path, which pumps by
		// fall-through after HandleBlockDirect's own already-exists guard). Pure
		// sync-request plumbing: no validation, UTXO write, or commit-order change.
		// peer/state/isCheckpointBlock were all resolved by the preamble above.
		sm.pumpBlockRequests(peer, state, isCheckpointBlock, bmsg.blockHash)

		return blockAdmitDirect, nil
	}

	msgBlock := bmsg.block
	if msgBlock == nil {
		return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] block message carries no block", bmsg.blockHash)
	}

	// Determine block height. The authoritative source is the headers-first
	// header chain (headerHeight from the preamble): it is PoW-verified and
	// parent-independent. This matters because the window streams blocks
	// ahead of commit (early-ack), so block N+1 can be processed before N is
	// committed — a parent (prev-block-header) lookup would then race the
	// commit and fail with BLOCK_NOT_FOUND, wedging the node. Below the
	// hardcoded checkpoint IBD is always in headers-first mode, so
	// headerHeight is populated for every normal windowed block.
	//
	// The block.Height()/parent-lookup path is retained ONLY as a fallback
	// for edge cases outside headers-first mode (headerHeight <= 0), where
	// no parent race exists.
	prevBlockHash := msgBlock.Header.PrevBlock
	bmsg.block = nil

	block := bsvutil.NewBlock(msgBlock)

	var blockHeightUint32 uint32

	switch {
	case headerHeight > 0:
		h, convErr := safeconversion.Int32ToUint32(headerHeight)
		if convErr != nil {
			return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] failed to convert header-chain height", bmsg.blockHash, convErr)
		}

		blockHeightUint32 = h
		block.SetHeight(headerHeight)
	case block.Height() > 0:
		h, convErr := safeconversion.Int32ToUint32(block.Height())
		if convErr != nil {
			return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] failed to convert block height", bmsg.blockHash, convErr)
		}

		blockHeightUint32 = h
	default:
		_, prevMeta, headerErr := sm.blockchainClient.GetBlockHeader(sm.ctx, &prevBlockHash)
		if headerErr != nil {
			return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] failed to get prev block header for height determination", bmsg.blockHash, headerErr)
		}

		blockHeightUint32 = prevMeta.Height + 1
		block.SetHeight(int32(blockHeightUint32)) //nolint:gosec
	}

	// Check window eligibility. An ineligible block is processed directly.
	// Checkpoint blocks are always ineligible: windowed blocks skip
	// runPostBlockProcessing, which for a checkpoint block advances
	// sm.nextCheckpoint, sends PushGetHeadersMsg, and clears headersFirstMode.
	// Allowing a checkpoint block into the window would permanently stall the
	// header pipeline.  Flush any pending window first (ordering/parent-
	// availability guarantee) and then process the checkpoint normally.
	if !sm.legacyUnified(blockHeightUint32) || isCheckpointBlock {
		// Not eligible for window — process directly using the full HandleBlockDirect flow.
		// We already extracted prevBlockHash and cleared bmsg.block above.
		//
		// In pipeline mode flushWindow() is a non-blocking hand-off to the flush
		// worker, so the pending window commits asynchronously and this direct
		// call to HandleBlockDirect may race it. That race is safe and cannot
		// gap or reorder the committed chain. Windowed blocks commit only via
		// ProcessBlockWindow→AddBlock/StoreBlock (a single atomic row in the
		// blocks table; there is no header-ahead table), so a window block's row
		// exists only after its full commit. If the in-flight window parent has
		// not yet committed when HandleBlockDirect runs, its pre-flight
		// GetBlockHeader(prev) (handle_block.go:71) queries that same blocks
		// table and returns ErrBlockNotFound. NewProcessingError wraps it but
		// preserves the inner code, so errors.Is(directErr, ErrBlockNotFound)
		// is true. The ErrBlockNotFound branch immediately below then
		// issues PushGetBlocksMsg and returns (false, nil) — no disconnect, no
		// reject, tip not advanced — and the block is simply re-requested once
		// the window has committed. The committed tip only ever advances via the
		// FIFO worker's contiguous ascending commit sequence.
		// (The drain loop also flushes on the !added return, but that is too
		// late — it runs after HandleBlockDirect has already returned.)
		//
		// For the CHECKPOINT block the async hand-off is not enough: its parent may
		// still be committing in the window worker, so HandleBlockDirect's pre-flight
		// parent check races the worker and fails (ErrBlockNotFound), then only the
		// slow re-request path recovers — the deterministic checkpoint-boundary
		// cold-start stall. flushWindowSync blocks until the pending window has
		// actually committed, making the parent-availability guarantee real. Other
		// (non-checkpoint) direct blocks keep the cheaper async hand-off.
		if isCheckpointBlock {
			flushWindowSync()
		} else {
			flushWindow()
		}

		directErr := sm.HandleBlockDirect(sm.ctx, peer, bmsg.blockHash, msgBlock)
		if directErr != nil {
			if errors.Is(directErr, errors.ErrBlockNotFound) {
				// Checkpoint deferral (checkpoint-boundary stall elimination). On a
				// fresh sync the checkpoint's parent is structurally uncommitted at
				// first delivery, and dropping the block here loses it forever: the
				// preamble wiped every fetch ledger on arrival, this arm returns nil
				// so the requeue gate never fires, the header cursor never rewinds,
				// and the getblocks fallback below is dead in headers-first mode
				// (processInvMsg discards all inv). The only recovery was the
				// 3-minute sync-peer rotation — the entire observed pause. Keep the
				// delivery instead and retry from the refill tick once the parent
				// lands; the rotation stays the guaranteed backstop via the deferral
				// deadline and the one-shot re-defer bar.
				if isCheckpointBlock && sm.settings.Legacy.InFlightRefillInterval > 0 && sm.deferBarredCheckpoint != bmsg.blockHash {
					sm.deferredCheckpoint = &deferredCheckpointBlock{
						msgBlock:   msgBlock,
						bmsg:       &blockQueueMsg{blockHash: bmsg.blockHash, blockHeight: bmsg.blockHeight, peer: bmsg.peer},
						peer:       peer,
						state:      state,
						prevHash:   prevBlockHash,
						deferredAt: time.Now(),
					}
					sm.logger.Infof("[handleBlockMsgWithWindow][%s] deferring checkpoint block height %d until parent %s commits (retried on the refill tick)", bmsg.blockHash, blockHeightUint32, prevBlockHash)

					return blockAdmitDirect, nil
				}

				sm.logger.Infof("Block %v has missing parent %v, requesting missing blocks", bmsg.blockHash, prevBlockHash)

				bestBlockHeader, bestBlockHeaderMeta, getErr := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
				if getErr != nil {
					sm.logger.Errorf("Failed to get best block header: %v", getErr)
					return blockAdmitDirect, nil
				}

				locator, locErr := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
				if locErr != nil {
					sm.logger.Errorf("Failed to get block locator for the block hash %s: %v", bmsg.blockHash, locErr)
					return blockAdmitDirect, nil
				}

				zeroHash := chainhash.Hash{}
				if pushErr := peer.PushGetBlocksMsg(locator, &zeroHash); pushErr != nil {
					sm.logger.Errorf("Failed to send getblocks message: %v", pushErr)
				}

				return blockAdmitDirect, nil
			}

			if errors.Is(directErr, context.Canceled) || errors.IsContextError(directErr) {
				return blockAdmitDirect, nil
			}

			serviceError := errors.Is(directErr, errors.ErrServiceError) || errors.Is(directErr, errors.ErrStorageError)
			if !catchingBlocks && !serviceError {
				peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
			}

			sm.logger.Errorf("Failed to process new block in service blockQueueMsg %v: %v", bmsg.blockHash, directErr)
			return blockAdmitDirect, directErr
		}

		sm.runPostBlockProcessing(peer, state, bmsg, isCheckpointBlock)
		return blockAdmitDirect, nil
	}

	// Block-assembly back-pressure gate (coinbase-maturity ceiling), parity with
	// HandleBlockDirect (handle_block.go). When parking is enabled and the cache is
	// evaluable we classify NON-BLOCKINGLY instead of freezing the drain goroutine
	// for up to windowMaturityMaxWait: a block within the gate is prepared and
	// admitted now; a block beyond the gate is prepared, early-acked and PARKED, so
	// the drain loop returns immediately and can refetch the low block and flush.
	// Parked blocks are released back into the window ascending as block assembly
	// advances (releaseParkedBlocks, on the refill tick). When parking is disabled
	// or the cache is not yet evaluable we keep the original blocking wait —
	// byte-identical to today.
	parkThisBlock := false

	if park != nil {
		admit, evaluable := sm.blockAssemblyGateAdmitsCached(blockHeightUint32)
		switch {
		case !evaluable:
			if waitErr := sm.waitForBlockAssemblyReadyCached(sm.ctx, blockHeightUint32); waitErr != nil {
				return blockAdmitDirect, waitErr
			}
		case admit:
			// Within the gate: admit now (fall through to prepare + wa.add).
		default:
			// Beyond the gate: park after preparing. BOTH park caps are checked before
			// prepareBlockForWindow so a full buffer never pays the full prepare pass
			// (subtree build + blob writes) for a block that will only be rejected and
			// requeued via the tolerated-error direct path (self-correcting). The byte
			// pre-check is exact, not an estimate: the prepared block's SizeInBytes is
			// literally MsgBlock().SerializeSize() (prepareBlockForWindow). The
			// post-prepare byte check below stays as defence in depth.
			if park.countFull() {
				// The peer just delivered this block; we refuse it (park full),
				// which is self-backpressure, not a dead peer. Stamp lastBlockTime
				// so the stall detector does not rotate our best data source
				// mid-refusal-storm. See noteSyncPeerBlockDelivery.
				sm.noteSyncPeerBlockDelivery(peer)
				return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] park buffer full (count) at height %d; will re-fetch", bmsg.blockHash, blockHeightUint32)
			}

			if park.full(int64(msgBlock.SerializeSize())) {
				sm.noteSyncPeerBlockDelivery(peer)
				return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] park buffer full (bytes) at height %d; will re-fetch", bmsg.blockHash, blockHeightUint32)
			}

			parkThisBlock = true
		}
	} else {
		if waitErr := sm.waitForBlockAssemblyReadyCached(sm.ctx, blockHeightUint32); waitErr != nil {
			return blockAdmitDirect, waitErr
		}
	}

	// Prepare exactly once: both the admit and the park paths need a prepared block.
	prepared, prepErr := sm.prepareBlockForWindow(sm.ctx, peer, bmsg.blockHash, msgBlock, blockHeightUint32)
	if prepErr != nil {
		if errors.Is(prepErr, context.Canceled) || errors.IsContextError(prepErr) {
			return blockAdmitDirect, nil
		}

		serviceError := errors.Is(prepErr, errors.ErrServiceError) || errors.Is(prepErr, errors.ErrStorageError)
		if !catchingBlocks && !serviceError {
			peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
		}

		sm.logger.Errorf("[handleBlockMsgWithWindow][%s] prepareBlockForWindow failed: %v", bmsg.blockHash, prepErr)

		return blockAdmitDirect, prepErr
	}

	if parkThisBlock {
		if park.full(int64(prepared.SizeInBytes)) { //nolint:gosec
			sm.noteSyncPeerBlockDelivery(peer)
			return blockAdmitDirect, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] park buffer full (bytes) at height %d; will re-fetch", bmsg.blockHash, blockHeightUint32)
		}

		park.add(prepared)
		sm.claimWindowBlock(bmsg.blockHash, blockHeightUint32)
		sm.logger.Debugf("[handleBlockMsgWithWindow][%s] parked block height %d ahead of block assembly; %d parked", bmsg.blockHash, blockHeightUint32, park.len())

		// A parked block arrived from the peer — proof it is alive even though we
		// defer committing it. Stamp lastBlockTime so a long park run does not look
		// like a stall. See noteSyncPeerBlockDelivery.
		sm.noteSyncPeerBlockDelivery(peer)

		return blockAdmitParked, nil
	}

	wa.add(prepared)
	sm.claimWindowBlock(bmsg.blockHash, blockHeightUint32)

	// Refresh the sync peer's last-block time on the accept path (mirrors the
	// non-window path; prevents a false sync-peer rotation when blockBacklog drains).
	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	// Advance the headers-first block-download pump for every windowed block.
	sm.pumpBlockRequests(peer, state, isCheckpointBlock, bmsg.blockHash)

	return blockAdmitWindowed, nil
}

// pumpBlockRequests advances the headers-first block-download pipeline: it either
// refills the in-flight window from the pending header list (fetchHeaderBlocks) or,
// when the header list is exhausted and nothing is in flight and we are not yet
// current, sends a getblocks from our best block so the peer keeps delivering the
// next range. This is the same pump the direct path (handleBlockMsg) runs by
// fall-through after processing a block; extracting it lets both the window
// accept path and the window already-committed skip path drive sync forward
// identically. A checkpoint block is a no-op here (checkpoint handling advances
// the header pipeline separately via runPostBlockProcessing / HandleBlockDirect).
//
// All state touched (headerList, startHeader, requestedBlocks) is
// drain-goroutine-only; callers must invoke this on the drain goroutine.
func (sm *SyncManager) pumpBlockRequests(peer *peerpkg.Peer, state *peerSyncState, isCheckpointBlock bool, blockHash chainhash.Hash) {
	if isCheckpointBlock {
		return
	}

	dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()

	// Snapshot startHeader under headerMu; fetchHeaderBlocks takes headerMu
	// itself, so call it only after releasing the lock.
	sm.headerMu.Lock()
	hasStartHeader := sm.startHeader != nil
	sm.headerMu.Unlock()

	if hasStartHeader && state.requestedBlocks.Len() < dynamicMax {
		sm.topUpBlockFetch()
	} else if !sm.current() && state.requestedBlocks.Len() == 0 {
		sm.logger.Debugf("[pumpBlockRequests][%s] no in-flight blocks, requesting more from peer", blockHash)

		latestBlockHeader, _, getBestErr := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
		if getBestErr != nil {
			sm.logger.Errorf("[pumpBlockRequests] Failed to get best block header: %v", getBestErr)
		} else {
			locator := blockchain.BlockLocator([]*chainhash.Hash{latestBlockHeader.Hash()})
			if pushErr := peer.PushGetBlocksMsg(locator, &zeroHash); pushErr != nil {
				sm.logger.Errorf("[pumpBlockRequests] Failed to send getblocks message to peer %s: %v", peer.String(), pushErr)
			}
		}
	}
}

// runPostBlockProcessing handles the peer-state and header-pipeline updates that
// must happen after a block is successfully processed. Called by handleBlockMsgWithWindow
// on the direct (non-windowed) path.
func (sm *SyncManager) runPostBlockProcessing(peer *peerpkg.Peer, state *peerSyncState, bmsg *blockQueueMsg, isCheckpointBlock bool) {
	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	heightUpdate := bmsg.blockHeight
	blkHashUpdate := &bmsg.blockHash

	if heightUpdate <= 0 {
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &bmsg.blockHash)
		if err != nil {
			sm.logger.Errorf("Failed to get block header for block %v: %v", bmsg.blockHash, err)
		} else {
			blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf("failed to convert block height to int32: %v", err)
			}

			heightUpdate = blockHeightInt32
		}
	}

	sm.logger.Infof("accepted block %v at height %d", bmsg.blockHash, heightUpdate)
	sm.rejectedTxns.Clear()

	if heightUpdate != 0 {
		peer.UpdateLastBlockHeight(heightUpdate)
		sm.logger.Debugf("peer %s reports new best height %d, current %v", peer.String(), peer.LastBlock(), sm.current())

		if sm.current() {
			go sm.peerNotifier.UpdatePeerHeights(blkHashUpdate, heightUpdate, peer)

			if err := sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/handleBlockMsg"); err != nil {
				sm.logger.Errorf("[Sync Manager] failed to send FSM RUN event %v", err)
			}

			sm.resetFeeFilterToDefault()
		}
	}

	if !isCheckpointBlock {
		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()

		// Snapshot startHeader under headerMu; fetchHeaderBlocks takes headerMu
		// itself, so call it only after releasing the lock.
		sm.headerMu.Lock()
		hasStartHeader := sm.startHeader != nil
		sm.headerMu.Unlock()

		if hasStartHeader && state.requestedBlocks.Len() < dynamicMax {
			sm.topUpBlockFetch()
		} else if !sm.current() && state.requestedBlocks.Len() == 0 {
			sm.logger.Debugf("Not current, and no headers to sync to, fetching more headers")

			latestBlockHeader, _, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
			if err != nil {
				sm.logger.Errorf("Failed to get best block header: %v", err)
				return
			}

			locator := blockchain.BlockLocator([]*chainhash.Hash{latestBlockHeader.Hash()})
			if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
				sm.logger.Errorf("Failed to send getblocks message to peer %s: %v", peer.String(), err)
			}
		}

		return
	}

	// Advance the block-level checkpoint tracker now that this checkpoint's
	// block has committed. The next interval's headers are NOT requested here
	// anymore — handleHeadersMsg already pipelined them ahead when it reached
	// this checkpoint's headers, so requesting again would be a redundant,
	// duplicate getheaders. We only advance nextCheckpoint (so the next
	// checkpoint block is recognised and kept in the header list) and, at the
	// final checkpoint, switch to normal mode.
	sm.headerMu.Lock()
	prevHeight := sm.nextCheckpoint.Height
	sm.nextCheckpoint = sm.findNextHeaderCheckpoint(prevHeight)

	if sm.nextCheckpoint != nil {
		sm.headerMu.Unlock()
		return
	}

	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.headerListSeed = nil
	sm.headerMu.Unlock()

	sm.logger.Infof("Reached the final checkpoint -- switching to normal mode")

	locator := blockchain.BlockLocator([]*chainhash.Hash{&bmsg.blockHash})
	if err := peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
		sm.logger.Errorf("Failed to send getblocks message to peer %s: %v", peer.String(), err)
	}
}

// topUpBlockFetch tops up the in-flight block-fetch window. When the
// multi-peer flag is enabled (ParallelFetchPeers > 1) it routes to the
// disjoint multi-peer scheduler (assignBlocksAcrossPeers), distributing new
// blocks across all eligible peers rather than the sync peer alone. When the
// flag is off (<=1) it calls fetchHeaderBlocks directly — byte-identical to the
// pre-feature single-peer behaviour.
//
// Drain-goroutine only: both callees touch drain-goroutine-only state
// (headerList, startHeader, requestedBlocks per peer). Call this only from the
// blockHandler drain goroutine (handleBlockMsg, runPostBlockProcessing,
// pumpBlockRequests).
func (sm *SyncManager) topUpBlockFetch() {
	if sm.settings != nil && sm.settings.Legacy.ParallelFetchPeers > 1 {
		sm.assignBlocksAcrossPeers()
		return
	}
	sm.fetchHeaderBlocks()
}

// fetchHeaderBlocks creates and sends a request to the syncPeer for the next
// list of blocks to be downloaded based on the current list of headers.
//
// It walks the header list from startHeader across a per-node gRPC
// (haveInventory) and a final peer send (QueueMessage), neither of which may
// hold headerMu. To stay race-free without holding the lock across I/O it uses
// the generation counter: it snapshots headerGen under the lock, does the I/O
// with the lock released, then re-takes the lock and aborts the walk if
// headerGen changed (i.e. resetHeaderState wiped the list from the outer loop
// while we were unlocked). All *list.Element traversal happens under the lock.
func (sm *SyncManager) fetchHeaderBlocks() {
	// Nothing to do if there is no sync peer.
	sp := sm.loadSyncPeer()
	if sp == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no sync peer")
		return
	}

	// Snapshot the generation and the starting element under the lock. A change
	// in headerGen at any later re-take means the list was reset out from under
	// us and the walk must stop.
	sm.headerMu.Lock()
	gen := sm.headerGen
	e := sm.startHeader
	headerListLen := sm.headerList.Len()
	sm.headerMu.Unlock()

	// Nothing to do if there is no start header.
	if e == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no start header")
		return
	}

	// Calculate how many blocks to request to reach the dynamic max limit.
	// The limit adjusts based on observed block sizes (20 for small, down to 1 for >2GB).
	peerState, exists := sm.peerStates.Get(sp)
	if !exists {
		sm.logger.Warnf("[fetchHeaderBlocks] sync peer state not found")
		return
	}

	currentInFlight := peerState.requestedBlocks.Len()
	dynamicMaxInFlight := sm.blockSizeTracker.calculateMaxInFlightBlocks()
	maxBlocks := dynamicMaxInFlight - currentInFlight
	if maxBlocks <= 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] Already at max in-flight blocks (%d/%d), not requesting more", currentInFlight, dynamicMaxInFlight)
		return
	}

	avgBlockSize := sm.blockSizeTracker.getAverageSize()
	sm.logger.Debugf("[fetchHeaderBlocks] Header list: %d blocks, in-flight: %d/%d, avg size: %d bytes, requesting: %d more",
		headerListLen, currentInFlight, dynamicMaxInFlight, avgBlockSize, maxBlocks)

	// Build up a getdata request for the list of blocks the headers
	// describe. Size the InvList to maxBlocks rather than headerList.Len()
	// because the loop below breaks at maxBlocks — sizing to headerList.Len()
	// (often 2000) caused large repeated allocations (~16 KB) when only a
	// handful of slots ever get used (maxBlocks shrinks to 1 for >2 GB blocks).
	getDataMessage := wire.NewMsgGetDataSizeHint(uint(maxBlocks)) // nolint:gosec
	numRequested := 0

	// Collect the hashes added to this getdata batch. They are recorded into the
	// in-flight ledgers ONLY AFTER a successful non-blocking send below; a
	// dropped getdata must not leave phantom in-flight entries the window never
	// re-tops.
	pendingHashes := make([]*chainhash.Hash, 0, maxBlocks)

	for e != nil {
		// Read the current node's hash and the next element under the lock,
		// re-checking the generation first. All *list.Element pointer traversal
		// (e.Value, e.Next()) must happen here — resetHeaderState's list.Init()
		// mutates those pointers concurrently.
		sm.headerMu.Lock()
		if sm.headerGen != gen {
			sm.headerMu.Unlock()
			sm.logger.Debugf("[fetchHeaderBlocks] header state reset mid-walk, aborting")
			break
		}

		node, ok := e.Value.(*headerNode)
		next := e.Next()
		sm.headerMu.Unlock()

		if !ok {
			sm.logger.Warnf("Header list node type is not a headerNode")
			e = next
			continue
		}

		// Runway cap (see fetchRunwayHorizon): pause the walk at the parkable horizon
		// so the fetch cursor cannot outrun block assembly. Break before advancing
		// startHeader so the walk resumes here once the horizon slides up.
		if horizon, capped := sm.fetchRunwayHorizon(); capped && node.height >= 0 && uint32(node.height) > horizon {
			sm.logger.Debugf("[fetchHeaderBlocks] runway horizon %d reached at height %d; pausing walk", horizon, node.height)
			break
		}

		iv := wire.NewInvVect(wire.InvTypeBlock, node.hash)

		// haveInventory issues a gRPC — must run with headerMu released.
		haveInv, err := sm.haveInventory(iv)
		if err != nil {
			sm.logger.Warnf("Unexpected failure when checking for "+
				"existing inventory during header block "+
				"fetch: %v", err)
		}

		if !haveInv {
			if err = getDataMessage.AddInvVect(iv); err != nil {
				sm.logger.Warnf("Unexpected failure when adding inventory to getdata message: %v", err)
				break
			}

			pendingHashes = append(pendingHashes, node.hash)

			numRequested++
		}

		// Re-take the lock to advance startHeader, aborting if the list was
		// reset while we were doing the gRPC above.
		sm.headerMu.Lock()
		if sm.headerGen != gen {
			sm.headerMu.Unlock()
			sm.logger.Debugf("[fetchHeaderBlocks] header state reset mid-walk, aborting")
			break
		}
		sm.startHeader = next
		sm.headerMu.Unlock()

		e = next

		if numRequested >= maxBlocks {
			sm.logger.Debugf("[fetchHeaderBlocks] Limiting to %d block(s) from %s", numRequested, sp)
			break
		}
	}

	if len(getDataMessage.InvList) > 0 {
		// Non-blocking send: a write-stalled peer must never wedge the drain
		// goroutine (the refill tick calls this every InFlightRefillInterval).
		if !sp.TryQueueMessage(getDataMessage) {
			sm.logger.Debugf("[fetchHeaderBlocks] outputQueue full for peer %s, deferring getdata to next tick", sp.String())
			// A dropped getdata must leave no phantom in-flight entry, but the
			// startHeader cursor has ALREADY advanced past these hashes (above), so
			// the monotonic forward walk can never re-reach them. Re-queue them for
			// the next pass's re-fetch drain instead of silently discarding them —
			// mirrors assignBlocksAcrossPeers' dropped-send handling. Without this a
			// single dropped frontier getdata orphans the committed-tip+1 block and
			// wedges the whole strictly-ascending download (the mainnet IBD wedge).
			sm.assignedMu.Lock()
			if sm.refetchBlocks == nil {
				sm.refetchBlocks = make(map[chainhash.Hash]struct{})
			}
			for _, h := range pendingHashes {
				sm.refetchBlocks[*h] = struct{}{}
			}
			sm.assignedMu.Unlock()
			return
		}

		// Record the in-flight entries only after the getdata actually went out,
		// so a dropped batch leaves no phantom entries the window never re-tops.
		// Also record the assignment (peer + time) in the stall-detector ledgers,
		// keeping this single-peer path SYMMETRIC with assignBlocksAcrossPeers: both
		// reconcileLostAssignments and checkHeadStall scan assignedTo, so without
		// this a block requested here whose requestedBlocks TTL (60s) lapses before
		// it arrives is tracked in NO ledger — a ledgerless frontier orphan that
		// pins the tip and wedges IBD. Recording it here lets the timeout scans
		// recover it. Arrival cleanup (handleBlockPreamble) already deletes all
		// three ledgers unconditionally, so this cannot leak.
		assignAt := time.Now()
		sm.assignedMu.Lock()
		if sm.assignedTo == nil {
			sm.assignedTo = make(map[chainhash.Hash]*peerpkg.Peer)
		}
		if sm.assignedAt == nil {
			sm.assignedAt = make(map[chainhash.Hash]time.Time)
		}
		for _, h := range pendingHashes {
			sm.requestedBlocks.Set(*h, struct{}{})
			peerState.requestedBlocks.Set(*h, struct{}{})
			sm.assignedTo[*h] = sp
			sm.assignedAt[*h] = assignAt
			// A block that actually went out is no longer awaiting re-fetch.
			delete(sm.refetchBlocks, *h)
		}
		sm.assignedMu.Unlock()
	}
}

// maintainInFlightWindow tops the in-flight block-fetch window back up to the
// memory-aware cap (calculateMaxInFlightBlocks), independently of block
// processing. It is the continuous-refill driver: the refill ticker in
// blockHandler fires it on the SAME single drain goroutine that reads
// blockQueue/headersQueue, so it never runs concurrently with block or header
// processing on that goroutine. It touches ONLY the fetch/request side —
// fetchHeaderBlocks — and never wa.add / ProcessBlockWindow / the accumulator,
// so the K-block coinbase-maturity gate is untouched.
//
// Without it the fetch window is only topped up as a side effect of processing a
// block (pumpBlockRequests / runPostBlockProcessing). A slot frees the instant a
// block arrives — requestedBlocks is deleted in handleBlockPreamble before the
// block is processed — then sits empty until that block finishes processing,
// collapsing a nominally deep window into depth-1 request/response ping-pong and
// starving the peer feed (the RX=0 gap). Firing this on a short ticker keeps the
// window continuously at the cap.
//
// GETDATA TOP-UP ONLY — no getheaders re-arm. handleHeadersMsg already keeps
// exactly one getheaders outstanding at all times: the checkpoint-boundary
// pipeline send (PushGetHeadersMsg toward headerCheckpoint.Hash) and the
// non-checkpoint continuation send (PushGetHeadersMsg toward the same
// headerCheckpoint.Hash) — see the two sends in handleHeadersMsg's
// receivedCheckpoint and trailing branches below. A proactive re-arm from here
// would re-send an identical locator toward the same headerCheckpoint.Hash; the
// peer answers both, and the second, overlapping response fails the header
// linkage check in handleHeadersMsg ("Received block header that does not
// properly connect to the chain") and triggers DisconnectWithWarning — a self-
// inflicted disconnect loop in precisely the RX=0 window this change exists to
// fix. So when the header runway is exhausted (startHeader == nil) this returns
// silently and relies on the already-outstanding getheaders (recovery of a lost
// one falls to the 30s watchdog rotation). This omission is locked in by
// TestRunwayExhaustion_NoDuplicateGetheaders.
//
// MUST be called on the drain goroutine only.
func (sm *SyncManager) maintainInFlightWindow() {
	if !sm.headersFirstMode.Load() {
		// Continuous refill is a headers-first IBD concern only. Cheap atomic
		// load + return so the tick costs nothing once IBD completes.
		return
	}

	sp := sm.loadSyncPeer()
	if sp == nil {
		sm.logger.Debugf("[maintainInFlightWindow] no sync peer, skipping refill")
		return
	}

	// Head-of-line stall check (Task 2.4) runs BEFORE the runway guard: a stalled
	// head peer is most dangerous exactly when the window is full and the runway is
	// exhausted (nothing new to assign, ordered commit paused on the missing head).
	// It is an internal no-op unless ParallelFetchPeers > 1. Any disconnect it does
	// frees that peer's assignments for reassignment on this same or the next tick.
	sm.checkHeadStall(time.Now())

	// checkHeadStall only recovers the HEAD orphan. A NON-head block can also be
	// stranded: the global requestedBlocks ledger has a 60s TTL, but assignedTo
	// has none, so if a re-fetched block's requestedBlocks entry expires while its
	// peer stays connected yet never delivers it, the block is left in assignedTo,
	// below the cursor, outstanding to nobody — the same orphan class, via a
	// different trigger. Reconcile catches these each tick and re-enqueues them.
	sm.reconcileLostAssignments(time.Now())

	// reconcileLostAssignments + checkHeadStall both key on assignedTo. The
	// frontier can be orphaned OUTSIDE assignedTo entirely: the single-peer
	// fetchHeaderBlocks checkpoint-boundary send records only in requestedBlocks,
	// so a dropped/expired getdata there strands the frontier in NO ledger below
	// the monotonic cursor — the headers-first wedge where the committed tip
	// freezes while headers keep flowing and only a silent-peer rotation
	// accidentally re-requests it. Recover it here, keyed on the committed-tip
	// frontier; assignBlocksAcrossPeers below drains it (lowest-height-first) on
	// this same tick.
	sm.reconcileFrontierGap(time.Now())

	// Snapshot whether there is header runway under headerMu, then RELEASE the
	// lock before calling fetchHeaderBlocks (which re-takes headerMu itself and
	// must never be called with it held — the lock is non-reentrant).
	sm.headerMu.Lock()
	hasRunway := sm.startHeader != nil
	sm.headerMu.Unlock()

	if !hasRunway {
		// Runway exhausted: a getheaders is already outstanding from
		// handleHeadersMsg. Do NOT re-arm — that self-disconnects. Return
		// silently and let the existing request repopulate the runway.
		sm.logger.Debugf("[maintainInFlightWindow] no header runway, deferring to outstanding getheaders")
		return
	}

	// Multi-peer disjoint scheduler (Task 2.3): when ParallelFetchPeers > 1,
	// distribute the header-runway walk across up to ParallelFetchPeers eligible
	// peers (the sync peer plus others), assigning each walked block to exactly
	// ONE peer. This supersedes the single-peer fetchHeaderBlocks top-up (the sync
	// peer is one of the N fetch peers). assignBlocksAcrossPeers falls back to
	// fetchHeaderBlocks internally when fewer than 2 targets are eligible.
	//
	// When ParallelFetchPeers <= 1 (flag-off) the path is byte-identical to the
	// pre-feature single-peer behaviour: no eligibleFetchPeers call, no new alloc,
	// exactly the fetchHeaderBlocks top-up.
	if sm.settings.Legacy.ParallelFetchPeers > 1 {
		sm.assignBlocksAcrossPeers()
		return
	}

	peerState, exists := sm.peerStates.Get(sp)
	if !exists {
		sm.logger.Debugf("[maintainInFlightWindow] sync peer state not found, skipping refill")
		return
	}

	// Only top up when below the dynamic cap. fetchHeaderBlocks requests exactly
	// cap-currentInFlight, skips already-requested hashes and never rewinds
	// startHeader, so calling it more often can neither exceed the cap nor
	// double-request. The non-blocking getdata send (Task 2) means a write-stalled
	// peer cannot wedge this drain goroutine — a dropped top-up self-heals on the
	// next tick.
	if peerState.requestedBlocks.Len() < sm.blockSizeTracker.calculateMaxInFlightBlocks() {
		sm.fetchHeaderBlocks()
	}
}

// fetchTarget is one peer participating in a multi-peer disjoint assignment
// pass. spare is the peer's remaining per-peer capacity (K minus its current
// in-flight); it decrements as blocks are assigned. batch accumulates the
// getdata InvList and pending accumulates the same hashes for post-send
// recording into THIS peer's requestedBlocks.
type fetchTarget struct {
	peer    *peerpkg.Peer
	state   *peerSyncState
	spare   int
	batch   *wire.MsgGetData
	pending []*chainhash.Hash
}

// assignBlocksAcrossPeers is the multi-peer disjoint-range block scheduler
// (Task 2.3). It generalizes fetchHeaderBlocks: instead of walking the header
// runway and requesting every un-have block from the single sync peer, it
// distributes that same walk across up to ParallelFetchPeers eligible peers
// (the sync peer plus eligibleFetchPeers), assigning each walked block to
// EXACTLY ONE peer.
//
// It preserves fetchHeaderBlocks' headerGen protocol exactly: snapshot headerGen
// + startHeader under headerMu; per node re-check headerGen and read
// hash/next under the lock, then RELEASE the lock across haveInventory (gRPC);
// re-take, re-check, advance startHeader once per assigned block; abort the walk
// on any headerGen change. headerMu is NEVER held across haveInventory or
// TryQueueMessage. Drain-goroutine only.
//
// Invariants:
//   - DISJOINT: startHeader is the shared cursor, advanced exactly once per
//     assigned block regardless of which peer got it, so no hash goes to two peers.
//   - Per-peer bounded by K (MaxBlocksInTransitPerPeer): a peer's running spare.
//   - Total bounded by Budget = min(BlockDownloadWindow, dynamic byte cap) minus
//     the current total in-flight across ALL peers — NOT per-peer×N.
//   - Record each assigned hash into its ASSIGNED peer's requestedBlocks only
//     AFTER that peer's getdata send succeeds (phantom-free, like
//     fetchHeaderBlocks). A dropped send leaves no in-flight entry.
//
// Falls back to the single-peer fetchHeaderBlocks when fewer than 2 targets are
// assignable, so the byte-identical single-peer path is preserved whenever
// parallelism cannot actually be exercised.
func (sm *SyncManager) assignBlocksAcrossPeers() {
	sp := sm.loadSyncPeer()
	if sp == nil {
		sm.logger.Warnf("[assignBlocksAcrossPeers] called with no sync peer")
		return
	}

	syncState, exists := sm.peerStates.Get(sp)
	if !exists {
		sm.logger.Warnf("[assignBlocksAcrossPeers] sync peer state not found")
		return
	}

	// Build the fetch-peer set: the sync peer PLUS up to (ParallelFetchPeers-1)
	// other eligible peers. The sync peer is one of the N fetch peers now.
	k := sm.settings.Legacy.MaxBlocksInTransitPerPeer

	targets := make([]*fetchTarget, 0, sm.settings.Legacy.ParallelFetchPeers)
	appendTarget := func(p *peerpkg.Peer, st *peerSyncState) {
		spare := k - st.requestedBlocks.Len()
		if spare <= 0 {
			// A peer at/over its per-peer cap contributes no spare this pass, but
			// still counts toward total in-flight (accounted separately below).
			return
		}
		targets = append(targets, &fetchTarget{peer: p, state: st, spare: spare})
	}

	appendTarget(sp, syncState)
	for _, p := range sm.eligibleFetchPeers(sp, sm.settings.Legacy.ParallelFetchPeers-1) {
		st, ok := sm.peerStates.Get(p)
		if !ok {
			continue
		}
		appendTarget(p, st)
	}

	// Fall back to the single-peer path when parallelism cannot be exercised
	// (fewer than 2 peers with spare capacity). This keeps behaviour identical to
	// the pre-multi-peer top-up when only one peer is usable.
	if len(targets) < 2 {
		if syncState.requestedBlocks.Len() < sm.blockSizeTracker.calculateMaxInFlightBlocks() {
			sm.fetchHeaderBlocks()
		}
		return
	}

	// Total in-flight = sum over ALL peerStates of requestedBlocks.Len() (every
	// peer, not just the fetch targets — a non-target peer's outstanding blocks
	// still consume the shared window budget).
	totalInFlight := 0
	for _, st := range sm.peerStates.Range() {
		totalInFlight += st.requestedBlocks.Len()
	}

	// Budget = min(BlockDownloadWindow, dynamic byte cap). toAssign is how many
	// more blocks the shared window can absorb this pass.
	budget := sm.settings.Legacy.BlockDownloadWindow
	if dynamicCap := sm.blockSizeTracker.calculateMaxInFlightBlocks(); dynamicCap < budget {
		budget = dynamicCap
	}

	toAssign := budget - totalInFlight
	if toAssign <= 0 {
		sm.logger.Debugf("[assignBlocksAcrossPeers] window full (in-flight %d >= budget %d), nothing to assign", totalInFlight, budget)
		return
	}

	// Re-fetch orphaned blocks FIRST. Blocks freed by the head-of-line stall
	// detector, or dropped on a full send queue, sit BELOW the monotonic cursor,
	// so the forward walk below can never re-reach them. They are also the lowest
	// (commit-blocking) blocks, so they get first claim on this pass's budget.
	toAssign = sm.drainRefetchBlocks(targets, toAssign)

	// Snapshot headerGen + startHeader under the lock — identical protocol to
	// fetchHeaderBlocks. A headerGen change at any later re-take means the list was
	// reset out from under us and the walk must stop.
	sm.headerMu.Lock()
	gen := sm.headerGen
	e := sm.startHeader
	sm.headerMu.Unlock()

	if e == nil {
		// No forward runway. Do NOT return: any re-fetch batches queued above still
		// need to be sent by the send loop below.
		sm.logger.Debugf("[assignBlocksAcrossPeers] no forward runway; sending re-fetch batches only")
	}

	assigned := 0

	for e != nil && toAssign > 0 {
		// Re-check generation and read the current node + next under the lock. All
		// *list.Element traversal happens here.
		sm.headerMu.Lock()
		if sm.headerGen != gen {
			sm.headerMu.Unlock()
			sm.logger.Debugf("[assignBlocksAcrossPeers] header state reset mid-walk, aborting")
			break
		}

		node, ok := e.Value.(*headerNode)
		next := e.Next()
		sm.headerMu.Unlock()

		if !ok {
			sm.logger.Warnf("[assignBlocksAcrossPeers] header list node type is not a headerNode")
			e = next
			continue
		}

		// Runway cap: stop the walk once the frontier reaches the parkable horizon
		// so the fetch cursor cannot climb unboundedly ahead of block assembly and
		// saturate the park. Break BEFORE advancing startHeader so the cursor stays
		// on this node and the walk resumes here as the horizon slides up.
		if horizon, capped := sm.fetchRunwayHorizon(); capped && node.height >= 0 && uint32(node.height) > horizon {
			sm.logger.Debugf("[assignBlocksAcrossPeers] runway horizon %d reached at height %d; pausing walk (assigned %d)", horizon, node.height, assigned)
			break
		}

		iv := wire.NewInvVect(wire.InvTypeBlock, node.hash)

		// haveInventory issues a gRPC — must run with headerMu released.
		haveInv, err := sm.haveInventory(iv)
		if err != nil {
			sm.logger.Warnf("[assignBlocksAcrossPeers] unexpected failure checking inventory during header block fetch: %v", err)
		}

		// Skip a block that is already IN FLIGHT (requested and outstanding). This is
		// the source fix for the duplicate-admission → create-vs-create deadlock:
		// resetHeaderState rewinds this cursor on every sync-peer rotation but does NOT
		// clear the in-flight ledger, so without this guard the re-walk re-requests
		// still-outstanding blocks to a SECOND peer — both copies then arrive and one
		// is admitted to the window twice, colliding on the txs unique index (40P01).
		// haveInventory only covers COMMITTED blocks; sm.requestedBlocks is the
		// in-flight set the refetch drain already trusts (drainRefetchBlocks). Safe on
		// this MULTI-PEER walk only: a genuinely lost in-flight block is recovered by
		// checkHeadStall / reconcileLostAssignments → refetchBlocks, so skipping it here
		// cannot strand it. The single-peer fetchHeaderBlocks walk is deliberately NOT
		// changed — it has no such recovery and relies on the re-walk to re-request.
		_, inFlight := sm.requestedBlocks.Get(*node.hash)

		// Also skip a block the window pipeline OWNS (parked or in an in-flight
		// flush job). requestedBlocks cannot cover this: the preamble wipes it when
		// the block ARRIVES, but the block then lives on in the park for minutes,
		// invisible to haveInventory (uncommitted). Re-buying it here is what
		// manufactured the parked twins behind the mainnet duplicate-commit storm
		// (same recovery reasoning as the in-flight skip above: a genuinely lost
		// owned block has its ownership released on every job/park exit, so
		// skipping it here cannot strand it).
		if !haveInv && !inFlight && !sm.windowBlockOwned(*node.hash) {
			// Pick the assignable target with the MOST spare capacity. If none has
			// spare left, the per-peer caps are exhausted for this pass; stop.
			best := pickMaxSpareTarget(targets)
			if best == nil {
				sm.logger.Debugf("[assignBlocksAcrossPeers] all peers at per-peer cap, stopping (assigned %d)", assigned)
				break
			}

			if err = best.batchAdd(iv, node.hash); err != nil {
				sm.logger.Warnf("[assignBlocksAcrossPeers] failed to add inv to getdata for peer %s: %v", best.peer, err)
				break
			}

			best.spare--
			toAssign--
			assigned++
		}

		// Advance the shared cursor once per WALKED node (matching
		// fetchHeaderBlocks, which advances past have-inventory nodes too so it
		// never re-walks them). Re-take the lock, abort on reset.
		sm.headerMu.Lock()
		if sm.headerGen != gen {
			sm.headerMu.Unlock()
			sm.logger.Debugf("[assignBlocksAcrossPeers] header state reset mid-walk, aborting")
			break
		}
		sm.startHeader = next
		sm.headerMu.Unlock()

		e = next
	}

	// Send each target's batch and record its hashes only on a successful send.
	for _, tgt := range targets {
		if tgt.batch == nil || len(tgt.batch.InvList) == 0 {
			continue
		}

		if !tgt.peer.TryQueueMessage(tgt.batch) {
			sm.logger.Debugf("[assignBlocksAcrossPeers] outputQueue full for peer %s, deferring %d block(s) to next tick", tgt.peer.String(), len(tgt.batch.InvList))
			// Not recorded: a dropped getdata must leave no phantom in-flight entry.
			// The cursor has already advanced past these blocks, so the forward walk
			// can never re-reach them — re-queue them for the next pass's re-fetch
			// drain instead of relying on a re-walk that cannot happen.
			sm.assignedMu.Lock()
			if sm.refetchBlocks == nil {
				sm.refetchBlocks = make(map[chainhash.Hash]struct{})
			}
			for _, h := range tgt.pending {
				sm.refetchBlocks[*h] = struct{}{}
			}
			sm.assignedMu.Unlock()
			continue
		}

		// Record into THIS peer's requestedBlocks (and the global ledger) only after
		// the getdata actually went out, so the auth gate at handleBlockPreamble
		// accepts delivery from this specific peer. Track the assignment (peer +
		// time) for the head-of-line stall detector (Task 2.4); these maps stay in
		// one-to-one correspondence with the global requestedBlocks ledger.
		assignAt := time.Now()
		sm.assignedMu.Lock()
		// Lazy-init: production sets these in New, but the maps may be nil in a
		// minimal test-constructed SyncManager. A nil-map WRITE panics (delete /
		// range on nil are safe), so guard the only write site.
		if sm.assignedTo == nil {
			sm.assignedTo = make(map[chainhash.Hash]*peerpkg.Peer)
		}
		if sm.assignedAt == nil {
			sm.assignedAt = make(map[chainhash.Hash]time.Time)
		}
		for _, h := range tgt.pending {
			sm.requestedBlocks.Set(*h, struct{}{})
			tgt.state.requestedBlocks.Set(*h, struct{}{})
			sm.assignedTo[*h] = tgt.peer
			sm.assignedAt[*h] = assignAt
			// A re-fetch that actually went out is no longer orphaned; drop it from
			// the set. delete on a nil map is a safe no-op.
			delete(sm.refetchBlocks, *h)
		}
		sm.assignedMu.Unlock()

		sm.logger.Debugf("[assignBlocksAcrossPeers] assigned %d block(s) to peer %s", len(tgt.pending), tgt.peer.String())
	}
}

// drainRefetchBlocks re-requests orphaned blocks (freed by the head-of-line
// stall detector or dropped on a full send queue) BEFORE the forward walk.
// Those blocks sit below the monotonic startHeader cursor, which is never
// rewound, so the forward walk can never re-reach them; and because the window
// commits strictly ascending, a single such orphan pins the committed tip and
// wedges the whole download. Draining them first also gives the lowest,
// commit-blocking blocks first claim on the pass's budget.
//
// For each still-orphaned hash it batchAdds an inv to the eligible target with
// the most spare capacity, decrementing toAssign and the target's spare, and
// drops from the set any block we already have. Recording into requestedBlocks
// (and removal from the set) happens in the shared send loop of
// assignBlocksAcrossPeers, so an orphan stays queued until its getdata actually
// goes out — a dropped send simply retries next pass. Returns the remaining
// toAssign budget. Drain-goroutine only; assignedMu is never held across the
// haveInventory gRPC.
func (sm *SyncManager) drainRefetchBlocks(targets []*fetchTarget, toAssign int) int {
	sm.assignedMu.Lock()
	if len(sm.refetchBlocks) == 0 {
		sm.assignedMu.Unlock()
		return toAssign
	}

	pending := make([]chainhash.Hash, 0, len(sm.refetchBlocks))
	for h := range sm.refetchBlocks {
		pending = append(pending, h)
	}
	sm.assignedMu.Unlock()

	// Re-fetch the LOWEST heights first. The contiguous committed-tip+1 block (and
	// the rest of the in-gate range) is the only work that advances block assembly
	// and drains the park; it must win the pass's small budget over any far-ahead
	// orphans. Go map-range order is random, so under parallelFetchPeers>1 tip+1
	// would otherwise lose the race to higher-height hashes and the tip would stall.
	// Height comes from headerHeightIndex (guarded by headerMu); an unknown height
	// (hash not currently in the header index) sorts last.
	heights := make(map[chainhash.Hash]int32, len(pending))

	sm.headerMu.Lock()
	for _, h := range pending {
		if ht, ok := sm.headerHeightIndex[h]; ok {
			heights[h] = ht
		} else {
			heights[h] = math.MaxInt32
		}
	}
	sm.headerMu.Unlock()

	sort.Slice(pending, func(i, j int) bool {
		return heights[pending[i]] < heights[pending[j]]
	})

	// While the park is count-full, fetchRunwayHorizon clamps to the maturity gate.
	// Skip re-sending any orphan KNOWN to be beyond that horizon: it would only earn
	// another park-full reject and re-queue, churning the pass budget and starving the
	// in-gate tip+1. Skipping (continue) — NOT breaking — preserves toAssign for the
	// in-gate work and leaves the far-ahead orphan queued in refetchBlocks for a later
	// pass (re-sent the instant releaseParkedBlocks frees a park slot or it falls within
	// the sliding gate — no strand). Unknown-height orphans (sorted last, MaxInt32) are
	// NEVER skipped, so an in-gate block whose header-index entry was consumed stays
	// eligible. capped is false off-path (parking off / above checkpoint / cache
	// unpolled / park not full) → no skip, byte-identical to today.
	horizon, capped := sm.fetchRunwayHorizon()

	for _, h := range pending {
		if toAssign <= 0 {
			break
		}

		if capped {
			if ht := heights[h]; ht >= 0 && ht != math.MaxInt32 && uint32(ht) > horizon { //nolint:gosec // ht guarded >= 0
				continue
			}
		}

		// Already re-assigned and recorded on a prior pass — skip (no double-request).
		if _, inFlight := sm.requestedBlocks.Get(h); inFlight {
			continue
		}

		hh := h // stable address for the pointer stored in the target's pending list
		iv := wire.NewInvVect(wire.InvTypeBlock, &hh)

		haveInv, err := sm.haveInventory(iv)
		if err != nil {
			sm.logger.Warnf("[drainRefetchBlocks] inventory check failed for %s: %v", hh, err)
		}

		if haveInv {
			// We already have it after all; it is no longer an orphan.
			sm.assignedMu.Lock()
			delete(sm.refetchBlocks, hh)
			sm.assignedMu.Unlock()

			continue
		}

		best := pickMaxSpareTarget(targets)
		if best == nil {
			// No spare capacity this pass; leave the rest queued for next tick.
			break
		}

		if err = best.batchAdd(iv, &hh); err != nil {
			sm.logger.Warnf("[drainRefetchBlocks] failed to add inv to getdata for peer %s: %v", best.peer, err)
			break
		}

		best.spare--
		toAssign--
	}

	return toAssign
}

// pickMaxSpareTarget returns the fetchTarget with the greatest remaining spare
// capacity (>0), or nil if none has spare left. Ties are broken by slice order
// (the sync peer is first), which keeps assignment deterministic given a fixed
// target set.
func pickMaxSpareTarget(targets []*fetchTarget) *fetchTarget {
	var best *fetchTarget
	for _, t := range targets {
		if t.spare <= 0 {
			continue
		}
		if best == nil || t.spare > best.spare {
			best = t
		}
	}
	return best
}

// fetchRunwayHorizon returns the highest block height the headers-first fetch
// scheduler may request this pass, and whether that cap is active.
//
// When park-ahead is live the fetch frontier MUST be bounded to
// cachedBA + maxBehind + parkCap. The park buffer, by construction, only ever holds
// beyond-gate blocks (height > cachedBA + maxBehind), so bounding the frontier to
// parkCap above the gate means at most parkCap beyond-gate blocks can ever be in the
// park at once — it can never saturate. Without this cap the monotonic fetch cursor
// climbs unboundedly ahead of a lagging block assembly (an ~10k-height span was
// observed on mainnet against a 20-block gate and 1024-slot park), overflowing the
// park; each overflow reject is requeued into refetchBlocks and re-requested, a
// positive-feedback churn that starves the one contiguous tip+1 block that would
// advance block assembly — a permanent livelock under parallelFetchPeers>1.
//
// The cap is gated exactly like the non-blocking maturity gate
// (blockAssemblyGateAdmitsCached): active only when parking is live, maxBehind and
// parkCap are positive, the BA cache has been polled (cached>0), and we are in the
// below-checkpoint prefix where parking actually happens. When inactive it returns
// (0,false) and the forward walk runs uncapped — byte-identical to the pre-park path.
// Drain-goroutine only (reads cachedBlockAssemblyHeight atomically).
func (sm *SyncManager) fetchRunwayHorizon() (uint32, bool) {
	if !sm.parkAheadActive.Load() {
		return 0, false
	}

	maxBehind := sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly
	parkCap := sm.settings.Legacy.ParallelWindowMaxParkedBlocks

	if maxBehind <= 0 || parkCap <= 0 || sm.chainParams == nil {
		return 0, false
	}

	cached := sm.cachedBlockAssemblyHeight.Load()
	if !sm.baHeightPolled.Load() {
		// BA cache not yet polled; capping now would wrongly clamp the frontier to
		// ~parkCap. Leave the walk uncapped until the poller reports. (A REAL
		// height of 0 — fresh node — must cap normally, hence the flag, not
		// cached == 0.)
		return 0, false
	}

	if !model.BelowCheckpoint(sm.chainParams.Checkpoints, cached) {
		// Above the checkpoint the gate uses the blocking slow path and never parks,
		// so there is no park to saturate — no cap needed.
		return 0, false
	}

	span := uint32(maxBehind) + uint32(parkCap) //nolint:gosec // both guarded > 0 above

	park := sm.parkRef.Load()

	if sm.settings.Legacy.ParkRunwayByteSized {
		// Byte-sized runway: instead of the binary full→gate / else→full-static-cap
		// clamp below, size the beyond-gate runway N from the park's REMAINING byte
		// room at the current average block size, so the horizon retreats SMOOTHLY as
		// the park fills and springs back as releaseParkedBlocks frees bytes. This
		// removes the fat-block span flap (full→gate→full) that pulls un-parkable
		// far-ahead blocks into the refetch churn and starves the in-gate tip+1.
		// N = clamp(remainingBytes/avgSize, 1, parkCap):
		//   - floor 1 keeps the horizon strictly above the maturity gate, so the
		//     frontier is always requestable (strictly better than the old binary
		//     collapse to exactly the gate, which could push a stale-cache frontier
		//     one height above the horizon).
		//   - the park byte budget (park.budget) and blockSizeTracker average are the
		//     SAME signals the park itself and calculateWindowK already use.
		// Fall back to the full static cap (byte-identical to the flag-off path) when
		// there is no park, the byte budget is disabled (ParallelWindowParkedMemoryFraction=0,
		// budget<=0 — count-only mode), or no size samples exist yet (avg<=0, cold start).
		if park != nil && park.budget > 0 {
			if avg := sm.blockSizeTracker.getAverageSize(); avg > 0 {
				remaining := park.budget - park.bytesAccum

				var n int64
				switch {
				case remaining < avg:
					n = 1
				case remaining/avg > int64(parkCap):
					n = int64(parkCap)
				default:
					n = remaining / avg
				}

				span = uint32(maxBehind) + uint32(n) //nolint:gosec // maxBehind>0, 1<=n<=parkCap
			}
		}
	} else if park != nil && park.atCapacity() {
		// Legacy binary park-full backpressure: while the park cannot accept another
		// beyond-gate block, drop the parkCap runway and clamp the frontier to the
		// maturity gate, so neither the forward walk nor the refetch drain pulls
		// un-parkable far-ahead blocks (which would only be park-rejected and requeued,
		// churning the budget and starving the in-gate tip+1). Binary: full → gate,
		// otherwise → the full static cap. The drain goroutine is the sole reader and
		// writer of park, so atCapacity is race-free here. Self-clearing: releaseParkedBlocks
		// freeing a slot flips atCapacity→false and the runway springs back next tick.
		span = uint32(maxBehind) //nolint:gosec // guarded > 0 above
	}

	if cached > math.MaxUint32-span {
		return 0, false
	}

	return cached + span, true
}

// batchAdd lazily allocates the target's getdata message and appends one inv,
// tracking the hash in pending for post-send recording.
func (t *fetchTarget) batchAdd(iv *wire.InvVect, hash *chainhash.Hash) error {
	if t.batch == nil {
		// Size the hint to the peer's spare capacity — the most it can be assigned.
		t.batch = wire.NewMsgGetDataSizeHint(uint(t.spare)) //nolint:gosec
		t.pending = make([]*chainhash.Hash, 0, t.spare)
	}

	if err := t.batch.AddInvVect(iv); err != nil {
		return err
	}

	t.pending = append(t.pending, hash)

	return nil
}

// eligibleFetchPeers returns up to max peers from peerStates that are sync
// candidates, connected, and not the excluded sync peer.  The returned slice
// order is map-iteration order (non-deterministic) — the caller uses them in
// order and stops at max.
func (sm *SyncManager) eligibleFetchPeers(exclude *peerpkg.Peer, max int) []*peerpkg.Peer {
	if max <= 0 {
		return nil
	}

	result := make([]*peerpkg.Peer, 0, max)

	for p, state := range sm.peerStates.Range() {
		if p == exclude {
			continue
		}

		if !state.syncCandidate {
			continue
		}

		if !p.Connected() {
			continue
		}

		result = append(result, p)

		if len(result) >= max {
			break
		}
	}

	return result
}

// checkHeadStall is the head-of-line stalling-timeout (Task 2.4, the Bitcoin
// Core BLOCK_STALLING_TIMEOUT model). Under disjoint multi-peer download blocks
// commit in ascending height order, so a stalled peer holding the lowest-height
// (next-needed) outstanding block pauses the ordered commit even while higher
// blocks pile up behind it. This finds that HEAD block, and if its assigned peer
// has held it longer than BlockStallTimeout, disconnects ONLY that peer and
// frees ALL of its assignments so the next assignBlocksAcrossPeers tick
// reassigns them to other peers.
//
// It is a no-op unless multi-peer download is enabled (ParallelFetchPeers > 1),
// so the single-peer path is byte-identical to before the feature.
//
// now is injected so tests control the clock. Called on the drain goroutine
// (from maintainInFlightWindow, before assignBlocksAcrossPeers); the tracking
// maps are guarded by assignedMu because handleDonePeerMsg mutates them from the
// other blockHandler goroutine.
//
// No block is lost by freeing: the head's hashes are removed from the assigned
// peer's requestedBlocks and the global ledger, so haveInventory still reports
// them missing and they are re-requested next tick; a late block arriving from
// the disconnected peer is a harmless GetBlockExists no-op.
// reconcileLostAssignments re-enqueues blocks whose in-flight assignment must be
// abandoned, for either of two reasons:
//
//  1. LOST: the global requestedBlocks ledger is an expiringmap with a 60s TTL
//     and no eviction callback, while assignedTo/assignedAt have no TTL. A block
//     re-fetched to a peer that stays connected but never delivers it will have
//     its requestedBlocks entry expire, leaving it stranded in assignedTo —
//     outstanding to nobody, below the monotonic cursor.
//  2. STALE: a block outstanding to its assigned peer longer than
//     BlockInFlightTimeout. checkHeadStall only rescues the single HEAD block;
//     non-head blocks assigned to peers that accept the getdata but never
//     deliver would otherwise pin the shared in-flight budget until their 60s
//     TTL, so toAssign goes <= 0, the head re-fetch cannot drain, and the whole
//     download freezes for up to a minute. Freeing stale blocks bounds that
//     freeze to BlockInFlightTimeout. This is the Bitcoin Core BLOCK_DOWNLOAD_
//     TIMEOUT analog; unlike checkHeadStall it does NOT disconnect the peer, so
//     a merely-slow peer is not churned — only the individual stuck block moves.
//
// Freed blocks re-enter the re-fetch set and are re-requested next tick. Runs
// each scheduler tick (a cheap in-memory scan bounded by the in-flight cap).
// Drain-goroutine only; assignedMu is a leaf lock held here only across in-memory
// map operations — never a gRPC or a send. It reads/deletes requestedBlocks (its
// own internal lock) while holding assignedMu, which is safe because
// requestedBlocks has no eviction callback, so nothing takes assignedMu from
// under the requestedBlocks lock (no inversion).
func (sm *SyncManager) reconcileLostAssignments(now time.Time) {
	if sm.settings.Legacy.ParallelFetchPeers <= 1 {
		return
	}

	timeout := sm.settings.Legacy.BlockInFlightTimeout

	sm.assignedMu.Lock()
	defer sm.assignedMu.Unlock()

	for h, p := range sm.assignedTo {
		_, tracked := sm.requestedBlocks.Get(h)
		stale := timeout > 0 && now.Sub(sm.assignedAt[h]) > timeout

		if tracked && !stale {
			continue
		}

		// Abandon this assignment and re-enqueue the block. delete during range is
		// safe in Go.
		if sm.refetchBlocks == nil {
			sm.refetchBlocks = make(map[chainhash.Hash]struct{})
		}
		sm.refetchBlocks[h] = struct{}{}
		delete(sm.assignedTo, h)
		delete(sm.assignedAt, h)

		// Drop the ledger entries so the block is immediately re-requestable and
		// the peer's spare capacity frees (no-op if the entry already expired).
		sm.requestedBlocks.Delete(h)
		if st, ok := sm.peerStates.Get(p); ok {
			st.requestedBlocks.Delete(h)
		}
	}
}

// reconcileFrontierGap re-requests the frontier block (committed_tip+1) when it
// has become orphaned in NO ledger. reconcileLostAssignments and checkHeadStall
// both key on assignedTo, but the single-peer fetchHeaderBlocks path — still
// fired at every checkpoint boundary in handleHeadersMsg even under
// ParallelFetchPeers>1 — records a getdata ONLY in requestedBlocks (60s TTL) and,
// on a dropped send, enqueues nothing to refetchBlocks. A frontier lost there is
// tracked in no ledger, sits below the monotonic startHeader cursor, and is
// re-requestable today only by the accidental cursor rewind a silent-peer
// rotation performs every ~3 min — the observed headers-first wedge (tip frozen
// while headers keep flowing and checkpoints keep verifying). This closes that
// split-brain by checking the frontier directly, keyed on the committed-tip
// frontier rather than assignedTo, so recovery is independent of which sub-path
// lost the block. Drain-goroutine only (same caller as reconcileLostAssignments).
func (sm *SyncManager) reconcileFrontierGap(now time.Time) {
	if sm.settings.Legacy.ParallelFetchPeers <= 1 {
		// Single-peer machinery is unchanged; its top-up owns liveness there.
		return
	}

	// Frontier = the lowest header whose block has not been consumed yet:
	// headerList.Front(), skipping a stale leading seed (a retained checkpoint /
	// committed-tip node that has no block of its own to fetch). O(1) — no scan.
	sm.headerMu.Lock()
	var (
		frontier     chainhash.Hash
		haveFrontier bool
	)

	for e := sm.headerList.Front(); e != nil; e = e.Next() {
		if e == sm.headerListSeed {
			continue
		}

		if node, ok := e.Value.(*headerNode); ok && node.hash != nil {
			frontier = *node.hash
			haveFrontier = true
		}

		break
	}
	sm.headerMu.Unlock()

	if !haveFrontier {
		sm.frontierGapHash = chainhash.Hash{}
		return
	}

	// Outstanding to a peer, already in hand (parked/committing), or already
	// queued for re-fetch? Then it is not orphaned: clear the debounce, leave it.
	notOrphaned := false
	if _, inFlight := sm.requestedBlocks.Get(frontier); inFlight {
		notOrphaned = true
	} else if sm.windowBlockOwned(frontier) {
		notOrphaned = true
	}

	if !notOrphaned {
		sm.assignedMu.Lock()
		if _, assigned := sm.assignedTo[frontier]; assigned {
			notOrphaned = true
		} else if _, queued := sm.refetchBlocks[frontier]; queued {
			notOrphaned = true
		}
		sm.assignedMu.Unlock()
	}

	if notOrphaned {
		sm.frontierGapHash = chainhash.Hash{}
		return
	}

	// Frontier is orphaned right now. Debounce: it may just be mid-flight and
	// briefly untracked (its 60s global requestedBlocks entry lapsed while a slow
	// peer is still delivering). Only re-request once the SAME frontier has stayed
	// orphaned longer than BlockInFlightTimeout, so an in-flight block that arrives
	// within that window is never touched, yet a true orphan recovers in seconds
	// instead of the old multi-minute silent-peer rotation.
	if sm.frontierGapHash != frontier {
		sm.frontierGapHash = frontier
		sm.frontierGapSince = now

		return
	}

	timeout := sm.settings.Legacy.BlockInFlightTimeout
	if timeout > 0 && now.Sub(sm.frontierGapSince) < timeout {
		return
	}

	orphanedFor := now.Sub(sm.frontierGapSince)

	// Persisted orphan: outstanding to nobody for > BlockInFlightTimeout. Re-enqueue
	// once and re-arm the debounce so we do not churn on every subsequent tick.
	// drainRefetchBlocks re-checks haveInventory before sending, so a benign false
	// positive self-corrects with no redundant getdata.
	sm.assignedMu.Lock()
	if sm.refetchBlocks == nil {
		sm.refetchBlocks = make(map[chainhash.Hash]struct{})
	}

	sm.refetchBlocks[frontier] = struct{}{}
	sm.assignedMu.Unlock()

	sm.frontierGapSince = now

	sm.logger.Warnf("[reconcileFrontierGap] frontier block %s orphaned for %s (lost on the single-peer fetch path); re-enqueuing for re-fetch", frontier, orphanedFor)
}

func (sm *SyncManager) checkHeadStall(now time.Time) {
	if sm.settings.Legacy.ParallelFetchPeers <= 1 {
		return
	}

	timeout := sm.settings.Legacy.BlockStallTimeout
	if timeout <= 0 {
		return
	}

	// Find the HEAD = lowest-height outstanding block, and read its assignment,
	// all under assignedMu (a cheap in-memory scan; no I/O). headerHeightIndex is
	// guarded by headerMu, so take that too for the height lookups. Lock order is
	// headerMu -> assignedMu; both are leaf locks released before any peer send.
	//
	// This scan runs BEFORE the backpressure suppression below so we can tell
	// whether the outstanding head is the exact block the committer is blocked
	// on (see frontierStalled). Under park-ahead, localReadBackpressured() is
	// almost always true during IBD, so a blanket suppression here lets a silent
	// frontier peer age all the way to the coarse 60s-TTL / rotation recovery
	// (measured: 53-170s pipeline-idle stalls). Carving the frontier out of the
	// suppression restores the ~2s fast peer-swap for exactly that case.
	sm.headerMu.Lock()
	sm.assignedMu.Lock()

	var (
		headHash   chainhash.Hash
		headPeer   *peerpkg.Peer
		headAt     time.Time
		headHeight int32 = -1
		haveHead   bool
	)

	for h, p := range sm.assignedTo {
		height, ok := sm.headerHeightIndex[h]
		if !ok {
			// No authoritative height for this hash (e.g. its header-list node was
			// already consumed). It cannot be the head we order commits on; skip it.
			continue
		}

		if !haveHead || height < headHeight {
			haveHead = true
			headHeight = height
			headHash = h
			headPeer = p
			headAt = sm.assignedAt[h]
		}
	}

	sm.assignedMu.Unlock()
	sm.headerMu.Unlock()

	if !haveHead || headPeer == nil {
		return
	}

	// frontierStalled is true when the lowest outstanding (assigned-but-not-yet-
	// delivered) block is exactly the one block the committer needs next:
	// releaseParkedBlocks admits only height == cachedBlockAssemblyHeight+1, so
	// when the head equals that, every higher parked block is held hostage and
	// the whole commit pipeline is genuinely idle — the opposite of local
	// read-backpressure. In that case we must NOT suppress: the silent frontier
	// peer is the real fault and needs the fast swap.
	cached := sm.cachedBlockAssemblyHeight.Load()
	frontierStalled := sm.baHeightPolled.Load() && int64(headHeight) == int64(cached)+1

	if !frontierStalled {
		// Never execute a peer for OUR OWN backpressure: while the node throttles
		// its reads because local validation is behind, the head block ages without
		// any peer fault. Suppress, stamp, and require a full clean timeout window
		// AFTER suppression ends before firing — measured live, unconditional 2s
		// fires during self-backpressure seeded ~a third of the rotation-cascade
		// storms. A genuinely hung pipeline un-suppresses itself via the
		// stale-backlog escape in localReadBackpressured, so this cannot mask a
		// real stall indefinitely.
		if sm.localReadBackpressured() {
			sm.headStallSuppressedAt = now
			return
		}

		if !sm.headStallSuppressedAt.IsZero() && now.Sub(sm.headStallSuppressedAt) <= timeout {
			return
		}
	}

	if now.Sub(headAt) <= timeout {
		// The next-needed block's peer is still within its grace period. Do NOT
		// touch it — disconnecting an honest, not-yet-timed-out peer is forbidden.
		return
	}

	sm.logger.Warnf("[checkHeadStall] head block %s (height %d) stalled %s past %s timeout on peer %s; disconnecting and freeing its assignments",
		headHash, headHeight, now.Sub(headAt), timeout, headPeer)

	// Disconnect ONLY the single head peer (never mass-disconnect).
	headPeer.DisconnectWithWarning("head-of-line block-stalling timeout: next-needed block not delivered in time")

	// Free that peer's assignments so they are re-eligible next tick. This mirrors
	// what handleDonePeerMsg will also do once the disconnect propagates, but doing
	// it here makes the reassignment immediate rather than waiting for the
	// done-peer message.
	sm.freePeerAssignments(headPeer)
}

// freePeerAssignments removes every block currently assigned to peer from the
// stall-detector tracking maps AND from the global requestedBlocks ledger, so
// those blocks are re-eligible for assignment to another peer on the next
// scheduler tick (haveInventory will still report them missing). It is safe to
// call from either blockHandler goroutine (it takes assignedMu) and is a cheap
// scan over the small in-flight set. It never disconnects — callers decide that.
func (sm *SyncManager) freePeerAssignments(peer *peerpkg.Peer) {
	if peer == nil {
		return
	}

	sm.assignedMu.Lock()

	freed := make([]chainhash.Hash, 0)
	for h, p := range sm.assignedTo {
		if p == peer {
			freed = append(freed, h)
		}
	}

	for _, h := range freed {
		delete(sm.assignedTo, h)
		delete(sm.assignedAt, h)
		// Enqueue for re-fetch. The shared cursor has already advanced past these
		// blocks, so the forward walk can never re-reach them; without this they
		// would be orphaned below the cursor and wedge the ascending commit.
		if sm.refetchBlocks == nil {
			sm.refetchBlocks = make(map[chainhash.Hash]struct{})
		}
		sm.refetchBlocks[h] = struct{}{}
	}

	sm.assignedMu.Unlock()

	// Drop the global ledger entries outside assignedMu (the ledger is
	// independently locked) so the freed blocks are immediately re-requestable.
	// Also clear them from the peer's OWN requestedBlocks map when it is still in
	// peerStates: on the stall path the peer is disconnected but its done-peer
	// message (which would clearRequestedState) has not arrived yet, so without
	// this the stale per-peer entries would linger until then.
	peerState, hasState := sm.peerStates.Get(peer)
	for _, h := range freed {
		sm.requestedBlocks.Delete(h)
		if hasState {
			peerState.requestedBlocks.Delete(h)
		}
	}

	if len(freed) > 0 {
		sm.logger.Debugf("[freePeerAssignments] freed %d block assignment(s) from peer %s for reassignment", len(freed), peer)
	}
}

// handleHeadersMsg handles block header messages from all peers.  Headers are
// requested when performing a headers-first sync.
func (sm *SyncManager) handleHeadersMsg(hmsg *headersMsg) {
	sm.logger.Debugf("[handleHeadersMsg] received headers message with %d headers from %s", len(hmsg.headers.Headers), hmsg.peer)
	peer := hmsg.peer

	_, resolved, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		sm.logger.Warnf("Received headers message from unknown peer %s", peer)
		return
	}
	if resolved != peer {
		// Stream peers (e.g. BlockPriority DATA1) are not registered in
		// peerStates directly - resolved via their association's primary peer.
		sm.logger.Debugf("[handleHeadersMsg] resolved stream peer %s to primary peer %s", peer, resolved)
		peer = resolved
	}

	// The remote peer is misbehaving if we didn't request headers.
	msg := hmsg.headers
	numHeaders := len(msg.Headers)

	if !sm.headersFirstMode.Load() {
		reason := fmt.Sprintf("Got %d unrequested headers from %s", numHeaders, peer.String())
		peer.DisconnectWithWarning(reason)

		return
	}

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// ensure we have a valid starting point for header validation
	sm.headerMu.Lock()
	headerListEmpty := sm.headerList.Back() == nil
	sm.headerMu.Unlock()

	if headerListEmpty {
		sm.logger.Warnf("Header list is empty, attempting to recover sync state")

		bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
		if err != nil {
			peer.DisconnectWithWarning(fmt.Sprintf("Failed to get best block header: %v", err))
			return
		}

		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			peer.DisconnectWithWarning(fmt.Sprintf("Failed to convert block height: %v", err))
			return
		}

		// resetHeaderState takes headerMu itself; call it with the lock released.
		sm.resetHeaderState(bestBlockHeader.Hash(), bestBlockHeightInt32)

		sm.headerMu.Lock()
		stillEmpty := sm.headerList.Back() == nil
		sm.headerMu.Unlock()

		if stillEmpty {
			peer.DisconnectWithWarning("Failed to initialize header sync state")
			return
		}
	}

	// C3: past the final checkpoint headerCheckpoint is nil (cleared in the
	// receivedCheckpoint branch below when findNextHeaderCheckpoint returns
	// nil), but headersFirstMode is only cleared later by the block handler.
	// A headers message arriving in that window must not deref a nil cursor
	// (node.height == sm.headerCheckpoint.Height and the trailing getheaders
	// both would panic): there is nothing left to verify or request, so mirror
	// the final-checkpoint path and stop here without requesting more headers.
	sm.headerMu.Lock()
	headerCheckpointNil := sm.headerCheckpoint == nil
	sm.headerMu.Unlock()

	if headerCheckpointNil {
		sm.logger.Debugf("[handleHeadersMsg] ignoring headers past the final checkpoint from %s", peer)
		return
	}

	// Header-list nodes are removed ONLY on block-fetch progress, never on a
	// header-download event. A checkpoint interval on mainnet spans many
	// getheaders messages (MaxBlockHeadersPerMsg = 2000; an interval is 30-50k
	// blocks), and intervals overlap block fetching. So Back()-at-message-start
	// is NOT necessarily the stale DB-best seed — on any non-first message of an
	// interval it is a real header ~2000 blocks below the checkpoint tip whose
	// block has not been requested yet (block fetch is bounded to
	// calculateMaxInFlightBlocks, 1-20). Removing it here would splice out a node
	// whose block is still pending, so fetchHeaderBlocks' Next() walk would skip
	// it and its block would never be fetched — a header-list gap that wedges the
	// sync. The DB-best seed is instead dropped exactly once in fetchHeaderBlocks
	// (headerListDummy), and every real node is removed from the front by
	// handleBlockPreamble only after its block is processed.

	// Process all the received headers ensuring each one connects to the
	// previous and that checkpoints match.
	receivedCheckpoint := false

	var finalHash *chainhash.Hash

	for _, blockHeader := range msg.Headers {
		blockHash := blockHeader.BlockHash()
		finalHash = &blockHash

		node := headerNode{hash: &blockHash}

		// Hold headerMu for the list read/append and the checkpoint comparison.
		// DisconnectWithWarning is a peer send, so capture the outcome under the
		// lock and disconnect/return AFTER releasing.
		var (
			disconnectReason string
			shouldDisconnect bool
			checkpointHit    bool
		)

		sm.headerMu.Lock()

		prevNodeEl := sm.headerList.Back()
		if prevNodeEl == nil {
			sm.headerMu.Unlock()
			peer.DisconnectWithWarning("Header list does not contain a previous element as expected")

			return
		}

		// Ensure the header properly connects to the previous one and
		// add it to the list of headers.
		prevNode := prevNodeEl.Value.(*headerNode)
		if prevNode.hash.IsEqual(&blockHeader.PrevBlock) {
			node.height = prevNode.height + 1
			e := sm.headerList.PushBack(&node)
			sm.headerHeightIndex[*node.hash] = node.height

			if sm.startHeader == nil {
				sm.startHeader = e
			}
		} else {
			// Post-reset grace: right after resetHeaderState a successor sync
			// peer's first in-flight batch legitimately fails to connect to the
			// rebuilt list's seed. Chain-killing those peers extended the
			// measured rotation cascades (19 kills in one window). Within the
			// grace window drop the batch quietly; the peer re-serves from the
			// fresh locator on the next getheaders.
			recentReset := !sm.lastHeaderResetAt.IsZero() && time.Since(sm.lastHeaderResetAt) < 60*time.Second
			sm.headerMu.Unlock()

			if recentReset {
				sm.logger.Debugf("dropping non-connecting header batch from %s within the post-reset grace window", peer)
				return
			}

			peer.DisconnectWithWarning("Received block header that does not properly connect to the chain")

			return
		}

		// Verify the header at the header-request checkpoint height matches.
		// headerCheckpoint (not nextCheckpoint) is the checkpoint this batch
		// heads toward: after a boundary it runs ahead of the block-level
		// nextCheckpoint. Every checkpoint-height header is still fully
		// verified here and a mismatch still disconnects — advancing the
		// cursor never skips a checkpoint's verification.
		if node.height == sm.headerCheckpoint.Height {
			if node.hash.IsEqual(sm.headerCheckpoint.Hash) {
				checkpointHit = true
			} else {
				shouldDisconnect = true
				disconnectReason = fmt.Sprintf("Block header at height %d/hash "+
					"%s does NOT match expected checkpoint hash of %s",
					node.height, node.hash,
					sm.headerCheckpoint.Hash)
			}
		}
		sm.headerMu.Unlock()

		if shouldDisconnect {
			peer.DisconnectWithWarning(disconnectReason)
			return
		}

		if checkpointHit {
			receivedCheckpoint = true

			sm.logger.Infof("Verified downloaded block "+
				"header against checkpoint at height "+
				"%d/hash %s", node.height, node.hash)

			break
		}
	}

	// When this header is a checkpoint, switch to fetching the blocks for
	// all the headers since the last checkpoint AND, to keep the header list
	// ahead of block fetching, immediately request the NEXT interval's headers
	// so they download concurrently. This eliminates the checkpoint-boundary
	// stall: previously the next-interval getheaders was deferred until the
	// checkpoint BLOCK was processed, so block fetching idled at each boundary
	// while headers-first had nothing to fetch.
	if receivedCheckpoint {
		// No list surgery here: node removal is tied to block-fetch progress, not
		// to this header-download event (see the note above where the anchor
		// capture used to be). fetchHeaderBlocks drops the DB-best seed once and
		// requests the pending blocks; the block-commit front-removal in
		// handleBlockPreamble consumes the list from the oldest end as blocks are
		// processed. This is safe across the multi-message continuous model
		// because it never removes a node whose block is still pending.
		sm.headerMu.Lock()
		listLen := sm.headerList.Len()
		sm.headerMu.Unlock()

		sm.logger.Infof("Received %v block headers: Fetching blocks", listLen)
		// fetchHeaderBlocks takes headerMu itself; call it with the lock released.
		sm.fetchHeaderBlocks()

		// Advance the header-request cursor to the next checkpoint and request
		// that interval's headers now. The block-level nextCheckpoint is left
		// untouched (the block handler still advances it as the checkpoint
		// block commits) — only the header look-ahead runs ahead. When there is
		// no further checkpoint we simply stop requesting headers here; the
		// switch to normal/getblocks mode still happens in the block handler
		// once the final checkpoint block is processed (findNextHeaderCheckpoint
		// returns nil there), so we never request headers past the final
		// checkpoint.
		reachedHash := finalHash

		sm.headerMu.Lock()
		sm.headerCheckpoint = sm.findNextHeaderCheckpoint(sm.headerCheckpoint.Height)
		var nextCheckpointHash *chainhash.Hash
		var nextCheckpointHeight int32
		if sm.headerCheckpoint != nil {
			nextCheckpointHash = sm.headerCheckpoint.Hash
			nextCheckpointHeight = sm.headerCheckpoint.Height
		}
		sm.headerMu.Unlock()

		if nextCheckpointHash != nil {
			locator := blockchain.BlockLocator([]*chainhash.Hash{reachedHash})

			if err := peer.PushGetHeadersMsg(locator, nextCheckpointHash); err != nil {
				sm.logger.Warnf("Failed to send pipelined getheaders message to peer %s: %v", peer.String(), err)
			} else {
				sm.logger.Infof("handleHeadersMsg - Pipelining headers ahead to checkpoint height %d from peer %s",
					nextCheckpointHeight, peer.String())
			}
		}

		return
	}

	// This header is not a checkpoint, so request the next batch of
	// headers starting from the latest known header and ending with the
	// header-request checkpoint.
	sm.headerMu.Lock()
	headerCheckpointHash := sm.headerCheckpoint.Hash
	sm.headerMu.Unlock()

	locator := blockchain.BlockLocator([]*chainhash.Hash{finalHash})

	if err := peer.PushGetHeadersMsg(locator, headerCheckpointHash); err != nil {
		sm.logger.Warnf("Failed to send getheaders message to peer %s: %v", peer.String(), err)
	}
}

// haveInventory returns whether the inventory represented by the passed
// inventory vector is known.  This includes checking all the various places
// inventory can be when it is in different states such as blocks that are part
// of the main chain, on a side chain, in the orphan pool, and transactions that
// are in the memory pool (either the main pool or orphan pool).
func (sm *SyncManager) haveInventory(invVect *wire.InvVect) (bool, error) {
	switch invVect.Type {
	case wire.InvTypeBlock:
		// single round-trip: GetBlockHeader tells us both existence and validity
		_, meta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &invVect.Hash)
		if err != nil {
			// block not found (or transient error) — trigger re-request
			return false, nil
		}

		// block exists but was marked invalid — re-request so it can be reprocessed
		return !meta.Invalid, nil

	case wire.InvTypeTx:
		// check whether this transaction exists in the utxo store
		// which means it has been processed completely at our end
		utxo, err := sm.utxoStore.Get(sm.ctx, &invVect.Hash, fields.Fee)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				return false, nil
			}

			return false, err
		}

		return utxo != nil, nil
	}

	// The requested inventory is is an unsupported type, so just claim
	// it is known to avoid requesting it.
	return true, nil
}

// handleInvMsg handles inv messages from all peers.
// We examine the inventory advertised by the remote peer and act accordingly.
func (sm *SyncManager) handleInvMsg(imsg *invMsg) {
	sm.logger.Debugf("[handleInvMsg] received inv message with %d inv vectors from %s", len(imsg.inv.InvList), imsg.peer)
	peer := imsg.peer

	state, resolved, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		sm.logger.Warnf("[handleInvMsg] Received inv message from unknown peer %s", peer)
		return
	}
	if resolved != peer {
		// Stream peers (e.g. BlockPriority DATA1) are not registered in
		// peerStates directly - resolved via their association's primary peer.
		sm.logger.Debugf("[handleInvMsg] resolved stream peer %s to primary peer %s", peer, resolved)
		peer = resolved
	}

	// Attempt to find the final block in the inventory list.  There may
	// not be one.
	lastBlock := -1
	invVects := imsg.inv.InvList

	for i := len(invVects) - 1; i >= 0; i-- {
		if invVects[i].Type == wire.InvTypeBlock {
			lastBlock = i
			break
		}
	}

	// If this inv contains a block announcement, and this isn't coming from
	// our current sync peer, then update the last
	// announced block for this peer. We'll use this information later to
	// update the heights of peers based on blocks we've accepted that they
	// previously announced.
	sp := sm.loadSyncPeer()
	if lastBlock != -1 && peer != sp {
		peer.UpdateLastAnnouncedBlock(&invVects[lastBlock].Hash)
	}

	// Ignore invs from peers that aren't the sync if we are not current.
	// Helps prevent fetching a mass of orphans.
	if peer != sp && !sm.current() {
		return
	}

	// If a peer announces a block we already
	// know of, then update their current block height.
	if lastBlock != -1 {
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &invVects[lastBlock].Hash)
		if err == nil {
			blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf("failed to convert block height to int32: %v", err)
			}

			peer.UpdateLastBlockHeight(blockHeightInt32)
		}
	}

	// by default, we do not process transactions / blocks
	// only when we are in the running state we process transaction and new block messages
	processInvs := false

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		sm.logger.Errorf("[handleInvMsg] Failed to get current FSM state: %v", err)
	} else if fsmState != nil && *fsmState == teranodeblockchain.FSMStateRUNNING {
		processInvs = true
	}

	wg := sync.WaitGroup{}

	// Request the advertised inventory if we don't already have it.  Also,
	// request parent blocks of orphans if we receive one we already have.
	// Finally, attempt to detect potential stalls due to long side chains
	// we already have and request more blocks to prevent them.
	for i, iv := range invVects {
		if iv.Type == wire.InvTypeBlock {
			// process blocks in serial
			sm.processInvMsg(i, iv, processInvs, peer, exists, state, lastBlock)
			continue
		}

		// process all remaining inv vectors in parallel
		wg.Add(1)

		go func(i int, iv *wire.InvVect) {
			defer wg.Done()

			// Ignore unsupported inventory types.
			sm.processInvMsg(i, iv, processInvs, peer, exists, state, lastBlock)
		}(i, iv)
	}

	// wait for all inv vectors to be processed
	wg.Wait()

	// Request as much as possible at once.  Anything that won't fit into
	// the request will be requested on the next inv message.
	numRequested := 0
	gdmsg := wire.NewMsgGetData()

outside:
	for state.requestQueue.Length() != 0 {
		// shift the first items from the request queue until we have enough to send in a single message
		iv, found := state.requestQueue.Shift()
		if !found {
			break
		}

		switch iv.Type {
		case wire.InvTypeBlock:
			// Request the block if there is not already a pending request.
			if _, exists = sm.requestedBlocks.Get(iv.Hash); !exists {
				if err = gdmsg.AddInvVect(iv); err != nil {
					sm.logger.Warnf("Unexpected failure when adding inventory to getdata message: %v", err)
					break outside
				}

				sm.requestedBlocks.Set(iv.Hash, struct{}{})
				state.requestedBlocks.Set(iv.Hash, struct{}{})

				numRequested++
			}

		case wire.InvTypeTx:
			// Request the transaction if there is not already a pending request.
			if _, exists = sm.requestedTxns.Get(iv.Hash); !exists {
				if err = gdmsg.AddInvVect(iv); err != nil {
					sm.logger.Warnf("Unexpected failure when adding inventory to getdata message: %v", err)
					break outside
				}

				sm.requestedTxns.Set(iv.Hash, struct{}{})
				state.requestedTxns.Set(iv.Hash, struct{}{})

				numRequested++
			}
		}

		if numRequested >= maxRequestedBlocks {
			sm.logger.Debugf("[handleInvMsg] Limiting to %d item(s) from %s", numRequested, peer)
			break
		}
	}

	if len(gdmsg.InvList) > 0 {
		sm.logger.Debugf("[handleInvMsg] Requesting %d items from %s", len(gdmsg.InvList), peer)
		peer.QueueMessage(gdmsg, nil)
	}
}

func (sm *SyncManager) processInvMsg(i int, iv *wire.InvVect, processInvs bool, peer *peerpkg.Peer, exists bool, state *peerSyncState, lastBlock int) {
	switch iv.Type {
	case wire.InvTypeBlock:
	case wire.InvTypeTx:
		if !processInvs {
			// If we are not in running state, we are not interested in new transaction or block messages
			sm.logger.Debugf("[handleInvMsg] Ignoring inv message from %s, not in running state", peer)
			return
		}
	default:
		return
	}

	// Add the inventory to the cache of known inventory
	// for the peer.
	peer.AddKnownInventory(iv)

	// Ignore inventory when we're in headers-first mode.
	if sm.headersFirstMode.Load() {
		return
	}

	// Request the inventory if we don't already have it.
	haveInv, err := sm.haveInventory(iv)
	if err != nil {
		sm.logger.Warnf("[handleInvMsg] Unexpected failure when checking for "+
			"existing inventory during inv message "+
			"processing: %v", err)

		return
	}

	if !haveInv {
		if iv.Type == wire.InvTypeTx {
			// Skip the transaction if it has already been rejected.
			if _, exists = sm.rejectedTxns.Get(iv.Hash); exists {
				return
			}
		}

		// Add it to the request queue.
		state.requestQueue.Append(iv)

		return
	}

	if iv.Type == wire.InvTypeBlock {
		// We already have the final block advertised by this inventory message, so force a request for more.  This
		// should only happen if we're on a really long side chain.
		if i == lastBlock {
			// Request blocks after this one up to the final one the remote peer knows about (zero stop hash).
			locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, &iv.Hash, 0)
			if err != nil {
				sm.logger.Errorf("[handleInvMsg] Failed to get block locator for the block hash %s, %v", iv.Hash.String(), err)
			} else {
				_ = peer.PushGetBlocksMsg(locator, &zeroHash)
			}
		}
	}
}

type blockQueueMsg struct {
	block       *wire.MsgBlock
	blockHash   chainhash.Hash
	blockHeight int32
	peer        *peerpkg.Peer
	reply       chan error
}

// windowEntry holds a prepared block ready for ProcessBlockWindow. The
// accumulator no longer owns the peer's reply channel: the drain goroutine
// sends every windowed block's ack at accept-time (see ackWindowedBlock), so
// flush never touches a reply.
type windowEntry struct {
	block *model.Block
}

// windowAccumulator is a drain-goroutine-local accumulator for the
// byte-budgeted window admission path. It is NOT safe for concurrent use;
// only the single blockQueue drain goroutine ever touches it.
type windowAccumulator struct {
	entries      []windowEntry
	bytesAccum   int64 // sum of block sizes already in the window
	windowBudget int64 // derived from GOMEMLIMIT × fraction at flush time
	maxBlocks    int   // upper bound on K (MaxBlocksBehindBlockAssembly)
}

// newWindowAccumulator returns an accumulator with the given budget and cap.
func newWindowAccumulator(windowBudget int64, maxBlocks int) *windowAccumulator {
	return &windowAccumulator{
		entries:      make([]windowEntry, 0, 8),
		windowBudget: windowBudget,
		maxBlocks:    maxBlocks,
	}
}

// add appends a prepared block to the window and tracks its byte size.
func (wa *windowAccumulator) add(block *model.Block) {
	wa.entries = append(wa.entries, windowEntry{block: block})
	wa.bytesAccum += int64(block.SizeInBytes) //nolint:gosec
}

// full reports whether the window has hit either its byte budget or its
// block-count cap. The block-count cap (maxBlocks, set from
// MaxBlocksBehindBlockAssembly) bounds how far the window can run ahead of
// block assembly regardless of block size: without it a stream of tiny blocks
// could grow the window to thousands of entries before the byte budget fills.
// A maxBlocks <= 0 disables the count cap (matching calculateWindowK), leaving
// the byte budget as the sole limit.
func (wa *windowAccumulator) full() bool {
	if len(wa.entries) == 0 {
		return false
	}

	if wa.bytesAccum >= wa.windowBudget {
		return true
	}

	return wa.maxBlocks > 0 && len(wa.entries) >= wa.maxBlocks
}

// empty reports whether there are no blocks in the window.
func (wa *windowAccumulator) empty() bool {
	return len(wa.entries) == 0
}

// parkStore holds prepared, early-acked blocks that are beyond the block-assembly
// maturity gate (more than MaxBlocksBehindBlockAssembly ahead of block assembly).
// They are released back into the window accumulator, ascending and contiguous
// with the committed tip, as block assembly advances (see releaseParkedBlocks,
// run on the continuous-refill tick).
//
// Drain-goroutine-local: only the single blockQueue drain goroutine ever touches
// it, so it needs no lock — same single-owner discipline as windowAccumulator.
//
// A parked block is already early-acked (its prefetch-budget reservation has been
// released) and its subtrees are already flushed to the blob store, so its heap
// cost is the same KB-scale header/coinbase/subtree-list as a block sitting in the
// window accumulator. The bound is therefore primarily the count cap (maxBlocks)
// with the GOMEMLIMIT-fraction byte budget as a secondary ceiling.
type parkStore struct {
	entries    []windowEntry
	bytesAccum int64
	budget     int64 // GOMEMLIMIT × ParallelWindowParkedMemoryFraction; 0 disables the byte cap
	maxBlocks  int   // hard count cap (ParallelWindowMaxParkedBlocks); primary bound
}

func newParkStore(budget int64, maxBlocks int) *parkStore {
	return &parkStore{entries: make([]windowEntry, 0, 16), budget: budget, maxBlocks: maxBlocks}
}

// countFull reports whether the count cap is hit (checkable before prepare so a
// full buffer never pays a wasted prepareBlockForWindow).
func (ps *parkStore) countFull() bool {
	return ps.maxBlocks > 0 && len(ps.entries) >= ps.maxBlocks
}

// full reports whether adding a block of `next` bytes would breach either cap.
func (ps *parkStore) full(next int64) bool {
	if ps.countFull() {
		return true
	}

	return ps.budget > 0 && ps.bytesAccum+next > ps.budget
}

// atCapacity reports whether the park can no longer accept a further beyond-gate
// block — the count cap is hit, OR adding another block of the current AVERAGE
// parked size would breach the byte budget. The fetch runway clamp
// (fetchRunwayHorizon) uses this rather than countFull() alone so a park that
// fills by BYTES (large blocks, e.g. the fat 2019 mainnet range) triggers the
// clamp just as a count-full park does; without the byte arm the clamp never
// engages under large blocks, the forward walk keeps pulling un-parkable
// far-ahead blocks, and IBD churns on "park buffer full (bytes)". The average is
// self-calibrating to the actual parked block sizes and avoids needing the next
// block's size (which the clamp does not have). Drain-goroutine only.
func (ps *parkStore) atCapacity() bool {
	if ps.countFull() {
		return true
	}

	if ps.budget <= 0 || len(ps.entries) == 0 {
		return false
	}

	avg := ps.bytesAccum / int64(len(ps.entries))

	return ps.bytesAccum+avg > ps.budget
}

func (ps *parkStore) add(b *model.Block) {
	ps.entries = append(ps.entries, windowEntry{block: b})
	ps.bytesAccum += int64(b.SizeInBytes) //nolint:gosec
}

func (ps *parkStore) len() int { return len(ps.entries) }

// claimWindowBlock records that the window pipeline owns this block (parked,
// accumulated, or in an in-flight flush job). Nil-map-safe: ownership is a
// window-path feature and many callers/tests construct a SyncManager without it.
func (sm *SyncManager) claimWindowBlock(hash chainhash.Hash, height uint32) {
	if sm.windowOwnedBlocks == nil {
		return
	}

	sm.windowOwnedBlocks.Set(hash, height)
}

// windowBlockOwned reports whether the window pipeline currently owns the block.
func (sm *SyncManager) windowBlockOwned(hash chainhash.Hash) bool {
	return sm.windowOwnedBlocks != nil && sm.windowOwnedBlocks.Exists(hash)
}

// releaseWindowBlock releases ownership of a single block.
func (sm *SyncManager) releaseWindowBlock(hash chainhash.Hash) {
	if sm.windowOwnedBlocks == nil {
		return
	}

	sm.windowOwnedBlocks.Delete(hash)
}

// gateContiguousWindow enforces the committed-chain contiguity invariant on a
// drained window job before it is handed to the committer. It runs on the
// drain goroutine (inside flushWindow/flushWindowSync), which owns park and
// lastHandedWindowEnd, so it needs no locking.
//
// Why: the direct window path admits gate-eligible blocks in ARRIVAL order.
// When peer churn loses a range before admission (mainnet 609471-609480),
// every window flushed afterwards starts above the hole; ProcessBlockWindow
// then fails "previous block not found", burns the serial bounded-recovery
// passes, and escalates to a sync-peer rotation — the only thing that
// re-fetched the hole. Result: multi-minute stalls between commit bursts.
// The park path already has the needed invariant (releaseParkedBlocks feeds
// only the contiguous run from the committed tip); this applies the same rule
// at flush time: hand the contiguous ascending run that continues
// lastHandedWindowEnd, and route post-gap strays into the park, where the
// release machinery feeds them back in order once the hole fills. The hole
// itself is re-fetched by the existing head-stall/reconcile machinery (the
// strays stay OWNED while parked, so only the truly missing range is re-bought).
//
// Seed/re-seed: tracker 0 (nothing handed yet) or a job starting at/below the
// tracker accepts the job's own start (idempotent re-commit after a fatal
// rotation re-syncs from the committed best-block) — the gate can therefore
// never wedge on a stale high-water mark. With park == nil (parking disabled)
// there is nowhere to hold strays, so the gate is a pass-through and the old
// recovery semantics apply unchanged.
func (sm *SyncManager) gateContiguousWindow(job windowFlushJob, park *parkStore) windowFlushJob {
	blocks := job.blocks
	if len(blocks) == 0 {
		return job
	}

	if park == nil {
		sm.lastHandedWindowEnd = blocks[len(blocks)-1].Height
		return job
	}

	// Wholly beyond a hole: hand nothing, park everything. The tracker alone is
	// NOT sufficient evidence of a hole — commits that bypass this gate (the
	// rotation recovery re-drive, direct/checkpoint commits) advance the chain
	// without advancing lastHandedWindowEnd, leaving it stale-LOW. A window
	// starting at or below cached+1 is contiguous with the COMMITTED chain
	// (cached is a stale-low-or-equal lower bound once polled), so it must be
	// handed regardless of the tracker; misjudging it parked a perfectly
	// contiguous window into a full park and livelocked the fresh mainnet sync
	// at 33333 (drop -> re-fetch -> same misjudgment).
	frontier := sm.lastHandedWindowEnd
	if sm.baHeightPolled.Load() {
		if cached := sm.cachedBlockAssemblyHeight.Load(); cached > frontier {
			frontier = cached
		}
	}

	if sm.lastHandedWindowEnd != 0 && blocks[0].Height > frontier+1 {
		sm.logger.Warnf("[gateContiguousWindow] window %d-%d starts beyond lost range after frontier %d (handed %d); parking %d blocks until the gap fills", blocks[0].Height, blocks[len(blocks)-1].Height, frontier, sm.lastHandedWindowEnd, len(blocks))

		for _, b := range blocks {
			sm.parkStrayWindowBlock(park, b)
		}

		job.blocks = nil

		return job
	}

	// Hand the ascending run up to the first internal hole; park the rest.
	// drainJob has already sorted ascending and deduped by height.
	end := 1
	for end < len(blocks) && blocks[end].Height == blocks[end-1].Height+1 {
		end++
	}

	if end < len(blocks) {
		sm.logger.Warnf("[gateContiguousWindow] window has internal gap after %d; handing %d blocks, parking %d until the gap fills", blocks[end-1].Height, end, len(blocks)-end)

		for _, b := range blocks[end:] {
			sm.parkStrayWindowBlock(park, b)
		}
	}

	job.blocks = blocks[:end]
	sm.lastHandedWindowEnd = blocks[end-1].Height

	return job
}

// parkStrayWindowBlock parks a post-gap stray for ordered re-release. If the
// park cannot take it (caps), the block is dropped and its ownership released
// so the refetch machinery can re-buy it — a full park must never wedge the
// gap-fill (the block was already early-acked, so dropping owes nothing).
func (sm *SyncManager) parkStrayWindowBlock(park *parkStore, b *model.Block) {
	if park.full(int64(b.SizeInBytes)) { //nolint:gosec
		sm.releaseWindowBlock(*b.Hash())
		sm.logger.Warnf("[gateContiguousWindow] park full; dropping stray block height %d for re-fetch", b.Height)

		return
	}

	park.add(b)
}

// deferredCheckpointBlock holds a checkpoint block's first delivery while its
// parent finishes committing (see the deferredCheckpoint field doc).
type deferredCheckpointBlock struct {
	msgBlock   *wire.MsgBlock
	bmsg       *blockQueueMsg // copy with reply nil: already acked at drop time
	peer       *peerpkg.Peer
	state      *peerSyncState
	prevHash   chainhash.Hash
	deferredAt time.Time
}

// deferredCheckpointMaxWait bounds how long a deferred checkpoint block may
// wait for its parent before the deferral gives up, requeues the hash for
// re-fetch, and bars re-deferral — guaranteeing the sync-peer rotation backstop
// still fires if the parent never commits (a double-fault this fix must not mask).
const deferredCheckpointMaxWait = 3 * time.Minute

// retryDeferredCheckpoint drives the deferred checkpoint block to commit once
// its parent lands. Runs on the drain goroutine's refill tick (20ms default),
// so the commit happens within ~one tick of the parent instead of after the
// 3-minute rotation. All slot access is drain-goroutine-only.
func (sm *SyncManager) retryDeferredCheckpoint() {
	d := sm.deferredCheckpoint
	if d == nil {
		return
	}

	// Committed elsewhere (a rotation's re-delivery raced the deferral): done.
	if exists, err := sm.blockchainClient.GetBlockExists(sm.ctx, &d.bmsg.blockHash); err == nil && exists {
		sm.deferredCheckpoint = nil
		return
	}

	// Deadline: the parent never committed — something else is wrong. Requeue
	// the hash for a normal re-fetch, bar re-deferral so the next delivery takes
	// the old arm verbatim, and let the rotation backstop do its job.
	if time.Since(d.deferredAt) > deferredCheckpointMaxWait {
		sm.deferredCheckpoint = nil
		sm.deferBarredCheckpoint = d.bmsg.blockHash
		sm.requeueFailedBlock(d.bmsg.blockHash)
		sm.logger.Warnf("[retryDeferredCheckpoint][%s] parent %s still uncommitted after %s; requeuing checkpoint block and disabling further deferral for it", d.bmsg.blockHash, d.prevHash, deferredCheckpointMaxWait)

		return
	}

	// Parent gate: cheap existence probe before re-driving the full commit.
	if _, _, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &d.prevHash); err != nil {
		return // parent still committing; try again next tick
	}

	if err := sm.HandleBlockDirect(sm.ctx, d.peer, d.bmsg.blockHash, d.msgBlock); err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			return // lost the race with the parent's commit finalisation; next tick
		}

		// Any other failure: the deferred bytes are unvalidated (a bad block from
		// a misbehaving peer escapes the usual reject path by design here — the
		// hash IS the hardcoded checkpoint, so the content that matters is pinned).
		// Clear the slot and requeue so the block is re-fetched from another peer.
		sm.deferredCheckpoint = nil
		sm.requeueFailedBlock(d.bmsg.blockHash)
		sm.logger.Warnf("[retryDeferredCheckpoint][%s] deferred commit failed (%v); requeuing for re-fetch", d.bmsg.blockHash, err)

		return
	}

	sm.runPostBlockProcessing(d.peer, d.state, d.bmsg, true)
	sm.deferredCheckpoint = nil
	sm.logger.Infof("[retryDeferredCheckpoint][%s] deferred checkpoint block committed after parent %s arrived", d.bmsg.blockHash, d.prevHash)
}

// releaseWindowBlocks releases ownership of every block in a handled flush job.
func (sm *SyncManager) releaseWindowBlocks(blocks []*model.Block) {
	if sm.windowOwnedBlocks == nil {
		return
	}

	for _, b := range blocks {
		if b != nil {
			sm.windowOwnedBlocks.Delete(*b.Hash())
		}
	}
}

// windowFlushJob is a drained, ascending-sorted window ready to commit. It is
// produced on the drain goroutine (drainJob) and consumed either synchronously
// (flush → commitWindowJob) or, in pipeline mode, by the single flush worker
// (flushWorker → commitWindowJob).
type windowFlushJob struct {
	blocks []*model.Block

	// done, when non-nil, is closed by the flush worker AFTER it has finished
	// handling this job (committed, or skipped because poisoned/shutdown). It
	// turns an otherwise fire-and-forget hand-off into a synchronous barrier: the
	// drain goroutine can hand off a job and wait for the worker to quiesce,
	// guaranteeing every earlier in-flight window has committed. Normal (async)
	// jobs leave it nil. Used by flushWindowSync for the direct/checkpoint path,
	// whose HandleBlockDirect requires its parent block already committed.
	done chan struct{}
}

// drainJob drains and resets the accumulator, sorts the drained blocks ascending
// by height, and returns them as a windowFlushJob. It returns ok=false (and an
// empty job) when the accumulator holds no blocks. It runs on the drain
// goroutine and performs no I/O — the commit itself lives in commitWindowJob.
//
// Entries are sorted ascending before any processing so the per-block fallback
// loop (recoverWindowCommit) can satisfy AddBlock's parent-availability
// requirement even if the window accumulated out-of-order arrivals.
func (wa *windowAccumulator) drainJob() (windowFlushJob, bool) {
	if wa.empty() {
		return windowFlushJob{}, false
	}

	entries := wa.entries
	wa.entries = make([]windowEntry, 0, 8)
	wa.bytesAccum = 0

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].block.Height < entries[j].block.Height
	})

	// De-duplicate by height. Under refetch/park churn the SAME block can be added
	// to the accumulator more than once (a re-delivery arriving while the original
	// is still queued — window add() does not dedupe). Committing a block twice makes
	// ProcessBlockWindow create that block's txs concurrently, which collides on the
	// partitioned txs unique index and deadlocks Postgres (40P01) — the root cause of
	// the create-vs-create deadlock that (via the flushWorker poison latch) was
	// freezing IBD. The window path is below-checkpoint only, where the chain is
	// pinned to exactly one block per height, so a repeated height IS the same block;
	// keep the first occurrence. The slice is already ascending by height, so
	// duplicates are adjacent and strict-ascending commit order is preserved.
	blocks := make([]*model.Block, 0, len(entries))

	for i, e := range entries {
		if i > 0 && e.block.Height == entries[i-1].block.Height {
			continue
		}

		blocks = append(blocks, e.block)
	}

	return windowFlushJob{blocks: blocks}, true
}

// commitWindowJob submits a drained window to ProcessBlockWindow. On error it
// runs a bounded infra-retry over the per-block idempotent ProcessBlock loop
// (recoverWindowCommit); if that ultimately fails it escalates by disconnecting
// the current sync peer so the pipeline rotates and re-requests the uncommitted
// suffix from our committed best-block. It returns true iff it escalated to a
// fatal disconnect, so the pipeline worker can poison itself and commit no
// later window after the resulting gap.
//
// ProcessBlockWindow is called under a context deadline: PeerProcessingTimeout
// scaled by the batch size (min 3 minutes) so a hung call cannot block the
// caller indefinitely.
//
// commitWindowJob never acks and never sends on any reply channel: the drain
// goroutine sends every windowed block's reply at accept-time (see
// ackWindowedBlock), so once here the blocks are already early-acked. It
// therefore only commits or, on unrecoverable failure, disconnects the sync
// peer — it never rejects a block to the peer and never advances
// best-block/progress on the fatal path.
func (sm *SyncManager) commitWindowJob(ctx context.Context, job windowFlushJob) bool {
	blocks := job.blocks
	if len(blocks) == 0 {
		return false
	}

	// Ownership release: whatever happens to this job — committed, fatal
	// escalation, or a panic unwinding through commitWindowJobRecovered — these
	// blocks leave the pipeline here. On the failure paths they re-sync via peer
	// rotation from the committed best-block, and the admission guard must not
	// skip that re-delivery, so a leaked claim is never acceptable. Deferred so
	// the panic path releases too.
	defer sm.releaseWindowBlocks(blocks)

	sm.logger.Debugf("[windowAccumulator] flushing window of %d blocks to ProcessBlockWindow", len(blocks))

	// Derive a deadline from PeerProcessingTimeout × batch size so the caller
	// cannot be blocked indefinitely by a slow ProcessBlockWindow. The minimum is
	// the raw PeerProcessingTimeout (covers a batch of one) and the parent ctx is
	// also respected (e.g. sm.ctx cancellation on shutdown).
	perBlock := sm.settings.Legacy.PeerProcessingTimeout
	if perBlock <= 0 {
		perBlock = 3 * time.Minute
	}
	deadline := perBlock * time.Duration(len(blocks)) //nolint:gosec
	flushCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	err := sm.blockValidation.ProcessBlockWindow(flushCtx, blocks, "", "legacy")
	if err == nil {
		// Success: acks were already sent at accept-time, nothing to do here.
		return false
	}

	// Post-ack obligation: these blocks were already early-acked at accept-time,
	// so they can no longer be rejected to the peer. On a commit failure the only
	// remaining outcomes are (a) recover the commit idempotently, or (b) escalate
	// by disconnecting the sync peer so the pipeline rotates and re-requests the
	// uncommitted suffix from our committed best-block. This never touches a
	// reply channel and never advances best-block/progress on the fatal path.
	sm.logger.Warnf("[windowAccumulator] ProcessBlockWindow error, entering bounded recovery: %v", err)

	if recErr := sm.recoverWindowCommit(ctx, blocks); recErr != nil {
		reason := fmt.Sprintf("post-ack window commit unrecoverable after %d attempts, disconnecting to trigger sync peer rotation: %v", windowCommitRetryCap, recErr)
		sm.logger.Errorf("[windowAccumulator] %s", reason)

		if sp := sm.loadSyncPeer(); sp != nil {
			sp.DisconnectWithWarning(reason)
		}

		return true
	}

	return false
}

// flush is the synchronous (pipeline-off) path: it drains the accumulator and
// commits the resulting window inline on the calling (drain) goroutine. It is
// byte-identical to the pre-pipeline behaviour — drainJob + commitWindowJob are
// the exact two halves of the original flush. In pipeline mode the drain
// goroutine calls drainJob directly and hands the job to flushWorker instead.
func (wa *windowAccumulator) flush(ctx context.Context, sm *SyncManager) {
	if j, ok := wa.drainJob(); ok {
		sm.commitWindowJob(ctx, j)
	}
}

// shutdownFlushHandoff performs the prompt, non-blocking pipeline shutdown
// hand-off. It attempts a single non-blocking send of the pending partial
// window to the flush worker, then closes jobs so the worker drains and exits.
//
// A blocking hand-off could stall shutdown for up to a full per-block commit
// deadline (PeerProcessingTimeout x batchSize, minutes) if the worker is
// mid-commit and the depth-1 slot is already full. So when the slot is full we
// ABANDON the pending window rather than block. This is safe: an uncommitted
// window advances no persistent state — the worker touches no reply channel and
// the chain tip only moves for committed windows — so on restart sync resumes
// from the committed best-block header and re-fetches the abandoned blocks.
//
// Only the single drain goroutine ever calls this or otherwise sends on/closes
// jobs, so neither the select-send nor the close can race a concurrent sender.
// jobs is nil when the pipeline sub-flag is off; then there is nothing to do.
func (sm *SyncManager) shutdownFlushHandoff(wa *windowAccumulator, jobs chan windowFlushJob) {
	if jobs == nil {
		return
	}

	if wa != nil && !wa.empty() {
		if j, ok := wa.drainJob(); ok {
			select {
			case jobs <- j:
			default:
				sm.releaseWindowBlocks(j.blocks)
				sm.logger.Warnf("[blockHandler] shutdown: abandoning pending window of %d blocks (worker busy); it will be re-synced from the committed best-block on restart", len(j.blocks))
			}
		}
	}

	close(jobs)
}

// flushWorker is the single FIFO flush-worker goroutine used in pipeline mode.
// It commits each handed-off window in strict produced (ascending, contiguous)
// order so window W fully commits before W+1 — the chain is sequential, so W's
// blocks are W+1's parents.
//
// Consensus-critical: after a commitWindowJob escalates to a fatal disconnect,
// the worker sets poisoned=true and thereafter DRAINS the remaining queued jobs
// WITHOUT committing any of them. The worker is the only committer of windowed
// blocks; ineligible/checkpoint blocks commit synchronously on the drain path
// via HandleBlockDirect, and those two paths are reconciled fail-closed by
// HandleBlockDirect's pre-flight GetBlockHeader(prev) → ErrBlockNotFound →
// re-request as described in handleBlockMsgWithWindow above. Because the worker
// processes strictly FIFO, this guarantees no later window is committed after a
// gap (a committed gap would be a consensus bug). Only the drain goroutine ever
// sends on or closes jobs.
//
// Shutdown: the normal shutdown path is the drain goroutine closing jobs (see
// blockHandler's sm.quit branch); the worker then finishes committing whatever
// is already queued (unless poisoned) and returns when the range over jobs
// ends. Stop() closes sm.quit but does NOT cancel sm.ctx, so the ctx.Done()
// branch below does not fire on a normal Stop(); it only fires if the context
// this worker was started with is cancelled elsewhere, in which case the worker
// drains the channel WITHOUT committing any further window and exits.
// windowRelinksAfterPoison reports whether a window arriving at a poisoned
// flushWorker is contiguous with the committed chain — its lowest block is at or
// below committedBest+1. After a fatal commit the sync peer is disconnected and the
// pipeline re-requests the uncommitted suffix from our committed best-block, so a
// re-delivered window starts at committedBest+1; clearing the poison latch for such
// a window lets the (idempotent) commit be retried instead of discarded forever. A
// window starting ABOVE committedBest+1 is a real gap and must stay poisoned.
// Best-effort: on a best-block lookup error (or empty window) it returns false so
// the worker stays safely poisoned. Called only while poisoned (rare).
func (sm *SyncManager) windowRelinksAfterPoison(ctx context.Context, job windowFlushJob) bool {
	if len(job.blocks) == 0 || sm.blockchainClient == nil {
		return false
	}

	_, meta, err := sm.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil || meta == nil {
		sm.logger.Warnf("[flushWorker] poison-recovery best-block lookup failed, staying poisoned: %v", err)
		return false
	}

	return job.blocks[0].Height <= meta.Height+1
}

func (sm *SyncManager) flushWorker(ctx context.Context, jobs <-chan windowFlushJob) {
	poisoned := false

	for {
		select {
		case <-ctx.Done():
			// Context cancelled: exit without committing further. Drain any
			// buffered jobs (committing none) so the drain goroutine's pending
			// send, if any, does not block, then return once the channel closes.
			// Release any barrier waiter on each drained job so flushWindowSync
			// cannot hang on shutdown, and release block ownership so nothing
			// stays claimed by a job that will never commit.
			for job := range jobs {
				sm.releaseWindowBlocks(job.blocks)

				if job.done != nil {
					close(job.done)
				}
			}

			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			// Recover from a prior fatal commit once a fresh, tip-aligned window
			// arrives. A fatal commit disconnects the sync peer (commitWindowJob),
			// which rotates and re-requests the uncommitted suffix from our committed
			// best-block, so the re-delivered window starts at committedBest+1.
			// Clearing the latch for such a contiguous window (NOT for one that starts
			// beyond committedBest+1 — a genuine gap) converts what was a permanent,
			// restart-only wedge into an idempotent re-commit. Without this the poison
			// flag (a goroutine-lifetime local) never resets and a single transient
			// commit failure — e.g. a Postgres 40P01 deadlock in the UTXO create
			// batcher — freezes the tip until the process restarts.
			if poisoned && sm.windowRelinksAfterPoison(ctx, job) {
				sm.logger.Warnf("[flushWorker] tip-aligned window (first height %d) arrived after poison; clearing latch and retrying commit", job.blocks[0].Height)
				poisoned = false
			}

			if poisoned {
				// A prior window hit a fatal gap; commit no later window. The
				// discarded blocks re-sync after rotation/restart, so their
				// ownership must be released or the re-delivery is skipped forever.
				sm.logger.Warnf("[flushWorker] poisoned after fatal window commit, discarding queued window of %d blocks", len(job.blocks))
				sm.releaseWindowBlocks(job.blocks)
			} else if sm.commitWindowJobRecovered(ctx, job) {
				poisoned = true
			}

			// Release any barrier waiter AFTER the job has been fully handled
			// (committed, or skipped when poisoned). A synchronous flusher
			// (flushWindowSync) blocks on this so it observes the commit. Closed
			// even when poisoned so the waiter never hangs — HandleBlockDirect's
			// own parent-availability check then handles the (rare) poisoned case.
			if job.done != nil {
				close(job.done)
			}
		}
	}
}

// commitWindowJobRecovered wraps commitWindowJob so a panic in the commit path
// (ProcessBlockWindow / recovery) is converted into the SAME fatal-poison
// outcome as an unrecoverable commit error, instead of killing the flushWorker
// goroutine. A dead worker would never drain jobs, so the drain goroutine's
// blocking `jobs <- j` send would wedge forever — a silent permanent IBD stall.
//
// On panic it returns true (poison): the worker keeps draining and discarding
// queued windows WITHOUT committing any (no committed gap — semantically
// identical to commitWindowJob's fatal-disconnect escalation), and the drain
// goroutine's sends still complete. The panic is not a normal commit failure,
// so we do not attempt bounded recovery or a peer disconnect here; poisoning
// halts all further commits, and the uncommitted suffix re-syncs from the
// committed best-block on the next restart / peer rotation.
func (sm *SyncManager) commitWindowJobRecovered(ctx context.Context, job windowFlushJob) (poison bool) {
	defer func() {
		if r := recover(); r != nil {
			poison = true

			sm.logger.Errorf("[flushWorker] recovered panic in window commit, poisoning worker (no further windows committed): %v", r)
		}
	}()

	return sm.commitWindowJob(ctx, job)
}

// windowCommitRetryCap bounds the number of recovery passes flush makes over the
// per-block idempotent ProcessBlock loop after a ProcessBlockWindow failure. It
// is small because recovery only helps for transient infra errors; a fatal error
// escalates immediately without consuming the cap.
const windowCommitRetryCap = 3

// windowCommitRetryBackoff is the short pause between bounded recovery passes so
// a transient infra dependency has a moment to settle without stalling the drain
// goroutine for long.
const windowCommitRetryBackoff = 250 * time.Millisecond

// recoverWindowCommit re-drives the commit for an already-early-acked window via
// the per-block idempotent ProcessBlock loop (creates are idempotent so
// re-processing a committed block is safe). Blocks must already be sorted
// ascending by height so each AddBlock can locate its parent.
//
// Classification uses the SAME predicate as the peer path (peer_server.go): an
// ErrServiceError/ErrStorageError is infra/transient (retryable up to the cap
// with a short backoff); anything else is fatal and escalates immediately. It
// returns nil once the whole window commits, or the escalating error when the
// retry cap is exhausted on an infra error or a fatal error is hit.
func (sm *SyncManager) recoverWindowCommit(ctx context.Context, blocks []*model.Block) error {
	var lastErr error

	for attempt := 1; attempt <= windowCommitRetryCap; attempt++ {
		lastErr = nil

		for _, block := range blocks {
			if err := sm.ProcessBlock(ctx, block); err != nil {
				lastErr = err

				// Fatal (non-infra) errors cannot be recovered by retrying; escalate now.
				if !errors.Is(err, errors.ErrServiceError) && !errors.Is(err, errors.ErrStorageError) {
					sm.logger.Errorf("[windowAccumulator] recovery hit fatal error for height %d, escalating: %v", block.Height, err)
					return err
				}

				sm.logger.Warnf("[windowAccumulator] recovery infra error for height %d (attempt %d/%d): %v", block.Height, attempt, windowCommitRetryCap, err)

				break
			}
		}

		if lastErr == nil {
			return nil
		}

		if attempt < windowCommitRetryCap {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(windowCommitRetryBackoff):
			}
		}
	}

	return lastErr
}

// ackWindowedBlock sends the accept-time ack for a block that has just been
// added to the window, applying withhold-on-full back-pressure. It runs on the
// single drain goroutine after wa.add.
//
//   - Not full: ack immediately (release the peer to stream the next block) and
//     arm/re-arm the flush timer. The window keeps filling.
//   - Full: flush FIRST (commit the batch while the peer is still parked on this
//     block's ack — this is the back-pressure), THEN ack, THEN stop the timer.
//     Acking before the flush would drop the back-pressure and let the peer race
//     ahead of the commit.
//
// The timer manipulation is delegated to the caller's closures so the timer
// state stays owned by the drain goroutine.
func (sm *SyncManager) ackWindowedBlock(reply chan error, wa *windowAccumulator, flushWindow, armTimer, stopTimer func()) {
	if wa.full() {
		sm.logger.Debugf("[blockHandler] window budget exhausted, flushing before ack (back-pressure)")
		flushWindow()

		if reply != nil {
			reply <- nil
		}

		stopTimer()

		return
	}

	// Not full: early-ack releases the peer to stream the next block so the
	// window can keep filling, then arm the flush timer.
	if reply != nil {
		reply <- nil
	}

	armTimer()
}

// releaseParkedBlocks moves parked far-ahead blocks that block assembly has now
// matured past back into the window accumulator. It runs on the drain goroutine
// (the continuous-refill tick), so it needs no lock. Two invariants make it safe:
//
//   - CONTIGUITY: it releases ONLY the ascending run that is contiguous with the
//     committed tip — the first released height must equal cached+1 and each
//     subsequent height must be exactly one higher. A gap stops the run. This
//     guarantees a released block's parent is already committed (<= cached) or was
//     released earlier in the same ascending run, so a flush can never present
//     ProcessBlockWindow with a missing parent (recoverWindowCommit would treat
//     that as fatal and escalate to a sync-peer disconnect + pipeline poison).
//   - CEILING: it never releases past cached+maxBehind, the exact coinbase-maturity
//     inequality of the admission gate, so parking never admits a block early and a
//     single pass releases at most maxBehind blocks (so a large block-assembly jump
//     cannot monopolise the drain goroutine in one iteration).
func (sm *SyncManager) releaseParkedBlocks(park *parkStore, wa *windowAccumulator, flushWindow, armTimer func()) {
	if park == nil || park.len() == 0 {
		return
	}

	maxBehind := sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly
	if maxBehind <= 0 {
		return
	}

	cached := sm.cachedBlockAssemblyHeight.Load()
	// Trust the cache once the poller has reported — a genuine height of 0
	// (fresh node) must release the run 1..maxBehind, not freeze the park.
	if !sm.baHeightPolled.Load() || cached > math.MaxUint32-uint32(maxBehind) {
		return
	}

	ceiling := cached + uint32(maxBehind)

	sort.Slice(park.entries, func(i, j int) bool {
		return park.entries[i].block.Height < park.entries[j].block.Height
	})

	next := cached + 1 // required height for contiguity with the committed tip
	survivors := make([]windowEntry, 0, park.len())
	released := 0

	for i := range park.entries {
		b := park.entries[i].block
		h := b.Height

		switch {
		case h <= cached:
			// At/below the committed tip. This DOES happen: before the ownership
			// ledger, a re-fetched copy of an already-parked block became a parked
			// TWIN, and once the first copy committed the twin surfaced here (the
			// mainnet duplicate-commit storm proved the old "not committed by any
			// other path" assumption false — the other path is a second copy of
			// itself). The block was already early-acked, so just drop it (do not
			// re-add) and release its ownership so a legitimate future re-delivery
			// is not skipped forever.
			park.bytesAccum -= int64(b.SizeInBytes) //nolint:gosec
			sm.releaseWindowBlock(*b.Hash())
			sm.logger.Debugf("[releaseParkedBlocks] dropping parked block height %d at/below committed tip %d", h, cached)
		case h == next && h <= ceiling:
			wa.add(b)
			park.bytesAccum -= int64(b.SizeInBytes) //nolint:gosec
			next++
			released++

			if wa.full() {
				flushWindow()
			}
		default:
			// Gap (h != next) or beyond the ceiling. Sorted ascending, so this and
			// every higher parked block must stay. Keep the remainder and stop.
			survivors = append(survivors, park.entries[i:]...)

			park.entries = survivors
			if released > 0 {
				if !wa.empty() {
					armTimer()
				}

				sm.logger.Debugf("[releaseParkedBlocks] released %d parked blocks into window (cached %d, ceiling %d, %d still parked)", released, cached, ceiling, park.len())
			}

			return
		}
	}

	// Fell off the end: every remaining parked block was dropped or released.
	park.entries = survivors

	if released > 0 {
		if !wa.empty() {
			armTimer()
		}

		sm.logger.Debugf("[releaseParkedBlocks] released %d parked blocks into window (cached %d, ceiling %d, %d still parked)", released, cached, ceiling, park.len())
	}
}

// effectiveGOMEMLIMIT reads the current GOMEMLIMIT without modifying it.
// Returns a 6 GB fallback when the limit is unset (math.MaxInt64) or <= 0.
func effectiveGOMEMLIMIT() int64 {
	const fallback = 6 * 1024 * 1024 * 1024 // 6 GB

	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return fallback
	}

	return limit
}

// windowBudgetBytes converts the GOMEMLIMIT fraction to a byte budget.
// Returns 0 when fraction <= 0 (window path disabled).
func windowBudgetBytes(fraction float64) int64 {
	if fraction <= 0 {
		return 0
	}

	return int64(float64(effectiveGOMEMLIMIT()) * fraction)
}

// windowFlushTimerInterval is the idle timeout after which a partially-filled
// window is flushed. It bounds how long the last few blocks of a window wait
// for a budget-filling block that never arrives (e.g. at the tail of a sync).
const windowFlushTimerInterval = 200 * time.Millisecond

// blockHandler is the main handler for the sync manager.  It must be run as a
// goroutine.  It processes block and inv messages in a separate goroutine
// from the peer handlers so the block (MsgBlock) messages are handled by a
// single thread without needing to lock memory data structures.  This is
// important because the sync manager controls which blocks are needed and how
// the fetching should proceed.
func (sm *SyncManager) blockHandler() {
	ticker := time.NewTicker(syncPeerTickerInterval)
	defer ticker.Stop()

	// This buffer holds one *blockQueueMsg (a *wire.MsgBlock pointer) per slot.
	// With prefetch disabled a small fixed depth suffices: OnBlock keeps at most
	// one block per peer in flight, so the queue barely fills.
	//
	// With prefetch enabled the depth must be at least the byte-budget admission
	// ceiling (budget / minInFlightBlockWeight). Otherwise a full pipeline would
	// block blockHandler on `blockQueue <-`, and since that goroutine is the sole
	// consumer of msgChan, disconnects, sync-peer rotation, inv, headers and tx
	// dispatch would stall for EVERY peer — cross-peer head-of-line blocking. The
	// deeper queue does not raise the memory ceiling: the blocks it references are
	// still bounded in total bytes by the prefetch budget (AcquireBlockPrefetch),
	// so at most ~budget bytes of MsgBlocks are pinned regardless of slot count.
	// The slot count is clamped so a misconfigured multi-TB budget can't size a
	// huge channel backing array; beyond the clamp the budget still bounds memory
	// and the sm.quit-guarded enqueue still can't deadlock, only backpressure.
	maxBlockQueue := 100
	if sm.blockPrefetchBudget != nil {
		if ceiling := int(sm.blockPrefetchBudgetBytes / minInFlightBlockWeight); ceiling > maxBlockQueue {
			maxBlockQueue = ceiling
		}
		if maxBlockQueue > maxBlockQueueSlots {
			maxBlockQueue = maxBlockQueueSlots
		}
	}

	// create a block queue to handle block messages in a separate goroutine, in order
	blockQueue := make(chan *blockQueueMsg, maxBlockQueue)

	// headersQueue delivers headers messages to the SAME single drain goroutine
	// that processes blockQueue. Header processing mutates the shared
	// headers-first state (headerList, startHeader, nextCheckpoint,
	// headerCheckpoint) that block processing also mutates, so both must run on
	// one goroutine to stay race-free. The headers-first pipeline lets interval
	// N+1's headers arrive while interval N's blocks are still in flight, so
	// running handleHeadersMsg on its own goroutine (as it used to) races with
	// block processing; serialising it here removes that race by construction
	// while keeping the early-getheaders latency win (the pipelined getheaders
	// still fires from this goroutine). Header batches are infrequent (one per
	// interval), so a small buffer suffices.
	headersQueue := make(chan *headersMsg, maxBlockQueue)

	// start the block queue handler
	go func() {
		windowFraction := sm.settings.Legacy.ParallelWindowMemoryFraction
		windowEnabled := windowFraction > 0

		// Pipeline mode: when the window is enabled AND the pipeline sub-flag is
		// set, window commits run on a single dedicated FIFO flush worker so the
		// next window fills while the current one commits. Bounded to two windows
		// in flight (channel depth 1 + the one the worker holds). Only this drain
		// goroutine ever sends on or closes jobs.
		pipelineEnabled := windowEnabled && sm.settings.Legacy.ParallelWindowPipeline

		var wa *windowAccumulator
		var flushTimer *time.Timer
		var jobs chan windowFlushJob

		if windowEnabled {
			budget := windowBudgetBytes(windowFraction)
			// The window's block-count cap can be set BELOW the maturity ceiling
			// (MaxBlocksBehindBlockAssembly) so the pipeline flush worker commits a
			// small window while the drain fills the next one from the remaining
			// runway — overlapping CPU-side prep with disk-side commit instead of
			// alternating (the IBD commit sawtooth). WindowMaxBlocks == 0 (or >= the
			// ceiling) falls back to the ceiling, byte-identical to before. The
			// maturity ceiling itself (releaseParkedBlocks) is unchanged.
			maxBlocks := sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly
			if wmb := sm.settings.Legacy.WindowMaxBlocks; wmb > 0 && wmb < maxBlocks {
				maxBlocks = wmb
			}
			wa = newWindowAccumulator(budget, maxBlocks)
			flushTimer = time.NewTimer(windowFlushTimerInterval)
			flushTimer.Stop() // don't fire until we have blocks
		}

		if pipelineEnabled {
			jobs = make(chan windowFlushJob, 1)
			go sm.flushWorker(sm.ctx, jobs)
		}

		// Continuous-refill ticker: fires maintainInFlightWindow on THIS drain
		// goroutine to keep the in-flight block-fetch window at the cap between
		// block completions (getdata top-up only, non-blocking). Disabled when the
		// interval is <= 0. maintainInFlightWindow is a cheap no-op outside
		// headers-first IBD, so the tick costs nothing post-sync.
		var refillC <-chan time.Time

		if sm.settings.Legacy.InFlightRefillInterval > 0 {
			refillTicker := time.NewTicker(sm.settings.Legacy.InFlightRefillInterval)
			defer refillTicker.Stop()

			refillC = refillTicker.C
		}

		// Park store for far-ahead blocks (non-blocking maturity gate). Requires the
		// window path AND the refill tick (its release trigger); if parking is
		// requested without the refill tick we warn and leave park nil, so the gate
		// falls back to the original blocking wait (a no-op, never a wedge).
		var park *parkStore
		if windowEnabled && sm.settings.Legacy.ParallelWindowParkAhead {
			if refillC == nil {
				sm.logger.Warnf("[blockHandler] parallelWindowParkAhead requires inFlightRefillInterval>0 for the release trigger; parking disabled")
			} else {
				parkBudget := windowBudgetBytes(sm.settings.Legacy.ParallelWindowParkedMemoryFraction)
				park = newParkStore(parkBudget, sm.settings.Legacy.ParallelWindowMaxParkedBlocks)
				sm.parkAheadActive.Store(true)
				sm.parkRef.Store(park)
			}
		}

		flushWindow := func() {
			if wa != nil && !wa.empty() {
				// Contiguity gate (stall-burst fix): hand only the run continuing
				// the last handed height; post-gap strays go to the park for
				// ordered re-release. A gate that empties the job hands nothing.
				if j, ok := wa.drainJob(); ok {
					j = sm.gateContiguousWindow(j, park)

					if len(j.blocks) > 0 {
						if pipelineEnabled {
							// Hand the drained window to the worker. The blocking send is
							// the depth-1 back-pressure that bounds in-flight windows.
							jobs <- j
						} else {
							sm.commitWindowJob(sm.ctx, j)
						}
					}
				}
			}

			if flushTimer != nil {
				flushTimer.Stop()
			}
		}

		// flushWindowSync is the synchronous variant of flushWindow used before a
		// direct/checkpoint commit (see handleBlockMsgWithWindow). In pipeline mode
		// it hands a barrier job (the pending window, possibly empty) to the flush
		// worker and BLOCKS until the worker signals it finished committing — FIFO
		// ordering means every earlier in-flight window has committed too, so a
		// following HandleBlockDirect is guaranteed to find its parent already in the
		// blockchain. Without this the async hand-off lets the direct commit race the
		// worker and fail its parent check, stalling at the checkpoint boundary until
		// the slow re-request path recovers. Only the drain goroutine calls this, so
		// it never races another sender on jobs.
		flushWindowSync := func() {
			if wa == nil || !pipelineEnabled {
				// Pipeline off: flushWindow already commits inline on this goroutine.
				flushWindow()
				return
			}

			done := make(chan struct{})

			// drainJob yields ok=false (empty job) when nothing is pending; an empty
			// job is still a valid barrier — it commits nothing but, being FIFO after
			// any in-flight window, its completion proves that window committed.
			// The contiguity gate applies here too: strays are parked and the
			// (possibly emptied) job is still handed — an empty barrier is valid.
			j, _ := wa.drainJob()
			j = sm.gateContiguousWindow(j, park)
			j.done = done

			select {
			case jobs <- j:
			case <-sm.quit:
				return
			case <-sm.ctx.Done():
				return
			}

			select {
			case <-done:
			case <-sm.quit:
			case <-sm.ctx.Done():
			}

			if flushTimer != nil {
				flushTimer.Stop()
			}
		}

		// armTimer (re-)starts the idle flush timer using the stop-drain-reset
		// idiom: Stop returns false when the timer has already fired, in which
		// case the channel may hold a value that must be drained before Reset —
		// otherwise the select picks up a stale expiry and triggers a spurious
		// empty flush on the next iteration.
		armTimer := func() {
			if flushTimer != nil {
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}

				flushTimer.Reset(windowFlushTimerInterval)
			}
		}

		stopTimer := func() {
			if flushTimer != nil {
				flushTimer.Stop()
			}
		}

		var timerC <-chan time.Time
		if flushTimer != nil {
			timerC = flushTimer.C
		}

		for {
			select {
			case <-sm.quit:
				// Prompt, non-blocking shutdown hand-off of the pending window,
				// then close jobs so the worker exits. Only the drain goroutine
				// (this goroutine) sends on or closes jobs.
				sm.shutdownFlushHandoff(wa, jobs)

				// Parked blocks were already early-acked and backlog-decremented at
				// park time, and their data lives in the blob store; on shutdown they
				// are simply discarded and re-synced from the committed best-block on
				// restart. No reply or backlog action is owed here.
				if park != nil && park.len() > 0 {
					sm.logger.Warnf("[blockHandler] shutdown: discarding %d parked blocks; will re-sync from committed best-block on restart", park.len())

					for i := range park.entries {
						sm.releaseWindowBlock(*park.entries[i].block.Hash())
					}

					park.entries = nil
					park.bytesAccum = 0
				}

				// Best-effort drain of already-queued blocks with an error reply
				// before exiting. Under prefetch each queued block has an
				// awaitBlockResult goroutine holding budget and waiting on its
				// reply; replying here lets them exit promptly on shutdown instead
				// of waiting for the peer's quit/ctx to fire. The feeder (the outer
				// loop) races the same sm.quit close, so a block it enqueues after
				// this drain returns is not caught here — that block's
				// awaitBlockResult still exits via sp.quit/sp.ctx.Done() (the
				// backstop), and the feeder's enqueue is itself sm.quit-guarded so
				// it can never deadlock. This drain only makes the common case
				// prompt; it is not relied on for correctness.
				for {
					select {
					case msg := <-blockQueue:
						// Keep the backlog decrement and its progress stamp paired
						// on every completion path (here the shutdown drain) so the
						// liveness invariant holds uniformly; rotation is moot during
						// shutdown, but the uniform pairing is easier to reason about.
						sm.blockBacklog.Add(-1)
						sm.noteBacklogProgress()

						if msg.reply != nil {
							msg.reply <- errors.NewServiceError("sync manager shutting down")
						}
					default:
						return
					}
				}
			case <-timerC:
				sm.logger.Debugf("[blockHandler] window flush timer fired")
				flushWindow()
			case <-refillC:
				// Continuous-refill top-up on the drain goroutine. No-op unless in
				// headers-first IBD; getdata top-up only, never getheaders re-arm.
				// maintainInFlightWindow runs FIRST so the low (tip+1) block keeps
				// priority on the refetch budget, then release any parked far-ahead
				// blocks that block assembly has now matured past. No-op when parking
				// is disabled (park is nil).
				sm.maintainInFlightWindow()
				sm.releaseParkedBlocks(park, wa, flushWindow, armTimer)
				sm.retryDeferredCheckpoint()
			case hmsg := <-headersQueue:
				// Runs on the SAME goroutine as block processing, so the shared
				// headers-first state is never touched concurrently.
				// handleHeadersMsg links, verifies, appends and (at a
				// checkpoint) fires the pipelined getheaders — all cheap and
				// non-blocking, interleaved with block processing rather than
				// racing it.
				sm.handleHeadersMsg(hmsg)
			case msg := <-blockQueue:
				sm.logger.Debugf("[blockHandler][%s] processing block queue message into handleBlockMsg", msg.blockHash)

				if !windowEnabled {
					err := sm.handleBlockMsg(msg)

					// A completion advances the backlog: stamp it so the stall check
					// treats the pipeline as live for another window (see
					// noteBacklogProgress / localReadBackpressured).
					sm.blockBacklog.Add(-1)
					sm.noteBacklogProgress()

					// Tolerated (non-peer-fault) failure: re-fetch the block instead
					// of dropping it. handleBlockPreamble already cleared it from all
					// in-flight tracking on arrival, so without this it is lost and the
					// ascending commit pipeline wedges. A peer-fault (consensus-invalid)
					// block is NOT requeued — the peer is disconnected in OnBlock.
					if err != nil && !BlockProcessingErrorIsPeerFault(err) {
						sm.requeueFailedBlock(msg.blockHash)
					}

					if msg.reply != nil {
						msg.reply <- err
					}

					continue
				}

				// Window path: call handleBlockMsgWithWindow.
				outcome, err := sm.handleBlockMsgWithWindow(msg, wa, flushWindow, flushWindowSync, park)
				switch outcome {
				case blockAdmitParked:
					// Parked ahead of block assembly. Decrement the backlog and
					// early-ack now (releasing the prefetch-budget reservation) exactly
					// as for an admitted block: a parked block is prepared, its subtrees
					// are in the blob store, and it is held in the drain-owned park
					// buffer (its own memory budget) until releaseParkedBlocks admits it.
					// No requeue, no flush, no timer — releaseParkedBlocks arms the timer
					// when it admits.
					sm.blockBacklog.Add(-1)

					if msg.reply != nil {
						msg.reply <- nil
					}
				case blockAdmitWindowed:
					// Block was added to the window accumulator. Send the ack at
					// accept-time (or, when the window is now full, after the
					// full-flush commit — withhold-on-full back-pressure).
					sm.blockBacklog.Add(-1)
					sm.ackWindowedBlock(msg.reply, wa, flushWindow, armTimer, stopTimer)
				default: // blockAdmitDirect (incl. park-overflow tolerated error)
					// Block was processed directly (or failed).
					// handleBlockMsgWithWindow does not send the reply on the
					// direct path — we send the outcome here.
					sm.blockBacklog.Add(-1)

					// Requeue a tolerated direct-path failure (see the non-window
					// branch above for why); peer-fault blocks are left for OnBlock
					// to disconnect on.
					if err != nil && !BlockProcessingErrorIsPeerFault(err) {
						sm.requeueFailedBlock(msg.blockHash)
					}

					if msg.reply != nil {
						msg.reply <- err
					}

					// Flush any pending window now that an ineligible block arrived.
					flushWindow()
				}
			}
		}
	}()

out:
	for {
		select {
		case <-ticker.C:
			sm.handleCheckSyncPeer()
		case m := <-sm.msgChan:
			// whenever legacy receives a message, check if we are current
			// this call should have the current state cached, so it should be fast
			currentState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
			if err != nil {
				sm.logger.Errorf("[SyncManager] failed to get fsm current state")
			}

			// we reached current in legacy, and current FSM state is not Running, send RUN event
			if currentState != nil && *currentState != teranodeblockchain.FSMStateRUNNING {
				if sm.current() { // only call this when we are not in the running state, it's an expensive call
					sm.logger.Infof("[SyncManager] Legacy reached current, sending RUN event to FSM")
					if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/blockHandler"); err != nil {
						sm.logger.Infof("[Sync Manager] failed to send FSM RUN event %v", err)
					}

					sm.resetFeeFilterToDefault()
				}
			}

			switch msg := m.(type) {
			case *newPeerMsg:
				sm.handleNewPeerMsg(msg.peer)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case *txMsg:
				go func(msg *txMsg) {
					// process tx messages in parallel
					sm.handleTxMsg(msg)
					if msg.reply != nil {
						msg.reply <- struct{}{}
					}
				}(msg)

			case *blockMsg:
				sm.logger.Debugf("[blockHandler][%s] queueing block for validation", msg.block.Hash())

				// A 0->1 transition opens a fresh backpressure window: stamp its
				// start so localReadBackpressured can tell slow-but-progressing
				// validation from a genuine processing hang. Enqueues into an
				// already-non-empty backlog deliberately do NOT stamp — only
				// completions advance processing, so letting a peer refresh the
				// liveness signal merely by feeding more blocks into a hung
				// pipeline would mask the hang.
				if sm.blockBacklog.Add(1) == 1 {
					sm.noteBacklogProgress()
				}

				// Guard the enqueue with sm.quit. This is the sole feeder of
				// blockQueue; without the guard, a full queue whose consumer has
				// already exited on shutdown would block here forever, so the loop
				// would never reach the sm.quit case, close(handlerDone) would never
				// run, and Stop() (which waits on handlerDone) would hang.
				select {
				case blockQueue <- &blockQueueMsg{
					block:       msg.block.MsgBlock(),
					blockHash:   *msg.block.Hash(),
					blockHeight: msg.block.Height(),
					peer:        msg.peer,
					reply:       msg.reply,
				}:
				case <-sm.quit:
					// Enqueue aborted on shutdown: undo the Add(1) above and keep
					// the decrement paired with its progress stamp, matching every
					// other completion path (uniform invariant; harmless here).
					sm.blockBacklog.Add(-1)
					sm.noteBacklogProgress()

					if msg.reply != nil {
						msg.reply <- errors.NewServiceError("sync manager shutting down")
					}
				}

			case *invMsg:
				go sm.handleInvMsg(msg)

			case *headersMsg:
				// Serialise header processing onto the single drain goroutine
				// (the same consumer as blocks) so it never races block
				// processing over the shared headers-first state. Like the
				// blockQueue send above, this send is only reached from this
				// outer loop, which also watches sm.quit, so it cannot block
				// indefinitely after shutdown.
				headersQueue <- msg

			case *donePeerMsg:
				sm.handleDonePeerMsg(msg.peer)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case getSyncPeerMsg:
				var peerID int32

				if sp := sm.loadSyncPeer(); sp != nil {
					peerID = sp.ID()
				}
				msg.reply <- peerID

			case isCurrentMsg:
				sm.logger.Warnf("isCurrentMsg is deprecated, use current() instead")
				msg.reply <- sm.current()

			case pauseMsg:
				// Wait until the sender unpauses the manager.
				<-msg.unpause

			default:
				sm.logger.Warnf("Invalid message type in block handler: %T", msg)
			}

		case <-sm.quit:
			break out
		}
	}

	close(sm.handlerDone)
	sm.logger.Infof("Block handler done")
}

// NewPeer informs the sync manager of a newly active peer.
func (sm *SyncManager) NewPeer(peer *peerpkg.Peer, done chan struct{}) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		if done != nil {
			done <- struct{}{}
		}
		return
	}
	sm.msgChan <- &newPeerMsg{peer: peer, reply: done}
}

// QueueTx adds the passed transaction message and peer to the block handling
// queue. Responds to the done channel argument after the tx message is
// processed.
func (sm *SyncManager) QueueTx(tx *bsvutil.Tx, peer *peerpkg.Peer, done chan struct{}) {
	// Don't accept more transactions if we're shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		if done != nil {
			done <- struct{}{}
		}
		return
	}

	sm.msgChan <- &txMsg{tx: tx, peer: peer, reply: done}
}

// QueueBlock adds the passed block message and peer to the block handling
// queue. Responds to the done channel argument after the block message is
// processed.
func (sm *SyncManager) QueueBlock(block *bsvutil.Block, peer *peerpkg.Peer, done chan error) {
	// Don't accept more blocks if we're shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		done <- nil
		return
	}

	sm.msgChan <- &blockMsg{block: block, peer: peer, reply: done}
}

// UsePrefetchIngestion reports whether OnBlock should take the bounded async
// prefetch path. It requires a configured budget AND that we are not on
// regression net: the block-acceptance tooling depends on submit-then-query
// ordering, which only the synchronous path (OnBlock returns after the block is
// fully processed) guarantees. So regtest keeps synchronous ingestion — paired
// with, and for the same reason as, the regtest exception in BlockRequested. It
// shares the peerpkg.UseBlockPrefetchIngestion predicate with the read-loop's
// shouldArmProcessingTimer so both agree on when prefetch is active (a positive
// budget matches a non-nil budget semaphore, since it is created iff the byte
// budget is positive). A nil chainParams fails closed to the synchronous path.
func (sm *SyncManager) UsePrefetchIngestion() bool {
	if sm.chainParams == nil {
		// Fail closed to the synchronous path: without params we cannot rule out
		// regtest, and sync ingestion is the conservative default. Guarding here
		// matters because sm.chainParams.Net is evaluated as a call argument,
		// before UseBlockPrefetchIngestion's budget short-circuit could guard it.
		return false
	}

	return peerpkg.UseBlockPrefetchIngestion(sm.blockPrefetchBudgetBytes, sm.chainParams.Net)
}

// BlockRequested reports whether blockHash is one we have an outstanding
// getdata request for from the given peer (resolving stream peers to their
// association primary, as handleBlockMsg does). It lets the read-loop reject
// unrequested blocks BEFORE they consume prefetch budget, mirroring the
// unrequested-block check in handleBlockMsg. Under async prefetch this is what
// preserves the original backpressure: without it a misbehaving peer could
// admit a flood of unrequested blocks against the shared budget — starving the
// real sync peer and inflating buffered-block memory — before the downstream
// per-block disconnect fires. On regtest it always returns true; the regression
// harness intentionally feeds unrequested/duplicate blocks.
func (sm *SyncManager) BlockRequested(peer *peerpkg.Peer, blockHash *chainhash.Hash) bool {
	if sm.isRegtest() {
		return true
	}

	// Resolve stream sub-peers to their association primary, as handleBlockMsg
	// does; BlockRequested only reads the resolved state, so the primary itself
	// is not needed here.
	state, _, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		return false
	}

	_, requested := state.requestedBlocks.Get(*blockHash)
	if requested {
		return true
	}

	// Same tolerance as handleBlockMsg: a still-needed block whose ledger entry
	// was cleared by a rotation (or expired) is not an offense.
	return sm.blockStillNeeded(*blockHash)
}

// AcquireBlockPrefetch reserves prefetch budget for a block of the given
// serialized size and returns the amount actually reserved, which the caller
// MUST later hand back to ReleaseBlockPrefetch exactly once. The weight is
// clamped to the total budget so a block larger than the whole budget is
// admitted alone (it waits until every other in-flight block has drained),
// which preserves the original one-block-at-a-time backpressure for huge
// blocks and guarantees Acquire can never deadlock on an oversized block.
//
// It returns an error only if ctx is cancelled while waiting (shutdown), in
// which case nothing was reserved, OR the benign ErrDuplicateBlockInFlight
// sentinel when blockHash is already in flight (dedup — again nothing reserved).
// When prefetch is disabled it is a no-op returning (0, nil), which also skips
// dedup (the synchronous path already keeps one block in flight per peer). While
// blocked waiting for budget it increments blockPrefetchWaiters so the stall
// detector can tell self-backpressure apart from a genuinely stalled peer.
//
// The caller MUST hand blockHash back to ReleaseBlockPrefetch with the returned
// weight on success: the hash lives in the in-flight set for exactly the same
// lifetime as the reserved budget (inserted here, deleted on release), so the
// dedup half and the byte half of this admission gate never drift.
func (sm *SyncManager) AcquireBlockPrefetch(ctx context.Context, quit <-chan struct{}, blockHash chainhash.Hash, size int64) (int64, error) {
	if sm.blockPrefetchBudget == nil {
		return 0, nil
	}

	// Floor the weight so a flood of tiny blocks can't admit an unbounded number
	// of in-flight goroutines within the byte budget, then clamp to the budget so
	// an oversized block is admitted alone (and budgets smaller than the floor
	// still process one block at a time rather than deadlocking).
	weight := size
	if weight < minInFlightBlockWeight {
		weight = minInFlightBlockWeight
	}
	if weight > sm.blockPrefetchBudgetBytes {
		weight = sm.blockPrefetchBudgetBytes
	}

	// Dedup: reserve the hash BEFORE reserving budget. Inserting ahead of the
	// (possibly blocking) Acquire is deliberate — it bounds duplicates even while
	// a copy is parked waiting for budget, so N copies of one requested,
	// near-budget-sized block cannot each grab budget and fill it. A hash already
	// present is a duplicate: drop it (nothing reserved, nothing inserted).
	sm.inFlightBlocksMu.Lock()
	if _, dup := sm.inFlightBlocks[blockHash]; dup {
		sm.inFlightBlocksMu.Unlock()
		return 0, ErrDuplicateBlockInFlight
	}
	sm.inFlightBlocks[blockHash] = struct{}{}
	sm.inFlightBlocksMu.Unlock()

	// removeInFlight undoes the reservation above. It runs only when the budget
	// Acquire fails (ctx/quit cancel): nothing was reserved, so the hash must not
	// linger. On success the hash stays until ReleaseBlockPrefetch deletes it.
	removeInFlight := func() {
		sm.inFlightBlocksMu.Lock()
		delete(sm.inFlightBlocks, blockHash)
		sm.inFlightBlocksMu.Unlock()
	}

	// Fast path: budget available right now, no waiter accounting needed.
	if sm.blockPrefetchBudget.TryAcquire(weight) {
		return weight, nil
	}

	// Slow path: we must wait for in-flight blocks to drain. Flag that this
	// read-loop is backpressured by our own processing so the stall detector
	// does not mistake the resulting read stall for a slow peer.
	sm.blockPrefetchWaiters.Add(1)
	defer sm.blockPrefetchWaiters.Add(-1)

	// Abort the wait on peer teardown too, not just whole-process ctx cancellation:
	// the caller's ctx (the ServiceManager errgroup Init context) is cancelled on
	// daemon shutdown but not by legacy.Server.Stop() alone, while quit (the peer's
	// quit channel) closes on both individual disconnect and shutdown. This mirrors
	// awaitBlockResult so a budget-parked read-loop never outlives its peer. The
	// linking goroutine only exists while we are blocked (the rare backpressure
	// case) and exits as soon as the acquire resolves.
	if quit != nil {
		var cancel context.CancelFunc

		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		go func() {
			select {
			case <-quit:
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	if err := sm.blockPrefetchBudget.Acquire(ctx, weight); err != nil {
		// Nothing reserved: drop the hash we inserted before parking so a torn-down
		// or cancelled acquire never leaks a slot in the dedup set.
		removeInFlight()
		return 0, err
	}

	return weight, nil
}

// ReleaseBlockPrefetch returns budget reserved by AcquireBlockPrefetch and drops
// the block's hash from the in-flight dedup set. The two are released together
// (same lifetime as the reservation) so the dedup and byte halves of the
// admission gate never drift. A zero weight (nothing reserved) still deletes the
// hash but skips the budget Release; a nil budget (prefetch disabled) is a no-op.
// Only ever called for hashes that AcquireBlockPrefetch successfully admitted —
// the dup/early-return paths never reach here (OnBlock does not spawn
// awaitBlockResult for them), so no hash is deleted that was not first inserted.
func (sm *SyncManager) ReleaseBlockPrefetch(blockHash chainhash.Hash, weight int64) {
	if sm.blockPrefetchBudget == nil {
		return
	}

	sm.inFlightBlocksMu.Lock()
	delete(sm.inFlightBlocks, blockHash)
	sm.inFlightBlocksMu.Unlock()

	if weight <= 0 {
		return
	}
	sm.blockPrefetchBudget.Release(weight)
}

// noteBacklogProgress records that the block backlog just advanced — a block was
// enqueued to open a fresh backpressure window, or one finished processing. It
// must run on every backlog transition that constitutes progress: the 0->1
// enqueue and every completion decrement. localReadBackpressured treats a stamp
// older than blockProcessingStallTimeout as a hung pipeline rather than
// slow-but-progressing validation, so keeping this current is what lets the
// stall detector distinguish the two. Enqueues into an already-non-empty backlog
// deliberately do NOT call this (only completions advance processing).
func (sm *SyncManager) noteBacklogProgress() {
	sm.lastBacklogProgress.Store(time.Now().UnixNano())
}

// blockProcessingStallTimeout is how long a non-empty block backlog may go
// without advancing before localReadBackpressured stops suppressing the
// sync-peer stall check. It tracks settings.Legacy.PeerProcessingTimeout — the
// per-message watchdog that this progress-aware rule replaces for prefetched
// blocks — and falls back to defaultBlockProcessingStallTimeout when settings
// are absent (unit-test SyncManagers) or the value is unset.
func (sm *SyncManager) blockProcessingStallTimeout() time.Duration {
	if sm.settings != nil && sm.settings.Legacy.PeerProcessingTimeout > 0 {
		return sm.settings.Legacy.PeerProcessingTimeout
	}

	return defaultBlockProcessingStallTimeout
}

// localReadBackpressured reports whether the node is currently throttling its
// own network reads because local block processing cannot keep up. The stall
// detector skips its checks while this holds, since zero throughput then
// reflects our validation speed, not the sync peer's health. With prefetch
// enabled that is when read-loops are blocked acquiring budget; with prefetch
// disabled it is the original condition of any block queued or mid-validation.
// On the kill-switch path (prefetch disabled, budget nil) suppression stays
// UNCONDITIONAL, exactly as pre-prefetch: the per-message watchdog is still
// armed for blocks there and owns processing-stall liveness, so timeout-gating
// would rotate a healthy sync peer on a legitimately slow block. The
// progress-aware timeout applies only under prefetch, where that watchdog is
// disarmed for blocks and this is the compensating liveness signal.
// ReadBackpressured reports whether the node is currently throttling its own
// network reads because local block processing is behind. Exported for the
// peer layer's idle-timer gate: while WE are the reason no bytes flow, peers
// must not be executed for idleness (the 125s PeerIdleTimeout was the largest
// disconnect class in the measured rotation cascades). Atomics only —
// safe from any goroutine.
func (sm *SyncManager) ReadBackpressured() bool {
	return sm.localReadBackpressured()
}

func (sm *SyncManager) localReadBackpressured() bool {
	// A non-empty local backlog means blocks are queued or mid-validation, so a
	// stale last-block-time and zero throughput normally reflect our own
	// validation speed, not the sync peer's health. Suppress the stall check —
	// but only while the backlog is still ADVANCING. Disarming the per-message
	// watchdog for prefetched blocks removed the only timeout over the processing
	// phase; if we suppressed on any non-zero backlog, a genuine hang
	// (store/validator deadlock, Aerospike overload) would leave the backlog
	// pinned >=1 forever and the node would silently stop syncing with no
	// rotation. So a backlog that has not advanced for longer than
	// blockProcessingStallTimeout is treated as a stalled pipeline, not
	// slow-but-progressing validation: stop suppressing so handleCheckSyncPeer
	// logs and rotates — restoring the pre-prefetch liveness signal without the
	// false rotation of a merely-slow block that motivated disarming the
	// watchdog. Deliberately do NOT fall through to the waiter check when the
	// backlog is stale: a hung pipeline with a full budget accumulates waiters,
	// and we WANT rotation then.
	if sm.blockBacklog.Load() > 0 {
		// Kill switch (prefetch disabled, budget nil): the per-message processing
		// watchdog is still armed for blocks and owns processing-stall liveness,
		// exactly as pre-prefetch. Keep the original UNCONDITIONAL suppression here —
		// timeout-gating would rotate a healthy sync peer on a legitimately slow
		// block, churn the "proven synchronous" path never had. The progress-aware
		// timeout below applies only under prefetch, where the watchdog is disarmed
		// for blocks and this is the compensating liveness signal.
		if sm.blockPrefetchBudget == nil {
			return true
		}

		return time.Since(time.Unix(0, sm.lastBacklogProgress.Load())) < sm.blockProcessingStallTimeout()
	}

	// Under prefetch also suppress while a read-loop is parked in
	// AcquireBlockPrefetch waiting for budget. In the running system that implies
	// a backlog too, but the explicit waiter signal keeps the accounting clear
	// (and unit-testable in isolation).
	return sm.blockPrefetchBudget != nil && sm.blockPrefetchWaiters.Load() > 0
}

// sendDuringShutdown delivers v on ch, recovering from the "send on closed
// channel" panic that races teardown. Inv delivery runs on peer read-loop
// goroutines (OnInv -> QueueInv), but the channels they target are torn down by
// a different goroutine during shutdown: the kafka async producer closes
// legacyKafkaInvCh in its Stop(), and the block handler stops draining msgChan.
// The shutdown flag check in QueueInv narrows but cannot close that window — a
// flag check and a channel send are not atomic against a concurrent close — so
// a late inv would otherwise crash the whole process. Dropping an inv during
// shutdown is safe: inv is an advisory announcement, re-sent by the peer (or a
// later session) on the next connection. Returns false if the channel was closed.
func sendDuringShutdown[T any](ch chan T, v T) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()

	ch <- v

	return true
}

// QueueInv adds the passed inv message and peer to the block handling queue.
func (sm *SyncManager) QueueInv(inv *wire.MsgInv, peer *peerpkg.Peer) {
	// No channel handling here because peers do not need to block on inv
	// messages.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	// write all tx inv messages to Kafka and read from there
	// this allows us to stop reading in certain cases, but still have the inv messages to catch up on
	if sm.legacyKafkaInvCh != nil {
		// split inv message to transactions and blocks
		invBlockMsg := wire.NewMsgInv()
		invTxMsg := wire.NewMsgInv()

		for _, invVect := range inv.InvList {
			if invVect.Type == wire.InvTypeBlock {
				if err := invBlockMsg.AddInvVect(invVect); err != nil {
					sm.logger.Errorf("failed to add inv vector to inv block message: %v", err)
					continue
				}
			} else {
				if err := invTxMsg.AddInvVect(invVect); err != nil {
					sm.logger.Errorf("failed to add inv vector to inv tx message: %v", err)
					continue
				}
			}
		}

		if len(invBlockMsg.InvList) > 0 {
			netsyncInvMsg := invMsg{inv: invBlockMsg, peer: peer}
			sendDuringShutdown[interface{}](sm.msgChan, &netsyncInvMsg)
		}

		if len(invTxMsg.InvList) > 0 {
			msg := sm.newKafkaMessageFromInv(invTxMsg, peer)

			value, err := proto.Marshal(msg)
			if err != nil {
				sm.logger.Errorf("failed to marshal kafka inv topic message: %v", err)
				return
			}

			// write to Kafka
			sm.logger.Debugf("writing INV message to Kafka from peer %s, length: %d", peer.String(), len(value))
			sendDuringShutdown(sm.legacyKafkaInvCh, &kafka.Message{
				Value: value,
			})
		}
	} else {
		netsyncInvMsg := invMsg{inv: inv, peer: peer}
		sendDuringShutdown[interface{}](sm.msgChan, &netsyncInvMsg)
	}
}

// QueueHeaders adds the passed headers message and peer to the block handling
// queue.
func (sm *SyncManager) QueueHeaders(headers *wire.MsgHeaders, peer *peerpkg.Peer) {
	// No channel handling here because peers do not need to block on
	// headers messages.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	sm.msgChan <- &headersMsg{headers: headers, peer: peer}
}

// DonePeer informs the blockmanager that a peer has disconnected.
func (sm *SyncManager) DonePeer(peer *peerpkg.Peer, done chan struct{}) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		if done != nil {
			done <- struct{}{}
		}
		return
	}

	sm.logger.Infof("Done peer %s", peer)
	sm.msgChan <- &donePeerMsg{peer: peer, reply: done}
}

// blockAssemblyGateAdmitsCached is the non-blocking form of the maturity gate. It
// returns (admit, evaluable): `evaluable` is true only when the cached fast path
// applies (positive maxBehind, below-checkpoint, cache polled and non-overflow);
// when evaluable, `admit` is the exact coinbase-maturity inequality used by the
// blocking fast path (cached+maxBehind >= blockHeight over the stale-LOW cache),
// so it can never wrongly admit. When !evaluable the caller must fall back to the
// blocking wait. This is the single source of truth for the gate predicate;
// waitForBlockAssemblyReadyCached calls it so the two can never drift.
func (sm *SyncManager) blockAssemblyGateAdmitsCached(blockHeight uint32) (admit, evaluable bool) {
	maxBehind := sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly
	if maxBehind > 0 && sm.chainParams != nil && model.BelowCheckpoint(sm.chainParams.Checkpoints, blockHeight) {
		// baHeightPolled (not cached > 0) decides trustworthiness: a genuine
		// height of 0 on a fresh node must arm the gate, or far-ahead blocks
		// fall into the blocking wait and wedge the drain at genesis+maxBehind.
		if cached := sm.cachedBlockAssemblyHeight.Load(); sm.baHeightPolled.Load() && cached <= math.MaxUint32-uint32(maxBehind) {
			return cached+uint32(maxBehind) >= blockHeight, true
		}
	}

	return false, false
}

// waitForBlockAssemblyReadyCached enforces the coinbase-maturity back-pressure
// bound (block-assembly must be within MaxBlocksBehindBlockAssembly of blockHeight)
// without paying a per-block gRPC round-trip on the serial drain path in the
// common case.
//
// FAST PATH: engaged ONLY for blocks at or below the highest hardcoded checkpoint
// (model.BelowCheckpoint). In that certified prefix the chain is pinned — no reorg
// can occur — so block-assembly height is monotonic and the cached height
// (refreshed in the background by blockAssemblyHeightPoller) is a stale-LOW-or-equal
// lower bound on the true height. If that lower bound already satisfies
// cached+maxBehind >= blockHeight, the true (>=) height satisfies it too, so the
// fast path can never wrongly pass, and it returns nil with no gRPC.
//
// SLOW PATH: taken for every block ABOVE the checkpoint (reorg-possible, where a
// reorg could LOWER block-assembly height and leave a stale-HIGH cache), and for a
// below-checkpoint block whose cache is unpolled (0), overflowing, or at/near the
// bound. It falls through to the real fresh-gRPC retry loop, whose behaviour (and
// overflow guard) is unchanged. Restricting the fast path to below the checkpoint
// makes correctness independent of MaxBlocksBehindBlockAssembly staying below the
// coinbase-maturity window: above the checkpoint the fresh gRPC is always used.
func (sm *SyncManager) waitForBlockAssemblyReadyCached(ctx context.Context, blockHeight uint32) error {
	maxBehind := sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly

	// Fast/cache path only below the highest hardcoded checkpoint (reorg-safe) and
	// only when maxBehind is a valid positive window; a non-positive value or an
	// above-checkpoint block defers entirely to the slow path, which validates and
	// guards the bound with a fresh gRPC read.
	//
	// Below the checkpoint the cached height (refreshed every
	// blockAssemblyHeightPollInterval by blockAssemblyHeightPoller) is a
	// stale-LOW-or-equal lower bound on the true block-assembly height: the chain
	// is pinned so the true height is monotonic and can only be >= cached. So if
	// cached+maxBehind >= blockHeight, the true height also satisfies the bound and
	// the maturity guarantee is preserved without any gRPC.
	if admit, evaluable := sm.blockAssemblyGateAdmitsCached(blockHeight); evaluable {
		if admit {
			return nil
		}

		// Cache is usable (polled, non-overflow) but behind the bound. Instead of
		// the coarse exponential-backoff fresh-gRPC wait (20ms,80ms,...,5s steps
		// that leave the parallel window idle in ~5s bursts), re-check the
		// poller-refreshed cache at a short fixed interval so the window
		// re-engages within one interval of block assembly advancing — a smooth,
		// steady release at block-assembly's rate. No gRPC in this loop; the
		// background poller does that. The loop is bounded by windowMaturityMaxWait
		// so a genuine block-assembly stall (cache never advances) is still
		// detected and escalated with an error, preserving the stall-detection
		// semantics of the old 100-retry exponential path.
		return sm.waitForBlockAssemblyCachePoll(ctx, blockHeight, maxBehind)
	}

	// Slow path: unchanged fresh-gRPC wait (retry loop + overflow guard). Taken for
	// above-checkpoint blocks, a non-positive window, or a below-checkpoint block
	// whose cache is unpolled (0) or would overflow the uint32 addition.
	return blockassemblyutil.WaitForBlockAssemblyReady(ctx, sm.logger, sm.blockAssembly, blockHeight, maxBehind)
}

// waitForBlockAssemblyCachePoll re-checks the poller-refreshed cached
// block-assembly height at windowMaturityRecheckInterval until it satisfies the
// coinbase-maturity bound (cached+maxBehind >= blockHeight), returning nil as
// soon as it does. It does NO gRPC — the background blockAssemblyHeightPoller
// keeps the cache fresh — so the parallel window re-engages within one interval
// of block assembly advancing rather than on the old exponential backoff.
//
// The loop is bounded by windowMaturityMaxWait: on expiry it returns an error so
// the caller's existing recover/escalation path fires (a genuine block-assembly
// stall is never masked as forever-waiting). It also returns promptly on ctx
// cancellation or sm.quit (shutdown). The maturity guarantee is unchanged: the
// gate condition is identical to the fast path, only the re-check cadence differs
// (fixed-interval cache read instead of exponential fresh gRPC); the stale-LOW
// cache makes a pass imply the true height also clears the bound.
func (sm *SyncManager) waitForBlockAssemblyCachePoll(ctx context.Context, blockHeight uint32, maxBehind int) error {
	deadline := time.NewTimer(windowMaturityMaxWait)
	defer deadline.Stop()

	ticker := time.NewTicker(windowMaturityRecheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.quit:
			return errors.NewProcessingError("[waitForBlockAssemblyCachePoll] shutting down while waiting for block assembly to reach block height %d (cached height %d)", blockHeight, sm.cachedBlockAssemblyHeight.Load())
		case <-ctx.Done():
			return errors.NewProcessingError("[waitForBlockAssemblyCachePoll] context cancelled while waiting for block assembly to reach block height %d (cached height %d)", blockHeight, sm.cachedBlockAssemblyHeight.Load(), ctx.Err())
		case <-deadline.C:
			// Bounded escalation: block assembly did not advance within the max wait,
			// so treat it as a stall and surface an error to the caller's recovery
			// path, matching the old exponential path's retry-exhaustion behaviour.
			return errors.NewProcessingError("[waitForBlockAssemblyCachePoll] block assembly is behind, block height %d, cached block assembly height %d, gave up after %s", blockHeight, sm.cachedBlockAssemblyHeight.Load(), windowMaturityMaxWait)
		case <-ticker.C:
			cached := sm.cachedBlockAssemblyHeight.Load()
			// Re-apply the same overflow-guarded bound as the fast path; the cache is
			// monotonic below the checkpoint so it stays usable once it was. Gated on
			// the poller having reported (not cached > 0): a genuine height of 0 on a
			// fresh node satisfies the bound for early blocks and must release them.
			if sm.baHeightPolled.Load() && cached <= math.MaxUint32-uint32(maxBehind) && cached+uint32(maxBehind) >= blockHeight {
				return nil
			}
		}
	}
}

// blockAssemblyHeightPoller periodically refreshes cachedBlockAssemblyHeight
// from the block-assembly service so waitForBlockAssemblyReadyCached can serve
// the per-block maturity check as an atomic read. It runs until sm.quit closes
// or ctx is cancelled. On a poll error it logs a single line and keeps the last
// cached value (never zeroes it). A nil blockAssembly (test setups) is handled
// by the caller not starting the poller.
func (sm *SyncManager) blockAssemblyHeightPoller(ctx context.Context) {
	ticker := time.NewTicker(blockAssemblyHeightPollInterval)
	defer ticker.Stop()

	poll := func() {
		state, err := sm.blockAssembly.GetBlockAssemblyState(ctx)
		if err != nil {
			sm.logger.Warnf("[blockAssemblyHeightPoller] failed to get block assembly state, keeping last cached height %d: %v", sm.cachedBlockAssemblyHeight.Load(), err)
			return
		}

		sm.cachedBlockAssemblyHeight.Store(state.CurrentHeight)
		// Order matters for the fresh-node case: publish the height before
		// declaring the cache trustworthy, so a reader that sees the flag also
		// sees a real (possibly zero) height, never the unpolled zero.
		sm.baHeightPolled.Store(true)
	}

	// Prime the cache immediately so the fast path is armed without waiting a
	// full interval after start.
	poll()

	for {
		select {
		case <-sm.quit:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

// Start begins the core block handler which processes block and inv messages.
func (sm *SyncManager) Start() {
	// Already started?
	if atomic.AddInt32(&sm.started, 1) != 1 {
		return
	}

	sm.logger.Infof("Starting sync manager")

	// Start the background block-assembly height poller alongside the drain
	// goroutine so the per-block maturity check reads a cached height instead
	// of doing a gRPC round-trip. Skip when there is no block-assembly client
	// (test setups); the cached check then always takes its slow path, which
	// no-ops on a nil client exactly as before.
	if sm.blockAssembly != nil {
		go sm.blockAssemblyHeightPoller(sm.ctx)
	}

	go sm.blockHandler()
}

// Stop gracefully shuts down the sync manager by stopping all asynchronous
// handlers and waiting for them to finish.
func (sm *SyncManager) Stop() error {
	if atomic.AddInt32(&sm.shutdown, 1) != 1 {
		sm.logger.Warnf("Sync manager is already in the process of " +
			"shutting down")
		return nil
	}

	sm.logger.Infof("Sync manager shutting down")
	close(sm.quit)
	<-sm.handlerDone

	sm.orphanTxs.Stop()
	sm.requestedTxns.Stop()
	sm.requestedBlocks.Stop()

	// DC15 / review C1: quiesce Put then drain the tx-announce batcher before
	// tearing down transports.
	sm.closeTxAnnounceBatcher()

	// DC11: stop the legacy INV async producer so its final flush runs during
	// shutdown. Safe here — handlerDone above guarantees no more sends to
	// legacyKafkaInvCh, which producer.Stop() closes. Stop() has no caller ctx to
	// honour (Stop() takes none), so it is raced against an internal timeout: a
	// wedged broker flush can't block shutdown, and the outstanding Stop() finishes
	// the flush later if it can.
	if sm.legacyKafkaInvProducer != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), util.DefaultBatcherDrainTimeout)
		kafka.StopProducerCtx(stopCtx, sm.logger, "legacy INV", sm.legacyKafkaInvProducer)
		cancel()
	}

	return nil
}

// announceTx queues a transaction for peer announcement via the tx-announce
// batcher, unless the batcher has been closed by closeTxAnnounceBatcher during
// shutdown. go-batcher v2.0.4 panics on Put-after-Close, and this is called from
// the txmeta Kafka listener goroutine (not joined by Stop), so the read lock
// pairs with the write lock in closeTxAnnounceBatcher to make a post-close Put a
// safe no-op.
func (sm *SyncManager) announceTx(item *TxHashAndFee) {
	sm.txAnnounceMu.RLock()
	defer sm.txAnnounceMu.RUnlock()

	if !sm.txAnnounceClosed && sm.txAnnounceBatcher != nil {
		sm.txAnnounceBatcher.Put(item)
	}
}

// closeTxAnnounceBatcher marks the tx-announce batcher closed (so further
// announceTx calls become no-ops) and then drains it under a bounded timeout.
// Taking the write lock first waits for any in-flight announceTx (holding the
// read lock) to finish, so no Put can race the drain. Idempotent.
func (sm *SyncManager) closeTxAnnounceBatcher() {
	sm.txAnnounceMu.Lock()
	alreadyClosed := sm.txAnnounceClosed
	sm.txAnnounceClosed = true
	sm.txAnnounceMu.Unlock()

	if alreadyClosed || sm.txAnnounceBatcher == nil {
		return
	}

	util.DrainBatcher(sm.logger, "netsync_tx_announce", util.DefaultBatcherDrainTimeout, sm.txAnnounceBatcher.Close)
}

// SyncPeerID returns the ID of the current sync peer, or 0 if there is none.
func (sm *SyncManager) SyncPeerID() int32 {
	reply := make(chan int32)
	sm.msgChan <- getSyncPeerMsg{reply: reply}

	return <-reply
}

// IsCurrent returns whether the sync manager believes it is synced with
// the connected peers.
func (sm *SyncManager) IsCurrent() bool {
	return sm.current()
}

// Pause pauses the sync manager until the returned channel is closed.
//
// Note that while paused, all peer and block processing is halted.  The
// message sender should avoid pausing the sync manager for long durations.
func (sm *SyncManager) Pause() chan<- struct{} {
	c := make(chan struct{})
	sm.msgChan <- pauseMsg{c}

	return c
}

// New constructs a new SyncManager. Use Start to begin processing asynchronous
// block, tx, and inv updates.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient teranodeblockchain.ClientI,
	validationClient validator.Interface, utxoStore utxostore.Store, subtreeStore blob.Store,
	subtreeValidation subtreevalidation.Interface, blockValidation blockvalidation.Interface,
	blockAssembly blockassembly.ClientI, config *Config) (*SyncManager, error) {
	initPrometheusMetrics()

	sm := SyncManager{
		ctx:          ctx,
		settings:     tSettings,
		peerNotifier: config.PeerNotifier,
		// txMemPool:     config.TxMemPool,
		orphanTxs:       expiringmap.New[chainhash.Hash, *orphanTxAndParents](tSettings.Legacy.OrphanEvictionDuration).WithMaxSize(tSettings.Legacy.MaxOrphanTxs),
		chainParams:     config.ChainParams,
		rejectedTxns:    txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns), // limit map size to maxRejectedTxns
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),   // give peers 10 seconds to respond
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](60 * time.Second),   // give peers 60 seconds to respond
		peerStates:      txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		// Hash-keyed ownership ledger for blocks between window admission and
		// commit (see the field doc): prevents the parked-twin double-commit.
		windowOwnedBlocks: txmap.NewSyncedMap[chainhash.Hash, uint32](),
		// progressLogger:  newBlockProgressLogger("Processed", log),
		msgChan:           make(chan interface{}, maxMsgQueueSize),
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
		assignedTo:        make(map[chainhash.Hash]*peerpkg.Peer),
		assignedAt:        make(map[chainhash.Hash]time.Time),
		refetchBlocks:     make(map[chainhash.Hash]struct{}),
		blockSizeTracker: newBlockSizeTrackerWithBudgets(10, // track last 10 blocks for rolling average
			tSettings.Legacy.InFlightTxBudget, tSettings.Legacy.InFlightByteBudget),
		quit: make(chan struct{}),
		// feeEstimator:            config.FeeEstimator,
		minSyncPeerNetworkSpeed: config.MinSyncPeerNetworkSpeed,
		handlerDone:             make(chan struct{}),
		// teranode stores etc.
		logger:            logger,
		blockchainClient:  blockchainClient,
		validationClient:  validationClient,
		utxoStore:         utxoStore,
		subtreeStore:      subtreeStore,
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
		blockAssembly:     blockAssembly,
	}

	// The fail-closed inline lever is a no-op unless the outpoint-only below-checkpoint
	// path is also enabled (legacyFailClosed depends on legacyOutpointOnly). Warn so an
	// operator A/B-testing the new flag alone is not silently getting nothing.
	if tSettings.BlockValidation.LegacyBelowCheckpointFailClosed && !tSettings.BlockValidation.OutpointOnlyBelowCheckpoint {
		logger.Warnf("[netsync] blockvalidation_legacy_below_checkpoint_fail_closed is set but has no effect without blockvalidation_outpoint_only_below_checkpoint")
	}

	// Bounded async block prefetch: with a positive budget OnBlock admits a
	// block against this global byte-weighted semaphore and returns, so the
	// read-loop downloads the next block while the current one is validated.
	// The budget caps the total serialized bytes of in-flight blocks; a budget
	// of 0 disables prefetch entirely (synchronous, one-block-in-flight).
	if budget := tSettings.Legacy.BlockPrefetchBufferBytes; budget > 0 {
		sm.blockPrefetchBudgetBytes = budget
		sm.blockPrefetchBudget = semaphore.NewWeighted(budget)
		// Dedup half of the same admission gate as the budget semaphore, created
		// in lockstep with it: paired 1:1 with each budget reservation so at most
		// one copy of a block hash is ever admitted/queued at a time.
		sm.inFlightBlocks = make(map[chainhash.Hash]struct{})
	}

	// The fail-closed inline lever is a no-op unless the outpoint-only below-checkpoint
	// path is also enabled (legacyFailClosed depends on legacyOutpointOnly). Warn so an
	// operator A/B-testing the new flag alone is not silently getting nothing.
	if tSettings.BlockValidation.LegacyBelowCheckpointFailClosed && !tSettings.BlockValidation.OutpointOnlyBelowCheckpoint {
		logger.Warnf("[netsync] blockvalidation_legacy_below_checkpoint_fail_closed is set but has no effect without blockvalidation_outpoint_only_below_checkpoint")
	}

	// create the transaction announcement batcher
	sm.txAnnounceBatcher = batcher.NewWithDeduplicationAndPool[TxHashAndFee](maxRequestedTxns, 1*time.Second, func(batch []*TxHashAndFee) {
		sm.logger.Debugf("announcing %d transactions to peers", len(batch))

		// process the batch
		sm.peerNotifier.AnnounceNewTransactions(batch)
	}, true,
		batcher.WithName("netsync_tx_announce"),
		batcher.WithLogger(logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithTracer(tracing.Tracer("SyncManager").OTelTracer()),
	)

	// set an eviction function for orphan transactions
	// this will be called when an orphan transaction is evicted from the map
	sm.orphanTxs.WithEvictionFunction(func(txHash chainhash.Hash, orphanTx *orphanTxAndParents) bool {
		// try to process one last time
		// passing in block height 0, which will default to utxo store block height in validator
		if _, err := sm.validationClient.Validate(sm.ctx, orphanTx.tx, 0); err != nil {
			sm.logger.Debugf("failed to validate orphan transaction when evicting %v: %v", txHash, err)
		} else {
			sm.logger.Debugf("evicted orphan transaction %v", txHash)
		}

		return true
	})

	// add the number of orphan transactions to the prometheus metric
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-sm.quit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// update the number of orphan transactions
				prometheusLegacyNetsyncOrphans.Set(float64(sm.orphanTxs.Len()))
			}
		}
	}()

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	sm.checkpointsDisabled = config.DisableCheckpoints

	if !config.DisableCheckpoints {
		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf("failed to convert block height to int32: %v", err)
		}

		// Initialize the next checkpoint based on the current height. Both the
		// block-level tracker and the header-request look-ahead cursor start
		// aligned; resetHeaderState re-affirms the alignment. These writes run in
		// New(), before Start() launches blockHandler, so they are single-threaded
		// startup and need no headerMu (nothing else can access the fields yet).
		sm.nextCheckpoint = sm.findNextHeaderCheckpoint(bestBlockHeightInt32)
		sm.headerCheckpoint = sm.nextCheckpoint

		if sm.nextCheckpoint != nil {
			sm.resetHeaderState(bestBlockHeader.Hash(), bestBlockHeightInt32)
		}
	} else {
		sm.logger.Infof("Checkpoints are disabled")
	}

	sm.startKafkaListeners(ctx, err)

	return &sm, nil
}

func (sm *SyncManager) startKafkaListeners(ctx context.Context, _ error) {
	blockControlChan := make(chan bool, 1) // control channel for block-related listeners (buffered to prevent blocking)
	txControlChan := make(chan bool, 1)    // control channel for transaction-related listeners (buffered to prevent blocking)

	// start a go routine to control the kafka listeners based on FSM state
	// Block-related listeners (INV, blocks final): always enabled
	// Transaction-related listeners (txmeta): enabled only when in RUNNING state
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				// Block-related listeners are always enabled. The only FSM state
				// that previously disabled them (legacy sync mode) was removed; no
				// automated path ever entered it — an operator could only reach it
				// manually via the setfsmstate CLI / FSM admin endpoint.
				blockEnabled := true

				// Non-blocking send to avoid deadlock if no one is reading
				select {
				case blockControlChan <- blockEnabled:
				default:
				}

				// Transaction-related listeners: enable only when RUNNING
				isRunning, _ := sm.blockchainClient.IsFSMCurrentState(sm.ctx, teranodeblockchain.FSMStateRUNNING)

				// Non-blocking send to avoid deadlock if no one is reading
				select {
				case txControlChan <- isRunning:
				default:
				}
			}
		}
	}()

	var blockListenersCh []chan bool // channels for block-related listeners
	var txListenersCh []chan bool    // channels for tx-related listeners

	// Kafka for INV messages (responds to requests from other nodes)
	legacyInvConfigURL := sm.settings.Kafka.LegacyInvConfig
	if legacyInvConfigURL != nil {
		sm.legacyKafkaInvCh = make(chan *kafka.Message, 10_000)

		producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, sm.logger, legacyInvConfigURL, &sm.settings.Kafka)
		if err != nil {
			sm.logger.Errorf("[Legacy Manager] error starting kafka producer: %v", err)
			return
		}

		// Retain the producer (DC11) so SyncManager.Stop() can flush it synchronously.
		sm.legacyKafkaInvProducer = producer

		// start a go routine to start the kafka producer
		go func() {
			producer.Start(sm.ctx, sm.legacyKafkaInvCh)
		}()

		// INV listener receives inventory messages from other nodes
		controlCh := make(chan bool)
		blockListenersCh = append(blockListenersCh, controlCh)

		go kafka.StartKafkaControlledListener(ctx, sm.logger, "inv.legacy"+"."+sm.settings.ClientName, controlCh, legacyInvConfigURL, sm.kafkaINVListener)
	}

	// Kafka for blocks final messages (announces blocks to peers)
	blocksFinalConfigURL := sm.settings.Kafka.BlocksFinalConfig
	if blocksFinalConfigURL != nil {
		controlCh := make(chan bool)
		blockListenersCh = append(blockListenersCh, controlCh)

		go kafka.StartKafkaControlledListener(ctx, sm.logger, "blocksfinal.legacy"+"."+sm.settings.ClientName, controlCh, blocksFinalConfigURL, sm.kafkaBlocksFinalListener)
	}

	// Kafka for txmeta messages (announces transactions to peers)
	txmetaKafkaURL := sm.settings.Kafka.TxMetaConfig

	if txmetaKafkaURL != nil {
		controlCh := make(chan bool)
		txListenersCh = append(txListenersCh, controlCh)

		// disable replay for txmeta in the legacy service, we do not have to replay anything, ever
		values := txmetaKafkaURL.Query()
		values.Set("replay", "0")

		txmetaKafkaURL.RawQuery = values.Encode()

		go kafka.StartKafkaControlledListener(ctx, sm.logger, "txmeta.legacy"+"."+sm.settings.ClientName, controlCh, txmetaKafkaURL, sm.kafkaTXmetaListener)
	}

	// Tx announcements to legacy peers are handled entirely by the txmeta Kafka path.
	// Subtree notifications are NOT used for tx announcements — they caused all txs in
	// reorganized subtrees to be re-announced to peers after every new block.

	// Control block listeners based on blockControlChan
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case control := <-blockControlChan:
				for _, ch := range blockListenersCh {
					ch <- control
				}
			}
		}
	}()

	// Control transaction listeners based on txControlChan
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case control := <-txControlChan:
				for _, ch := range txListenersCh {
					ch <- control
				}
			}
		}
	}()
}

func (sm *SyncManager) kafkaINVListener(ctx context.Context, kafkaURL *url.URL, groupID string) {
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		var message kafkamessage.KafkaInvTopicMessage

		err := proto.Unmarshal(msg.Value, &message)
		if err != nil {
			sm.logger.Errorf("[kafkaINVListener] failed to unmarshal kafka inv topic message: %v", err)
			return nil // ignore any errors, the message might be old and/or the peer is already disconnected
		}

		invMsg, err := sm.newInvFromKafkaMessage(&message)
		if err != nil {
			sm.logger.Errorf("[kafkaINVListener] failed to create inv msg from kafka message: %v", err)
			return nil
		}

		sm.logger.Debugf("[kafkaINVListener] Received INV message from Kafka from peer %s", message.PeerAddress)

		// Process the INV message directly, requesting data from other nodes will be queued on the outputQueue
		go sm.handleInvMsg(invMsg)

		return nil
	}, &sm.settings.Kafka)
}

func (sm *SyncManager) kafkaBlocksFinalListener(ctx context.Context, kafkaURL *url.URL, groupID string) {
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		if msg.Key == nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener] no Kafka message key specified, skipping message")
			// not going to retry, if we don't have a key/hash
			return nil
		}

		hash, err := chainhash.NewHashFromStr(string(msg.Key))
		if err != nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener][%s] failed to create hash from Kafka message key: %v", hash, err)
			// not going to retry, if we cannot parse the message
			return nil
		}

		var blockMsg kafkamessage.KafkaBlocksFinalTopicMessage
		if err := proto.Unmarshal(msg.Value, &blockMsg); err != nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener][%s] failed to unmarshal kafka block topic message: %v", hash, err)
			// not going to retry, if we cannot parse the message
			return nil
		}

		header, err := model.NewBlockHeaderFromBytes(blockMsg.Header)
		if err != nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener][%s] failed to create block header from Kafka message: %v", hash, err)
			// not going to retry, if we cannot parse the message
			return nil
		}

		// create wireBlockHeader
		wireBlockHeader := header.ToWireBlockHeader()

		sm.logger.Infof("[kafkaBlocksFinalListener] received block final message from Kafka: %s, %s", hash, header.String())
		sm.peerNotifier.RelayInventory(wire.NewInvVect(wire.InvTypeBlock, hash), wireBlockHeader)

		return nil
	}, &sm.settings.Kafka)
}

// kafkaTXmetaListener processes TxMeta Kafka messages in binary batch format.
// Messages use a binary batch format:
// [4 bytes]  - entry count (uint32, little-endian)
// For each entry:
//
//	[32 bytes] - tx hash (raw bytes)
//	[1 byte]   - action (0=ADD, 1=DELETE)
//	[4 bytes]  - content length (uint32, little-endian) - 0 for DELETE
//	[N bytes]  - content (metaBytes) - only for ADD
func (sm *SyncManager) kafkaTXmetaListener(ctx context.Context, kafkaURL *url.URL, groupID string) {
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		return sm.processTXmetaBatchMessage(msg.Value)
	}, &sm.settings.Kafka)
}

// processTXmetaBatchMessage processes a binary batch message from the txmeta Kafka topic.
// It parses the batch format, deserializes metadata for ADD entries, and announces
// non-coinbase transactions to peers via the txAnnounceBatcher.
// Coinbase transactions are intentionally skipped to avoid peer bans.
//
// Two wire formats are accepted, distinguished by a multi-byte signature at
// the start of the message (mirrors services/subtreevalidation/txmetaHandler.go):
//
//	v1 (legacy)
//	  [4 bytes] entry count (uint32 LE)
//	  per entry: [32 hash][1 action][4 contentLen][N content]
//
//	v2 (partition-aware)
//	  [1 byte magic=0xFF][1 byte version=0x02][2 reserved=0][4 entry count LE]
//	  per entry: [8 xxhash][32 hash][1 action][4 contentLen][N content]
//
// v2 detection requires the full 4-byte header signature AND a plausible
// entry count for the buffer length, otherwise the message is parsed as v1.
// This avoids misclassifying v1 messages whose entry count happens to begin
// with 0xFF (counts 255, 511, 767, ...).
//
// The xxhash prefix in v2 is read and discarded — netsync only needs the
// 32-byte tx hash to announce; partition-aligned cache writes are a
// subtreevalidation concern.
func (sm *SyncManager) processTXmetaBatchMessage(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	var (
		offset     int
		entryCount uint32
		isV2       bool
	)

	// Speculative v2 detection: require the full header signature
	// (magic + version + reserved bytes) and an entry count that fits in the
	// remaining buffer at the minimum v2 entry size. Any failure falls
	// through to v1 — never silently drops a valid v1 message.
	if len(data) >= txmetacache.WireV2HeaderLen &&
		data[0] == txmetacache.WireV2Magic &&
		data[1] == txmetacache.WireV2Version &&
		data[2] == 0 && data[3] == 0 {
		candidateCount := binary.LittleEndian.Uint32(data[4:])
		remaining := uint64(len(data) - txmetacache.WireV2HeaderLen)
		if uint64(candidateCount)*uint64(txmetacache.WireV2MinEntrySize) <= remaining {
			entryCount = candidateCount
			offset = txmetacache.WireV2HeaderLen
			isV2 = true
		}
	}

	if !isV2 {
		entryCount = binary.LittleEndian.Uint32(data[:4])
		offset = 4
	}

	// Per-entry header size (excluding content). The shared constants in
	// stores/txmetacache encode the same numbers; using them here keeps
	// the producer and the receiver pinned to one source of truth.
	entryHeaderSize := txmetacache.WireV1MinEntrySize
	if isV2 {
		entryHeaderSize = txmetacache.WireV2MinEntrySize
	}

	// Process each entry
	for i := uint32(0); i < entryCount; i++ {
		if offset+entryHeaderSize > len(data) {
			sm.logger.Errorf("[kafkaTXmetaListener] truncated message at entry %d", i)
			return nil
		}

		// v2: skip the 8-byte xxhash prefix; netsync doesn't use it.
		if isV2 {
			offset += 8
		}

		// Read hash (32 bytes)
		var hash chainhash.Hash
		copy(hash[:], data[offset:offset+32])
		offset += 32

		// Read action (1 byte)
		action := data[offset]
		offset++

		// Read content length (4 bytes)
		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if action == txmetacache.WireActionADD {
			// Handle ADD
			if offset+int(contentLen) > len(data) {
				sm.logger.Errorf("[kafkaTXmetaListener] truncated content at entry %d", i)
				return nil
			}

			content := data[offset : offset+int(contentLen)]
			offset += int(contentLen)

			sm.logger.Debugf("Received tx message from Kafka: %v", hash)

			var txMeta meta.Data
			if err := meta.NewMetaDataFromBytes(content, &txMeta); err != nil {
				sm.logger.Errorf("Failed to create tx meta data from bytes: %v", err)
				continue
			}

			if txMeta.IsCoinbase {
				continue
			}

			// Never announce transactions that arrived as part of a block or
			// announced subtree. The txmeta topic also carries those (block
			// validation, subtree validation, legacy sync pre-warm) to populate
			// the subtree-validation cache; relaying them as fresh mempool txs
			// floods peers with getdata for transactions that are long mined —
			// and often already pruned.
			if txMeta.InBlock {
				continue
			}

			sm.announceTx(&TxHashAndFee{
				TxHash: hash,
				Fee:    txMeta.Fee,
				Size:   txMeta.SizeInBytes,
			})
		} else {
			offset += int(contentLen)
			continue
		}
	}

	return nil
}
