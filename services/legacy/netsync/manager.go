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
	"google.golang.org/protobuf/proto"
)

const (
	// defaultMaxInFlightBlocks is the default maximum number of blocks that
	// should be in the request queue for headers-first mode. This is the
	// starting value for small blocks, and will be dynamically adjusted down
	// based on observed block sizes to avoid memory issues with large blocks.
	defaultMaxInFlightBlocks = 20

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
	legacyKafkaInvCh          chan *kafka.Message
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
	assignedMu    sync.Mutex
	assignedTo    map[chainhash.Hash]*peerpkg.Peer
	assignedAt    map[chainhash.Hash]time.Time
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
	// path. Entries are removed on successful re-send or on receipt. Guarded by
	// assignedMu (same leaf lock as assignedTo/assignedAt); bounded by the total
	// in-flight cap, so it cannot grow unbounded.
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

	// While blocks are queued or mid-validation locally, OnBlock deliberately
	// stops reading from the peer (it blocks on blockProcessed), so zero
	// throughput and a stale last-block-time measure our own validation speed,
	// not the peer's health. Skip stall checks until the backlog drains — a
	// genuinely stalled peer keeps failing them afterwards. The deferred
	// updateNetwork still runs, keeping throughput samples fresh for the next
	// tick.
	if backlog := sm.blockBacklog.Load(); backlog > 0 {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check skipped: %d blocks pending local processing", sp.String(), backlog)
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

	// If no violations detected, the sync peer is healthy — nothing to do.
	if !isNetworkSpeedViolation && !isLastBlockTimeViolation {
		return
	}

	var reason string
	if isNetworkSpeedViolation {
		reason = "network speed violation"
	} else if isLastBlockTimeViolation {
		reason = "last block time out of range"
	}
	sm.logger.Debugf("[CheckSyncPeer] sync peer %s is stalled due to %s, updating sync peer", sp.String(), reason)

	state, exists := sm.peerStates.Get(sp)
	if !exists {
		return
	}

	sm.logger.Debugf("[CheckSyncPeer] removing sync peer %s", sp.String())

	sm.clearRequestedState(state)
	sm.updateSyncPeer(state)
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
		sm.updateSyncPeer(state)
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

// updateSyncPeer picks a new peer to sync from.
func (sm *SyncManager) updateSyncPeer(_ *peerSyncState) {
	sp, sps := sm.loadSyncPeerAndState()
	sm.logger.Infof("Updating sync peer, last block: %v, violations: %v, headers-first mode: %v",
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

	state, exists := sm.peerStates.Get(resolvedPeer)
	if !exists {
		// Stream peers (e.g. BlockPriority) are not registered in peerStates
		// directly - look up via their association's primary peer instead.
		if assoc := resolvedPeer.AssociationRef(); assoc != nil {
			primary := assoc.PrimaryPeer()
			if primary != nil {
				state, exists = sm.peerStates.Get(primary)
				if exists {
					sm.logger.Debugf("[%s][%s] resolved stream peer %s to primary peer %s", caller, bmsg.blockHash, resolvedPeer, primary)
					resolvedPeer = primary
				}
			}
		}
		if !exists {
			sm.logger.Errorf("[%s][%s] Received block message from unknown peer %s", caller, bmsg.blockHash, resolvedPeer)
			err = errors.NewServiceError("[%s] Received block message from unknown peer %s", caller, resolvedPeer)
			return
		}
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
			reason := fmt.Sprintf("Got unrequested block %v", bmsg.blockHash)
			resolvedPeer.DisconnectWithWarning(reason)
			err = errors.NewServiceError("Got unrequested block %v", bmsg.blockHash)
			return
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
func (sm *SyncManager) handleBlockMsgWithWindow(bmsg *blockQueueMsg, wa *windowAccumulator, flushWindow func()) (addedToWindow bool, err error) {
	sm.logger.Debugf("[handleBlockMsgWithWindow][%s] received block height %d from %s", bmsg.blockHash, bmsg.blockHeight, bmsg.peer)

	peer, state, catchingBlocks, isCheckpointBlock, headerHeight, preambleErr := sm.handleBlockPreamble("handleBlockMsgWithWindow", bmsg)
	if preambleErr != nil {
		return false, preambleErr
	}

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
	blockExists, existsErr := sm.blockchainClient.GetBlockExists(sm.ctx, &bmsg.blockHash)
	if existsErr != nil {
		sm.logger.Errorf("[handleBlockMsgWithWindow][%s] failed to check if block exists: %s", bmsg.blockHash, existsErr)
		return false, errors.NewProcessingError("failed to check if block exists", existsErr)
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

		return false, nil
	}

	msgBlock := bmsg.block
	if msgBlock == nil {
		return false, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] block message carries no block", bmsg.blockHash)
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
			return false, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] failed to convert header-chain height", bmsg.blockHash, convErr)
		}

		blockHeightUint32 = h
		block.SetHeight(headerHeight)
	case block.Height() > 0:
		h, convErr := safeconversion.Int32ToUint32(block.Height())
		if convErr != nil {
			return false, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] failed to convert block height", bmsg.blockHash, convErr)
		}

		blockHeightUint32 = h
	default:
		_, prevMeta, headerErr := sm.blockchainClient.GetBlockHeader(sm.ctx, &prevBlockHash)
		if headerErr != nil {
			return false, errors.NewProcessingError("[handleBlockMsgWithWindow][%s] failed to get prev block header for height determination", bmsg.blockHash, headerErr)
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
		flushWindow()

		directErr := sm.HandleBlockDirect(sm.ctx, peer, bmsg.blockHash, msgBlock)
		if directErr != nil {
			if errors.Is(directErr, errors.ErrBlockNotFound) {
				sm.logger.Infof("Block %v has missing parent %v, requesting missing blocks", bmsg.blockHash, prevBlockHash)

				bestBlockHeader, bestBlockHeaderMeta, getErr := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
				if getErr != nil {
					sm.logger.Errorf("Failed to get best block header: %v", getErr)
					return false, nil
				}

				locator, locErr := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
				if locErr != nil {
					sm.logger.Errorf("Failed to get block locator for the block hash %s: %v", bmsg.blockHash, locErr)
					return false, nil
				}

				zeroHash := chainhash.Hash{}
				if pushErr := peer.PushGetBlocksMsg(locator, &zeroHash); pushErr != nil {
					sm.logger.Errorf("Failed to send getblocks message: %v", pushErr)
				}

				return false, nil
			}

			if errors.Is(directErr, context.Canceled) || errors.IsContextError(directErr) {
				return false, nil
			}

			serviceError := errors.Is(directErr, errors.ErrServiceError) || errors.Is(directErr, errors.ErrStorageError)
			if !catchingBlocks && !serviceError {
				peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
			}

			sm.logger.Errorf("Failed to process new block in service blockQueueMsg %v: %v", bmsg.blockHash, directErr)
			return false, directErr
		}

		sm.runPostBlockProcessing(peer, state, bmsg, isCheckpointBlock)
		return false, nil
	}

	// Restore block-assembly back-pressure parity with HandleBlockDirect: stop
	// netsync outrunning block assembly by more than MaxBlocksBehindBlockAssembly.
	// The proven direct path waits here (handle_block.go); the window path had
	// dropped it. Only the window-add path needs this — the direct/checkpoint
	// branch above already goes through HandleBlockDirect, which waits itself.
	// On error we return it (addedToWindow=false), matching HandleBlockDirect.
	if waitErr := sm.waitForBlockAssemblyReadyCached(sm.ctx, blockHeightUint32); waitErr != nil {
		return false, waitErr
	}

	// Eligible for window: prepare the block synchronously on the drain goroutine
	// (prepareBlockForWindow does not mutate any SyncManager state that isn't
	// read-only after startup).
	prepared, prepErr := sm.prepareBlockForWindow(sm.ctx, peer, bmsg.blockHash, msgBlock, blockHeightUint32)
	if prepErr != nil {
		if errors.Is(prepErr, context.Canceled) || errors.IsContextError(prepErr) {
			return false, nil
		}

		serviceError := errors.Is(prepErr, errors.ErrServiceError) || errors.Is(prepErr, errors.ErrStorageError)
		if !catchingBlocks && !serviceError {
			peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
		}

		sm.logger.Errorf("[handleBlockMsgWithWindow][%s] prepareBlockForWindow failed: %v", bmsg.blockHash, prepErr)
		return false, prepErr
	}

	wa.add(prepared)

	// Refresh the sync peer's last-block time on the accept path, mirroring the
	// non-window path (handleBlockMsg -> HandleBlockDirect, see the same call in
	// handleBlockMsg). Windowed blocks never reach that refresh, so without this
	// lastBlockTime goes stale during a sustained window run even though blocks
	// are being accepted and committed. When blockBacklog later drains to 0 the
	// stall detector (handleCheckSyncPeer) would then see a large time-since-last-
	// block and could falsely rotate a healthy sync peer. Only runs on the accept
	// (addedToWindow=true) outcome; the ineligible/checkpoint/reject branches go
	// through HandleBlockDirect, which already refreshes it.
	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	// Advance the headers-first pipeline pump for every windowed block.
	// The peer-height-update and FSM-RUN blocks are intentionally omitted here:
	// those are guarded by sm.current() and windowed blocks are all below the
	// hardcoded checkpoint (not current), so they would never fire. fetchHeaderBlocks
	// MUST run so the sync peer keeps being asked for more blocks while the window
	// accumulates — without it the in-flight count falls to zero after requestedBlocks
	// deletion in the preamble and the pipeline stalls until the window flushes.
	// All state touched here (headerList, startHeader, requestedBlocks) is
	// drain-goroutine-only; this call is safe because we are still on that goroutine.
	sm.pumpBlockRequests(peer, state, isCheckpointBlock, bmsg.blockHash)

	return true, nil
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
			return
		}

		// Record the in-flight entries only after the getdata actually went out,
		// so a dropped batch leaves no phantom entries the window never re-tops.
		for _, h := range pendingHashes {
			sm.requestedBlocks.Set(*h, struct{}{})
			peerState.requestedBlocks.Set(*h, struct{}{})
		}
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

		iv := wire.NewInvVect(wire.InvTypeBlock, node.hash)

		// haveInventory issues a gRPC — must run with headerMu released.
		haveInv, err := sm.haveInventory(iv)
		if err != nil {
			sm.logger.Warnf("[assignBlocksAcrossPeers] unexpected failure checking inventory during header block fetch: %v", err)
		}

		if !haveInv {
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

	for _, h := range pending {
		if toAssign <= 0 {
			break
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

	_, exists := sm.peerStates.Get(peer)
	if !exists {
		// Stream peers (e.g. BlockPriority DATA1) are not registered in
		// peerStates directly - resolve via their association's primary peer.
		if assoc := peer.AssociationRef(); assoc != nil {
			primary := assoc.PrimaryPeer()
			if primary != nil {
				_, exists = sm.peerStates.Get(primary)
				if exists {
					sm.logger.Debugf("[handleHeadersMsg] resolved stream peer %s to primary peer %s", peer, primary)
					peer = primary
				}
			}
		}
		if !exists {
			sm.logger.Warnf("Received headers message from unknown peer %s", peer)
			return
		}
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
			sm.headerMu.Unlock()
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

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		// Stream peers (e.g. BlockPriority DATA1) are not registered in
		// peerStates directly - resolve via their association's primary peer.
		if assoc := peer.AssociationRef(); assoc != nil {
			primary := assoc.PrimaryPeer()
			if primary != nil {
				state, exists = sm.peerStates.Get(primary)
				if exists {
					sm.logger.Debugf("[handleInvMsg] resolved stream peer %s to primary peer %s", peer, primary)
					peer = primary
				}
			}
		}
		if !exists {
			sm.logger.Warnf("[handleInvMsg] Received inv message from unknown peer %s", peer)
			return
		}
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

// windowFlushJob is a drained, ascending-sorted window ready to commit. It is
// produced on the drain goroutine (drainJob) and consumed either synchronously
// (flush → commitWindowJob) or, in pipeline mode, by the single flush worker
// (flushWorker → commitWindowJob).
type windowFlushJob struct {
	blocks []*model.Block
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

	blocks := make([]*model.Block, len(entries))
	for i, e := range entries {
		blocks[i] = e.block
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
func (sm *SyncManager) flushWorker(ctx context.Context, jobs <-chan windowFlushJob) {
	poisoned := false

	for {
		select {
		case <-ctx.Done():
			// Context cancelled: exit without committing further. Drain any
			// buffered jobs (committing none) so the drain goroutine's pending
			// send, if any, does not block, then return once the channel closes.
			for range jobs {
			}

			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			if poisoned {
				// A prior window hit a fatal gap; commit no later window.
				sm.logger.Warnf("[flushWorker] poisoned after fatal window commit, discarding queued window of %d blocks", len(job.blocks))
				continue
			}

			if sm.commitWindowJobRecovered(ctx, job) {
				poisoned = true
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

	// TODO make this configurable.
	//
	// This buffer pins one *wire.MsgBlock per slot. Each MsgBlock carries
	// its go-wire decode arena (≥4 MiB per block today), so the previous
	// 10_000-deep queue could pin ~40 GiB of arena memory ahead of the
	// sequential processor. On a memory-constrained box that turns into
	// the dominant live-heap source and starves the GC. Cap at a small
	// value: enough to absorb processor-stall jitter, far below anything
	// that would meaningfully pin memory. The downloader naturally
	// back-pressures via TCP when the queue is full.
	maxBlockQueue := 100

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
			maxBlocks := sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly
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

		flushWindow := func() {
			if wa != nil && !wa.empty() {
				if pipelineEnabled {
					// Hand the drained window to the worker. The blocking send is
					// the depth-1 back-pressure that bounds in-flight windows.
					if j, ok := wa.drainJob(); ok {
						jobs <- j
					}
				} else {
					wa.flush(sm.ctx, sm)
				}
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

				return
			case <-timerC:
				sm.logger.Debugf("[blockHandler] window flush timer fired")
				flushWindow()
			case <-refillC:
				// Continuous-refill top-up on the drain goroutine. No-op unless in
				// headers-first IBD; getdata top-up only, never getheaders re-arm.
				sm.maintainInFlightWindow()
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
					sm.blockBacklog.Add(-1)

					if msg.reply != nil {
						msg.reply <- err
					}

					continue
				}

				// Window path: call handleBlockMsgWithWindow.
				added, err := sm.handleBlockMsgWithWindow(msg, wa, flushWindow)
				if !added {
					// Block was processed directly (or failed).
					// handleBlockMsgWithWindow does not send the reply on the
					// direct path — we send the outcome here.
					sm.blockBacklog.Add(-1)

					if msg.reply != nil {
						msg.reply <- err
					}

					// Flush any pending window now that an ineligible block arrived.
					flushWindow()
				} else {
					// Block was added to the window accumulator. Send the ack at
					// accept-time (or, when the window is now full, after the
					// full-flush commit — withhold-on-full back-pressure).
					sm.blockBacklog.Add(-1)
					sm.ackWindowedBlock(msg.reply, wa, flushWindow, armTimer, stopTimer)
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

				sm.blockBacklog.Add(1)

				blockQueue <- &blockQueueMsg{
					block:       msg.block.MsgBlock(),
					blockHash:   *msg.block.Hash(),
					blockHeight: msg.block.Height(),
					peer:        msg.peer,
					reply:       msg.reply,
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
	if maxBehind > 0 && sm.chainParams != nil && model.BelowCheckpoint(sm.chainParams.Checkpoints, blockHeight) {
		// Guard the uint32 addition against wraparound exactly as the slow-path
		// helper does; on possible overflow, defer to the slow path.
		if cached := sm.cachedBlockAssemblyHeight.Load(); cached > 0 && cached <= math.MaxUint32-uint32(maxBehind) {
			if cached+uint32(maxBehind) >= blockHeight {
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
			// monotonic below the checkpoint so it stays usable once it was.
			if cached > 0 && cached <= math.MaxUint32-uint32(maxBehind) && cached+uint32(maxBehind) >= blockHeight {
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
