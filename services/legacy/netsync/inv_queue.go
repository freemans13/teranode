package netsync

import (
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// The transaction announcement queue.
//
// A peer's inv message names up to wire.MaxInvPerMsg transactions. Each one
// used to get a goroutine and a store Get, and on 2026-10-19 that reached
// 223,118 goroutines on Hetzner mainnet before the container's memory limit
// killed the node. Now the handler goroutine cuts a message's transaction
// items into chunks, admits each chunk against the peer's share and the node
// cap, and pushes it onto one node-wide queue without waiting. Sixteen workers
// drain the queue into UTXO store batch reads, and the memory the path can
// hold is the cap, legacy_invQueueMaxHashes, times about 44 bytes a hash.
const (
	// invLookupWorkers is the number of goroutines that take announcements
	// off invQueue and look them up in the UTXO store. It caps the store
	// reads in flight from this path, which the review of PR 2045 asked for,
	// and is not a memory bound (16 x 4,096 hashes in flight is about 10 MB),
	// so it is a constant rather than a setting.
	invLookupWorkers = 16

	// invPeerShare is the most announced hashes one peer may have queued or
	// being looked up at once: two maximum-size announcements. It has to hold
	// at least one whole announcement, because checkTxInvs pushes all thirteen
	// chunks of a 50,000-hash message in microseconds and a worker releases a
	// chunk only after a store round trip of milliseconds; a share of 32,768
	// would drop the tail of every maximum-size message from an honest peer on
	// an idle node. Two covers a peer sending two such messages back to back;
	// holding more than two in flight needs the peer to deliver 7 MB of inv
	// faster than the workers drain it, which no internet peer's link does.
	// It is clamped to the node cap when the cap is smaller.
	invPeerShare = 2 * wire.MaxInvPerMsg

	// defaultInvQueueMaxHashes is the fallback for a legacy_invQueueMaxHashes
	// below one chunk, and the same number as the setting's default in
	// settings/settings.go: eight maximum-size announcements.
	defaultInvQueueMaxHashes = 8 * wire.MaxInvPerMsg

	// The reasons announcements are dropped, as prometheusLegacyNetsyncInvDropped
	// labels them.
	invDropPeerShare    = "peer_share"
	invDropNodeCap      = "node_cap"
	invDropLookupFailed = "lookup_failed"
)

// invDropReasons lists every drop reason so the metric has a zero row for
// each before the first drop.
var invDropReasons = []string{invDropPeerShare, invDropNodeCap, invDropLookupFailed}

// invChunk is up to txInvLookupChunk transaction announcements from one peer,
// waiting on invQueue for a worker. state is the peer's registration at the
// time of queueing. A worker compares it with the current one, so a peer that
// reconnected while the chunk waited is not asked for hashes it announced on
// its previous connection.
type invChunk struct {
	peer  *peerpkg.Peer
	state *peerSyncState
	items []*wire.InvVect
}

// invQueueMaxHashes returns legacy_invQueueMaxHashes, or the default with a
// warning when the setting is below one chunk. Zero is deliberately not
// unbounded, unlike legacy_maxOrphanTxs: an unbounded queue is the incident.
func invQueueMaxHashes(logger ulogger.Logger, tSettings *settings.Settings) int {
	n := tSettings.Legacy.InvQueueMaxHashes
	if n < txInvLookupChunk {
		logger.Warnf("[legacy] legacy_invQueueMaxHashes=%d is below one chunk of %d, using the default %d", n, txInvLookupChunk, defaultInvQueueMaxHashes)

		return defaultInvQueueMaxHashes
	}

	return n
}

// initInvQueue builds the announcement queue for a cap of maxHashes. The
// channel holds one pointer per chunk and is sized to the cap in hashes, so
// the hash count, not the channel, is the limit that binds: chunks range from
// one hash to txInvLookupChunk, and a channel sized in chunks would bind first
// on the small announcements SV Node's trickle relay sends.
func (sm *SyncManager) initInvQueue(maxHashes int) {
	sm.invQueueCap = maxHashes
	sm.invQueue = make(chan *invChunk, maxHashes)
}

// invShare is the per-peer share in force: invPeerShare, or the cap when the
// cap is smaller, in which case one peer can fill the queue.
func (sm *SyncManager) invShare() int {
	return min(invPeerShare, sm.invQueueCap)
}

// admitTxInvs cuts a message's transaction announcements into chunks and
// queues each one that fits both the peer's share and the node cap. A chunk
// that fits neither is dropped whole and counted by hashes. It never waits,
// because it runs on the goroutine that handles the peer's message. The
// counts it raises are given back by releaseInvChunk once a worker has
// finished with the chunk, so they cover hashes being looked up as well as
// hashes waiting.
func (sm *SyncManager) admitTxInvs(peer *peerpkg.Peer, state *peerSyncState, txInvs []*wire.InvVect) {
	share := int64(sm.invShare())
	capHashes := int64(sm.invQueueCap)

	for start := 0; start < len(txInvs); start += txInvLookupChunk {
		items := txInvs[start:min(start+txInvLookupChunk, len(txInvs))]
		n := int64(len(items))

		if state.queuedInvs.Add(n) > share {
			state.queuedInvs.Add(-n)
			sm.dropTxInvs(peer, n, invDropPeerShare)

			continue
		}

		if sm.invQueued.Add(n) > capHashes {
			sm.invQueued.Add(-n)
			state.queuedInvs.Add(-n)
			sm.dropTxInvs(peer, n, invDropNodeCap)

			continue
		}

		select {
		case sm.invQueue <- &invChunk{peer: peer, state: state, items: items}:
		default:
			// The channel is sized to the cap, so this needs a queue narrower
			// than the cap, which production never builds.
			sm.invQueued.Add(-n)
			state.queuedInvs.Add(-n)
			sm.dropTxInvs(peer, n, invDropNodeCap)
		}
	}

	prometheusLegacyNetsyncInvQueuedHashes.Set(float64(sm.invQueued.Load()))
}

// dropTxInvs counts and logs announcements that were not, or will not be,
// looked up. Their items were marked known for the peer before this, which
// stops the node announcing them back, and nothing is requested: the node
// relies on another peer announcing the transaction, or on fetching it with
// the block that mines it.
func (sm *SyncManager) dropTxInvs(peer *peerpkg.Peer, n int64, reason string) {
	prometheusLegacyNetsyncInvDropped.WithLabelValues(reason).Add(float64(n))
	sm.logger.Debugf("[admitTxInvs] dropping %d transaction announcements from %s: %s", n, peer, reason)
}

// releaseInvChunk gives a chunk's hashes back to its peer's share and to the
// node count once a worker has finished with it.
func (sm *SyncManager) releaseInvChunk(c *invChunk) {
	n := int64(len(c.items))
	c.state.queuedInvs.Add(-n)
	prometheusLegacyNetsyncInvQueuedHashes.Set(float64(sm.invQueued.Add(-n)))
}
