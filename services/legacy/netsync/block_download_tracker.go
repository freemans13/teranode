package netsync

import (
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

const (
	// blockRequestAssignmentTTL is how long a peer stays on the hook for a block
	// we asked it for. It is the ceiling the disconnect decision uses: a peer
	// that delivers within it is answering our question, however late, and must
	// keep its connection. Inherited from the per-peer map this replaced, whose
	// comment explained the hour is what legacy sync and checkpoint batches need.
	blockRequestAssignmentTTL = 60 * time.Minute

	// blockRequestRetryInterval is how long we wait before an announced block is
	// worth asking for again. It is deliberately far shorter than the ownership
	// ceiling: after a minute we are willing to ask somebody else, but the
	// original peer is still not punished if its copy turns up. Inherited from
	// the global map this replaced.
	blockRequestRetryInterval = 60 * time.Second

	// maxTrackedBlockDownloads bounds how many distinct blocks the ledger will
	// track, so a flood of announcements cannot grow it without limit. Eviction
	// is oldest-first: a burst of new announcements must never displace the
	// frontier block we are actually waiting on, because a block that arrives
	// after its record was dropped looks unrequested and costs the peer its
	// connection.
	maxTrackedBlockDownloads = 50_000
)

// blockDownloadTracker records which peers owe us which blocks.
//
// It replaces two separate expiring maps — one global, one per peer — that
// between them could only express a single owner per hash. The frontier race
// already breaks that assumption: when the sync peer goes quiet we deliberately
// ask a second peer for the same block, and both of them are then entitled to
// deliver it without being disconnected. Holding the ownership the other way
// round, as a set of (block, peer) pairs, says that directly.
//
// Entries age out on their own. That matters because the call that is supposed
// to release a departing peer's blocks does not always run: handleDonePeerMsg
// returns early for any peer that is not registered in peerStates, which
// includes the stream sub-peers a BlockPriority association resolves through. An
// assignment nothing ever clears must not pin a hash forever.
//
// There is no background goroutine. The two maps this replaced each ran a
// cleanup ticker that was stopped, never cleared, by the code meant to release a
// peer's requests — the cleanup looked like it was happening and was not. Expiry
// here is done by the callers' own reads and writes, so there is nothing to
// start, nothing to stop, and nothing that can silently stop working.
//
// Every method is safe on a nil receiver: reads answer "nothing is owed" and
// writes do nothing. Reading a nil tracker as "we never asked for this" is the
// safe direction — it costs a misbehaving-looking peer its connection rather
// than admitting a block nobody requested.
type blockDownloadTracker struct {
	mu  sync.Mutex
	ttl time.Duration
	// now is the clock, injectable so tests can age assignments without sleeping.
	now func() time.Time
	// byHash answers "who owes us this block, and since when".
	byHash map[chainhash.Hash]map[*peerpkg.Peer]time.Time
	// byPeer answers "what does this peer owe us", so a peer's own outstanding
	// count and its removal are both O(what that peer owes) rather than O(all).
	byPeer    map[*peerpkg.Peer]map[chainhash.Hash]struct{}
	lastSweep time.Time
}

// newBlockDownloadTracker builds a ledger whose assignments expire after ttl.
func newBlockDownloadTracker(ttl time.Duration) *blockDownloadTracker {
	return &blockDownloadTracker{
		ttl:    ttl,
		now:    time.Now,
		byHash: make(map[chainhash.Hash]map[*peerpkg.Peer]time.Time),
		byPeer: make(map[*peerpkg.Peer]map[chainhash.Hash]struct{}),
	}
}

// Add records that we have asked peer p for block h. Asking the same peer again
// refreshes the assignment, which is what we want: the clock should run from the
// most recent time we actually asked.
func (t *blockDownloadTracker) Add(p *peerpkg.Peer, h chainhash.Hash) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()

	if t.byHash == nil {
		t.byHash = make(map[chainhash.Hash]map[*peerpkg.Peer]time.Time)
	}

	if t.byPeer == nil {
		t.byPeer = make(map[*peerpkg.Peer]map[chainhash.Hash]struct{})
	}

	owners := t.byHash[h]
	if owners == nil {
		owners = make(map[*peerpkg.Peer]time.Time, 1)
		t.byHash[h] = owners
	}

	owners[p] = now

	hashes := t.byPeer[p]
	if hashes == nil {
		hashes = make(map[chainhash.Hash]struct{}, 1)
		t.byPeer[p] = hashes
	}

	hashes[h] = struct{}{}

	t.maybeSweepLocked(now)
}

// HasOwner reports whether peer p is currently on the hook for block h. This is
// the question the disconnect decision asks, so a false answer costs a peer its
// connection — expiry is judged against the full ownership ceiling, not the far
// shorter re-request window.
func (t *blockDownloadTracker) HasOwner(p *peerpkg.Peer, h chainhash.Hash) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	at, ok := t.byHash[h][p]

	return ok && !t.expiredAt(at, now, t.ttl)
}

// RequestedWithin reports whether anybody was asked for block h within maxAge.
// This is the question the inv path asks before requesting a block, so a false
// answer means "ask somebody", not "punish somebody".
func (t *blockDownloadTracker) RequestedWithin(h chainhash.Hash, maxAge time.Duration) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	for _, at := range t.byHash[h] {
		if !t.expiredAt(at, now, maxAge) {
			return true
		}
	}

	return false
}

// Remove drops block h entirely, whoever was asked for it. Used when the block
// has arrived and nobody's copy is wanted any more.
func (t *blockDownloadTracker) Remove(h chainhash.Hash) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.removeHashLocked(h)
}

// RemoveOwner cancels just this peer's obligation for block h and leaves any
// other peer still owing it. The frontier race needs the difference: when a
// raced block arrives it cancels the request with the peers it asked and nobody
// else.
func (t *blockDownloadTracker) RemoveOwner(p *peerpkg.Peer, h chainhash.Hash) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.removeOwnerLocked(p, h)
}

// CountForPeer returns how many blocks this peer still owes us, ignoring
// assignments that have aged out. This feeds the per-peer in-flight budget, so
// counting a dead assignment would spend budget on a block that is never coming.
func (t *blockDownloadTracker) CountForPeer(p *peerpkg.Peer) int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	n := 0

	for h := range t.byPeer[p] {
		if at, ok := t.byHash[h][p]; ok && !t.expiredAt(at, now, t.ttl) {
			n++
		}
	}

	return n
}

// PeersWithDownloads returns how many distinct peers have at least one live
// assignment. This number widens every peer's block download deadline, so a peer
// whose only assignment has aged out must not count.
func (t *blockDownloadTracker) PeersWithDownloads() int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	n := 0

	for p, hashes := range t.byPeer {
		for h := range hashes {
			if at, ok := t.byHash[h][p]; ok && !t.expiredAt(at, now, t.ttl) {
				n++
				break
			}
		}
	}

	return n
}

// ClearPeer releases everything this peer owed us, so the next announcement of
// any of those blocks fetches them from somewhere else.
func (t *blockDownloadTracker) ClearPeer(p *peerpkg.Peer) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for h := range t.byPeer[p] {
		t.removeOwnerFromHashLocked(p, h)
	}

	delete(t.byPeer, p)
}

// Clear forgets every assignment. Used when the sync peer changes, so blocks the
// previous peer failed to send are not ignored when they are announced again.
func (t *blockDownloadTracker) Clear() {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.byHash = make(map[chainhash.Hash]map[*peerpkg.Peer]time.Time)
	t.byPeer = make(map[*peerpkg.Peer]map[chainhash.Hash]struct{})
}

// Len returns how many distinct blocks are currently owed by somebody, ignoring
// assignments that have aged out.
func (t *blockDownloadTracker) Len() int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	n := 0

	for _, owners := range t.byHash {
		for _, at := range owners {
			if !t.expiredAt(at, now, t.ttl) {
				n++
				break
			}
		}
	}

	return n
}

// clock reads the injected time source, tolerating a tracker built as a struct
// literal without one.
func (t *blockDownloadTracker) clock() time.Time {
	if t.now == nil {
		return time.Now()
	}

	return t.now()
}

// expiredAt reports whether an assignment made at `at` has aged past maxAge. A
// non-positive maxAge means "never expires", which is what a tracker built
// without a ttl gets.
func (t *blockDownloadTracker) expiredAt(at, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		return false
	}

	return now.Sub(at) >= maxAge
}

// removeOwnerLocked drops one (block, peer) pair from both directions.
func (t *blockDownloadTracker) removeOwnerLocked(p *peerpkg.Peer, h chainhash.Hash) {
	t.removeOwnerFromHashLocked(p, h)

	if hashes, ok := t.byPeer[p]; ok {
		delete(hashes, h)

		if len(hashes) == 0 {
			delete(t.byPeer, p)
		}
	}
}

// removeOwnerFromHashLocked drops the pair from byHash only, dropping the hash
// entirely once nobody owes it. The caller is responsible for byPeer, so
// ClearPeer can delete a peer's whole set in one go.
func (t *blockDownloadTracker) removeOwnerFromHashLocked(p *peerpkg.Peer, h chainhash.Hash) {
	owners, ok := t.byHash[h]
	if !ok {
		return
	}

	delete(owners, p)

	if len(owners) == 0 {
		delete(t.byHash, h)
	}
}

// removeHashLocked drops a block and every peer that owed it.
func (t *blockDownloadTracker) removeHashLocked(h chainhash.Hash) {
	for p := range t.byHash[h] {
		if hashes, ok := t.byPeer[p]; ok {
			delete(hashes, h)

			if len(hashes) == 0 {
				delete(t.byPeer, p)
			}
		}
	}

	delete(t.byHash, h)
}

// maybeSweepLocked drops aged-out assignments, and then, if the ledger is still
// over its size cap, the oldest live ones. Sweeping runs at most once every
// quarter of the ttl so it costs nothing in the steady state; readers do not
// depend on it having run, because they check each assignment's own age.
func (t *blockDownloadTracker) maybeSweepLocked(now time.Time) {
	overCap := len(t.byHash) > maxTrackedBlockDownloads
	due := t.ttl > 0 && now.Sub(t.lastSweep) >= t.ttl/4

	if !overCap && !due {
		return
	}

	t.lastSweep = now

	for h, owners := range t.byHash {
		for p, at := range owners {
			if t.expiredAt(at, now, t.ttl) {
				t.removeOwnerLocked(p, h)
			}
		}
	}

	if len(t.byHash) <= maxTrackedBlockDownloads {
		return
	}

	t.evictOldestLocked(maxTrackedBlockDownloads * 9 / 10)
}

// evictOldestLocked shrinks the ledger to keep at most `target` blocks, dropping
// the least recently requested first. Oldest-first is the whole point: dropping
// a block we are still waiting on turns its arrival into an unrequested block
// and disconnects an honest peer, and the block we have waited longest for is
// the one least likely to still be coming.
func (t *blockDownloadTracker) evictOldestLocked(target int) {
	type aged struct {
		hash chainhash.Hash
		at   time.Time
	}

	ages := make([]aged, 0, len(t.byHash))

	for h, owners := range t.byHash {
		newest := time.Time{}

		for _, at := range owners {
			if at.After(newest) {
				newest = at
			}
		}

		ages = append(ages, aged{hash: h, at: newest})
	}

	sort.Slice(ages, func(i, j int) bool { return ages[i].at.Before(ages[j].at) })

	for i := 0; i < len(ages)-target; i++ {
		t.removeHashLocked(ages[i].hash)
	}
}
