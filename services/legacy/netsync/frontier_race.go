package netsync

import (
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE FRONTIER RACE. A block can only come from one peer at a time, so a block much larger than
// its neighbours, arriving from a slow peer, takes its size divided by that one peer's bandwidth
// however many other peers are idle. The time-based read-ahead asks for blocks minutes early so
// that is usually hidden. When it is not, this asks a second peer for the same block and keeps
// the first request running; whichever copy lands first is used, and the other is discarded
// without either peer losing its connection, because the download ledger records both as owners.
//
// It fires only when both hold:
//   - the chain is about to wait: the block's estimated finish, from its declared size, the bytes
//     received and the rate so far, is later than when the chain will reach it at the measured
//     commit rate;
//   - the peer is the slow one: its rate is under raceSlowFraction of the median of the other
//     peers', so a second peer can actually be expected to beat it.
//
// A slow peer with enough lead is left alone, and so is a fast peer on a huge block. One block is
// raced at a time.
//
// It replaces the racer deleted in cac9c173a, which raced on a fixed 20-second timer and
// deliberately skipped any peer still receiving bytes: exactly the case of a slow peer part way
// through a big block, which is what stalled mainnet on 2026-09-23.

const (
	// raceCheckInterval is how often the race is considered. It runs on its own ticker because
	// while the chain waits on a block no commit arrives to trigger anything else.
	raceCheckInterval = 5 * time.Second
	// raceMinElapsed is how long a stream must run before its rate is trusted.
	raceMinElapsed = 5 * time.Second
	// raceSlowFraction is how far below the other peers' median a peer must be to count as slow.
	raceSlowFraction = 0.5
	// raceMinSamples is how many other peers' rates a median needs.
	raceMinSamples = 2
	// peerRateWeight is the weight of a peer's newest completed block in its rolling rate.
	peerRateWeight = 0.5
	// raceExpiry is how long a race mark blocks another race when neither copy of the block
	// ever completes, for example because both peers dropped. Without it one lost block would
	// switch the race off for good.
	raceExpiry = 10 * time.Minute
	// maxConcurrentRaces is how many blocks may be raced at once, slow-peer and queued races
	// together. SV Node allows three fetches of one block; this allows three raced blocks.
	maxConcurrentRaces = 3
)

// blockStream is one block body arriving from the wire.
type blockStream struct {
	hash   chainhash.Hash
	height int32
	// owner is the one peer the ledger says owes this block, or nil when it is not exactly one.
	owner *peerpkg.Peer
	total int64
	read  atomic.Int64
	// lastRead is when bytes last arrived for this block, in unix nanoseconds.
	lastRead atomic.Int64
	start    time.Time
	// requestedAt is when the ledger first recorded a request for this block, zero if none.
	requestedAt time.Time
	// admitWait and path are set by admitPipelineSink under the registry's lock: how long this
	// block waited for an admission slot, and which path its bytes took.
	admitWait time.Duration
	path      string
}

func (s *blockStream) rate(now time.Time) float64 {
	elapsed := now.Sub(s.start).Seconds()
	if elapsed <= 0 {
		return 0
	}

	return float64(s.read.Load()) / elapsed
}

// countingReader counts the bytes the sink reads from a block stream.
type countingReader struct {
	r io.Reader
	s *blockStream
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.s.read.Add(int64(n))
		c.s.lastRead.Store(time.Now().UnixNano())
	}

	return n, err
}

// raceCandidate is why a block was picked, for the log line.
type raceCandidate struct {
	eta, need    time.Duration
	rate, median float64
}

// streamRegistry holds the blocks arriving now and each peer's rate on completed blocks.
type streamRegistry struct {
	mu     sync.Mutex
	active map[*blockStream]struct{}
	rates  map[*peerpkg.Peer]float64
	raced  map[chainhash.Hash]time.Time
	// lastBlock is when each peer last finished delivering a block.
	lastBlock map[*peerpkg.Peer]time.Time
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		lastBlock: make(map[*peerpkg.Peer]time.Time),
		active:    make(map[*blockStream]struct{}),
		rates:     make(map[*peerpkg.Peer]float64),
		raced:     make(map[chainhash.Hash]time.Time),
	}
}

func (r *streamRegistry) start(hash chainhash.Hash, height int32, owner *peerpkg.Peer, total int64, now time.Time) *blockStream {
	s := &blockStream{hash: hash, height: height, owner: owner, total: total, start: now}

	r.mu.Lock()
	r.active[s] = struct{}{}
	r.mu.Unlock()

	return s
}

// finish removes a stream. A complete one records its owner's rate and ends any race for the
// block, since the block is now here.
func (r *streamRegistry) finish(s *blockStream, now time.Time, complete bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.active, s)

	if !complete {
		return
	}

	delete(r.raced, s.hash)

	if s.owner == nil {
		return
	}

	r.lastBlock[s.owner] = now

	if bps := s.rate(now); bps > 0 {
		if prev, ok := r.rates[s.owner]; ok {
			bps = peerRateWeight*bps + (1-peerRateWeight)*prev
		}

		r.rates[s.owner] = bps
	}
}

func (r *streamRegistry) peerRate(p *peerpkg.Peer) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rates[p]
}

func (r *streamRegistry) markRaced(h chainhash.Hash, now time.Time) {
	r.mu.Lock()
	r.raced[h] = now
	r.mu.Unlock()
}

func (r *streamRegistry) racing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.raced) > 0
}

// forgetPeer drops a departed peer's rate and activity.
func (r *streamRegistry) forgetPeer(p *peerpkg.Peer) {
	r.mu.Lock()
	delete(r.rates, p)
	delete(r.lastBlock, p)
	r.mu.Unlock()
}

// lastBlockBytes is when block bytes last came from p: the latest byte of a block it is sending
// now, or the moment it finished its last block. Other traffic on the connection, pings and
// announcements, does not count: a peer that has dropped our request still sends those.
func (r *streamRegistry) lastBlockBytes(p *peerpkg.Peer) time.Time {
	if r == nil || p == nil {
		return time.Time{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	latest := r.lastBlock[p]

	for s := range r.active {
		if s.owner != p {
			continue
		}

		if at := s.lastRead.Load(); at > 0 {
			if t := time.Unix(0, at); t.After(latest) {
				latest = t
			}
		}
	}

	return latest
}

// raceSlotLocked drops race marks past their expiry and reports whether another race may start.
func (r *streamRegistry) raceSlotLocked(now time.Time) bool {
	for h, at := range r.raced {
		if now.Sub(at) > raceExpiry {
			delete(r.raced, h)
		}
	}

	return len(r.raced) < maxConcurrentRaces
}

// queuedRaceAllowed reports whether h may be raced as a queued block: it is not arriving, not
// already raced, and a race slot is free.
func (r *streamRegistry) queuedRaceAllowed(h chainhash.Hash, now time.Time) bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, raced := r.raced[h]; raced {
		return false
	}

	for s := range r.active {
		if s.hash == h {
			return false
		}
	}

	return r.raceSlotLocked(now)
}

// pickRace returns the block the chain is most urgently about to wait on from a slow peer, if
// any. tip is the committed height and commitRate the blocks a second joining the chain.
func (r *streamRegistry) pickRace(now time.Time, tip int32, commitRate float64) (*blockStream, raceCandidate, bool) {
	if commitRate <= 0 {
		return nil, raceCandidate{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.raceSlotLocked(now) {
		return nil, raceCandidate{}, false
	}

	var (
		best     *blockStream
		bestCand raceCandidate
	)

	for s := range r.active {
		if now.Sub(s.start) < raceMinElapsed || s.read.Load() <= 0 || s.height <= tip {
			continue
		}

		if _, raced := r.raced[s.hash]; raced {
			continue
		}

		rate := s.rate(now)
		if rate <= 0 {
			continue
		}

		median, ok := r.medianOfOthersLocked(s, now)
		if !ok || rate >= raceSlowFraction*median {
			continue
		}

		remaining := s.total - s.read.Load()
		if remaining <= 0 {
			continue
		}

		eta := time.Duration(float64(remaining) / rate * float64(time.Second))
		need := time.Duration(float64(s.height-tip) / commitRate * float64(time.Second))

		if eta <= need {
			continue
		}

		if best == nil || need < bestCand.need {
			best = s
			bestCand = raceCandidate{eta: eta, need: need, rate: rate, median: median}
		}
	}

	return best, bestCand, best != nil
}

// medianOfOthersLocked is the median rate of every peer other than s's owner: each other active
// stream that has been measured, and each other peer's rate on completed blocks, one value per
// peer, the live stream preferred.
func (r *streamRegistry) medianOfOthersLocked(s *blockStream, now time.Time) (float64, bool) {
	byPeer := make(map[*peerpkg.Peer]float64)
	var anonymous []float64

	for o := range r.active {
		if o == s || (o.owner != nil && o.owner == s.owner) || now.Sub(o.start) < raceMinElapsed {
			continue
		}

		if bps := o.rate(now); bps > 0 {
			if o.owner == nil {
				anonymous = append(anonymous, bps)
			} else {
				byPeer[o.owner] = bps
			}
		}
	}

	for p, bps := range r.rates {
		if p == s.owner {
			continue
		}

		if _, live := byPeer[p]; !live {
			byPeer[p] = bps
		}
	}

	rates := anonymous
	for _, bps := range byPeer {
		rates = append(rates, bps)
	}

	if len(rates) < raceMinSamples {
		return 0, false
	}

	sort.Float64s(rates)

	mid := len(rates) / 2
	if len(rates)%2 == 1 {
		return rates[mid], true
	}

	return (rates[mid-1] + rates[mid]) / 2, true
}

// chooseRacer picks who to ask: never an owner, the fastest measured peer first, then the peer
// with the fewest blocks already queued, because a request waits behind what a peer already owes.
func (r *streamRegistry) chooseRacer(candidates, owners []*peerpkg.Peer, queued func(*peerpkg.Peer) int) *peerpkg.Peer {
	r.mu.Lock()
	defer r.mu.Unlock()

	isOwner := make(map[*peerpkg.Peer]bool, len(owners))
	for _, o := range owners {
		isOwner[o] = true
	}

	var best *peerpkg.Peer

	better := func(a, b *peerpkg.Peer) bool {
		ra, rb := r.rates[a], r.rates[b]
		if ra != rb {
			return ra > rb
		}

		return queued(a) < queued(b)
	}

	for _, p := range candidates {
		if p == nil || isOwner[p] {
			continue
		}

		if best == nil || better(p, best) {
			best = p
		}
	}

	return best
}

// trackBlockStreams wraps the installed block sink so every block body arriving is measured.
func (sm *SyncManager) trackBlockStreams(inner func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)) func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
	return func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
		height, _ := sm.headerCache.HeightOf(hash)

		var owner *peerpkg.Peer
		if owners := sm.blockDownloads.OwnersOf(hash); len(owners) == 1 {
			owner = owners[0]
		}

		s := sm.streams.start(hash, height, owner, n, time.Now())
		if at, ok := sm.blockDownloads.RequestedAt(hash); ok {
			s.requestedAt = at
		}

		converted, err := inner(hash, header, countingReader{r: r, s: s}, n)

		now := time.Now()
		sm.streams.finish(s, now, err == nil && s.read.Load() >= n)

		// The committed tip is read only for a download worth reporting: it is a blockchain
		// call, and this runs on the peer's read loop for every block.
		sm.streams.mu.Lock()
		_, worth := s.report(now, s.height)
		sm.streams.mu.Unlock()

		if worth {
			tip, _, _ := sm.committedTip()

			sm.streams.mu.Lock()
			line, _ := s.report(now, tip)
			sm.streams.mu.Unlock()

			sm.logger.Infof("[blockDownload][%s] %s", hash, line)
		}

		return converted, err
	}
}

// runFrontierRace considers the race every raceCheckInterval until the manager quits.
func (sm *SyncManager) runFrontierRace() {
	ticker := time.NewTicker(raceCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.quit:
			return
		case <-ticker.C:
			sm.maybeRaceSlowBlock(time.Now())
			sm.maybeRaceQueuedBlock(time.Now())
		}
	}
}

// maybeRaceSlowBlock asks a second peer for the block the chain is about to wait on, if one is
// arriving from a slow peer. The committed tip is read here, outside any lock.
func (sm *SyncManager) maybeRaceSlowBlock(now time.Time) {
	if sm.streams == nil {
		return
	}

	tip, _, ok := sm.committedTip()
	if !ok {
		return
	}

	s, c, ok := sm.streams.pickRace(now, tip, sm.commitRate.rate())
	if !ok {
		return
	}

	eligible := sm.eligibleBlockPeers()
	candidates := make([]*peerpkg.Peer, 0, len(eligible))

	for _, bp := range eligible {
		if bp.state != nil && bp.state.BestKnownHeight() > 0 && bp.state.BestKnownHeight() < s.height {
			continue
		}

		candidates = append(candidates, bp.peer)
	}

	racer := sm.streams.chooseRacer(candidates, sm.blockDownloads.OwnersOf(s.hash), sm.blockDownloads.CountForPeer)
	if racer == nil {
		sm.logger.Infof("[frontierRace][%s] chain reaches height %d in %s but the block needs %s more at %.1f MB/s against a median of %.1f MB/s; no other peer to ask",
			s.hash, s.height, c.need.Round(time.Second), c.eta.Round(time.Second), c.rate/1e6, c.median/1e6)

		return
	}

	if !sm.askRacer(racer, s.hash, now) {
		return
	}

	sm.logger.Infof("[frontierRace][%s] asked %s for block %d as well: the chain reaches it in %s, it needs %s more at %.1f MB/s against a median of %.1f MB/s",
		s.hash, racer, s.height, c.need.Round(time.Second), c.eta.Round(time.Second), c.rate/1e6, c.median/1e6)
}

// askRacer records racer as a second owner of h and sends it the getdata. Recording first means
// whichever copy lands second is still admitted rather than costing a peer its connection.
func (sm *SyncManager) askRacer(racer *peerpkg.Peer, h chainhash.Hash, now time.Time) bool {
	if !sm.blockDownloads.Add(racer, h) {
		return false
	}

	getData := wire.NewMsgGetDataSizeHint(1)
	if err := getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &h)); err != nil {
		sm.blockDownloads.RemoveOwner(racer, h)

		return false
	}

	sm.streams.markRaced(h, now)
	racer.QueueMessage(getData, nil)

	if prometheusLegacyNetsyncFrontierRaces != nil {
		prometheusLegacyNetsyncFrontierRaces.Inc()
	}

	return true
}
