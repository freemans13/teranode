package netsync

import (
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE FRONTIER RACE, SV Node's rule. A block comes from one peer at a time. When the peer sending
// the block the chain needs is struggling, delivering under raceStallRate after raceSlowFetchAfter,
// one other peer is asked for it and the struggling peer is disconnected, as SV Node drops a
// staller. Disconnecting stops its half-converted copy, so the extra copy converts instead of
// being drained as a duplicate. A block is raced only when every copy of it is struggling, at most
// once in raceSlowFetchAfter, and another peer is asked only while the block has fewer than
// maxBlockCopies live copies: on 2026-09-24 a race that re-armed whenever a copy finished asked
// three peers for the same 2 GB block and threw all three copies away while a 13 MB/s peer, which
// was not struggling at all, finished the first. At the cap the struggling peers are still
// disconnected; only the extra request is skipped.
//
// SV Node: DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT is 30 s, DEFAULT_MIN_BLOCK_STALLING_RATE is
// 100 KB/s.
//
// The race judges only a block whose bytes have started, and only in headers-first mode:
// trackBlockStreams takes a stream's height from the header cache, which is empty above the last
// checkpoint, so pickRace's height test skips every tip stream. That makes the race narrower than SV
// Node's parallel fetch, which after DEFAULT_BLOCK_DOWNLOAD_SLOW_FETCH_TIMEOUT (30 s) asks another
// peer for the first in-flight block of a peer whose block-stream bandwidth is below the stalling
// rate, bytes started or not (FindNextBlocksToDownload, net_processing.cpp:462-507, capped at
// DEFAULT_MAX_BLOCK_PARALLEL_FETCH, 3). A peer that has not started sending a block is not
// struggling with it here: it may be sending blocks queued ahead of it, or reading it from disk
// before its first byte. That case is handled in both modes by the download pass,
// assignWantedBlocks, which after blockRequestRetryInterval lets a quiet owner off and asks another
// peer: below the last checkpoint the pass names blocks from the header cache, above it from the
// ledger, one head-of-queue block per quiet owner (appendOutstandingAtTip). The 60-second quiet
// rule is the looser cousin of SV Node's: any quiet owner rather than a bandwidth test, 60 s rather
// than 30, one re-ask per owner per retry window rather than three parallel fetches.
// blockRequestRetryInterval's comment explains what an SV Node peer is doing while it is quiet. Do
// not extend the race to it. A block that has not started because it waits behind other blocks
// at a peer that is busy, neither quiet nor struggling on it, is the queued re-ask's
// (queued_reask.go), which runs on this ticker, keeps the owner, and shares the race's mark.

const (
	// raceCheckInterval is how often the race is considered. It runs on its own ticker because
	// while the chain waits on a block no commit arrives to trigger anything else.
	raceCheckInterval = 5 * time.Second
	// raceSlowFetchAfter is how long a block must have been arriving before its peer is judged.
	raceSlowFetchAfter = 30 * time.Second
	// raceStallRate is the delivery rate, in bytes a second, below which a peer is struggling.
	raceStallRate = 100_000
	// peerRateWeight is the weight of a peer's newest completed block in its rolling rate.
	peerRateWeight = 0.5
	// raceExpiry is how long a block's race mark is kept. The mark is when the block's newest
	// extra copy was asked for; another copy is asked for only after raceSlowFetchAfter, and only
	// when every copy is late (see maxBlockCopies).
	raceExpiry = 10 * time.Minute
	// maxBlockCopies is the most live copies of one block at once: the first request and at most
	// two extra copies, from the race or the queued re-ask. A live copy is an owner not let off
	// the block, or one sending it now (blockCopies); a forgiven owner sending nothing is not a
	// copy. SV Node fetches the first in-flight block from up to DEFAULT_MAX_BLOCK_PARALLEL_FETCH
	// (3) peers (net/net.h:167). It asks for another copy only when every connected peer the
	// block is in flight from is stalling, and counts those stallers against the cap
	// (stallerCount < maxParallelFetch, net/net_processing.cpp:464-496). Its DetectStalling then
	// disconnects a staller (net_processing.cpp:5446-5466), which takes it out of the count. A
	// forgiven owner here is never disconnected for its silence, so it is not counted.
	maxBlockCopies = 3
	// minRateSample is the least delivery time one rate sample covers. A block that took less is
	// pooled with the peer's next blocks until together they took this long. A transfer that short
	// is mostly round trip and buffering: on 2026-10-07 one 1 MB copy read in 5 ms gave a peer a
	// rate of 190 MB/s, and one such sample in the rolling rate made that peer most of the measured
	// bandwidth. Pooling rather than dropping keeps small blocks measured: at early heights every
	// block takes far less than a second, and a peer never measured holds one block.
	minRateSample = time.Second
	// rateDecayAfter is how long a peer that owes blocks may send no block bytes before its rate
	// starts to fall: the peerQueueSeconds a peer's queue is sized to keep it busy for.
	rateDecayAfter = peerQueueSeconds * time.Second
	// rateDecayHalfLife is how long a silent peer's rate takes to halve after rateDecayAfter. SV
	// Node averages each peer's block-stream bandwidth over its last 60 s in 5 s spots
	// (net/stream.cpp:312-346, net/stream.h:173, net/net.cpp:2617), so a peer that stops sending is at half its rate
	// 30 s later. The rate here never reaches zero, which would read as unmeasured.
	rateDecayHalfLife = raceSlowFetchAfter
)

// blockStream is one block body arriving from the wire.
type blockStream struct {
	hash   chainhash.Hash
	height int32
	// owner is the peer sending this copy, when the download ledger says that peer owes the
	// block. It is nil for a copy from a peer that does not owe the block: such a copy is drained
	// unwritten (admitPipelineSink), and gives no peer a rate, an activity time or a size sample,
	// and does not count as the block arriving. Every field is set before the stream is published
	// (add), because the sync manager reads them from other goroutines.
	owner *peerpkg.Peer
	// sender is the peer sending this copy, resolved to its association primary, owed or not, or
	// nil when the reader names no peer. Its bytes keep that peer's connection busy whether or
	// not they are kept, so they count in what it is sending (pending).
	sender *peerpkg.Peer
	total  int64
	read   atomic.Int64
	// lastRead is when bytes last arrived for this block, or when its admission slot was granted
	// if later (admit), in unix nanoseconds.
	lastRead atomic.Int64
	// received is the node-wide count of block bytes received, or nil.
	received *atomic.Int64
	// start is when the copy's clock starts: its first byte, moved to when its admission slot
	// was granted if it waited for one (admit). Written under the registry's lock after add.
	start time.Time
	// awaiting is true while the copy waits for an admission slot (admitPipelineSink). This node
	// reads none of its bytes then, so the copy is not judged and its owner is not quiet.
	awaiting atomic.Bool
	// waited is how long the copy waited for admission, the amount start was moved forward.
	// Written under the registry's lock.
	waited time.Duration
	// requestedAt is when the ledger first recorded a request for this block, zero if none.
	requestedAt time.Time
	// from is when owner could start sending this block: the later of when it was asked for it and
	// when it finished the block before, and never after the first byte. A completed block is timed
	// from it, so a peer that takes long to start a block is not measured as fast.
	from time.Time
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

		if c.s.received != nil {
			c.s.received.Add(int64(n))
		}
	}

	return n, err
}

// raceCandidate is why a block was picked, for the log line.
type raceCandidate struct {
	eta, need, age time.Duration
	rate           float64
}

// streamRegistry holds the blocks arriving now and each peer's rate on completed blocks.
type streamRegistry struct {
	mu     sync.Mutex
	active map[*blockStream]struct{}
	rates  map[*peerpkg.Peer]float64
	raced  map[chainhash.Hash]time.Time
	// lastBlock is when each peer last finished delivering a block.
	lastBlock map[*peerpkg.Peer]time.Time
	// pooled is each peer's completed blocks not yet in its rate, until they cover minRateSample.
	pooled map[*peerpkg.Peer]rateSample
	// decay is the factor a peer's rate is cut to while it owes blocks and sends none
	// (decayQuiet). It stays until the peer's next rate sample.
	decay map[*peerpkg.Peer]float64
}

// rateSample is block bytes delivered and the time they took.
type rateSample struct {
	bytes int64
	took  time.Duration
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{
		lastBlock: make(map[*peerpkg.Peer]time.Time),
		pooled:    make(map[*peerpkg.Peer]rateSample),
		decay:     make(map[*peerpkg.Peer]float64),
		active:    make(map[*blockStream]struct{}),
		rates:     make(map[*peerpkg.Peer]float64),
		raced:     make(map[chainhash.Hash]time.Time),
	}
}

func (r *streamRegistry) start(hash chainhash.Hash, height int32, owner *peerpkg.Peer, total int64, now time.Time) *blockStream {
	s := &blockStream{hash: hash, height: height, owner: owner, sender: owner, total: total, start: now}
	r.add(s)

	return s
}

// add publishes s. Readers take r.mu and the sink goroutine that built s does not, so a field
// that is not atomic must not change after this unless it is written under r.mu, as path and
// admitWait are.
func (r *streamRegistry) add(s *blockStream) {
	r.mu.Lock()
	r.active[s] = struct{}{}
	r.mu.Unlock()
}

// awaitAdmission marks s as waiting for an admission slot.
func (r *streamRegistry) awaitAdmission(s *blockStream) {
	if r == nil || s == nil {
		return
	}

	s.awaiting.Store(true)
}

// admit ends s's wait for admission at now. A copy that waited has its clock, and its owner's
// silence, start again at now: the wait was this node's, not the peer's. A copy that did not wait
// is left as it is.
func (r *streamRegistry) admit(s *blockStream, now time.Time) {
	if r == nil || s == nil || !s.awaiting.Load() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if now.After(s.start) {
		s.waited += now.Sub(s.start)
		s.start = now
	}

	if at := s.lastRead.Load(); at < now.UnixNano() {
		s.lastRead.Store(now.UnixNano())
	}

	s.awaiting.Store(false)
}

// awaitAdmissionOf and admitOf are awaitAdmission and admit for the stream behind a reader that
// trackBlockStreams handed down. Any other reader is left alone.
func (r *streamRegistry) awaitAdmissionOf(reader io.Reader) {
	if c, ok := reader.(countingReader); ok {
		r.awaitAdmission(c.s)
	}
}

func (r *streamRegistry) admitOf(reader io.Reader, now time.Time) {
	if c, ok := reader.(countingReader); ok {
		r.admit(c.s, now)
	}
}

// awaitingAdmission reports whether the copy of h from p waits for an admission slot.
func (r *streamRegistry) awaitingAdmission(h chainhash.Hash, p *peerpkg.Peer) bool {
	if r == nil || p == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h && s.owner == p && s.awaiting.Load() {
			return true
		}
	}

	return false
}

// couldStart is when p could start sending a block it was asked for at requested and whose first
// byte came at firstByte: the later of the request and the end of p's block before, and never
// after the first byte. A peer sends its queue one block at a time, so a block starts when the
// one ahead of it ends.
func (r *streamRegistry) couldStart(p *peerpkg.Peer, requested, firstByte time.Time) time.Time {
	from := requested

	r.mu.Lock()
	if prev := r.lastBlock[p]; prev.After(from) {
		from = prev
	}
	r.mu.Unlock()

	if from.IsZero() || from.After(firstByte) {
		return firstByte
	}

	return from
}

// finish removes a stream. A complete one adds its block to its owner's next rate sample, timed
// from when the owner could start it (blockStream.from). The block's race mark is left alone, so
// it is never raced twice.
func (r *streamRegistry) finish(s *blockStream, now time.Time, complete bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.active, s)

	if !complete {
		return
	}

	if s.owner == nil {
		return
	}

	r.lastBlock[s.owner] = now

	// Timed from when the owner could start, less this node's own admission wait.
	firstByte := s.start.Add(-s.waited)

	from := s.from
	if from.IsZero() || from.After(firstByte) {
		from = firstByte
	}

	took := now.Sub(from) - s.waited
	if took <= 0 {
		return
	}

	pool := r.pooled[s.owner]
	pool.bytes += s.read.Load()
	pool.took += took

	if pool.took < minRateSample {
		r.pooled[s.owner] = pool

		return
	}

	delete(r.pooled, s.owner)

	bps := float64(pool.bytes) / pool.took.Seconds()
	if bps <= 0 {
		return
	}

	// The previous rate counts as decayed: a peer that went quiet and then sends again is
	// judged on how it has delivered lately, not on the rate it had before it stopped.
	if _, ok := r.rates[s.owner]; ok {
		bps = peerRateWeight*bps + (1-peerRateWeight)*r.rateLocked(s.owner)
	}

	r.rates[s.owner] = bps
	delete(r.decay, s.owner)
}

// decayQuiet cuts the rate of each peer that owes blocks and has sent no block bytes for more than
// rateDecayAfter, halving it every rateDecayHalfLife after that. owedSince is, for each peer that
// owes blocks, when it was asked for the block at the head of its queue; the silence runs from
// the later of that and its last block bytes. A rate measured on blocks a peer no longer sends
// otherwise stayed as it was: a peer that sent one fast block and then stopped kept the top rate,
// kept most of the measured bandwidth, and kept every other peer on standby.
func (r *streamRegistry) decayQuiet(now time.Time, owedSince map[*peerpkg.Peer]time.Time) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for p, since := range owedSince {
		if r.rates[p] <= 0 || r.awaitingFromLocked(p) {
			continue
		}

		if last := r.lastBlockBytesLocked(p); last.After(since) {
			since = last
		}

		quiet := now.Sub(since) - rateDecayAfter
		if quiet <= 0 {
			continue
		}

		f := math.Pow(0.5, quiet.Seconds()/rateDecayHalfLife.Seconds())
		if cur, ok := r.decay[p]; !ok || f < cur {
			r.decay[p] = f
		}
	}
}

// awaitingFromLocked reports whether a copy from owner p waits for an admission slot: p is then
// not quiet, this node is not reading. Called with r.mu held.
func (r *streamRegistry) awaitingFromLocked(p *peerpkg.Peer) bool {
	for s := range r.active {
		if s.owner == p && s.awaiting.Load() {
			return true
		}
	}

	return false
}

// rateLocked is p's rate with any decay applied. Called with r.mu held.
func (r *streamRegistry) rateLocked(p *peerpkg.Peer) float64 {
	bps := r.rates[p]
	if f, ok := r.decay[p]; ok {
		bps *= f
	}

	return bps
}

// pending is how many bytes are still to come on every copy p is sending now, owed or not: they
// all hold up what p sends next. n counts only the blocks p owes, the ones in its ledger queue.
func (r *streamRegistry) pending(p *peerpkg.Peer) (int64, int) {
	if r == nil || p == nil {
		return 0, 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		bytes int64
		n     int
	)

	for s := range r.active {
		if s.sender != p {
			continue
		}

		if s.owner == p {
			n++
		}

		bytes += max(0, s.total-s.read.Load())
	}

	return bytes, n
}

// arriving reports whether bytes of block h are arriving now from a peer that owes it.
func (r *streamRegistry) arriving(h chainhash.Hash) bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h && s.owner != nil {
			return true
		}
	}

	return false
}

// arrivingFrom reports the progress of block h's bytes from peer p, which owes it: read so far,
// its declared size, and when they began. ok is false when none are arriving from p.
func (r *streamRegistry) arrivingFrom(h chainhash.Hash, p *peerpkg.Peer) (read, total int64, start time.Time, ok bool) {
	if r == nil || p == nil {
		return 0, 0, time.Time{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for s := range r.active {
		if s.hash == h && s.owner == p {
			return s.read.Load(), s.total, s.start, true
		}
	}

	return 0, 0, time.Time{}, false
}

// arrivingBytes is the declared size of every block arriving now from a peer that owes it, and how
// many there are. A copy from a peer that does not owe the block is drained, not held, so its
// declared size is not counted against the disk.
func (r *streamRegistry) arrivingBytes() (int64, int) {
	if r == nil {
		return 0, 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		total int64
		n     int
	)

	for s := range r.active {
		if s.owner == nil {
			continue
		}

		total += s.total
		n++
	}

	return total, n
}

// medianRate is the median of the peers' measured rates on completed blocks, or zero with none.
func (r *streamRegistry) medianRate() float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	rates := make([]float64, 0, len(r.rates))
	for p := range r.rates {
		rates = append(rates, r.rateLocked(p))
	}

	if len(rates) == 0 {
		return 0
	}

	sort.Float64s(rates)

	return rates[len(rates)/2]
}

func (r *streamRegistry) peerRate(p *peerpkg.Peer) float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rateLocked(p)
}

// wasRaced reports whether h had an extra copy asked for, by the race or by the queued re-ask,
// which share the mark, less than raceSlowFetchAfter ago. The newest copy then has not had SV
// Node's slow-fetch time, and no further copy is asked for.
func (r *streamRegistry) wasRaced(h chainhash.Hash, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireRacesLocked(now)

	at, raced := r.raced[h]

	return raced && now.Sub(at) < raceSlowFetchAfter
}

func (r *streamRegistry) markRaced(h chainhash.Hash, now time.Time) {
	r.mu.Lock()
	r.raced[h] = now
	r.mu.Unlock()
}

// forgetPeer drops a departed peer's rate and activity.
func (r *streamRegistry) forgetPeer(p *peerpkg.Peer) {
	r.mu.Lock()
	delete(r.rates, p)
	delete(r.lastBlock, p)
	delete(r.pooled, p)
	delete(r.decay, p)
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

	return r.lastBlockBytesLocked(p)
}

// lastBlockBytesLocked is lastBlockBytes with r.mu held.
func (r *streamRegistry) lastBlockBytesLocked(p *peerpkg.Peer) time.Time {
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

// expireRacesLocked drops race marks past their expiry.
func (r *streamRegistry) expireRacesLocked(now time.Time) {
	for h, at := range r.raced {
		if now.Sub(at) > raceExpiry {
			delete(r.raced, h)
		}
	}
}

// pickRace returns the lowest block above the tip whose every copy is struggling, if the chain
// will reach it before it arrives, and the peers sending those copies. Each copy from a peer that
// owes the block is judged, as SV Node judges each peer a block is in flight from: one copy
// younger than raceSlowFetchAfter or at raceStallRate or more keeps the block out of the race.
// A block whose newest extra copy was asked for less than raceSlowFetchAfter ago is left alone
// too. tip is the committed height and commitRate the blocks a second joining the chain; with no
// rate measured the chain is taken to need the block now.
func (r *streamRegistry) pickRace(now time.Time, tip int32, commitRate float64) (*blockStream, raceCandidate, []*peerpkg.Peer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireRacesLocked(now)

	var (
		best     *blockStream
		bestCand raceCandidate
	)

	healthy := make(map[chainhash.Hash]bool)

	for s := range r.active {
		// A copy waiting for admission is not judged, and keeps its block out of the race: its
		// bytes are not read because this node is busy.
		if s.owner != nil && (s.awaiting.Load() || now.Sub(s.start) < raceSlowFetchAfter || s.rate(now) >= raceStallRate) {
			healthy[s.hash] = true
		}
	}

	for s := range r.active {
		if s.height <= tip || s.owner == nil || healthy[s.hash] {
			continue
		}

		if at, raced := r.raced[s.hash]; raced && now.Sub(at) < raceSlowFetchAfter {
			continue
		}

		rate := s.rate(now)
		if rate >= raceStallRate {
			continue
		}

		var need time.Duration
		if commitRate > 0 {
			need = time.Duration(float64(s.height-tip) / commitRate * float64(time.Second))
		}

		eta := time.Duration(1<<63 - 1)
		if remaining := s.total - s.read.Load(); rate > 0 {
			eta = time.Duration(float64(remaining) / rate * float64(time.Second))
		}

		if eta <= need {
			continue
		}

		if best == nil || s.height < best.height {
			best = s
			bestCand = raceCandidate{eta: eta, need: need, rate: rate, age: now.Sub(s.start)}
		}
	}

	if best == nil {
		return nil, raceCandidate{}, nil, false
	}

	var stalling []*peerpkg.Peer

	for s := range r.active {
		if s.hash == best.hash && s.owner != nil {
			stalling = append(stalling, s.owner)
		}
	}

	return best, bestCand, stalling, true
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
		ra, rb := r.rateLocked(a), r.rateLocked(b)
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

		// The stream is the sender's, not the ledger owner's: a peer that does not owe the block
		// could otherwise set an owner's rate, keep a stalled owner looking busy, and set the
		// largest recent block size. owingSender names the sender only when it owes the block.
		now := time.Now()
		owner := sm.owingSender(r, hash)

		sender := deliveringPeer(r)
		if sender != nil && sm.peerStates != nil {
			_, sender, _ = sm.peerStateResolvingPrimary(sender)
		}

		s := &blockStream{hash: hash, height: height, owner: owner, sender: sender, total: n, start: now, received: &sm.waste.received}
		if at, ok := sm.blockDownloads.RequestedAt(hash); ok {
			s.requestedAt = at
		}

		if owner != nil {
			asked, _ := sm.blockDownloads.RequestedOf(owner, hash)
			s.from = sm.streams.couldStart(owner, asked, now)
		}

		sm.streams.add(s)

		converted, err := inner(hash, header, countingReader{r: r, s: s}, n)

		now = time.Now()

		// Complete when the sink succeeded. The reader starts after the 80-byte header, so the
		// bytes counted never reach n, and a test against n counted no stream as complete.
		complete := err == nil
		sm.streams.finish(s, now, complete)

		sm.streams.mu.Lock()
		drained := s.path == admitRawDuplicate || s.path == admitLocalFault || s.path == admitNotOwed
		sm.streams.mu.Unlock()

		switch {
		case !complete:
			sm.waste.streamsFailed.Add(1)
			sm.waste.bytesWasted.Add(s.read.Load())
		case drained:
			sm.waste.bytesWasted.Add(s.read.Load())
		}

		// The size ladder and the queue estimate read the average block size. Only the path
		// that decodes a whole block used to feed it, and with the park on that path never
		// runs, so every block has to feed it here. Only a converted copy from a peer that
		// owes the block: any peer can declare any size for a block it was not asked for, and a
		// drained copy of a block another copy converted would count that block twice.
		if complete && converted && s.owner != nil && sm.blockSizeTracker != nil {
			sm.blockSizeTracker.addBlockSize(n)
		}

		// The committed tip is read only for a download worth reporting: it is a blockchain
		// call, and this runs on the peer's read loop for every block.
		sm.streams.mu.Lock()
		_, worth := s.report(now, s.height)
		sm.streams.mu.Unlock()

		if worth {
			// The lead is only printed for a block whose height is known, so the
			// tip is only read for one.
			var tip int32
			if s.height > 0 {
				tip, _, _ = sm.committedTip()
			}

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

	ticks := 0

	for {
		select {
		case <-sm.quit:
			return
		case <-ticker.C:
			sm.decayQuietRates(time.Now())
			sm.maybeRaceSlowBlock(time.Now())
			sm.maybeReaskQueuedBlock(time.Now())

			if ticks++; ticks%queueReportEvery == 0 {
				sm.logDownloadQueues()
			}
		}
	}
}

// decayQuietRates cuts the rate of each peer that owes blocks and has gone quiet
// (streamRegistry.decayQuiet). It runs on the race's ticker, ahead of the rules that read rates.
func (sm *SyncManager) decayQuietRates(now time.Time) {
	if sm.streams == nil || sm.blockDownloads == nil {
		return
	}

	owedSince := make(map[*peerpkg.Peer]time.Time)

	for p, queue := range sm.blockDownloads.Queues() {
		if len(queue) > 0 {
			owedSince[p] = queue[0].at
		}
	}

	sm.streams.decayQuiet(now, owedSince)
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

	s, c, stalling, ok := sm.streams.pickRace(now, tip, sm.commitRate.rate())
	if !ok {
		return
	}

	// At the cap no extra copy is asked for, but each stalling copy is still dropped, as SV Node's
	// DetectStalling drops a staller whatever its parallel fetch count (net_processing.cpp:5446-5466).
	// Before, the cap returned first: two forgiven owners and one copy at 50 KB/s left the block
	// to the peer layer's deadline, an hour or more.
	owners := sm.blockDownloads.OwnersOf(s.hash)
	if copies := sm.blockCopies(s.hash, owners); copies >= maxBlockCopies {
		sm.dropStallingCopies(s, stalling)
		sm.logger.Infof("[frontierRace][%s] dropped %v, which were sending block %d at under %.0f KB/s; %d live copies, so no other peer was asked", s.hash, stalling, s.height, float64(raceStallRate)/1e3, copies)

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

	racer := sm.streams.chooseRacer(candidates, owners, sm.blockDownloads.CountForPeer)
	if racer == nil {
		sm.logger.Debugf("[frontierRace][%s] block %d is arriving at %.0f KB/s but there is no other peer to ask", s.hash, s.height, c.rate/1e3)

		return
	}

	if !sm.askRacer(racer, s.hash, now) {
		return
	}

	// Each peer sending a copy is struggling, and each is dropped as SV Node drops a staller. The
	// copy converting stops, which frees the block for the extra copy; left connected, the extra
	// copy would arrive as a duplicate and be drained unwritten.
	sm.dropStallingCopies(s, stalling)

	sm.logger.Infof("[frontierRace][%s] asked %s for block %d and dropped %v, which were sending it at under %.0f KB/s; one at %.0f KB/s after %s",
		s.hash, racer, s.height, stalling, float64(raceStallRate)/1e3, c.rate/1e3, c.age.Round(time.Second))
}

// dropStallingCopies disconnects each peer sending a struggling copy of s's block.
func (sm *SyncManager) dropStallingCopies(s *blockStream, stalling []*peerpkg.Peer) {
	for _, p := range stalling {
		p.DisconnectWithInfo(fmt.Sprintf("stalling on block %d at under %.0f KB/s", s.height, float64(raceStallRate)/1e3))
	}
}

// blockCopies is how many live copies of h there are among owners: an owner not let off the
// block, or one whose copy is arriving now. A forgiven owner sending nothing will not deliver
// (ownerArrival reads it as far off), and counting it held the race and the queued re-ask off a
// block whose only copy was stalling.
func (sm *SyncManager) blockCopies(h chainhash.Hash, owners []*peerpkg.Peer) int {
	active, _ := sm.blockDownloads.ActiveOwners(h)

	n := 0

	for _, o := range owners {
		if slices.Contains(active, o) {
			n++

			continue
		}

		if _, _, _, arriving := sm.streams.arrivingFrom(h, o); arriving {
			n++
		}
	}

	return n
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

// queueReportEvery is how many race checks pass between download queue reports: every 30 s.
const queueReportEvery = 6

// logDownloadQueues reports, for each eligible peer, what it owes and what it is sending, and in
// one summary line how many peers are idle or below streamingPeerDepth requests, and the bytes
// really held ahead of the chain against the disk backstop. The download is judged on that line:
// no peer below two requests unless the backstop is reached.
//
// The queue lines describe the headers-first scheduler, so they are printed only in
// headers-first mode. Above the last checkpoint blocks arrive on the inv path, which never
// uses the scheduler, and every peer reads as idle there whatever it is doing. The
// download-waste line counts every delivery in every mode and is always printed.
//
// The same tick publishes the download gauges and counters, ahead of the guard below,
// because publishDownloadMetrics copes with each of the fields it checks being nil.
func (sm *SyncManager) logDownloadQueues() {
	sm.publishDownloadMetrics()

	if sm.streams == nil || sm.blockSizeTracker == nil || sm.blockDownloads == nil {
		return
	}

	if sm.headersFirstMode.Load() {
		sm.logSchedulerQueues()
	}

	w := &sm.waste
	sm.logger.Infof("[downloadWaste] since start: received %.1f GB; duplicate copies drained %d, converted %d; copies drained for this node's own store faults %d; streams cut part way %d; %.1f GB wasted; peers dropped owing blocks %d (%d blocks); blocks re-asked after a quiet peer %d, behind a slow queue or arriving late %d",
		float64(w.received.Load())/1e9, w.dupDrained.Load(), w.dupConverted.Load(), w.localFaultDrained.Load(), w.streamsFailed.Load(),
		float64(w.bytesWasted.Load())/1e9, w.droppedOwing.Load(), w.blocksOwedAtDrop.Load(), w.reAskedQuiet.Load(), w.reAskedQueued.Load())
}

// publishDownloadMetrics sets the download gauges, blocks owed, heights the header cache names
// and bytes held ahead of the chain, and adds the waste counters' increase since the last call.
// The report tick is its only production caller, so the gauges lag by up to 30 seconds.
func (sm *SyncManager) publishDownloadMetrics() {
	if prometheusLegacyNetsyncBlocksOwed == nil {
		return
	}

	prometheusLegacyNetsyncBlocksOwed.Set(float64(sm.blockDownloads.Len()))
	prometheusLegacyNetsyncHeaderCacheHeights.Set(float64(sm.headerCache.Len()))

	var largest int64
	if sm.blockSizeTracker != nil {
		largest = sm.blockSizeTracker.largestRecentSize()
	}

	prometheusLegacyNetsyncBytesAhead.Set(float64(sm.bytesAhead(largest)))

	sm.waste.publish()
}

// logSchedulerQueues is logDownloadQueues' per-peer lines and summary, for the headers-first
// scheduler.
func (sm *SyncManager) logSchedulerQueues() {

	largest := sm.blockSizeTracker.largestRecentSize()
	eligible := sm.eligibleBlockPeers()
	idle, short := 0, 0
	depth := sm.streamingPeerDepth()
	warming := sm.downloadWarming(time.Now())
	var fastest float64
	for _, bp := range eligible {
		fastest = max(fastest, sm.streams.peerRate(bp.peer))
	}

	for _, bp := range eligible {
		owed := sm.blockDownloads.CountForPeer(bp.peer)
		remaining, sending := sm.streams.pending(bp.peer)

		peerDepth := sm.peerQueueDepth(bp.peer, depth, fastest, warming)

		if owed == 0 && sending == 0 {
			idle++
		}

		if owed < peerDepth {
			short++
		}

		sm.logger.Infof("[downloadQueue] %s owes %d of %d, sending %d with %.0f MB left, rate %.1f MB/s",
			bp.peer, owed, peerDepth, sending, float64(remaining)/1e6, sm.streams.peerRate(bp.peer)/1e6)
	}

	sm.logger.Infof("[downloadQueue] %d eligible peers, %d idle; %d below their speed-scaled depth of up to %d requests; %.1f GB held ahead of the chain, parked and arriving, against a %.1f GB backstop; %d blocks owed; receiving %.1f MB/s",
		len(eligible), idle, short, depth, float64(sm.bytesAhead(largest))/1e9, float64(parkBackstopBytes)/1e9, sm.blockDownloads.Len(), sm.waste.rateSinceLast(time.Now())/1e6)
}
