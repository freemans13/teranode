package netsync

import (
	"bytes"
	"context"
	"io"
	"math/big"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// Why this file exists.
//
// The wire layer can already read a block's body straight from the socket to a
// store without ever building it as a Go object, and every piece of that was
// written and left switched off: the handler is registered at startup, but it
// checks for a sink and a gate before streaming anything and neither was ever
// installed, so every block on every node has taken the decoding path. This is
// what installs them.
//
// It matters more than a saved allocation. A decoded block reserves download
// budget by its serialized size, and a block larger than that whole budget is
// clamped to it, so it can only proceed once the budget is completely empty.
// The semaphore behind that budget stops at the first waiter it cannot satisfy
// and leaves every waiter behind it blocked, which its own documentation says is
// deliberate. Each of those waiters is a peer's read loop, and a blocked read
// loop reads nothing further from its socket. Measured on Hetzner mainnet on
// 2026-09-10: one goroutine holding the queue head for two minutes against a
// 319 MB block, five peer read loops stacked behind it, and sixteen of the last
// two hundred blocks large enough to do the same thing. A streamed body never
// reserves that budget, because there is nothing in memory to bound.

// streamedBodyRequestWindow is how recently this node must have asked for a
// block before a peer may write its body to our disk.
//
// It is generous on purpose. The gate's job is to refuse bodies nobody asked
// for, not to enforce timeliness: a body that arrives late is still a body this
// node wanted, and refusing it means paying for the download again. The
// tracker's own assignment records expire well before this, so in practice the
// tracker is the tighter bound and this is the backstop.
const streamedBodyRequestWindow = 60 * 60 * 1000000000 // one hour, in nanoseconds

// installStreamingBlockPath wires the three functions the wire layer needs
// before it will stream a block body to disk instead of decoding it. Called
// once, from the sync manager's construction, and only when the park is
// available: the park's store is where a streamed body goes, and its entry
// table is what makes the body findable afterwards.
//
// Installing a sink without a gate is refused by the wire layer itself, which
// falls back to decoding rather than opening the door. This installs all three
// together for the same reason.
//
// set also carries whether the wire layer may ignore its size threshold. The
// two decisions are made from the same PipelineReceive check and passed on the
// same call, so a sink can never be installed with the wrong size policy: the
// park sink only pays for a block too large to hold in memory, but the
// pipeline sink converts a block as it arrives and pays at every size.
func (sm *SyncManager) installStreamingBlockPath(set func(
	sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
	gate func(chainhash.Hash, *wire.BlockHeader) error,
	del func(chainhash.Hash) error,
	streamsEverySize bool,
)) {
	if sm == nil || sm.blockPark == nil || !sm.blockPark.Enabled() {
		return
	}

	// A nil settings pointer here is not a recoverable nil panic the way it
	// would be for most fields: the reviewer measured Settings.Legacy at
	// offset 4728 and PipelineReceive at 5112, both past the 4096-byte guard
	// page a Go process leaves unmapped at low addresses, so reading through
	// a nil settings pointer this far in can fault outside the range the
	// runtime is guaranteed to turn back into an ordinary panic. Computed
	// once here, matching the sm.settings != nil && pattern already used
	// elsewhere in this package (e.g. handle_block.go's
	// quickValidationAllowed), and reused below rather than re-read, so
	// there is exactly one place that can get this wrong instead of three.
	pipelineOn := sm.settings != nil && sm.settings.Legacy.PipelineReceive

	sink := sm.streamingBlockSink
	del := sm.streamingBlockDelete

	if pipelineOn {
		// admitPipelineSink wraps the download-admission budget around the
		// pipeline sink. See that method's doc comment for why it has to wrap
		// the sink itself rather than being charged in the on-disk message
		// handler that runs after the sink has already finished.
		sink = sm.admitPipelineSink(sm.pipelineBlockSink)
		// The delete callback is chosen from the same check, on the same
		// call, as the sink: whichever sink actually wrote something for a
		// hash is the only thing that knows how to clean it up again, so a
		// sink can never be installed with the wrong delete callback any
		// more than it can be installed with the wrong size policy (see
		// streamsEverySize below). streamingBlockDelete only knows about the
		// park's blob store; the pipeline sink instead writes subtree files,
		// so it needs pipelineBlockDelete's own cleanup.
		del = sm.pipelineBlockDelete
	}

	sm.logger.Infof("[legacy] streaming block path installed, pipeline=%v", pipelineOn)

	set(sink, sm.streamingBlockGate, del, pipelineOn)
}

// admitPipelineSink wraps inner (the pipeline sink) with the download-admission
// budget AcquireBlockPrefetch/ReleaseBlockPrefetch already implement, charged
// one slot per block on this path (see AcquireBlockPrefetch's own
// PipelineReceive branch, manager.go). That budget was sized for exactly this
// call site — sm.blockPrefetchBudgetBytes is derived from
// MaxBlocksInTransitPerPeer specifically when PipelineReceive is on — but was
// never reachable from it: AcquireBlockPrefetch is only ever called from
// OnBlock, which the peer only dispatches for a whole *wire.MsgBlock, and with
// the pipeline on every block comes back as *peer.MsgBlockOnDisk instead, so
// OnBlock, and the admission check inside it, never runs for this route.
//
// Charged here, wrapping the sink itself, NOT in handleBlockOnDiskMsg (the
// on-disk message handler that runs once the body is already fully on disk).
// By the time that handler's message even exists, the conversion this budget
// is meant to bound is already finished: inner has already streamed the whole
// body through the subtree builder and its ~50MB dedup map
// (newPipelineDedupMap, pipeline_sink.go) on this peer's own read-loop
// goroutine. A charge that only runs after that resident cost has already been
// paid bounds nothing real — which is the exact lesson the byte-budget version
// of this same check already taught (ReleaseBlockPrefetchBytes's own doc
// comment): charging after the fact cannot be un-taught by moving the charge to
// a different post-hoc call site. Charging before inner runs instead blocks the
// read loop that would otherwise start that work, the same trade-off OnBlock
// already accepts for the decoded path: a peer over budget reads nothing
// further until a slot frees.
//
// ctx is sm.ctx, not a per-peer context, and quit is nil: the wire layer's
// external message handler (services/legacy/peer/wire_streaming.go) calls the
// installed sink with only a hash, a header and a reader — no peer and no
// per-connection quit channel are in scope at that layer (streamingBlockGate's
// own doc comment notes the identical limitation for the gate, which runs one
// step earlier in the same call). The accepted cost: a read loop parked here
// unblocks on budget freeing up or on daemon shutdown, but not on that one
// peer's own teardown — unlike OnBlock's acquire, which is handed sp.quit for
// exactly that. A parked, now-dead peer's read-loop goroutine therefore
// lingers until one of those two things happens rather than exiting the moment
// its connection drops. Threading a per-connection quit channel this deep would
// mean changing the wire layer's sink signature, which is out of this task's
// scope; the trade-off is bounded (one goroutine, not an unbounded leak) and is
// recorded here rather than fixed quietly.
func (sm *SyncManager) admitPipelineSink(inner func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)) func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
	return func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
		weight, err := sm.AcquireBlockPrefetch(sm.ctx, nil, hash, n)
		if err != nil {
			if errors.Is(err, ErrDuplicateBlockInFlight) {
				// A copy of this hash is already being converted (or waiting
				// for budget) by another peer's read loop — the frontier
				// race's own duplicate delivery, not a fault. It must not
				// disconnect this peer or fail this message: decline the
				// conversion and defer to the plain body-write path, exactly
				// as pipelineBlockSink's own unresolvable-parent and
				// not-legacyUnified declines already do (see that function's
				// doc comment) — r is still untouched at this point, so
				// handing it to streamingBlockSink is safe. The existing
				// park/drain machinery (AdoptWritten's "already hold this
				// block" branch) resolves the resulting duplicate body
				// exactly as it always has.
				return sm.streamingBlockSink(hash, header, r, n)
			}

			// ctx cancelled (shutdown): nothing was reserved, nothing to
			// convert.
			return false, err
		}
		defer sm.ReleaseBlockPrefetch(hash, weight)

		return inner(hash, header, r, n)
	}
}

// streamingBlockGate answers whether a peer may write this block's body to our
// disk, and is the only check that runs before the bytes land. Nothing
// downstream can refuse a write that has already happened.
//
// Three questions, in the order that makes each of the later ones meaningful.
//
// First, did this node ask for this block. A peer that can choose what to write
// to our disk can fill it, and no amount of later verification gets the space
// back. The check is by hash rather than by peer because the wire handler is
// registered globally with go-wire and has no peer in scope; a body for a hash
// we asked somebody for is a body we wanted, and which peer answered is settled
// later on the same paths that already settle it for a decoded block.
//
// Second, is the header's declared target at least as hard as the chain's own
// limit. This is the check without which the third one gates nothing: a header
// carries the target it claims to meet, so a peer picks an easy one and always
// passes. This codebase's own hardening work measured 64 of 64 forged headers
// passing a target check that lacked this floor.
//
// Third, does the header actually meet that now-bounded target. This is the
// work that makes minting distinct block hashes expensive, which is what stops
// an attacker filling the park with unlimited fabrications.
//
// The body itself is not checked here and cannot be: it has not been read yet.
// A body that turns out to be rubbish is caught when the park reads it back and
// the block's transactions are parsed and its merkle root rebuilt, exactly as
// for a decoded block. What the gate buys is that the bytes on our disk are
// always bytes we asked for, under a hash somebody paid real work to produce.
func (sm *SyncManager) streamingBlockGate(hash chainhash.Hash, header *wire.BlockHeader) error {
	if header == nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] no header", hash)
	}

	// The body is filed under hash, so a hash the header does not produce would
	// put bytes on disk under a name that is not theirs — and every later reader
	// trusts the name.
	if got := header.BlockHash(); !got.IsEqual(&hash) {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] the header hashes to %s", hash, got)
	}

	if sm.blockDownloads == nil || !sm.blockDownloads.RequestedWithin(hash, streamedBodyRequestWindow) {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] this node did not ask for this block", hash)
	}

	// No chain means no floor to check against, and an unbounded write is the
	// wrong thing to default to.
	if sm.chainParams == nil || sm.chainParams.PowLimit == nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] no chain parameters, so there is no difficulty floor", hash)
	}

	// A larger target is easier work. Refusing a target above the chain's limit
	// is what bounds the header's own claim before it is tested against it.
	target := blockchain.CompactToBig(header.Bits)
	if target == nil || target.Sign() <= 0 || target.Cmp(sm.chainParams.PowLimit) > 0 {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] declared target %s is easier than the chain limit %s",
			hash, describeTarget(target), sm.chainParams.PowLimit)
	}

	var headerBytes bytes.Buffer
	if err := header.Serialize(&headerBytes); err != nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] could not serialize the header", hash, err)
	}

	modelHeader, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] could not read the header", hash, err)
	}

	if met, _, err := modelHeader.HasMetTargetDifficulty(); !met {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] the header does not meet its own target", hash, err)
	}

	return nil
}

// describeTarget renders a target for a refusal message, including the nil case
// CompactToBig can produce, so the log line never says "<nil> is easier than".
func describeTarget(t *big.Int) string {
	if t == nil {
		return "unreadable"
	}

	return t.String()
}

// streamingBlockSink writes a streamed body to the park's store, in exactly the
// form the park writes a decoded block: header, transaction count, transactions,
// under the block's own hash.
//
// Byte-identical is the whole point. The park reads a body back with the same
// deserializer whichever path put it there, so a streamed block needs no second
// read path, no second file type and no flag distinguishing the two. The wire
// layer hands in the parsed header rather than header bytes now, because the
// pipeline sink that follows this one needs it as structure; this sink's own job
// is bytes, so it re-serializes the header back onto the front of the body.
//
// The bool it returns is always false: this path never converts a block, it
// only ever writes the whole body byte-for-byte under FileTypeMsgBlock, so
// nothing downstream may treat what it wrote as a converted record — see
// BlockBody.Converted.
func (sm *SyncManager) streamingBlockSink(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
	if sm.blockPark == nil {
		return false, errors.NewProcessingError("[streamingBlockSink][%s] no park to write to", hash)
	}

	// The park's file format is a whole serialized block, so this path puts the
	// header back in front of the body. The wire layer stopped doing that when the
	// sink contract began carrying the header as structure, because the pipeline
	// sink wants the structure and this one wants the bytes.
	var headerBytes bytes.Buffer
	if err := header.Serialize(&headerBytes); err != nil {
		return false, errors.NewProcessingError("[streamingBlockSink][%s] could not re-serialize the header", hash, err)
	}

	return false, sm.blockPark.WriteStreamedBody(sm.ctx, hash, io.MultiReader(bytes.NewReader(headerBytes.Bytes()), r), n)
}

// streamingBlockDelete removes a body already written under hash, for the case
// where the write succeeded and the block only then turned out unusable — a
// stream ending short of what it declared, or a transaction count that will not
// parse.
//
// An orphaned body is worse than a failed download. A failed download is simply
// asked for again by the walk, while bytes sitting on disk under a well-formed
// hash look legitimate to everything downstream, and nothing there knows to
// distrust them.
func (sm *SyncManager) streamingBlockDelete(hash chainhash.Hash) error {
	if sm.blockPark == nil {
		return nil
	}

	sm.blockPark.Delete(context.Background(), parkedBlock{hash: hash})

	return nil
}

// parkFileType is the one file type a parked block is stored under, named here
// so the streaming sink and the park's own writer cannot drift apart.
var parkFileType = fileformat.FileTypeMsgBlock

// blockOnDiskMsg tells the consumer that a block's body reached the park's store
// straight off the wire, so the park needs an entry for bytes that are already
// down.
//
// It carries no block and no reader. That is the point of the whole path: the
// body never existed as a Go object, so there is nothing here to charge against
// the download budget and nothing for a read loop to wait on.
type blockOnDiskMsg struct {
	body peerpkg.BlockBody
	peer *peerpkg.Peer
}

// QueueBlockOnDisk hands a streamed block to the consumer. It mirrors QueueBlock
// and deliberately takes no reply channel and no hand-off channel: the peer's
// read loop is already free by the time this is called, because the bytes went
// to disk as they arrived rather than into a buffer somebody has to release.
func (sm *SyncManager) QueueBlockOnDisk(body peerpkg.BlockBody, peer *peerpkg.Peer) {
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	sm.msgChan <- &blockOnDiskMsg{body: body, peer: peer}
}

// handleBlockOnDiskMsg registers a streamed block with the park and asks for its
// parent's children to be looked at.
//
// Consumer goroutine only. The drain queue is that goroutine's own state, and
// the park's admission ordering is what the rest of this machinery assumes; five
// separate stalls this week came from one of those being touched from elsewhere.
//
// The drain request is what stops a streamed block waiting on the sweep. The
// sweep runs every thirty seconds, which would be added to every block large
// enough to stream whose parent is already in the chain — the common case, not a
// corner.
func (sm *SyncManager) handleBlockOnDiskMsg(msg *blockOnDiskMsg) {
	if msg == nil || sm.blockPark == nil {
		return
	}

	// The streamed route never touched sm.blockDownloads, the map
	// handleBlockMsg releases via RemoveOwner/ForgiveOwners (manager.go) the
	// moment it dequeues a decoded block. Before this branch made the pipeline
	// send every block down this route, that gap only missed blocks above the
	// decode-size threshold (rare); with PipelineReceive on it is every block,
	// so a delivering peer's CountForPeer sticks at MaxBlocksInTransitPerPeer
	// after roughly sixteen deliveries and the scheduler (block_scheduler.go:144)
	// stops asking that peer for anything until the hour-long assignment TTL
	// expires.
	//
	// Released HERE — at message intake into this single consumer, the same
	// point handleBlockMsg releases it, NOT at the block's eventual commit.
	// By the time this message exists the peer has already fully answered: the
	// sink ran to completion on the peer's own read loop before
	// QueueBlockOnDisk was ever called, so there is no copy still being
	// converted for a duplicate to race against — releasing later, after
	// AdoptWritten or the eventual park commit (either of which an unreachable
	// parent or a full park can delay indefinitely), would only widen the
	// leak's own window instead of closing it. Unconditional on what happens
	// below: the peer earned the release by delivering the bytes, whether this
	// node ends up keeping them (AdoptWritten) or discarding them (unreachable
	// parent, full park).
	//
	// Gated on PipelineReceive: before this branch, a block only reached this
	// on-disk route above the 64 MiB decode threshold, and that pre-existing
	// (rare) behaviour is not this task's to change — only the route this
	// branch made universal is.
	if sm.settings != nil && sm.settings.Legacy.PipelineReceive {
		sm.blockDownloads.RemoveOwner(msg.peer, msg.body.Hash)
		sm.blockDownloads.ForgiveOwners(msg.body.Hash, blockRequestRetryInterval)
	}

	entry := parkedBlock{
		hash:      msg.body.Hash,
		prevBlock: msg.body.Header.PrevBlock,
		size:      msg.body.Size,
		peer:      msg.peer,
	}

	// msg.body.Converted says whether THIS delivery's sink actually converted
	// the block, straight from the sink's own return value — see
	// BlockBody.Converted. It is deliberately NOT inferred by asking whether a
	// converted record happens to exist for this hash: a record surviving from
	// an unrelated earlier attempt, or from a racing duplicate delivery of the
	// same hash, would look identical to one this delivery produced, and
	// charging by that inference once misattributed a stale or foreign
	// record's size to a delivery that never wrote it.
	//
	// Gating the store lookup on this flag also means the lookup itself never
	// runs unless the pipeline actually converted something on THIS call: with
	// PipelineReceive off, streamingBlockSink is the only sink ever installed
	// and it always reports converted=false (see its own doc comment), so this
	// whole block below is skipped and handleBlockOnDiskMsg does exactly the
	// store I/O it always did — none — on that path. Before this gate existed,
	// the lookup ran for every streamed block regardless of the setting, which
	// meant an extra blob-store round trip, and therefore an extra wait on the
	// store's shared read-permit pool, on the single goroutine that commits
	// blocks in order — a path that used to do no I/O at all.
	if msg.body.Converted {
		// The pipeline sink, when it is the one active, has already written
		// this hash's body as a converted record — a few hundred bytes under
		// FileTypeBlock — before the wire layer ever got here, so there is
		// nothing on disk shaped like the whole block msg.body.Size describes.
		// Charging that size against the park's budget would over-charge a
		// pipelined block by orders of magnitude and starve the park into
		// believing it is nearly full when it holds almost nothing, so the two
		// paths deliberately charge different numbers: the plain streaming
		// path charges the whole block's wire size because that is what
		// write/WriteStreamedBody put on disk, and this charges the record's
		// own length instead.
		if size, found, err := sm.blockPark.convertedRecordSize(sm.ctx, entry.hash); err != nil {
			sm.logger.Warnf("[blockOnDisk][%s] failed to check for a converted record, charging the whole block's wire size instead: %v", entry.hash, err)
		} else if found {
			entry.size = size
		} else {
			// The sink says it converted this hash, but the record is not
			// there. Something else already removed it — for example a racing
			// discard on the same hash — between the sink returning and this
			// handler running. Charging the whole block's wire size here would
			// be wrong the OTHER way for a genuinely converted block, but there
			// is no better number left to charge, so this falls back to it and
			// says so rather than silently mischarging.
			sm.logger.Warnf("[blockOnDisk][%s] the sink reports this delivery converted, but no converted record is on disk; charging the whole block's wire size instead", entry.hash)
		}
	}

	// A parked block is only ever committable if its parent is something this
	// node is going to get: already in the chain, or still ahead of us in the
	// header list because we asked for it. A block above a hole that is in
	// neither is unreachable, and the drain will keep offering it to the chain
	// for as long as it is held.
	//
	// Measured on mainnet on 2026-09-10, and caused by this path: three streamed
	// blocks whose parents were never in the header list produced 23,111 "the
	// parent is missing again" retries in two hours, against zero on each of the
	// three preceding days. Each retry is a store lookup on the goroutine that
	// commits blocks, so an unreachable block does not merely sit there, it
	// competes with the work the operator is waiting for.
	//
	// The decoded path never had to ask this question, because a block only
	// reaches its park call after handleBlockMsg has walked the header list for
	// it. Streaming skips that walk by design, which is the point of it, so the
	// question has to be asked here instead.
	if !sm.parentIsReachable(entry.prevBlock) {
		sm.logger.Infof("[blockOnDisk][%s] parent %s is neither in the chain nor in the header list, so this block is unreachable; discarding the body",
			entry.hash, entry.prevBlock)

		sm.blockPark.Delete(sm.ctx, entry)

		return
	}

	// Said at info, once per streamed block, because without it there is no way
	// to tell from a running node whether this path is carrying anything at all.
	// Both paths end with a body in the same store under the same name, so the
	// park's own files cannot answer it and neither can the block's size.
	sm.logger.Infof("[blockOnDisk][%s] body streamed to disk, %d bytes, %d txs, parent %s",
		entry.hash, entry.size, msg.body.TxCount, entry.prevBlock)

	if !sm.blockPark.AdoptWritten(entry) {
		// Either we already hold this block, in which case the body on disk is
		// the one the existing entry points at and there is nothing to do, or
		// the park is full, in which case the bytes are an orphan: on disk under
		// a well-formed hash with nothing pointing at them. Delete covers both,
		// because deleting a body we already hold would take the live one with
		// it — so only the second case may delete.
		if !sm.blockPark.Has(entry.hash) {
			sm.blockPark.Delete(sm.ctx, entry)
		}

		return
	}

	// Hand the drain request to the consumer rather than queueing it here.
	//
	// This runs on blockHandler, and drainQueue is owned by the dispatchBlocks
	// consumer alone with no lock, on the stated invariant that every producer of
	// a drain request already runs on that goroutine. Calling scheduleDrain from
	// here broke that twice over: it raced the queue, and it could not wake a
	// consumer already asleep in its select, so a streamed block whose parent was
	// already committed parked and stayed there.
	//
	// Measured on mainnet on 2026-09-10: block 783,942 committed at 15:21:00, a
	// 177 MB block whose parent was that block streamed to disk at 15:21:17, and
	// nothing committed it. The operator's own probe read idle with 115 blocks
	// parked holding 4.9 GB.
	//
	// parkCommits is the existing route for precisely this, used by the sweep. Its
	// arm on the consumer restores the entry, which is a no-op for one already
	// registered, and then schedules the drain on the goroutine that owns the
	// queue. Sending also wakes a sleeping consumer, which queueing never could.
	// Only ask for a drain when the parent is actually IN THE CHAIN. A parent
	// that is merely parked means this block is not committable yet, and its own
	// commit will schedule the drain when it lands.
	//
	// parentIsReachable above is a different question and deliberately looser: a
	// parked parent makes the body worth keeping. Using that same answer to
	// trigger a drain sent the consumer after blocks whose parent was not
	// committed, and each attempt fails with the parent missing and costs a store
	// lookup on the goroutine that commits blocks. Measured on mainnet on
	// 2026-09-10 in one 45-second window with nothing committing: five blocks
	// streamed to disk and six of those failures.
	//
	// The sweep already applies this rule before it posts, which is why it does
	// not produce them.
	if !sm.parentIsInChain(entry.prevBlock) {
		return
	}

	sm.submitParkCommit(parkCommit{entry: entry})
}

// parentIsInChain reports whether this node has committed the parent, which is
// the condition for the block being committable now rather than merely worth
// keeping. Consumer goroutine only, like its caller.
func (sm *SyncManager) parentIsInChain(parent chainhash.Hash) bool {
	if sm.blockchainClient == nil {
		return false
	}

	_, _, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &parent)

	return err == nil
}

// parentIsReachable reports whether a parked block's parent is something this
// node will end up holding: in the chain now, or still ahead of us in the header
// list because we asked for it.
//
// Consumer goroutine only. It takes headerMu for the index lookup alone and
// releases it before the store call, because every other reader of the list
// holds that lock and a blocking client call underneath it would serialise the
// whole sync path.
func (sm *SyncManager) parentIsReachable(parent chainhash.Hash) bool {
	// The park first, and this was the omission that made this function a hole
	// factory. A parked parent is a block we already hold on disk, waiting for
	// ITS own parent, so a child of it is exactly as reachable as a child of a
	// committed block. Leaving the park out meant discarding bodies whose parents
	// were sitting a few inches away.
	//
	// Measured on mainnet on 2026-09-10, hours after this function shipped:
	// sixteen of seventeen holes had their parent in the park at that moment, and
	// there was not one discard in four days of log before this deployed. It also
	// formed a loop with the frontier race, which re-requests the missing block,
	// receives it, and has it discarded again: one hash went round four times on a
	// thirty-second period, and 14 GB of the 28.7 GB streamed in the log is that
	// loop.
	if sm.blockPark.Has(parent) {
		return true
	}

	sm.headerMu.Lock()
	_, inList := sm.headerIndex[parent]
	sm.headerMu.Unlock()

	if inList {
		return true
	}

	if sm.blockchainClient == nil {
		return true
	}

	// Not in the list, so the only way it is reachable is that we hold it
	// already. A store error reads as reachable: refusing a block because our
	// own storage was briefly unwell would throw away a completed download over
	// a condition that is over in seconds, which is the same judgement the
	// drain's retry-later disposition makes.
	_, _, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &parent)
	if err == nil {
		return true
	}

	return !errors.Is(err, errors.ErrBlockNotFound) && !errors.Is(err, errors.ErrNotFound)
}
