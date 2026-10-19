package utxoset

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// SpendAndCreateMulti writes the NET effect of a list of transactions.
//
// The list is ordered parents first, and a block's transactions mostly spend each other: on
// mainnet above the checkpoint nearly every input of a chained block names a parent in the same
// block. Written one transaction at a time, every such output is inserted into the UTXO table,
// then deleted from it by its child moments later, and the delete copies it into the spend
// journal. The insert and the delete each touch the UTXO table's index, and that index is the
// working set that does not fit in memory. This method skips both: an output created and spent
// inside one chunk of the list never becomes a UTXO row. Its child's spend of it is written
// straight to the spend journal, which is the one row a spend leaves behind anyway.
//
// Above the checkpoint the list is written as whole families in self-contained chunks; see
// writeFamilies. Every chunk is one database transaction that spends its transactions' outside
// coins and stores the transactions together, so every commit leaves a true state: a transaction
// is stored if and only if its spends are made. There is nothing to give back after a failure
// and nothing to recover after a crash, and no writer needs to know whether another writer of the
// same transaction exists or is alive.
//
// At or below the checkpoint, where quick validation applies a block with its block carried from
// birth and outpoint-only spends, the list keeps the two-step write: step 1 spends every outside
// coin of the whole list in chunks, and step 2 stores the transactions in dependency-level
// order. There is a gap between the steps, with spends and no transaction, and it is harmless
// there: a block at or below the checkpoint is valid by rule and is never abandoned, so a retry
// of the list always comes, repeats the spends as the same spends and finishes the write. That
// route exists because a below-checkpoint block can be one chain tens of thousands of
// transactions deep, which family chunking would cut every 256 transactions. See routesToTwoStep.
//
// It does NOT net, and hands the list to the per-transaction default instead, for a list whose
// options the netted write does not model: create-only or spend-only, and frozen, conflicting or
// locked creates.
//
// A transaction in the list that turns out to exist already, which the caller's pre-check makes
// a race with another writer, is reported MultiTxExisted. Above the checkpoint the chunk finds it
// under the transaction's lock before it decides what to net, so a child of it spends its outputs
// for real. In the two-step write a parent that exists and has a child in the list stops step 2
// at that level and the rest of the list goes to the per-transaction default, which spends them.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	list, err := utxo.PrepareSpendAndCreateMulti(txs, opts...)
	if err != nil {
		return nil, err
	}

	if len(txs) == 0 {
		return []utxo.SpendAndCreateMultiResult{}, nil
	}

	if below, err := s.nettedBelowGate(list.Options); below {
		if err != nil {
			return nil, err
		}

		return s.netBelow(ctx, txs, list, blockHeight)
	}

	if !s.canNet(list.Options) {
		return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiWidthsFor(s.settings), txs, blockHeight, opts...)
	}

	w := s.newNettedWrite(txs, list, blockHeight, opts)

	if s.routesToTwoStep(list.Options) {
		return w.runTwoStep(ctx)
	}

	return w.writeFamilies(ctx)
}

// forceTwoStep makes routesToTwoStep send every list to the two-step write. A test and benchmark
// hook, so the two writers can be compared on one store and one list. false in production.
var forceTwoStep bool

// routesToTwoStep reports whether a list is written by the two-step write instead of family
// chunks. It is the one seam between the two writers.
//
// A list is a below-checkpoint list under the same condition PR 1919's nettedBelowGate uses, so
// the two writers agree where they meet: it carries a block at or below the highest checkpoint,
// and its spends are outpoint-only (IgnoreFlags.SkipUTXOHashCheck and SkipExtendedInputs), which
// is quick validation's fast path. PR 1909's list option does not exist on this branch; both
// fields the gate reads do.
func (s *Store) routesToTwoStep(o *utxo.CreateOptions) bool {
	if forceTwoStep {
		return true
	}

	mi, mined := minedBlock(o.MinedBlockInfos)

	return mined && model.BelowCheckpoint(s.checkpoints, mi.BlockHeight) &&
		o.IgnoreFlags.SkipUTXOHashCheck && o.SkipExtendedInputs
}

// canNet reports whether the netted write models every option of the list.
func (s *Store) canNet(o *utxo.CreateOptions) bool {
	return !o.SpendOnly && !o.CreateOnly && !o.Frozen && !o.Conflicting && !o.Locked
}

// multiSpendChunkInputs bounds the inputs one step-1 transaction spends. A variable so a test
// can force several chunks.
var multiSpendChunkInputs = 8192

// multiCreateChunkTxs bounds the transactions one step-2 transaction creates. The create takes
// one advisory lock per transaction (lockTxids), held to commit, and Postgres keeps those in a
// shared table sized max_locks_per_transaction x max_connections, 6,400 slots at the defaults
// mainnet runs. A chunk cut by bytes alone put 30,000 locks in one transaction and failed with
// "out of shared memory". At this bound and multiConcurrency chunks in flight the list holds at
// most about 2,000. A variable so a test can force one transaction per chunk.
var multiCreateChunkTxs = 256

// multiFault is a test hook called after step 1, as "spent", and after each commit unit of step
// 2, as "created group N" with N counting from 0: a group of merged levels, or one level split
// into parallel chunks. An error from it stops the write there. errMultiCrash stops it as a crash
// would, with no cleanup; any other error is an ordinary failure, and the write gives back the
// spends of the transactions it did not write. nil in production.
var multiFault func(stage string) error

// multiCrash is the error a multiFault hook returns to stop the write as a crash would: the
// process is gone, so nothing gives the step-1 spends back. It is its own type, matched by
// identity, for the reason nettedParentExists gives.
type multiCrash struct{}

func (multiCrash) Error() string {
	return "[utxoset][SpendAndCreateMulti] simulated crash"
}

var errMultiCrash error = multiCrash{}

// multiCommitTimeout bounds a chunk's commit. The commit runs on a context detached from the
// caller's, so that a cancel landing while it is in flight cannot leave it committed on the
// server and failed on the client: detached, it gives a true answer unless the connection
// itself fails.
const multiCommitTimeout = time.Minute

// multiCreateCommitted is a test hook called with the transaction count of each step-2 chunk
// transaction that commits. nil in production.
var multiCreateCommitted func(txs int)

// multiCreateBeforeCommit is a test hook called just before each step-2 chunk transaction
// commits, with the context the commit will run under and the chunk's txids. nil in production.
var multiCreateBeforeCommit func(ctx context.Context, txids []chainhash.Hash)

// multiCreateAfterCommit is a test hook called after each step-2 chunk transaction commits and
// before its results are set. An error from it is returned as the commit's, which is the shape
// of a commit that the server made and the client did not hear about. nil in production.
var multiCreateAfterCommit func() error

// multiConcurrency is how many chunk transactions run at once. Each holds one pool connection
// and takes no second one, so at most half the pool, leaving the rest of the node its share,
// and at most 8, which with multiCreateChunkTxs bounds the advisory locks held at once.
func (s *Store) multiConcurrency() int {
	return min(max(int(s.pool.Config().MaxConns)/2, 1), 8)
}

// multiTx is one transaction of the list as the netted write tracks it.
type multiTx struct {
	pos     int
	tx      *bt.Tx
	txid    chainhash.Hash
	parents []int
	// netted[vin] is true when input vin's parent is in the list. nil when none is.
	netted []bool
	// nettedOuts is the number of this transaction's outputs a child in the list spends.
	nettedOuts int
	claims     []inputClaim
	spends     []*utxo.Spend
	result     utxo.SpendAndCreateMultiResult
}

func (it *multiTx) dead() bool {
	return it.result.Status == utxo.MultiTxFailed || it.result.Status == utxo.MultiTxParentFailed
}

func (it *multiTx) restoreClaims() {
	for i, in := range it.tx.Inputs {
		if in != nil && i < len(it.claims) {
			in.PreviousTxSatoshis = it.claims[i].satoshis
			in.PreviousTxScript = it.claims[i].script
		}
	}
}

type nettedWrite struct {
	s           *Store
	txs         []*bt.Tx
	list        *utxo.SpendAndCreateMultiList
	blockHeight uint32
	opts        []utxo.CreateOption
	items       []*multiTx
	// position is each transaction's index in items, by txid.
	position map[chainhash.Hash]int

	// What the write did and where its time went, for the slow-list log line. The chunk
	// timings are summed across chunks that run in parallel, so they can exceed wall time.
	spendChunks  int
	levels       int
	createChunks atomic.Int64
	lockNs       atomic.Int64
	createNs     atomic.Int64
	journalNs    atomic.Int64
	commitNs     atomic.Int64
	bodyBytes    atomic.Int64
}

// multiSlowListLog is the step-1-plus-step-2 time above which a list gets an info log line
// saying where the time went.
const multiSlowListLog = time.Second

func (s *Store) newNettedWrite(txs []*bt.Tx, list *utxo.SpendAndCreateMultiList, blockHeight uint32,
	opts []utxo.CreateOption) *nettedWrite {
	position := make(map[chainhash.Hash]int, len(txs))
	for i := range txs {
		position[list.TxIDs[i]] = i
	}

	w := &nettedWrite{s: s, txs: txs, list: list, blockHeight: blockHeight, opts: opts, items: make([]*multiTx, len(txs)),
		position: position}

	for i, tx := range txs {
		it := &multiTx{pos: i, tx: tx, txid: list.TxIDs[i], parents: list.ParentsInList[i], claims: captureClaims(tx),
			spends: make([]*utxo.Spend, len(tx.Inputs))}

		if len(it.parents) > 0 {
			it.netted = make([]bool, len(tx.Inputs))

			for vin, in := range tx.Inputs {
				if _, ok := position[*in.PreviousTxIDChainHash()]; ok {
					it.netted[vin] = true
				}
			}
		}

		w.items[i] = it
	}

	return w
}

// runTwoStep is the write for a list at or below the checkpoint: step 1, then step 2, with no
// database transaction across them. See SpendAndCreateMulti.
func (w *nettedWrite) runTwoStep(ctx context.Context) (_ []utxo.SpendAndCreateMultiResult, err error) {
	start := time.Now()

	var spent time.Time

	defer func() {
		if spent.IsZero() {
			spent = time.Now()
		}

		w.logIfSlow(start, spent, time.Now())
	}()

	w.checkNettedInputs()
	w.failDescendants()

	if err := w.spendOutside(ctx); err != nil {
		return nil, err
	}

	spent = time.Now()

	// A failed transaction's descendants are failed too. Any outside spend of theirs that a
	// parallel step-1 chunk already committed stays: below the checkpoint a block is valid by
	// rule, so a failure here is not a block that loses to a fork, and a retry of the list always
	// comes and repeats those spends as the same spends.
	w.failDescendants()

	if multiFault != nil {
		if err := multiFault("spent"); err != nil {
			return nil, err
		}
	}

	if err := w.createLevels(ctx); err != nil {
		if err != errNettedParentExists { //nolint:errorlint // matched by identity; see nettedParentExists
			return nil, err
		}

		if err := w.finishPerTransaction(ctx); err != nil {
			return nil, err
		}
	}

	return w.results(), nil
}

func (w *nettedWrite) results() []utxo.SpendAndCreateMultiResult {
	out := make([]utxo.SpendAndCreateMultiResult, len(w.items))
	for i, it := range w.items {
		out[i] = it.result
	}

	return out
}

// checkNettedInputs does in memory what the spend statement does for an input whose parent is
// in the list: the output must be one the create would store as a UTXO, and the input's claimed
// value and script must match it. A parent created by this list carries no flags and no
// maturity, because canNet admits no flagged create and a coinbase is never in a list, so there
// is nothing else the statement would refuse. The input is then decorated, as the statement's
// RETURNING decorates a spent input.
func (w *nettedWrite) checkNettedInputs() {
	genesis := w.s.settings.ChainCfgParams.GenesisActivationHeight

	for _, it := range w.items {
		if it.netted == nil {
			continue
		}

		failures := 0

		var first error

		for vin, in := range it.tx.Inputs {
			if !it.netted[vin] {
				continue
			}

			parent := w.items[w.position[*in.PreviousTxIDChainHash()]]
			rec := &utxo.Spend{TxID: &parent.txid, Vout: in.PreviousTxOutIndex,
				SpendingData: &spendpkg.SpendingData{TxID: &it.txid, Vin: vin}}
			it.spends[vin] = rec

			out := parent.tx.Outputs[in.PreviousTxOutIndex]
			if out == nil || out.LockingScript == nil || !utxo.ShouldStoreOutputAsUTXO(out, w.blockHeight, genesis) {
				rec.Err = errors.NewTxNotFoundError("[utxoset][SpendAndCreateMulti] output %d of %s is not a spendable output", in.PreviousTxOutIndex, parent.txid.String())
			} else if err := claimMismatch(in, rec, int64(out.Satoshis), *out.LockingScript, nil, w.list.Options.IgnoreFlags.SkipUTXOHashCheck); err != nil { //nolint:gosec // satoshis fit int64
				rec.Err = err
			} else {
				decorateInput(in, int64(out.Satoshis), *out.LockingScript, nil) //nolint:gosec // satoshis fit int64
			}

			if rec.Err != nil {
				failures++

				if first == nil {
					first = rec.Err
				}
			}
		}

		if failures > 0 {
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxFailed, Spends: it.spends,
				Err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(it.spends), first)}
		}
	}
}

// failDescendants marks every transaction with a dead parent as ParentFailed, in list order, so
// a whole subtree goes with its root. It returns the ones it newly marked.
func (w *nettedWrite) failDescendants() []*multiTx {
	var marked []*multiTx

	for _, it := range w.items {
		if it.dead() {
			continue
		}

		for _, p := range it.parents {
			if w.items[p].dead() {
				it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxParentFailed, Spends: it.spends,
					Err: errors.NewProcessingError("SpendAndCreateMulti: a parent of %s earlier in the list failed", it.txid.String())}
				marked = append(marked, it)

				break
			}
		}
	}

	return marked
}

// spendOutside is step 1. Chunks touch disjoint UTXOs, because no outpoint is spent twice in a
// list, so they run in parallel without waiting on each other's row locks.
func (w *nettedWrite) spendOutside(ctx context.Context) error {
	var (
		chunks [][]*multiTx
		cur    []*multiTx
		inputs int
	)

	for _, it := range w.items {
		if it.dead() {
			continue
		}

		outside := len(it.tx.Inputs)

		for _, n := range it.netted {
			if n {
				outside--
			}
		}

		if outside == 0 {
			continue
		}

		if len(cur) > 0 && inputs+outside > multiSpendChunkInputs {
			chunks = append(chunks, cur)
			cur, inputs = nil, 0
		}

		cur = append(cur, it)
		inputs += outside
	}

	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	w.spendChunks = len(chunks)

	if len(chunks) == 0 {
		return nil
	}

	if err := w.s.ensureSpendJournalPartition(ctx, w.blockHeight); err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(w.s.multiConcurrency())

	for _, chunk := range chunks {
		g.Go(func() error { return w.spendChunk(gCtx, chunk) })
	}

	return g.Wait()
}

// spendChunk spends one chunk's outside inputs in one database transaction. A transaction with
// any failed input is taken out and the rest run again in a fresh database transaction, for the
// reason runSpendAndCreateBatch gives: only a whole rollback lets a competing spend that waited
// on our delete see the row again.
func (w *nettedWrite) spendChunk(ctx context.Context, chunk []*multiTx) error {
	flags := w.list.Options.IgnoreFlags
	active := chunk

	for round := 0; len(active) > 0; round++ {
		if round > 0 {
			for _, it := range active {
				it.restoreClaims()
			}
		}

		dbTx, err := w.s.pool.Begin(ctx)
		if err != nil {
			return errors.NewStorageError("[utxoset][SpendAndCreateMulti] begin spend chunk", err)
		}

		items := make([]*spendItem, len(active))
		for k, it := range active {
			items[k] = &spendItem{tx: it.tx, blockHeight: w.blockHeight, ignoreFlags: flags, skip: it.netted}
		}

		plan := planSpends(items)

		if err = w.s.runSpendPlan(ctx, dbTx, plan); err != nil {
			_ = dbTx.Rollback(ctx)
			return err
		}

		survivors := make([]*multiTx, 0, len(active))

		for k, it := range active {
			recs := plan.perItem[k]

			var (
				failures int
				first    error
			)

			for vin, sp := range recs {
				if it.netted != nil && it.netted[vin] {
					continue
				}

				it.spends[vin] = sp

				if sp != nil && sp.Err != nil {
					failures++

					if first == nil {
						first = sp.Err
					}
				}
			}

			if failures == 0 {
				survivors = append(survivors, it)
				continue
			}

			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxFailed, Spends: it.spends,
				Err: errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", failures, len(recs), first)}
		}

		if len(survivors) == len(active) {
			if err = dbTx.Commit(ctx); err != nil {
				_ = dbTx.Rollback(ctx)
				return errors.NewStorageError("[utxoset][SpendAndCreateMulti] commit spend chunk", err)
			}

			return nil
		}

		_ = dbTx.Rollback(ctx)
		active = survivors
	}

	return nil
}

// nettedParentExists stops step 2 when a transaction whose outputs the list nets turns out to
// exist already. It is its own type, matched by identity, because the errors package matches
// its own errors by code: a sentinel built from it would also match every unrelated processing
// error, and a real failure would be mistaken for this one.
type nettedParentExists struct{}

func (nettedParentExists) Error() string {
	return "[utxoset][SpendAndCreateMulti] a parent in the list already exists"
}

var errNettedParentExists error = nettedParentExists{}

// nettedKey names one output by its parent's txid and its packed key.
type nettedKey [48]byte

// nettedSpender is the transaction in the list that spends a netted output, and which of its
// inputs does it. The input index goes into the journal row, so GetSpend names it.
type nettedSpender struct {
	txid []byte
	vin  int32
}

func newNettedKey(txid []byte, ukey [16]byte) nettedKey {
	var k nettedKey

	copy(k[:32], txid)
	copy(k[32:], ukey[:])

	return k
}

// createLevels is step 2.
//
// Levels are written in order, and the cost it is shaped around is the commit. A deep chain is
// one transaction per level, and a commit per level is ~3 ms; mainnet block 955958 has 1,055
// transactions in 592 levels and spent 4.4 s here when every level committed alone, against
// ~0.6 s for a flat block of the same size. So consecutive levels are gathered into one pending
// group while it stays within multiCreateChunkTxs transactions and spendAndCreateBatchByteBudget
// bytes, the same bounds one chunk has, and the group is written as one chunk transaction. A
// level that would push the group past either bound flushes the group first and starts the
// next. A level that alone exceeds a bound is written as before: split into chunks that run
// multiConcurrency at a time, all of them committed before the next level starts.
//
// Merging keeps the crash argument intact. A merged group commits a parent's netted journal rows
// and the identity of the child that spends them in the same database transaction, and groups
// commit in level order, so every transaction that exists still has all of its spends. A parent
// in a group that turns out to exist already (errNettedParentExists) rolls the whole group back:
// none of its members gets a result, so finishPerTransaction, which takes every live transaction
// still MultiTxNotAttempted, hands the group and everything after it to the per-transaction
// default. Results are only set after a commit, so a rolled-back group leaves none behind.
func (w *nettedWrite) createLevels(ctx context.Context) error {
	spender := make(map[nettedKey]nettedSpender)
	level := make([]int, len(w.items))
	maxLevel := 0

	for i, it := range w.items {
		if it.dead() {
			continue
		}

		for vin, in := range it.tx.Inputs {
			if it.netted == nil || !it.netted[vin] {
				continue
			}

			parent := in.PreviousTxIDChainHash()
			spender[newNettedKey(parent[:], Pack(parent[:], in.PreviousTxOutIndex))] =
				nettedSpender{txid: it.txid[:], vin: int32(vin)} //nolint:gosec // an input index fits int32
		}

		for _, p := range it.parents {
			level[i] = max(level[i], level[p]+1)
			w.items[p].nettedOuts++
		}

		maxLevel = max(maxLevel, level[i])
	}

	w.levels = maxLevel + 1
	byLevel := make([][]*multiTx, maxLevel+1)
	sizes := make(map[*multiTx]int, len(w.items))

	for i, it := range w.items {
		if !it.dead() {
			byLevel[level[i]] = append(byLevel[level[i]], it)
			sizes[it] = it.tx.Size()
		}
	}

	if err := w.s.ensureTxBodyPartition(ctx, w.blockHeight); err != nil {
		return err
	}

	if mi, mined := minedBlock(w.list.Options.MinedBlockInfos); mined {
		if err := w.s.ensureTxMinedPartition(ctx, mi.BlockHeight); err != nil {
			return err
		}
	}

	if len(spender) > 0 {
		if err := w.s.ensureSpendJournalPartition(ctx, w.blockHeight); err != nil {
			return err
		}
	}

	var (
		pending      []*multiTx
		pendingBytes int
		group        int
	)

	// committed runs the fault hook after one commit unit and counts it.
	committed := func() error {
		stage := "created group " + strconv.Itoa(group)
		group++

		if multiFault != nil {
			return multiFault(stage)
		}

		return nil
	}

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}

		if err := w.createChunk(ctx, pending, spender); err != nil {
			return err
		}

		pending, pendingBytes = nil, 0

		return committed()
	}

	for _, members := range byLevel {
		if len(members) == 0 {
			continue
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		levelBytes := 0
		for _, it := range members {
			levelBytes += sizes[it]
		}

		if len(members) > multiCreateChunkTxs || levelBytes > spendAndCreateBatchByteBudget {
			if err := flush(); err != nil {
				return err
			}

			if err := w.createSplitLevel(ctx, members, sizes, spender); err != nil {
				return err
			}

			if err := committed(); err != nil {
				return err
			}

			continue
		}

		if len(pending)+len(members) > multiCreateChunkTxs || pendingBytes+levelBytes > spendAndCreateBatchByteBudget {
			if err := flush(); err != nil {
				return err
			}
		}

		pending = append(pending, members...)
		pendingBytes += levelBytes
	}

	return flush()
}

// createSplitLevel writes one level too wide for a single chunk: cut into chunks by the chunk
// bounds, multiConcurrency in flight, and every one committed before it returns.
//
// The chunks share the caller's context, not one derived from the group, so a chunk that fails
// does not cancel its siblings. They are independent, each safe to commit on its own by the crash
// argument above, and the commonest failure, errNettedParentExists, is not a fault at all. A
// cancelled sibling caught mid-commit could be committed on the server while its client saw only
// the cancellation; it then never got a result, and the per-transaction hand-off found the record
// this very call had written and reported it MultiTxExisted. Wait still returns the first error.
func (w *nettedWrite) createSplitLevel(ctx context.Context, members []*multiTx, sizes map[*multiTx]int,
	spender map[nettedKey]nettedSpender) error {
	var g errgroup.Group

	g.SetLimit(w.s.multiConcurrency())

	start, size := 0, 0

	for i, it := range members {
		txSize := sizes[it]

		if i > start && (size+txSize > spendAndCreateBatchByteBudget || i-start >= multiCreateChunkTxs) {
			chunk := members[start:i]
			g.Go(func() error { return w.createChunk(ctx, chunk, spender) })
			start, size = i, 0
		}

		size += txSize
	}

	if start < len(members) {
		chunk := members[start:]
		g.Go(func() error { return w.createChunk(ctx, chunk, spender) })
	}

	return g.Wait()
}

// nettedRows is a plan's spend-journal rows for the outputs the list spends.
type nettedRows struct {
	sats      []int64
	created   []int32
	spendable []int32
	flags     []int16
	mined     []int32
	blockIDs  []int32
	ukeys     [][16]byte
	txids     [][]byte
	spenders  [][]byte
	vins      []int32
	scripts   [][]byte
}

// takeNetted removes from the plan every UTXO a child in the list spends and returns the
// spend-journal row each becomes: the UTXO as the create would have written it, spent at the
// list's height by that child.
func (p *createPlan) takeNetted(spender map[nettedKey]nettedSpender) *nettedRows {
	j := &nettedRows{}
	keep := 0

	for c := range p.utxoTxids {
		by, ok := spender[newNettedKey(p.utxoTxids[c], p.utxoUkeys[c])]
		if !ok {
			p.utxoSats[keep] = p.utxoSats[c]
			p.utxoHeights[keep] = p.utxoHeights[c]
			p.utxoSpendable[keep] = p.utxoSpendable[c]
			p.utxoLeaves[keep] = p.utxoLeaves[c]
			p.utxoFlags[keep] = p.utxoFlags[c]
			p.utxoUkeys[keep] = p.utxoUkeys[c]
			p.utxoTxids[keep] = p.utxoTxids[c]
			p.utxoScripts[keep] = p.utxoScripts[c]
			p.utxoMined[keep] = p.utxoMined[c]
			p.utxoBlockIDs[keep] = p.utxoBlockIDs[c]
			keep++

			continue
		}

		j.sats = append(j.sats, p.utxoSats[c])
		j.created = append(j.created, p.utxoHeights[c])
		j.spendable = append(j.spendable, p.utxoSpendable[c])
		j.flags = append(j.flags, p.utxoFlags[c])
		j.mined = append(j.mined, p.utxoMined[c])
		j.blockIDs = append(j.blockIDs, p.utxoBlockIDs[c])
		j.ukeys = append(j.ukeys, p.utxoUkeys[c])
		j.txids = append(j.txids, p.utxoTxids[c])
		j.spenders = append(j.spenders, by.txid)
		j.vins = append(j.vins, by.vin)
		j.scripts = append(j.scripts, p.utxoScripts[c])
	}

	p.utxoSats = p.utxoSats[:keep]
	p.utxoHeights = p.utxoHeights[:keep]
	p.utxoSpendable = p.utxoSpendable[:keep]
	p.utxoLeaves = p.utxoLeaves[:keep]
	p.utxoFlags = p.utxoFlags[:keep]
	p.utxoUkeys = p.utxoUkeys[:keep]
	p.utxoTxids = p.utxoTxids[:keep]
	p.utxoScripts = p.utxoScripts[:keep]
	p.utxoMined = p.utxoMined[:keep]
	p.utxoBlockIDs = p.utxoBlockIDs[:keep]

	return j
}

// nettedJournalSQL writes the spend-journal rows of outputs created and spent inside one list.
// The columns are the ones spendJournalSQL copies off a deleted UTXO, with no hash_override: a
// UTXO this list creates has never been reassigned.
const nettedJournalSQL = `
INSERT INTO spend_journal (spent_height, spending_vin, satoshis, created_height, spendable_from, flags,
                           mined_height, block_id, ukey, txid, spending_txid, script)
SELECT $1::int, j.spending_vin, j.satoshis, j.created_height, j.spendable_from, j.flags,
       j.mined_height, j.block_id, j.ukey, j.txid, j.spending_txid, j.script
  FROM unnest($2::bigint[], $3::int[], $4::int[], $5::smallint[], $6::int[], $7::int[],
              $8::uuid[], $9::bytea[], $10::bytea[], $11::bytea[], $12::int[])
    AS j(satoshis, created_height, spendable_from, flags, mined_height, block_id,
         ukey, txid, spending_txid, script, spending_vin)`

// createChunk creates one chunk in one database transaction: the claims, the bodies and
// surviving UTXOs, and the spend-journal rows of the netted ones. The chunk is part of one level
// or several whole consecutive levels, parents before children. Results are set only after the
// commit.
func (w *nettedWrite) createChunk(ctx context.Context, chunk []*multiTx, spender map[nettedKey]nettedSpender) error {
	items := make([]*createItem, len(chunk))
	for k, it := range chunk {
		items[k] = &createItem{tx: it.tx, blockHeight: w.blockHeight, options: w.list.Options.ItemOptions(it.pos)}
	}

	plan := w.s.planCreates(items)

	for k := range chunk {
		if plan.errs[k] != nil {
			return plan.errs[k]
		}
	}

	journal := plan.takeNetted(spender)

	dbTx, err := w.s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] begin create chunk", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	t0 := time.Now()

	for _, b := range plan.bodies {
		w.bodyBytes.Add(int64(len(b)))
	}

	if err = w.s.lockTxids(ctx, dbTx, plan.txids); err != nil {
		return err
	}

	t1 := time.Now()
	w.lockNs.Add(int64(t1.Sub(t0)))

	if err = w.s.runCreatePlan(ctx, dbTx, plan); err != nil {
		return err
	}

	existed := make([]bool, len(chunk))

	for k, it := range chunk {
		switch err := plan.errs[k]; {
		case err == nil:
		case errors.Is(err, errors.ErrTxExists):
			if it.nettedOuts > 0 {
				return errNettedParentExists
			}

			existed[k] = true
		default:
			return err
		}
	}

	t2 := time.Now()
	w.createNs.Add(int64(t2.Sub(t1)))

	if err = w.writeNettedJournal(ctx, dbTx, journal); err != nil {
		return err
	}

	t3 := time.Now()
	w.journalNs.Add(int64(t3.Sub(t2)))

	if multiCreateBeforeCommit != nil {
		txids := make([]chainhash.Hash, len(chunk))
		for k, it := range chunk {
			txids[k] = it.txid
		}

		multiCreateBeforeCommit(ctx, txids)
	}

	// The commit runs on a context the caller cannot cancel; see multiCommitTimeout.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), multiCommitTimeout)
	err = dbTx.Commit(commitCtx)

	cancel()

	if err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] commit create chunk", err)
	}

	w.commitNs.Add(int64(time.Since(t3)))
	w.createChunks.Add(1)

	committed = true

	if multiCreateAfterCommit != nil {
		if err = multiCreateAfterCommit(); err != nil {
			return err
		}
	}

	if multiCreateCommitted != nil {
		multiCreateCommitted(len(chunk))
	}

	for k, it := range chunk {
		if existed[k] {
			it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxExisted, Spends: it.spends}
			continue
		}

		it.result = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxCreated, Meta: plan.perItem[k], Spends: it.spends}
	}

	return nil
}

func (w *nettedWrite) writeNettedJournal(ctx context.Context, dbTx pgx.Tx, j *nettedRows) error {
	if len(j.ukeys) == 0 {
		return nil
	}

	if _, err := dbTx.Exec(ctx, nettedJournalSQL, int32(w.blockHeight), //nolint:gosec // a height fits int32
		j.sats, j.created, j.spendable, j.flags, j.mined, j.blockIDs, j.ukeys, j.txids, j.spenders, j.scripts, j.vins); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] journal netted spends", err)
	}

	return nil
}

// finishPerTransaction hands every transaction step 2 has not settled to the per-transaction
// default, in list order. Their outside spends repeat there as the same spends, and a spend of a
// parent this write already created finds the journal row naming it.
func (w *nettedWrite) finishPerTransaction(ctx context.Context) error {
	var (
		rest  []*multiTx
		txs   []*bt.Tx
		txids []chainhash.Hash
		idxs  []int
	)

	listIdxs := w.list.Options.SubtreeIdxs

	for _, it := range w.items {
		if it.dead() || it.result.Status != utxo.MultiTxNotAttempted {
			continue
		}

		it.restoreClaims()

		rest = append(rest, it)
		txs = append(txs, it.tx)
		txids = append(txids, it.txid)

		if listIdxs != nil {
			idxs = append(idxs, listIdxs[it.pos])
		}
	}

	if len(rest) == 0 {
		return nil
	}

	// The shorter list carries its own txids and, from each transaction's place in the original
	// list, its own subtree indexes; both replace the original list's.
	opts := append(append([]utxo.CreateOption{}, w.opts...), utxo.WithTXIDs(txids))
	if listIdxs != nil {
		opts = append(opts, utxo.WithSubtreeIdxs(idxs))
	}

	results, err := utxo.DefaultSpendAndCreateMulti(ctx, w.s, utxo.SpendAndCreateMultiWidthsFor(w.s.settings), txs, w.blockHeight, opts...)
	for k, r := range results {
		rest[k].result = r
	}

	return err
}

// logIfSlow writes one line for a list that took longer than multiSlowListLog, saying how big
// and how deep it was and where the time went. Step 1 is the outside spends, step 2 the
// creates; the chunk timings inside step 2 are summed across parallel chunks.
func (w *nettedWrite) logIfSlow(start, spent, end time.Time) {
	if end.Sub(start) < multiSlowListLog {
		return
	}

	w.s.logger.Infof("[utxoset][SpendAndCreateMulti] slow list at height %d: %d txs, %d levels, %.1f MB of bodies; step 1 %s in %d chunks; step 2 %s in %d chunks (lock %s, create %s, journal %s, commit %s)",
		w.blockHeight, len(w.items), w.levels, float64(w.bodyBytes.Load())/1e6,
		spent.Sub(start).Round(time.Millisecond), w.spendChunks,
		end.Sub(spent).Round(time.Millisecond), w.createChunks.Load(),
		time.Duration(w.lockNs.Load()).Round(time.Millisecond), time.Duration(w.createNs.Load()).Round(time.Millisecond),
		time.Duration(w.journalNs.Load()).Round(time.Millisecond), time.Duration(w.commitNs.Load()).Round(time.Millisecond))
}
