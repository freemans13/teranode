package netsync

import (
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
)

// subtreeEmitFunc is called once per completed subtree, in block order. The
// builder drops its reference to all three artefacts as soon as this returns, so
// an implementation that needs them beyond the call must take its own copy.
type subtreeEmitFunc func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error

// blockStreamBuilder turns a stream of a block's transactions into subtrees,
// emitting each one as it fills and keeping only its root hash afterwards.
//
// It can decide the whole partition before the first transaction arrives because
// a block message carries its transaction count immediately after the 80-byte
// header. That is what makes streaming possible at all: the subtree sizes are
// known up front rather than discovered at the end.
//
// Resident state is one subtree under construction plus one 32-byte hash per
// completed subtree. On mainnet, where a subtree holds 4096 transactions, a
// 260,000-transaction block is 64 hashes and one subtree rather than the whole
// block.
type blockStreamBuilder struct {
	maxItems     int
	txCount      int
	subtreeCount int
	subtreeSize  int
	finalLeaves  int

	emit func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error
	acc  *merkleAccumulator

	current     *subtreepkg.Subtree
	currentData *subtreepkg.Data
	currentMeta *subtreepkg.Meta
	currentCap  int

	subtreeHashes []chainhash.Hash
	emitted       int
	seen          int
	failed        error

	// dedup rejects a transaction whose hash has already appeared in this block.
	//
	// The merkle root cannot catch that: the duplicate-last-when-odd rule means a
	// mutated transaction list produces the same root, the same header and the
	// same block hash as the honest one, so CheckMerkleRoot passes on it. This is
	// the only thing that catches it, and it is required below the checkpoint too,
	// because a checkpoint anchors the block HASH while the peer supplies the BODY
	// (model/check_duplicate_txs.go:23).
	//
	// Put IS the check: it fails when the hash repeats, so there is no second
	// scan and nothing is re-read. Nil means no dedup, which is the behaviour a
	// caller that supplies no map had before.
	dedup txmap.TxMap
}

// newBlockStreamBuilder prepares a builder for a block declaring txCount
// transactions, including its coinbase, partitioned into subtrees of at most
// maxItems leaves. It performs no duplicate-transaction detection; see
// newBlockStreamBuilderWithDedup.
func newBlockStreamBuilder(txCount, maxItems int, coinbase *bt.Tx, emit subtreeEmitFunc) (*blockStreamBuilder, error) {
	return newBlockStreamBuilderWithDedup(txCount, maxItems, coinbase, emit, nil)
}

// newBlockStreamBuilderWithDedup prepares a builder that rejects a block
// carrying the same transaction twice. dedup may be an in-memory map or the
// disk-backed one; both satisfy txmap.TxMap, and the pipeline does not care
// which is in use. A nil dedup disables the check, matching
// newBlockStreamBuilder's previous behaviour.
func newBlockStreamBuilderWithDedup(txCount, maxItems int, coinbase *bt.Tx, emit subtreeEmitFunc, dedup txmap.TxMap) (*blockStreamBuilder, error) {
	if coinbase == nil {
		return nil, errors.NewProcessingError("[blockStreamBuilder] no coinbase transaction")
	}

	if emit == nil {
		return nil, errors.NewProcessingError("[blockStreamBuilder] no emit function")
	}

	// A coinbase-only block (txCount <= 1) has no transactions to stream: the
	// caller's early return for this case (handle_block.go prepareSubtrees)
	// produces zero subtrees and zero files, but this builder would instead emit
	// one subtree whose root is the go-subtree CoinbasePlaceholder constant —
	// the same placeholder root for every coinbase-only block in the chain, so
	// every one of them would write three files under the same three keys,
	// overwriting each other, and hand back a subtree list production never
	// produces. Refuse it outright rather than diverge from that caller.
	if txCount <= 1 {
		return nil, errors.NewProcessingError("[blockStreamBuilder] refusing a coinbase-only block, got tx count %d", txCount)
	}

	size, count, finalLeaves, err := partitionLegacyBlock(txCount, maxItems)
	if err != nil {
		return nil, err
	}

	acc, err := newMerkleAccumulator(count, coinbase.TxIDChainHash(), uint64(coinbase.Size()))
	if err != nil {
		return nil, err
	}

	b := &blockStreamBuilder{
		maxItems:      maxItems,
		txCount:       txCount,
		subtreeCount:  count,
		subtreeSize:   size,
		finalLeaves:   finalLeaves,
		emit:          emit,
		acc:           acc,
		subtreeHashes: make([]chainhash.Hash, 0, count),
		dedup:         dedup,
	}

	if err = b.startSubtree(); err != nil {
		return nil, err
	}

	// The coinbase occupies slot zero as a placeholder. Its real transaction id
	// is substituted when the first subtree's root is taken, which is what
	// merkleAccumulator does on the first Add.
	if err = b.current.AddCoinbaseNode(); err != nil {
		return nil, errors.NewSubtreeError("[blockStreamBuilder] failed adding the coinbase placeholder", err)
	}

	b.seen = 1

	return b, nil
}

// startSubtree opens the next subtree, sized from the partition decided in the
// constructor. Only the final subtree may be smaller than the rest.
func (b *blockStreamBuilder) startSubtree() error {
	capacity := b.subtreeSize
	if b.emitted == b.subtreeCount-1 && b.subtreeCount > 1 && b.finalLeaves < b.subtreeSize {
		capacity = b.finalLeaves
	}

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
	if err != nil {
		return errors.NewSubtreeError("[blockStreamBuilder] failed creating subtree %d of %d", b.emitted, b.subtreeCount, err)
	}

	b.current = st
	b.currentData = subtreepkg.NewSubtreeData(st)
	b.currentMeta = subtreepkg.NewSubtreeMeta(st)
	b.currentCap = capacity

	return nil
}

// AddTx adds one transaction in block order, emitting the current subtree if it
// becomes full. txHash is passed in rather than recomputed because the caller has
// already hashed the transaction to know what it is.
func (b *blockStreamBuilder) AddTx(tx *bt.Tx, txHash *chainhash.Hash) error {
	if b.failed != nil {
		return b.failed
	}

	if tx == nil || txHash == nil {
		return b.fail(errors.NewProcessingError("[blockStreamBuilder] nil transaction at index %d", b.seen))
	}

	if b.seen >= b.txCount {
		return b.fail(errors.NewBlockInvalidError("[blockStreamBuilder] peer sent more transactions than the %d it declared", b.txCount))
	}

	if b.dedup != nil {
		if err := b.dedup.Put(*txHash, uint64(b.dedup.Length())); err != nil {
			// go-tx-map's own in-memory implementations signal a repeat with their
			// package sentinel wrapped in fmt.Errorf, not with teranode's
			// errors.ErrTxExists — only the disk-backed model.DiskTxMapUint64
			// translates to that sentinel. model/Block.go:1280 already carries
			// this same dual check for the same reason; match it here so the
			// test holds regardless of which concrete txmap.TxMap is supplied.
			if errors.Is(err, errors.ErrTxExists) || strings.Contains(err.Error(), "hash already exists in map") {
				return b.fail(errors.NewBlockInvalidError("[blockStreamBuilder] block contains duplicate transaction %s (CVE-2012-2459)", txHash))
			}

			return b.fail(errors.NewProcessingError("[blockStreamBuilder] failed recording transaction %s for duplicate detection", txHash, err))
		}
	}

	nodeIdx := b.current.Length()

	// Fee is stamped zero. Subtree fees are not consensus-checked below the
	// highest hard-coded checkpoint, and the inputs are not decorated on this
	// path, so a real fee is neither available nor needed.
	if err := b.current.AddNode(*txHash, 0, uint64(tx.Size())); err != nil {
		return b.fail(errors.NewSubtreeError("[blockStreamBuilder] failed adding transaction %s to subtree %d", txHash, b.emitted, err))
	}

	if err := b.currentData.AddTx(tx, nodeIdx); err != nil {
		return b.fail(errors.NewTxError("[blockStreamBuilder] failed adding transaction %s to subtree data", txHash, err))
	}

	if err := b.currentMeta.SetTxInpointsFromTx(tx); err != nil {
		return b.fail(errors.NewTxError("[blockStreamBuilder] failed adding transaction %s to subtree meta", txHash, err))
	}

	b.seen++

	// The final subtree is never auto-emitted here, even when a transaction
	// fills it exactly: Finish is the only caller allowed to emit it. Without
	// this guard, a block whose transaction count divides evenly into the
	// partition (e.g. 20 leaves as 8+8+4) fills the last subtree on the final
	// AddTx call and emits it mid-stream, which is indistinguishable from a
	// truncated block that happened to stop at the right length. Deferring to
	// Finish keeps one place responsible for deciding the stream is complete.
	if b.current.Length() >= b.currentCap && b.emitted < b.subtreeCount-1 {
		if err := b.emitCurrent(); err != nil {
			return err
		}
	}

	return nil
}

// emitCurrent hands the completed subtree to the caller, folds its root into the
// accumulator, and drops every reference to it.
func (b *blockStreamBuilder) emitCurrent() error {
	isLast := b.emitted == b.subtreeCount-1

	if err := b.emit(b.emitted, b.current, b.currentData, b.currentMeta); err != nil {
		return b.fail(err)
	}

	if err := b.acc.Add(b.current, isLast); err != nil {
		return b.fail(err)
	}

	b.subtreeHashes = append(b.subtreeHashes, *b.current.RootHash())
	b.emitted++

	b.current = nil
	b.currentData = nil
	b.currentMeta = nil

	if b.emitted < b.subtreeCount {
		return b.startSubtree()
	}

	return nil
}

// Finish emits any partial final subtree and returns the block's merkle root
// together with the subtree root hashes in block order.
func (b *blockStreamBuilder) Finish() (*chainhash.Hash, []chainhash.Hash, error) {
	if b.failed != nil {
		return nil, nil, b.failed
	}

	if b.seen != b.txCount {
		return nil, nil, b.fail(errors.NewBlockInvalidError("[blockStreamBuilder] stream ended after %d of the %d transactions it declared", b.seen, b.txCount))
	}

	if b.current != nil && b.current.Length() > 0 {
		if err := b.emitCurrent(); err != nil {
			return nil, nil, err
		}
	}

	root, err := b.acc.Root()
	if err != nil {
		return nil, nil, b.fail(err)
	}

	return root, b.subtreeHashes, nil
}

// SubtreeHashes returns the subtree root hashes emitted so far, in block order.
func (b *blockStreamBuilder) SubtreeHashes() []chainhash.Hash {
	return b.subtreeHashes
}

// fail latches the first error. Once a block has failed, there is no second
// source for the subtree files it was writing, so continuing would leave a block
// whose artefacts are incomplete rather than absent.
func (b *blockStreamBuilder) fail(err error) error {
	if b.failed == nil {
		b.failed = err
		b.current = nil
		b.currentData = nil
		b.currentMeta = nil
	}

	return b.failed
}
