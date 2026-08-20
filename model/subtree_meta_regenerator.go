package model

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// SubtreeMetaRegeneratorI defines the interface for regenerating missing subtree meta files
type SubtreeMetaRegeneratorI interface {
	// RegenerateMeta attempts to rebuild meta from subtreedata (local or from peers)
	RegenerateMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Meta, error)
}

// SubtreeStoreReader is a subset of blob.Store for reading subtree data
type SubtreeStoreReader interface {
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
}

// SubtreeStoreWriter extends SubtreeStoreReader with write capability for storing regenerated meta
type SubtreeStoreWriter interface {
	SubtreeStoreReader
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
}

// SubtreeMetaRegenerator handles regenerating missing subtree meta files
type SubtreeMetaRegenerator struct {
	logger               ulogger.Logger
	subtreeStore         SubtreeStoreWriter
	peerURLs             []string
	getBlockHeight       func() uint32
	blockHeightRetention uint32
	peerFetchTimeout     time.Duration
}

// cacheBustCounter produces the token appended to a peer request URL when a
// first attempt came back with an unusable body, so the retry cannot be served
// from that peer's cache.
//
// Process-wide and clock-seeded, deliberately, because it has to be unique
// across regenerators and not merely within one. blockvalidation builds a fresh
// SubtreeMetaRegenerator for every validation attempt (createMetaRegenerator is
// called per ValidateBlock and per ReValidateBlock), so a per-instance counter
// would restart at zero each time and every retry would request the identical
// "?cachebust=1" URL. nginx caches that URL under its own key like any other, so
// a busted request whose generation also aborted would leave the block wedged
// for the whole upstream TTL — exactly the failure this retry exists to break.
// blockvalidation's sibling counter gets process-lifetime uniqueness by living
// on the long-lived Server; this is the model-package equivalent, with a clock
// seed so a node restart mid-poison does not replay a token either.
var cacheBustCounter = newCacheBustCounter()

func newCacheBustCounter() *atomic.Uint64 {
	c := &atomic.Uint64{}
	c.Store(uint64(time.Now().UnixNano()))

	return c
}

// NewSubtreeMetaRegenerator creates a new SubtreeMetaRegenerator instance.
// peerURLs are the announcing peers' DataHub base URLs, which already include
// the peer's API prefix (e.g. http://peer:9090/api/v1) — the same base every
// other subtree_data fetcher appends only the resource path to.
//
// peerFetchTimeout bounds one peer's fetch; a non-positive value falls back to
// DefaultPeerFetchTimeout so the fetch is never left unbounded.
func NewSubtreeMetaRegenerator(logger ulogger.Logger, subtreeStore SubtreeStoreWriter, peerURLs []string,
	getBlockHeight func() uint32, blockHeightRetention uint32, peerFetchTimeout time.Duration) *SubtreeMetaRegenerator {
	if peerFetchTimeout <= 0 {
		peerFetchTimeout = DefaultPeerFetchTimeout
	}

	return &SubtreeMetaRegenerator{
		logger:               logger.New("meta_regenerator"),
		subtreeStore:         subtreeStore,
		peerURLs:             peerURLs,
		getBlockHeight:       getBlockHeight,
		blockHeightRetention: blockHeightRetention,
		peerFetchTimeout:     peerFetchTimeout,
	}
}

// RegenerateMeta attempts to rebuild meta from subtreedata (local store or peers)
// Returns the regenerated meta or an error if regeneration fails.
//
// Sources are tried in order — the local store, then each announcing peer — and
// a source counts as used only once it has yielded a body complete enough to
// build a meta from. A truncated local file, or a peer serving a poisoned cache
// entry, therefore falls through to the next source instead of ending the
// attempt. Committing to a source before validating its body meant one bad
// source wedged regeneration even with a healthy peer behind it, and because a
// truncated local file is re-read on every retry, the block never validated.
func (r *SubtreeMetaRegenerator) RegenerateMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Meta, error) {
	r.logger.Debugf("[RegenerateMeta][%s] attempting to regenerate subtree meta", subtreeHash.String())

	// Every source's own failure is kept so a total failure can name all of them
	// in one line. The incident that motivated this logged only a generic "not
	// available locally or from peers", which said nothing about why.
	attempts := make([]string, 0, 1+len(r.peerURLs))

	meta, err := r.tryLocal(ctx, subtreeHash, subtree)
	if err == nil {
		r.logger.Infof("[RegenerateMeta][%s] regenerated meta from the local subtree data", subtreeHash.String())
		return meta, nil
	}

	attempts = append(attempts, fmt.Sprintf("local: %v", err))
	r.logger.Debugf("[RegenerateMeta][%s] local subtreedata unusable: %v", subtreeHash.String(), err)

	// lastErr starts as the local failure so the returned error always carries a
	// cause: with no peers configured it explains why the local lookup missed,
	// rather than reporting a bare "not available".
	lastErr := err

	for _, peerURL := range r.peerURLs {
		meta, err = r.tryPeer(ctx, subtreeHash, subtree, peerURL)
		if err == nil {
			r.logger.Infof("[RegenerateMeta][%s] regenerated meta from peer %s", subtreeHash.String(), peerURL)
			return meta, nil
		}

		attempts = append(attempts, fmt.Sprintf("%s: %v", peerURL, err))
		lastErr = err

		r.logger.Debugf("[RegenerateMeta][%s] peer %s unusable: %v", subtreeHash.String(), peerURL, err)
	}

	// One WARN per failed regeneration carrying every source's cause, rather
	// than one per source: on the routine missing-meta path the per-source lines
	// are noise, and on failure the aggregate is what a reader actually needs.
	r.logger.Warnf("[RegenerateMeta][%s] subtreedata not available from any source - %s", subtreeHash.String(), strings.Join(attempts, "; "))

	return nil, errors.NewProcessingError("[RegenerateMeta][%s] subtreedata not available locally or from peers", subtreeHash.String(), lastErr)
}

// tryLocal reads the local subtree_data and builds the meta from it, failing if
// the stored body cannot fill every node so the caller moves on to a peer.
func (r *SubtreeMetaRegenerator) tryLocal(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Meta, error) {
	data, err := r.getLocalSubtreeData(ctx, subtreeHash, subtree)
	if err != nil {
		return nil, err
	}

	if missing := MissingSubtreeDataTxs(subtree, data); missing > 0 {
		return nil, errors.NewProcessingError("[RegenerateMeta][%s] local subtree_data is incomplete (%d of %d txs missing)", subtreeHash.String(), missing, subtree.Length())
	}

	return r.buildAndStoreMeta(ctx, subtreeHash, subtree, data)
}

// tryPeer fetches subtree_data from one peer and builds the meta from it.
//
// A peer answering 200 with a body too short for the subtree is retried once
// with a cache-busting URL before the peer is given up on. Peers front the
// asset service with an nginx proxy_cache that stores any 200 for its TTL, so
// an aborted on-demand generation that reached the client as "200 + empty body"
// is replayed to every byte-identical request (issue 1368). The cache key
// includes the query string while nginx location matching ignores it, so the
// busted URL reaches the same handler and misses the cache — the only lever
// available against a fleet we cannot update.
func (r *SubtreeMetaRegenerator) tryPeer(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, peerURL string) (*subtreepkg.Meta, error) {
	data, err := r.fetchCompleteFromPeer(ctx, subtreeHash, subtree, peerURL, false)
	if err != nil && errors.Is(err, errors.ErrExternal) {
		r.logger.Debugf("[RegenerateMeta][%s] peer %s served an unusable body, retrying past its cache: %v", subtreeHash.String(), peerURL, err)

		data, err = r.fetchCompleteFromPeer(ctx, subtreeHash, subtree, peerURL, true)
	}

	if err != nil {
		return nil, err
	}

	return r.buildAndStoreMeta(ctx, subtreeHash, subtree, data)
}

// fetchCompleteFromPeer fetches one peer's subtree_data and rejects a body that
// cannot fill every node.
//
// The incomplete-body error is classified ErrExternal — no local component
// failed — which is also how tryPeer recognises that one cache-busting retry is
// worth attempting. It mirrors blockvalidation's newPoisonedSubtreeDataError for
// the same condition.
func (r *SubtreeMetaRegenerator) fetchCompleteFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, peerURL string, bypassCache bool) (*subtreepkg.Data, error) {
	data, err := r.getSubtreeDataFromPeer(ctx, subtreeHash, subtree, peerURL, bypassCache)
	if err != nil {
		return nil, err
	}

	if missing := MissingSubtreeDataTxs(subtree, data); missing > 0 {
		return nil, errors.NewExternalError("[RegenerateMeta][%s] peer %s served incomplete subtree_data (%d of %d txs missing) - poisoned cache entry or aborted on-demand generation", subtreeHash.String(), peerURL, missing, subtree.Length())
	}

	return data, nil
}

// getLocalSubtreeData reads subtree data from local store
func (r *SubtreeMetaRegenerator) getLocalSubtreeData(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Data, error) {
	if r.subtreeStore == nil {
		return nil, errors.NewNotFoundError("subtree store not available")
	}

	reader, err := r.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()

	return subtreepkg.NewSubtreeDataFromReader(subtree, reader)
}

// DefaultPeerFetchTimeout is the fallback bound on one peer's fetch (all 503
// retries plus the body stream) when the caller supplies no timeout. This fetch
// runs inline in Block.Valid on a context with no deadline, where the shared
// client would otherwise allow a hung peer the full http_streaming_timeout per
// attempt — retries multiplied by that window.
//
// It bounds one fetch, covering all of that fetch's 503 retries and its body
// stream rather than any single attempt: under sustained 503 backoff the later
// attempts get progressively less of it. It is NOT a whole-peer bound, because
// tryPeer can make a second, cache-busting fetch, and each fetch starts this
// budget afresh. Worst case per peer is therefore twice this value, and worst
// case for one RegenerateMeta call is that again per configured peer — which
// matters because the call runs in a validOrderAndBlessed errgroup goroutine
// that holds a pooled parent-spends map for its whole duration.
//
// Sized to match settings.DefaultSubtreeDataFetchTimeout, which bounds the same
// subtree_data payload fetched from the same peer endpoint by
// check_block_subtrees.go. The budget has to cover streaming the body, so it is
// set by how long the payload takes rather than by how long a validation ought
// to take: a mainnet-size subtree_data cannot be streamed in seconds, and a
// budget too small to finish makes the fetch fail every time on exactly the
// blocks that need it most.
//
// Operators configure this via blockvalidation_subtree_meta_peer_fetch_timeout;
// settings.DefaultSubtreeMetaPeerFetchTimeout carries the same value, held to it
// by TestSubtreeMetaPeerFetchTimeout_ConstantsDoNotDrift. The two are separate
// constants only because model must not import settings.
const DefaultPeerFetchTimeout = 10 * time.Minute

// getSubtreeDataFromPeer fetches subtree data from a peer via HTTP. The peer's
// base URL already carries its API prefix, so only the resource path is
// appended. Retries on 503 — the peer's asset service may reject under
// admission control while it generates the file on-demand.
func (r *SubtreeMetaRegenerator) getSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, peerURL string, bypassCache bool) (*subtreepkg.Data, error) {
	ctx, cancel := context.WithTimeout(ctx, r.peerFetchTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/subtree_data/%s", peerURL, subtreeHash.String())
	if bypassCache {
		url = fmt.Sprintf("%s?cachebust=%d", url, cacheBustCounter.Add(1))
	}

	body, err := util.DoHTTPRequestBodyReaderWithRetry(ctx, url)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = body.Close()
	}()

	return subtreepkg.NewSubtreeDataFromReader(subtree, body)
}

// buildAndStoreMeta creates meta from subtree data and stores it for future use
func (r *SubtreeMetaRegenerator) buildAndStoreMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, data *subtreepkg.Data) (*subtreepkg.Meta, error) {
	meta, err := r.buildMetaFromSubtreeData(subtree, data)
	if err != nil {
		return nil, err
	}

	r.storeRegeneratedMeta(ctx, subtreeHash, meta)

	// The outcome is logged by RegenerateMeta, at Info and naming the source that
	// worked. Repeating it here — at Warn, and without the source — is the noise
	// the aggregated failure line was meant to remove, not add to.
	return meta, nil
}

// buildMetaFromSubtreeData creates meta from subtree data containing all transactions
func (r *SubtreeMetaRegenerator) buildMetaFromSubtreeData(subtree *subtreepkg.Subtree, data *subtreepkg.Data) (*subtreepkg.Meta, error) {
	meta := subtreepkg.NewSubtreeMeta(subtree)

	hasCoinbasePlaceholder := subtree.Length() > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)

	for i, tx := range data.Txs {
		if tx == nil {
			continue // Skip nil entries (e.g., coinbase placeholder)
		}

		// Skip coinbase placeholder at index 0
		if i == 0 && hasCoinbasePlaceholder {
			continue
		}

		if err := meta.SetTxInpointsFromTx(tx); err != nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] failed to set inpoints for tx %s: %v", tx.TxID(), err)
		}
	}

	// Final assertion on the same predicate the per-source checks use: a body that
	// cannot fill every node must never become a meta, because a meta with an
	// empty tail reads downstream as "transaction not found" and condemns a valid
	// block.
	if missing := MissingSubtreeDataTxs(subtree, data); missing > 0 {
		return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] incomplete subtree data: %d of %d txs missing", missing, subtree.Length())
	}

	return meta, nil
}

// storeRegeneratedMeta stores the regenerated meta for future use (non-blocking, warns on failure)
func (r *SubtreeMetaRegenerator) storeRegeneratedMeta(ctx context.Context, subtreeHash *chainhash.Hash, meta *subtreepkg.Meta) {
	if r.subtreeStore == nil {
		return
	}

	metaBytes, err := meta.Serialize()
	if err != nil {
		r.logger.Warnf("[storeRegeneratedMeta][%s] failed to serialize meta: %v", subtreeHash.String(), err)
		return
	}

	dah := r.getBlockHeight() + r.blockHeightRetention
	if err := r.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, metaBytes, options.WithDeleteAt(dah)); err != nil {
		r.logger.Warnf("[storeRegeneratedMeta][%s] failed to store meta: %v", subtreeHash.String(), err)
	}
}

// SubtreeStoreAdapter adapts a SubtreeStore (read-only) to SubtreeStoreWriter
// Use this when you don't need to store regenerated meta
type SubtreeStoreAdapter struct {
	SubtreeStore
}

// Set is a no-op for read-only stores
func (a *SubtreeStoreAdapter) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return nil
}

// GetIoReader delegates to the underlying SubtreeStore
func (a *SubtreeStoreAdapter) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return a.SubtreeStore.GetIoReader(ctx, key, fileType, opts...)
}
