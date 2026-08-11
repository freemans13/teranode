package model

import (
	"context"
	"fmt"
	"io"
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
}

// NewSubtreeMetaRegenerator creates a new SubtreeMetaRegenerator instance.
// peerURLs are the announcing peers' DataHub base URLs, which already include
// the peer's API prefix (e.g. http://peer:9090/api/v1) — the same base every
// other subtree_data fetcher appends only the resource path to.
func NewSubtreeMetaRegenerator(logger ulogger.Logger, subtreeStore SubtreeStoreWriter, peerURLs []string,
	getBlockHeight func() uint32, blockHeightRetention uint32) *SubtreeMetaRegenerator {
	return &SubtreeMetaRegenerator{
		logger:               logger.New("meta_regenerator"),
		subtreeStore:         subtreeStore,
		peerURLs:             peerURLs,
		getBlockHeight:       getBlockHeight,
		blockHeightRetention: blockHeightRetention,
	}
}

// RegenerateMeta attempts to rebuild meta from subtreedata (local store or peers)
// Returns the regenerated meta or an error if regeneration fails
func (r *SubtreeMetaRegenerator) RegenerateMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Meta, error) {
	r.logger.Warnf("[RegenerateMeta][%s] attempting to regenerate subtree meta", subtreeHash.String())

	// Try local subtreedata first
	data, err := r.getLocalSubtreeData(ctx, subtreeHash, subtree)
	if err == nil {
		return r.buildAndStoreMeta(ctx, subtreeHash, subtree, data)
	}
	r.logger.Debugf("[RegenerateMeta][%s] local subtreedata not found: %v", subtreeHash.String(), err)

	// Fall back to peers
	var lastErr error

	for _, peerURL := range r.peerURLs {
		data, err = r.getSubtreeDataFromPeer(ctx, subtreeHash, subtree, peerURL)
		if err == nil {
			return r.buildAndStoreMeta(ctx, subtreeHash, subtree, data)
		}

		lastErr = err
		r.logger.Warnf("[RegenerateMeta][%s] peer %s failed: %v", subtreeHash.String(), peerURL, err)
	}

	if lastErr != nil {
		return nil, errors.NewProcessingError("[RegenerateMeta][%s] subtreedata not available locally or from peers", subtreeHash.String(), lastErr)
	}

	return nil, errors.NewProcessingError("[RegenerateMeta][%s] subtreedata not available locally or from peers", subtreeHash.String())
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

// peerFetchTimeout bounds one peer's fetch (all 503 retries plus the body
// stream). This fetch runs inline in Block.Valid on a context with no
// deadline, where the shared client would otherwise allow a hung peer the full
// http_streaming_timeout (5 minutes by default) per attempt. Note this is a
// whole-peer budget, not a per-attempt one: the previous bare http.Client gave
// 30s to a single attempt with no retries, so under sustained 503 backoff the
// last attempt here gets considerably less than 30s.
const peerFetchTimeout = 30 * time.Second

// getSubtreeDataFromPeer fetches subtree data from a peer via HTTP. The peer's
// base URL already carries its API prefix, so only the resource path is
// appended. Retries on 503 — the peer's asset service may reject under
// admission control while it generates the file on-demand.
func (r *SubtreeMetaRegenerator) getSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, peerURL string) (*subtreepkg.Data, error) {
	ctx, cancel := context.WithTimeout(ctx, peerFetchTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/subtree_data/%s", peerURL, subtreeHash.String())

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
	r.logger.Warnf("[RegenerateMeta][%s] successfully regenerated meta", subtreeHash.String())

	return meta, nil
}

// buildMetaFromSubtreeData creates meta from subtree data containing all transactions
func (r *SubtreeMetaRegenerator) buildMetaFromSubtreeData(subtree *subtreepkg.Subtree, data *subtreepkg.Data) (*subtreepkg.Meta, error) {
	meta := subtreepkg.NewSubtreeMeta(subtree)

	for i, tx := range data.Txs {
		if tx == nil {
			continue // Skip nil entries (e.g., coinbase placeholder)
		}

		// Skip coinbase placeholder at index 0
		if i == 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		if err := meta.SetTxInpointsFromTx(tx); err != nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] failed to set inpoints for tx %s: %v", tx.TxID(), err)
		}
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
