package model

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// mockSubtreeStoreWriter implements SubtreeStoreWriter for testing
type mockSubtreeStoreWriter struct {
	storedMeta  map[string][]byte
	subtreeData map[string][]byte
	getErr      error
	setErr      error
}

func newMockSubtreeStoreWriter() *mockSubtreeStoreWriter {
	return &mockSubtreeStoreWriter{
		storedMeta:  make(map[string][]byte),
		subtreeData: make(map[string][]byte),
	}
}

func (m *mockSubtreeStoreWriter) GetIoReader(_ context.Context, key []byte, fileType fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}

	keyStr := string(key) + "." + string(fileType)
	if data, ok := m.subtreeData[keyStr]; ok {
		return io.NopCloser(newBytesReader(data)), nil
	}
	return nil, errors.NewNotFoundError("not found")
}

func (m *mockSubtreeStoreWriter) Set(_ context.Context, key []byte, fileType fileformat.FileType, value []byte, _ ...options.FileOption) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.storedMeta[string(key)+"."+string(fileType)] = value
	return nil
}

type bytesReader struct {
	data   []byte
	offset int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

// createTestSubtree creates a simple subtree for testing
func createTestSubtree(txHashes []chainhash.Hash) *subtreepkg.Subtree {
	nodes := make([]subtreepkg.Node, len(txHashes)+1)
	// First node is coinbase placeholder
	nodes[0] = subtreepkg.Node{Hash: subtreepkg.CoinbasePlaceholderHashValue}
	for i, h := range txHashes {
		nodes[i+1] = subtreepkg.Node{Hash: h}
	}
	return &subtreepkg.Subtree{Nodes: nodes}
}

// createTestTransaction creates a simple transaction for testing
func createTestTransaction(t *testing.T, prevTxIDHex string, prevVout uint32) *bt.Tx {
	t.Helper()

	prevTxID, err := chainhash.NewHashFromStr(prevTxIDHex)
	require.NoError(t, err)

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{{
		UnlockingScript:    &bscript.Script{},
		PreviousTxOutIndex: prevVout,
	}}
	err = tx.Inputs[0].PreviousTxIDAdd(prevTxID)
	require.NoError(t, err)

	err = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
	require.NoError(t, err)

	return tx
}

func TestSubtreeMetaRegenerator_RegenerateMeta_FromLocal(t *testing.T) {
	// Create test transactions
	prevTxID1 := "0000000000000000000000000000000000000000000000000000000000000001"
	prevTxID2 := "0000000000000000000000000000000000000000000000000000000000000002"

	tx1 := createTestTransaction(t, prevTxID1, 0)
	tx2 := createTestTransaction(t, prevTxID2, 1)

	txHash1 := *tx1.TxIDChainHash()
	txHash2 := *tx2.TxIDChainHash()

	// Create subtree with the transaction hashes
	subtree := createTestSubtree([]chainhash.Hash{txHash1, txHash2})
	subtreeHash := subtree.RootHash()

	// Create subtree data containing the transactions
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	// Serialize subtree data for the mock store
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Setup mock store with subtree data
	mockStore := newMockSubtreeStoreWriter()
	mockStore.subtreeData[string(subtreeHash[:])+"."+string(fileformat.FileTypeSubtreeData)] = subtreeDataBytes

	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, nil, "", func() uint32 { return 100 }, 288)

	// Test regeneration
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)

	// Verify meta contains correct inpoints
	inpoints1, err := meta.GetTxInpoints(1)
	require.NoError(t, err)
	require.NotNil(t, inpoints1)

	inpoints2, err := meta.GetTxInpoints(2)
	require.NoError(t, err)
	require.NotNil(t, inpoints2)

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

func TestSubtreeMetaRegenerator_RegenerateMeta_FromPeer(t *testing.T) {
	// Create test transactions
	prevTxID1 := "0000000000000000000000000000000000000000000000000000000000000001"
	prevTxID2 := "0000000000000000000000000000000000000000000000000000000000000002"

	tx1 := createTestTransaction(t, prevTxID1, 0)
	tx2 := createTestTransaction(t, prevTxID2, 1)

	txHash1 := *tx1.TxIDChainHash()
	txHash2 := *tx2.TxIDChainHash()

	// Create subtree with the transaction hashes
	subtree := createTestSubtree([]chainhash.Hash{txHash1, txHash2})
	subtreeHash := subtree.RootHash()

	// Create subtree data containing the transactions
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	// Serialize subtree data for HTTP response
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/api/v1/subtree_data/" + subtreeHash.String()
		if r.URL.Path == expectedPath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(subtreeDataBytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Setup mock store without local subtree data (so it falls back to peer)
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL}, "/api/v1", func() uint32 { return 100 }, 288)

	// Test regeneration
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)

	// Verify meta contains correct inpoints
	inpoints1, err := meta.GetTxInpoints(1)
	require.NoError(t, err)
	require.NotNil(t, inpoints1)

	inpoints2, err := meta.GetTxInpoints(2)
	require.NoError(t, err)
	require.NotNil(t, inpoints2)

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

func TestSubtreeMetaRegenerator_RegenerateMeta_AllSourcesFail(t *testing.T) {
	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	txHash1 := *tx1.TxIDChainHash()

	subtree := createTestSubtree([]chainhash.Hash{txHash1})
	subtreeHash := subtree.RootHash()

	// Create mock HTTP server that always returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Setup mock store without subtree data
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := NewSubtreeMetaRegenerator(logger, mockStore, []string{server.URL}, "/api/v1", func() uint32 { return 100 }, 288)

	// Test regeneration should fail
	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.Error(t, err)
	require.Nil(t, meta)
	require.Contains(t, err.Error(), "subtreedata not available locally or from peers")
}

func TestSubtreeMetaRegenerator_RegenerateMeta_NilStore_PeerFallback(t *testing.T) {
	// Create test transaction
	prevTxID1 := "0000000000000000000000000000000000000000000000000000000000000001"
	tx1 := createTestTransaction(t, prevTxID1, 0)
	txHash1 := *tx1.TxIDChainHash()

	// Create subtree
	subtree := createTestSubtree([]chainhash.Hash{txHash1})
	subtreeHash := subtree.RootHash()

	// Create subtree data
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(subtreeDataBytes)
	}))
	defer server.Close()

	logger := ulogger.TestLogger{}

	// Create regenerator with nil store - should still work via peer
	regenerator := NewSubtreeMetaRegenerator(logger, nil, []string{server.URL}, "/api/v1", func() uint32 { return 100 }, 288)

	meta, err := regenerator.RegenerateMeta(context.Background(), subtreeHash, subtree)

	require.NoError(t, err)
	require.NotNil(t, meta)
}

// TestSubtreeMetaRegenerator_RegenerateMeta_RejectsIncompleteSubtreeData pins the
// completeness of a regenerated meta. go-subtree's data deserializer stops at io.EOF
// without checking it filled every node, so a truncated .subtreeData file yields
// trailing nil transactions. Those become nodes with no recorded parent hashes, and
// feeding such a meta to the within-block duplicate-inputs check sends it down the
// nil-parents path, which rejects the whole block as invalid. Regeneration has to fail
// loudly rather than hand back a partial meta while logging success.
func TestSubtreeMetaRegenerator_RegenerateMeta_RejectsIncompleteSubtreeData(t *testing.T) {
	ctx := context.Background()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)
	tx2 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000002", 1)

	subtree := createTestSubtree([]chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()})
	subtreeHash := subtree.RootHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	subtreeData.Txs[1] = tx1
	subtreeData.Txs[2] = tx2

	full, err := subtreeData.Serialize()
	require.NoError(t, err)

	// Cut the file after the first transaction. Subtree data is bare concatenated
	// transactions with no header, so this is exactly the shape a short write leaves
	// on disk: tx1 parses, then EOF, and node 2 never gets its inpoints.
	truncated := full[:len(tx1.SerializeBytes())]
	require.Less(t, len(truncated), len(full), "fixture must actually be short")

	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, truncated))

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, "", func() uint32 { return 100 }, 288)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree)

	require.Error(t, err, "regeneration from truncated subtree data must fail, not return a partial meta")
	require.Nil(t, meta)

	// A meta that failed the completeness check must not reach disk either.
	_, err = store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.Error(t, err, "incomplete meta must not be persisted")
}

// TestSubtreeMetaRegenerator_RejectsMissingInpointsAtNodeZero covers the one node the
// completeness check would otherwise let through. Meta.Serialize exempts index 0
// unconditionally, because the FIRST subtree of a block carries the coinbase
// placeholder there — but every other subtree has a real transaction at index 0. An
// empty .subtreeData for such a subtree used to rebuild into a meta with no recorded
// parents that serialized cleanly and then overwrote the intact file on disk, leaving a
// poisoned cache that rejects a valid block on every restart.
func TestSubtreeMetaRegenerator_RejectsMissingInpointsAtNodeZero(t *testing.T) {
	ctx := context.Background()

	tx1 := createTestTransaction(t, "0000000000000000000000000000000000000000000000000000000000000001", 0)

	// No coinbase placeholder: a single real transaction at index 0, as every
	// subtree after the first one has.
	subtree := &subtreepkg.Subtree{Nodes: []subtreepkg.Node{{Hash: *tx1.TxIDChainHash()}}}
	subtreeHash := subtree.RootHash()

	// An empty subtree data file: the deserializer breaks on io.EOF without error,
	// so every node comes back nil.
	store := memory.New()
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, []byte{}))

	// An intact meta the poisoned rebuild must not be allowed to replace.
	intact := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, intact.SetTxInpointsFromTx(tx1))

	intactBytes, err := intact.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, intactBytes))

	regenerator := NewSubtreeMetaRegenerator(ulogger.TestLogger{}, store, nil, "", func() uint32 { return 100 }, 288)

	meta, err := regenerator.RegenerateMeta(ctx, subtreeHash, subtree)
	require.Error(t, err, "a rebuild with no inpoints for the real transaction at node 0 must fail")
	require.Nil(t, meta)

	stored, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, intactBytes, stored, "the failed rebuild must not overwrite the intact meta")
}

func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_Success(t *testing.T) {
	mockStore := newMockSubtreeStoreWriter()
	logger := ulogger.TestLogger{}

	regenerator := &SubtreeMetaRegenerator{
		logger:               logger,
		subtreeStore:         mockStore,
		getBlockHeight:       func() uint32 { return 100 },
		blockHeightRetention: 288,
	}

	// Create a simple subtree and meta
	hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	subtree := &subtreepkg.Subtree{
		Nodes: []subtreepkg.Node{
			{Hash: subtreepkg.CoinbasePlaceholderHashValue},
			{Hash: *hash1},
		},
	}
	subtreeHash := subtree.RootHash()

	meta := subtreepkg.NewSubtreeMeta(subtree)
	// Initialize TxInpoints for non-coinbase nodes to make serialization happy
	// The first node (coinbase placeholder) is at index 0, so we need to set index 1
	meta.TxInpoints[1] = subtreepkg.TxInpoints{
		ParentTxHashes: []chainhash.Hash{},
	}

	regenerator.storeRegeneratedMeta(context.Background(), subtreeHash, meta)

	// Verify meta was stored
	require.Len(t, mockStore.storedMeta, 1)
}

// TestSubtreeMetaRegenerator_StoreRegeneratedMeta_ReplacesCorruptFile pins the repair
// down to disk. Regeneration now also runs for a file that is present but corrupt, not
// only for a missing one, so the store write has to be allowed to overwrite. A blob
// store that refuses the overwrite leaves the corrupt bytes in place and the node
// rebuilds the same meta on every read, forever.
func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_ReplacesCorruptFile(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	regenerator := &SubtreeMetaRegenerator{
		logger:               ulogger.TestLogger{},
		subtreeStore:         store,
		getBlockHeight:       func() uint32 { return 100 },
		blockHeightRetention: 288,
	}

	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	subtree := &subtreepkg.Subtree{
		Nodes: []subtreepkg.Node{
			{Hash: subtreepkg.CoinbasePlaceholderHashValue},
			{Hash: *hash1},
		},
	}
	subtreeHash := subtree.RootHash()

	// A torn meta file is already on disk under the key regeneration will write to.
	corrupt := []byte("torn meta file")
	require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, corrupt))

	meta := subtreepkg.NewSubtreeMeta(subtree)
	meta.TxInpoints[1] = subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

	regenerator.storeRegeneratedMeta(ctx, subtreeHash, meta)

	expected, err := meta.Serialize()
	require.NoError(t, err)

	stored, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
	require.NoError(t, err)
	require.Equal(t, expected, stored, "regenerated meta must replace the corrupt file on disk")
}

func TestSubtreeMetaRegenerator_StoreRegeneratedMeta_NilStore(t *testing.T) {
	logger := ulogger.TestLogger{}

	regenerator := &SubtreeMetaRegenerator{
		logger:       logger,
		subtreeStore: nil, // No store
	}

	hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	subtree := &subtreepkg.Subtree{
		Nodes: []subtreepkg.Node{
			{Hash: subtreepkg.CoinbasePlaceholderHashValue},
			{Hash: *hash1},
		},
	}
	subtreeHash := subtree.RootHash()

	meta := subtreepkg.NewSubtreeMeta(subtree)
	meta.TxInpoints[1] = subtreepkg.TxInpoints{}

	// Should not panic with nil store
	regenerator.storeRegeneratedMeta(context.Background(), subtreeHash, meta)
}

func TestSubtreeStoreAdapter(t *testing.T) {
	// Create a mock SubtreeStore
	mockStore := NewLocalSubtreeStore()

	adapter := &SubtreeStoreAdapter{SubtreeStore: mockStore}

	// Test Set (should be no-op)
	err := adapter.Set(context.Background(), []byte("key"), fileformat.FileTypeSubtreeMeta, []byte("value"))
	require.NoError(t, err)

	// Verify nothing was stored (adapter's Set is a no-op)
	require.Empty(t, mockStore.FileData)
}
