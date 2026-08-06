package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// TestFetchAndStoreSubtreeData_NonCanonicalEncoding pins issue 1421 on the
// catchup prewarm. This path runs BEFORE block validation and stores the
// re-serialized form, so an unchecked non-minimal payload is laundered to disk
// and every downstream check then sees only canonical bytes — the per-transaction
// check can never fire for that subtree. Review demonstrated the bypass with a
// throwaway probe; this is that probe made permanent.
func TestFetchAndStoreSubtreeData_NonCanonicalEncoding(t *testing.T) {
	baseURL := "http://non-canonical-peer:8000"
	peerID := "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
	ctx := context.Background()

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	st, err := subtree.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, st.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, st.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := st.RootHash()
	subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
	testBlock := &model.Block{Height: 100}

	// Canonical payload: the three non-coinbase transactions concatenated, as
	// the peer's on-demand generator produces them (coinbase omitted).
	var canonicalBuf bytes.Buffer
	for i := 1; i <= 3; i++ {
		canonicalBuf.Write(txs[i].Bytes())
	}

	canonical := canonicalBuf.Bytes()

	newServer := func() *Server {
		return &Server{
			logger:       ulogger.TestLogger{},
			subtreeStore: memory.New(),
			settings:     test.CreateBaseTestSettings(t),
		}
	}

	t.Run("canonical payload is accepted and stored", func(t *testing.T) {
		server := newServer()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, canonical))

		require.NoError(t, server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false))
	})

	t.Run("non-minimal CompactSize is rejected, not laundered to disk", func(t *testing.T) {
		// Rewrite the first transaction's input-count prefix from its minimal
		// 1-byte form to the 3-byte form. go-bt accepts it and re-serializes
		// canonically, so every hash-based check downstream still passes —
		// which is exactly why the encoding has to be rejected here.
		firstTx := txs[1].Bytes()
		idx := bytes.Index(canonical, firstTx)
		require.GreaterOrEqual(t, idx, 0, "fixture must contain the first transaction verbatim")

		mangled := make([]byte, 0, len(canonical)+2)
		mangled = append(mangled, canonical[:idx+4]...)   // through version
		mangled = append(mangled, 0xfd, firstTx[4], 0x00) // non-minimal input count
		mangled = append(mangled, canonical[idx+5:]...)   // the rest, unchanged

		server := newServer()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, mangled))

		err := server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-canonical subtree data")
		require.True(t, errors.Is(err, errors.ErrTxInvalid))

		// The canonicalised form must NOT have been written: storing it would
		// discard the evidence and bless a subtree SV Node rejects at parse.
		exists, existsErr := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, existsErr)
		require.False(t, exists, "non-canonical payload must not be stored")
	})
}
