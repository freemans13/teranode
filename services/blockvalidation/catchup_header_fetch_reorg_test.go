package blockvalidation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// fetchAcrossPeerReorg runs catchupGetBlockHeaders against a peer that serves a full first batch
// of 10,000 headers from chain A and then, on the second request, answers from secondBatch: as a
// peer does when it reorged between the two requests. target is the block catchup is asked for.
func fetchAcrossPeerReorg(t *testing.T, chainA, secondBatch []*model.BlockHeader, target *model.BlockHeader) (*catchup.Result, int, error) {
	t.Helper()

	server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
	t.Cleanup(cleanup)

	mockUTXOStore.On("GetBlockHeight").Return(uint32(0))

	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(chainA[0], &model.BlockHeaderMeta{Height: 0}, nil)
	mockBlockchainClient.On("GetBlockLocator", mock.Anything, chainA[0].Hash(), uint32(0)).
		Return([]*chainhash.Hash{chainA[0].Hash()}, nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("block not found")).Maybe()

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	requests := 0

	httpmock.RegisterResponder("GET", `=~^http://test-peer/headers_from_common_ancestor/.*`,
		func(*http.Request) (*http.Response, error) {
			requests++

			batch := chainA[:maxBlockHeadersPerRequest]
			if requests > 1 {
				batch = secondBatch
			}

			var body []byte
			for _, h := range batch {
				body = append(body, h.Bytes()...)
			}

			return httpmock.NewBytesResponse(200, body), nil
		})

	result, _, err := server.catchupGetBlockHeaders(context.Background(), &model.Block{Header: target}, "peer-reorg", "http://test-peer")

	return result, requests, err
}

// requireOneChain asserts every header in headers builds on the one before it.
func requireOneChain(t *testing.T, headers []*model.BlockHeader) {
	t.Helper()

	for i := 1; i < len(headers); i++ {
		require.True(t, headers[i].HashPrevBlock.IsEqual(headers[i-1].Hash()),
			"chain broken at position %d: parent %s, previous header %s", i, headers[i].HashPrevBlock.String(), headers[i-1].Hash().String())
	}
}

// TestCatchupGetBlockHeaders_PeerReorgsBetweenRequests covers the seam an honest peer creates when
// its chain reorgs between two header requests: the second batch no longer builds on the last
// header of the first. Appending it as is left a broken link that findCommonAncestor now charges
// to the peer as NetworkInvalidResponse. The fetch must instead hand on one linked chain.
//
// Mutation proof: delete the splice in catchupGetBlockHeaders and both subtests fail requireOneChain
// at position 10,000, where the second batch was appended unlinked.
func TestCatchupGetBlockHeaders_PeerReorgsBetweenRequests(t *testing.T) {
	chainA := testhelpers.CreateTestHeaders(t, maxBlockHeadersPerRequest)

	t.Run("fork_point_held_splices_onto_new_branch", func(t *testing.T) {
		// The peer's new branch forks at a9990: a9991..a9999 are stale.
		forkPoint := maxBlockHeadersPerRequest - 10
		branch := forkHeadersFrom(t, chainA[forkPoint], 20, uint32(forkPoint+1), 1000) //nolint:gosec // small test height

		result, requests, err := fetchAcrossPeerReorg(t, chainA, branch, branch[len(branch)-1])
		require.NoError(t, err)
		require.Equal(t, 2, requests)

		require.Len(t, result.Headers, forkPoint+1+len(branch), "the stale a9991..a9999 must be dropped and the new branch appended")
		requireOneChain(t, result.Headers)
		require.Equal(t, chainA[forkPoint].Hash().String(), result.Headers[forkPoint].Hash().String())
		require.Equal(t, branch[0].Hash().String(), result.Headers[forkPoint+1].Hash().String())
		require.True(t, result.ReachedTarget, "the new branch carries the target")
	})

	t.Run("fork_point_not_held_stops_with_linked_prefix", func(t *testing.T) {
		// The second batch builds on a header this fetch never saw, so there is nothing to splice
		// onto. Keep the linked first batch and stop, without blaming the peer.
		nBits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		unseen := &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Bits: *nBits, Timestamp: chainA[0].Timestamp}
		testhelpers.MineHeader(unseen)

		branch := forkHeadersFrom(t, unseen, 20, 1, 1000)

		result, requests, err := fetchAcrossPeerReorg(t, chainA, branch, branch[len(branch)-1])
		require.NoError(t, err, "a peer whose chain moved under the fetch is not at fault")
		require.Equal(t, 2, requests)

		require.Len(t, result.Headers, maxBlockHeadersPerRequest, "only the linked first batch is kept")
		requireOneChain(t, result.Headers)
		require.False(t, result.ReachedTarget)
		require.Equal(t, "Peer chain changed during header fetch", result.StopReason)
	})
}

func TestLinkedPrefixLength(t *testing.T) {
	headers := testhelpers.CreateTestHeaders(t, 5)

	require.Equal(t, 5, linkedPrefixLength(headers, headers[4].Hash()), "parent is the newest header: keep all")
	require.Equal(t, 3, linkedPrefixLength(headers, headers[2].Hash()))
	require.Equal(t, 1, linkedPrefixLength(headers, headers[0].Hash()))
	require.Equal(t, 0, linkedPrefixLength(headers, &chainhash.Hash{0x01}), "parent not held")
	require.Equal(t, 0, linkedPrefixLength(nil, headers[0].Hash()))
}
