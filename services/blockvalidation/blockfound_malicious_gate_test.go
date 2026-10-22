package blockvalidation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// maliciousGateP2PClient reports one peer as malicious and every other peer as not.
type maliciousGateP2PClient struct {
	*subtreeAttributionP2PClient
	malicious string
}

func (c *maliciousGateP2PClient) IsPeerMalicious(_ context.Context, peerID string) (bool, string, error) {
	if peerID == c.malicious {
		return true, "marked by the test", nil
	}

	return false, "", nil
}

// TestBlockFoundChMaliciousGateAnswersStoredBlock: a peer marked malicious announces a block on a
// blockFoundCh worker. If the block is already stored, the WaitToComplete caller gets nil, as for
// any other peer. If it is not stored, the caller gets the malicious error. Either way nothing is
// fetched from the malicious peer.
func TestBlockFoundChMaliciousGateAnswersStoredBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.peerMaliciousCache = nil
	s.p2pClient = &maliciousGateP2PClient{subtreeAttributionP2PClient: &subtreeAttributionP2PClient{}, malicious: "peerM"}

	require.True(t, s.isPeerMalicious(ctx, "peerM"), "setup: peerM must be malicious")

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	fetchKey := "GET http://malicious-peer/block/" + block.Hash().String()
	httpmock.RegisterResponder("GET", "http://malicious-peer/block/"+block.Hash().String(), httpmock.NewStringResponder(http.StatusInternalServerError, ""))

	stored := s.blockValidation
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, &gap054ExistsChain{ClientI: s.blockchainClient}, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	require.NoError(t, gap054Deliver(ctx, t, s, block.Hash(), "http://malicious-peer", "peerM"), "a stored block must answer nil, also for a malicious peer")
	require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "a stored block must not be fetched")

	s.blockValidation = stored

	err := gap054Deliver(ctx, t, s, block.Hash(), "http://malicious-peer", "peerM")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "got %v", err)
	require.Contains(t, err.Error(), "peer peerM is marked as malicious")
	require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "nothing may be fetched from a malicious announcer")
}
