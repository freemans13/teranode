package longest_chain

import (
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/stretchr/testify/require"
)

var (
	blockWait = 5 * time.Second
)

func setupLongestChainTest(t *testing.T, utxoStoreOverride string) (td *daemon.TestDaemon, block101 *model.Block) {
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		// EnableFullLogging: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(tSettings *settings.Settings) {
			url, err := url.Parse(utxoStoreOverride)
			require.NoError(t, err)
			tSettings.UtxoStore.UtxoStore = url
		},
	})

	// Set the FSM state to RUNNING...
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	block101, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block101, blockWait, true)

	return td, block101
}
