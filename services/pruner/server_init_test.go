package pruner

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	utxologger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Turning on UTXO store logging (logging=true on the store URL) wraps the store
// in stores/utxo/logger. Init must still find the wrapped store's pruner, so
// logging stays about logging and does not stop the pruner service starting.
func TestInit_LoggedSQLStoreProvidesPrunerService(t *testing.T) {
	utxosql.ResetPrunerServiceForTests()
	t.Cleanup(utxosql.ResetPrunerServiceForTests)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	utxoURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	baseStore, err := utxosql.New(ctx, logger, tSettings, utxoURL)
	require.NoError(t, err)

	loggedStore := utxologger.New(ctx, logger, baseStore)

	chainURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	chainStore, err := blockchainsql.New(logger, chainURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = chainStore.Close(context.Background()) })

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, chainStore, nil, nil)
	require.NoError(t, err)

	server := New(ctx, logger, tSettings, loggedStore, blockchainClient, nil)

	require.NoError(t, server.Init(ctx))
	require.NotNil(t, server.prunerService)
}
