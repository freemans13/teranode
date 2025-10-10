package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	storesql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	helper "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_stores_sql ./test/...

func Test_SetGetFSMState(t *testing.T) {
	connStr, teardown, err := helper.SetupTestPostgresContainer()

	if err != nil {
		t.Logf("Error setting up postgres container: %v", err)
	}

	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	err = s.SetFSMState(context.Background(), "CATCHING_BLOCKS")
	require.NoError(t, err)

	state, err := s.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "CATCHING_BLOCKS", state)

	err = s.SetFSMState(context.Background(), "RUNNING")
	require.NoError(t, err)

	state, err = s.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "RUNNING", state)
}
