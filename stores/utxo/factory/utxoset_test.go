package factory

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestUtxosetSchemeIsRegistered proves the delete-on-spend store can actually be
// instantiated by the factory.
//
// Until it is in availableDatabases no settings URL can reach it, so the store cannot be
// deployed, cannot be measured end to end, and every claim about its behaviour is
// theoretical. Registration is the whole difference between a package and a store.
//
// The scheme is deliberately its own rather than a query parameter on "postgres": the two
// stores have incompatible schemas, and a typo in a parameter that silently selected the
// wrong one against a live database is not a failure mode worth having.
func TestUtxosetSchemeIsRegistered(t *testing.T) {
	dbInit, ok := availableDatabases["utxoset"]
	require.True(t, ok, "utxoset scheme must be registered in the factory")

	dsn := os.Getenv("UTXOSET_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres@localhost:5441/soak?sslmode=disable"
	}

	ctx := context.Background()

	// Decide whether to skip on postgres reachability ALONE, using a plain postgres URL.
	// Skipping on the utxoset:// attempt instead would swallow the very bug this test
	// exists to catch: pgx does not know the scheme, mangles the whole URL into a config
	// parameter and falls back to a unix socket, which surfaces as an unreachable server.
	probe, err := url.Parse(dsn)
	require.NoError(t, err)

	if pool, pErr := pgxpool.New(ctx, probe.String()); pErr != nil {
		t.Skipf("skipping: cannot reach postgres: %v", pErr)
	} else {
		if pErr = pool.Ping(ctx); pErr != nil {
			pool.Close()
			t.Skipf("skipping: cannot reach postgres: %v", pErr)
		}

		pool.Close()
	}

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	u.Scheme = "utxoset" // exactly what a settings URL would carry

	store, err := dbInit(ctx, ulogger.TestLogger{}, settings.NewSettings(), u)
	require.NoError(t, err, "the factory must connect from a utxoset:// URL")

	defer func() { _ = store.Close(ctx) }()

	require.IsType(t, &utxoset.Store{}, store)
	require.True(t, store.SupportsOutpointOnlySpend(),
		"the below-checkpoint fast path is the reason this store exists")
}
