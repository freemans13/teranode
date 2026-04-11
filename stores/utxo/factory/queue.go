package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/queue"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func init() {
	availableDatabases["postgresqueue"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (utxo.Store, error) {
		store, err := queue.New(ctx, logger, tSettings, storeURL)
		if err != nil {
			return nil, err
		}
		store.Start(ctx)
		return store, nil
	}
}
