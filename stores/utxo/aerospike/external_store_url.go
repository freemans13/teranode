package aerospike

import (
	"net/url"

	"github.com/bsv-blockchain/teranode/errors"
)

// parseExternalStoreURL parses the externalStore query parameter of the
// utxostore URL. That value is a blob store URL in its own right, and an HTTP
// blob store URL can carry a working password in its userinfo. url.Parse
// embeds its whole input in the error it returns, so only the reason is kept.
func parseExternalStoreURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		var parseErr *url.Error
		if errors.As(err, &parseErr) {
			return nil, errors.NewConfigurationError("invalid externalStore URL in utxostore", parseErr.Err)
		}

		return nil, errors.NewConfigurationError("invalid externalStore URL in utxostore")
	}

	return u, nil
}
