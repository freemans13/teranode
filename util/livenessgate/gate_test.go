package livenessgate

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

type stubClient struct {
	stamp time.Time
	found bool
	err   error
}

func (s *stubClient) GetHeaderReceivedAt(_ context.Context, _ *chainhash.Hash) (time.Time, bool, error) {
	return s.stamp, s.found, s.err
}

func TestShouldUseSubtreeOnlyPath(t *testing.T) {
	h := chainhash.HashH([]byte("block"))
	now := time.Now()

	cases := []struct {
		name    string
		enabled bool
		client  *stubClient
		window  time.Duration
		want    bool
	}{
		{"disabled", false, &stubClient{stamp: now, found: true}, time.Minute, false},
		{"enabled+found+fresh", true, &stubClient{stamp: now.Add(-10 * time.Second), found: true}, time.Minute, true},
		{"enabled+found+stale", true, &stubClient{stamp: now.Add(-5 * time.Minute), found: true}, time.Minute, false},
		{"enabled+absent", true, &stubClient{found: false}, time.Minute, false},
		{"enabled+err", true, &stubClient{err: errors.NewError("rpc error")}, time.Minute, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldUseSubtreeOnlyPath(context.Background(), tc.client, &h, tc.enabled, tc.window)
			require.Equal(t, tc.want, got)
		})
	}
}
