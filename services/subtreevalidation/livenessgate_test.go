package subtreevalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestShouldUseSubtreeOnlyPath_TruthTable(t *testing.T) {
	h := chainhash.HashH([]byte("block"))
	now := time.Now()

	cases := []struct {
		name       string
		enabled    bool
		window     time.Duration
		stamp      time.Time
		found      bool
		lookupErr  error
		wantResult bool
	}{
		{"setting off, fresh stamp", false, time.Minute, now.Add(-10 * time.Second), true, nil, false},
		{"setting off, no stamp", false, time.Minute, time.Time{}, false, nil, false},
		{"setting on, fresh stamp within window", true, time.Minute, now.Add(-10 * time.Second), true, nil, true},
		{"setting on, stale stamp outside window", true, time.Minute, now.Add(-5 * time.Minute), true, nil, false},
		{"setting on, stamp absent", true, time.Minute, time.Time{}, false, nil, false},
		{"setting on, lookup error", true, time.Minute, time.Time{}, false, errors.NewError("rpc"), false},
		// Use comfortably-wide margins (30s vs 90s for a 60s window) so slow or
		// loaded CI doesn't push the "within window" case past the boundary
		// before the assertion runs.
		{"setting on, stamp comfortably within window", true, time.Minute, now.Add(-30 * time.Second), true, nil, true},
		{"setting on, stamp comfortably outside window", true, time.Minute, now.Add(-90 * time.Second), true, nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &blockchain.Mock{}
			mockClient.On("GetHeaderReceivedAt", mock.Anything, &h).Return(tc.stamp, tc.found, tc.lookupErr)

			tSettings := settings.NewSettings()
			tSettings.SubtreeValidation.AssumeTxsBroadcastToAllNodes = tc.enabled
			tSettings.SubtreeValidation.LivenessWindow = tc.window

			srv := &Server{
				logger:           ulogger.TestLogger{},
				settings:         tSettings,
				blockchainClient: mockClient,
			}

			got := srv.ShouldUseSubtreeOnlyPath(context.Background(), &h)
			require.Equal(t, tc.wantResult, got)
		})
	}
}
