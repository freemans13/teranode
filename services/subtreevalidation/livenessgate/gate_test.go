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
	stamp    time.Time
	found    bool
	err      error
	getCalls int
}

func (s *stubClient) GetHeaderReceivedAt(_ context.Context, _ *chainhash.Hash) (time.Time, bool, error) {
	s.getCalls++
	return s.stamp, s.found, s.err
}

func TestDecide(t *testing.T) {
	h := chainhash.HashH([]byte("block"))
	now := time.Now()
	rpcErr := errors.NewError("rpc error")

	cases := []struct {
		name         string
		enabled      bool
		client       *stubClient
		window       time.Duration
		wantDecision Decision
		wantErr      error
		wantLabel    string
	}{
		{"disabled short-circuits before client call", false, &stubClient{stamp: now, found: true}, time.Minute, DecisionSubtreeData, nil, "subtreedata"},
		{"zero window short-circuits before client call", true, &stubClient{stamp: now, found: true}, 0, DecisionSubtreeData, nil, "subtreedata"},
		{"negative window short-circuits before client call", true, &stubClient{stamp: now, found: true}, -time.Second, DecisionSubtreeData, nil, "subtreedata"},
		{"fresh stamp yields subtree-only", true, &stubClient{stamp: now.Add(-10 * time.Second), found: true}, time.Minute, DecisionSubtreeOnly, nil, "subtreeonly"},
		{"stale stamp yields subtreedata", true, &stubClient{stamp: now.Add(-5 * time.Minute), found: true}, time.Minute, DecisionSubtreeData, nil, "subtreedata"},
		{"absent stamp yields notfound", true, &stubClient{found: false}, time.Minute, DecisionNotFound, nil, "notfound"},
		{"client error yields err with cause", true, &stubClient{err: rpcErr}, time.Minute, DecisionError, rpcErr, "err"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision, err := Decide(context.Background(), tc.client, &h, tc.enabled, tc.window)
			require.Equal(t, tc.wantDecision, decision)
			require.Equal(t, tc.wantErr, err)
			require.Equal(t, tc.wantLabel, decision.String())
		})
	}

	t.Run("disabled and non-positive window skip the client call entirely", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			enabled bool
			window  time.Duration
		}{
			{"disabled", false, time.Minute},
			{"zero window", true, 0},
			{"negative window", true, -time.Second},
		} {
			t.Run(tc.name, func(t *testing.T) {
				sc := &stubClient{stamp: now, found: true}
				decision, err := Decide(context.Background(), sc, &h, tc.enabled, tc.window)
				require.Equal(t, DecisionSubtreeData, decision)
				require.NoError(t, err)
				require.Zero(t, sc.getCalls, "client must not be called when the gate is inactive")
			})
		}
	})
}
