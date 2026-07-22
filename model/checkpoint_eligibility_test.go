package model

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// eligibilityStore is a minimal utxo.Store stub: nil-embedded interface panics on
// any method except SupportsOutpointOnlySpend.
type eligibilityStore struct {
	utxo.Store
	supports bool
}

func (s *eligibilityStore) SupportsOutpointOnlySpend() bool { return s.supports }

// TestBelowCheckpoint: the single boundary definition. highest>0 (no checkpoints
// = never below), height>0 (genesis excluded), height <= highest inclusive.
func TestBelowCheckpoint(t *testing.T) {
	cps := []chaincfg.Checkpoint{{Height: 100}, {Height: 2000}, {Height: 500}}

	tests := []struct {
		name        string
		checkpoints []chaincfg.Checkpoint
		height      uint32
		want        bool
	}{
		{name: "no checkpoints", checkpoints: nil, height: 1, want: false},
		{name: "empty checkpoints", checkpoints: []chaincfg.Checkpoint{}, height: 1, want: false},
		{name: "genesis excluded", checkpoints: cps, height: 0, want: false},
		{name: "height 1", checkpoints: cps, height: 1, want: true},
		{name: "below highest", checkpoints: cps, height: 1999, want: true},
		{name: "at highest (inclusive)", checkpoints: cps, height: 2000, want: true},
		{name: "above highest", checkpoints: cps, height: 2001, want: false},
		{name: "negative-height checkpoint ignored", checkpoints: []chaincfg.Checkpoint{{Height: -5}}, height: 1, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, BelowCheckpoint(tt.checkpoints, tt.height))
		})
	}
}

// TestOutpointOnlyEligible: full truth table for the shared outpoint-only gate.
// Selection is purely store-capability driven now that the opt-in flag is retired:
// every conjunct (store supports outpoint-only, params present, below checkpoint)
// must hold; any one missing keeps the gate closed (fail-safe). A store that does
// not support outpoint-only spends (e.g. Aerospike) always falls back to the normal
// below-checkpoint validation path.
func TestOutpointOnlyEligible(t *testing.T) {
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 2000}}

	supported := &eligibilityStore{supports: true}
	unsupported := &eligibilityStore{supports: false}

	tests := []struct {
		name   string
		store  *eligibilityStore
		params *chaincfg.Params
		height uint32
		want   bool
	}{
		{name: "all conjuncts hold, below", store: supported, params: &params, height: 1000, want: true},
		{name: "at checkpoint", store: supported, params: &params, height: 2000, want: true},
		{name: "above checkpoint", store: supported, params: &params, height: 2001, want: false},
		{name: "height 0 (genesis)", store: supported, params: &params, height: 0, want: false},
		{name: "store does not support (Aerospike-like) falls back", store: unsupported, params: &params, height: 1000, want: false},
		{name: "nil params", store: supported, params: nil, height: 1000, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, OutpointOnlyEligible(tt.store, tt.params, tt.height))
		})
	}

	t.Run("nil store", func(t *testing.T) {
		require.False(t, OutpointOnlyEligible(nil, &params, 1000))
	})

	t.Run("no checkpoints on params", func(t *testing.T) {
		noCp := chaincfg.RegressionNetParams
		noCp.Checkpoints = nil
		require.False(t, OutpointOnlyEligible(supported, &noCp, 1000))
	})
}
