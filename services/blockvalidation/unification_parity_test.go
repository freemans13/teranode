package blockvalidation

// Task 7 — unification parity across the three below-checkpoint routing configs.
//
// This locks in that the two fast-path variants converge on the same observable
// commit outcome (quick_validated persisted on the block row) and that the
// default (both flags off) leaves quick_validated=false so block assembly
// reconciles as before:
//
//	unified ON  (LegacyUnifiedBelowCheckpoint):  routes through quickValidateBlock,
//	                                             commitBlock stamps quick_validated=true.
//	inline ON   (LegacyBelowCheckpointFailClosed): routes through the inline path,
//	                                             buildAddBlockOpts stamps quick_validated=true (T6).
//	both OFF:                                    full validation, quick_validated=false,
//	                                             WithCreateConflicting retained.
//
// LONG-TERM CONVERGENCE: the inline fail-closed variant exists only as the A/B
// lever for measuring the win on a live node without the fuller unified-route
// change. The unified route (LegacyUnifiedBelowCheckpoint), which reuses the same
// server-side machinery native catchup uses, is the intended long-term home; the
// inline variant should be retired once the unified route is proven in a soak.
// See project_legacy_ibd_staged_unification. A future edit MUST keep the two
// paths' fail-closed / no-conflicting-node guarantee identical — this test and
// the netsync fail_closed tests are the guard against one path drifting.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestUnification_Parity drives all three configs against a real sqlitememory
// blockchain store and asserts the persisted quick_validated flag matches the
// route's contract. Each block is chained onto the store's current tip and given
// a distinct ID so the three rows are independent.
func TestUnification_Parity(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	// A single hard-coded checkpoint well above the test heights so the shared
	// below-checkpoint gate (model.OutpointOnlyEligible) holds.
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 100000}}

	tests := []struct {
		name           string
		unified        bool
		failClosed     bool
		id             uint32
		wantQuickValid bool
	}{
		{name: "unified ON stamps quick_validated", unified: true, failClosed: false, id: 71, wantQuickValid: true},
		{name: "inline fail-closed ON stamps quick_validated", unified: false, failClosed: true, id: 72, wantQuickValid: true},
		{name: "both OFF leaves quick_validated false", unified: false, failClosed: false, id: 73, wantQuickValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = tt.unified
			tSettings.BlockValidation.LegacyBelowCheckpointFailClosed = tt.failClosed
			tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = tt.unified || tt.failClosed
			p := params
			tSettings.ChainCfgParams = &p

			bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

			// Chain each block onto the current tip so AddBlock's previous-block
			// lookup succeeds. Below-checkpoint height (well under 100000).
			bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			block := buildStampTestBlock(t, ctx, subtreeStore, bestHeader.Hash(), "", bestMeta.Height+1, tt.id)

			switch {
			case tt.unified:
				// Unified route: commitBlock unconditionally stamps quick_validated=true.
				require.NoError(t, bv.commitBlock(ctx, block, "legacy", "TestUnification_Parity"))
			default:
				// Inline / both-off: the stamp is derived by quickValidatedEligible and
				// applied by buildAddBlockOpts — the exact production stamp site. Every
				// subtree is pre-present here (blessedWithoutRevalidation=true), so the
				// only lever that differs between the two rows is the flag.
				eligible := bv.quickValidatedEligible(block, "legacy", true)
				require.Equal(t, tt.failClosed, eligible,
					"quickValidatedEligible must track the fail-closed flag when all other conjuncts hold")

				opts := bv.buildAddBlockOpts(block, eligible)
				require.NoError(t, blockchainClient.AddBlock(ctx, block, "legacy", opts...))
			}

			_, meta, err := blockchainClient.GetBlockHeader(ctx, block.Hash())
			require.NoError(t, err)
			require.Equal(t, tt.wantQuickValid, meta.QuickValidated,
				"config %q must persist quick_validated=%v", tt.name, tt.wantQuickValid)
		})
	}
}
