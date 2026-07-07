package blockvalidation

// Gate symmetry: services/legacy/netsync.SyncManager.legacyOutpointOnly (the create
// side, which stamps fee=0 minimal-create UTXO rows) and
// BlockValidation.checkpointConfirmedAncestor (the validation side, which gates the
// checkBlockRewardAndFees no-inflation skip) must never disagree in a way that lets
// fee=0 rows exist without the matching validation skip also being available for
// them — otherwise every later revalidation (reconsiderblock, catchup) of that block
// would wrongly fail as BLOCK_INVALID.
//
// legacyOutpointOnly is unexported in a different package, so it cannot be called
// directly from here. netsyncLegacyOutpointOnlyGate re-derives its exact documented
// conjuncts for comparison; services/legacy/netsync/outpoint_only_test.go
// (TestSyncManager_legacyOutpointOnly) is the exhaustive truth table proving the real
// function matches this formula — keep both in sync if either changes.

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// netsyncLegacyOutpointOnlyGate mirrors netsync.SyncManager.legacyOutpointOnly's
// conjuncts: the setting must be on, the store must support the fast path, the block
// must be checkpoint-certified, and the height must be at/below the highest
// hard-coded checkpoint.
func netsyncLegacyOutpointOnlyGate(settingEnabled, storeSupportsOutpointOnly, checkpointCertified bool, height, highestCheckpoint uint32) bool {
	if !settingEnabled {
		return false
	}

	if !storeSupportsOutpointOnly {
		return false
	}

	if !checkpointCertified {
		return false
	}

	return height <= highestCheckpoint
}

// TestGateSymmetry_LegacyOutpointOnlyImpliesValidationSkip is the table-driven proof
// requested for the checkpoint-certified fix: for every combination of the four
// inputs the two gates share, whenever the create-side gate (mirrored above) would
// engage, the real validation-side skip predicate — storeSupportsOutpointOnly &&
// checkpointConfirmedAncestor && height<=highestCheckpoint, exactly as computed at
// the Block.Valid call site and inside checkBlockRewardAndFees — must also be true.
func TestGateSymmetry_LegacyOutpointOnlyImpliesValidationSkip(t *testing.T) {
	const highestCheckpoint = uint32(1000)

	checkpoints := []chaincfg.Checkpoint{{Height: int32(highestCheckpoint), Hash: &chainhash.Hash{0xAA}}}

	heights := []uint32{0, 1, 500, highestCheckpoint, highestCheckpoint + 1, highestCheckpoint + 500}
	bools := []bool{false, true}

	for _, settingEnabled := range bools {
		for _, storeSupports := range bools {
			for _, certified := range bools {
				for _, height := range heights {
					name := fmt.Sprintf("enabled=%v/store=%v/certified=%v/height=%d", settingEnabled, storeSupports, certified, height)

					t.Run(name, func(t *testing.T) {
						createGate := netsyncLegacyOutpointOnlyGate(settingEnabled, storeSupports, certified, height, highestCheckpoint)
						if !createGate {
							// Implication is vacuously true when the create-side gate is off.
							return
						}

						// Build the validation side with the identical store-support and
						// certified inputs used above.
						store := &utxo.MockUtxostore{SupportsOutpointOnlySpendResult: storeSupports}
						u := &BlockValidation{
							settings:         &settings.Settings{ChainCfgParams: &chaincfg.Params{Checkpoints: checkpoints}},
							blockchainClient: &blockchain.Mock{},
							utxoStore:        store,
							logger:           ulogger.TestLogger{},
						}
						block := &model.Block{Header: &model.BlockHeader{}, Height: height}

						ancestorConfirmed := u.checkpointConfirmedAncestor(context.Background(), block, certified)
						validationSkip := storeSupports && ancestorConfirmed && height <= blockchain.HighestCheckpointHeight(checkpoints)

						require.True(t, validationSkip,
							"legacyOutpointOnly()==true (enabled=%v store=%v certified=%v height=%d) must imply the validation-side no-inflation skip also engages",
							settingEnabled, storeSupports, certified, height)
					})
				}
			}
		}
	}
}
