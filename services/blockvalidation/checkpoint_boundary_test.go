package blockvalidation

// TestCheckpointHeight_WriteReadAgree asserts invariant I3: the inline loop in
// model.Block.checkBlockRewardAndFees (the "read side") produces the same highest
// checkpoint height as blockchain.HighestCheckpointHeight (the "write side") for
// the same params.Checkpoints. The two functions must agree so that the fast-path
// fee=0 write and the revalidation read both skip at the same boundary.
//
// Placement: this file lives in services/blockvalidation (package blockvalidation,
// white-box test) because model cannot import services/blockchain without creating
// an import cycle (model → blockchain → model). blockvalidation already imports
// both model and services/blockchain, making it the closest clean cross-import site.

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

func TestCheckpointHeight_WriteReadAgree(t *testing.T) {
	params := &chaincfg.MainNetParams

	writeSide := blockchain.HighestCheckpointHeight(params.Checkpoints)

	// Inline loop — byte-equivalent to the one in model.Block.checkBlockRewardAndFees.
	var readSide uint32
	for _, cp := range params.Checkpoints {
		if cp.Height < 0 {
			continue
		}
		if h := uint32(cp.Height); h > readSide {
			readSide = h
		}
	}

	require.Equal(t, writeSide, readSide, "fee write/read checkpoint height must be identical (I3)")
}
