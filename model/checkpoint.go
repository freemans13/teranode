package model

import "github.com/bsv-blockchain/go-chaincfg"

// HighestCheckpointHeight returns the largest non-negative Height in the supplied
// checkpoint list, or 0 if the list is empty. This is the single source of truth
// for the below-checkpoint boundary: the block-validation write paths, the legacy
// netsync write path, the validator height guard, and checkBlockRewardAndFees' read
// path all derive the boundary from this one function, so the fee=0 write side and
// the fee-check skip read side can never disagree (invariant I3).
//
// services/blockchain.HighestCheckpointHeight delegates here; model sits below
// services/blockchain in the import graph, so the shared definition lives here to
// avoid the import cycle that previously forced a hand-copied loop in Block.go.
func HighestCheckpointHeight(checkpoints []chaincfg.Checkpoint) uint32 {
	var highest uint32
	for _, cp := range checkpoints {
		if cp.Height < 0 {
			continue
		}
		if h := uint32(cp.Height); h > highest {
			highest = h
		}
	}

	return highest
}
