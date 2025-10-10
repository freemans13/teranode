// Package work provides utilities for calculating blockchain proof-of-work values.
//
// This package implements the work calculation algorithms used in the Bitcoin blockchain
// to determine the cumulative proof-of-work for blocks and chains. The work calculation
// is fundamental to the consensus mechanism, as it determines which chain has the most
// accumulated computational effort and should be considered the valid chain.
//
// The work value represents the expected number of hash operations required to produce
// a block with the given difficulty target. Higher work values indicate more computational
// effort was expended to create the block.
package work

import (
	"encoding/binary"
	"math/big"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// CalculateWork calculates the cumulative work for a block given the previous work and difficulty.
// This function computes the total proof-of-work by adding the work required for the current
// block (based on its difficulty target) to the cumulative work of all previous blocks.
//
// The work calculation follows the Bitcoin protocol where work is inversely proportional
// to the difficulty target. A lower target (higher difficulty) requires more work to find
// a valid block hash. The formula used is:
//
//	work = 2^256 / (target + 1)
//	cumulative_work = previous_work + work
//
// This matches the bitcoin-sv GetBlockProof function which calculates:
//
//	return (~bnTarget / (bnTarget + 1)) + 1;
//
// Which simplifies to: 2^256 / (target + 1)
//
// The +1 in the denominator prevents division by zero and handles edge cases.
//
// This cumulative work value is used by the consensus algorithm to determine which chain
// has the most proof-of-work and should be considered the main chain.
//
// Parameters:
//   - prevWork: The cumulative work hash from all previous blocks in the chain
//   - nBits: The difficulty target for the current block in compact representation
//
// Returns:
//   - *chainhash.Hash: The new cumulative work value as a hash
//   - error: Any error encountered during the calculation (currently always nil)
func CalculateWork(prevWork *chainhash.Hash, nBits model.NBit) (*chainhash.Hash, error) {
	// Calculate work for this block using CalcBlockWork
	bits := binary.LittleEndian.Uint32(nBits.CloneBytes())
	work := CalcBlockWork(bits)

	// Add to previous work
	newWork := new(big.Int).Add(new(big.Int).SetBytes(bt.ReverseBytes(prevWork.CloneBytes())), work)

	b := bt.ReverseBytes(newWork.Bytes())
	hash := &chainhash.Hash{}
	copy(hash[:], b)

	return hash, nil
}

// CalcBlockWork calculates the work value for a single block from its difficulty bits.
// This is equivalent to bitcoin-sv's GetBlockProof function.
//
// The formula used is: work = 2^256 / (target + 1)
//
// This function is useful for calculating the work contribution of a single block
// without needing to know the previous cumulative work.
//
// Parameters:
//   - bits: The difficulty target in compact representation
//
// Returns:
//   - *big.Int: The work value for this block
func CalcBlockWork(bits uint32) *big.Int {
	// Convert compact bits to NBit
	bytesLittleEndian := make([]byte, 4)
	bytesLittleEndian[0] = byte(bits)
	bytesLittleEndian[1] = byte(bits >> 8)
	bytesLittleEndian[2] = byte(bits >> 16)
	bytesLittleEndian[3] = byte(bits >> 24)

	nBit, err := model.NewNBitFromSlice(bytesLittleEndian)
	if err != nil {
		return big.NewInt(0)
	}

	target := nBit.CalculateTarget()

	// Return zero if target is negative or zero (invalid block)
	if target.Sign() <= 0 {
		return big.NewInt(0)
	}

	// Calculate work: 2^256 / (target + 1)
	denominator := new(big.Int).Add(target, big.NewInt(1))
	work := new(big.Int).Div(new(big.Int).Lsh(big.NewInt(1), 256), denominator)

	return work
}
