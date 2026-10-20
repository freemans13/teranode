package model

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/util"
)

// lockTimeThreshold is bitcoin-sv's LOCKTIME_THRESHOLD: an nLockTime below it is a block height,
// at or above it a unix timestamp.
const lockTimeThreshold = 500_000_000

// CoinbaseFinalityViolation applies bitcoin-sv's IsFinalTx to a block's coinbase and returns
// bitcoin-sv's reject reason, bad-txns-nonfinal, when the coinbase is not final at height, or ""
// when it is. bitcoin-sv's ContextualCheckBlock applies IsFinalTx to every transaction in the block;
// regular transactions get it from the validator, but the coinbase never goes through the validator,
// so a block with a non-final coinbase was accepted (bitcoin-sv/teranode#4981).
//
// A time-based nLockTime is compared as ContextualCheckBlock does: against the block's own header
// timestamp before CSV activates, against the parent's median time past (BIP113) from CSV onwards.
// parentMedianTimePast is the median time past of the 11 blocks ending at the block's parent, or 0
// when the caller has no ancestor headers. In that case, from CSV onwards, a time-based nLockTime is
// not evaluated, the same way Valid skips the median-time-past header rule without ancestor headers;
// the sequence-number short-circuit and a height-based nLockTime are still enforced.
//
// Deliberately a reason rather than an error, like CoinbaseCommonRuleViolation: the verdict depends
// on whether the body is bound to its header, which only the caller knows. The caller must guarantee
// a non-nil coinbaseTx with at least one input.
func CoinbaseFinalityViolation(coinbaseTx *bt.Tx, header *BlockHeader, height uint32, params *chaincfg.Params, parentMedianTimePast uint32) string {
	lockTimeCutoff := header.Timestamp
	if height >= params.CSVHeight {
		if parentMedianTimePast == 0 && coinbaseTx.LockTime >= lockTimeThreshold {
			return ""
		}

		lockTimeCutoff = parentMedianTimePast
	}

	if util.IsTransactionFinal(coinbaseTx, height, lockTimeCutoff) != nil {
		return "bad-txns-nonfinal"
	}

	return ""
}
