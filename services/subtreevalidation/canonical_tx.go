package subtreevalidation

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// checkCanonicalTxEncoding rejects a transaction whose wire bytes are not the
// shortest encoding of themselves (issue 1421).
//
// Bitcoin and SV Node require every CompactSize length prefix to be minimal —
// writing the value 1 as `fd 01 00` instead of `01` is a hard parse error there.
// go-bt's varint reader has no such check: it accepts the over-long form and
// RE-SERIALIZES it canonically, so the txid and therefore the merkle root are
// unchanged. Every hash-based check downstream passes, precisely because
// canonicalization defeats them, so the parse is the only place that can still
// see it. Fail closed here rather than storing the canonicalized bytes and
// discarding the evidence that this node accepted bytes SV Node rejects at
// parse. Reject — do not "fix up" by accepting the canonical form, which would
// let a malformed transaction change identity mid-flight.
//
// This is deliberately a ProcessingError and NOT a TxInvalidError, and must stay
// that way. Because go-bt canonicalizes, a non-minimal prefix is a property of
// the DELIVERY rather than of the block: the same subtree from an honest peer
// parses to identical transactions and validates fine.
// BlockValidation.ValidateBlock tests errors.Is(err, ErrTxInvalid) on the
// subtree result, so a TxInvalidError would reach storeInvalidBlock and mark a
// perfectly valid block permanently invalid, surviving restart, after which
// isUnvalidatablePeerError stops us trying another source — handing any peer we
// fetch subtree data from a cheap way to poison a valid block. Wrapping does not
// contain it either: errors.Is walks the chain by CODE, so a TxInvalidError
// inside a ProcessingError still matches ErrTxInvalid. ProcessingError instead
// buys bounded retry across peers and then CatchupMaxAttemptsPerBlock, so the
// node stalls rather than recording a verdict it cannot justify and recovers as
// soon as one peer serves canonical bytes — the same liveness profile as the
// sibling "subtree data does not match subtree" check in getSubtreeMissingTxs.
//
// The check is a length comparison, not a re-parse: any non-minimal prefix makes
// the consumed wire bytes exceed the canonical serialization.
// util.CanonicalTxSize picks the standard or extended form without serializing,
// because this loop is deliberately allocation-free.
func checkCanonicalTxEncoding(tx *bt.Tx, bytesRead int64) error {
	if tx == nil {
		return errors.NewProcessingError("nil transaction")
	}

	canonicalSize := int64(util.CanonicalTxSize(tx))

	if bytesRead != canonicalSize {
		return errors.NewProcessingError("transaction %s is not canonically encoded: %d wire bytes for a %d-byte canonical serialization (non-minimal CompactSize prefix)", tx.TxIDChainHash().String(), bytesRead, canonicalSize)
	}

	return nil
}
