package subtreevalidation

import (
	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// checkCanonicalTxEncoding rejects a transaction whose wire bytes are not the
// shortest encoding of themselves (issue 1421).
//
// Bitcoin and SV Node require every CompactSize length prefix to be written in
// its minimal form — writing the value 1 as `fd 01 00` instead of `01` is a
// hard parse error there. go-bt's varint reader has no such check: it accepts
// the over-long form and then RE-SERIALIZES it canonically, so the txid
// Teranode computes is the canonical one. That combination is what makes this
// a consensus hazard rather than a curiosity: an attacker ships non-minimal
// bytes while committing the canonical txids in the merkle tree, so
// Teranode's merkle check passes and the block is accepted, while every SV
// Node fleet member rejects the same block at parse time. Neither side logs
// an error; the fleets simply disagree about which chain is real, and because
// the block is already on disk a restart does not clear it.
//
// The check is a length comparison, not a re-parse: any non-minimal prefix
// makes the consumed wire bytes longer than the canonical serialization, so
// comparing the bytes consumed against the canonical size detects every such
// encoding at negligible cost. Do not "fix up" the transaction by accepting
// the canonical form — that would let a malformed transaction change identity
// mid-flight; reject it.
//
// Teranode's subtree data carries EXTENDED transactions (each input also
// carrying its parent's satoshis and locking script), which serialize longer
// than tx.Size() reports. The comparison is therefore made against whichever
// serialization the transaction actually uses, or a legitimate extended
// transaction would be rejected as non-canonical. The extended size is
// computed arithmetically (util.ExtendedTxSize): calling ExtendedBytes here
// would allocate and copy every transaction twice, on the very loop that was
// built to read transactions without allocating.
func checkCanonicalTxEncoding(tx *bt.Tx, bytesRead int64) error {
	if tx == nil {
		return errors.NewProcessingError("nil transaction")
	}

	canonicalSize := int64(tx.Size())
	if tx.IsExtended() {
		canonicalSize = int64(util.ExtendedTxSize(tx))
	}

	if bytesRead != canonicalSize {
		return errors.NewTxInvalidError("transaction %s is not canonically encoded: %d wire bytes for a %d-byte canonical serialization (non-minimal CompactSize prefix)", tx.TxIDChainHash().String(), bytesRead, canonicalSize)
	}

	return nil
}

// omittedCoinbaseTx returns the coinbase the subtree-data serialization leaves
// out, or nil. go-subtree parses a leading coinbase into Txs[0] for a subtree
// whose first node is the coinbase placeholder, but writes the file from index
// 1 — so the parser consumes bytes the serialization never re-emits, and the
// canonical-size comparison has to account for them (issue 1421).
func omittedCoinbaseTx(subtree *subtreepkg.Subtree, data *subtreepkg.Data) *bt.Tx {
	if subtree == nil || data == nil || len(subtree.Nodes) == 0 || len(data.Txs) == 0 {
		return nil
	}

	if !subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		return nil
	}

	return data.Txs[0]
}
