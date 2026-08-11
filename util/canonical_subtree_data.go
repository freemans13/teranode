package util

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// CountingReader counts the bytes a parser consumes, so a caller that cannot
// instrument the parse itself can still compare what came off the wire against
// what the parsed result serializes back to (issue 1421).
type CountingReader struct {
	r io.Reader
	n int64
}

// NewCountingReader wraps r. Wrap the reader the PARSER reads from (inside any
// buffering), so the count is bytes consumed by the parse rather than bytes
// pulled from the network including read-ahead. Not safe for concurrent use.
func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{r: r}
}

func (c *CountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

// BytesConsumed returns the number of bytes read so far.
func (c *CountingReader) BytesConsumed() int64 {
	return c.n
}

// CanonicalTxSize returns the size of tx in the serialization it actually uses,
// without serializing it. tx must be non-nil.
//
// Subtree data may carry EXTENDED transactions (each input also carrying its
// parent's satoshis and locking script), which serialize longer than tx.Size()
// reports, so the form has to be chosen per transaction or every legitimate
// extended transaction measures short and looks non-canonical.
//
// One go-bt quirk: bt.Tx.IsExtended reports false for an extended transaction
// with no inputs (it tests tx.Inputs == nil before the extended flag), so such a
// payload is measured in its standard form and rejected with a misattributed
// reason. Harmless — a transaction with no inputs is consensus-invalid anyway.
func CanonicalTxSize(tx *bt.Tx) int {
	if tx.IsExtended() {
		return ExtendedTxSize(tx)
	}

	return tx.Size()
}

// OmittedCoinbaseTx returns the coinbase the subtree-data serialization leaves
// out, or nil. go-subtree parses a leading coinbase into Txs[0] for a subtree
// whose first node is the coinbase placeholder, but writes the file from index
// 1 — so the parser consumes bytes the serialization never re-emits, and
// CheckCanonicalSubtreeData has to account for them (issue 1421).
func OmittedCoinbaseTx(subtree *subtreepkg.Subtree, data *subtreepkg.Data) *bt.Tx {
	if subtree == nil || data == nil || len(subtree.Nodes) == 0 || len(data.Txs) == 0 {
		return nil
	}

	if !subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		return nil
	}

	return data.Txs[0]
}

// CheckCanonicalSubtreeData rejects a subtree-data payload whose wire bytes are
// not the shortest encoding of themselves (issue 1421).
//
// The subtree-data parsers do not expose per-transaction byte counts, so the
// check is made over the whole payload: any non-minimal CompactSize prefix
// anywhere makes the parser consume more bytes than the parsed data serializes
// back to. Pass omittedCoinbase (see OmittedCoinbaseTx) when the payload carried
// a coinbase the serialization deliberately omits, or a perfectly canonical
// first-subtree payload is rejected; its canonical size is ADDED rather than the
// consumed bytes being ignored, so a non-minimal coinbase is still caught.
//
// The failure must stay a ProcessingError and never become a TxInvalidError:
// both callers reach a verdict machine that reads ErrTxInvalid as "this block is
// bad" (subtreevalidation via ValidateBlock -> storeInvalidBlock, blockvalidation's
// catchup prewarm via releaseCatchupLock -> reportMalicious), and non-minimality
// is a property of the DELIVERY, not the block. Wrapping does not contain it —
// errors.Is walks the chain by code. Full reasoning on
// subtreevalidation.checkCanonicalTxEncoding.
func CheckCanonicalSubtreeData(consumed int64, serialized []byte, omittedCoinbase *bt.Tx) error {
	canonicalSize := int64(len(serialized))
	if omittedCoinbase != nil {
		canonicalSize += int64(CanonicalTxSize(omittedCoinbase))
	}

	if consumed != canonicalSize {
		return errors.NewProcessingError("subtree data is not canonically encoded: %d wire bytes for a %d-byte canonical serialization (non-minimal CompactSize prefix)", consumed, canonicalSize)
	}

	return nil
}
