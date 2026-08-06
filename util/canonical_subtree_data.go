package util

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
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
// pulled from the network including read-ahead.
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
// without serializing it.
func CanonicalTxSize(tx *bt.Tx) int {
	if tx.IsExtended() {
		return ExtendedTxSize(tx)
	}

	return tx.Size()
}

// CheckCanonicalSubtreeData rejects a subtree-data payload whose wire bytes are
// not the shortest encoding of themselves (issue 1421).
//
// Bitcoin and SV Node require every CompactSize length prefix to be minimal;
// go-bt does not check, and RE-SERIALIZES the over-long form canonically, so a
// non-minimal payload parses to transactions with canonical txids. Every
// hash-based check downstream therefore passes, and the caller stores the
// canonicalised bytes — silently normalising the payload and discarding the
// evidence, after which nothing can tell that this node accepted bytes every
// SV Node rejects at parse.
//
// The subtree-data parsers do not expose per-transaction byte counts, so the
// check is made over the whole payload: the bytes the parser consumed must
// equal the bytes the parsed data serializes back to. Any non-minimal prefix
// anywhere in the payload makes the wire form longer.
//
// omittedCoinbase must be supplied when the payload carried a coinbase that
// the serialization deliberately omits — the first subtree of a block, where
// go-subtree parses the coinbase into Txs[0] but writes the file from index 1.
// Without it a perfectly canonical first-subtree payload is rejected, because
// the consumed count legitimately exceeds the serialized length by exactly the
// coinbase. Its canonical size is added rather than the consumed bytes being
// ignored, so a non-minimal coinbase is still caught.
func CheckCanonicalSubtreeData(consumed int64, serialized []byte, omittedCoinbase *bt.Tx) error {
	canonicalSize := int64(len(serialized))
	if omittedCoinbase != nil {
		canonicalSize += int64(CanonicalTxSize(omittedCoinbase))
	}

	if consumed != canonicalSize {
		return errors.NewTxInvalidError("subtree data is not canonically encoded: %d wire bytes for a %d-byte canonical serialization (non-minimal CompactSize prefix)", consumed, canonicalSize)
	}

	return nil
}
