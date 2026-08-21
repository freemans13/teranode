package model

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

const (
	// subtreeMetaEntryCountSize is the width of the little-endian uint32 entry
	// count that go-subtree's Meta.serializeTxInpoints writes straight after the
	// root hash.
	subtreeMetaEntryCountSize = 4

	// subtreeMetaHeaderSize is the fixed header go-subtree's Meta.Serialize
	// emits: the subtree root hash followed by the entry count.
	subtreeMetaHeaderSize = chainhash.HashSize + subtreeMetaEntryCountSize
)

// NewSubtreeMetaFromValidatedReader deserializes a .subtreeMeta stream after
// validating its fixed 36-byte header — the subtree root hash the file was
// built for and the entry count it claims — against the subtree it is being
// read for (issue 1425). The file is only a cache, but its consumers trust
// the contents: a short claimed count leaves tail transactions with zero
// recorded inputs (block validation then spuriously rejects a valid block on
// the nil-parents guard), an over-long count panics once it runs past the
// destination slice — on every restart, since the file is on disk — and a
// foreign file attributes another subtree's inputs to this one, the one shape
// that can genuinely hide an in-block double-spend.
//
// The count is compared against Length() because that is what
// Meta.serializeTxInpoints writes. Note this is not the same as the slice the
// deserializer fills: block validation installs a pooled node allocator, which
// rounds capacity up to a size class, so a short final subtree carries real
// headroom and an over-claim inside it fills padding quietly rather than
// panicking. Comparing against the serialized count catches both. Every
// producer writes that count keyed by the subtree root, so a mismatch always
// means a torn or foreign file, never a legitimate one.
func NewSubtreeMetaFromValidatedReader(subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree, reader io.Reader) (*subtreepkg.Meta, error) {
	if subtree == nil {
		return nil, errors.NewProcessingError("cannot validate subtree meta for %s: subtree is nil", subtreeHash.String())
	}

	var metaHeader [subtreeMetaHeaderSize]byte
	if _, err := io.ReadFull(reader, metaHeader[:]); err != nil {
		return nil, errors.NewProcessingError("failed to read subtree meta header for %s", subtreeHash.String(), err)
	}

	if !bytes.Equal(metaHeader[:chainhash.HashSize], subtreeHash[:]) {
		// Print the foreign hash in display order like every other hash in the
		// logs, or the one line meant for triage shows two incomparable hex
		// strings (the byte-order trap behind the phantom-fork misdiagnosis).
		metaRootHash := chainhash.Hash(metaHeader[:chainhash.HashSize])

		return nil, errors.NewProcessingError("subtree meta root hash mismatch for %s: meta was built for %s", subtreeHash.String(), metaRootHash.String())
	}

	subtreeLength, err := safeconversion.IntToUint32(subtree.Length())
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert subtree length for %s", subtreeHash.String(), err)
	}

	if claimedCount := binary.LittleEndian.Uint32(metaHeader[chainhash.HashSize:]); claimedCount != subtreeLength {
		return nil, errors.NewProcessingError("subtree meta entry count mismatch for %s: meta claims %d entries, subtree has %d transactions", subtreeHash.String(), claimedCount, subtreeLength)
	}

	subtreeMeta, err := subtreepkg.NewSubtreeMetaFromReader(subtree, io.MultiReader(bytes.NewReader(metaHeader[:]), reader))
	if err != nil {
		return nil, errors.NewProcessingError("failed to deserialize subtree meta for %s", subtreeHash.String(), err)
	}

	return subtreeMeta, nil
}
