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
// checking its fixed 36-byte header — the root hash the file was built for and
// the entry count it claims — against the subtree and key it is being read for
// (issue 1425).
//
// Two constraints are not obvious from the call site. The count is compared
// against Length(), because that is what Meta.serializeTxInpoints writes; Size()
// is cap(Nodes) and is larger whenever a pooled allocator or a short final
// subtree leaves headroom. And the subtree is compared against the key, because
// RootHash() returns the .subtree header's cached bytes and is never recomputed.
//
// Every producer writes the count as the subtree's node count keyed by its root,
// so any mismatch means a torn or foreign file. Callers with a regenerator behind
// them should rebuild rather than trust the file.
func NewSubtreeMetaFromValidatedReader(subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree, reader io.Reader) (*subtreepkg.Meta, error) {
	if subtree == nil {
		return nil, errors.NewProcessingError("cannot validate subtree meta for %s: subtree is nil", subtreeHash.String())
	}

	// The subtree has to answer to the same hash before the meta is checked
	// against it, or the check compares one file's claim to another's.
	// DeserializeFromReader caches the .subtree header's root and RootHash()
	// never recomputes it, so a foreign subtree stored under this key would
	// otherwise pass: full subtrees in a block share a leaf count, so the entry
	// count matches too. Done here rather than only at the call sites so every
	// caller inherits it.
	subtreeRootHash := subtree.RootHash()
	if subtreeRootHash == nil {
		return nil, errors.NewProcessingError("cannot validate subtree meta for %s: subtree has no root hash", subtreeHash.String())
	}

	if !subtreeRootHash.IsEqual(&subtreeHash) {
		return nil, errors.NewProcessingError("subtree does not match its key for %s: subtree file was built for %s", subtreeHash.String(), subtreeRootHash.String())
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
