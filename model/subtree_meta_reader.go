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

// NewSubtreeMetaFromValidatedReader deserializes a .subtreeMeta stream after
// validating its fixed 36-byte header — the subtree root hash the file was
// built for and the entry count it claims — against the subtree it is being
// read for (issue 1425). The file is only a cache, but its consumers trust
// the contents: a short claimed count leaves tail transactions with zero
// recorded inputs (block validation then spuriously rejects a valid block on
// the nil-parents guard), an over-long count writes past the deserializer's
// slice and panics on every restart since the file is on disk, and a foreign
// file attributes another subtree's inputs to this one — the one shape that
// can genuinely hide an in-block double-spend. Every producer writes the
// count as the subtree's node count keyed by the subtree root, so a mismatch
// always means a torn or foreign file, never a legitimate one.
func NewSubtreeMetaFromValidatedReader(subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree, reader io.Reader) (*subtreepkg.Meta, error) {
	var metaHeader [36]byte
	if _, err := io.ReadFull(reader, metaHeader[:]); err != nil {
		return nil, errors.NewProcessingError("failed to read subtree meta header for %s", subtreeHash.String(), err)
	}

	if !bytes.Equal(metaHeader[:32], subtreeHash[:]) {
		// Print the foreign hash in display order like every other hash in the
		// logs, or the one line meant for triage shows two incomparable hex
		// strings (the byte-order trap behind the phantom-fork misdiagnosis).
		metaRootHash := chainhash.Hash(metaHeader[0:32])

		return nil, errors.NewProcessingError("subtree meta root hash mismatch for %s: meta was built for %s", subtreeHash.String(), metaRootHash.String())
	}

	subtreeLength, err := safeconversion.IntToUint32(subtree.Length())
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert subtree length for %s", subtreeHash.String(), err)
	}

	if claimedCount := binary.LittleEndian.Uint32(metaHeader[32:36]); claimedCount != subtreeLength {
		return nil, errors.NewProcessingError("subtree meta entry count mismatch for %s: meta claims %d entries, subtree has %d transactions", subtreeHash.String(), claimedCount, subtreeLength)
	}

	subtreeMeta, err := subtreepkg.NewSubtreeMetaFromReader(subtree, io.MultiReader(bytes.NewReader(metaHeader[:]), reader))
	if err != nil {
		return nil, errors.NewProcessingError("failed to deserialize subtree meta for %s", subtreeHash.String(), err)
	}

	return subtreeMeta, nil
}
