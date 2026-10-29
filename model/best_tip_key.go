package model

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// BestTipLowWaterStateKey is the state key under which the blockchain store records the lowest
// place the best tip moved down to. Only a write that makes the best tip sort worse records it
// (invalidating or deleting a block), and block assembly clears it once it has replayed from it.
const BestTipLowWaterStateKey = "BestTipLowWater"

// bestTipKeyVersion is the first byte of an encoded BestTipKey.
const bestTipKeyVersion = 1

// BestTipKey is a block's place in the order the blockchain store picks its best tip by: most
// chain work first, then the lowest peer id, then the lowest block id. The order is total, and
// without an invalidation the best tip only ever moves to a block that sorts better, so a reader
// that keeps the key of the last block it handled finds every later best tip among the valid
// blocks that sort better than that key.
//
// ChainWork is the stored cumulative work, 32 bytes big-endian, so comparing it bytewise
// compares the work. Hash identifies the block and plays no part in the order.
type BestTipKey struct {
	ChainWork []byte
	PeerID    string
	ID        uint32
	Hash      chainhash.Hash
}

// NewBestTipKey returns the place of the block with this header and metadata.
func NewBestTipKey(header *BlockHeader, meta *BlockHeaderMeta) BestTipKey {
	return BestTipKey{
		ChainWork: append([]byte(nil), meta.ChainWork...),
		PeerID:    meta.PeerID,
		ID:        meta.ID,
		Hash:      *header.Hash(),
	}
}

// Bytes serialises the key: a version byte, the chain work and the peer id each behind a 4-byte
// length, the id, and the hash.
func (k BestTipKey) Bytes() []byte {
	b := make([]byte, 0, 1+4+len(k.ChainWork)+4+len(k.PeerID)+4+chainhash.HashSize)
	b = append(b, bestTipKeyVersion)
	b = appendBestTipBytes(b, k.ChainWork)
	b = appendBestTipBytes(b, []byte(k.PeerID))
	b = binary.LittleEndian.AppendUint32(b, k.ID)

	return append(b, k.Hash[:]...)
}

// NewBestTipKeyFromBytes parses a key written by Bytes. It fails on anything else, an empty or
// truncated value included, rather than guessing.
func NewBestTipKeyFromBytes(b []byte) (BestTipKey, error) {
	k, rest, err := readBestTipKey(b)
	if err != nil {
		return BestTipKey{}, err
	}

	if len(rest) != 0 {
		return BestTipKey{}, errors.NewProcessingError("invalid best tip key: %d trailing bytes", len(rest))
	}

	return k, nil
}

// BestTipLowWater is the low-water mark of the best tip: the key of the best tip right after it
// moved down, the lowest such key since the mark was last cleared. Generation grows on every
// write, including one that keeps the old key because it was lower, so a reader that clears the
// mark only when it still holds the bytes it read never clears a drop it has not seen.
type BestTipLowWater struct {
	Key        BestTipKey
	Generation uint64
}

// Bytes serialises the mark: the key followed by the generation.
func (m BestTipLowWater) Bytes() []byte {
	return binary.LittleEndian.AppendUint64(m.Key.Bytes(), m.Generation)
}

// NewBestTipLowWaterFromBytes parses a mark written by Bytes.
func NewBestTipLowWaterFromBytes(b []byte) (BestTipLowWater, error) {
	k, rest, err := readBestTipKey(b)
	if err != nil {
		return BestTipLowWater{}, err
	}

	if len(rest) != 8 {
		return BestTipLowWater{}, errors.NewProcessingError("invalid best tip low-water mark: expected 8 bytes of generation, got %d", len(rest))
	}

	return BestTipLowWater{Key: k, Generation: binary.LittleEndian.Uint64(rest)}, nil
}

func appendBestTipBytes(b, v []byte) []byte {
	// Chain work is 32 bytes and a peer id a short string; neither comes near 4 GiB.
	b = binary.LittleEndian.AppendUint32(b, uint32(len(v))) //nolint:gosec // see above

	return append(b, v...)
}

func readBestTipBytes(b []byte) ([]byte, []byte, error) {
	if len(b) < 4 {
		return nil, nil, errors.NewProcessingError("invalid best tip key: truncated length")
	}

	n := binary.LittleEndian.Uint32(b)
	b = b[4:]

	if uint64(n) > uint64(len(b)) {
		return nil, nil, errors.NewProcessingError("invalid best tip key: field of %d bytes, %d left", n, len(b))
	}

	return append([]byte(nil), b[:n]...), b[n:], nil
}

func readBestTipKey(b []byte) (BestTipKey, []byte, error) {
	if len(b) == 0 || b[0] != bestTipKeyVersion {
		return BestTipKey{}, nil, errors.NewProcessingError("invalid best tip key: unknown version")
	}

	var (
		k    BestTipKey
		peer []byte
		err  error
	)

	rest := b[1:]

	if k.ChainWork, rest, err = readBestTipBytes(rest); err != nil {
		return BestTipKey{}, nil, err
	}

	if peer, rest, err = readBestTipBytes(rest); err != nil {
		return BestTipKey{}, nil, err
	}

	k.PeerID = string(peer)

	if len(rest) < 4+chainhash.HashSize {
		return BestTipKey{}, nil, errors.NewProcessingError("invalid best tip key: truncated id or hash")
	}

	k.ID = binary.LittleEndian.Uint32(rest)
	copy(k.Hash[:], rest[4:4+chainhash.HashSize])

	return k, rest[4+chainhash.HashSize:], nil
}
