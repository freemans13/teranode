package peer

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// BlockBody describes a block whose transactions were streamed straight to the
// park's blob store rather than decoded into memory.
//
// It exists because nothing between the wire and the park reads a transaction.
// The park's stateless check needs the header, to see the block hashes to what
// we asked for and meets its own target difficulty, and needs the transaction
// count to be non-zero. The park's byte budget needs the size, which the wire
// message declares. The arrival path needs the previous block hash, which is in
// the header. On mainnet 91% of blocks arrive out of order and take exactly that
// path, so for those the four-gigabyte object was built only to be serialised
// straight back out.
type BlockBody struct {
	// Header is the block's own 80-byte header, read off the wire.
	Header wire.BlockHeader

	// TxCount is the transaction count varint that followed it.
	TxCount uint64

	// Size is the serialized block size, taken from the wire message's declared
	// payload length rather than by walking anything.
	Size int64

	// Hash is Header.BlockHash(), computed once when the header was read.
	Hash chainhash.Hash
}
