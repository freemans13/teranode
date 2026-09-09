package peer

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
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

// MsgBlockOnDisk is a block message whose body was streamed to the park rather
// than decoded. It satisfies wire.Message so go-wire's external handler can
// return it in place of a *wire.MsgBlock.
//
// Its encode and decode methods are never called: this message is produced by
// streamingBlockHandler and consumed inside this process, never re-read from or
// written to a peer. They return an error rather than doing nothing quietly, so
// a caller that starts using them finds out immediately.
type MsgBlockOnDisk struct {
	BlockBody
}

// Bsvdecode is not supported. See the type comment.
func (m *MsgBlockOnDisk) Bsvdecode(io.Reader, uint32, wire.MessageEncoding) error {
	return errors.NewProcessingError("MsgBlockOnDisk cannot be decoded: its body is on disk, not on the wire")
}

// BsvEncode is not supported. See the type comment.
func (m *MsgBlockOnDisk) BsvEncode(io.Writer, uint32, wire.MessageEncoding) error {
	return errors.NewProcessingError("MsgBlockOnDisk cannot be encoded: its body is on disk, not in memory")
}

// Command reports the same wire command a decoded block does, so anything
// switching on the command string treats the two alike.
func (m *MsgBlockOnDisk) Command() string { return wire.CmdBlock }

// MaxPayloadLength defers to the decoded block's answer.
func (m *MsgBlockOnDisk) MaxPayloadLength(pver uint32) uint64 {
	return (&wire.MsgBlock{}).MaxPayloadLength(pver)
}
