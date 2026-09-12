package netsync

import (
	"bufio"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// errBlockTxStreamDone reports that every declared transaction has been read. It
// is a normal end, not a failure, and is distinct from a truncated body.
//
// HAZARD: teranode's (*errors.Error).Is matches purely on the numeric error
// CODE (errors/errors.go), not on identity or message. This sentinel is a
// NewProcessingError, so any OTHER error in this file built with
// NewProcessingError becomes indistinguishable from clean exhaustion to a
// caller doing errors.Is(err, errBlockTxStreamDone) — a real failure would
// read as "nothing left to read," and a corrupt block would be treated as a
// complete one. Nothing else in this file may use NewProcessingError. Give
// every other failure here its own, different code (NewBlockInvalidError,
// as used below, is fine — it's code 11, this sentinel is code 4).
var errBlockTxStreamDone = errors.NewProcessingError("block transaction stream exhausted")

// maxBlockTxCount is the absolute ceiling on the peer-declared transaction count.
//
// It sits inside uint32 deliberately: the duplicate map the sink allocates from
// this count is txmap.NewSplitSwissMapUint64, which takes a uint32, and a count of
// 1<<32 would wrap to zero and silently produce a map that dedups nothing.
// 1<<31 is above any block this network will carry and still half of what uint32
// holds.
const maxBlockTxCount = 1 << 31

// minSerializedTxSize is the smallest a serialized transaction can possibly be:
// 4 version, 1 input count, 1 output count, 4 locktime. No real transaction is
// this small, which is the point — it is a floor for arithmetic, not an estimate.
const minSerializedTxSize = 10

// blockTxStreamReadBufferSize is the buffer between the socket and the decoder.
// Large enough that a transaction of ordinary size is assembled without a syscall
// per field, small enough that one per in-flight block is not a memory concern.
const blockTxStreamReadBufferSize = 256 * 1024

// blockTxStream yields a block's transactions one at a time from a reader
// positioned at the transaction count varint.
//
// It exists so that a block is never a whole object in memory. The count is read
// eagerly because the subtree partition is decided before the first transaction
// arrives; the transactions are read lazily and nothing here retains them.
type blockTxStream struct {
	r       *bufio.Reader
	txCount uint64
	read    uint64
}

// newBlockTxStream reads the declared transaction count and positions the stream
// at the first transaction. payloadLen is the block's declared wire size, and is
// what makes the count checkable rather than merely bounded.
func newBlockTxStream(r io.Reader, payloadLen int64) (*blockTxStream, error) {
	br := bufio.NewReaderSize(r, blockTxStreamReadBufferSize)

	count, err := wire.ReadVarInt(br, wire.ProtocolVersion)
	if err != nil {
		// NewBlockInvalidError, not NewProcessingError: a body too short to carry
		// a transaction count is a malformed block from the peer, not a local
		// processing fault, and giving it errBlockTxStreamDone's code would make
		// errors.Is match it against clean exhaustion (see the sentinel's comment).
		return nil, errors.NewBlockInvalidError("[blockTxStream] could not read the transaction count", err)
	}

	if count == 0 {
		return nil, errors.NewBlockInvalidError("[blockTxStream] block has no transactions, not even a coinbase")
	}

	// The body's own length is the tighter of the two bounds and the one that
	// actually stops the amplification: the caller sizes a duplicate map from this
	// count before a single transaction arrives, so a peer that declares two
	// billion transactions in a two-hundred-byte body would have this node
	// allocate for the claim and then receive nothing. A transaction cannot be
	// smaller than minSerializedTxSize, so a body this long cannot hold more than
	// this many, and a larger claim is a lie catchable for free.
	if payloadLen > 0 && count > uint64(payloadLen)/minSerializedTxSize {
		return nil, errors.NewBlockInvalidError("[blockTxStream] block declares %d transactions, more than its %d-byte body can hold", count, payloadLen)
	}

	if count > maxBlockTxCount {
		return nil, errors.NewBlockInvalidError("[blockTxStream] block declares %d transactions, above the %d limit", count, uint64(maxBlockTxCount))
	}

	return &blockTxStream{r: br, txCount: count}, nil
}

// TxCount is the transaction count the peer declared, including the coinbase.
func (s *blockTxStream) TxCount() uint64 {
	return s.txCount
}

// Next returns the next transaction and its hash, or errBlockTxStreamDone once
// every declared transaction has been read.
//
// The hash is computed here and handed on, rather than left for a caller to
// recompute: go-bt's TxIDChainHash re-serializes the whole transaction on a cache
// miss and does not populate its own cache, so a second caller asking for it pays
// the full cost again.
func (s *blockTxStream) Next() (*bt.Tx, *chainhash.Hash, error) {
	if s.read >= s.txCount {
		return nil, nil, errBlockTxStreamDone
	}

	tx := &bt.Tx{}
	if _, err := tx.ReadFrom(s.r); err != nil {
		return nil, nil, errors.NewBlockInvalidError("[blockTxStream] failed reading transaction %d of the %d declared", s.read, s.txCount, err)
	}

	s.read++

	hash := tx.TxIDChainHash()
	tx.SetTxHash(hash)

	return tx, hash, nil
}
