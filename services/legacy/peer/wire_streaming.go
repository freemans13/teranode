package peer

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
)

// defaultStreamToDiskAtLeast is the payload size at or above which a block's
// body is streamed to disk instead of decoded. Task 5 derives the live value
// from the process memory limit; this is the fallback when that is unset.
const defaultStreamToDiskAtLeast = 64 << 20 // 64 MiB

// streamToDiskAtLeast is the live threshold. A package variable so the daemon
// can set it from the memory limit at startup and tests can move it.
var streamToDiskAtLeast int64 = defaultStreamToDiskAtLeast

// blockBodySink stores a block body streamed off the wire. Nil until the sync
// manager installs one, and a nil sink means every block is decoded, which is
// what keeps callers that never wire a store working unchanged.
var blockBodySink func(hash chainhash.Hash, r io.Reader, n int64) error

// streamingBlockHandler is a wire.SetExternalHandler implementation for the
// "block" message that decodes the block payload directly from the network
// reader, avoiding the default ReadMessageWithEncodingN behaviour of
// allocating the full payload as a []byte before calling Bsvdecode. On fat
// blocks (multi-GB testnet stress blocks) that buffer alone reached
// ~2.86 GB of legacy heap inuse during sync, the second-largest contributor
// to RSS after the per-tx scratch buffer.
//
// Note on the wire-level DoubleHash checksum: the default path verifies the
// peer-supplied checksum over the payload bytes. This handler skips it, so
// the early-rejection signal that checksum provides is lost. Integrity is
// preserved by the existing downstream validation in
// netsync.HandleBlockDirect — PoW via HasMetTargetDifficulty, merkle root
// reconstruction during subtree preparation, and per-tx parse + validate.
// Any payload corruption that a wire-level checksum would have caught also
// fails one of those downstream checks; what we give up is rejecting a bad
// block before paying the decode cost. Preserving the checksum under
// streaming would require a TeeReader → SHA-256 pass over multi-GB
// payloads, which is not justified given the downstream guarantees.
//
// Above streamToDiskAtLeast, and only when a sink is installed, the body is
// never decoded at all: the header is read, checked, and everything after it
// goes straight to the sink. That is what keeps a multi-gigabyte block from
// ever existing as a Go object on this path.
func streamingBlockHandler(r io.Reader, length uint64, totalBytes int) (int, wire.Message, []byte, error) {
	// Cap the inner reader so a malformed varint cannot read past the declared
	// payload boundary and desync the next ReadMessage call.
	lr := &io.LimitedReader{R: r, N: int64(length)}

	msg, err := readBlockMessage(lr, length)

	// Drain any unread payload bytes so the next ReadMessage starts on a clean
	// header boundary, and surface a short stream as an error. io.Copy on a
	// LimitedReader returns nil if the underlying reader EOFs before N reaches
	// 0, so lr.N is checked explicitly.
	var drainErr error

	if lr.N > 0 {
		if _, copyErr := io.Copy(io.Discard, lr); copyErr != nil {
			drainErr = copyErr
		} else if lr.N > 0 {
			drainErr = fmt.Errorf("streaming block: peer declared %d byte payload but stream ended with %d bytes unread", length, lr.N)
		}
	}

	if err == nil {
		err = drainErr
	}

	// totalBytes accounts for the header already read by
	// ReadMessageWithEncodingN; add the full declared payload length so the
	// caller's bytesReceived counter stays consistent with the non-streaming
	// path regardless of how many bytes were actually consumed before erroring.
	return totalBytes + int(length), msg, nil, err
}

// readBlockMessage returns the block either decoded or as a body on disk,
// depending on its declared size and whether a sink is installed.
func readBlockMessage(lr *io.LimitedReader, length uint64) (wire.Message, error) {
	if blockBodySink == nil || int64(length) < streamToDiskAtLeast {
		msg := &wire.MsgBlock{}

		return msg, msg.Bsvdecode(lr, wire.ProtocolVersion, wire.BaseEncoding)
	}

	var header wire.BlockHeader
	if err := header.Deserialize(lr); err != nil {
		return nil, fmt.Errorf("streaming block: could not read the header: %w", err)
	}

	hash := header.BlockHash()

	// The stateless check runs HERE, before a byte of body is stored, and it is
	// the whole reason a peer cannot fill this node's disk. Proof of work is what
	// stops an attacker minting unlimited distinct "blocks"; it is header-only,
	// so it costs nothing to do at the wire.
	//
	// It used to run in the park, in validateParkCandidate, AFTER the body had
	// been written. That was safe when the body arrived already decoded, because
	// the decode itself bounded what a peer could send. Streaming removes that
	// bound, so the check has to move ahead of the write. WriteAdmitted skips it
	// for a streamed body precisely because it has already happened here.
	if err := checkBlockHeaderStandsAlone(&header, hash); err != nil {
		return nil, err
	}

	// Everything from the transaction count onward goes to the sink untouched,
	// so what is stored is byte-for-byte what a serialized block is: header,
	// count, transactions. The count is read back out of the stored bytes rather
	// than here, because reading it here would mean putting it back.
	var counted countingReader

	counted.r = lr

	if err := blockBodySink(hash, &counted, int64(length)-int64(wire.MaxBlockHeaderPayload)); err != nil {
		return nil, fmt.Errorf("streaming block %s: could not store the body: %w", hash, err)
	}

	txCount, err := wire.ReadVarInt(bytes.NewReader(counted.first), wire.ProtocolVersion)
	if err != nil {
		return nil, fmt.Errorf("streaming block %s: could not read the transaction count: %w", hash, err)
	}

	return &MsgBlockOnDisk{BlockBody{
		Header:  header,
		TxCount: txCount,
		Size:    int64(length),
		Hash:    hash,
	}}, nil
}

// countingReader passes bytes straight through while keeping the first nine,
// which is enough to hold any transaction-count varint. That lets the count be
// read without buffering the body or reading the file back.
type countingReader struct {
	r     io.Reader
	first []byte
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)

	if len(c.first) < 9 && n > 0 {
		want := 9 - len(c.first)
		if want > n {
			want = n
		}

		c.first = append(c.first, p[:want]...)
	}

	return n, err
}

// checkBlockHeaderStandsAlone is everything about a block that can be judged
// from its 80-byte header, with no transactions and no chain context.
//
// It is the same pair of questions the park used to ask after writing the body:
// does the block hash to what the header says, and does it meet its own target
// difficulty. Asked here, before the body is stored, they are what stops a peer
// filling the disk with rubbish. Whether nBits itself is right needs chain
// context and stays where it is.
func checkBlockHeaderStandsAlone(header *wire.BlockHeader, hash chainhash.Hash) error {
	var headerBytes bytes.Buffer
	if err := header.Serialize(&headerBytes); err != nil {
		return fmt.Errorf("streaming block %s: could not serialize the header: %w", hash, err)
	}

	h, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return fmt.Errorf("streaming block %s: could not read the header: %w", hash, err)
	}

	if met, _, err := h.HasMetTargetDifficulty(); !met {
		return fmt.Errorf("streaming block %s: does not meet its own target difficulty: %w", hash, err)
	}

	return nil
}

var registerStreamingBlockHandlerOnce sync.Once

// RegisterStreamingBlockHandler installs the streaming "block" handler with
// go-wire globally. Safe to call multiple times; the registration runs at
// most once. Call this once during legacy service startup, after any other
// wire-level configuration (e.g. wire.SetLimits).
func RegisterStreamingBlockHandler() {
	registerStreamingBlockHandlerOnce.Do(func() {
		wire.SetExternalHandler(wire.CmdBlock, streamingBlockHandler)
	})
}
