package peer

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// makeTestBlock builds a wire.MsgBlock with synthetic transactions whose
// scripts have a fixed size, useful for confirming the streaming decoder
// reconstructs the same structure that the buffered path produces.
func makeTestBlock(t *testing.T, numTxs, scriptLen int) *wire.MsgBlock {
	t.Helper()

	prev := chainhash.Hash{}
	merkle := chainhash.Hash{}
	header := wire.NewBlockHeader(1, &prev, &merkle, 0x1d00ffff, 0)

	block := wire.NewMsgBlock(header)
	for i := 0; i < numTxs; i++ {
		tx := wire.NewMsgTx(1)

		script := make([]byte, scriptLen)
		_, err := rand.Read(script)
		require.NoError(t, err)

		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: prev, Index: uint32(i)},
			SignatureScript:  script,
			Sequence:         0xffffffff,
		})
		tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: script})

		block.AddTransaction(tx)
	}

	return block
}

// installTestSink sets the three package-level streaming hooks for the
// duration of a test and returns a function restoring their previous values,
// so one test cannot leak state into the next.
func installTestSink(t *testing.T,
	sink func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) error,
	gate func(chainhash.Hash, *wire.BlockHeader) error,
	del func(chainhash.Hash) error,
) func() {
	t.Helper()

	prevSink, prevGate, prevDelete := blockBodySink, blockBodyGate, blockBodyDelete
	blockBodySink, blockBodyGate, blockBodyDelete = sink, gate, del

	return func() {
		blockBodySink, blockBodyGate, blockBodyDelete = prevSink, prevGate, prevDelete
	}
}

// limitedOver wraps b as the *io.LimitedReader readBlockMessage takes, bounded
// to its own length exactly as the wire layer bounds the declared payload.
func limitedOver(b []byte) *io.LimitedReader {
	return &io.LimitedReader{R: bytes.NewReader(b), N: int64(len(b))}
}

// testBlockPayload builds a block with numTxs synthetic transactions and a
// non-zero merkle root, and returns it alongside its full wire serialisation:
// header, transaction count, transactions. The merkle root must be non-zero
// (unlike makeTestBlock's) so a test can tell the real header apart from a
// zero-value one.
func testBlockPayload(t *testing.T, numTxs int) (*wire.MsgBlock, []byte) {
	t.Helper()

	prev := chainhash.Hash{0x01}
	merkle := chainhash.Hash{0xab, 0xcd, 0xef}
	header := wire.NewBlockHeader(1, &prev, &merkle, 0x1d00ffff, 0)

	block := wire.NewMsgBlock(header)
	for i := 0; i < numTxs; i++ {
		tx := wire.NewMsgTx(1)
		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: prev, Index: uint32(i)},
			SignatureScript:  []byte{byte(i)},
			Sequence:         0xffffffff,
		})
		tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: []byte{0x51}})
		require.NoError(t, block.AddTransaction(tx))
	}

	var buf bytes.Buffer
	require.NoError(t, block.Serialize(&buf))

	return block, buf.Bytes()
}

// TestStreamingBlockHandler_SinkReceivesTheHeader pins the new contract. The sink
// needs the header as a value, not as bytes at the front of a reader: the pipeline
// that will replace the body-storing sink needs the coinbase and the merkle root,
// and re-parsing bytes the wire layer has already parsed is waste on the read loop.
func TestStreamingBlockHandler_SinkReceivesTheHeader(t *testing.T) {
	streamToDiskAtLeast = 0
	t.Cleanup(func() { streamToDiskAtLeast = defaultStreamToDiskAtLeast })

	var gotHeader *wire.BlockHeader

	var gotHash chainhash.Hash

	restore := installTestSink(t,
		func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) error {
			gotHash = hash
			gotHeader = header

			_, err := io.Copy(io.Discard, r)

			return err
		},
		func(chainhash.Hash, *wire.BlockHeader) error { return nil },
		func(chainhash.Hash) error { return nil },
	)
	defer restore()

	blk, payload := testBlockPayload(t, 3)

	msg, err := readBlockMessage(limitedOver(payload), uint64(len(payload)))
	require.NoError(t, err)
	require.NotNil(t, msg)

	require.NotNil(t, gotHeader, "the sink must be handed the parsed header")
	require.Equal(t, blk.Header.MerkleRoot.String(), gotHeader.MerkleRoot.String(),
		"and it must be the block's own header, not a zero value")
	require.Equal(t, blk.BlockHash().String(), gotHash.String())
}

func TestStreamingBlockHandler_RoundTrip(t *testing.T) {
	wire.SetLimits(4000000000)

	want := makeTestBlock(t, 8, 256)

	var wire1 bytes.Buffer
	_, err := wire.WriteMessageN(&wire1, want, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	// Skip the 24-byte header so the handler receives the payload reader
	// it would get inside ReadMessageWithEncodingN.
	payloadLen := uint64(wire1.Len() - 24)
	_ = wire1.Next(24)

	n, msg, raw, err := streamingBlockHandler(&wire1, payloadLen, 24)
	require.NoError(t, err)
	require.Equal(t, int(payloadLen)+24, n)
	require.Nil(t, raw, "streaming handler must not retain the payload bytes")

	got, ok := msg.(*wire.MsgBlock)
	require.True(t, ok, "expected *wire.MsgBlock, got %T", msg)
	require.Equal(t, want.BlockHash(), got.BlockHash())
	require.Equal(t, len(want.Transactions), len(got.Transactions))

	for i := range want.Transactions {
		require.Equal(t,
			want.Transactions[i].TxHash(),
			got.Transactions[i].TxHash(),
			"tx %d hash mismatch", i)
	}
}

// TestStreamingBlockHandler_DrainsOnError verifies that a corrupted payload
// does not leave unread bytes on the underlying reader — otherwise the next
// ReadMessage call would parse those bytes as a fresh wire header and
// desync the stream.
func TestStreamingBlockHandler_DrainsOnError(t *testing.T) {
	const declared = 1024
	const truncated = 32

	// declared length 1024 but only 32 bytes of garbage followed by an
	// identifiable tail so we can detect whether the handler over-reads.
	payload := bytes.NewBuffer(make([]byte, truncated))
	tail := []byte("MARKER-AFTER-PAYLOAD")
	src := io.MultiReader(payload, bytes.NewReader(make([]byte, declared-truncated)), bytes.NewReader(tail))

	_, _, _, _ = streamingBlockHandler(src, declared, 0)

	got := make([]byte, len(tail))
	_, err := io.ReadFull(src, got)
	require.NoError(t, err)
	require.Equal(t, tail, got, "handler must drain exactly the declared payload, leaving the next message intact")
}

// TestStreamingBlockHandler_ShortStreamReturnsError verifies that when the
// underlying reader EOFs before the declared payload length is fully
// consumed, the handler returns an error. Without this check, a successful
// Bsvdecode followed by an undersized stream would silently succeed (io.Copy
// reports EOF as nil) and the next ReadMessage would desync.
func TestStreamingBlockHandler_ShortStreamReturnsError(t *testing.T) {
	const declared = 1024
	// 100 zero bytes is enough for Bsvdecode to parse an empty block (80
	// byte header + 1 byte tx-count varint = 81 bytes), with 19 bytes left
	// in the stream that the drain will consume before hitting EOF — well
	// short of the 1024 byte declared length.
	src := bytes.NewReader(make([]byte, 100))

	_, _, _, err := streamingBlockHandler(src, declared, 0)
	require.Error(t, err, "handler must surface short-stream as an error")
	require.Contains(t, err.Error(), "stream ended with", "error must identify the short-stream condition")
}

// TestRegisterStreamingBlockHandler_DispatchesViaWire verifies that after
// registration, wire.ReadMessageWithEncodingN takes the streaming code
// path for the "block" command: the returned payload slice must be nil
// (the streaming handler does not retain bytes) and the decoded block
// must match the original. This proves the handler is installed for the
// correct command, not just that calls do not panic.
func TestRegisterStreamingBlockHandler_DispatchesViaWire(t *testing.T) {
	wire.SetLimits(4000000000)
	RegisterStreamingBlockHandler()

	want := makeTestBlock(t, 4, 64)

	var buf bytes.Buffer
	_, err := wire.WriteMessageN(&buf, want, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	_, msg, raw, err := wire.ReadMessageWithEncodingN(&buf, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)
	require.Nil(t, raw, "streaming handler must not return the payload slice")

	got, ok := msg.(*wire.MsgBlock)
	require.True(t, ok, "expected *wire.MsgBlock, got %T", msg)
	require.Equal(t, want.BlockHash(), got.BlockHash())
	require.Equal(t, len(want.Transactions), len(got.Transactions))

	// Re-registering must not displace the installed handler — verify the
	// streaming path still wins after a second Register call.
	RegisterStreamingBlockHandler()
	RegisterStreamingBlockHandler()

	buf.Reset()
	_, err = wire.WriteMessageN(&buf, want, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	_, _, raw, err = wire.ReadMessageWithEncodingN(&buf, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)
	require.Nil(t, raw, "streaming handler must remain installed after repeated Register calls")
}
