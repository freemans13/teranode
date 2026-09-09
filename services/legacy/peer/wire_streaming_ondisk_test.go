package peer

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// serialisedBlock builds a real block payload: header, transaction count, then
// that many transactions.
func serialisedBlock(t *testing.T, txs int) ([]byte, chainhash.Hash) {
	t.Helper()

	// wire.NewBlockHeader stamps Timestamp as time.Now(), which would make the
	// header hash, and therefore whether it meets its own target, different on
	// every run. The sink-path test below needs a header that reliably passes
	// checkBlockHeaderStandsAlone, so the timestamp is pinned and nonce 1 is
	// the smallest nonce whose hash actually meets the 0x207fffff target for
	// that fixed header; picked without checking, a nonce fails about half the
	// time, since that target sits at roughly the midpoint of the hash space.
	hdr := wire.NewBlockHeader(1, &chainhash.Hash{0x11}, &chainhash.Hash{0x22}, 0x207fffff, 1)
	hdr.Timestamp = time.Unix(1231006505, 0)
	blk := wire.NewMsgBlock(hdr)

	for i := 0; i < txs; i++ {
		tx := wire.NewMsgTx(1)
		tx.AddTxIn(&wire.TxIn{SignatureScript: []byte{byte(i)}, Sequence: 0xffffffff})
		tx.AddTxOut(&wire.TxOut{Value: 1, PkScript: []byte{0x51}})
		require.NoError(t, blk.AddTransaction(tx))
	}

	var buf bytes.Buffer
	require.NoError(t, blk.Serialize(&buf))

	return buf.Bytes(), hdr.BlockHash()
}

// Above the threshold the handler must not build a wire.MsgBlock at all. It
// reads the header, checks it, and hands the rest of the payload to the sink.
func TestStreamingBlockHandlerSendsALargeBodyToTheSink(t *testing.T) {
	payload, hash := serialisedBlock(t, 4)

	var (
		gotHash chainhash.Hash
		gotBody []byte
	)

	blockBodySink = func(h chainhash.Hash, r io.Reader, n int64) error {
		gotHash = h
		b, err := io.ReadAll(r)
		gotBody = b

		return err
	}
	t.Cleanup(func() { blockBodySink = nil })

	streamToDiskAtLeast = 1 // every block is "large" for this test
	t.Cleanup(func() { streamToDiskAtLeast = defaultStreamToDiskAtLeast })

	n, msg, buf, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)
	require.Nil(t, buf)
	require.Equal(t, 24+len(payload), n, "the whole message is accounted for")

	onDisk, ok := msg.(*MsgBlockOnDisk)
	require.True(t, ok, "a large block must come back as a body on disk, not a decoded block")
	require.Equal(t, hash, onDisk.Hash)
	require.Equal(t, uint64(4), onDisk.TxCount)
	require.Equal(t, int64(len(payload)), onDisk.Size)

	require.Equal(t, hash, gotHash, "the sink is keyed by the block hash")
	require.Equal(t, payload[80:], gotBody,
		"the sink gets everything after the header, byte for byte, including the transaction count")
}

// Below the threshold nothing changes: the block is decoded as it always was.
func TestStreamingBlockHandlerStillDecodesASmallBlock(t *testing.T) {
	payload, hash := serialisedBlock(t, 2)

	blockBodySink = func(chainhash.Hash, io.Reader, int64) error {
		t.Fatal("a small block must not reach the sink")
		return nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	streamToDiskAtLeast = int64(len(payload)) + 1
	t.Cleanup(func() { streamToDiskAtLeast = defaultStreamToDiskAtLeast })

	_, msg, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)

	blk, ok := msg.(*wire.MsgBlock)
	require.True(t, ok, "a small block is still a decoded block")
	require.Len(t, blk.Transactions, 2)
	require.Equal(t, hash, blk.BlockHash())
}

// A block that fails the header check must never be stored. This is what stops a
// peer filling the disk: the body is only written after proof of work passes.
func TestStreamingBlockHandlerRefusesABadHeaderBeforeStoring(t *testing.T) {
	payload, _ := serialisedBlock(t, 4)

	// Break the proof of work by making the target impossible to have met.
	// nBits sits at offset 72 of the 80-byte header.
	copy(payload[72:76], []byte{0x00, 0x00, 0x00, 0x00})

	called := false

	blockBodySink = func(chainhash.Hash, io.Reader, int64) error {
		called = true
		return nil
	}
	t.Cleanup(func() { blockBodySink = nil })

	streamToDiskAtLeast = 1
	t.Cleanup(func() { streamToDiskAtLeast = defaultStreamToDiskAtLeast })

	_, _, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.Error(t, err, "a header that fails its own target must be refused")
	require.False(t, called, "nothing may be written for a block that failed the check")
}

// With no sink installed, a large block falls back to decoding. That is what
// keeps every test and every caller that never wires a store working.
func TestStreamingBlockHandlerFallsBackWithNoSink(t *testing.T) {
	payload, _ := serialisedBlock(t, 2)

	blockBodySink = nil
	streamToDiskAtLeast = 1
	t.Cleanup(func() { streamToDiskAtLeast = defaultStreamToDiskAtLeast })

	_, msg, _, err := streamingBlockHandler(bytes.NewReader(payload), uint64(len(payload)), 24)
	require.NoError(t, err)
	require.IsType(t, &wire.MsgBlock{}, msg)
}
