package model

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestBestTipKey_RoundTrip(t *testing.T) {
	key := BestTipKey{
		ChainWork: bytes.Repeat([]byte{0x01}, 32),
		PeerID:    "12D3KooWpeer",
		ID:        123456,
		Hash:      chainhash.Hash{1, 2, 3},
	}

	got, err := NewBestTipKeyFromBytes(key.Bytes())
	require.NoError(t, err)
	require.Equal(t, key, got)

	mark := BestTipLowWater{Key: key, Generation: 7}

	gotMark, err := NewBestTipLowWaterFromBytes(mark.Bytes())
	require.NoError(t, err)
	require.Equal(t, mark, gotMark)

	// An empty key, as a probe uses, encodes too.
	empty, err := NewBestTipKeyFromBytes(BestTipKey{}.Bytes())
	require.NoError(t, err)
	require.Empty(t, empty.ChainWork)
}

func TestBestTipKey_RejectsOtherBytes(t *testing.T) {
	key := BestTipKey{ChainWork: bytes.Repeat([]byte{0x01}, 32), PeerID: "p", ID: 1}
	encoded := key.Bytes()

	// The 8-byte sequence number an unreleased build persisted.
	seq := binary.LittleEndian.AppendUint64(nil, 42)

	for name, b := range map[string][]byte{
		"empty":           nil,
		"sequence number": seq,
		"truncated":       encoded[:len(encoded)-1],
		"trailing":        append(append([]byte(nil), encoded...), 0),
		"field too long":  {bestTipKeyVersion, 0xff, 0xff, 0xff, 0xff},
	} {
		_, err := NewBestTipKeyFromBytes(b)
		require.Error(t, err, name)
	}

	_, err := NewBestTipLowWaterFromBytes(encoded)
	require.Error(t, err, "a mark carries its generation")
}
