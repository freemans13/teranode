package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// Use the package's existing newTestPark (block_park_test.go:57). It builds a
// park over a REAL file-backed blob store through the production constructor,
// and returns the park plus its directory.
//
// Do NOT hand-roll a blockPark struct literal here. The billing map `charged`
// is written on every admit and a literal that omits it panics on a nil map.
// And the in-memory blob store is not usable at all: its key derivation ignores
// WithSubDirectory and WithNoHashPrefix, which the park always passes, so a
// test writing without those options lands on the same key by accident and
// passes for the wrong reason.

func TestHoldsBlock_FindsAWholeBlockOnDisk(t *testing.T) {
	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park}
	hash := chainhash.Hash{0x01}

	require.False(t, sm.holdsBlock(context.Background(), hash),
		"nothing has been written, so nothing is held")

	require.NoError(t, sm.blockPark.store.Set(context.Background(), hash[:], parkFileType, []byte("body"), parkOpts...))

	require.True(t, sm.holdsBlock(context.Background(), hash),
		"a whole block written by the streaming path must be found")
}

func TestHoldsBlock_FindsAConvertedRecordOnDisk(t *testing.T) {
	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park}
	hash := chainhash.Hash{0x02}

	require.NoError(t, sm.blockPark.store.Set(context.Background(), hash[:], fileformat.FileTypeBlock, []byte("record"), parkOpts...))

	require.True(t, sm.holdsBlock(context.Background(), hash),
		"a converted record written by the pipeline path must be found too, or every pipelined block is downloaded twice")
}

// TestHoldsBlock_DoesNotConsultTheEntryMap is the point of the whole task. The
// park's own Has() reads the in-memory map, which is empty on a restarting node
// before recovery and cannot answer for a block on disk.
func TestHoldsBlock_DoesNotConsultTheEntryMap(t *testing.T) {
	park, _ := newTestPark(t, "")
	sm := &SyncManager{logger: ulogger.TestLogger{}, blockPark: park}
	hash := chainhash.Hash{0x03}

	require.NoError(t, sm.blockPark.store.Set(context.Background(), hash[:], parkFileType, []byte("body"), parkOpts...))

	require.False(t, sm.blockPark.Has(hash),
		"the entry map knows nothing about it, which is the state a restart is in")
	require.True(t, sm.holdsBlock(context.Background(), hash),
		"and the files must answer anyway")
}

func TestHoldsBlock_IsSafeWithNoPark(t *testing.T) {
	sm := &SyncManager{logger: ulogger.TestLogger{}}

	require.False(t, sm.holdsBlock(context.Background(), chainhash.Hash{0x04}),
		"no park means nothing is held, which is the safe direction: we re-ask rather than skip")
}
