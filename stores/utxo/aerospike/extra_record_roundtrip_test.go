package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestUnspendableOutputSlotRoundTrip pins the assumption the extra-record reader
// rests on: an output that was never entered into the UTXO set leaves its slot
// nil in the stored list, and that slot must come back as an UNTYPED nil.
//
// This matters because the reader distinguishes shapes by type. If a nil slot
// ever came back as a typed []uint8 of length zero instead, it would fall
// through to the reader's "unexpected length" branch and fail the read of a
// perfectly healthy record — breaking every paginated transaction that carries
// a data output (OP_FALSE OP_RETURN and friends, which ShouldStoreOutputAsUTXO
// declines to store). Nothing in our own code decides this; the Aerospike
// client's encoding does, so it is worth pinning against the real thing.
//
// The zero-length cases are pinned deliberately too: they are what a typed-nil
// or empty byte slice would look like coming back. Our writer never produces
// them, which is exactly why the reader is entitled to treat a zero-length
// element as damage rather than as an unspent output.
func TestUnspendableOutputSlotRoundTrip(t *testing.T) {
	ctx := context.Background()

	container, err := runAerospikeTestContainer(ctx)
	if err != nil {
		t.Skipf("Skipping Aerospike integration test: container not available (%v)", err)
	}

	defer func() { _ = container.Terminate(ctx) }()

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, aErr := aerospike.NewClient(host, port)
	require.Nil(t, aErr)

	defer client.Close()

	key, aErr := aerospike.NewKey("test", "extrarecord", "roundtrip")
	require.Nil(t, aErr)

	// The shape create.go writes: one slot per output, positionally, with an
	// unspendable output left nil and a spendable one holding its 32-byte hash.
	stored := []interface{}{
		nil,
		aerospike.NewBytesValue(make([]byte, 32)),
		[]byte(nil),
		[]byte{},
	}

	aErr = client.PutBins(nil, key, aerospike.NewBin("utxos", aerospike.NewListValue(stored)))
	require.Nil(t, aErr)

	record, aErr := client.Get(nil, key, "utxos")
	require.Nil(t, aErr)
	require.NotNil(t, record)

	got, ok := record.Bins["utxos"].([]interface{})
	require.True(t, ok)
	require.Len(t, got, 4)

	require.Nil(t, got[0], "an unspendable output's slot must come back as an untyped nil")

	spendable, ok := got[1].([]uint8)
	require.True(t, ok)
	require.Len(t, spendable, 32, "a spendable unspent output is its 32-byte hash")

	// A typed-nil and an empty byte slice both decode to a zero-length []uint8,
	// distinct from the untyped nil above. Our writer never stores either.
	for i := 2; i <= 3; i++ {
		empty, isBytes := got[i].([]uint8)
		require.True(t, isBytes, "element %d decodes as bytes, not as an untyped nil", i)
		require.Empty(t, empty)
	}

	// End to end: the reader must accept the real round-tripped list, leaving the
	// unspendable slot unspent rather than failing the record.
	spendingDatas := make([]*spendpkg.SpendingData, 2)
	require.NoError(t, applyExtraRecordUTXOs(&chainhash.Hash{}, 1, got[:2], 0, spendingDatas))
	require.Nil(t, spendingDatas[0])
	require.Nil(t, spendingDatas[1], "a 32-byte element is unspent, so it records no spending data")
}
