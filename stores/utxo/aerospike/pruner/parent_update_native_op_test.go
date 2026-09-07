package pruner

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// makeParentUpdates builds n distinct parentUpdateInfo entries with valid keys.
func makeParentUpdates(t *testing.T, n int) map[string]*parentUpdateInfo {
	t.Helper()

	updates := make(map[string]*parentUpdateInfo, n)

	for i := 0; i < n; i++ {
		var h chainhash.Hash
		h[0] = byte(i + 1)

		key, err := aerospike.NewKey("test", "utxo", h[:])
		require.NoError(t, err)

		updates[h.String()] = &parentUpdateInfo{key: key, childHashes: []*chainhash.Hash{&h}}
	}

	return updates
}

// isUDF reports whether a batch record is a Lua NewBatchUDF invocation (as
// opposed to the native/BatchWrite operate-path).
func isUDF(rec aerospike.BatchRecordIfc) bool {
	_, ok := rec.(*aerospike.BatchUDF)
	return ok
}

// Parent updates must prefer the native-op builder when it is available.
func TestBuildParentUpdateRecords_PrefersNativeBuilderOverUDF(t *testing.T) {
	nativeCalls := 0

	s := &Service{
		luaPackage: "teranode", // provider always sets this as the UDF fallback
		buildAddDeletedChildrenRecord: func(p *aerospike.BatchUDFPolicy, key *aerospike.Key, childHashes []interface{}) aerospike.BatchRecordIfc {
			nativeCalls++
			return aerospike.NewBatchWrite(aerospike.NewBatchWritePolicy(), key, aerospike.TouchOp())
		},
	}

	updates := makeParentUpdates(t, 2)

	records := s.buildParentUpdateRecords(updates)
	require.Len(t, records, len(updates))

	require.Equal(t, len(updates), nativeCalls, "native builder must be invoked once per parent update")

	for i := 0; i < len(records); i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must not be a NewBatchUDF when the native builder is present", i)
	}
}

// With no native builder but a lua package configured, the parent update path
// falls back to the Lua UDF call.
func TestBuildParentUpdateRecords_FallsBackToUDFWhenNoNativeBuilder(t *testing.T) {
	s := &Service{luaPackage: "teranode"}

	updates := makeParentUpdates(t, 2)

	records := s.buildParentUpdateRecords(updates)
	require.Len(t, records, len(updates))

	for i := 0; i < len(records); i++ {
		require.Truef(t, isUDF(records[i]), "parent update %d must be a NewBatchUDF when only luaPackage is set", i)
	}
}

// With neither a native builder nor a lua package, the fallback uses the
// plain MapPutItems BatchWrite (so
// results are parsed via KEY_NOT_FOUND, not a SUCCESS map).
func TestBuildParentUpdateRecords_PlainMapWriteWhenNoLuaNoNative(t *testing.T) {
	s := &Service{fieldDeletedChildren: "deletedChildren"}

	updates := makeParentUpdates(t, 2)

	records := s.buildParentMapUpdateRecords(updates)
	require.Len(t, records, len(updates))

	for i := 0; i < len(records); i++ {
		require.Falsef(t, isUDF(records[i]), "parent update %d must be a BatchWrite, not a UDF", i)
	}
}
