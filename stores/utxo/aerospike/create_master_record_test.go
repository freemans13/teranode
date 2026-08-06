package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// batchResults builds one CREATE_ONLY batch's per-record outcomes. A nil entry means the
// record was written by us; a non-nil one is the error Aerospike returned for it.
func batchResults(t *testing.T, errs ...aerospike.Error) []aerospike.BatchRecordIfc {
	t.Helper()

	records := make([]aerospike.BatchRecordIfc, len(errs))

	for i, err := range errs {
		key, keyErr := aerospike.NewKey("test", "txmeta", []byte{byte(i)})
		require.NoError(t, keyErr)

		record := aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("x", 1)))
		record.BatchRec().Err = err
		records[i] = record
	}

	return records
}

func keyExists() aerospike.Error {
	return &aerospike.AerospikeError{ResultCode: types.KEY_EXISTS_ERROR}
}

// TestClassifyCreateBatchResults pins the rule that decides whether THIS writer created a
// transaction, which is the fix for issue 1442.
//
// The consequential case is the third one. Record 0 is the master — it alone carries the
// block references, or the unmined marker when there are none. A writer that finds the
// master already present but fills in a missing child has NOT created the transaction:
// the mined-state metadata on record 0 belongs to whoever wrote it. Reporting success there
// is a lie the caller acts on, because block validation only repairs mined information when
// the store says the transaction already exists, so the master keeps the earlier writer's
// unmined marker and a mined transaction stays recorded as unmined.
//
// That case needs no concurrency at all: a partial batch failure deliberately leaves its
// records in place for the next attempt, so a later sequential create from a different
// caller lands exactly here.
func TestClassifyCreateBatchResults(t *testing.T) {
	tests := []struct {
		name             string
		errs             []aerospike.Error
		wantMaster       bool
		wantFailures     bool
		wantPresentCount int
	}{
		{
			name:       "we wrote every record, including the master",
			errs:       []aerospike.Error{nil, nil},
			wantMaster: true,
		},
		{
			name:             "every record already existed",
			errs:             []aerospike.Error{keyExists(), keyExists()},
			wantMaster:       false,
			wantPresentCount: 2,
		},
		{
			// The regression: master already present, child written by us.
			name:             "master already present, we only filled in a child",
			errs:             []aerospike.Error{keyExists(), nil},
			wantMaster:       false,
			wantPresentCount: 1,
		},
		{
			// The mirror: we own the master, someone else had already written a child.
			// We did create the transaction, so this must report success.
			name:             "we wrote the master, a child already existed",
			errs:             []aerospike.Error{nil, keyExists()},
			wantMaster:       true,
			wantPresentCount: 1,
		},
		{
			name:         "a genuine failure is not mistaken for already-present",
			errs:         []aerospike.Error{nil, &aerospike.AerospikeError{ResultCode: types.TIMEOUT}},
			wantMaster:   true,
			wantFailures: true,
		},
		{
			name:         "a failure on the master is a failure, not a creation",
			errs:         []aerospike.Error{&aerospike.AerospikeError{ResultCode: types.TIMEOUT}, nil},
			wantMaster:   false,
			wantFailures: true,
		},
		{
			name:       "single-record transaction we wrote",
			errs:       []aerospike.Error{nil},
			wantMaster: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			masterCreated, hasFailures, alreadyPresent := classifyCreateBatchResults(batchResults(t, tt.errs...))

			require.Equal(t, tt.wantMaster, masterCreated,
				"masterCreated decides whether the caller is told it created this transaction")
			require.Equal(t, tt.wantFailures, hasFailures)
			require.Len(t, alreadyPresent, tt.wantPresentCount)
		})
	}
}

// TestIsKeyExists pins the narrow error match the classifier relies on: only Aerospike's
// already-exists result counts as "someone got there first". Any other error, and any
// non-Aerospike error, must fall through to the failure path rather than being quietly
// treated as a completed previous attempt.
func TestIsKeyExists(t *testing.T) {
	require.True(t, isKeyExists(&aerospike.AerospikeError{ResultCode: types.KEY_EXISTS_ERROR}))
	require.False(t, isKeyExists(&aerospike.AerospikeError{ResultCode: types.TIMEOUT}))
	require.False(t, isKeyExists(&aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}))
	require.False(t, isKeyExists(errors.NewProcessingError("not an aerospike error")))
}
