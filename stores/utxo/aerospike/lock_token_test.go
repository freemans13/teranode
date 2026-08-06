package aerospike

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenerateLockToken exercises the one piece of the lock-fencing change that can be
// verified without a live Aerospike server: acquireLock itself calls the concrete
// client directly, so anything that reaches it panics on a nil client before there is
// anything left to assert. generateLockToken has no such dependency, so this test
// checks it directly:
//   - it succeeds and returns a 16-byte value hex-encoded as 32 characters
//   - repeated calls return distinct tokens, which is the property the fence depends on:
//     if two acquisitions could ever produce the same token, the filter expression in
//     releaseLock could no longer tell them apart
func TestGenerateLockToken(t *testing.T) {
	const wantHexLen = 32 // 16 bytes, hex-encoded

	seen := make(map[string]struct{})

	const iterations = 1000
	for i := 0; i < iterations; i++ {
		token, err := generateLockToken()
		require.NoError(t, err)
		require.Len(t, token, wantHexLen)

		decoded, err := hex.DecodeString(token)
		require.NoError(t, err, "token must be valid hex")
		require.Len(t, decoded, 16)

		_, dup := seen[token]
		require.False(t, dup, "generateLockToken produced a duplicate token: %s", token)
		seen[token] = struct{}{}
	}
}
