package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

func ceilingManager(t *testing.T, ceiling int) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.settings.Legacy.BlockFailureBackoffBase = time.Second
	sm.settings.Legacy.BlockFailureBackoffMaxDuration = 10 * time.Second
	sm.settings.Legacy.BlockFailureAttemptCeiling = ceiling
	sm.blockFailureBackoff = expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute).
		WithMaxSize(blockFailureBackoffMaxTracked)

	t.Cleanup(func() { sm.blockFailureBackoff.Stop() })

	return sm
}

// TestBlockGivenUpOn_StopsAtTheCeiling pins the bound that does not exist today.
// The retry SPACING is capped at 150s but the attempt COUNT is not, so a block
// that can never be accepted is asked for every 150 seconds for ever.
func TestBlockGivenUpOn_StopsAtTheCeiling(t *testing.T) {
	sm := ceilingManager(t, 3)
	hash := chainhash.Hash{0x01}

	for i := 0; i < 3; i++ {
		require.False(t, sm.blockGivenUpOn(hash),
			"attempt %d is inside the ceiling and must still be retried", i+1)
		sm.recordBlockFailureBackoff(hash)
	}

	require.True(t, sm.blockGivenUpOn(hash),
		"past the ceiling the block must be given up on rather than re-requested for ever")
}

func TestBlockGivenUpOn_ACeilingOfZeroNeverGivesUp(t *testing.T) {
	sm := ceilingManager(t, 0)
	hash := chainhash.Hash{0x02}

	for i := 0; i < 50; i++ {
		sm.recordBlockFailureBackoff(hash)
	}

	require.False(t, sm.blockGivenUpOn(hash),
		"zero disables the ceiling, which is the prior behaviour and must stay reachable")
}

func TestBlockGivenUpOn_ASuccessfulBlockStartsFresh(t *testing.T) {
	sm := ceilingManager(t, 3)
	hash := chainhash.Hash{0x03}

	for i := 0; i < 5; i++ {
		sm.recordBlockFailureBackoff(hash)
	}

	require.True(t, sm.blockGivenUpOn(hash))

	// What the success path already does on every commit.
	sm.blockFailureBackoff.Delete(hash)

	require.False(t, sm.blockGivenUpOn(hash),
		"a block that later commits must not stay given up on")
}

func TestBlockGivenUpOn_IsSafeWithNoBackoffMap(t *testing.T) {
	sm := newRaceManager(t)

	require.False(t, sm.blockGivenUpOn(chainhash.Hash{0x04}),
		"with the backoff disabled nothing is ever given up on")
}
