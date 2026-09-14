package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParkIsBoundedByTheReadAheadDepth is what replaces the entry cap. With the
// committer stopped, the number of blocks outstanding must never exceed the
// depth, because the pass never names more than that many heights above the
// committed tip. This is the property the whole of 2026-09-12 was spent failing
// to achieve by counting.
func TestParkIsBoundedByTheReadAheadDepth(t *testing.T) {
	const depth = int32(8)

	sm, _, rec := cacheManager(t, 500, depth)

	// The committer never runs: the committed height stays where it is, so every
	// pass names the same range and nothing drains.
	for i := 0; i < 20; i++ {
		sm.fetchHeaderBlocks()
	}

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"sanity: the passes must have asked for something")

	require.LessOrEqual(t, sm.blockDownloads.Len(), int(depth),
		"with the committer stopped, no more than the read-ahead depth may ever be outstanding")
}
