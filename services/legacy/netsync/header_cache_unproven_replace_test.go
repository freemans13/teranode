package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// These tests pin when a below-checkpoint fill may replace the run the cache
// already holds instead of only extending it. Before this rule, a short run of
// headers that linked to the committed tip and stopped short of the next
// checkpoint was extended-only for ever: every honest reply was anchored at the
// tip, not at the fake run's top, so Fill refused it, nothing in the fake run was
// Wantable, the tip never moved and Prune never cleared it. One cheap reply
// stopped the below-checkpoint walk until restart.

// walkCheckpoints puts the checkpoint the walk is heading for at height 20 and
// a later one at 100, so every fill in these tests is below the last
// checkpoint and Fill takes its extend branch once the cache names a height
// above the tip.
func walkCheckpoints(honestHashes []chainhash.Hash) []chaincfg.Checkpoint {
	return []chaincfg.Checkpoint{
		{Height: 20, Hash: &honestHashes[19]},
		{Height: 100, Hash: &chainhash.Hash{0x77}},
	}
}

// A fake unproven run linked to the tip is displaced by an honest reply from
// the same tip that reaches higher, and the walk then carries on from the
// honest run's own top until the checkpoint proves it and its blocks become
// wantable.
func TestHeaderCache_UnprovenRunIsReplacedByAHigherReplyFromTheTip(t *testing.T) {
	tip := chainhash.Hash{0xaa}
	honest, honestHashes := linkedRun(tip, 30)
	cache := newHeaderCache().WithCheckpoints(walkCheckpoints(honestHashes))

	fake, _ := forgedRun(tip, 6)
	require.True(t, cache.Fill(tip, 1, fake), "the attacker's short run links to the tip and is cached")

	_, ok := cache.Wantable(1)
	require.False(t, ok, "nothing in an unproven run below the checkpoint is wantable")

	// The honest peer answers from the tip, as an honest peer does when it
	// cannot place the fake top hash the extending locator leads with.
	require.True(t, cache.Fill(tip, 1, honest[:10]),
		"an honest reply from the tip that reaches higher must replace an unproven run")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(10), top)

	for i := 0; i < 10; i++ {
		got, ok := cache.At(int32(i + 1)) //nolint:gosec // a small test height
		require.True(t, ok)
		require.Equal(t, honestHashes[i], got, "height %d must name the honest header", i+1)
	}

	// The next reply continues from the honest top, and reaches the checkpoint.
	require.True(t, cache.Fill(tip, 1, honest[10:25]), "the walk extends from the honest run's own top")
	require.Equal(t, int32(20), cache.ProvenTo())

	for h := int32(1); h <= 20; h++ {
		got, ok := cache.Wantable(h)
		require.True(t, ok, "height %d is proven by the checkpoint and must be wantable", h)
		require.Equal(t, honestHashes[h-1], got)
	}

	_, ok = cache.Wantable(21)
	require.False(t, ok, "above the matched checkpoint nothing is proven yet")
}

// A run that a checkpoint has proven above the tip is never displaced, not even
// by a longer run from the tip. Two shapes. A run that differs at the
// checkpoint height is refused by the checkpoint itself. A run that shares the
// proven prefix and diverges above the checkpoint passes that check, and only
// the rule that a proven held run is never replaced keeps the held run's
// suffix in place.
func TestHeaderCache_ProvenRunIsNotDisplacedByAnUnprovenReply(t *testing.T) {
	tip := chainhash.Hash{0xab}
	honest, honestHashes := linkedRun(tip, 25)
	cache := newHeaderCache().WithCheckpoints(walkCheckpoints(honestHashes))

	require.True(t, cache.Fill(tip, 1, honest))
	require.Equal(t, int32(20), cache.ProvenTo())

	longer, _ := forgedRun(tip, 40)
	require.False(t, cache.Fill(tip, 1, longer), "a run contradicting the checkpoint must not replace a proven one")

	divergent, _ := forgedRun(honestHashes[19], 10)
	divergent = append(append([]*wire.BlockHeader{}, honest[:20]...), divergent...)
	require.False(t, cache.Fill(tip, 1, divergent),
		"a run sharing the proven prefix but diverging above it must not replace the held run")

	require.Equal(t, int32(20), cache.ProvenTo())

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(25), top)

	for h := int32(1); h <= 25; h++ {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, honestHashes[h-1], got, "height %d must still name the proven chain", h)
	}
}

// An unproven run is not displaced by a reply from the tip that reaches less
// high: a peer that lags behind the walk must not reset it, and an attacker
// needs at least as much chain as the run already held.
func TestHeaderCache_UnprovenRunIsNotDisplacedByALowerReply(t *testing.T) {
	tip := chainhash.Hash{0xac}
	held, heldHashes := linkedRun(tip, 10)
	cache := newHeaderCache().WithCheckpoints(walkCheckpoints(append(heldHashes, make([]chainhash.Hash, 20)...)))

	require.True(t, cache.Fill(tip, 1, held))

	shorter, _ := forgedRun(tip, 9)
	require.False(t, cache.Fill(tip, 1, shorter), "a reply reaching lower than the held run must not replace it")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(10), top)

	got, ok := cache.At(1)
	require.True(t, ok)
	require.Equal(t, heldHashes[0], got)
}
