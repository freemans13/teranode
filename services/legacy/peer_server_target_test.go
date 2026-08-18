package legacy

import (
	"testing"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

// TestAutomaticOutboundTarget pins that MaxPeers bounds the automatic tier and
// nothing else.
//
// Permanent peers are budgeted separately by MaxAddnodePeers, so they must not
// shrink this target — that is what makes the addnode budget additive rather
// than a share of MaxPeers, and it is how svnode arranges the same two tiers:
// its addnode semaphore is sized independently of nMaxConnections, and its
// inbound arithmetic (nMaxConnections minus outbound and feeler) never
// mentions addnode at all.
func TestAutomaticOutboundTarget(t *testing.T) {
	tests := []struct {
		name       string
		configured uint32
		maxPeers   int
		want       uint32
	}{
		{name: "ample headroom leaves the target alone", configured: 8, maxPeers: 125, want: 8},
		{name: "cap below the target binds", configured: 8, maxPeers: 5, want: 5},
		{name: "cap equal to the target", configured: 8, maxPeers: 8, want: 8},
		{name: "zero configured target stays zero", configured: 0, maxPeers: 125, want: 0},
		{name: "zero cap yields no automatic peers", configured: 8, maxPeers: 0, want: 0},
		{name: "negative cap is treated as none", configured: 8, maxPeers: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, automaticOutboundTarget(tt.configured, tt.maxPeers))
		})
	}
}

// TestAddnodePeers pins the addnode budget itself.
//
// svnode enforces it with a semaphore of MaxAddnodePeers permits, so a longer
// -addnode list waits rather than growing the node without bound. Teranode's
// list is fixed at startup, so the equivalent is to dial the first budget-many
// and report the rest as ignored — silently truncating would leave an operator
// believing peers were connected that never were.
func TestAddnodePeers(t *testing.T) {
	four := []string{"a", "b", "c", "d"}

	tests := []struct {
		name        string
		configured  []string
		budget      int
		wantDial    []string
		wantDropped int
	}{
		{name: "within budget dials all", configured: four, budget: 8, wantDial: four},
		{name: "exactly at budget dials all", configured: four, budget: 4, wantDial: four},
		{name: "over budget dials the first few", configured: four, budget: 2, wantDial: []string{"a", "b"}, wantDropped: 2},
		{name: "zero budget dials none", configured: four, budget: 0, wantDial: []string{}, wantDropped: 4},
		{name: "negative budget dials none", configured: four, budget: -1, wantDial: []string{}, wantDropped: 4},
		{name: "none configured", configured: nil, budget: 8, wantDial: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dial, dropped := addnodePeers(tt.configured, tt.budget)
			require.Equal(t, tt.wantDial, dial)
			require.Equal(t, tt.wantDropped, dropped)
			require.LessOrEqual(t, len(dial), max(tt.budget, 0), "must never dial more named peers than the budget")
			require.Equal(t, len(tt.configured), len(dial)+dropped, "every configured peer is either dialed or reported dropped")
		})
	}
}

// TestCountExcludingPermanentIsAdditive pins the other half of the addnode
// budget: named peers must not eat the capacity that MaxPeers governs.
//
// The peer cap is enforced at the door in handleAddPeerMsg, so if permanent
// peers counted there, giving a node eight named peers would silently cost it
// eight inbound slots — the separate budget granted at startup and taken back
// on the first inbound connection. svnode avoids this by deriving inbound
// capacity from nMaxConnections minus the outbound and feeler budgets only,
// leaving addnode out of the sum entirely.
func TestCountExcludingPermanentIsAdditive(t *testing.T) {
	state := &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
	}

	for i := int32(0); i < 3; i++ {
		state.inboundPeers.Set(i, &serverPeer{})
	}

	for i := int32(0); i < 2; i++ {
		state.outboundPeers.Set(i, &serverPeer{})
	}

	require.Equal(t, 5, state.CountExcludingPermanent())
	require.Equal(t, 5, state.Count())

	// Named peers are additive: they raise the total the node holds without
	// drawing down the budget MaxPeers governs.
	for i := int32(0); i < 4; i++ {
		state.persistentPeers.Set(i, &serverPeer{})
	}

	require.Equal(t, 5, state.CountExcludingPermanent(),
		"permanent peers must not consume the capacity MaxPeers bounds")
	require.Equal(t, 9, state.Count(),
		"the node still holds them, and Count still reports the true total")
}

// TestClaimsNetgroup pins the last place a named peer could charge the
// automatic tier.
//
// The outbound group tally exists to stop the node spending several of its
// limited automatic slots on one network segment: newAddressFunc skips any
// candidate whose group is already represented. A named peer does not occupy an
// automatic slot, so if it claimed a group anyway, configuring one would cost
// the node an independently chosen address for a slot the named peer never
// took — the same charge the separate addnode budget exists to remove, levied
// in a different currency. svnode makes exactly this exclusion, and says why:
// addnode peers are left out of the setConnected group set because they "do not
// use our outbound slots".
//
// Claim and release must agree. handleAddPeerMsg and handleDonePeerMsg both
// call this, so a drift between them is impossible by construction rather than
// by two conditions being kept in step by hand.
func TestClaimsNetgroup(t *testing.T) {
	tests := []struct {
		name       string
		inbound    bool
		persistent bool
		want       bool
	}{
		{name: "automatic outbound claims its group", want: true},
		{name: "named outbound peer claims nothing", persistent: true},
		{name: "inbound peer claims nothing", inbound: true},
		{name: "inbound and named claims nothing", inbound: true, persistent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, claimsNetgroup(tt.inbound, tt.persistent))
		})
	}
}
