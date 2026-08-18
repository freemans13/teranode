package legacy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAutomaticOutboundTarget pins the interaction between the automatic
// outbound target and MaxPeers once permanent peers stopped consuming automatic
// slots.
//
// Excluding addnode peers from the automatic tier is what makes the
// replenishment deficit meaningful, but it also means the node's outbound total
// is the target plus those peers. The original clamp compared MaxPeers against
// the target alone, so with the tier split in place a node given addnode peers
// would sit above the cap it was configured with — silently, because each half
// was individually within bounds.
func TestAutomaticOutboundTarget(t *testing.T) {
	tests := []struct {
		name       string
		configured uint32
		maxPeers   int
		permanent  int
		want       uint32
	}{
		{name: "no permanent peers leaves the target alone", configured: 8, maxPeers: 125, permanent: 0, want: 8},
		{name: "permanent peers reserve their share", configured: 8, maxPeers: 10, permanent: 4, want: 6},
		{name: "ample headroom is not clamped", configured: 8, maxPeers: 125, permanent: 4, want: 8},
		{name: "exactly enough room for both", configured: 8, maxPeers: 12, permanent: 4, want: 8},
		{name: "one short of enough room", configured: 8, maxPeers: 11, permanent: 4, want: 7},
		{name: "permanent peers fill the cap exactly", configured: 8, maxPeers: 4, permanent: 4, want: 0},
		{name: "permanent peers exceed the cap", configured: 8, maxPeers: 2, permanent: 4, want: 0},
		{name: "connect-only: cap equals the named peers", configured: 8, maxPeers: 3, permanent: 3, want: 0},
		{name: "cap alone still binds with no permanent peers", configured: 8, maxPeers: 5, permanent: 0, want: 5},
		{name: "zero configured target stays zero", configured: 0, maxPeers: 125, permanent: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := automaticOutboundTarget(tt.configured, tt.maxPeers, tt.permanent)
			require.Equal(t, tt.want, got)

			// The property the clamp exists for: the outbound total the node
			// will actually aim at must never exceed the cap it was given.
			if tt.maxPeers >= 0 {
				require.LessOrEqual(t, int(got)+tt.permanent, max(tt.maxPeers, tt.permanent),
					"automatic target plus permanent peers must stay within MaxPeers")
			}
		})
	}
}
