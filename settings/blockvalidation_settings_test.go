package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWindowBarrierCollapse_DefaultsOff guards the loader-vs-struct-tag class of
// bug: the struct tag default is documentation-only; only the explicit getBool
// call in settings.go populates the runtime value. WindowBarrierCollapse changes
// the ProcessBlockWindow concurrency structure, so it MUST default off — a
// missing loader entry would leave it false by luck, but an accidental default
// flip (or the wrong getBool default) would silently enable the overlap path in
// every deployment. This test fails loudly if that happens.
func TestWindowBarrierCollapse_DefaultsOff(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.False(t, tSettings.BlockValidation.WindowBarrierCollapse,
		"WindowBarrierCollapse must default to false; got true. "+
			"The overlap path is opt-in per deployment (needs pool budget + below-checkpoint). "+
			`If this fails, check the getBool("blockvalidation_windowBarrierCollapse", false, ...) `+
			"loader entry in settings.go and the struct-tag default.")
}
