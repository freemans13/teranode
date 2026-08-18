package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacyFeelerSettings_LoaderReadsKeys guards the two feeler settings
// against the trap this branch has already fallen into once: a field can carry
// a full set of struct tags and still be dead, because nothing but a getInt or
// getDuration line in NewSettings actually reads the key. Without that line the
// field stays at its Go zero value, which for the budget is the documented
// "disable everything" value.
//
// Two assertions, for two different failure modes. The default check pins the
// shipped contract (one probe, two minutes apart) and only runs under a context
// carrying no settings.conf override. The override check runs everywhere: a
// distinctive value set at the winning precedence must come back out.
func TestLegacyFeelerSettings_LoaderReadsKeys(t *testing.T) {
	const (
		budgetKey   = "legacy_maxFeelerPeers"
		intervalKey = "legacy_feelerInterval"
	)

	// gocore resolves key.<context> ahead of the bare key, so a plain Set on the
	// base key is shadowed by any context override in settings.conf. Set at the
	// precedence that wins under the ambient context to keep this hermetic.
	ctx := gocore.Config().GetContext()
	winBudget, winInterval := budgetKey, intervalKey

	if ctx != "" {
		winBudget = budgetKey + "." + ctx
		winInterval = intervalKey + "." + ctx
	}

	if ctx == "" || ctx == "dev" {
		def := NewSettings()
		require.Equal(t, 1, def.Legacy.MaxFeelerPeers,
			"the shipped default is one probe, matching svnode's single feeler")
		require.Equal(t, 120*time.Second, def.Legacy.FeelerInterval,
			"the shipped default is svnode's FEELER_INTERVAL")
	}

	gocore.Config().Set(winBudget, "4")
	gocore.Config().Set(winInterval, "7s")

	t.Cleanup(func() {
		gocore.Config().Set(winBudget, "")
		gocore.Config().Set(winInterval, "")
	})

	loaded := NewSettings()

	require.Equal(t, 4, loaded.Legacy.MaxFeelerPeers,
		"NewSettings must read %s under context %q", budgetKey, ctx)
	require.Equal(t, 7*time.Second, loaded.Legacy.FeelerInterval,
		"NewSettings must read %s under context %q", intervalKey, ctx)
}
