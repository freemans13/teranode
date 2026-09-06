package settings_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// The struct tag is documentation; only the getInt call in settings.go loads a value. This
// test fails if either setting is declared but never loaded (the field would read 0, not 1).
func TestQuickWindowSettingsAreLoaded(t *testing.T) {
	t.Setenv("blockvalidation_quick_window_blocks", "3")
	t.Setenv("blockvalidation_quick_window_budget_mib", "256")

	s := settings.NewSettings("test")
	require.Equal(t, 3, s.BlockValidation.QuickWindowBlocks)
	require.Equal(t, 256, s.BlockValidation.QuickWindowBudgetMiB)
}

func TestQuickWindowSettingsDefaults(t *testing.T) {
	s := settings.NewSettings("test")
	require.Equal(t, 1, s.BlockValidation.QuickWindowBlocks)
	require.Equal(t, 0, s.BlockValidation.QuickWindowBudgetMiB)
}
