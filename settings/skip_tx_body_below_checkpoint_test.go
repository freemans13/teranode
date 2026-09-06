package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestSkipTxBodyBelowCheckpoint_LoaderReadsKey guards the utxoset body-skip switch against the
// field-exists-but-loader-never-reads-it bug: a `key:` struct tag loads nothing on its own, so
// without a getBool in NewSettings the field stays false and the documented setting is
// silently unreadable.
//
// The default is false, which is also the Go zero value, so asserting the default alone would
// pass against a loader that never ran. The honest test sets the key and reads it back.
func TestSkipTxBodyBelowCheckpoint_LoaderReadsKey(t *testing.T) {
	const key = "utxostore_skipTxBodyBelowCheckpoint"

	// gocore resolves key.<context> first, so a runtime Set on the bare key is shadowed by any
	// settings.conf override under the ambient context. Set at the precedence that wins.
	ctx := gocore.Config().GetContext()
	winKey := key

	if ctx != "" {
		winKey = key + "." + ctx
	}

	require.False(t, NewSettings().UtxoStore.SkipTxBodyBelowCheckpoint,
		"the default must be off: skipping bodies is opt-in")

	gocore.Config().Set(winKey, "true")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	require.True(t, NewSettings().UtxoStore.SkipTxBodyBelowCheckpoint,
		"loader must read %s under context %q", key, ctx)
}
