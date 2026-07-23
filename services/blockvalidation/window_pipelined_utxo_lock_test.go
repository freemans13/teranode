package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newPipelinedUtxoLockSettings builds a minimal *settings.Settings carrying
// only the two values guardPipelinedUtxoLock reads: the two-stage window
// pipeline flag and the UTXO-lock-skip flag.
func newPipelinedUtxoLockSettings(pipelineOn, skipUtxoLock bool) *settings.Settings {
	s := &settings.Settings{}
	s.Legacy.PipelineWindowCommit = pipelineOn
	s.BlockValidation.QuickValidateSkipUtxoLock = skipUtxoLock

	return s
}

// TestGuardPipelinedUtxoLock_FlagOff_NeverErrors: with the two-stage pipeline
// OFF the guard is a no-op regardless of QuickValidateSkipUtxoLock, so every
// existing (flag-off) deployment is completely unaffected by this guard
// (I6-style byte-identical behaviour for the pre-existing path).
func TestGuardPipelinedUtxoLock_FlagOff_NeverErrors(t *testing.T) {
	u := &BlockValidation{settings: newPipelinedUtxoLockSettings(false, false)}
	require.NoError(t, u.guardPipelinedUtxoLock(), "pipeline flag OFF must never invoke the guard")

	u2 := &BlockValidation{settings: newPipelinedUtxoLockSettings(false, true)}
	require.NoError(t, u2.guardPipelinedUtxoLock(), "pipeline flag OFF must never invoke the guard, regardless of QuickValidateSkipUtxoLock")
}

// TestGuardPipelinedUtxoLock_FlagOn_RequiresSkipUtxoLock is the regression
// test for adversarial review Findings 4/5: the two-stage prepare/commit
// pipeline's CommitWindow unlock pass either silently vanishes across the
// PrepareBlockWindow/CommitBlockWindow RPC boundary (block.SubtreeSlices is
// prepare-side in-memory state, lost on re-deserialisation) or, if it were
// carried across, can race the next window's spend on the SAME tx row under
// Postgres READ COMMITTED and clobber the spend's delete_at_height. Both
// hazards vanish when QuickValidateSkipUtxoLock is on (UTXOs are created
// unlocked and the unlock pass never runs at all), so the pipeline must fail
// closed unless that setting is also enabled.
func TestGuardPipelinedUtxoLock_FlagOn_RequiresSkipUtxoLock(t *testing.T) {
	u := &BlockValidation{settings: newPipelinedUtxoLockSettings(true, false)}
	require.Error(t, u.guardPipelinedUtxoLock(),
		"pipeline flag ON with QuickValidateSkipUtxoLock OFF must fail closed")

	u2 := &BlockValidation{settings: newPipelinedUtxoLockSettings(true, true)}
	require.NoError(t, u2.guardPipelinedUtxoLock(),
		"pipeline flag ON with QuickValidateSkipUtxoLock ON must pass — both unlock hazards are removed by construction")
}

// TestPrepareWindow_FailsClosed_PipelineOnWithoutSkipUtxoLock proves the guard
// is actually wired into PrepareWindow's entry path (not merely defined but
// unused): a below-checkpoint window is rejected before any C1/C2 work runs.
func TestPrepareWindow_FailsClosed_PipelineOnWithoutSkipUtxoLock(t *testing.T) {
	u := &BlockValidation{
		settings: newPipelinedUtxoLockSettings(true, false),
		logger:   ulogger.TestLogger{},
	}

	blk := &model.Block{Height: 1}
	err := u.PrepareWindow(context.Background(), []*model.Block{blk}, "peer")
	require.Error(t, err, "PrepareWindow must fail closed when the pipeline flag is on without QuickValidateSkipUtxoLock")
}

// TestCommitWindow_FailsClosed_PipelineOnWithoutSkipUtxoLock is the
// defense-in-depth counterpart: CommitWindow re-checks the same guard so a
// standalone CommitBlockWindow RPC call (never preceded by this process's own
// PrepareWindow) cannot bypass it either.
func TestCommitWindow_FailsClosed_PipelineOnWithoutSkipUtxoLock(t *testing.T) {
	u := &BlockValidation{
		settings: newPipelinedUtxoLockSettings(true, false),
		logger:   ulogger.TestLogger{},
	}

	blk := &model.Block{Height: 1}
	err := u.CommitWindow(context.Background(), []*model.Block{blk}, "peer")
	require.Error(t, err, "CommitWindow must fail closed when the pipeline flag is on without QuickValidateSkipUtxoLock")
}
