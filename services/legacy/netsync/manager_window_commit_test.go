package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// commitSpyBlockValidation drives the post-ack window-commit path: it can fail
// ProcessBlockWindow to force flush into recovery, and independently control how
// many per-block ProcessBlock (recovery) calls fail and with which error. flush
// owns no reply channel, so the tests assert on commit/disconnect observables
// (call counts + sync-peer Connected()) rather than reply sends.
type commitSpyBlockValidation struct {
	blockvalidation.MockBlockValidation

	// windowErr is returned by every ProcessBlockWindow call to force recovery.
	windowErr error

	// perBlockErr is returned by the first perBlockFailUntil ProcessBlock calls;
	// ProcessBlock succeeds thereafter.
	perBlockErr       error
	perBlockFailUntil int32

	processWindowCalls atomic.Int32
	processBlockCalls  atomic.Int32
}

func (s *commitSpyBlockValidation) ProcessBlockWindow(_ context.Context, _ []*model.Block, _, _ string) error {
	s.processWindowCalls.Add(1)
	return s.windowErr
}

func (s *commitSpyBlockValidation) ProcessBlock(_ context.Context, _ *model.Block, _ uint32, _, _ string, _ uint32) error {
	n := s.processBlockCalls.Add(1)
	if n <= s.perBlockFailUntil {
		return s.perBlockErr
	}

	return nil
}

var _ blockvalidation.Interface = (*commitSpyBlockValidation)(nil)

// newConnectedSyncPeer builds a genuinely connected peer and installs it as the
// SyncManager's current sync peer, so a fatal-path disconnect is observable via
// Connected() flipping to false. Returns the peer for assertions.
func newConnectedSyncPeer(t *testing.T, sm *SyncManager) *peer.Peer {
	t.Helper()

	remote, smPeer, err := MakeConnectedPeers(t, peer.Config{
		UserAgentName:    "commit-test",
		UserAgentVersion: "1.0",
	}, peer.Config{
		UserAgentName:    "commit-test",
		UserAgentVersion: "1.0",
	}, 200)
	require.NoError(t, err)
	require.True(t, smPeer.Connected(), "freshly connected sync peer must report connected")
	_ = remote

	sm.storeSyncPeer(smPeer, &syncPeerState{})

	return smPeer
}

// TestFlush_InfraErrorRetriesToSuccess proves the bounded infra-retry: when
// ProcessBlockWindow fails and the per-block recovery loop fails with an infra
// (ErrStorageError) on its first pass but succeeds thereafter, flush retries
// (bounded, using the infra-classification predicate) and the batch commits —
// with NO sync-peer disconnect.
func TestFlush_InfraErrorRetriesToSuccess(t *testing.T) {
	spy := &commitSpyBlockValidation{
		windowErr:   errors.NewStorageError("window commit transient failure"),
		perBlockErr: errors.NewStorageError("recovery transient failure"),
	}
	sm := newAckTestSyncManager(t, spy)
	syncP := newConnectedSyncPeer(t, sm)

	const n = 3
	// Fail only the first recovery ProcessBlock call (one transient infra error),
	// then succeed — so the second bounded pass commits the whole window.
	spy.perBlockFailUntil = 1

	wa := newWindowAccumulator(1<<40, 20)
	for i := 0; i < n; i++ {
		wa.add(newMinimalModelBlock(t, uint32(500+i)))
	}

	wa.flush(sm.ctx, sm)

	require.Equal(t, int32(1), spy.processWindowCalls.Load(), "ProcessBlockWindow attempted exactly once")
	require.Greater(t, spy.processBlockCalls.Load(), int32(n),
		"recovery must retry the per-block loop after an infra failure (more than one pass)")
	require.True(t, syncP.Connected(),
		"infra-retry that ultimately succeeds must NOT disconnect the sync peer")
	require.True(t, wa.empty(), "flush must drain the window regardless of the recovery path")
}

// TestFlush_FatalErrorDisconnectsSyncPeer proves the escalation: when the
// per-block recovery always returns a NON-infra (fatal) error, flush exhausts
// or skips the bounded retry, then disconnects the current sync peer so the
// pipeline rotates and re-requests from committed best-block. It advances no
// best-block/progress state (there is none to touch here) and, because the
// blocks were already early-acked, flush owns no reply channel to send on.
func TestFlush_FatalErrorDisconnectsSyncPeer(t *testing.T) {
	spy := &commitSpyBlockValidation{
		windowErr:         errors.NewStorageError("window commit transient failure"),
		perBlockErr:       errors.NewBlockInvalidError("recovery fatal validation failure"),
		perBlockFailUntil: 1 << 30, // always fail
	}
	sm := newAckTestSyncManager(t, spy)
	syncP := newConnectedSyncPeer(t, sm)

	wa := newWindowAccumulator(1<<40, 20)
	wa.add(newMinimalModelBlock(t, 500))

	wa.flush(sm.ctx, sm)

	require.False(t, syncP.Connected(),
		"a non-infra fatal recovery error must disconnect the current sync peer")
	require.True(t, wa.empty(), "flush must still drain the window on the fatal path")
}

// TestFlush_InfraRetryExhaustionDisconnects proves that an infra error which
// never clears exhausts the bounded retry cap and then escalates by
// disconnecting the sync peer (rather than looping forever).
func TestFlush_InfraRetryExhaustionDisconnects(t *testing.T) {
	spy := &commitSpyBlockValidation{
		windowErr:         errors.NewServiceError("window commit transient failure"),
		perBlockErr:       errors.NewStorageError("recovery never clears"),
		perBlockFailUntil: 1 << 30, // always fail with an infra error
	}
	sm := newAckTestSyncManager(t, spy)
	syncP := newConnectedSyncPeer(t, sm)

	wa := newWindowAccumulator(1<<40, 20)
	wa.add(newMinimalModelBlock(t, 500))

	start := time.Now()
	wa.flush(sm.ctx, sm)

	require.False(t, syncP.Connected(),
		"an infra error that never clears must exhaust the retry cap and disconnect")
	require.GreaterOrEqual(t, spy.processBlockCalls.Load(), int32(windowCommitRetryCap),
		"recovery must attempt the per-block loop up to the retry cap before escalating")
	require.Less(t, time.Since(start), 5*time.Second,
		"bounded retry must not loop indefinitely")
	require.True(t, wa.empty(), "flush must drain the window on the exhaustion path")
}
