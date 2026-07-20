// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

// Tests for HeaderProgressStalled, the liveness clock that bounds the header
// freeze the widened catch-up deadlines would otherwise permit.
//
// The direction of its answer matters more than the answer itself: the sole
// consumer (peer.shouldExtendHeadersDeadline) grants the widened window when
// this returns FALSE. So every "we cannot tell" case must resolve to true, or
// the widest patience in the system is handed to the peer we have the least
// evidence about.

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestHeaderProgressStalled(t *testing.T) {
	newManager := func(t *testing.T, timeout time.Duration) *SyncManager {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Legacy.HeaderDeliveryTimeout = timeout

		return &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings}
	}

	t.Run("rollback lever disables the measure entirely", func(t *testing.T) {
		sm := newManager(t, 0)
		sm.headersFirstMode.Store(true)

		require.False(t, sm.HeaderProgressStalled(),
			"HeaderDeliveryTimeout=0 must report no stall so the peer layer keeps its pre-change behaviour")
	})

	t.Run("outside headers-first mode the measure does not apply", func(t *testing.T) {
		sm := newManager(t, 90*time.Second)

		require.False(t, sm.HeaderProgressStalled(),
			"the frontier is only single-sourced during headers-first; elsewhere this must not speak")
	})

	t.Run("an unstamped clock reports STALLED, not healthy", func(t *testing.T) {
		sm := newManager(t, 90*time.Second)
		sm.headersFirstMode.Store(true)

		require.True(t, sm.HeaderProgressStalled(),
			"a frontier we have never seen move must not be reported as advancing: false is consumed as 'be patient' and would grant the full IBD window to a peer that answered nothing")
	})

	t.Run("a recent batch is not a stall", func(t *testing.T) {
		sm := newManager(t, 90*time.Second)
		sm.headersFirstMode.Store(true)
		sm.lastHeaderProgressAt.Store(time.Now().UnixNano())

		require.False(t, sm.HeaderProgressStalled(),
			"the frontier moved just now, so the peer has earned its extension")
	})

	t.Run("a batch older than the timeout is a stall", func(t *testing.T) {
		sm := newManager(t, 90*time.Second)
		sm.headersFirstMode.Store(true)
		sm.lastHeaderProgressAt.Store(time.Now().Add(-91 * time.Second).UnixNano())

		require.True(t, sm.HeaderProgressStalled(),
			"no batch has connected for longer than HeaderDeliveryTimeout: the frontier is frozen")
	})
}
