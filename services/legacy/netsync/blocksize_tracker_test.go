// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockSizeTracker_RollingAverages verifies addBlockStats maintains both
// rolling averages and honours the maxSamples window. getAverageSize feeds the
// fetch-scheduler logging and remains live after the svnode-aligned fetch bake-in.
func TestBlockSizeTracker_RollingAverages(t *testing.T) {
	bst := newBlockSizeTracker(3) // window of 3

	bst.addBlockStats(100, 10)
	bst.addBlockStats(200, 20)
	bst.addBlockStats(300, 30)

	require.Equal(t, int64(200), bst.getAverageSize(), "avg of 100/200/300")
	require.Equal(t, int64(20), bst.getAverageTxCount(), "avg of 10/20/30")

	// Fourth sample evicts the first from both windows.
	bst.addBlockStats(600, 60)
	require.Equal(t, int64((200+300+600)/3), bst.getAverageSize())
	require.Equal(t, int64((20+30+60)/3), bst.getAverageTxCount())
}
