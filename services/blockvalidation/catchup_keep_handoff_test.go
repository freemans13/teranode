package blockvalidation

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestKeepBlockAssemblyHandoff pins issue 2067's rule: a catchup keeps handing unseen
// transactions to block assembly only when it starts from RUNNING and fetches no more than
// CatchupKeepBlockAssemblyMaxBlocks blocks. A long run, a run from any other state, a failed
// state read and a bound of zero all keep the hand-off off, as before the rule existed.
func TestKeepBlockAssemblyHandoff(t *testing.T) {
	const bound = 6

	cases := []struct {
		name     string
		state    blockchain.FSMStateType
		stateErr error
		blocks   int
		bound    int
		want     bool
	}{
		{name: "short run from RUNNING", state: blockchain.FSMStateRUNNING, blocks: 2, bound: bound, want: true},
		{name: "run of exactly the bound from RUNNING", state: blockchain.FSMStateRUNNING, blocks: bound, bound: bound, want: true},
		{name: "run above the bound from RUNNING", state: blockchain.FSMStateRUNNING, blocks: bound + 1, bound: bound, want: false},
		{name: "short run from IDLE", state: blockchain.FSMStateIDLE, blocks: 2, bound: bound, want: false},
		{name: "short run already in CATCHINGBLOCKS", state: blockchain.FSMStateCATCHINGBLOCKS, blocks: 2, bound: bound, want: false},
		{name: "state read fails", stateErr: errors.NewServiceError("blockchain unavailable"), blocks: 2, bound: bound, want: false},
		{name: "bound of zero turns the rule off", state: blockchain.FSMStateRUNNING, blocks: 1, bound: 0, want: false},
	}

	blockUpTo := testhelpers.CreateTestBlockChain(t, 2)[1]

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.CatchupKeepBlockAssemblyMaxBlocks = tc.bound

			bc := &blockchain.Mock{}
			if tc.stateErr != nil {
				bc.On("GetFSMCurrentState", mock.Anything).Return((*blockchain.FSMStateType)(nil), tc.stateErr).Maybe()
			} else {
				state := tc.state
				bc.On("GetFSMCurrentState", mock.Anything).Return(&state, nil).Maybe()
			}

			srv := &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bc}

			got := srv.keepBlockAssemblyHandoff(context.Background(), &CatchupContext{blockUpTo: blockUpTo}, tc.blocks)
			require.Equal(t, tc.want, got, fmt.Sprintf("state=%s blocks=%d bound=%d", tc.state, tc.blocks, tc.bound))
		})
	}
}
