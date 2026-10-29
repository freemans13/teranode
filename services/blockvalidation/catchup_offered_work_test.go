package blockvalidation

import (
	"math/big"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The gate weighs the offered list only once the fetch reached the announced block. A
// capped fetch is only a prefix: its work must never be mistaken for the announced tip's
// work, or a heavier chain beyond the cap cannot be synced. A fetch that ran past the
// announced block, to the peer's registry tip, is weighed whole.
func TestOfferedChainHasLessWork_OnlyCompleteOfferIsCompared(t *testing.T) {
	blocks := testhelpers.CreateTestBlockChain(t, 4)
	ancestor := &model.BlockHeaderMeta{Height: 100, ChainWork: big.NewInt(1000).Bytes()}
	offered := []*model.BlockHeader{blocks[1].Header}
	prefixWork := offeredChainWork(ancestor, offered)
	fullWork := offeredChainWork(ancestor, []*model.BlockHeader{blocks[1].Header, blocks[2].Header})
	pastTargetWork := offeredChainWork(ancestor, []*model.BlockHeader{blocks[1].Header, blocks[2].Header, blocks[3].Header})
	require.Greater(t, fullWork.Cmp(prefixWork), 0)
	require.Greater(t, pastTargetWork.Cmp(fullWork), 0)
	for _, tc := range []struct {
		name     string
		target   *model.Block
		headers  []*model.BlockHeader
		reached  bool
		bestWork *big.Int
		want     bool
	}{
		{"complete less-work offer", blocks[1], offered, true, fullWork, true},
		{"complete equal-work offer", blocks[1], offered, true, prefixWork, false},
		{"complete heavier offer", blocks[2], []*model.BlockHeader{blocks[1].Header, blocks[2].Header}, true, prefixWork, false},
		{"capped prefix of heavier offer", blocks[2], offered, false, new(big.Int).Add(prefixWork, big.NewInt(1)), false},
		{"less-work offer fetched past the announced block", blocks[1], []*model.BlockHeader{blocks[1].Header, blocks[2].Header, blocks[3].Header}, true, new(big.Int).Add(pastTargetWork, big.NewInt(1)), true},
		{"offer past the announced block heavier than our tip", blocks[1], []*model.BlockHeader{blocks[1].Header, blocks[2].Header, blocks[3].Header}, true, fullWork, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &Server{logger: ulogger.TestLogger{}}
			headers := append([]*model.BlockHeader{blocks[0].Header}, tc.headers...)
			ctx := &CatchupContext{blockUpTo: tc.target, commonAncestorMeta: ancestor, commonAncestorIndex: 0,
				commonAncestorHash: blocks[0].Header.Hash(),
				bestBlockMeta:      &model.BlockHeaderMeta{Height: 102, ChainWork: tc.bestWork.Bytes()},
				headersFetchResult: &catchup.Result{Headers: headers, ReachedTarget: tc.reached},
			}
			require.Equal(t, tc.want, srv.offeredChainHasLessWork(ctx))
		})
	}
}

// A list that skips a block weighs less than the chain it claims to be, so a peer could
// otherwise talk us out of a heavier chain it announced, or pass a gap off as secret mining.
// It is an invalid response, charged to the peer, whether or not the fetch reached the
// announced block.
func TestVerifyOfferedHeadersLink(t *testing.T) {
	blocks := testhelpers.CreateTestBlockChain(t, 4)
	for _, tc := range []struct {
		name    string
		headers []*model.BlockHeader
		reached bool
		invalid bool
	}{
		{"linked list", []*model.BlockHeader{blocks[0].Header, blocks[1].Header, blocks[2].Header, blocks[3].Header}, true, false},
		{"gap inside the list", []*model.BlockHeader{blocks[0].Header, blocks[1].Header, blocks[3].Header}, true, true},
		{"gap in a fetch that never reached the announced block", []*model.BlockHeader{blocks[0].Header, blocks[2].Header, blocks[3].Header}, false, true},
		{"first header does not follow the ancestor", []*model.BlockHeader{blocks[0].Header, blocks[2].Header}, false, true},
		{"nothing after the ancestor", []*model.BlockHeader{blocks[0].Header}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &CatchupContext{blockUpTo: blocks[3], commonAncestorIndex: 0,
				commonAncestorHash: blocks[0].Header.Hash(),
				headersFetchResult: &catchup.Result{Headers: tc.headers, ReachedTarget: tc.reached},
			}

			err := verifyOfferedHeadersLink(ctx)
			if tc.invalid {
				require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "got %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
