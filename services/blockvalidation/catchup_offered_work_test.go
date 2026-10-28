package blockvalidation

import (
	"math/big"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A capped header fetch is only a prefix: its work must never be mistaken for
// the announced tip's work, or a heavier chain beyond the cap cannot be synced.
func TestOfferedChainHasLessWork_OnlyCompleteOfferIsCompared(t *testing.T) {
	blocks := testhelpers.CreateTestBlockChain(t, 3)
	ancestor := &model.BlockHeaderMeta{Height: 100, ChainWork: big.NewInt(1000).Bytes()}
	offered := []*model.BlockHeader{blocks[1].Header}
	prefixWork := offeredChainWork(ancestor, offered)
	fullWork := offeredChainWork(ancestor, []*model.BlockHeader{blocks[1].Header, blocks[2].Header})
	require.Greater(t, fullWork.Cmp(prefixWork), 0)
	for _, tc := range []struct {
		name     string
		target   *model.Block
		headers  []*model.BlockHeader
		bestWork *big.Int
		want     bool
	}{
		{"complete less-work offer", blocks[1], offered, fullWork, true},
		{"complete equal-work offer", blocks[1], offered, prefixWork, false},
		{"complete heavier offer", blocks[2], []*model.BlockHeader{blocks[1].Header, blocks[2].Header}, prefixWork, false},
		{"capped prefix of heavier offer", blocks[2], offered, new(big.Int).Add(prefixWork, big.NewInt(1)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &Server{logger: ulogger.TestLogger{}}
			headers := append([]*model.BlockHeader{blocks[0].Header}, tc.headers...)
			ctx := &CatchupContext{blockUpTo: tc.target, commonAncestorMeta: ancestor, commonAncestorIndex: 0,
				bestBlockMeta:      &model.BlockHeaderMeta{Height: 102, ChainWork: tc.bestWork.Bytes()},
				headersFetchResult: &catchup.Result{Headers: headers},
			}
			require.Equal(t, tc.want, srv.offeredChainHasLessWork(ctx))
		})
	}
}
