package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestQuickValidateBlock_NonFinalCoinbase pins bitcoin-sv/teranode#4981 on the quick-validation
// path, which never calls block.Valid: checkQuickValidationCoinbase applies
// model.CoinbaseFinalityViolation. A coinbase whose height-based nLockTime equals the block height,
// with a non-final input sequence, is non-final; the body is bound by the coinbase-only rule, so the
// verdict is invalid and the block is never committed.
func TestQuickValidateBlock_NonFinalCoinbase(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
	suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Height = 100
	block.CoinbaseTx.LockTime = block.Height
	block.CoinbaseTx.Inputs[0].SequenceNumber = 0
	block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()

	err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound non-final coinbase must condemn invalid, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")
	require.Contains(t, err.Error(), "bad-txns-nonfinal")

	suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
