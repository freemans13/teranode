package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// finalityMTPLag is how far the parent's median time past sits below the candidate time in these
// tests: a time lock between the two is final by the header time and non-final by the median time
// past, which is the comparison bitcoin-sv makes from CSV onwards.
const finalityMTPLag = uint32(600)

// newCoinbaseFinalityHarness is the coinbase-height harness with CSV active at the candidate height,
// so a time-based coinbase nLockTime is measured against the parent's median time past, and with the
// assembler wired to the harness's blockchain mock for the median-time-past lookup.
func newCoinbaseFinalityHarness(t *testing.T) *coinbaseHeightHarness {
	t.Helper()

	params := chaincfg.RegressionNetParams
	params.CSVHeight = 1

	h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)
	h.server.blockAssembler.blockchainClient = h.blockchai

	return h
}

// parentMTP is the median time past the tests give the candidate's parent.
func (h *coinbaseHeightHarness) parentMTP() uint32 {
	return h.candidate.Time - finalityMTPLag
}

// warmMTP puts the parent's median time past in the assembler's memo, as a candidate built on that
// parent leaves it.
func (h *coinbaseHeightHarness) warmMTP(t *testing.T) {
	t.Helper()

	parent, err := chainhash.NewHash(h.candidate.PreviousHash)
	require.NoError(t, err)

	h.server.blockAssembler.placeMtpFloor(&mtpFloorEntry{parent: *parent, minTime: int64(h.parentMTP()) + 1})
}

// timeLockedCoinbase is a pool coinbase whose time-based nLockTime is enforced: its input sequence is
// not final.
func timeLockedCoinbase(t *testing.T, lockTime uint32) *bt.Tx {
	t.Helper()

	coinbase := coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000)
	coinbase.LockTime = lockTime
	coinbase.Inputs[0].SequenceNumber = 0

	return coinbase
}

// TestSubmitMiningSolution_CoinbaseMustBeFinal pins bitcoin-sv's IsFinalTx rule on the coinbase of a
// block this node mines (bitcoin-sv/teranode#4981). block.Valid on the submit path has no ancestor
// headers, so from CSV onwards it cannot evaluate a time-based nLockTime, and the block goes from the
// submit path straight to AddBlock. The submit path therefore fetches the parent's median time past
// itself, but only when the coinbase's verdict depends on it.
func TestSubmitMiningSolution_CoinbaseMustBeFinal(t *testing.T) {
	t.Run("a time lock at or after the parent's median time past is refused", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)
		h.warmMTP(t)

		// Final by the header time, which is what block.Valid would fall back to without headers
		// before CSV, and non-final by the median time past bitcoin-sv uses after it.
		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

		// The pool may submit again with a final coinbase, so the job stays and assembly is not
		// reset: nothing local is at fault.
		require.NotNil(t, h.server.jobStore.Get(h.id), "the job must survive a refused coinbase")
		h.requireNoReset(t)
	})

	t.Run("a time lock before the parent's median time past is accepted", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)
		h.warmMTP(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()-1))
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.blockchai.AssertCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		h.blockchai.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})

	t.Run("a cold memo is filled from the blockchain service", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		// A chain of eleven headers from the chain start, the parent last, one block every ten
		// minutes ending ten minutes before the candidate time. Their median is the sixth, sixty
		// minutes before the candidate time.
		headers := make([]*model.BlockHeader, 11)
		prev := &chainhash.Hash{}

		for i := range headers {
			headers[i] = &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prev,
				HashMerkleRoot: &chainhash.Hash{byte(i)},
				Timestamp:      h.candidate.Time - uint32(len(headers)-i)*600,
				Bits:           h.nBits,
			}
			prev = headers[i].Hash()
		}

		parent := headers[len(headers)-1]
		h.candidate.PreviousHash = parent.Hash().CloneBytes()

		// GetBlockHeaders answers newest first.
		newestFirst := make([]*model.BlockHeader, len(headers))
		for i, header := range headers {
			newestFirst[len(headers)-1-i] = header
		}

		h.blockchai.On("GetBlockHeader", mock.Anything, parent.Hash()).Return(parent, &model.BlockHeaderMeta{}, nil)
		h.blockchai.On("GetBlockHeaders", mock.Anything, parent.Hash(), mock.Anything).Return(newestFirst, []*model.BlockHeaderMeta{}, nil)

		mtp := h.candidate.Time - 3600

		resp, err := h.submit(t, timeLockedCoinbase(t, mtp))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")

		resp, err = h.submit(t, timeLockedCoinbase(t, mtp-1))
		require.NoError(t, err)
		require.True(t, resp.Ok)
	})

	t.Run("an unknown median time past refuses the solution", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)
		h.blockchai.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewServiceUnavailableError("blockchain down"))

		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()-1))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "cannot check that the coinbase is final")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		h.requireNoReset(t)
	})

	t.Run("a coinbase whose verdict does not depend on the median time past needs no lookup", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		// Cold memo and no GetBlockHeader expectation: a lookup would fail the mock.
		coinbase := timeLockedCoinbase(t, h.candidate.Time)
		coinbase.Inputs[0].SequenceNumber = bt.DefaultSequenceNumber

		resp, err := h.submit(t, coinbase)
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.blockchai.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})

	t.Run("a height lock is refused without a lookup", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, postGenesisCandidateHeight))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		h.blockchai.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})
}

// TestSubmitMiningSolution_RecreatedCoinbaseIsFinal pins the coinbase block assembly builds when a
// solution omits one. The solution's time and nonce used to be copied into that coinbase as its
// nLockTime and input sequence, which made it non-final for any non-zero nonce
// (bitcoin-sv/teranode#4981). They belong in the header only.
func TestSubmitMiningSolution_RecreatedCoinbaseIsFinal(t *testing.T) {
	h := newCoinbaseFinalityHarness(t)
	h.warmMTP(t)

	nTime := h.candidate.Time

	// The recreated coinbase carries a fresh random extranonce, so the merkle root, and with it
	// whether a given nonce meets the target, cannot be known in advance. Regtest's target takes
	// about half of all hashes, so resubmit until one does; a miss is a proof-of-work rejection,
	// which deletes the job and resets assembly, so put the job back each time.
	var (
		resp *blockassembly_api.OKResponse
		err  error
	)

	for attempt := 0; attempt < 64; attempt++ {
		h.server.jobStore.Set(h.id, h.job, time.Minute)

		resp, err = h.server.submitMiningSolution(context.Background(), &BlockSubmissionRequest{
			SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
				Id:    h.idBytes,
				Nonce: 7,
				Time:  &nTime,
			},
		})
		if err == nil {
			break
		}

		require.Contains(t, err.Error(), "target difficulty", "the only acceptable failure is a nonce that misses the target")

		select {
		case <-h.resets:
		case <-time.After(time.Second):
		}
	}

	require.NoError(t, err)
	require.True(t, resp.Ok)

	block := h.addedBlock(t)
	require.Equal(t, uint32(7), block.Header.Nonce, "the nonce belongs in the header")
	require.Equal(t, nTime, block.Header.Timestamp, "the time belongs in the header")
	require.Zero(t, block.CoinbaseTx.LockTime, "the time must not be copied into the coinbase nLockTime")
	require.Equal(t, bt.DefaultSequenceNumber, block.CoinbaseTx.Inputs[0].SequenceNumber, "the nonce must not be copied into the coinbase sequence")
	require.NoError(t, util.IsTransactionFinal(block.CoinbaseTx, block.Height, h.parentMTP()))
}
