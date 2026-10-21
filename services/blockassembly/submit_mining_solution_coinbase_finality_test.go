package blockassembly

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// finalityMTPLag is how far the parent's median time past sits below the candidate time in these
// tests: a time lock between the two is final by the header time and non-final by the median time
// past, which is the comparison bitcoin-sv makes from CSV onwards.
const finalityMTPLag = uint32(600)

// headerLookupCounter is a real blockchain client that counts the header reads the median-time-past
// lookup makes, so a test can assert that a lookup did or did not happen without mocking the client.
type headerLookupCounter struct {
	blockchain.ClientI
	lookups atomic.Int32
}

func (c *headerLookupCounter) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.lookups.Add(1)

	return c.ClientI.GetBlockHeader(ctx, hash)
}

func (c *headerLookupCounter) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.lookups.Add(1)

	return c.ClientI.GetBlockHeaders(ctx, hash, n)
}

// coinbaseFinalityHarness is the coinbase-height harness plus the real blockchain client the
// assembler reads the parent's median time past from.
type coinbaseFinalityHarness struct {
	*coinbaseHeightHarness
	chain *headerLookupCounter
}

// newCoinbaseFinalityHarness is the coinbase-height harness with CSV active at the candidate height,
// so a time-based coinbase nLockTime is measured against the parent's median time past, and with the
// assembler reading headers from a sqlitememory blockchain store. That store holds only genesis, so
// the harness's parent is unknown to it until a test stores a chain and points the candidate at it.
// AddBlock still goes to the coinbase-height harness's client, which records the mined block.
func newCoinbaseFinalityHarness(t *testing.T) *coinbaseFinalityHarness {
	t.Helper()

	params := chaincfg.RegressionNetParams
	params.CSVHeight = 1

	h := newCoinbaseHeightHarness(t, &params, postGenesisCandidateHeight, 5_000_000)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, h.server.blockAssembler.settings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, h.server.blockAssembler.settings, store, nil, nil)
	require.NoError(t, err)

	chain := &headerLookupCounter{ClientI: client}
	h.server.blockAssembler.blockchainClient = chain

	return &coinbaseFinalityHarness{coinbaseHeightHarness: h, chain: chain}
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
		require.Zero(t, h.chain.lookups.Load(), "no median-time-past lookup was needed")
	})

	t.Run("a cold memo is filled from the blockchain service", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		// Eleven blocks on the real store stamped base..base+10, so the parent's median time past is
		// base+5: an hour before the candidate time.
		mtp := h.candidate.Time - 3600
		parent := buildFutureChain(t, t.Context(), h.chain.ClientI, mtp-5)
		h.candidate.PreviousHash = parent.Hash().CloneBytes()

		resp, err := h.submit(t, timeLockedCoinbase(t, mtp))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		require.NotZero(t, h.chain.lookups.Load(), "a cold memo must be filled from the blockchain service")

		resp, err = h.submit(t, timeLockedCoinbase(t, mtp-1))
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.blockchai.AssertCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("an unknown median time past refuses the solution", func(t *testing.T) {
		// The harness's parent is not in the store, so the lookup fails as it would against an
		// unreachable or out-of-step blockchain service.
		h := newCoinbaseFinalityHarness(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, h.parentMTP()-1))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "cannot check that the coinbase is final")
		h.blockchai.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		h.requireNoReset(t)
	})

	t.Run("a coinbase whose verdict does not depend on the median time past needs no lookup", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		// Cold memo and a parent the store does not hold: a lookup would refuse the solution.
		coinbase := timeLockedCoinbase(t, h.candidate.Time)
		coinbase.Inputs[0].SequenceNumber = bt.DefaultSequenceNumber

		resp, err := h.submit(t, coinbase)
		require.NoError(t, err)
		require.True(t, resp.Ok)
		require.Zero(t, h.chain.lookups.Load(), "no median-time-past lookup was needed")
	})

	t.Run("a height lock is refused without a lookup", func(t *testing.T) {
		h := newCoinbaseFinalityHarness(t)

		resp, err := h.submit(t, timeLockedCoinbase(t, postGenesisCandidateHeight))
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-nonfinal")
		require.Zero(t, h.chain.lookups.Load(), "no median-time-past lookup was needed")
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
