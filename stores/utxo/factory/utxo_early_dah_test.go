package factory

// Tests for maybeLatchEarlyDAHBoundary — the helper that publishes the highest
// hardcoded checkpoint height to a store's early-DAH boundary once the main chain's
// header at that height matches the pinned checkpoint hash. Fail-safe: any error,
// missing header, or hash mismatch must leave the boundary unset and return false so
// the caller probes again on a later block notification; a matched hash publishes the
// boundary once and returns true so the caller stops probing for the rest of the
// process lifetime.

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// earlyDAHSetterStub records whether SetEarlyDAHBoundary was called and with what
// height, without pulling in a full store implementation.
type earlyDAHSetterStub struct {
	called bool
	height uint32
}

func (s *earlyDAHSetterStub) SetEarlyDAHBoundary(h uint32) {
	s.called = true
	s.height = h
}

func TestMaybeLatchEarlyDAHBoundary(t *testing.T) {
	const checkpointHeight = uint32(1000)

	nBits := model.NBit{0xff, 0xff, 0x00, 0x1d}
	zeroHash := &chainhash.Hash{}

	newHeader := func(nonce uint32) *model.BlockHeader {
		return &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  zeroHash,
			HashMerkleRoot: zeroHash,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           nBits,
			Nonce:          nonce,
		}
	}

	cpHeader := newHeader(1000)   // the pinned checkpoint block
	otherHeader := newHeader(999) // a different block at the checkpoint height

	pinnedHash := cpHeader.Hash()
	checkpoints := []chaincfg.Checkpoint{{Height: int32(checkpointHeight), Hash: pinnedHash}}

	settingsWith := func(cps []chaincfg.Checkpoint, enabled bool) *settings.Settings {
		s := &settings.Settings{ChainCfgParams: &chaincfg.Params{Checkpoints: cps}}
		s.UtxoStore.EarlyDAHBelowCheckpoint = enabled

		return s
	}

	t.Run("header matches pinned hash: setter called with highest, returns true", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil)

		store := &earlyDAHSetterStub{}
		tSettings := settingsWith(checkpoints, true)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, store)

		require.True(t, latched)
		require.True(t, store.called)
		require.Equal(t, checkpointHeight, store.height)
	})

	t.Run("header missing: setter not called, returns false", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)

		store := &earlyDAHSetterStub{}
		tSettings := settingsWith(checkpoints, true)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, store)

		require.False(t, latched)
		require.False(t, store.called)
	})

	t.Run("header lookup error: setter not called, returns false", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader(nil), []*model.BlockHeaderMeta(nil), errors.NewServiceError("boom"))

		store := &earlyDAHSetterStub{}
		tSettings := settingsWith(checkpoints, true)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, store)

		require.False(t, latched)
		require.False(t, store.called)
	})

	t.Run("hash mismatch: setter not called, returns false", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader{otherHeader}, []*model.BlockHeaderMeta{{}}, nil)

		store := &earlyDAHSetterStub{}
		tSettings := settingsWith(checkpoints, true)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, store)

		require.False(t, latched)
		require.False(t, store.called)
	})

	t.Run("feature flag off: setter not called, returns true without any blockchain query", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil).Maybe()

		store := &earlyDAHSetterStub{}
		tSettings := settingsWith(checkpoints, false)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, store)

		require.True(t, latched)
		require.False(t, store.called)
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("no checkpoints configured: setter not called, returns true without any blockchain query", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil).Maybe()

		store := &earlyDAHSetterStub{}
		tSettings := settingsWith(nil, true)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, store)

		require.True(t, latched)
		require.False(t, store.called)
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("store does not implement the setter interface: returns true without any blockchain query", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil).Maybe()

		tSettings := settingsWith(checkpoints, true)

		latched := maybeLatchEarlyDAHBoundary(context.Background(), ulogger.TestLogger{}, tSettings, mockBC, struct{}{})

		require.True(t, latched)
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})
}
