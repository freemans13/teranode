package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockForks(t *testing.T) {
	initPrometheusMetrics()

	testBlockHeaders := []*model.BlockHeader{
		testBlockHeader,
		{
			Version:        1,
			HashPrevBlock:  testBlockHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      234254767,
			Bits:           model.NBit{},
			Nonce:          234342,
		},
	}
	testBlockHeadersMeta := []*model.BlockHeaderMeta{
		testBlockHeaderMeta,
		{
			ID:          1234567890,
			Height:      123,
			TxCount:     125,
			SizeInBytes: 321,
			Miner:       "miner",
			BlockTime:   234254767,
			Timestamp:   234254767,
			ChainWork:   []byte("chainwork"),
		},
	}

	t.Run("Valid request with default limit", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil).Once()
		mockRepo.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).Return(testBlockHeaders, testBlockHeadersMeta, nil).Once()

		echoContext.SetPath("/block/forks/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(testBlockHeader.String())

		err := httpServer.GetBlockForks(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, echo.MIMEApplicationJSON, responseRecorder.Header().Get("Content-Type"))

		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		require.NotNil(t, response)
		assert.Len(t, response, 1)
		tree := response["tree"].(map[string]interface{})
		assert.Equal(t, float64(0), tree["id"])
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", tree["hash"])
		assert.Equal(t, "Miner", tree["miner"])
		assert.Equal(t, float64(1), tree["height"])
		assert.Equal(t, float64(3), tree["size"])
		assert.Len(t, tree["children"].([]interface{}), 1)

		childTree := tree["children"].([]interface{})[0].(map[string]interface{})
		assert.Equal(t, float64(1234567890), childTree["id"])
		assert.Equal(t, "71cbd4adaf3e4dc1bd8bbf6315663d039c494b5d5a23cb6a716545cc4af75dcf", childTree["hash"])
		assert.Equal(t, float64(123), childTree["height"])
		assert.Equal(t, float64(321), childTree["size"])
		assert.Equal(t, float64(125), childTree["tx_count"])
		assert.Equal(t, "miner", childTree["miner"])
	})

	t.Run("Invalid limit", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/forks/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(testBlockHeader.String())
		echoContext.QueryParams().Set("limit", "invalid")

		err := httpServer.GetBlockForks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid limit parameter", echoErr.Message)
	})

	t.Run("Invalid block hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/forks/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalidhash")

		err := httpServer.GetBlockForks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash length", echoErr.Message)
	})

	t.Run("Invalid block hash format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/forks/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")

		err := httpServer.GetBlockForks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid block hash format -> UNKNOWN (0): encoding/hex: invalid byte: U+007A 'z'", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewStorageError("error getting block header"))

		echoContext.SetPath("/block/forks/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockForks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "STORAGE_ERROR (69): error getting block header", echoErr.Message)
	})
}
