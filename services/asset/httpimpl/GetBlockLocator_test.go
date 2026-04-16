package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockLocator(t *testing.T) {
	initPrometheusMetrics()

	// Create test block hashes for the locator response
	hash1, _ := chainhash.NewHashFromStr("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
	hash2, _ := chainhash.NewHashFromStr("8d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea994")
	hash3, _ := chainhash.NewHashFromStr("7d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea993")
	testLocator := []*chainhash.Hash{hash1, hash2, hash3}

	t.Run("success without hash parameter", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return test data (doesn't take context in mock)
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, testBlockHeaderMeta, nil)

		// Mock GetBlockLocator to return test data (takes hash and height)
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), testBlockHeaderMeta.Height).Return(testLocator, nil)

		// No query parameters - should use best block
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		require.Contains(t, response, "block_locator")

		locatorArray, ok := response["block_locator"].([]interface{})
		require.True(t, ok)
		assert.Equal(t, 3, len(locatorArray))
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", locatorArray[0])
		assert.Equal(t, "8d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea994", locatorArray[1])
		assert.Equal(t, "7d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea993", locatorArray[2])
	})

	t.Run("success with hash parameter", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBlockLocator to return test data (takes hash and height)
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), uint32(0)).Return(testLocator, nil)

		// Set query parameter for hash
		echoContext.SetPath("/block_locator")
		echoContext.QueryParams().Add("hash", "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		require.Contains(t, response, "block_locator")

		locatorArray, ok := response["block_locator"].([]interface{})
		require.True(t, ok)
		assert.Equal(t, 3, len(locatorArray))
	})

	t.Run("success with hash and height parameters", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBlockLocator to return test data (takes hash and height)
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), uint32(100)).Return(testLocator, nil)

		// Set query parameters for hash and height
		echoContext.SetPath("/block_locator")
		echoContext.QueryParams().Add("hash", "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Add("height", "100")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		require.Contains(t, response, "block_locator")
	})

	t.Run("success with hash and invalid height parameter", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBlockLocator to return test data (height should be 0 since invalid)
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), uint32(0)).Return(testLocator, nil)

		// Set query parameters for hash and invalid height
		echoContext.SetPath("/block_locator")
		echoContext.QueryParams().Add("hash", "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Add("height", "invalid")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
	})

	t.Run("invalid hash parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// Set invalid hash parameter
		echoContext.SetPath("/block_locator")
		echoContext.QueryParams().Add("hash", "invalid")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message, "invalid hash parameter")
	})

	t.Run("invalid hash character", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// Set hash with invalid characters
		echoContext.SetPath("/block_locator")
		echoContext.QueryParams().Add("hash", "zd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99y")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message, "invalid hash parameter")
	})

	t.Run("GetBestBlockHeader not found error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return not found error
		mockRepo.On("GetBestBlockHeader").Return(nil, nil, errors.NewNotFoundError("best block header not found"))

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Contains(t, echoErr.Message, "best block header not found")
	})

	t.Run("GetBestBlockHeader error with 'not found' in message", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return error with "not found" in message
		mockRepo.On("GetBestBlockHeader").Return(nil, nil, errors.NewProcessingError("block not found in database"))

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Contains(t, echoErr.Message, "best block header not found")
	})

	t.Run("GetBestBlockHeader processing error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return processing error
		mockRepo.On("GetBestBlockHeader").Return(nil, nil, errors.NewProcessingError("database connection failed"))

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Contains(t, echoErr.Message, "error getting best block header")
	})

	t.Run("GetBlockLocator repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return test data
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, testBlockHeaderMeta, nil)

		// Mock GetBlockLocator to return error
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), testBlockHeaderMeta.Height).Return(nil, errors.NewProcessingError("error getting block locator"))

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Contains(t, echoErr.Message, "error getting block locator")
	})

	t.Run("empty block locator response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return test data
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, testBlockHeaderMeta, nil)

		// Mock GetBlockLocator to return empty array
		emptyLocator := []*chainhash.Hash{}
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), testBlockHeaderMeta.Height).Return(emptyLocator, nil)

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		require.Contains(t, response, "block_locator")

		locatorArray, ok := response["block_locator"].([]interface{})
		require.True(t, ok)
		assert.Equal(t, 0, len(locatorArray))
	})

	t.Run("single block in locator", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return test data
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, testBlockHeaderMeta, nil)

		// Mock GetBlockLocator to return single hash
		singleLocator := []*chainhash.Hash{hash1}
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), testBlockHeaderMeta.Height).Return(singleLocator, nil)

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		require.Contains(t, response, "block_locator")

		locatorArray, ok := response["block_locator"].([]interface{})
		require.True(t, ok)
		assert.Equal(t, 1, len(locatorArray))
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", locatorArray[0])
	})

	t.Run("large block locator response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock GetBestBlockHeader to return test data
		mockRepo.On("GetBestBlockHeader").Return(testBlockHeader, testBlockHeaderMeta, nil)

		// Create a large locator with many hashes
		largeLocator := make([]*chainhash.Hash, 20)
		for i := 0; i < 20; i++ {
			hash, _ := chainhash.NewHashFromStr("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
			largeLocator[i] = hash
		}
		mockRepo.On("GetBlockLocator", mock.AnythingOfType("*chainhash.Hash"), testBlockHeaderMeta.Height).Return(largeLocator, nil)

		// No query parameters
		echoContext.SetPath("/block_locator")

		// Call GetBlockLocator handler
		err := httpServer.GetBlockLocator(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		require.Contains(t, response, "block_locator")

		locatorArray, ok := response["block_locator"].([]interface{})
		require.True(t, ok)
		assert.Equal(t, 20, len(locatorArray))
	})
}
