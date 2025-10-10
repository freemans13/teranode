package smoke

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	utils "github.com/bsv-blockchain/teranode/test/utils"
)

// SharedTestLock is a mutex that ensures all tests in this package run sequentially
// This prevents port conflicts when multiple tests try to start daemons simultaneously
var SharedTestLock sync.Mutex

// RunSequentialTest is a helper function that ensures a test runs sequentially
// by acquiring the shared test lock before running the test function
func RunSequentialTest(t *testing.T, testFunc func(*testing.T)) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	testFunc(t)
}

// GetPeerInfoResponse represents the response structure for the getpeerinfo RPC call
type GetPeerInfoResponse struct {
	Result []bsvjson.GetPeerInfoResult `json:"result"`
	Error  *utils.JSONError            `json:"error"`
	ID     int                         `json:"id"`
}
