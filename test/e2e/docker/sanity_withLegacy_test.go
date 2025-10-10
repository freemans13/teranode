//go:build test_smoke || test_functional

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldSyncWithLegacyWhenInitialBlocksAreGeneratedOnLegacy$" -tags test_functional
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldSyncWithLegacy$" -tags test_functional
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldAllowBanLegacyPeers$" -tags test_functional
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldAllowBanTeranodePeers$" -tags test_functional

package smoke

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/tconfig"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const testHeight = uint32(0)
const legacySyncURL = "http://localhost:18332"

type SanitywithLegacyTestSuite struct {
	helper.TeranodeTestSuite
}

func TestSanitywithLegacyTestSuite(t *testing.T) {
	suite.Run(t, &SanitywithLegacyTestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyInitBlockHeight: testHeight,
					tconfig.KeyLocalSystemComposes: []string{
						"../../docker-compose.e2etest.legacy.yml",
					},
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.legacy",
						"docker.teranode2.test.legacy",
						"docker.teranode3.test.legacy",
					},
					tconfig.KeyIsLegacyTest: true,
				},
			),
		},
	},
	)
}

func (suite *SanitywithLegacyTestSuite) TestShouldSyncWithLegacyWhenInitialBlocksAreGeneratedOnLegacy() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	err = helper.WaitForBlockHeight(url, testHeight+101, 60*time.Second)
	if err != nil {
		suite.T().Fatal(err)
	}

	height, _ = helper.GetBlockHeight(url)
	t.Logf("Block height after mining: %d\n", height)
	assert.Equal(t, testHeight+101, height, "Height should match with initial generated blocks")
}

func (suite *SanitywithLegacyTestSuite) TestShouldSyncWithLegacy() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	cluster := suite.TeranodeTestEnv
	if err := cluster.StopNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	time.Sleep(20 * time.Second)

	if err := cluster.StartNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	ports := []int{8000}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(mappedPort.Int(), 60*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	err = helper.WaitForBlockHeight(url, testHeight+101, 60*time.Second)
	if err != nil {
		suite.T().Fatal(err)
	}

	height, _ = helper.GetBlockHeight(url)
	t.Logf("Block height after mining: %d\n", height)
	assert.Equal(t, testHeight+101, height, "Height should match with initial generated blocks")
}

func (suite *SanitywithLegacyTestSuite) TestShouldAllowBanLegacyPeers() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	time.Sleep(20 * time.Second)

	// Get IP addresses of legacy nodes
	svnode1 := suite.TeranodeTestEnv.LegacyNodes[0]

	t.Logf("Legacy node: %s\n", svnode1.Name)

	node1 := suite.TeranodeTestEnv.Nodes[0]

	var p2pResp helper.P2PRPCResponse

	teranode1RPCEndpoint := "http://" + node1.RPCURL

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getpeerinfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &p2pResp)
	if err != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if len(p2pResp.Result) == 0 {
		t.Errorf("Test failed: peers list is empty")
	} else {
		t.Logf("Test succeeded, retrieved P2P peers informations")
	}

	// Find and ban peers with port 18333
	var peersTo18333 []string
	for _, peer := range p2pResp.Result {
		// Handle both address formats - "ip:port" and "/ip4/ip/tcp/port"
		if strings.Contains(peer.Addr, ":18333") {
			peersTo18333 = append(peersTo18333, peer.Addr)
		}
	}

	t.Logf("Found %d peers with port 18333: %v", len(peersTo18333), peersTo18333)

	// Ban each peer with port 18333
	for _, peerAddr := range peersTo18333 {
		t.Logf("Banning peer: %s", peerAddr)
		_, err = helper.CallRPC(teranode1RPCEndpoint, "setban", []interface{}{peerAddr, "add", 1800, false})
		require.NoError(t, err)
	}

	_, err = helper.CallRPC(legacySyncURL, "generate", []interface{}{1})
	assert.NoError(t, err, "Failed to generate blocks")

	cluster := suite.TeranodeTestEnv
	if err := cluster.StopNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	if err := cluster.StartNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err = cluster.Nodes[0].BlockchainClient.Run(cluster.Context, "test")
	require.NoError(t, err)

	ports := []int{8000}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(mappedPort.Int(), 60*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	var (
		bestBlockOnSVNode1   helper.BestBlockHashResp
		bestBlockOnTeranode1 helper.BestBlockHashResp
	)

	// get best block hash from svnode1
	resp, err = helper.CallRPC(legacySyncURL, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON = json.Unmarshal([]byte(resp), &bestBlockOnSVNode1)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Best block hash on svnode1: %s", bestBlockOnSVNode1.Result)

	// get best block hash from teranode1
	resp, err = helper.CallRPC("http://"+node1.RPCURL, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON = json.Unmarshal([]byte(resp), &bestBlockOnTeranode1)

	require.NoError(t, errJSON, "JSON decoding error")

	t.Logf("Best block hash on teranode1: %s", bestBlockOnTeranode1.Result)

	assert.NotEqual(t, bestBlockOnSVNode1.Result, bestBlockOnTeranode1.Result, "Best block hash should not match")

	//remove ban
	// UnBan each peer with port 18333
	for _, peerAddr := range peersTo18333 {
		t.Logf("Banning peer: %s", peerAddr)
		_, err = helper.CallRPC(teranode1RPCEndpoint, "setban", []interface{}{peerAddr, "add", 1, false})
		require.NoError(t, err)
	}

	// Sleep for 2 seconds
	time.Sleep(2 * time.Second)

	// get best block hash from teranode1
	resp, err = helper.CallRPC("http://"+node1.RPCURL, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON = json.Unmarshal([]byte(resp), &bestBlockOnTeranode1)

	require.NoError(t, errJSON, "JSON decoding error")

	t.Logf("Best block hash on teranode1: %s", bestBlockOnTeranode1.Result)

	assert.NotEqual(t, bestBlockOnSVNode1.Result, bestBlockOnTeranode1.Result, "Best block hash should not match")
}

func (suite *SanitywithLegacyTestSuite) TestShouldAllowBanTeranodePeers() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	cluster := suite.TeranodeTestEnv
	if err := cluster.StopNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	time.Sleep(20 * time.Second)

	if err := cluster.StartNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	ports := []int{8000}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(mappedPort.Int(), 60*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	err = helper.WaitForBlockHeight(url, testHeight+101, 60*time.Second)
	if err != nil {
		suite.T().Fatal(err)
	}

	height, _ = helper.GetBlockHeight(url)
	t.Logf("Block height after mining: %d\n", height)
	assert.Equal(t, testHeight+101, height, "Height should match with initial generated blocks")

	// Get IP addresses of legacy nodes
	svnode1 := suite.TeranodeTestEnv.LegacyNodes[0]
	t.Logf("Legacy node: %s\n", svnode1.Name)

	svnode1Address := suite.TeranodeTestEnv.GetLegacyContainerIPAddress(&svnode1)

	t.Logf("Legacy node IP address: %s\n", svnode1Address)

	node1 := suite.TeranodeTestEnv.Nodes[0]
	node2 := suite.TeranodeTestEnv.Nodes[1]
	node3 := suite.TeranodeTestEnv.Nodes[2]

	// All Teranodes should ban another Teranode node
	_, err = helper.CallRPC("http://"+node1.RPCURL, "setban", []interface{}{node2.IPAddress, "add", 180, false})
	require.NoError(t, err)

	_, err = helper.CallRPC("http://"+node3.RPCURL, "setban", []interface{}{node2.IPAddress, "add", 180, false})
	require.NoError(t, err)

	_, err = helper.CallRPC("http://"+node1.RPCURL, "generate", []interface{}{1})
	assert.NoError(t, err, "Failed to generate blocks")

	var (
		bestBlockOnTeranode2 helper.BestBlockHashResp
		bestBlockOnTeranode1 helper.BestBlockHashResp
	)

	// get best block hash from node2
	resp, err := helper.CallRPC("http://"+node2.RPCURL, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON := json.Unmarshal([]byte(resp), &bestBlockOnTeranode2)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Best block hash: %s", bestBlockOnTeranode2.Result)

	// get best block hash from teranode1
	resp, err = helper.CallRPC("http://"+node1.RPCURL, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON = json.Unmarshal([]byte(resp), &bestBlockOnTeranode1)

	require.NoError(t, errJSON, "JSON decoding error")

	t.Logf("Best block hash: %s", bestBlockOnTeranode1.Result)

	assert.NotEqual(t, bestBlockOnTeranode2.Result, bestBlockOnTeranode1.Result, "Best block hash should not match")
}
