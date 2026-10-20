package blockvalidation

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	gap054EnvVar   = "GAP_BV054_CHILD"
	gap054OKMarker = "GAP_BV054_CHILD_SCENARIO_OK"
)

// gap054PanicChain wraps a real blockchain client and panics on the first call to the chosen
// method (after signalling on `panicking`), then delegates normally.
type gap054PanicChain struct {
	blockchain.ClientI
	onHeader   bool // true: panic in GetBlockHeader; false: panic in GetBlockExists
	calls      atomic.Int32
	panicking  chan struct{}
	panicValue string
}

func (c *gap054PanicChain) maybePanic(isHeaderCall bool) {
	if isHeaderCall != c.onHeader {
		return
	}

	if c.calls.Add(1) == 1 {
		close(c.panicking)
		panic(c.panicValue)
	}
}

func (c *gap054PanicChain) GetBlockHeader(ctx context.Context, h *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.maybePanic(true)
	return c.ClientI.GetBlockHeader(ctx, h)
}

func (c *gap054PanicChain) GetBlockExists(ctx context.Context, h *chainhash.Hash) (bool, error) {
	c.maybePanic(false)
	return c.ClientI.GetBlockExists(ctx, h)
}

// gap054PeerReportCounter wraps a blockchain client and counts ReportPeerFailure calls.
type gap054PeerReportCounter struct {
	blockchain.ClientI
	reports atomic.Int32
}

func (c *gap054PeerReportCounter) ReportPeerFailure(context.Context, *chainhash.Hash, string, string, string) error {
	c.reports.Add(1)
	return nil
}

// gap054RunChild re-executes this test binary running only childTest with the env flag set, and
// fails the (parent) test if the child process died or did not reach the success marker. A panic
// in a goroutine kills the whole test binary, so this is what turns "process crashed" into a
// clean, reportable failure.
func gap054RunChild(t *testing.T, childTest, specID string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run", "^"+childTest+"$", "-test.count=1", "-test.v", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), gap054EnvVar+"=1")

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	output := out.String()

	if err != nil || !strings.Contains(output, gap054OKMarker) {
		tail := output
		if idx := strings.Index(output, "panic:"); idx >= 0 {
			tail = output[idx:]
		}

		if len(tail) > 1500 {
			tail = tail[:1500]
		}

		t.Fatalf("%s: the service process did not survive an injected panic (child exit: %v, scenario completed: %v)\n--- child output ---\n%s",
			specID, err, strings.Contains(output, gap054OKMarker), tail)
	}
}

func gap054ChildOnly(t *testing.T) {
	t.Helper()

	if os.Getenv(gap054EnvVar) != "1" {
		t.Skip("subprocess scenario; driven by its parent test")
	}
}

// TestGapBV054_PriorityWorkerSurvivesPanic proves GAP-BV-054 for the priority-queue worker.
//
// Spec: BV-STARTUP-007: a panic while processing an announcement MUST be recovered and the
// service MUST continue processing other announcements (AC-BV-STARTUP-007.1).
//
// A panic is injected into the validation stage run by blockProcessingWorker (via
// processBlockWithPriority -> processBlockFound -> blockchainClient.GetBlockHeader). The
// scenario runs in a subprocess; the parent fails if the child process dies, which is what an
// unrecovered panic in the worker does (bitcoin-sv/teranode#4985).
func TestGapBV054_PriorityWorkerSurvivesPanic(t *testing.T) {
	if os.Getenv(gap054EnvVar) == "1" {
		t.Skip("parent driver")
	}

	gap054RunChild(t, "TestGapBV054_PriorityWorkerSurvivesPanic_Child", "BV-STARTUP-007 (blockProcessingWorker)")
}

func TestGapBV054_PriorityWorkerSurvivesPanic_Child(t *testing.T) {
	gap054ChildOnly(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: true, panicking: make(chan struct{}), panicValue: "injected panic in priority worker"}
	s.blockchainClient = chain // only processBlockFound (worker stage) reads this field

	require.NoError(t, s.Init(ctx))

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(),
		httpmock.NewBytesResponder(200, blockBytes))

	announce := func() {
		_, callErr := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:    block.Hash()[:],
			BaseUrl: "http://test-peer",
			PeerId:  "peerA",
		})
		require.NoError(t, callErr)
	}

	announce()

	select {
	case <-chain.panicking:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: the worker never reached the injected panic")
	}

	// Give a recovering implementation time to unwind, then announce again: the service must
	// still be alive and process it.
	time.Sleep(300 * time.Millisecond)
	announce()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		exists, existsErr := s.blockValidation.GetBlockExists(ctx, block.Hash())
		require.NoError(t, existsErr)

		if exists {
			println(gap054OKMarker)
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("BV-STARTUP-007: the block announced after the panic was never processed")
}

// TestGapBV054_CatchupConsumerSurvivesPanic proves GAP-BV-054 for the catchup consumer.
//
// Spec: BV-STARTUP-007 (AC-BV-STARTUP-007.1). A panic is injected into catchupFunc, which the
// Init catchup consumer goroutine runs via processCatchupChItem. A second catchup item must
// still be consumed afterwards, and the panicking block's in-progress marker must be cleared so
// a later announcement can retry it. Subprocess-isolated (bitcoin-sv/teranode#4985).
func TestGapBV054_CatchupConsumerSurvivesPanic(t *testing.T) {
	if os.Getenv(gap054EnvVar) == "1" {
		t.Skip("parent driver")
	}

	gap054RunChild(t, "TestGapBV054_CatchupConsumerSurvivesPanic_Child", "BV-STARTUP-007 (processCatchupChItem)")
}

func TestGapBV054_CatchupConsumerSurvivesPanic_Child(t *testing.T) {
	gap054ChildOnly(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	var calls atomic.Int32

	panicking := make(chan struct{})
	secondCall := make(chan struct{})

	s.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		switch calls.Add(1) {
		case 1:
			close(panicking)
			panic("injected panic in catchup")
		case 2:
			close(secondCall)
		}

		return nil
	}

	require.NoError(t, s.Init(ctx))

	item := processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: ""}

	// queueFetchedBlock sets this marker before sending to catchupCh; the recover must clear it.
	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "altpeer"}}, ttlcache.DefaultTTL)

	s.catchupCh <- item

	select {
	case <-panicking:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: the catchup consumer never ran catchupFunc")
	}

	time.Sleep(300 * time.Millisecond)

	require.Nil(t, s.processBlockNotify.Get(*block.Hash()), "the in-progress marker must be cleared after the panic")
	require.Nil(t, s.catchupAlternatives.Get(*block.Hash()), "the cached alternatives must be cleared after the panic")

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts, "the panicked cycle must count toward the per-block catchup cap")
	require.Equal(t, 1, attempts.Value())

	s.catchupCh <- item

	select {
	case <-secondCall:
		println(gap054OKMarker)
	case <-time.After(10 * time.Second):
		t.Fatal("BV-STARTUP-007: the catchup consumer did not process the item queued after the panic")
	}
}

// TestGapBV054_KafkaHandlerSurvivesPanic proves GAP-BV-054 for the goroutine started inside
// consumerMessageHandler, which runs blockHandler. A panic is injected into the existence check
// blockHandler performs; the handler must return nil so the message is committed rather than
// redelivered into the same panic, must not report the peer, and a following message must still
// be handled.
// Subprocess-isolated (bitcoin-sv/teranode#4985).
func TestGapBV054_KafkaHandlerSurvivesPanic(t *testing.T) {
	if os.Getenv(gap054EnvVar) == "1" {
		t.Skip("parent driver")
	}

	gap054RunChild(t, "TestGapBV054_KafkaHandlerSurvivesPanic_Child", "BV-STARTUP-007 (consumerMessageHandler goroutine)")
}

func TestGapBV054_KafkaHandlerSurvivesPanic_Child(t *testing.T) {
	gap054ChildOnly(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in kafka handler"}
	// blockHandler's GetBlockExists goes through s.blockValidation's blockchain client.
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{
		Hash:   block.Hash().String(),
		URL:    "http://test-peer",
		PeerId: "peerA",
	})
	require.NoError(t, err)

	reporter := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reporter

	handler := s.consumerMessageHandler(ctx)

	call := func() chan error {
		ch := make(chan error, 1)
		go func() { ch <- handler(&kafka.KafkaMessage{Value: msgBytes}) }()

		return ch
	}

	select {
	case herr := <-call():
		require.NoError(t, herr, "a panicking message must be committed, not redelivered")
		require.Zero(t, reporter.reports.Load(), "a panic in this node must not be reported as a peer failure")
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not return for the panicking message")
	}

	select {
	case herr := <-call():
		require.NoError(t, herr)
		require.Equal(t, 1, len(s.blockFoundCh), "the message after the panic must be handled and queued")
		println(gap054OKMarker)
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not return for the message after the panic")
	}
}

// gap054ReportPanicChain wraps a blockchain client and panics in ReportPeerFailure.
type gap054ReportPanicChain struct {
	blockchain.ClientI
}

func (c *gap054ReportPanicChain) ReportPeerFailure(context.Context, *chainhash.Hash, string, string, string) error {
	panic("injected panic in ReportPeerFailure")
}

// TestGapBV054_CatchupPanicInRealCatchupReleasesLock runs the real catchup() (not an injected
// catchupFunc) and panics in it after acquireCatchupLock, in the block-existence check that
// catchupGetBlockHeaders makes on the consumer goroutine. The recover in processCatchupChItem
// must see catchup's deferred releaseCatchupLock run first: the catchup flag is clear, the
// active catchup context is gone, the attempt is counted one time and the markers are cleared.
func TestGapBV054_CatchupPanicInRealCatchupReleasesLock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in catchup"}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "altpeer"}}, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: ""})

	select {
	case <-chain.panicking:
	default:
		t.Fatal("setup: catchup never reached the injected panic")
	}

	require.False(t, s.isCatchingUp.Load(), "catchup must release its lock during the unwind")

	s.activeCatchupCtxMu.RLock()
	active := s.activeCatchupCtx
	s.activeCatchupCtxMu.RUnlock()
	require.Nil(t, active, "the active catchup context must be cleared during the unwind")

	require.Nil(t, s.processBlockNotify.Get(*block.Hash()))
	require.Nil(t, s.catchupAlternatives.Get(*block.Hash()))

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts)
	require.Equal(t, 1, attempts.Value())
}

// TestGapBV054_CatchupPanicAfterCountDoesNotCountTwice makes the cycle fail with ErrExternal, which
// counts the attempt, and then panics in the ReportPeerFailure call that follows the count. The
// recover must not count the same cycle a second time.
func TestGapBV054_CatchupPanicAfterCountDoesNotCountTwice(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	s.blockchainClient = &gap054ReportPanicChain{ClientI: s.blockchainClient}
	s.catchupFunc = func(context.Context, *model.Block, string, string) error {
		return errors.NewExternalError("all peers failed to serve subtree data")
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts)
	require.Equal(t, 1, attempts.Value(), "a panic after the cycle was counted must not count it again")
	require.Nil(t, s.processBlockNotify.Get(*block.Hash()))
}

// TestGapBV054_WorkerPanicErrorIsNotAFetchFailure panics with a value that holds words the
// substring classifiers look for. The worker reads the recovered error with IsNetworkError and
// IsMaliciousResponseError, and a match requeues the block as a fetch failure. The recovered
// error must match neither.
func TestGapBV054_WorkerPanicErrorIsNotAFetchFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "http connection timeout: corrupt malformed response"}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	err := s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: "peerA"})

	select {
	case <-chain.panicking:
	default:
		t.Fatal("setup: processing never reached the injected panic")
	}

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "a recovered panic must be ERR_PROCESSING, got %v", err)
	require.False(t, errors.IsBlockCorrupt(err))
	require.False(t, errors.IsNetworkError(err), "a recovered panic must not read as a network error: %v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "a recovered panic must not read as a malicious response: %v", err)
}
