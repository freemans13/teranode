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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	gap054EnvVar   = "GAP_BV054_CHILD"
	gap054OKMarker = "GAP_BV054_CHILD_SCENARIO_OK"
)

// gap054PanicChain wraps a real blockchain client and panics on the first call to the chosen
// method (after signalling on `panicking`), then delegates normally. With always set, it panics
// on each call to that method.
type gap054PanicChain struct {
	blockchain.ClientI
	onHeader   bool // true: panic in GetBlockHeader; false: panic in GetBlockExists
	always     bool
	calls      atomic.Int32
	panicking  chan struct{}
	panicValue string
}

func (c *gap054PanicChain) maybePanic(isHeaderCall bool) {
	if isHeaderCall != c.onHeader {
		return
	}

	n := c.calls.Add(1)
	if n == 1 {
		close(c.panicking)
	}

	if n == 1 || c.always {
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

	initPrometheusMetrics()
	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathKafka))

	call := func() chan error {
		ch := make(chan error, 1)
		go func() { ch <- handler(&kafka.KafkaMessage{Value: msgBytes}) }()

		return ch
	}

	select {
	case herr := <-call():
		require.NoError(t, herr, "a panicking message must be committed, not redelivered")
		require.Zero(t, reporter.reports.Load(), "a panic in this node must not be reported as a peer failure")
		require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathKafka)), "the recover must count the panic")
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
// releaseCatchupLock must also record the cycle as a local failure, not as a success: the health
// check reads lastCatchupResult false, catchupSuccesses does not change, and the dashboard
// attempt has the error type local_panic, which no peer is charged for.
func TestGapBV054_CatchupPanicInRealCatchupReleasesLock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in catchup"}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "altpeer"}}, ttlcache.DefaultTTL)

	// A success from an earlier cycle, so the test sees the value change to false.
	s.catchupStatsMu.Lock()
	s.lastCatchupResult = true
	s.catchupStatsMu.Unlock()

	successesBefore := s.catchupSuccesses.Load()
	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathCatchup))

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: ""})

	select {
	case <-chain.panicking:
	default:
		t.Fatal("setup: catchup never reached the injected panic")
	}

	require.False(t, s.isCatchingUp.Load(), "catchup must release its lock during the unwind")

	s.activeCatchupCtxMu.RLock()
	active := s.activeCatchupCtx
	previous := s.previousCatchupAttempt
	s.activeCatchupCtxMu.RUnlock()
	require.Nil(t, active, "the active catchup context must be cleared during the unwind")

	require.Nil(t, s.processBlockNotify.Get(*block.Hash()))
	require.Nil(t, s.catchupAlternatives.Get(*block.Hash()))

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts)
	require.Equal(t, 1, attempts.Value())

	s.catchupStatsMu.RLock()
	lastResult := s.lastCatchupResult
	s.catchupStatsMu.RUnlock()
	require.False(t, lastResult, "a panicked catchup must not read as a success in the health check")
	require.Equal(t, successesBefore, s.catchupSuccesses.Load(), "a panicked catchup must not count as a success")

	require.NotNil(t, previous, "a panicked catchup must be recorded as a failed attempt")
	require.Equal(t, "local_panic", previous.ErrorType, "a panic is local and must not be scored against the peer")

	require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathCatchup)))
}

// TestGapBV054_CatchupPanicBeforeLockCountsAttempt panics before any catchup takes the lock, while
// blocksValidated still holds a positive count from an earlier cycle. The recover must count the
// attempt, not read the stale count as progress and clear the counter.
func TestGapBV054_CatchupPanicBeforeLockCountsAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	s.blocksValidated.Store(5)
	s.blockCatchupAttempts.Set(*block.Hash(), 1, ttlcache.DefaultTTL)

	s.catchupFunc = func(context.Context, *model.Block, string, string) error {
		panic("injected panic before the catchup lock")
	}

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts, "a panic before the lock must not clear the attempt counter")
	require.Equal(t, 2, attempts.Value(), "a panic before the lock must count toward the per-block catchup cap")
}

// gap054RunOnce runs runOncePerBlock in a goroutine and sends its error, or the recovered panic
// value, on the returned channels.
func gap054RunOnce(bv *BlockValidation, hash *chainhash.Hash, validate func(*ValidateBlockOptions) error) (chan error, chan any) {
	errCh := make(chan error, 1)
	panicCh := make(chan any, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()

		errCh <- bv.runOncePerBlock(hash, &ValidateBlockOptions{}, validate)
	}()

	return errCh, panicCh
}

// TestGapBV054_PanicInValidateReleasesWaiters panics inside the validate function of
// runOncePerBlock while a second caller waits for the same hash. The panic must continue to the
// first caller, the waiter must get an ERR_PROCESSING error, the entry must go after the grace
// window, and a later validation of the hash must run again.
func TestGapBV054_PanicInValidateReleasesWaiters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, s.blockchainClient, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)
	hash := block.Hash()

	started := make(chan struct{})
	release := make(chan struct{})

	_, firstPanic := gap054RunOnce(bv, hash, func(*ValidateBlockOptions) error {
		close(started)
		<-release
		panic("injected panic in validate")
	})

	<-started

	waiterErr, waiterPanic := gap054RunOnce(bv, hash, func(*ValidateBlockOptions) error {
		return errors.NewProcessingError("the waiter must not run its own validation")
	})

	// The entry stays until the grace window after the release, so the waiter blocks on it.
	time.Sleep(200 * time.Millisecond)
	close(release)

	select {
	case r := <-firstPanic:
		require.Equal(t, "injected panic in validate", r, "the panic must continue to the first caller")
	case <-time.After(5 * time.Second):
		t.Fatal("the first caller did not panic")
	}

	select {
	case err := <-waiterErr:
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing), "the waiter must get ERR_PROCESSING, got %v", err)
		require.NotContains(t, err.Error(), "must not run", "the waiter must get the stored result, not run validate")
	case r := <-waiterPanic:
		t.Fatalf("the waiter panicked: %v", r)
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter for the same hash is wedged after the panic")
	}

	require.Eventually(t, func() bool {
		_, ok := bv.blocksCurrentlyValidating.Get(*hash)
		return !ok
	}, 5*time.Second, 10*time.Millisecond, "the entry must go after the grace window")

	var ran atomic.Bool

	laterErr, _ := gap054RunOnce(bv, hash, func(*ValidateBlockOptions) error {
		ran.Store(true)
		return nil
	})

	select {
	case err := <-laterErr:
		require.NoError(t, err)
		require.True(t, ran.Load(), "a later validation of the hash must run again")
	case <-time.After(5 * time.Second):
		t.Fatal("a later validation of the hash is wedged after the panic")
	}
}

// TestGapBV054_PanicInValidateBlockDoesNotWedgeHash runs the real ValidateBlockWithOptions and
// panics in its first blockchain lookup, which is inside the runOncePerBlock closure. After the
// panic, a new validation of the same hash must complete and must reach the blockchain again.
func TestGapBV054_PanicInValidateBlockDoesNotWedgeHash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in ValidateBlock"}
	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	require.PanicsWithValue(t, "injected panic in ValidateBlock", func() {
		_ = bv.ValidateBlockWithOptions(ctx, block, "http://test-peer", nil)
	})

	require.Eventually(t, func() bool {
		_, ok := bv.blocksCurrentlyValidating.Get(*block.Hash())
		return !ok
	}, 5*time.Second, 10*time.Millisecond, "the entry must go after the grace window")

	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = bv.ValidateBlockWithOptions(ctx, block, "http://test-peer", nil)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a validation of the hash after the panic is wedged")
	}

	require.GreaterOrEqual(t, chain.calls.Load(), int32(2), "the second validation must run, not get a stored result")
}

// TestGapBV054_WorkerPanicCapSuppressesRedelivery panics on each delivery of one block from one
// peer, at the parent-header lookup that follows the download. After MaxCorruptAttemptsPerBlock
// panics, processBlockFound must stop that peer's deliveries before the download, with an error
// that does not requeue the block. A different peer must keep its own budget.
func TestGapBV054_WorkerPanicCapSuppressesRedelivery(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: true, always: true, panicking: make(chan struct{}), panicValue: "injected panic on each delivery"}
	s.blockchainClient = chain

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(),
		httpmock.NewBytesResponder(200, blockBytes))

	deliver := func(peerID string) error {
		return s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: peerID})
	}

	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker))

	for i := 1; i <= 2; i++ {
		err := deliver("peerA")
		require.Error(t, err)
		require.Contains(t, err.Error(), "see the PANIC log line", "delivery %d must panic", i)
		require.Equal(t, int32(i), chain.calls.Load())
	}

	require.Equal(t, panicsBefore+2, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker)))

	downloadsBefore := httpmock.GetTotalCallCount()

	err = deliver("peerA")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "a capped delivery must be ERR_PROCESSING, got %v", err)
	require.Contains(t, err.Error(), "recovered-panic cap reached")
	require.False(t, errors.IsBlockCorrupt(err))
	require.False(t, errors.IsNetworkError(err), "a capped delivery must not requeue as a fetch failure: %v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "a capped delivery must not requeue as a fetch failure: %v", err)
	require.Equal(t, int32(2), chain.calls.Load(), "a capped delivery must stop before the stage that panics")
	require.Equal(t, downloadsBefore, httpmock.GetTotalCallCount(), "a capped delivery must stop before the download")
	require.Equal(t, panicsBefore+2, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker)))

	err = deliver("peerB")
	require.Error(t, err)
	require.Contains(t, err.Error(), "see the PANIC log line", "a different peer must keep its own budget")
	require.Equal(t, int32(3), chain.calls.Load())
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

	initPrometheusMetrics()
	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker))

	err := s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker)), "the recover must count the panic")

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
