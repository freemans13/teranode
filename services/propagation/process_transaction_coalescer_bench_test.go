//go:build aerospike

package propagation

// BenchmarkProcessTransaction_FlagOffVsOn_RealAerospike compares the
// throughput of ProcessTransaction with the coalescer disabled
// (today's direct-validator path) vs enabled (coalescer batching) when
// driven by N concurrent submitters. Uses real Aerospike via
// testcontainers, mirroring the v1 bench at
// services/validator/validate_batch_aerospike_bench_test.go.
//
// Run:
//
//	go test -tags aerospike \
//	  -bench=BenchmarkProcessTransaction_FlagOffVsOn_RealAerospike \
//	  -benchmem -benchtime=10x -run=NONE -timeout=20m \
//	  ./services/propagation -v

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"

	utxotesthelper "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
)

func BenchmarkProcessTransaction_FlagOffVsOn_RealAerospike(b *testing.B) {
	for _, concurrency := range []int{32, 128, 512} {
		concurrency := concurrency
		for _, useBatch := range []bool{false, true} {
			useBatch := useBatch
			name := "direct"
			if useBatch {
				name = "coalescer"
			}
			b.Run(fmt.Sprintf("concurrency=%d/%s", concurrency, name), func(b *testing.B) {
				benchProcessTxOnAerospike(b, concurrency, useBatch)
			})
		}
	}
}

func benchProcessTxOnAerospike(b *testing.B, concurrency int, useBatch bool) {
	b.Helper()
	ctx := context.Background()
	ps, aeroStore, cleanup := newPropagationBackedByAerospike(b, useBatch)
	defer cleanup()

	// Pre-generate b.N rounds of `concurrency` fresh txs each so the
	// timed loop excludes generation overhead.
	rounds := make([][]*bt.Tx, b.N)
	for i := 0; i < b.N; i++ {
		parents := seedRandomParentsForCoalescerBench(b, ctx, aeroStore, concurrency)
		rounds[i] = buildChildrenForCoalescerBench(b, parents)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for _, tx := range rounds[i] {
			tx := tx
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{Tx: tx.Bytes()})
			}()
		}
		wg.Wait()
	}
}

// newPropagationBackedByAerospike spins up Aerospike via testcontainers,
// builds an Aerospike UTXO store, a real Validator wired to it, and a
// PropagationServer with Kafka producer set to nil and the flag set.
// Returns the server, the underlying aerospike store, plus a cleanup
// function. Mirrors validate_batch_aerospike_bench_test.go's
// newValidatorBackedByAerospike.
func newPropagationBackedByAerospike(b testing.TB, useBatch bool) (*PropagationServer, *aerostore.Store, func()) {
	b.Helper()
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.Validator.UseBatchValidation = useBatch

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
	if err != nil {
		b.Skipf("Aerospike testcontainer unavailable: %v", err)
	}

	host, err := container.Host(ctx)
	require.NoError(b, err)
	port, err := container.ServicePort(ctx)
	require.NoError(b, err)

	aerospikeContainerURL := fmt.Sprintf(
		"aerospike://%s:%d/test?set=test&block_retention=1&externalStore=file:///tmp/bench-coalescer-aero-external",
		host, port,
	)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(b, err)

	aeroStore, err := aerostore.New(ctx, logger, tSettings, aeroURL)
	require.NoError(b, err)
	aeroStore.SetExternalStore(memory.New())

	vIface, err := validator.New(ctx, logger, tSettings, aeroStore, nil, nil, nil, nil)
	require.NoError(b, err)

	// NO test-only overrides — both paths (flag-off and flag-on) must run
	// exactly the same validation logic on exactly the same input. If a
	// tx is invalid, both paths must reject it for the same reason. If
	// they reject for DIFFERENT reasons, that's a correctness bug in
	// ValidateBatch as a drop-in for per-tx Validate, not a perf concern.

	// Build a PropagationServer wired to the aerospike-backed validator,
	// with Kafka producer explicitly nil (enables coalescer path when useBatch=true).
	txStore := memory.New()
	ps := New(logger, tSettings, txStore, vIface, nil, nil, nil)

	// Manually wire the coalescer — bypassing Start() which blocks on gRPC bind.
	if useBatch {
		ps.coalescer = NewTxCoalescer(
			ctx, logger, vIface,
			tSettings.Validator.BatchMaxSize,
			tSettings.Validator.BatchMaxWait,
			tSettings.Validator.BatchMaxConcurrent,
		)
	}

	cleanup := func() {
		if ps.coalescer != nil {
			_ = ps.coalescer.Close(context.Background())
		}
		if termErr := container.Terminate(ctx); termErr != nil {
			b.Logf("warning: failed to terminate Aerospike container: %v", termErr)
		}
	}

	return ps, aeroStore, cleanup
}

// seedRandomParentsForCoalescerBench creates n parent txs in the
// Aerospike store via Create. Returns the parent txs so children can
// reference them. Mirrors validate_batch_aerospike_bench_test.go's
// seedRandomParentsForBench.
func seedRandomParentsForCoalescerBench(b testing.TB, ctx context.Context, s *aerostore.Store, n int) []*bt.Tx {
	b.Helper()
	parents := make([]*bt.Tx, n)
	for i := 0; i < n; i++ {
		tx, err := utxotesthelper.CreateTransaction(1)
		require.NoError(b, err)
		_, err = s.Create(ctx, tx, 0)
		require.NoError(b, err)
		parents[i] = tx
	}
	return parents
}

// buildChildrenForCoalescerBench builds one child tx per parent, each
// spending output 0 of its parent. The inputs are extended so
// utxo.GetSpends works in Phase C. Mirrors v1 bench's
// buildChildrenSpendingParentsForBench.
func buildChildrenForCoalescerBench(b testing.TB, parents []*bt.Tx) []*bt.Tx {
	b.Helper()
	children := make([]*bt.Tx, len(parents))
	for i, parent := range parents {
		ph := parent.TxIDChainHash()
		child := bt.NewTx()

		var lockScript *bscript.Script
		if parent.Outputs[0].LockingScript != nil {
			lockScript = parent.Outputs[0].LockingScript
		} else {
			lockScript = &bscript.Script{}
		}

		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   lockScript,
			PreviousTxSatoshis: parent.Outputs[0].Satoshis,
		}
		require.NoError(b, input.PreviousTxIDAdd(ph))
		child.Inputs = append(child.Inputs, input)

		require.NoError(b, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1))
		child.SetExtended(true)

		children[i] = child
	}
	return children
}
