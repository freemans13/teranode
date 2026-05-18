//go:build aerospike

package validator

// BenchmarkValidator_FlagOffVsOn_RealAerospike mirrors the propagation-level
// bench but exercises the validator directly: N concurrent submitters per
// round, calling Validate (per-tx path) or ValidateBatch (batched path),
// across the 4-variant matrix (baseline / merged-only / coalescer-only / both)
// at concurrency tiers {32,128,512,1024}.
//
// "Coalescer-only" at the validator level means "all N submitters dispatch
// their tx via a single ValidateBatch call per round" — the propagation
// coalescer's effect, modelled at the validator API. "Direct" means each
// submitter calls Validate(tx) concurrently.
//
// Run:
//
//	go test -tags aerospike \
//	  -bench=BenchmarkValidator_FlagOffVsOn_RealAerospike \
//	  -benchtime=10x -timeout 30m -run=NONE -count=1 ./services/validator -v

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

type validatorBenchVariant struct {
	name                string
	mergedOpsMode       string // "off" or "single"
	mergedMaxConcurrent int    // MergedOpsBatcherMaxConcurrent; 0 = unbounded (fresh goroutine per batch)
	useBatch            bool   // false = per-tx Validate; true = ValidateBatch
}

var validatorBenchVariants = []validatorBenchVariant{
	{"baseline", "off", 1024, false},
	{"merged_only", "single", 1024, false},
	{"merged_only_unbounded", "single", 0, false},
	{"coalescer_only", "off", 1024, true},
	{"both", "single", 1024, true},
	{"both_unbounded", "single", 0, true},
}

func BenchmarkValidator_FlagOffVsOn_RealAerospike(b *testing.B) {
	for _, concurrency := range []int{32, 128, 512, 1024} {
		concurrency := concurrency
		for _, v := range validatorBenchVariants {
			v := v
			b.Run(fmt.Sprintf("concurrency=%d/variant=%s", concurrency, v.name), func(b *testing.B) {
				benchValidatorFlagMatrix(b, concurrency, v)
			})
		}
	}
}

func benchValidatorFlagMatrix(b *testing.B, concurrency int, v validatorBenchVariant) {
	b.Helper()
	ctx := context.Background()

	val, aeroStore, cleanup := newValidatorBackedByAerospikeForFlagMatrix(b, v)
	defer cleanup()

	// Override CPU / BA / Kafka publish to isolate UTXO-store hot path.
	val.overrideCPUValidationForTest(func(_ *bt.Tx) error { return nil })
	val.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{}
	})
	val.overrideTxMetaPublishForTest(func(_ *bt.Tx, _ *meta.Data) {})

	// Pre-generate b.N rounds of `concurrency` fresh txs each.
	rounds := make([][]*bt.Tx, b.N)
	for i := 0; i < b.N; i++ {
		parents := seedRandomParentsForBench(b, ctx, aeroStore, concurrency)
		rounds[i] = buildChildrenSpendingParentsForBench(b, parents)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v.useBatch {
			// Single ValidateBatch call per round — models the coalescer's
			// effect at the validator API.
			_, err := val.ValidateBatch(ctx, rounds[i], 100)
			if err != nil {
				b.Fatal(err)
			}
		} else {
			// N concurrent Validate calls per round.
			var wg sync.WaitGroup
			for _, tx := range rounds[i] {
				tx := tx
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _ = val.Validate(ctx, tx, 100)
				}()
			}
			wg.Wait()
		}
	}
}

// newValidatorBackedByAerospikeForFlagMatrix is a sibling of
// newValidatorBackedByAerospike that takes a variant and applies the same
// prod-aligned per-op batcher settings as the propagation bench.
func newValidatorBackedByAerospikeForFlagMatrix(b testing.TB, v validatorBenchVariant) (*Validator, *aerostore.Store, func()) {
	b.Helper()
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.Validator.UseBatchValidation = v.useBatch

	// PR #887 merged-ops batcher toggle.
	tSettings.UtxoStore.MergedOpsBatcherMode = v.mergedOpsMode
	tSettings.UtxoStore.MergedOpsBatcherSize = 512
	tSettings.UtxoStore.MergedOpsBatcherDurationMillis = 1
	tSettings.UtxoStore.MergedOpsBatcherDrainMode = true
	tSettings.UtxoStore.MergedOpsBatcherMaxConcurrent = v.mergedMaxConcurrent

	// Prod-aligned per-op batcher settings — equal across all variants.
	tSettings.UtxoStore.GetBatcherSize = 512
	tSettings.UtxoStore.GetBatcherDurationMillis = 1
	tSettings.UtxoStore.GetBatcherDrainMode = true
	tSettings.UtxoStore.StoreBatcherSize = 512
	tSettings.UtxoStore.StoreBatcherDurationMillis = 1
	tSettings.Aerospike.StoreBatcherDuration = 1 * time.Millisecond
	tSettings.UtxoStore.StoreBatcherDrainMode = true
	tSettings.UtxoStore.SpendBatcherSize = 512
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1
	tSettings.UtxoStore.SpendBatcherDrainMode = false
	tSettings.UtxoStore.SpendBatcherConcurrency = 256
	tSettings.UtxoStore.LockedBatcherSize = 512
	tSettings.UtxoStore.LockedBatcherDurationMillis = 1
	tSettings.UtxoStore.LockedBatcherDrainMode = false
	tSettings.UtxoStore.BatcherMaxConcurrent = 512

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
	if err != nil {
		b.Skipf("Aerospike testcontainer unavailable: %v", err)
	}

	host, err := container.Host(ctx)
	require.NoError(b, err)
	port, err := container.ServicePort(ctx)
	require.NoError(b, err)

	aerospikeContainerURL := fmt.Sprintf(
		"aerospike://%s:%d/test?set=test&block_retention=1&externalStore=file:///tmp/bench-validator-flag-matrix-external",
		host, port,
	)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(b, err)

	aeroStore, err := aerostore.New(ctx, logger, tSettings, aeroURL)
	require.NoError(b, err)
	aeroStore.SetExternalStore(memory.New())

	iface, err := New(ctx, logger, tSettings, aeroStore, nil, nil, nil, nil)
	require.NoError(b, err)
	val := iface.(*Validator)

	cleanup := func() {
		if termErr := container.Terminate(ctx); termErr != nil {
			b.Logf("warning: failed to terminate Aerospike container: %v", termErr)
		}
	}

	return val, aeroStore, cleanup
}
