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
	crand "crypto/rand"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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

	// NO CPU override — both paths (per-tx Validate vs ValidateBatch) must
	// run real script verification on the same input. BA submit and Kafka
	// TxMeta publish are still overridden because they require external
	// infra and are not part of the per-tx CPU/UTXO hot path we are
	// measuring here.
	val.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{}
	})
	val.overrideTxMetaPublishForTest(func(_ *bt.Tx, _ *meta.Data) {})

	// Pre-generate b.N rounds of `concurrency` fresh txs each. Parents have
	// OP_TRUE locking scripts so children (with an empty unlocking script)
	// pass real script verification — see seedOpTrueParentsForFlagMatrix.
	rounds := make([][]*bt.Tx, b.N)
	for i := 0; i < b.N; i++ {
		parents := seedOpTrueParentsForFlagMatrix(b, ctx, aeroStore, concurrency)
		rounds[i] = buildOpTrueChildrenForFlagMatrix(b, parents)
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

// seedOpTrueParentsForFlagMatrix creates n parent txs in the Aerospike
// store, each with a single OP_TRUE (anyone-can-spend) output. This is
// the validator-bench analogue of seedRandomParentsForCoalescerBench in
// the propagation bench, and is required when the bench runs real CPU
// validation — children with an empty unlocking script trivially
// satisfy the OP_TRUE locking script.
//
// Each parent has a random unique PreviousTxID so the tx hash is
// distinct (avoiding CREATE_ONLY collisions) and PreviousTxOutIndex=0
// (not 0xFFFFFFFF) so IsCoinbase()==false and the coinbase-maturity
// lock does not apply.
func seedOpTrueParentsForFlagMatrix(b testing.TB, ctx context.Context, s *aerostore.Store, n int) []*bt.Tx {
	b.Helper()
	opTrue, err := bscript.NewFromHexString("51") // OP_1 / OP_TRUE
	require.NoError(b, err)

	parents := make([]*bt.Tx, n)
	emptyScript := bscript.Script{}

	for i := 0; i < n; i++ {
		tx := bt.NewTx()

		var randBytes [32]byte
		_, randErr := crand.Read(randBytes[:])
		require.NoError(b, randErr)
		uniqueHash, hashErr := chainhash.NewHash(randBytes[:])
		require.NoError(b, hashErr)

		in := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   &emptyScript,
			PreviousTxSatoshis: 2000,
			UnlockingScript:    &emptyScript,
			SequenceNumber:     0xFFFFFFFF,
		}
		require.NoError(b, in.PreviousTxIDAdd(uniqueHash))
		tx.Inputs = append(tx.Inputs, in)

		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      1000,
			LockingScript: opTrue,
		})

		_, err = s.Create(ctx, tx, 0)
		require.NoError(b, err)
		parents[i] = tx
	}
	return parents
}

// buildOpTrueChildrenForFlagMatrix builds one child tx per parent, each
// spending output 0 of its parent. PreviousTxScript is populated with
// the parent's OP_TRUE so the child is "extended" (the validator does
// not need to hydrate inputs from the UTXO store before CPU validation
// — matching the existing buildChildrenSpendingParentsForBench shape).
// UnlockingScript is a non-nil empty script, which trivially satisfies
// OP_TRUE under real script verification.
func buildOpTrueChildrenForFlagMatrix(b testing.TB, parents []*bt.Tx) []*bt.Tx {
	b.Helper()
	children := make([]*bt.Tx, len(parents))
	for i, parent := range parents {
		ph := parent.TxIDChainHash()
		child := bt.NewTx()

		emptyScript := bscript.Script{}
		in := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   parent.Outputs[0].LockingScript, // OP_TRUE
			PreviousTxSatoshis: parent.Outputs[0].Satoshis,
			UnlockingScript:    &emptyScript,
		}
		require.NoError(b, in.PreviousTxIDAdd(ph))
		child.Inputs = append(child.Inputs, in)

		// Pay less than parent's 1000 satoshis so fee = 500 satoshis
		// (valid). Output script is whatever PayToAddress produces;
		// children aren't themselves spent in this bench.
		require.NoError(b, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

		// Extended: PreviousTxScript / PreviousTxSatoshis already set,
		// so the validator's Phase-A hydration becomes a no-op.
		child.SetExtended(true)
		children[i] = child
	}
	return children
}
