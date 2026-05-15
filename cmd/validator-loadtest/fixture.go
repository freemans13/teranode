package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/propagation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
)

// dummyT satisfies test.TestingT so CreateBaseTestSettings can be called from
// outside a *testing.T context. Logs go to stderr; TempDir creates a real
// temporary directory that is removed when the fixture is cleaned up.
type dummyT struct {
	tmpDir string
}

func newDummyT() *dummyT {
	dir, err := os.MkdirTemp("", "validator-loadtest-*")
	if err != nil {
		log.Fatalf("fixture: mktemp: %v", err)
	}
	return &dummyT{tmpDir: dir}
}

func (d *dummyT) Errorf(format string, args ...interface{}) {
	log.Printf("[test] ERROR: "+format, args...)
}

func (d *dummyT) Logf(format string, args ...interface{}) {
	log.Printf("[test] "+format, args...)
}

func (d *dummyT) TempDir() string {
	return d.tmpDir
}

func (d *dummyT) cleanup() {
	_ = os.RemoveAll(d.tmpDir)
}

// fixture is the wiring the harness needs: a real Aerospike (via
// testcontainers) backing a real Validator wired into a PropagationServer
// with the coalescer constructed.
type fixture struct {
	ps        *propagation.PropagationServer
	aeroStore *aerostore.Store
	parents   []*bt.Tx
	cleanup   func()
}

// fixtureConfig collects the harness-side knobs.
type fixtureConfig struct {
	aerospikeURL       string
	useBatch           bool
	batchMaxSize       int
	batchMaxWait       time.Duration
	batchMaxConcurrent int
	connQueueSize      int
	parentPoolSize     int
}

// newFixture spins up an Aerospike testcontainer (or connects to
// cfg.aerospikeURL if non-empty), constructs the validator + propagation
// stack, and pre-seeds the requested number of parent UTXOs.
func newFixture(ctx context.Context, cfg fixtureConfig) *fixture {
	tracing.SetupMockTracer()

	dt := newDummyT()
	logger := ulogger.New("loadtest")

	tSettings := test.CreateBaseTestSettings(dt)
	tSettings.BlockAssembly.Disabled = true
	tSettings.Validator.UseBatchValidation = cfg.useBatch
	tSettings.Validator.BatchMaxSize = cfg.batchMaxSize
	tSettings.Validator.BatchMaxWait = cfg.batchMaxWait
	tSettings.Validator.BatchMaxConcurrent = cfg.batchMaxConcurrent
	// validator.New requires Kafka.TxMetaConfig to be non-nil even when
	// no Kafka producer is configured. The standalone binary doesn't
	// load settings.conf, so seed a dummy URL — it's only checked for
	// non-nilness; no Kafka client is started when the producer is nil.
	if tSettings.Kafka.TxMetaConfig == nil {
		dummyURL, _ := url.Parse("kafka://localhost:0/txmeta-disabled")
		tSettings.Kafka.TxMetaConfig = dummyURL
	}

	var (
		aeroURL     string
		containerFn = func() {}
	)
	if cfg.aerospikeURL != "" {
		aeroURL = cfg.aerospikeURL
	} else {
		container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
		if err != nil {
			log.Fatalf("fixture: aerospike container: %v", err)
		}
		host, hostErr := container.Host(ctx)
		if hostErr != nil {
			log.Fatalf("fixture: container host: %v", hostErr)
		}
		port, portErr := container.ServicePort(ctx)
		if portErr != nil {
			log.Fatalf("fixture: container port: %v", portErr)
		}
		aeroURL = fmt.Sprintf(
			"aerospike://%s:%d/test?set=test&block_retention=1&externalStore=file:///tmp/loadtest-aero-external&ConnectionQueueSize=%d",
			host, port, cfg.connQueueSize,
		)
		containerFn = func() { _ = container.Terminate(ctx) }
	}

	u, err := url.Parse(aeroURL)
	if err != nil {
		log.Fatalf("fixture: parse url: %v", err)
	}

	aeroStore, err := aerostore.New(ctx, logger, tSettings, u)
	if err != nil {
		log.Fatalf("fixture: aerospike store: %v", err)
	}
	aeroStore.SetExternalStore(memory.New())

	vIface, err := validator.New(ctx, logger, tSettings, aeroStore, nil, nil, nil, nil)
	if err != nil {
		log.Fatalf("fixture: validator: %v", err)
	}

	txStore := memory.New()
	ps := propagation.New(logger, tSettings, txStore, vIface, nil, nil, nil)

	if cfg.useBatch {
		ps.SetCoalescerForBench(propagation.NewTxCoalescer(
			ctx, logger, vIface,
			tSettings.Validator.BatchMaxSize,
			tSettings.Validator.BatchMaxWait,
			tSettings.Validator.BatchMaxConcurrent,
		))
	}

	parents := seedParents(ctx, aeroStore, cfg.parentPoolSize)

	return &fixture{
		ps:        ps,
		aeroStore: aeroStore,
		parents:   parents,
		cleanup: func() {
			ps.CloseCoalescerForBench(context.Background())
			containerFn()
			dt.cleanup()
		},
	}
}

// seedParents creates n parent records directly via aeroStore.Create
// (bypassing the validator). Each parent has one OP_TRUE output with
// 2000 satoshis so children can spend it with empty unlocking scripts.
func seedParents(ctx context.Context, s *aerostore.Store, n int) []*bt.Tx {
	opTrue, err := bscript.NewFromHexString("51") // OP_1 / OP_TRUE (anyone-can-spend)
	if err != nil {
		log.Fatalf("seedParents: opTrue: %v", err)
	}

	emptyScript := bscript.Script{}
	parents := make([]*bt.Tx, n)

	for i := 0; i < n; i++ {
		tx := bt.NewTx()

		// Unique random 32-byte PreviousTxID so every parent has a distinct tx hash.
		// PreviousTxOutIndex=0 (not 0xFFFFFFFF) ensures IsCoinbase()==false, which
		// prevents the Aerospike Lua script from applying the coinbase maturity lock.
		var randBytes [32]byte
		if _, err := crand.Read(randBytes[:]); err != nil {
			log.Fatalf("seedParents: rand: %v", err)
		}
		uniqueHash, hashErr := chainhash.NewHash(randBytes[:])
		if hashErr != nil {
			log.Fatalf("seedParents: hash: %v", hashErr)
		}

		in := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   &emptyScript,
			PreviousTxSatoshis: 2000, // input > output so fee = 1000 (valid)
			UnlockingScript:    &emptyScript,
			SequenceNumber:     0xFFFFFFFF,
		}
		if err := in.PreviousTxIDAdd(uniqueHash); err != nil {
			log.Fatalf("seedParents: txid add: %v", err)
		}
		tx.Inputs = append(tx.Inputs, in)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      1000,
			LockingScript: opTrue,
		})

		if _, err := s.Create(ctx, tx, 0); err != nil {
			log.Fatalf("seedParents: create %d: %v", i, err)
		}
		parents[i] = tx
	}
	return parents
}

// buildChildSpending returns a single non-extended child tx that
// spends parent's output 0. Non-extended = realistic wire shape;
// the validator must hydrate PreviousTxSatoshis from the store at
// Phase A.
func buildChildSpending(parent *bt.Tx) *bt.Tx {
	child := bt.NewTx()
	ph := parent.TxIDChainHash()

	emptyScript := bscript.Script{}
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		// Deliberately leave PreviousTxSatoshis and PreviousTxScript unset
		// to simulate a non-extended (wire-format) transaction.
		UnlockingScript: &emptyScript,
		SequenceNumber:  0xFFFFFFFF,
	}
	if err := in.PreviousTxIDAdd(ph); err != nil {
		log.Fatalf("buildChildSpending: txid add: %v", err)
	}
	child.Inputs = append(child.Inputs, in)

	// Pay less than parent's 1000 satoshis so fee = 500 satoshis (valid once hydrated).
	if err := child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500); err != nil {
		log.Fatalf("buildChildSpending: payto: %v", err)
	}
	return child
}
