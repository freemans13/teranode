package utxo_test

// ---------------------------------------------------------------------------
// Aged fan-out tx builders + bench config for the deferred-DAH lag bench
// ---------------------------------------------------------------------------
//
// Background: the postgres UTXO store's deferred DAH-setting path re-aggregates
// each tx's FULL spend history to decide when to stamp delete_at_height. Under
// IBD with a high-fan-out workload (one source tx spending to many outputs, each
// spent independently by later txs), this re-aggregation grows with fan-out k
// and can lag severely. This file provides:
//
//   - makeAgedFanoutTx(workerID, seq, k): a tx with k spendable P2PKH outputs
//     plus OP_FALSE OP_RETURN padding outputs (realistic byte budget).
//   - makeSpendOfVout(parent, vout): a child tx spending exactly one named vout
//     of parent — the primitive the deferred-DAH cursor aggregates.
//   - Config constants (all env-overridable) for the harness (Tasks 2+).
//
// Env knobs:
//
//	AGED_FANOUT_K=64      # outputs per fan-out tx (default 64)
//	AGED_PARENTS=200000   # fan-out txs to create in the aged-parents pool
//	AGE_SPAN=50000        # height gap between parent creation and spend
//	BACKLOG_BOUND=2000    # max in-flight un-DAH-stamped parents before back-pressure

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Config constants (env-overridable via envInt — defined in throughput_stable_test.go)
// ---------------------------------------------------------------------------

var (
	// agedFanoutK is the number of spendable P2PKH outputs per fan-out tx.
	agedFanoutK = envInt("AGED_FANOUT_K", 64)
	// agedParents is the number of fan-out parent txs to create in the pool.
	agedParents = envInt("AGED_PARENTS", 200000)
	// ageSpan is the height gap between parent creation and when spends arrive.
	ageSpan = envInt("AGE_SPAN", 50000)
	// backlogBound is the max in-flight un-DAH-stamped parents before workers
	// back-pressure (prevents the deferred-DAH cursor from being overwhelmed).
	backlogBound = envInt("BACKLOG_BOUND", 2000)
)

// ---------------------------------------------------------------------------
// Fan-out tx builders
// ---------------------------------------------------------------------------

// makeAgedFanoutTx creates a tx with k spendable P2PKH outputs plus OP_FALSE
// OP_RETURN padding outputs. Each tx has a unique txid driven by workerID and
// seq embedded in a fake input referencing a synthetic previous outpoint.
// Padding outputs are provably unspendable (ShouldStoreOutputAsUTXO returns
// false for OP_FALSE OP_RETURN) and do not affect spendable_count, but DO
// inflate raw_tx to a mainnet-realistic size.
func makeAgedFanoutTx(workerID, seq, k int) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1

	// Synthetic previous outpoint: unique per (workerID, seq) so txid is unique.
	// Mirrors makeGenesisTx's approach of encoding identity into the prev-hash bytes.
	// p2pkhScript is defined in throughput_test.go (same package).
	var h [32]byte
	h[0] = byte(workerID)
	h[1] = byte(workerID >> 8)
	h[2] = byte(workerID >> 16)
	h[3] = byte(workerID >> 24)
	h[4] = byte(seq)
	h[5] = byte(seq >> 8)
	h[6] = byte(seq >> 16)
	h[7] = byte(seq >> 24)
	h[8] = 0xAF // sentinel: distinguishes aged-fanout txs from plain genesis txs
	prev, _ := chainhash.NewHash(h[:])
	_ = tx.From(prev.String(), 0, p2pkhScript().String(), 0)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	// k spendable P2PKH outputs (value 1000 sat each). Each output has a unique
	// locking script derived from (workerID, seq, vout) so raw_tx bytes are
	// distinct and not LZ4-compressible to nothing (real scripts carry distinct
	// 20-byte pubkey hashes; identical padding would hide de-TOAST cost).
	for v := 0; v < k; v++ {
		script := agedP2PKHScript(workerID, seq, v)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      1000,
			LockingScript: script,
		})
	}

	// OP_FALSE OP_RETURN padding: 4 non-spendable outputs to give a realistic
	// raw_tx byte budget (mirrors padReprOutputs). Unique seed per output so
	// bytes are incompressible.
	const nPad = 4
	const payloadLen = 30
	for p := 0; p < nPad; p++ {
		b := make([]byte, 0, 3+payloadLen)
		b = append(b, bscript.OpFALSE, bscript.OpRETURN, byte(payloadLen))
		seed := uint64(workerID)*0x9e3779b9 + uint64(seq)*0x6c62272e + uint64(p)*0x517cc1b7
		for j := 0; j < payloadLen; j++ {
			b = append(b, byte(seed>>(uint(j%8)*8))^byte(j*7+p))
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      0,
			LockingScript: bscript.NewFromBytes(b),
		})
	}

	return tx
}

// agedP2PKHScript returns a unique P2PKH-shaped locking script for the given
// (workerID, seq, vout). The 20-byte pubkey hash is filled deterministically so
// every output has distinct bytes, preventing LZ4 compression from collapsing
// them and hiding de-TOAST cost during benchmarks.
func agedP2PKHScript(workerID, seq, vout int) *bscript.Script {
	// P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
	b := make([]byte, 25)
	b[0] = 0x76 // OP_DUP
	b[1] = 0xa9 // OP_HASH160
	b[2] = 0x14 // push 20 bytes
	for i := 0; i < 20; i++ {
		b[3+i] = byte(workerID*0x9b+seq*0x6d+vout*0x1f+i*0x37) ^ byte((workerID>>8)*0xb3+i)
	}
	b[23] = 0x88 // OP_EQUALVERIFY
	b[24] = 0xac // OP_CHECKSIG
	s := bscript.Script(b)
	return &s
}

// makeSpendOfVout builds a child tx spending exactly one named output (vout) of
// parent. The child carries one input referencing parent.TxIDChainHash():vout
// and one P2PKH output, appending exactly one row to the spends table. This is
// the primitive the deferred-DAH cursor aggregates when deciding whether a
// parent is fully spent.
func makeSpendOfVout(parent *bt.Tx, vout uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1

	parentOut := parent.Outputs[vout]
	_ = tx.From(
		parent.TxIDChainHash().String(),
		vout,
		parentOut.LockingScript.String(),
		parentOut.Satoshis,
	)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	outVal := parentOut.Satoshis / 2
	if outVal == 0 {
		outVal = 1
	}
	tx.AddOutput(&bt.Output{Satoshis: outVal, LockingScript: p2pkhScript()})
	return tx
}

// ---------------------------------------------------------------------------
// Unit test: makeAgedFanoutTx shape
// ---------------------------------------------------------------------------

// TestMakeAgedFanoutTx_Shape verifies that makeAgedFanoutTx produces exactly k
// spendable outputs (per utxo.ShouldStoreOutputAsUTXO) plus at least one
// additional non-spendable OP_FALSE OP_RETURN padding output.
//
// utxo.ShouldStoreOutputAsUTXO signature:
//
//	ShouldStoreOutputAsUTXO(output *bt.Output, blockHeight uint32, genesisActivationHeight uint32) bool
//
// We use a post-genesis blockHeight (>= genesisActivationHeight) so that only
// OP_FALSE OP_RETURN outputs are provably unspendable. Our P2PKH outputs are
// spendable; our padding outputs (OP_FALSE OP_RETURN) are not.
func TestMakeAgedFanoutTx_Shape(t *testing.T) {
	const k = 64
	// mainnet genesis activation height; post-genesis means only OP_FALSE OP_RETURN is unspendable.
	const genesisActivation = uint32(620538)
	const blockHeight = genesisActivation + 1

	tx := makeAgedFanoutTx(1, 0, k)

	spendable := 0
	for _, o := range tx.Outputs {
		if utxo.ShouldStoreOutputAsUTXO(o, blockHeight, genesisActivation) {
			spendable++
		}
	}
	require.Equal(t, k, spendable, "must have exactly k spendable outputs")
	require.Greater(t, len(tx.Outputs), k, "must also carry OP_RETURN padding")
}
