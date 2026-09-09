package netsync

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
)

// TestArenaIsReleasedAfterConversion is the measurement behind a claim the code
// makes in five places and, until this test, nowhere proved: that the wire
// block's decode arena becomes collectable once WireTxToGoBtTx has run.
//
// The claim matters because the conversion clones every script, so while the
// wire block is still reachable the process holds two complete copies. On
// mainnet at 3.44 GB a block that is roughly 7 GB of live heap against a 6 GiB
// soft limit, and the repo's own benchmark measures an eightfold slowdown in
// this exact loop when the limit is reached.
//
// It is measured rather than reasoned about because reasoning got it wrong. Every
// bt.Input retained a pointer into the wire transaction it came from, which kept
// that transaction's script slice alive, which kept its whole 4 MiB arena chunk
// alive. Since every chunk carries at least one input script, one retained
// pointer per input pinned the entire arena for as long as the converted
// transactions lived.
func TestArenaIsReleasedAfterConversion(t *testing.T) {
	if raceDetectorEnabled {
		// The detector allocates shadow memory for every access and changes
		// allocation behaviour throughout, so the share of the heap released
		// here is not the share a production build releases. The property under
		// test is a memory one, so there is nothing to salvage by loosening the
		// threshold: it would either stop catching the regression or start
		// failing at random.
		t.Skip("this measures heap proportions, which the race detector perturbs")
	}

	// A baseline before anything is built, because this test runs inside a shared
	// test binary and whatever ran before it leaves heap behind. An assertion
	// against a share of TOTAL heap passes alone and fails in the suite, which is
	// how the third version of this test went wrong. Every figure below is
	// therefore relative to a quantity measured here.
	var baseline runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&baseline)

	// Decoded from serialised bytes, not built object by object. This matters:
	// go-wire's decoder puts every script into a bump-allocated arena in 4 MiB
	// chunks, and a block assembled with NewMsgTx and AddTxIn has no arena at
	// all, so a test built that way cannot observe the retention it is about.
	// The first version of this test made exactly that mistake.
	block := wire.NewMsgBlock(wire.NewBlockHeader(1, &chainhash.Hash{}, &chainhash.Hash{}, 0, 0))

	for _, tx := range buildBenchTxs(t) {
		_ = block.AddTransaction(tx.MsgTx())
	}

	var serialised bytes.Buffer
	if err := block.Serialize(&serialised); err != nil {
		t.Fatalf("serialise: %v", err)
	}

	block = nil

	decoded := wire.NewMsgBlock(wire.NewBlockHeader(1, &chainhash.Hash{}, &chainhash.Hash{}, 0, 0))
	if err := decoded.Bsvdecode(bytes.NewReader(serialised.Bytes()), 0, wire.BaseEncoding); err != nil {
		t.Fatalf("decode: %v", err)
	}

	serialised.Reset()

	wireTxs := make([]*bsvutil.Tx, 0, len(decoded.Transactions))
	for _, msgTx := range decoded.Transactions {
		wt := bsvutil.NewTx(msgTx)
		_ = wt.Hash()
		wireTxs = append(wireTxs, wt)
	}

	decoded = nil

	var wireOnly runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&wireOnly)
	t.Logf("heap with the wire block alone: %.1f MB", float64(wireOnly.HeapAlloc)/(1<<20))

	converted := make([]*bt.Tx, 0, len(wireTxs))
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(wireTxs))

	for _, wireTx := range wireTxs {
		tx := &bt.Tx{}
		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			t.Fatalf("conversion failed: %v", err)
		}

		hashCopy := *wireTx.Hash()
		tx.SetTxHash(&hashCopy)
		txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
		converted = append(converted, tx)
	}

	var withBoth runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&withBoth)

	// MANDATORY, and the reason the first two versions of this test measured
	// nothing. The only later use of wireTxs is the assignment to nil below,
	// which is never read, so without this the compiler treats it as dead the
	// moment the loop ends and Go's precise collector reclaims it BEFORE the
	// measurement above. Both readings then come out identical and the test
	// reports no retention whether or not any exists.
	runtime.KeepAlive(wireTxs)

	// Drop the wire side, exactly as HandleBlockDirect intends to when
	// prepareSubtrees returns. Only the converted transactions stay reachable.
	wireTxs = nil

	var afterDrop runtime.MemStats

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&afterDrop)

	// Keep the converted side alive across the measurement, or the compiler is
	// entitled to collect it and the numbers mean nothing.
	runtime.KeepAlive(converted)
	runtime.KeepAlive(txMap)

	released := int64(withBoth.HeapAlloc) - int64(afterDrop.HeapAlloc)
	wireSize := int64(wireOnly.HeapAlloc) - int64(baseline.HeapAlloc)

	t.Logf("the wire block itself: %.1f MB", float64(wireSize)/(1<<20))
	t.Logf("heap with both copies: %.1f MB", float64(withBoth.HeapAlloc)/(1<<20))
	t.Logf("heap after dropping the wire block: %.1f MB", float64(afterDrop.HeapAlloc)/(1<<20))
	t.Logf("released: %.1f MB, which is %.0f%% of the wire block",
		float64(released)/(1<<20), 100*float64(released)/float64(wireSize))

	if wireSize <= 0 {
		t.Fatalf("the wire block measured %d bytes, so nothing here means anything", wireSize)
	}

	// Dropping the wire block must give back most of what the wire block cost.
	// Half is the floor rather than a round number: the converted transactions
	// keep their own copies of every script, so a little of the wire side is
	// legitimately shared, and the point is to catch the whole decode arena
	// being pinned rather than to pin an exact ratio across Go versions and
	// transaction shapes. Measured on this benchmark's shape the fixed code
	// gives back essentially all of it and the unfixed code about a third.
	if float64(released) < float64(wireSize)*0.5 {
		t.Fatalf("dropping the wire block gave back only %.1f MB of the %.1f MB it cost; the decode arena is still pinned, so the process carries two full copies of every block through the rest of the pipeline",
			float64(released)/(1<<20), float64(wireSize)/(1<<20))
	}
}
