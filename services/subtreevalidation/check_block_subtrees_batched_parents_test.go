package subtreevalidation

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// parentsBatch builds a batchState over txs with every transaction missing,
// as processTransactionsBatched does before resolveFromMemory.
func parentsBatch(txs []*bt.Tx) *batchState {
	b := &batchState{
		txs:      txs,
		hashes:   make([]chainhash.Hash, len(txs)),
		position: make(map[chainhash.Hash]int, len(txs)),
		heights:  make([][]uint32, len(txs)),
		parents:  make([][]int, len(txs)),
		fallback: make([]bool, len(txs)),
	}

	for i, tx := range txs {
		b.hashes[i] = *tx.TxIDChainHash()
		b.position[b.hashes[i]] = i
		b.missing = append(b.missing, i)
	}

	return b
}

// spendOf builds a transaction spending the given outputs of the given
// parents, with one OP_TRUE output.
func spendOf(tb testing.TB, lockTime uint32, parents []*bt.Tx, vouts []uint32) *bt.Tx {
	tb.Helper()

	tx := bt.NewTx()
	tx.LockTime = lockTime

	for n, p := range parents {
		in := &bt.Input{PreviousTxOutIndex: vouts[n], UnlockingScript: bscript.NewFromBytes([]byte{}), SequenceNumber: 0xffffffff}
		require.NoError(tb, in.PreviousTxIDAdd(p.TxIDChainHash()))
		tx.Inputs = append(tx.Inputs, in)
	}

	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: opTrue})

	return tx
}

// outsideTx builds a transaction with nOutputs OP_TRUE outputs spending an
// outpoint outside the batch.
func outsideTx(tb testing.TB, seed uint32, nOutputs int) *bt.Tx {
	tb.Helper()

	tx := bt.NewTx()
	tx.LockTime = seed

	in := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00}), SequenceNumber: 0xffffffff}
	require.NoError(tb, in.PreviousTxIDAdd(&chainhash.Hash{0xaa, byte(seed), byte(seed >> 8), byte(seed >> 16)}))
	tx.Inputs = append(tx.Inputs, in)

	for i := 0; i < nOutputs; i++ {
		tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: opTrue})
	}

	return tx
}

// A transaction spending several outputs of one same-batch parent records that
// parent once, and its distinct parents appear in the order its inputs name
// them.
func TestResolveFromMemory_ParentsDeduplicatedInOrder(t *testing.T) {
	a := outsideTx(t, 1, 3)
	b := outsideTx(t, 2, 2)
	c := outsideTx(t, 3, 1)
	child := spendOf(t, 4, []*bt.Tx{b, a, b, a, c, a}, []uint32{0, 0, 1, 2, 0, 1})
	grandchild := spendOf(t, 5, []*bt.Tx{child}, []uint32{0})

	batch := parentsBatch([]*bt.Tx{a, b, c, child, grandchild})

	_, _, err := resolveFromMemory(batch, batchedTestHeight)
	require.NoError(t, err)
	require.Equal(t, [][]int{nil, nil, nil, {1, 0, 2}, {3}}, batch.parents)
}

func BenchmarkResolveFromMemory_FanIn(b *testing.B) {
	for _, n := range []int{80_000, 160_000} {
		b.Run(fmt.Sprintf("parents=%d", n), func(b *testing.B) {
			txs := make([]*bt.Tx, 0, n+1)
			vouts := make([]uint32, n)

			for i := 0; i < n; i++ {
				txs = append(txs, outsideTx(b, uint32(i), 1)) //nolint:gosec // test data
			}

			txs = append(txs, spendOf(b, 0xffffffff, txs, vouts))
			batch := parentsBatch(txs)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := range batch.parents {
					batch.parents[j] = nil
				}

				if _, _, err := resolveFromMemory(batch, batchedTestHeight); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
