package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// BenchmarkValidateBatch_FallbackVsNative compares the throughput of
// ValidateBatch with the flag off vs on. Native uses a stub UTXO store
// so the benchmark measures the validator pipeline overhead, not store
// latency. Use this when iterating on native-path code to detect
// regressions vs the fallback baseline. Run locally; not a CI gate.
func BenchmarkValidateBatch_FallbackVsNative(b *testing.B) {
	const N = 256

	cases := []struct {
		name     string
		useBatch bool
	}{
		{"fallback", false},
		{"native", true},
	}

	for _, c := range cases {
		c := c
		b.Run(c.name, func(b *testing.B) {
			v, stub := newNativeValidator(b)
			installNoopCPUOverride(b, v)
			v.settings.Validator.UseBatchValidation = c.useBatch

			// Seed parents.
			parents := make([]chainhash.Hash, N)
			parentMap := map[[32]byte]*aerospike.ParentRecord{}
			txs := make([]*bt.Tx, N)
			for i := 0; i < N; i++ {
				parents[i] = chainhash.Hash{byte(i / 256), byte(i % 256), 0x99}
				var key [32]byte
				copy(key[:], parents[i][:])
				parentMap[key] = &aerospike.ParentRecord{BlockHeight: 1}
				txs[i] = minimalTxWithParent(b, parents[i])
			}
			stub.parents = parentMap

			// Override BA to accept everything (only matters for native).
			v.overrideBASubmitForTest(func(ctx context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
				return map[chainhash.Hash]error{}
			})
			// Override txmeta publish so we don't try to write to a nil kafka client.
			v.overrideTxMetaPublishForTest(func(*bt.Tx, *meta.Data) {})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := v.ValidateBatch(context.Background(), txs, 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
