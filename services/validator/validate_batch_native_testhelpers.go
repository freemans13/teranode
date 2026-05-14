package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// setBatchStoreForTest installs a batchUtxoStore stub on the Validator,
// bypassing the normal utxoStore type assertion in getBatchUtxoStore.
// This is a test seam — production code never calls this method.
func (v *Validator) setBatchStoreForTest(s batchUtxoStore) {
	v.batchStoreOverride = s
}

// overrideCPUValidationForTest installs a function that replaces the real
// TxValidator CPU-validation calls inside runCPUValidation. This allows
// tests to inject controlled per-tx failures without constructing a fully
// extended, signed transaction. Production code never calls this method.
func (v *Validator) overrideCPUValidationForTest(fn func(*bt.Tx) error) {
	v.cpuOverride = fn
}

// overrideBASubmitForTest installs a function that replaces the real
// submitToBlockAssemblyBatch implementation. Allows unit tests to control
// per-tx BA accept/reject without a live BlockAssembly service.
// Production code never calls this method.
func (v *Validator) overrideBASubmitForTest(fn func(ctx context.Context, txs []*bt.Tx) map[chainhash.Hash]error) {
	v.blockAssemblySubmitOverride = fn
}
