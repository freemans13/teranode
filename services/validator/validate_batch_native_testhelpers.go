package validator

import "github.com/bsv-blockchain/go-bt/v2"

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
