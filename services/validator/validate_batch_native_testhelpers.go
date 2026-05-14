package validator

// setBatchStoreForTest installs a batchUtxoStore stub on the Validator,
// bypassing the normal utxoStore type assertion in getBatchUtxoStore.
// This is a test seam — production code never calls this method.
func (v *Validator) setBatchStoreForTest(s batchUtxoStore) {
	v.batchStoreOverride = s
}
