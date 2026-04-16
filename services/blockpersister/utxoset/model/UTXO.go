// Package model defines the core data structures for UTXO (Unspent Transaction Output) management.
//
// This package provides the fundamental types and operations for handling UTXOs in the Teranode
// block persister service. UTXOs represent the unspent outputs from Bitcoin transactions that
// can be used as inputs in future transactions, forming the basis of Bitcoin's accounting model.
//
// The package includes:
//   - UTXO: Core structure representing an unspent transaction output
//   - UTXOKey: Unique identifier for UTXOs combining transaction hash and output index
//   - UTXOValue: Value and script data associated with a UTXO
//   - UTXOSet: Collection of UTXOs with efficient lookup and modification operations
//   - UTXODiff: Difference sets for tracking UTXO changes during block processing
//   - UTXOMap: High-performance map implementation optimized for UTXO operations
//
// These types are designed for high-performance operations on large UTXO sets, supporting
// the efficient processing of Bitcoin blocks with potentially millions of transactions.
// The implementation focuses on memory efficiency and fast lookup times to handle the
// scale requirements of the BSV blockchain.
//
// Thread safety considerations:
// Most types in this package are not inherently thread-safe and require external
// synchronization when accessed concurrently. The UTXOSet and related types include
// specific concurrency patterns documented in their respective implementations.
package model

// UTXO represents an unspent transaction output with its key and value.
//
// A UTXO is a fundamental unit in Bitcoin's accounting model, representing an output
// that has not yet been spent. It combines identification information with the actual
// value and script data that can be spent.
//
// The Val field is a pointer to allow for nil values when representing spent UTXOs
// or for memory optimization with large UTXO sets.
type UTXO struct {
	// Key uniquely identifies this UTXO by transaction hash and output index
	Key UTXOKey

	// Val contains the spendable value and script conditions for this UTXO
	Val *UTXOValue
}
