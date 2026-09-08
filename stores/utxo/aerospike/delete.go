// //go:build aerospike

// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to utxostore_utxoBatchSize UTXOs (default 128)
//   - Pagination Records: Additional records for transactions with more outputs than utxostore_utxoBatchSize (default 128)
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - totalUtxos: Total number of UTXOs
//   - recordUtxos: Number of UTXOs in this record
//   - spentUtxos: Number of spent UTXOs in this record
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records when outputs exceed utxostore_utxoBatchSize
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// Delete removes a transaction and everything Create wrote for it: the
// pagination records, the external blob, and the master record. If the
// transaction doesn't exist, the operation is considered successful.
//
// It used to remove the master record only. That left pagination records with no
// master, which are worse than a leak: Create writes pagination records
// CREATE_ONLY and classifyCreateBatchResults treats a KEY_EXISTS on a non-master
// record as "already present", so re-creating the transaction later would
// succeed while silently keeping the stale page contents from its previous life.
// An orphaned external blob is a plain leak, since the pruner is the only other
// deleter and it only ever looks at records it can still find.
//
// Order matters. The master is deleted LAST, so an interrupted Delete leaves the
// master in place and a repeat call can still find the pagination count and
// finish the job. Deleting the master first would strand the pages permanently.
//
// Parameters:
//   - ctx: Context for cancellation and for the external blob deletion
//   - hash: Transaction hash to delete
//
// Returns:
//   - nil if deletion successful or record not found
//   - error if deletion fails for other reasons
//
// Examples:
//
//	// Delete a transaction
//	err := store.Delete(ctx, txHash)
//	if err != nil {
//	    if errors.Is(err, aerospike.ErrKeyNotFound) {
//	        // Handle not found case
//	    } else {
//	        // Handle other errors
//	    }
//	}
//
// Metrics:
//   - prometheusUtxoMapDelete: Incremented on successful deletion
//   - prometheusUtxoMapErrors: Incremented on deletion errors
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(s.settings, 0)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	// Read the master's layout before removing anything: it is the only place
	// that says how many pagination records exist and whether the transaction
	// body lives in the external blob store.
	master, err := s.client.Get(nil, key, fields.External.String(), fields.TotalExtraRecs.String())
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		return errors.NewStorageError("error reading aerospike record before delete", err)
	}

	if err := s.deleteExtraRecords(policy, hash, master); err != nil {
		return err
	}

	if err := s.deleteExternalBlob(ctx, hash, master); err != nil {
		return err
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		if e, ok := err.(*aerospike.AerospikeError); ok {
			prometheusUtxoMapErrors.WithLabelValues("Delete", e.ResultCode.String()).Inc()
		} else {
			prometheusUtxoMapErrors.WithLabelValues("Delete", "unknown").Inc()
		}

		return errors.NewStorageError("error in aerospike delete key", err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}

// deleteExtraRecords removes the pagination records a transaction with more than
// utxoBatchSize outputs was split into. Keyed exactly as splitIntoBatches wrote
// them, so this stays in step with Create.
func (s *Store) deleteExtraRecords(policy *aerospike.WritePolicy, hash *chainhash.Hash, master *aerospike.Record) error {
	if master == nil || master.Bins == nil {
		return nil
	}

	totalExtraRecs, ok := master.Bins[fields.TotalExtraRecs.String()].(int)
	if !ok || totalExtraRecs <= 0 {
		return nil
	}

	for i := 1; i <= totalExtraRecs; i++ {
		extraKey, err := aerospike.NewKey(s.namespace, s.setName, uaerospike.CalculateKeySourceInternal(hash, uint32(i)))
		if err != nil {
			return errors.NewProcessingError("error in aerospike NewKey for pagination record %d", i, err)
		}

		if _, err := s.client.Delete(policy, extraKey); err != nil {
			if errors.Is(err, aerospike.ErrKeyNotFound) {
				continue
			}

			return errors.NewStorageError("error deleting aerospike pagination record %d", i, err)
		}
	}

	return nil
}

// deleteExternalBlob removes the blob holding the transaction body when it was
// stored outside Aerospike. The file type mirrors the write side in create.go: a
// transaction with no inputs is stored outputs-only.
func (s *Store) deleteExternalBlob(ctx context.Context, hash *chainhash.Hash, master *aerospike.Record) error {
	if master == nil || master.Bins == nil {
		return nil
	}

	if external, ok := master.Bins[fields.External.String()].(bool); !ok || !external {
		return nil
	}

	// Both file types are attempted rather than inferred. create.go writes the
	// outputs-only blob for zero-input transactions and the full .tx blob for
	// everything else, so exactly one of these exists — but the master's inputs
	// bin is not a reliable discriminator for an external transaction, whose
	// inputs live in the blob rather than on the record.
	for _, fileType := range []fileformat.FileType{fileformat.FileTypeTx, fileformat.FileTypeOutputs} {
		if err := s.externalStore.Del(ctx, hash[:], fileType); err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				continue
			}

			return errors.NewStorageError("error deleting external %s blob for %s", fileType, hash.String(), err)
		}
	}

	return nil
}
