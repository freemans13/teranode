package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// ParentRecord is a slim view of a parent tx record containing just enough
// for the validator to extend a child tx's inputs. BlockHeight is derived
// from the first entry in the fields.BlockHeights bin (0 when unmined).
// Bins holds the raw bin map so callers can extract additional fields using
// the existing fields package constants without forcing a single decode shape.
type ParentRecord struct {
	// BlockHeight is the height of the block that first mined this parent, or
	// 0 if the parent is unmined. Derived from fields.BlockHeights bin index 0.
	BlockHeight uint32
	// Bins holds the raw Aerospike bin map. Callers may read any field using
	// the fields package constants (e.g. fields.Outputs.String()).
	Bins aerospike.BinMap
}

// BatchGetParents fetches a slice of parent tx records in a single Aerospike
// BatchOperate call. It is a direct, pass-through method: no go-batcher, no
// buffering, no timeouts. The caller is responsible for chunking if needed.
//
// Returns:
//   - found:   map keyed by 32-byte parent hash to *ParentRecord for every
//     hash that had an Aerospike record.
//   - missing: slice of input hashes that had no record. This is NOT a fatal
//     error; the caller maps these to per-tx ErrTxMissingParent.
//   - err:     non-nil only when the entire BatchOperate call fails.
func (s *Store) BatchGetParents(ctx context.Context, parentHashes [][]byte) (map[[32]byte]*ParentRecord, [][]byte, error) {
	if len(parentHashes) == 0 {
		return map[[32]byte]*ParentRecord{}, nil, nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	records := make([]aerospike.BatchRecordIfc, len(parentHashes))
	for i, h := range parentHashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, h)
		if err != nil {
			return nil, nil, err
		}
		// nil bin list means fetch all bins.
		records[i] = aerospike.NewBatchRead(nil, key, nil)
	}

	batchOperateFn := s.client.BatchOperate
	if s.batchOperateFn != nil {
		batchOperateFn = s.batchOperateFn
	}

	if asErr := batchOperateFn(batchPolicy, records); asErr != nil {
		return nil, nil, asErr
	}

	found := make(map[[32]byte]*ParentRecord, len(parentHashes))
	var missing [][]byte

	for i, r := range records {
		rec := r.(*aerospike.BatchRead).Record
		if rec == nil {
			missing = append(missing, parentHashes[i])
			continue
		}

		var key [32]byte
		copy(key[:], parentHashes[i])

		bh := blockHeightFromBins(rec.Bins)
		found[key] = &ParentRecord{
			BlockHeight: bh,
			Bins:        rec.Bins,
		}
	}

	return found, missing, nil
}

// blockHeightFromBins extracts the first block height stored in the
// fields.BlockHeights bin. Returns 0 when the tx is unmined or the bin is
// absent/malformed — the caller must treat 0 as "unmined".
func blockHeightFromBins(bins aerospike.BinMap) uint32 {
	v, ok := bins[fields.BlockHeights.String()]
	if !ok {
		return 0
	}

	switch t := v.(type) {
	case []interface{}:
		if len(t) == 0 {
			return 0
		}
		switch first := t[0].(type) {
		case int:
			if first < 0 {
				return 0
			}
			return uint32(first) //nolint:gosec
		case uint32:
			return first
		}
	}

	return 0
}
