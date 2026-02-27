package utxocheck

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// CheckAllTxsExistInUTXO batch-checks whether all transaction hashes exist in the UTXO store.
// It processes hashes in batches, skipping coinbase placeholders.
// Returns false immediately when any tx is missing or still in Creating state.
// Returns true only when ALL non-coinbase txs exist and are fully created.
// On UTXO store error, returns (false, err) so callers can fall back to subtreeData.
func CheckAllTxsExistInUTXO(ctx context.Context, utxoStore utxo.Store, txHashes []chainhash.Hash, batchSize int) (bool, error) {
	if len(txHashes) == 0 {
		return true, nil
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(txHashes); i += batchSize {
		end := i + batchSize
		if end > len(txHashes) {
			end = len(txHashes)
		}

		batch := make([]*utxo.UnresolvedMetaData, 0, end-i)
		for j := i; j < end; j++ {
			if txHashes[j].Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				continue
			}
			batch = append(batch, &utxo.UnresolvedMetaData{
				Hash: txHashes[j],
				Idx:  j,
			})
		}

		if len(batch) == 0 {
			continue
		}

		if err := utxoStore.BatchDecorate(ctx, batch, fields.Creating); err != nil {
			return false, err
		}

		for _, item := range batch {
			if item.Err != nil || item.Data == nil || item.Data.Creating {
				return false, nil
			}
		}
	}

	return true, nil
}
