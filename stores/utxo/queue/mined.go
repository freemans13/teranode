package queue

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// SetMinedMulti updates the block ID for multiple transactions that have been mined.
// TODO: implement direct INSERT + UPDATE in Task 7.
func (s *Store) SetMinedMulti(_ context.Context, _ []*chainhash.Hash, _ utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	return nil, errors.NewProcessingError("not implemented")
}

// MarkTransactionsOnLongestChain is implemented in conflicting.go.
