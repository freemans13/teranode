package model

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// UTXODiff tracks changes to the UTXO set within a block.
//
// Maintains separate maps for UTXOs that are created (Added) and spent (Removed)
// during block processing. Not thread-safe.
type UTXODiff struct {
	// logger provides logging functionality
	logger ulogger.Logger
	// BlockHash is the hash of the block this diff belongs to
	BlockHash chainhash.Hash
	// Added contains newly created UTXOs
	Added UTXOMap
	// Removed contains spent UTXOs
	Removed UTXOMap
}

// NewUTXODiff creates a new UTXODiff.
func NewUTXODiff(logger ulogger.Logger, blockHash *chainhash.Hash) *UTXODiff {
	return &UTXODiff{
		logger:    logger,
		BlockHash: *blockHash,
		Added:     newUTXOMap(),
		Removed:   newUTXOMap(),
	}
}

// Add adds a UTXO to the map.
func (ud *UTXODiff) Add(txID chainhash.Hash, index uint32, value uint64, locktime uint32, script []byte) {
	uk := NewUTXOKey(txID, index)
	uv := NewUTXOValue(value, locktime, script)

	ud.Added.Put(uk, uv)
}

// Delete removes a UTXO from the set
// If the UTXO exists in Added, it's removed from there
// Otherwise, it's added to Removed
func (ud *UTXODiff) Delete(txID chainhash.Hash, index uint32) {
	uk := NewUTXOKey(txID, index)

	if ud.Added.Exists(uk) {
		ud.Added.Delete(uk)
	} else {
		ud.Removed.Put(uk, nil)
	}
}

// ProcessTx processes a transaction, updating the diff accordingly
// For non-coinbase transactions:
// - Marks inputs as spent (adds to Removed)
// - Adds outputs as new UTXOs (adds to Added)
func (ud *UTXODiff) ProcessTx(tx *bt.Tx) {
	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			ud.Delete(*input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)
		}
	}

	for i, output := range tx.Outputs {
		if output.LockingScript.IsData() {
			continue
		}

		ud.Add(*tx.TxIDChainHash(), uint32(i), output.Satoshis, tx.LockTime, *output.LockingScript)
	}
}

func NewUTXODiffFromReader(logger ulogger.Logger, r io.Reader) (*UTXODiff, error) {
	blockHash := new(chainhash.Hash)

	if _, err := io.ReadFull(r, blockHash[:]); err != nil {
		return nil, errors.NewStorageError("error reading block hash", err)
	}

	ud := NewUTXODiff(logger, blockHash)

	if err := ud.Removed.Read(r); err != nil {
		return nil, err
	}

	if err := ud.Added.Read(r); err != nil {
		return nil, err
	}

	return ud, nil
}

func (ud *UTXODiff) Write(w io.Writer) error {
	if _, err := w.Write(ud.BlockHash[:]); err != nil {
		return errors.NewProcessingError("error writing block hash", err)
	}

	if err := ud.Removed.Write(w); err != nil {
		return errors.NewProcessingError("error writing removed UTXOs", err)
	}

	if err := ud.Added.Write(w); err != nil {
		return errors.NewProcessingError("error writing added UTXOs", err)
	}

	return nil
}

/* Trim removes any UTXOs that are in both the Added and Removed maps. */
/* This can occur when processing multiple subtrees in parallel. */
func (ud *UTXODiff) Trim() {
	var keysToDelete []UTXOKey

	ud.Added.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
		if ud.Removed.Exists(uk) {
			keysToDelete = append(keysToDelete, uk)
		}

		return false
	})

	for _, uk := range keysToDelete {
		ud.Added.Delete(uk)
		ud.Removed.Delete(uk)
	}
}
