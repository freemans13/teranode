package model

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// UTXOKey represents a unique identifier for a Bitcoin transaction output.
//
// A UTXOKey uniquely identifies any transaction output by combining the transaction
// hash with the output index within that transaction. This follows Bitcoin's standard
// outpoint representation.
//
// Serialization format: 36 bytes total - 32 bytes for transaction hash (little-endian)
// followed by 4 bytes for output index (little-endian).
type UTXOKey struct {
	// TxID is the hash of the transaction that created this output
	TxID chainhash.Hash

	// Index specifies which output within the transaction (zero-indexed)
	Index uint32
}

// NewUTXOKey creates a new UTXOKey with the specified transaction ID and index.
func NewUTXOKey(txID chainhash.Hash, index uint32) UTXOKey {
	return UTXOKey{
		TxID:  txID,
		Index: index,
	}
}

// Hash generates a hash value for the UTXOKey modulo the provided value.
// Used for internal map distribution.
func (k UTXOKey) Hash(mod uint16) uint16 {
	return (uint16(k.TxID[0])<<8 | uint16(k.TxID[1])) % mod
}

// NewUTXOKeyFromBytes creates a new Outpoint from a byte slice. It expects a byte slice of exactly 36 bytes,
// where the first 32 bytes are the transaction ID (little endian) and the last 4 bytes are the index (little endian).
func NewUTXOKeyFromBytes(b []byte) (*UTXOKey, error) {
	if len(b) != 36 {
		return nil, errors.NewProcessingError("invalid outpoint length: expected 36 bytes, got %d", len(b))
	}

	txID, err := chainhash.NewHash(b[:32])
	if err != nil {
		return nil, errors.NewProcessingError("failed to create hash from bytes", err)
	}

	index := binary.LittleEndian.Uint32(b[32:])

	return &UTXOKey{
		TxID:  *txID,
		Index: index,
	}, nil
}

// Bytes returns a byte slice representation of the Outpoint. The first 32 bytes are
// the transaction ID (little endian) and the last 4 bytes are the index (little endian).
func (k *UTXOKey) Bytes() []byte {
	// Write the txid and a varint of the index to a byte slice
	serialized := make([]byte, 36)
	copy(serialized, k.TxID[:])
	binary.LittleEndian.PutUint32(serialized[32:], k.Index)

	return serialized
}

// NewUTXOKeyFromReader creates a UTXOKey by reading from an io.Reader.
func NewUTXOKeyFromReader(r io.Reader) (*UTXOKey, error) {
	o := new(UTXOKey)

	if _, err := io.ReadFull(r, o.TxID[:]); err != nil {
		return nil, errors.NewProcessingError("error reading txid", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &o.Index); err != nil {
		return nil, errors.NewProcessingError("error reading index", err)
	}

	return o, nil
}

// Write writes the UTXOKey to an io.Writer.
func (k *UTXOKey) Write(w io.Writer) error {
	var n int

	var err error

	if n, err = w.Write(k.TxID[:]); err != nil {
		return errors.NewProcessingError("error writing txid", err)
	}

	if n != 32 {
		return errors.NewProcessingError("invalid txid length", n)
	}

	if err := binary.Write(w, binary.LittleEndian, k.Index); err != nil {
		return errors.NewProcessingError("error writing index", err)
	}

	return nil
}

// String returns a string representation of the Outpoint, formatted as "txid:index". In this case,
// the txid is the big-endian representation of the transaction ID in hex format (64 characters).
func (k *UTXOKey) String() string {
	return fmt.Sprintf("%v:%8d", k.TxID, k.Index)
}

// Equal compares this UTXOKey with another for equality.
func (k *UTXOKey) Equal(other UTXOKey) bool {
	return k.TxID.IsEqual(&other.TxID) && k.Index == other.Index
}
