package model

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

type BlockHeader struct {
	// Version of the block.  This is not the same as the protocol version.
	Version uint32 `json:"version"` // When the block header bytes are built, this will be represented as 4 bytes in little endian.

	// Hash of the previous block header in the blockchain.
	HashPrevBlock *chainhash.Hash `json:"hash_prev_block"`

	// Merkle tree reference to hash of all transactions for the block.
	HashMerkleRoot *chainhash.Hash `json:"hash_merkle_root"`

	// Time the block was created im unix time.
	Timestamp uint32 `json:"timestamp"` // When the block header bytes are built, this will be represented as 4 bytes in little endian.

	// Difficulty target for the block.
	Bits NBit `json:"bits"` // This is the target threshold in little endian - this is the way it is store in a bitcoin block.

	// Nonce used to generate the block.
	Nonce uint32 `json:"nonce"` // When the block header bytes are built, this will be represented as 4 bytes in little endian.

}

var (
	// genesis block
	/*
		010000000
		000000000000000000000000000000000000000000000000000000000000000
		3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4
		a29ab5f49
		ffff001d
		1dac2b7c
		01
		01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000
	*/
	previousBlock = &chainhash.Hash{}
	merkleRoot, _ = chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
	bits, _       = NewNBitFromString("1d00ffff")

	BlockHeaderSize    = 80
	GenesisBlockHeader = &BlockHeader{
		Version:        1,
		Timestamp:      1231006505,
		Nonce:          2083236893,
		HashPrevBlock:  previousBlock,
		HashMerkleRoot: merkleRoot,
		Bits:           *bits,
	}

	GenesisBlockHeaderMeta = &BlockHeaderMeta{
		Height:      0,
		TxCount:     1,
		SizeInBytes: 285,
		Miner:       "The Times 03/Jan/2009 Chancellor on brink of second bailout for banks",
	}
)

func NewBlockHeaderFromBytes(headerBytes []byte) (*BlockHeader, error) {
	if len(headerBytes) != BlockHeaderSize {
		return nil, errors.NewProcessingError("block header should be 80 bytes long")
	}

	hashPrevBlock, err := chainhash.NewHash(headerBytes[4:36])
	if err != nil {
		return nil, errors.NewProcessingError("error creating previous block hash from bytes", err)
	}

	hashMerkleRoot, err := chainhash.NewHash(headerBytes[36:68])
	if err != nil {
		return nil, errors.NewProcessingError("error creating merkle root hash from bytes", err)
	}

	NBit, _ := NewNBitFromSlice(headerBytes[72:76])
	bh := &BlockHeader{
		Version:        binary.LittleEndian.Uint32(headerBytes[:4]),
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      binary.LittleEndian.Uint32(headerBytes[68:72]),
		Bits:           *NBit,
		Nonce:          binary.LittleEndian.Uint32(headerBytes[76:]),
	}

	return bh, nil
}

func NewBlockHeaderFromString(headerHex string) (*BlockHeader, error) {
	headerBytes, err := hex.DecodeString(headerHex)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding hex string to bytes", err)
	}

	return NewBlockHeaderFromBytes(headerBytes)
}

// NewBlockHeaderFromJSON Helper function to create a BlockHeader from a json string that we get from RPC
func NewBlockHeaderFromJSON(jsonString string, coinbaseTx ...*bt.Tx) (*BlockHeader, error) {
	var data map[string]interface{}

	if err := json.Unmarshal([]byte(jsonString), &data); err != nil {
		return nil, errors.NewProcessingError("error unmarshalling json", err)
	}

	version, ok := data["version"].(float64)
	if !ok {
		return nil, errors.NewProcessingError("error parsing version from json")
	}

	previousBlockHash, ok := data["previousblockhash"].(string)
	if !ok {
		previousBlockHash, ok = data["prevhash"].(string)
		if !ok {
			return nil, errors.NewProcessingError("error parsing previous block hash from json")
		}
	}

	hashPrevBlock, err := chainhash.NewHashFromStr(previousBlockHash)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding previous block hash from json", err)
	}

	var hashMerkleRoot []byte

	merkleRootStr, ok := data["merkleroot"].(string)
	if ok {
		hash, err := chainhash.NewHashFromStr(merkleRootStr)
		if err != nil {
			return nil, errors.NewProcessingError("error decoding merkle root hash from json", err)
		}

		hashMerkleRoot = hash.CloneBytes()
	} else {
		if len(coinbaseTx) == 0 {
			return nil, errors.NewProcessingError("error building merkle root hash from coinbase tx")
		}
		// try merkleProofs and build a proper merkle root hash from them
		hashMerkleRoot, err = buildMerkleRootHashFromProofs(data, coinbaseTx[0])
		if err != nil {
			return nil, errors.NewProcessingError("error building merkle root hash from merkleProofs", err)
		}
	}

	timestamp, ok := data["time"].(float64)
	if !ok {
		return nil, errors.NewProcessingError("error parsing timestamp from json")
	}

	bitsStr, ok := data["bits"].(string)
	if !ok {
		bitsStr, ok = data["nBits"].(string)
		if !ok {
			return nil, errors.NewProcessingError("error parsing bits from json")
		}
	}

	bits, err := NewNBitFromString(bitsStr)
	if err != nil {
		return nil, errors.NewProcessingError("error parsing bits from json", err)
	}

	nonce, ok := data["nonce"].(float64)
	if !ok {
		nonce = 0 // if nonce is not present, use zero
	}

	bh := &BlockHeader{
		Version:        uint32(version),
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: (*chainhash.Hash)(hashMerkleRoot),
		Timestamp:      uint32(timestamp),
		Bits:           *bits,
		Nonce:          uint32(nonce),
	}

	return bh, nil
}

func NewBlockHeaderFromMiningCandidate(mc *MiningCandidate, coinbaseTx ...*bt.Tx) (*BlockHeader, error) {
	version := mc.Version

	hashPrevBlock := mc.PreviousHash

	hashMerkleRoot := util.BuildMerkleRootFromCoinbase(coinbaseTx[0].TxIDChainHash()[:], mc.MerkleProof)

	timestamp := mc.Time

	bits, err := NewNBitFromSlice(mc.NBits)
	if err != nil {
		return nil, errors.NewProcessingError("error parsing bits from mining candidate", err)
	}

	bh := &BlockHeader{
		Version:        version,
		HashPrevBlock:  (*chainhash.Hash)(hashPrevBlock),
		HashMerkleRoot: (*chainhash.Hash)(hashMerkleRoot),
		Timestamp:      timestamp,
		Bits:           *bits,
	}

	return bh, nil
}

func buildMerkleRootHashFromProofs(data map[string]interface{}, coinbaseTx *bt.Tx) ([]byte, error) {
	proofs, ok := data["merkleProof"].([]interface{})
	if !ok {
		return nil, errors.NewProcessingError("error parsing merkle proofs from json")
	}

	hashes := make([][]byte, len(proofs))

	for i, proof := range proofs {
		p, ok := proof.(string)
		if !ok {
			return nil, errors.NewProcessingError("error parsing merkle proof from json")
		}

		hash, err := chainhash.NewHashFromStr(p)
		if err != nil {
			return nil, errors.NewProcessingError("error decoding proof hash from json", err)
		}

		hashes[i] = hash.CloneBytes()
	}

	return util.BuildMerkleRootFromCoinbase(coinbaseTx.TxIDChainHash()[:], hashes), nil
}

func (bh *BlockHeader) Hash() *chainhash.Hash {
	hash := chainhash.DoubleHashH(bh.Bytes())
	return &hash
}

func (bh *BlockHeader) String() string {
	return bh.Hash().String()
}

func (bh *BlockHeader) StringDump() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Version: %d\n", bh.Version))
	sb.WriteString(fmt.Sprintf("HashPrevBlock: %s\n", bh.HashPrevBlock.String()))
	sb.WriteString(fmt.Sprintf("HashMerkleRoot: %s\n", bh.HashMerkleRoot.String()))
	sb.WriteString(fmt.Sprintf("Timestamp: %d\n", bh.Timestamp))
	sb.WriteString(fmt.Sprintf("Bits: %s\n", bh.Bits.String()))
	sb.WriteString(fmt.Sprintf("Nonce: %d\n", bh.Nonce))

	return sb.String()
}

func (bh *BlockHeader) ToWireBlockHeader() *wire.BlockHeader {
	bitsUint32 := binary.LittleEndian.Uint32(bh.Bits.CloneBytes())
	wireBlockHeader := wire.NewBlockHeader(
		int32(bh.Version), // nolint: gosec
		bh.HashPrevBlock,
		bh.HashMerkleRoot,
		bitsUint32,
		bh.Nonce,
	)
	wireBlockHeader.Timestamp = time.Unix(int64(bh.Timestamp), 0)

	return wireBlockHeader
}

func (bh *BlockHeader) HasMetTargetDifficulty() (bool, *chainhash.Hash, error) {
	target := bh.Bits.CalculateTarget()
	hash := bh.Hash()

	var bn = big.NewInt(0)

	bn.SetBytes(bt.ReverseBytes(hash[:]))

	compare := bn.Cmp(target)
	if compare <= 0 {
		// fmt.Printf("BlockHeader SIMON DEBUG SUCCESS: %s\n", bh.StringDump())
		return true, hash, nil
	}

	return false, hash, errors.NewProcessingError("block header does not meet target %d: %032x >? %032x", compare, target.Bytes(), bn.Bytes())
}

func (bh *BlockHeader) Bytes() []byte {
	if bh == nil {
		return nil
	}

	blockHeaderBytes := make([]byte, 0, BlockHeaderSize)

	uint32Bytes := make([]byte, 4)

	binary.LittleEndian.PutUint32(uint32Bytes, bh.Version)
	blockHeaderBytes = append(blockHeaderBytes, uint32Bytes...)

	blockHeaderBytes = append(blockHeaderBytes, bh.HashPrevBlock.CloneBytes()...)
	blockHeaderBytes = append(blockHeaderBytes, bh.HashMerkleRoot.CloneBytes()...)

	binary.LittleEndian.PutUint32(uint32Bytes, bh.Timestamp)
	blockHeaderBytes = append(blockHeaderBytes, uint32Bytes...)

	blockHeaderBytes = append(blockHeaderBytes, bh.Bits.CloneBytes()...)

	binary.LittleEndian.PutUint32(uint32Bytes, bh.Nonce)
	blockHeaderBytes = append(blockHeaderBytes, uint32Bytes...)

	return blockHeaderBytes
}
