package model

/*
Here is a real example coinbase broken down...

01000000 .............................. Version
01 .................................... Number of inputs
| 00000000000000000000000000000000
| 00000000000000000000000000000000 ...  Previous outpoint TXID
| ffffffff ............................ Previous outpoint index
|
| 43 .................................. Input coinbase count of bytes (4 block height + 12 (extra nonces) + Arbitrary data length)
| |
| | 03 ................................ Bytes in height
| | | bfea07 .......................... Height: 518847
| |
| | 322f53696d6f6e204f72646973682061    (I think the 32 is wrong - we don't need another var int length here.)
| | 6e642053747561727420467265656d61
| | 6e206d61646520746869732068617070
| | 656e2f ............................ /Simon Ordish and Stuart Freeman made this happen/
| | 9a46 .............................. nonce.dat from seed1.hashzilla.io
| | 434790f7dbde ..................     Extranonce 1 (6 bytes)
| | a3430000 .......................... Extranonce 2 (4 bytes)
|
| ffffffff ............................ Sequence

01 .................................... Output count of bytes (1 or 2 if segwit)
| 8a08ac4a00000000 .................... Satoshis (25.04275756 BTC)
| 19 .................................. Size of locking script
| 76a9 ................................ opDUP, opHASH160
| 14 .................................. Length of hash - 20 bytes
| 8bf10d323ac757268eb715e613cb8e8e1d17
| 93aa ................................ Wallet (20 bytes)
| 88ac ................................ opEQUALVERIFY, opCHECKSIG
| 00000000 ............................ Locktime

*/

import (
	"encoding/binary"
	"encoding/hex"
	"log"

	base58 "github.com/bitcoin-sv/go-sdk/compat/base58" //nolint:depguard
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
)

// BuildCoinbase recombines the different parts of the coinbase transaction.
// See https://arxiv.org/pdf/1703.06545.pdf section 2.2 for more info.
func BuildCoinbase(c1 []byte, c2 []byte, extraNonce1 string, extraNonce2 string) []byte {
	e1, _ := hex.DecodeString(extraNonce1)
	e2, _ := hex.DecodeString(extraNonce2)

	a := []byte{}
	a = append(a, c1...)
	a = append(a, e1...)
	a = append(a, e2...)
	a = append(a, c2...)

	return a
}

// GetCoinbaseParts returns the two split coinbase parts from coinbase metadata.
// See https://arxiv.org/pdf/1703.06545.pdf section 2.2 for more info.
func GetCoinbaseParts(height uint32, coinbaseValue uint64, coinbaseText string, walletAddresses []string) (coinbase1 []byte, coinbase2 []byte, err error) {
	coinbase1 = makeCoinbase1(height, coinbaseText)

	ot, err := makeCoinbaseOutputTransactions(coinbaseValue, walletAddresses)
	if err != nil {
		return
	}

	coinbase2 = makeCoinbase2(ot)

	return
}

// func makeCoinbaseInputTransaction(coinbaseData []byte) []byte {
// 	buf := make([]byte, 32)                                 // 32 bytes - All bits are zero: Not a transaction hash reference
// 	buf = append(buf, []byte{0xff, 0xff, 0xff, 0xff}...)    // 4 bytes - All bits are ones: 0xFFFFFFFF
// 	buf = append(buf, VarInt(uint64(len(coinbaseData)))...) // Length of the coinbase data, from 2 to 100 bytes
// 	buf = append(buf, coinbaseData...)                      // Arbitrary data used for extra nonce and mining tags. In v2 blocks; must begin with block height
// 	buf = append(buf, []byte{0xff, 0xff, 0xff, 0xff}...)    //  4 bytes = Set to 0xFFFFFFFF
// 	return buf
// }

// AddressToScript comment
func AddressToScript(address string) (script []byte, err error) {
	decoded, err := base58.Decode(address)
	if err != nil {
		return nil, err
	}

	if len(decoded) != 25 {
		return nil, errors.NewProcessingError("invalid address length for '%s'", address)
	}

	// A P2SH address always begins with a '3', instead of a '1' as in P2PKH addresses.
	// This is because P2SH addresses have a version byte prefix of 0x05, instead of
	// the 0x00 prefix in P2PKH addresses, and these come out as a '3' and '1' after
	// base58check encoding.
	switch decoded[0] {
	case 0x00: // Pubkey hash (P2PKH address)
		fallthrough
	case 0x6f: // Testnet pubkey hash (P2PKH address)
		pubkey := decoded[1 : len(decoded)-4]

		ret := []byte{
			OpDUP,
			OpHASH160,
			0x14,
		}
		ret = append(ret, pubkey...)
		ret = append(ret, OpEQUALVERIFY)
		ret = append(ret, OpCHECKSIG)

		return ret, nil

	case 0x05: // Script hash (P2SH address)
		fallthrough
	case 0xc4: // Testnet script hash (P2SH address)
		redeemScriptHash := decoded[1 : len(decoded)-4]

		ret := []byte{
			OpHASH160,
			0x14,
		}
		ret = append(ret, redeemScriptHash...)
		ret = append(ret, OpEQUAL)

		return ret, nil

	default:
		return nil, errors.NewProcessingError("address %s is not supported", address)
	}
}

func makeCoinbaseOutputTransactions(coinbaseValue uint64, walletAddresses []string) ([]byte, error) {
	numberOfOutputs := uint64(len(walletAddresses))
	if numberOfOutputs == 0 {
		return nil, errors.NewInvalidArgumentError("no wallet addresses provided")
	}

	outputValue := coinbaseValue / numberOfOutputs
	outputChange := coinbaseValue % numberOfOutputs

	buf := make([]byte, 0)

	// Add the number of outputs
	buf = append(buf, VarInt(numberOfOutputs)...)

	for i, walletAddress := range walletAddresses {
		lockingScript, err := AddressToScript(walletAddress)
		if err != nil {
			return nil, err
		}

		// Add each output (the first output may have any rounding error change added to it)
		outputBytes := make([]byte, 8)

		if i == 0 {
			binary.LittleEndian.PutUint64(outputBytes[0:], outputValue+outputChange)
		} else {
			binary.LittleEndian.PutUint64(outputBytes[0:], outputValue)
		}

		outputBytes = append(outputBytes, VarInt(uint64(len(lockingScript)))...)
		outputBytes = append(outputBytes, lockingScript...)

		buf = append(buf, outputBytes...)
	}

	return buf, nil
}

func makeCoinbase1(height uint32, coinbaseText string) []byte {
	spaceForExtraNonce := 12

	blockHeightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockHeightBytes, height) // Block height

	arbitraryData := []byte{}
	arbitraryData = append(arbitraryData, 0x03)
	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
	arbitraryData = append(arbitraryData, []byte(coinbaseText)...)

	// Arbitrary data should leave enough space for the extra nonce
	if len(arbitraryData) > (100 - spaceForExtraNonce) {
		arbitraryData = arbitraryData[:100-spaceForExtraNonce] // Slice the arbitrary text so everything fits in 100 bytes
	}

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 1) // Version

	buf = append(buf, 0x01)                              // Number of input transaction - always one
	buf = append(buf, make([]byte, 32)...)               // Transaction hash - 4 bytes all bits are zero
	buf = append(buf, []byte{0xff, 0xff, 0xff, 0xff}...) // Coinbase data size - 4 bytes - All bits are ones: 0xFFFFFFFF (ffffffff)

	buf = append(buf, VarInt(uint64(len(arbitraryData)+spaceForExtraNonce))...) // nolint: gosec // Length of the coinbase data, from 2 to 100 bytes
	buf = append(buf, arbitraryData...)

	return buf
}

func makeCoinbase2(ot []byte) []byte {
	sq := []byte{0xff, 0xff, 0xff, 0xff}
	lt := make([]byte, 4)

	ot = append(sq, ot...)
	ot = append(ot, lt...)

	return ot
}

// VarInt takes an unsiged integer and  returns a byte array in VarInt format.
// See http://learnmeabitcoin.com/glossary/varint
func VarInt(i uint64) []byte {
	b := make([]byte, 9)
	if i < 0xfd {
		b[0] = byte(i)
		return b[:1]
	}

	if i < 0x10000 {
		iUint16, err := safeconversion.Uint64ToUint16(i)
		if err != nil {
			log.Printf("failed to convert uint64 to uint16: %s", err)
			return nil
		}

		b[0] = 0xfd
		binary.LittleEndian.PutUint16(b[1:3], iUint16)

		return b[:3]
	}

	if i < 0x100000000 {
		iUint32, err := safeconversion.Uint64ToUint32(i)
		if err != nil {
			log.Printf("failed to convert uint64 to uint16: %s", err)
			return nil
		}

		b[0] = 0xfe
		binary.LittleEndian.PutUint32(b[1:5], iUint32)

		return b[:5]
	}

	b[0] = 0xff
	binary.LittleEndian.PutUint64(b[1:9], i)

	return b
}
