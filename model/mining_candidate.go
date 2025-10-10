package model

import (
	"crypto/rand"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
)

// CreateCoinbaseTxCandidate creates a coinbase transaction for the mining candidate
// p2pk is an optional parameter to specify if the coinbase output should be a pay to public key
// instead of a pay to public key hash
func (mc *MiningCandidate) CreateCoinbaseTxCandidate(tSettings *settings.Settings, p2pk ...bool) (*bt.Tx, error) {
	// Create a new coinbase transaction
	arbitraryText := tSettings.Coinbase.ArbitraryText

	coinbasePrivKeys := tSettings.BlockAssembly.MinerWalletPrivateKeys

	walletAddresses := make([]string, len(coinbasePrivKeys))

	for i, coinbasePrivKey := range coinbasePrivKeys {
		privateKey, err := primitives.PrivateKeyFromWif(coinbasePrivKey)
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
		if err != nil {
			return nil, errors.NewProcessingError("can't create coinbase address", err)
		}

		walletAddresses[i] = walletAddress.AddressString
	}

	coinbaseTx, err := CreateCoinbase(mc.Height, mc.CoinbaseValue, arbitraryText, walletAddresses)
	if err != nil {
		return nil, err
	}

	if len(p2pk) > 0 && p2pk[0] {
		coinbaseTx.Version = 2

		// reset outputs
		coinbaseTx.Outputs = []*bt.Output{}

		// Add 1 coinbase output as a pay to public key
		privateKey, err := primitives.PrivateKeyFromWif(coinbasePrivKeys[0])
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		lockingScript := &bscript.Script{} // NewFromBytes(append(privateKey.SerialisePubKey(), bscript.OpCHECKSIG))
		_ = lockingScript.AppendPushData(privateKey.PubKey().Compressed())
		_ = lockingScript.AppendOpcodes(bscript.OpCHECKSIG)

		coinbaseTx.AddOutput(&bt.Output{
			Satoshis:      mc.CoinbaseValue,
			LockingScript: lockingScript,
		})
	}

	return coinbaseTx, nil
}

func (mc *MiningCandidate) CreateCoinbaseTxCandidateForAddress(tSettings *settings.Settings, address *string) (*bt.Tx, error) {
	arbitraryText := tSettings.Coinbase.ArbitraryText

	if address == nil {
		return nil, errors.NewConfigurationError("address is required for ")
	}

	coinbaseTx, err := CreateCoinbase(mc.Height, mc.CoinbaseValue, arbitraryText, []string{*address})
	if err != nil {
		return nil, err
	}

	return coinbaseTx, nil
}

func CreateCoinbase(height uint32, coinbaseValue uint64, arbitraryText string, addresses []string) (*bt.Tx, error) {
	a, b, err := GetCoinbaseParts(height, coinbaseValue, arbitraryText, addresses)
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	// The extranonce length is 12 bytes.  We need to add 12 bytes to the coinbase a part
	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	return coinbaseTx, nil
}
