// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tx, _         = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash1, _      = chainhash.NewHashFromStr("5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a")
	hash2, _      = chainhash.NewHashFromStr("b067b2d2a51cb3f63678cc2bf12efaa5d57235d296bcba09ead42f4147b63bf7")
	hash3, _      = chainhash.NewHashFromStr("0ab59604a1c249d0cbfe18f01fe423df3035840f9a609395ccd177d2b217cae6")
	hash4, _      = chainhash.NewHashFromStr("08c3d6e8388415d8f6190a40c0acb9328b41a89a5854468e62c2bbd1dc740460")
	hash5, _      = chainhash.NewHashFromStr("72629cff00e9f33dc7a96976717b7c86d4d168252c3550d3f24ae9f7bbe5cc68")
	utxoHashesMap = map[chainhash.Hash]struct{}{
		*hash1: {},
		*hash2: {},
		*hash3: {},
		*hash4: {},
		*hash5: {},
	}
)

func TestGetFeesAndUtxoHashes(t *testing.T) {
	t.Run("should return fees and utxo hashes", func(t *testing.T) {
		fees, utxoHashes, err := GetFeesAndUtxoHashes(context.Background(), tx, chaincfg.GenesisActivationHeight)
		require.NoError(t, err)

		assert.Equal(t, uint64(215), fees)
		assert.Equal(t, 5, len(utxoHashes))

		createdUtxoHashesMap := make(map[chainhash.Hash]struct{})

		for _, utxoHash := range utxoHashes {
			_, ok := utxoHashesMap[*utxoHash]
			assert.True(t, ok, "utxo hash not found in map: "+utxoHash.String())

			createdUtxoHashesMap[*utxoHash] = struct{}{}
		}

		for utxoHash := range utxoHashesMap {
			_, ok := createdUtxoHashesMap[utxoHash]
			assert.True(t, ok, "utxo hash not found in created map: "+utxoHash.String())
		}
	})
}

func TestCalculateUtxoStatus(t *testing.T) {
	// Test case when spendingTxId is not nil
	spendingTxId, _ := chainhash.NewHashFromStr("b067b2d2a51cb3f63678cc2bf12efaa5d57235d296bcba09ead42f4147b63bf7")
	spendingData := spendpkg.NewSpendingData(spendingTxId, 0)

	status := CalculateUtxoStatus(spendingData, 0, 0)
	assert.Equal(t, Status_SPENT, status)

	// Test case when lockTime is greater than 0 and less than 500000000 and greater than blockHeight
	status = CalculateUtxoStatus(nil, 400000000, 300000000)
	assert.Equal(t, Status_IMMATURE, status)

	// Test case when lockTime is greater than or equal to 500000000 and greater than current Unix time
	status = CalculateUtxoStatus(nil, uint32(time.Now().Add(1*time.Hour).Unix()), 0)
	assert.Equal(t, Status_IMMATURE, status)

	// Test case when spendingTxId is nil and lockTime is 0
	status = CalculateUtxoStatus(nil, 0, 0)
	assert.Equal(t, Status_OK, status)
}

func TestGetUtxoHashes(t *testing.T) {
	t.Run("should return utxo hashes", func(t *testing.T) {
		utxoHashes, err := GetUtxoHashes(tx)
		require.NoError(t, err)

		assert.Equal(t, 5, len(utxoHashes))

		createdUtxoHashesMap := make(map[chainhash.Hash]struct{})

		for _, utxoHash := range utxoHashes {
			_, ok := utxoHashesMap[*utxoHash]
			assert.True(t, ok, "utxo hash not found in map: "+utxoHash.String())

			createdUtxoHashesMap[*utxoHash] = struct{}{}
		}

		for utxoHash := range utxoHashesMap {
			_, ok := createdUtxoHashesMap[utxoHash]
			assert.True(t, ok, "utxo hash not found in created map: "+utxoHash.String())
		}
	})
}

func TestShouldStoreNonZeroUTXO(t *testing.T) {
	txID := "956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604"
	tx, err := bt.NewTxFromString("010000000000000000ef015400c3490d91f3f742e73e81bc37dfca4f24f9a73a17c90ccab3012ddbc795bb000000008a473044022006a960f73ea637af867f69ed69edd291bee1d6daec241649caf909fb864dcd3b022011c82189c4a3379aba85fdb907d341db8067e426d7660fbba05c12fa370fa8aa0141048e69627b4807fe4ab00002a01c4a26a50d558cce969708e75dc5bfb345bbe92f06082757c85cbcac4ff0bbb91e221c59d3f9e675125da07e8110fd7d9b0ab6eeffffffff00000000000000001976a9146f9e896bb7cd9d27ca5b18c3ec9587ff0be7895188ac0100000000000000001976a9144477154cba7f0474a578fe734e00bd60513fbab588ac00000000")
	require.NoError(t, err)
	require.Equal(t, txID, tx.TxIDChainHash().String())

	t.Run("should return true for non-zero UTXO", func(t *testing.T) {
		// TODO this should go when we remove the genesis activation height global variable
		chaincfg.GenesisActivationHeight = uint32(620538)
		assert.True(t, ShouldStoreOutputAsUTXO(tx.IsCoinbase(), tx.Outputs[0], chaincfg.GenesisActivationHeight-1))
		assert.True(t, ShouldStoreOutputAsUTXO(tx.IsCoinbase(), tx.Outputs[0], chaincfg.GenesisActivationHeight+1))
	})
}

func BenchmarkGetUtxoHashes(b *testing.B) {
	txs := make([]*bt.Tx, b.N)

	for i := 0; i < b.N; i++ {
		tx := bt.NewTx()
		_ = tx.From(
			"5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a",
			uint32(i),
			"76a914eb0bd5edba389198e73f8efabddfc61666969d1688ac",
			uint64(i),
		)
		txs[i] = tx
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := GetUtxoHashes(txs[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetUtxoHashes_ManyOutputs(b *testing.B) {
	// Create a mock transaction with 1000 outputs
	tx := bt.NewTx()
	for i := 0; i < 1000; i++ {
		_ = tx.From(
			"5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a",
			uint32(i),
			"76a914eb0bd5edba389198e73f8efabddfc61666969d1688ac",
			uint64(i),
		)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := GetUtxoHashes(tx)
		if err != nil {
			b.Fatal(err)
		}
	}
}
