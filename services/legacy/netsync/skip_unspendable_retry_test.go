package netsync

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingStore is a real store that counts the reads and stamps createUtxos makes on the
// skipped transactions' account: BatchDecorate is how StoredSubset asks a store without its
// own probe, and SetMinedMulti is the stamp.
type countingStore struct {
	utxo.Store
	mu      sync.Mutex
	lookups int
	stamped []chainhash.Hash
}

func (s *countingStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	s.mu.Lock()
	s.lookups++
	s.mu.Unlock()

	return s.Store.BatchDecorate(ctx, items, f...)
}

func (s *countingStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	s.mu.Lock()
	for _, h := range hashes {
		s.stamped = append(s.stamped, *h)
	}
	s.mu.Unlock()

	return s.Store.SetMinedMulti(ctx, hashes, info)
}

// retryFixture is a below-checkpoint block holding one ordinary transaction and one data-only
// transaction, a SyncManager set to skip the data one, and a real sqlitememory store behind a
// counter.
type retryFixture struct {
	sm           *SyncManager
	store        *countingStore
	normal, data *bt.Tx
	m            *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]
	block        *bsvutil.Block
}

func newRetryFixture(t *testing.T, name string) *retryFixture {
	t.Helper()

	ctx := context.Background()

	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = true

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	sqlStore, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	store := &countingStore{Store: sqlStore}

	normal, data, _ := twoTxMap(t)
	// The SQL store will not take an input without an unlocking script. Setting one changes the
	// txid, so the map is built afterwards.
	normal.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	data.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	m := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](2)
	m.Set(*normal.TxIDChainHash(), &TxMapWrapper{Tx: normal})
	m.Set(*data.TxIDChainHash(), &TxMapWrapper{Tx: data})

	block := bsvutil.NewBlock(&wire.MsgBlock{Header: wire.BlockHeader{Version: 1}})
	block.SetHeight(500)

	sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{}, utxoStore: store}

	return &retryFixture{sm: sm, store: store, normal: normal, data: data, m: m, block: block}
}

// An earlier attempt at a block can fail after storing some of its transactions as unmined,
// and if it got only as far as a data-only transaction, the retry creates the ordinary
// transaction fresh and finds nothing else already stored. The retry skips the data
// transaction, so nothing else would ever mark it mined: it must be marked here.
func TestCreateUtxos_RetryMarksMinedASkippedTransactionAnEarlierAttemptStored(t *testing.T) {
	ctx := context.Background()
	f := newRetryFixture(t, "retry_skipped_only")

	// The failed first attempt stored only the data transaction, unmined.
	_, _, err := f.store.SpendAndCreate(ctx, f.data, 500, utxo.WithCreateOnly())
	require.NoError(t, err)

	pre, err := f.store.Get(ctx, f.data.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Empty(t, pre.BlockIDs, "the earlier attempt left it unmined")

	require.NoError(t, f.sm.createUtxos(ctx, f.m, testBlockIdent(f.block), 7, true))

	post, err := f.store.Get(ctx, f.data.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Contains(t, post.BlockIDs, uint32(7), "the skipped transaction is recorded in the block that mined it")

	normal, err := f.store.Get(ctx, f.normal.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Contains(t, normal.BlockIDs, uint32(7), "the ordinary transaction was created by the retry")
}

// A first attempt finds no skipped transaction stored. It pays one batched read, stamps
// nothing on the skipped transaction's account, and does not store the skipped transaction.
func TestCreateUtxos_FirstAttemptOnlyAsksAboutSkippedTransactions(t *testing.T) {
	ctx := context.Background()
	f := newRetryFixture(t, "first_attempt")

	require.NoError(t, f.sm.createUtxos(ctx, f.m, testBlockIdent(f.block), 7, true))

	require.Equal(t, 1, f.store.lookups, "one batched read over the skipped transactions")
	require.Empty(t, f.store.stamped, "nothing was stored before, so nothing is stamped")

	_, err := f.store.Get(ctx, f.data.TxIDChainHash(), fields.BlockIDs)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "the skipped transaction is still not stored, got %v", err)

	normal, err := f.store.Get(ctx, f.normal.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Contains(t, normal.BlockIDs, uint32(7))
}
