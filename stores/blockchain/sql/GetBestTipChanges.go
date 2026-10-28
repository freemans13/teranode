package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// stampBestTipQuery records a change of the best tip. It gives the current best block the next
// sequence number unless that block already holds the highest one, so it is a no-op when the
// best did not change and safe to run after any write that might have changed it. The next
// number is the column's own maximum plus one: every caller holds slowPathMu, so two writers
// never read the same maximum.
//
// A block that becomes the best tip again takes a new number, which overwrites its old one. A
// reader that missed both changes then sees the branch that won in between, followed by this
// block, which is the order it needs to replay.
const stampBestTipQuery = `
	UPDATE blocks
	SET best_tip_seq = (SELECT COALESCE(MAX(best_tip_seq), 0) + 1 FROM blocks)
	WHERE id = (
		SELECT id FROM blocks WHERE invalid = false
		ORDER BY chain_work DESC, peer_id ASC, id ASC
		LIMIT 1
	)
	AND (best_tip_seq IS NULL OR best_tip_seq < (SELECT MAX(best_tip_seq) FROM blocks))
`

// stampBestTipAfterQuery is stampBestTipQuery for a write that may have removed the row holding
// the highest number: $1 is the maximum read before that write, and the best block is numbered
// above it unless it already holds it.
const stampBestTipAfterQuery = `
	UPDATE blocks
	SET best_tip_seq = $1 + 1
	WHERE id = (
		SELECT id FROM blocks WHERE invalid = false
		ORDER BY chain_work DESC, peer_id ASC, id ASC
		LIMIT 1
	)
	AND (best_tip_seq IS NULL OR best_tip_seq < $1)
`

// stampBestTip runs stampBestTipQuery on exec, which is the transaction that changed the best
// tip, so the change and its number commit together. The caller must hold slowPathMu. The error
// is returned raw, so a caller inside RetryTx can classify it.
func (s *SQL) stampBestTip(ctx context.Context, exec execQuerier) error {
	_, err := exec.ExecContext(ctx, stampBestTipQuery)

	return err
}

// GetBestTipChanges returns, in order, the blocks whose best-tip sequence number is greater than
// afterSeq and whose height is at least minHeight, at most limit of them. Each entry is the block
// that became the best tip at that number. The flags in the metadata are read now, so an entry
// for a block that was invalidated since reports Invalid.
//
// Returns the headers, their metadata and their sequence numbers as three slices of the same
// length. An empty result means nothing changed after afterSeq.
func (s *SQL) GetBestTipChanges(ctx context.Context, afterSeq uint64, minHeight, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, []uint64, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBestTipChanges")
	defer deferFn()

	q := `
		SELECT
		 b.id
		,b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.size_in_bytes
		,b.coinbase_tx
		,b.peer_id
		,b.height
		,b.tx_count
		,b.chain_work
		,b.mined_set
		,b.subtrees_set
		,b.invalid
		,b.inserted_at
		,b.processed_at
		,b.median_time_past
		,b.best_tip_seq
		FROM blocks b
		WHERE b.best_tip_seq > $1 AND b.height >= $2
		ORDER BY b.best_tip_seq ASC
		LIMIT $3
	`

	rows, err := s.db.QueryContext(ctx, q, afterSeq, minHeight, limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil, nil
		}

		return nil, nil, nil, errors.NewStorageError("failed to get best tip changes", err)
	}

	defer rows.Close()

	headers := make([]*model.BlockHeader, 0, preallocFor(uint64(limit)))
	metas := make([]*model.BlockHeaderMeta, 0, preallocFor(uint64(limit)))
	seqs := make([]uint64, 0, preallocFor(uint64(limit)))

	for rows.Next() {
		header := &model.BlockHeader{}
		meta := &model.BlockHeaderMeta{}

		var (
			hashPrevBlock  []byte
			hashMerkleRoot []byte
			nBits          []byte
			coinbaseBytes  []byte
			insertedAt     time.CustomTime
			processedAt    *time.CustomTime
			seq            uint64
		)

		if err = rows.Scan(
			&meta.ID,
			&header.Version,
			&header.Timestamp,
			&nBits,
			&header.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&meta.SizeInBytes,
			&coinbaseBytes,
			&meta.PeerID,
			&meta.Height,
			&meta.TxCount,
			&meta.ChainWork,
			&meta.MinedSet,
			&meta.SubtreesSet,
			&meta.Invalid,
			&insertedAt,
			&processedAt,
			&meta.MedianTimePast,
			&seq,
		); err != nil {
			return nil, nil, nil, errors.NewStorageError("failed to scan best tip change", err)
		}

		bits, bitsErr := model.NewNBitFromSlice(nBits)
		if bitsErr != nil {
			return nil, nil, nil, errors.NewStorageError("error in GetBestTipChanges: malformed n_bits (length %d, expected 4) for block height %d", len(nBits), meta.Height, bitsErr)
		}

		header.Bits = *bits

		if header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock); err != nil {
			return nil, nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		if header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot); err != nil {
			return nil, nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		if processedAt != nil {
			meta.ProcessedAt = &processedAt.Time
		}

		meta.Timestamp = uint32(insertedAt.Unix())

		if len(coinbaseBytes) > 0 {
			if coinbaseTx, txErr := bt.NewTxFromBytes(coinbaseBytes); txErr == nil {
				if miner, minerErr := util.ExtractCoinbaseMinerRaw(coinbaseTx, s.rawMinerTag); minerErr == nil {
					meta.Miner = miner
				}
			}
		}

		headers = append(headers, header)
		metas = append(metas, meta)
		seqs = append(seqs, seq)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, nil, errors.NewStorageError("failed to read best tip changes", err)
	}

	return headers, metas, seqs, nil
}

// GetBestTipSeq returns the highest best-tip sequence number written, or 0 when none has been.
func (s *SQL) GetBestTipSeq(ctx context.Context) (uint64, error) {
	var seq sql.NullInt64

	if err := s.db.QueryRowContext(ctx, `SELECT MAX(best_tip_seq) FROM blocks`).Scan(&seq); err != nil {
		return 0, errors.NewStorageError("failed to get best tip sequence", err)
	}

	if !seq.Valid || seq.Int64 < 0 {
		return 0, nil
	}

	return uint64(seq.Int64), nil
}
