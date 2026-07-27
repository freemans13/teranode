// Package utxoset is a delete-on-spend UTXO-set store.
//
// It inverts what the postgres store indexes. That store records a spend as the
// PRESENCE of a row in an append-only `spends` table and detects a double-spend as a
// unique-constraint violation, which means the indexed structure is the set of
// everything ever spent — measured at 392,884,647 rows and growing with every block.
// This store indexes only the live UNSPENT set — measured at 74,426,411 and roughly
// flat — and detects a double-spend by ABSENCE. A spend becomes a DELETE.
//
// That single DELETE does four jobs at once: it is the double-spend arbiter (zero rows
// affected means the outpoint is already gone), it is the decorate fetch (RETURNING
// hands back satoshis and locking script, so PreviousOutputsDecorate never parses a
// raw transaction), it is the reclaim (no sweep, no pruner, no pending_deletes), and it
// is the write. The DAH sweep, pruner, spent_bits fold, reconciler and stagnation
// monitor do not exist here — on the mainnet box those measured at 76.7% of all disk
// reads, 52% of statement WAL and 25-30% of CPU.
//
// Measured evidence behind the design (see docs/superpowers/specs/):
//   - arbiter index 8.4 GB at the projected tip, vs a 25 GB budget
//   - 19x less WAL per spend and 23x fewer full-page images than append-only
//   - index bloat plateaus at exactly 2.00x floor and fully reverses via
//     REINDEX CONCURRENTLY (3.1 s per 10M entries)
//
// M1 scope: the arbiter and the block/chunk ledger only. Sync mode — below the
// hardcoded checkpoint, where a reorg is impossible by rule, so no undo journal and no
// tx_meta window are needed. Those arrive in M3.
package utxoset

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NumLeaves is the partition count. Deliberately 8, matching the postgres store.
//
// Partitioning is BY LIST on leaf = txid[0] & 7 — derived from the TXID, not from the
// outpoint and not by hashing ukey. Every output of one transaction must co-locate in a
// single leaf, otherwise the BIP30 probe, SetLocked, freeze and reorg restore each fan
// out across all 8 partitions. That fan-out is the 21x by-hash penalty the postgres
// store's invariant 6 exists to prevent.
const NumLeaves = 8

// LeafFor returns the partition a txid routes to. Computed Go-side so both sides share
// one routing function and Go never has to reimplement PostgreSQL's hash_any on bytea.
func LeafFor(txid []byte) int16 {
	if len(txid) == 0 {
		return 0
	}

	return int16(txid[0] & (NumLeaves - 1))
}

// Pack builds the arbiter key: the first 12 bytes of the txid followed by the vout as
// big-endian uint32. Sixteen bytes, stored as a uuid.
//
// PostgreSQL's uuid is typlen 16 / typalign 'c', so it occupies exactly 16 bytes with no
// padding and compares bytewise — which is what lets prefix-first packing turn "every
// output of parent P" into a range scan rather than a separate index.
//
// The key is NON-UNIQUE by design and this is load-bearing. A 96-bit prefix can collide,
// so identity is established by the full 32-byte txid carried on the row and rechecked in
// every predicate; a collision then costs one extra heap visit and never a wrong answer.
// A UNIQUE 96-bit key would be a consensus bug: 2^48 of attacker work to make a
// legitimate output fail to create.
func Pack(txid []byte, vout uint32) [16]byte {
	var k [16]byte

	copy(k[:12], txid)
	k[12] = byte(vout >> 24)
	k[13] = byte(vout >> 16)
	k[14] = byte(vout >> 8)
	k[15] = byte(vout)

	return k
}

// Flag bits on utxo.flags.
const (
	FlagFrozen      int16 = 1 << 0
	FlagLocked      int16 = 1 << 1
	FlagConflicting int16 = 1 << 2
	FlagCoinbase    int16 = 1 << 3
)

// schemaSQL is the M1 schema: the arbiter plus the ledger that makes block application
// idempotent. Deliberately excludes the undo journal and tx_meta — below the checkpoint a
// reorg is impossible by rule, so neither is reachable.
const schemaSQL = `
-- ---------------------------------------------------------------------------
-- THE ARBITER. One row per spendable output. Inserted once, deleted once.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS utxo (
    satoshis        BIGINT   NOT NULL,
    created_height  INTEGER  NOT NULL,   -- BIP68 relative locktime, utxoHeights
    spendable_from  INTEGER  NOT NULL,   -- coinbase maturity and ReAssignUTXO delay
    leaf            SMALLINT NOT NULL,   -- txid[0] & 7; the partition key
    flags           SMALLINT NOT NULL DEFAULT 0,
    ukey            UUID     NOT NULL,   -- pack(txid, vout). NON-UNIQUE by design.
    txid            BYTEA    NOT NULL,   -- full 32 bytes: the authorising identity
    script          BYTEA    NOT NULL,   -- locking script; serves decorate directly
    hash_override   BYTEA                -- NULL except after ReAssignUTXO
) PARTITION BY LIST (leaf);

-- ---------------------------------------------------------------------------
-- THE LEDGER. Replay safety.
--
-- The arbiter key is non-unique, so there is no ON CONFLICT (txid, vout) to make
-- create idempotent. A block arriving twice, arriving out of order, or being
-- re-applied after a crash must therefore be rejected by ground truth: a durable
-- record written in the SAME transaction as the work it describes. This is what
-- keeps invariant 5 intact — nothing is authorised by a counter.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- THE UNDO JOURNAL. svnode's rev*.dat, in a table.
--
-- A delete-on-spend store destroys the row, so a reorg or a conflict resolution has
-- nothing to restore FROM unless the payload is captured at the moment it is deleted.
-- Re-deriving it from the block is not an option: this node retains 696 KB of blocks
-- against 2,838 GB of chain, and the subtree data it does keep is not in extended
-- format, so it records WHICH outpoints a block consumed and none of their satoshis or
-- scripts.
--
-- The journal row is therefore written in the SAME STATEMENT as the delete, not merely
-- the same transaction -- see spendJournalSQL. It carries every field needed to
-- reconstruct the arbiter row byte-for-byte, including hash_override, because
-- ReAssignUTXO splices an operator-supplied utxo hash that is NOT derivable from
-- (txid, vout, satoshis, script); recomputing it on restore would silently reverse a
-- court-ordered reassignment.
--
-- spending_txid is an ownership token, deliberately NOT indexed: a restore must match
-- the spender that actually took the coin, so a stale reorg record whose output has
-- since been re-spent by a different transaction matches nothing and is a no-op rather
-- than resurrecting a coin someone else now owns.
--
-- RANGE partitioned by spent_height so reclaim is DROP TABLE -- O(1), no scan, no
-- vacuum, no background job that can fall behind. Age clusters by insert time by
-- definition, which is why partition-drop works here and did not for the rejected
-- epoch-slab design (there, garbage clustered by SPEND time, which is decorrelated
-- from creation).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS utxo_undo (
    spent_height    INTEGER  NOT NULL,
    satoshis        BIGINT   NOT NULL,
    created_height  INTEGER  NOT NULL,
    spendable_from  INTEGER  NOT NULL,
    flags           SMALLINT NOT NULL,
    ukey            UUID     NOT NULL,
    txid            BYTEA    NOT NULL,
    spending_txid   BYTEA    NOT NULL,
    script          BYTEA    NOT NULL,
    hash_override   BYTEA
) PARTITION BY RANGE (spent_height);

CREATE TABLE IF NOT EXISTS applied_block (
    height       INTEGER NOT NULL,
    block_hash   BYTEA   NOT NULL,
    chunk_size   INTEGER NOT NULL,   -- re-offering a block with a different chunk_size
                                     -- must fail loudly, never silently re-cut
    chunk_count  INTEGER NOT NULL,
    completed    BOOLEAN NOT NULL DEFAULT FALSE,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (block_hash)
);
CREATE INDEX IF NOT EXISTS applied_block_height ON applied_block (height);

CREATE TABLE IF NOT EXISTS applied_chunk (
    block_hash   BYTEA   NOT NULL,
    chunk_idx    INTEGER NOT NULL,
    PRIMARY KEY (block_hash, chunk_idx)
);
`

// partitionSQL is applied per leaf.
//
// fillfactor is 90, not 100. The only routine UPDATE is the two-phase-commit SetLocked
// release; at fillfactor 100 that becomes a non-HOT update which must write into the
// arbiter index. The tuple never changes size (flags and spendable_from are fixed
// width), so page slack keeps it HOT with zero index writes. This costs disk, not index
// budget. The box has never run block assembly with a mempool, so there is no local
// evidence either way — this is measurement M4.
const partitionSQL = `
CREATE TABLE IF NOT EXISTS utxo_p%[1]d PARTITION OF utxo FOR VALUES IN (%[1]d)
  WITH (fillfactor = 90,
        autovacuum_vacuum_scale_factor  = 0,
        autovacuum_vacuum_threshold     = 200000,
        autovacuum_vacuum_cost_delay    = 0,
        autovacuum_vacuum_cost_limit    = 10000,
        autovacuum_analyze_scale_factor = 0.02);

-- The ONE index. There is deliberately no index on txid: every by-txid access is a
-- ukey range scan with a full-txid heap recheck. A (leaf, txid) index would cost
-- roughly 385M x 63 B = 24 GB and take the budget past 1.4x the entire allowance. Any query filtering on
-- txid without a ukey range bound is a review failure.
CREATE INDEX IF NOT EXISTS utxo_p%[1]d_ukey ON utxo_p%[1]d (ukey);
`

// CreateSchema installs the M1 schema. Idempotent.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return err
	}

	for i := 0; i < NumLeaves; i++ {
		if _, err := pool.Exec(ctx, fmt.Sprintf(partitionSQL, i)); err != nil {
			return err
		}
	}

	return nil
}
