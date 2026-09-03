// Package utxoset is a delete-on-spend UTXO-set store.
//
// It inverts what the postgres store indexes. That store records a spend as the
// PRESENCE of a row in an append-only `spends` table and detects a double-spend as a
// unique-constraint violation, which means the indexed structure is the set of
// everything ever spent — measured at 392,884,647 rows and growing with every block.
// This store indexes only the live UNSPENT set — measured at 74,426,411 and roughly
// flat — and detects a double-spend by ABSENCE. A spend becomes a DELETE.
//
// That single DELETE does four jobs at once: it arbitrates the double-spend (zero rows
// affected means the outpoint is already gone), it is the decorate fetch (RETURNING
// hands back satoshis and locking script, so PreviousOutputsDecorate never parses a
// raw transaction), it is the reclaim (no sweep, no pruner, no pending_deletes), and it
// is the write. The DAH sweep, pruner, spent_bits fold, reconciler and stagnation
// monitor do not exist here — on the mainnet box those measured at 76.7% of all disk
// reads, 52% of statement WAL and 25-30% of CPU.
//
// "No background job that can fall behind" is NOT true, and the qualifier matters.
// Autovacuum is that job: this store makes roughly 33,000 dead tuples per block forever,
// each pass is a full scan of a partition index, and it is unmeasured at scale. What
// delete-on-spend removes is the RECLAIM backlog, whose watermark on the old store sat
// 5,567 to 6,227 blocks behind the tip. It does not remove vacuum, and the autovacuum
// threshold below is sized deliberately for that reason.
//
// Measured evidence behind the design (see docs/superpowers/specs/):
//   - index bloat plateaus at exactly 2.00x its bulk-build floor and fully reverses via
//     REINDEX CONCURRENTLY (3.1 s per 10M entries)
//   - a packed 16-byte key costs 63.07 B per index entry at churn equilibrium, against
//     81.3 B for a bytea32 key
//
// Two claims that used to sit here have been REMOVED rather than softened, because
// neither has a source anywhere in this repository:
//
//   - "UTXO index 8.4 GB at the projected tip, vs a 25 GB budget". That figure is the
//     post-REINDEX floor at an assumed UTXO count, not a steady state, and the count it
//     assumed is the least certain input in the whole design. The honest position is that
//     nobody has run gettxoutsetinfo on a synced SV Node, so the tip index size is unknown.
//   - "19x less WAL per spend and 23x fewer full-page images than append-only".
//     Reconstructed from the underlying figures the best case is nearer 11.5x and 12.5x,
//     and the ratio INVERTS at high UTXO counts, because delete-on-spend does not avoid
//     the index write, it defers it to vacuum. Quote no ratio here until one is measured.
//
// M1 scope: the UTXO table, the spend journal and the block/chunk ledger. The tx_meta
// window arrives in M3.
//
// The journal is ALWAYS ON, and that is the one decision here worth stating twice. It
// reads as reorg insurance, which would make it dead weight below the hardcoded
// checkpoint where a reorg is impossible by rule. It is also the prune engine, and in
// that role it is load-bearing from genesis. See the spend_journal DDL comment below.
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

// Pack builds the UTXO table's key: the first 12 bytes of the txid followed by the vout as
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

// schemaSQL is the M1 schema: the UTXO table, the spend journal, and the ledger that makes
// block application idempotent. tx_meta is not here yet.
const schemaSQL = `
-- ---------------------------------------------------------------------------
-- THE UTXO TABLE. One row per spendable output. Inserted once, deleted once.
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
-- The UTXO table's key is non-unique, so there is no ON CONFLICT (txid, vout) to make
-- create idempotent. A block arriving twice, arriving out of order, or being
-- re-applied after a crash must therefore be rejected by ground truth: a durable
-- record written in the SAME transaction as the work it describes. This is what
-- keeps invariant 5 intact — nothing is authorised by a counter.
-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- THE SPEND JOURNAL. svnode's rev*.dat, in a table.
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
-- reconstruct the UTXO row byte-for-byte, including hash_override, because
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
--
-- IT HAS NO OFF-SWITCH, and the reason is not the one above.
--
-- As reorg insurance alone it would be pure overhead below the hardcoded checkpoint,
-- where a reorg is impossible by rule -- and it was switched off there, for exactly
-- that reason. But the journal is also the PRUNE ENGINE. A spend deletes one coin row
-- and signals nothing, so "that transaction's last output has now gone" is a fact about
-- absence that is recorded nowhere else. The journal records it for free: every spend
-- writes a row, rows are grouped by height, and a retiring partition therefore IS the
-- list of transactions to re-examine. Nothing else in the store can produce that list
-- without a counter on every spend or a scan that races.
--
-- With it off, nothing can be reclaimed for the entire initial sync. Mainnet's highest
-- checkpoint is 945,000, about 6.88 billion transactions are mined below it, and the
-- unreclaimable residue would be 165 to 444 GB on an 875 GB disk. Measured cost of
-- leaving it on: 354.8 bytes of WAL and 12.9 microseconds per spend, about 6% of the
-- per-block budget in the worst band. Not close.
-- ---------------------------------------------------------------------------
-- applied records HOW the spend was written, and it is the one column here that describes
-- the writer rather than the coin. TRUE means the spend was recorded by the block path
-- below the hardcoded checkpoint, which is the only path allowed to skip the
-- previous-output comparison (the outpoint-only option the validator refuses above the
-- checkpoint). A block there cannot be un-mined by rule, so a marked spend says its
-- spender is in a main-chain block that will never be taken back, and the reclaimer can
-- retire the parent without asking the identity table about the spender at all. That
-- matters twice over: it removes the two random heap probes that were 85 percent of a
-- reclaim batch, and it stops a spender that has no identity row (never stored, or already
-- reclaimed) from stranding its parents forever. A mempool spend at the tip is written
-- FALSE and takes the full three-probe path. Immutable once written, like every other
-- column in this row; nothing restores it because a restore deletes the row.
CREATE TABLE IF NOT EXISTS spend_journal (
    spent_height    INTEGER  NOT NULL,
    satoshis        BIGINT   NOT NULL,
    created_height  INTEGER  NOT NULL,
    spendable_from  INTEGER  NOT NULL,
    flags           SMALLINT NOT NULL,
    ukey            UUID     NOT NULL,
    txid            BYTEA    NOT NULL,
    spending_txid   BYTEA    NOT NULL,
    script          BYTEA    NOT NULL,
    hash_override   BYTEA,
    applied         BOOLEAN  NOT NULL DEFAULT false
) PARTITION BY RANGE (spent_height);

-- Existing databases predate the column. A constant default is a catalog-only change on
-- PostgreSQL 11 and later, so this is instant on a table of any size and propagates to every
-- attached partition. Rows written before the column read FALSE and take the full path.
ALTER TABLE spend_journal ADD COLUMN IF NOT EXISTS applied BOOLEAN NOT NULL DEFAULT false;

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

-- ---------------------------------------------------------------------------
-- THE IDENTITY TABLE. One row per transaction, from first sight to reclaim.
--
-- Partitioned BY LIST (leaf), the same eight-way split as utxo, and never by
-- created_height. Three reasons, worst first:
--
--   1. created_height is NOT a property of the transaction. A mempool create writes
--      tip+1, a guess; a block-application create of the SAME transaction writes the
--      block's height. One block of mempool residency makes them differ, so a key
--      including created_height admits the ordinary mempool-then-mined duplicate --
--      which is the "two outputs became four" failure this table exists to stop.
--   2. Fifteen Store methods take a txid and no height at all, and two of the
--      highest-volume ones have no height field in their argument type, so they could
--      not supply one even after an interface change.
--   3. Height only ever bought partition-drop reclaim, and a partition holds
--      transactions whose coins are still unspent, so it can never fire.
--
-- leaf is a REAL stored column. PARTITION BY LIST ((get_byte(txid,0) & 7)) is accepted
-- but then postgres bans every unique constraint on the table, and a GENERATED column
-- cannot be a partition key -- verified on 17.11 and 18.6, including 18's new VIRTUAL
-- generated columns.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tx_ident (
    leaf                 SMALLINT NOT NULL,   -- txid[0] & 7; the partition key
    txid                 BYTEA    NOT NULL,   -- full 32 bytes: THE identity
    created_height       INTEGER  NOT NULL,   -- first sight; immutable
    membership           BYTEA,               -- packed 12-byte triples: blockID, height, subtreeIdx
    off_chain_since      INTEGER,             -- see the comment on the index below
    fee                  BIGINT,
    size_in_bytes        INTEGER,
    tx_inpoints          BYTEA,
    locktime             INTEGER,
    created_at           BIGINT,
    conflicting_children BYTEA,
    flags                SMALLINT NOT NULL DEFAULT 0,

    -- LOAD-BEARING, and not defensive tidiness: this IS the global uniqueness rule.
    --
    -- Postgres enforces PRIMARY KEY (leaf, txid) only WITHIN a partition. Verified on
    -- 17.11 and 18.6: the same txid under leaf 0 and leaf 1 is ACCEPTED, two rows, and
    -- ON CONFLICT (leaf, txid) DO NOTHING reports the second as a fresh insert. Seven of
    -- the eight wrong values are in range, so no mistake fails safe.
    --
    -- The length test must come first and share the AND. get_byte on an empty bytea
    -- RAISES, where LeafFor returns 0 for an empty txid; the short-circuit turns that
    -- divergence into a clean constraint violation instead of an error from inside the
    -- expression.
    CONSTRAINT tx_ident_ck CHECK (length(txid) = 32 AND leaf = (get_byte(txid, 0) & 7)),
    CONSTRAINT tx_ident_membership_triples
        CHECK (membership IS NULL OR length(membership) % 12 = 0),
    PRIMARY KEY (leaf, txid)
) PARTITION BY LIST (leaf);

-- ---------------------------------------------------------------------------
-- THE BODY. Serialized transaction bytes, and nothing else.
--
-- This is the ONE part of a transaction whose life is bounded by a horizon rather
-- than by its coins. Everything on tx_ident is pinned while any output is unspent,
-- at any age, because the validator reads the parent's block ids and heights for
-- every input it spends. Keeping the bytes there too would make the transaction
-- archive permanent for that whole population: measured at 136 GB of out-of-line
-- storage today against roughly 95 GB free on the mainnet box, and past the disk
-- at the projected tip.
--
-- RANGE partitioned on created_height so reclaim is dropping a file. Measured on
-- 18.6: a partition drop returned 883 MB to the operating system for 228 KB of
-- crash-recovery journal, where the equivalent delete plus vacuum returned 128 KB
-- for 1.75 GB.
--
-- Uniqueness here is LOCAL to a partition, and that is fine rather than a
-- compromise: tx_ident already guarantees the txid appears once globally, and the
-- claim gates this insert, so two rows for one txid cannot be written.
--
-- NO DEFAULT PARTITION, ever. On 18.6 a default partition makes
-- ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY impossible on every other
-- partition of the table, and the concurrent form is what keeps a drop from
-- blocking readers.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tx_body (
    created_height  INTEGER NOT NULL,
    txid            BYTEA   NOT NULL,
    raw_tx          BYTEA,
    PRIMARY KEY (created_height, txid)
) PARTITION BY RANGE (created_height);
`

// partitionSQL is applied per leaf.
//
// fillfactor is 90, not 100. The only routine UPDATE is the two-phase-commit SetLocked
// release; at fillfactor 100 that becomes a non-HOT update which must write into the
// UTXO index. The tuple never changes size (flags and spendable_from are fixed
// width), so page slack keeps it HOT with zero index writes. This costs disk, not index
// budget. The box has never run block assembly with a mempool, so there is no local
// evidence either way — this is measurement M4.
const partitionSQL = `
CREATE TABLE IF NOT EXISTS utxo_p%[1]d PARTITION OF utxo FOR VALUES IN (%[1]d)
  WITH (fillfactor = 90,
        autovacuum_vacuum_scale_factor  = 0,
        -- Sized to the index, not pinned to a round number. This store makes roughly
        -- 33,000 dead tuples per block forever, so autovacuum is the one background job
        -- it does have, and unlike a pruner it CAN fall behind. Each pass is a full scan
        -- of this partition's index. At fat-band density 200,000 fires every 19 blocks,
        -- about 1.13 GB/block of index scanning, capping the node near 2.65 blk/s on that
        -- term alone; 1,000,000 gives ~0.23 GB/block for ~1.6 GB of resident dead rows.
        -- scale_factor stays 0 because the dead-row rate is proportional to block
        -- production, not to table size.
        autovacuum_vacuum_threshold     = 1000000,
        autovacuum_vacuum_cost_delay    = 0,
        autovacuum_vacuum_cost_limit    = 10000,
        autovacuum_analyze_scale_factor = 0.02);

-- The ONE index. There is deliberately no index on txid: every by-txid access is a
-- ukey range scan with a full-txid heap recheck. A (leaf, txid) index would cost
-- roughly 385M x 63 B = 24 GB and take the budget past 1.4x the entire allowance. Any query filtering on
-- txid without a ukey range bound is a review failure.
CREATE INDEX IF NOT EXISTS utxo_p%[1]d_ukey ON utxo_p%[1]d (ukey);

CREATE TABLE IF NOT EXISTS tx_ident_l%[1]d PARTITION OF tx_ident FOR VALUES IN (%[1]d);
`

// txIdentIndexSQL is the ONE secondary index tx_ident should ever carry, plus the reducer
// the settled predicate needs.
//
// off_chain_since is a CACHED ANSWER TO A CHAIN QUESTION, not a timer, and it can never be
// derived here. An index only answers questions about columns on the row it indexes, and
// "is any block containing me on the main chain" is not one: the answer changes when a
// block is invalidated and no row changes. This store cannot see the chain at all -- it
// holds a pool, a pushed-in height and journal state -- so the answer has to be written
// down by the four paths that already learned it. NULL means the last thing we were told is
// that a MAIN-CHAIN block contains this transaction.
//
// The index is PARTIAL, covering only the mempool. Measured on PG 18.6 at 43,000,000 rows
// with a 43,100-transaction mempool: 524,288 bytes, which is 0.0122 bytes per table row and
// 0.016% of the primary key beside it. The reload it serves costs 3,484 page fetches with a
// fresh mempool against 978,145 for the sequential scan it would otherwise need.
//
// Do NOT add INCLUDE columns to make it index-only. Measured on 18.6: including
// tx_inpoints refuses any transaction with 82 or more inputs at INSERT time --
// "index row size 2720 exceeds btree version 4 maximum 2704" -- which is transaction
// intake down, not a slow path.
const txIdentIndexSQL = `
CREATE INDEX IF NOT EXISTS tx_ident_off_chain_idx ON tx_ident (off_chain_since)
    WHERE off_chain_since IS NOT NULL;

-- Highest block height named by a packed membership, NULL if there is none.
--
-- The casts MUST be bigint. In postgres 255::int << 24 wraps to -16777216, SILENTLY, so an
-- int4 version returns negative heights and every "<= cutoff" test comes back true, which
-- would settle every transaction in the store. Verified on 18.6.
--
-- The MAXIMUM is what makes the settled predicate sound: it is at least the main-chain
-- height, so if the maximum is 288 deep the main-chain block is too. Taking the first or
-- the most favourable height instead lets a child mined low on a fork and re-mined recently
-- read as stable, which is what the incumbent SQL pruner does.
CREATE OR REPLACE FUNCTION mh_max(m bytea) RETURNS bigint
    LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
  SELECT max( (get_byte(m, i*12+4)::bigint << 24)
            | (get_byte(m, i*12+5)::bigint << 16)
            | (get_byte(m, i*12+6)::bigint <<  8)
            |  get_byte(m, i*12+7)::bigint )
    FROM generate_series(0, octet_length(m)/12 - 1) i
$fn$;

-- mh_strip returns the membership with every triple naming one of ids removed, in insertion
-- order.
--
-- The casts MUST be bigint, for exactly the reason mh_max's must. In PostgreSQL 255::int << 24
-- wraps to a negative number, silently, so an int4 version would compare a negative value
-- against a positive block id and strip nothing at all.
--
-- EVERY matching triple goes, not the first. One block can be stamped against a transaction
-- twice under different subtree indexes, so a first-match removal would leave it still claiming
-- a block the caller asked it to forget.
--
-- Stripping the last triple yields NULL rather than an empty value, which is what a transaction
-- no block has ever named already carries, so the two spellings of "no block" stay one. STRICT
-- keeps a NULL membership NULL rather than turning it into a value.
CREATE OR REPLACE FUNCTION mh_strip(m bytea, ids bigint[]) RETURNS bytea
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fn$
  SELECT string_agg(substring(m from i*12 + 1 for 12), ''::bytea ORDER BY i)
    FROM generate_series(0, octet_length(m)/12 - 1) i
   WHERE NOT ( ( (get_byte(m, i*12+0)::bigint << 24)
               | (get_byte(m, i*12+1)::bigint << 16)
               | (get_byte(m, i*12+2)::bigint <<  8)
               |  get_byte(m, i*12+3)::bigint ) = ANY (ids) )
$fn$;
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

	// After the partitions: an index on a partitioned parent is created on every existing
	// partition, and postgres adds it to any created later.
	if _, err := pool.Exec(ctx, txIdentIndexSQL); err != nil {
		return err
	}

	return nil
}
