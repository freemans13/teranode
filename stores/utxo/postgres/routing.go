package postgres

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Two-layer routing for the UTXO store. The Shard field is the key
// abstraction: it picks which pgxpool (and ultimately which postgres
// server) handles the item. Today NumShards=1 so it's a no-op at runtime,
// but the dispatch shape is shard-ready: when the deployment grows to N
// shards we just instantiate N pgxpools and the same Route() return drives
// dispatch.
//
// The Partition field is informational only — it is NOT used for client
// dispatch. Postgres prunes partitions itself via the schema's PARTITION
// BY LIST declaration when the WHERE clause references the hash.
// We keep the field on RouteKey so server-shard routing can be extended
// without touching call sites.
//
// Disjoint bytes of tx_hash are used for the two layers so they don't
// correlate: byte 0 → shard, byte 1 → in-shard partition.

const (
	// NumShards is the number of independent dispatch pipelines. Each shard
	// has its own per-op slot grid. Dispatch routes by byte 0 of the natural
	// key. With NumShards = 1 today, byte 0 is unused at runtime but the
	// dispatch shape is shard-ready.
	NumShards = 1

	// NumPartitions is the number of HASH partitions per table at the
	// postgres level. 1 for single-SSD dev/laptop; raise (2/4/8) for
	// multi-SSD production where partition-level parallelism on disk pays
	// off. The Partition field on RouteKey is informational; postgres
	// handles partition routing internally via PARTITION BY HASH.
	NumPartitions = 8
)

// RouteKey identifies the (shard, partition) destination for a single key.
// Shard always 0 today; preserved as a field so dispatch code stays
// shape-stable when sharding lands.
type RouteKey struct {
	Shard     int
	Partition int
}

// Route resolves the destination for a tx_hash. Byte 0 selects the shard;
// byte 1 (modulo NumPartitions) selects the in-shard partition. The byte
// choices are disjoint so a future increase in either NumShards or
// NumPartitions doesn't reshuffle the other layer's routing.
func Route(hash *chainhash.Hash) RouteKey {
	return RouteKey{
		Shard:     int(hash[0]) % NumShards,
		Partition: int(hash[1]) % NumPartitions,
	}
}

// RouteBytes is Route for a raw 32-byte slice (used where chainhash.Hash
// isn't convenient — e.g., when dealing with bytea coming back from pgx).
func RouteBytes(hash []byte) RouteKey {
	return RouteKey{
		Shard:     int(hash[0]) % NumShards,
		Partition: int(hash[1]) % NumPartitions,
	}
}

// PartitionSuffix returns the suffix used in partition table names — e.g.,
// "_p03". Centralised here so all callers format the same way.
func PartitionSuffix(partition int) string {
	return partitionSuffixes[partition]
}

// Pre-computed suffixes to avoid allocation on every dispatch.
var partitionSuffixes = func() []string {
	out := make([]string, NumPartitions)
	for i := 0; i < NumPartitions; i++ {
		// fmt.Sprintf is too slow for a hot path; build manually.
		// Two-digit zero-padded.
		hi := byte('0' + (i / 10))
		lo := byte('0' + (i % 10))
		out[i] = "_p" + string([]byte{hi, lo})
	}
	return out
}()
