package postgres

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Two-layer routing for the UTXO store. Today the deployment has a single
// postgres shard; the Shard field stays 0 and routing collapses to in-shard
// partition selection. When the deployment grows to N shards the same code
// dispatches by Shard first; only the destination resolution changes (one
// pgxpool per shard instead of one pool total). Phase-1 commitment recorded
// in the project memory.
//
// Disjoint bytes of tx_hash are used for the two layers so they don't
// correlate: byte 0 reserved for future shard routing, byte 1 for in-shard
// partition routing. With NumShards=1 today, byte 0 is unused at runtime but
// the code path is the same.

const (
	// NumShards is the number of postgres servers the store is sharded across.
	// 1 today; bump and supply per-shard pools when introducing horizontal
	// sharding.
	NumShards = 1

	// NumPartitions is the number of partitions per shard. Must match the
	// schema's PARTITION BY LIST modulus. The generated partition column on
	// each table is `(get_byte(<key>, 1) % NumPartitions)`.
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
