package kafka

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka/kafkatest"
	"github.com/stretchr/testify/require"
)

// TestLingerLatencyRegression proves that the `flush_frequency` query parameter
// — wired to franz-go's per-partition `kgo.ProducerLinger` — is responsible
// for the producer-side latency that starves the subtree-validator's txmeta
// cache at peak load on dev-scale-1/2.
//
// Setup mirrors the production pathology in miniature:
//   - many partitions on one topic (so records spread thinly across them)
//   - a feed rate slow enough per partition that batches don't fill on size
//     (the franz-go default ProducerBatchMaxBytes is 1 MiB and is never reached
//     here, so the per-partition flush is dominated by ProducerLinger)
//
// The test publishes the same records through two producers that differ only
// in `flush_frequency` and measures publish→consumer end-to-end latency.
//
// Hypothesis: with flush_frequency=1s (the legacy scale-1/scale-2 setting),
// p50 latency lands near 1 s because every record waits at the franz-go
// per-partition linger. With flush_frequency=10ms, p99 latency is a small
// multiple of 10 ms.
//
// `outer_batcher_linger` is left at its default (10 ms) for both runs so this
// test isolates ProducerLinger. Unit-level coverage that the two fields are
// decoupled lives in TestNewKafkaAsyncProducerFromURLOuterBatcherLinger.
//
// Run:  go test -v -run TestLingerLatencyRegression -timeout 5m ./util/kafka/
func TestLingerLatencyRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping kafka regression test in -short mode (requires Docker)")
	}

	logger := ulogger.TestLogger{}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	const (
		// Enough partitions that any single key hashes to a partition that, at
		// the test's slow feed rate, only receives a handful of records over
		// the run — exactly the regime where the franz-go per-partition linger
		// dominates flush timing.
		partitions = 32

		// Slow enough per-partition that batches never reach
		// ProducerBatchMaxBytes (1 MiB) — so flush is linger-driven.
		messageCount  = 200
		messageSize   = 256                   // small; we are testing latency, not throughput
		sendInterval  = 25 * time.Millisecond // 25ms × 200 = 5s of feeding
		consumerSlack = 8 * time.Second       // wait for last records to land
	)

	type caseResult struct {
		label   string
		linger  time.Duration
		p50     time.Duration
		p90     time.Duration
		p99     time.Duration
		samples int
	}

	run := func(t *testing.T, label, flushFrequency string) caseResult {
		t.Helper()

		topic := fmt.Sprintf("linger-regr-%s-%d", sanitizeTopicComponent(label), time.Now().UnixNano()%100000)
		query := fmt.Sprintf(
			"partitions=%d&replication=1&retention=60000&flush_frequency=%s&flush_messages=10000&flush_bytes=1048576",
			partitions, flushFrequency,
		)
		kafkaURL, err := url.Parse(env.TopicURL(topic, query))
		require.NoError(t, err)

		producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
		require.NoError(t, err)
		ch := make(chan *Message, messageCount)
		producer.Start(ctx, ch)

		consumerGroup := fmt.Sprintf("linger-regr-cg-%d", time.Now().UnixNano()%100000)
		consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, consumerGroup, true, nil)
		require.NoError(t, err)

		var (
			latenciesMu sync.Mutex
			latencies   = make([]time.Duration, 0, messageCount)
			sendTimes   sync.Map // key string -> time.Time of Publish()
			received    atomic.Int64
			doneCh      = make(chan struct{})
			closeOnce   sync.Once
		)
		closeDone := func() { closeOnce.Do(func() { close(doneCh) }) }

		consumer.Start(ctx, func(msg *KafkaMessage) error {
			if v, ok := sendTimes.Load(string(msg.Key)); ok {
				lat := time.Since(v.(time.Time))
				latenciesMu.Lock()
				latencies = append(latencies, lat)
				latenciesMu.Unlock()
			}
			if received.Add(1) >= int64(messageCount) {
				closeDone()
			}
			return nil
		})

		// Give the consumer time to join the group and get partitions assigned
		// before we publish anything; otherwise the first publishes get lost
		// behind a rebalance and skew the first records' latency upward.
		time.Sleep(2 * time.Second)

		payload := make([]byte, messageSize)
		for i := range payload {
			payload[i] = byte(i)
		}

		// Publish records spaced sendInterval apart so per-partition arrival
		// is sparse and neither linger trigger fills on size.
		for i := 0; i < messageCount; i++ {
			key := fmt.Sprintf("k-%s-%d", label, i)
			sendTimes.Store(key, time.Now())
			producer.Publish(&Message{
				Key:   []byte(key),
				Value: payload,
			})
			time.Sleep(sendInterval)
		}

		// Wait for the consumer to drain (or time out).
		select {
		case <-doneCh:
		case <-time.After(consumerSlack):
			t.Logf("%s: timed out waiting for all %d messages (received %d)",
				label, messageCount, received.Load())
		}

		require.NoError(t, producer.Stop())
		require.NoError(t, consumer.Close())

		latenciesMu.Lock()
		latsCopy := append([]time.Duration(nil), latencies...)
		latenciesMu.Unlock()
		sort.Slice(latsCopy, func(i, j int) bool { return latsCopy[i] < latsCopy[j] })

		pct := func(p float64) time.Duration {
			if len(latsCopy) == 0 {
				return 0
			}
			idx := int(float64(len(latsCopy)-1) * p / 100)
			if idx < 0 {
				idx = 0
			}
			if idx >= len(latsCopy) {
				idx = len(latsCopy) - 1
			}
			return latsCopy[idx]
		}

		linger, err := time.ParseDuration(flushFrequency)
		require.NoError(t, err)

		return caseResult{
			label:   label,
			linger:  linger,
			p50:     pct(50),
			p90:     pct(90),
			p99:     pct(99),
			samples: len(latsCopy),
		}
	}

	// Run the same scenario twice, differing only in flush_frequency.
	high := run(t, "linger-1s-production", "1s")
	low := run(t, "linger-10ms-proposed", "10ms")

	t.Log("\n=== publish→consume latency, 32 partitions, sparse feed ===")
	for _, r := range []caseResult{high, low} {
		t.Logf("  %-22s  linger=%-5s  p50=%-9s  p90=%-9s  p99=%-9s  samples=%d",
			r.label, r.linger, r.p50, r.p90, r.p99, r.samples)
	}

	require.GreaterOrEqual(t, high.samples, messageCount/2,
		"linger=1s: too few latency samples to draw a conclusion")
	require.GreaterOrEqual(t, low.samples, messageCount/2,
		"linger=10ms: too few latency samples to draw a conclusion")

	// Core hypothesis assertion: at sparse per-partition rates with the
	// production setting, p50 latency is dominated by linger and lands
	// somewhere on the order of the configured 1s. We require >300ms to
	// leave headroom for CI noise — but the *real* effect is typically
	// 500-1000ms.
	require.Greater(t, high.p50, 300*time.Millisecond,
		"flush_frequency=1s should produce p50 latency >300ms "+
			"(linger is the bottleneck; saw p50=%v)", high.p50)

	// And with linger=10ms, p99 should be a small multiple of the linger
	// even accounting for franz-go's own scheduling and the broker write.
	// 250ms is generous; the real number is typically <50ms in CI.
	require.Less(t, low.p99, 250*time.Millisecond,
		"flush_frequency=10ms should keep p99 latency <250ms "+
			"(linger no longer dominates; saw p99=%v)", low.p99)

	// And the linger=1s p50 must be much worse than linger=10ms p99 —
	// otherwise the two configurations are indistinguishable and the
	// hypothesis is not supported.
	require.Greater(t, high.p50, low.p99*3,
		"flush_frequency=1s p50 (%v) should be at least 3× flush_frequency=10ms p99 (%v)",
		high.p50, low.p99)
}
