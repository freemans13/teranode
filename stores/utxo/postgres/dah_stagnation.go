package postgres

import (
	"context"
	"fmt"
	"time"
)

// stallLevel classifies how loudly the stagnation monitor should report a
// partition whose watermark is not moving while backlog exists.
type stallLevel int

const (
	stallNone stallLevel = iota
	stallWarn
	stallError
)

// classifyStall applies the ONE stagnation rule, no exceptions: watermark
// frozen AND backlog > 0 escalates on wall-clock time since that partition's
// watermark last advanced — Warnf at threshold/2, Errorf at threshold. It is
// deliberately blind to WHY progress stopped (wedged backend, enabled=false
// left off, broken tip source, orphaned advisory lock, pathological plan):
// every frozen-progress cause must land in the same loud place. threshold<=0
// disables the alarm (explicit ops opt-out).
func classifyStall(sinceProgress time.Duration, backlog int64, threshold time.Duration) stallLevel {
	if backlog <= 0 || threshold <= 0 {
		return stallNone
	}

	if sinceProgress >= threshold {
		return stallError
	}

	if sinceProgress >= threshold/2 {
		return stallWarn
	}

	return stallNone
}

// stagnationTracker holds the per-partition progress bookkeeping for the
// stagnation monitor: the PREVIOUS tick's raw observed watermark and the
// wall-clock instant each partition last advanced. It deliberately does NOT
// keep a running maximum — RewindDAHWatermark moves watermarks BACKWARD on a
// reorg, and the post-rewind re-sweep (950 -> 960 -> 970, all below the old
// peak) is healthy forward progress that must refresh the advance clock, not
// read as a stall until the old peak is re-passed.
type stagnationTracker struct {
	prevWM      [numPartitions]int64
	lastAdvance [numPartitions]time.Time
	primed      bool
}

// prime baselines every partition's clock at now on the FIRST successful probe
// (later calls are no-ops), so a process restart with an old, real backlog does
// not instantly page — the frozen clock starts only once the monitor has
// actually observed the watermark fail to move.
func (t *stagnationTracker) prime(wm [numPartitions]int64, now time.Time) {
	if t.primed {
		return
	}

	for p := 0; p < numPartitions; p++ {
		t.prevWM[p] = wm[p]
		t.lastAdvance[p] = now
	}

	t.primed = true
}

// observe records one tick's raw watermark for a partition and returns the
// stall level plus how long the partition has gone without an advance. Any
// increase over the previous tick's value is an advance (clock refreshed,
// stallNone); a decrease (reorg rewind) is NOT an advance but still moves the
// baseline down so the subsequent re-sweep registers as progress.
func (t *stagnationTracker) observe(p int, wm, backlog int64, now time.Time, threshold time.Duration) (stallLevel, time.Duration) {
	if wm > t.prevWM[p] {
		t.prevWM[p] = wm
		t.lastAdvance[p] = now

		return stallNone, 0
	}

	t.prevWM[p] = wm // a rewind moves the baseline DOWN; unchanged is a no-op

	sinceProgress := now.Sub(t.lastAdvance[p])

	return classifyStall(sinceProgress, backlog, threshold), sinceProgress
}

// runDAHStagnationMonitor is the sweep's ONLY remaining "timeout", and it
// cancels nothing: a 60s ticker, independent of every CALL (a CALL blocked in
// Exec can never blind it), reading all partition watermarks in one query on
// the MAIN pool (never maint, so a saturated maint pool cannot blind it
// either). It owns the sweep progress metrics: watermark lag, rows-stamped
// delta, and the per-partition stalled gauge.
func (s *postgresPrunerService) runDAHStagnationMonitor(ctx context.Context) {
	cfg := s.store.settings.UtxoStore

	threshold := time.Duration(cfg.PostgresDAHSweepStallAlertSeconds) * time.Second

	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	var (
		tracker     stagnationTracker
		lastStamped int64
	)

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			safeTip := s.store.dahSafeTip(lag)
			if safeTip <= 0 {
				continue
			}

			rows, err := s.store.pool.Query(ctx,
				`SELECT partition, last_swept_height FROM dah_part_watermark ORDER BY partition`)
			if err != nil {
				if ctx.Err() == nil {
					s.store.logger.Warnf("[dahStagnation] watermark probe error (retry next tick): %v", err)
				}

				continue
			}

			var wm [numPartitions]int64

			var scanErr error

			for rows.Next() {
				var p int
				var h int64
				if scanErr = rows.Scan(&p, &h); scanErr != nil {
					break
				}

				if p >= 0 && p < numPartitions {
					wm[p] = h
				}
			}
			rows.Close()

			if scanErr == nil {
				scanErr = rows.Err()
			}

			if scanErr != nil {
				s.store.logger.Warnf("[dahStagnation] watermark scan error (retry next tick): %v", scanErr)
				continue
			}

			now := time.Now()

			tracker.prime(wm, now) // no-op after the first successful probe

			var maxBacklog int64

			for p := 0; p < numPartitions; p++ {
				backlog := safeTip - wm[p]
				if backlog < 0 {
					backlog = 0
				}

				if backlog > maxBacklog {
					maxBacklog = backlog
				}

				level, frozenFor := tracker.observe(p, wm[p], backlog, now, threshold)

				// Explicit gauge decision table: only stallError raises the
				// stalled gauge; advance, stallNone AND stallWarn all clear it.
				switch level {
				case stallWarn:
					s.store.logger.Warnf("[dahStagnation] partition %d watermark %d frozen for %s with backlog %d (safeTip %d)", p, wm[p], frozenFor.Truncate(time.Second), backlog, safeTip)
					prometheusDAHSweepStalled.WithLabelValues(partitionLabel(p)).Set(0)

				case stallError:
					s.store.logger.Errorf("[dahStagnation] partition %d STALLED: watermark %d frozen for %s with backlog %d (safeTip %d) — sweep is not progressing; check pg_blocking_pids on the CALL backend, dah_sweep_control.enabled, and the tip source", p, wm[p], frozenFor.Truncate(time.Second), backlog, safeTip)
					prometheusDAHSweepStalled.WithLabelValues(partitionLabel(p)).Set(1)

				case stallNone:
					prometheusDAHSweepStalled.WithLabelValues(partitionLabel(p)).Set(0)
				}
			}

			prometheusDAHSweepWatermarkLag.Set(float64(maxBacklog))

			var stamped int64
			if err := s.store.pool.QueryRow(ctx,
				`SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&stamped); err == nil {
				if stamped > lastStamped && lastStamped > 0 {
					prometheusDAHSweepRowsStamped.Add(float64(stamped - lastStamped))
				}

				lastStamped = stamped
			}
		}
	}
}

// partitionLabel formats a partition index for prometheus labels. No shared
// "%02d" partition-suffix helper exists in this package (dah_reconcile.go
// inlines its own fmt.Sprintf("%02d", partition) locally), so this stays a
// plain decimal string rather than introducing one just for a label value.
func partitionLabel(p int) string {
	return fmt.Sprintf("%d", p)
}
