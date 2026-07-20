#!/usr/bin/env python3
"""soak_report.py -- turn a scrape + tip series into judgeable numbers.

Consumes the JSONL from netsync_soak_scrape.sh and the TSV from tip_sampler.sh
and emits five streams:

  1. disconnects/hr by bucket
  2. fraction of replenish samples with conns >= target
  3. blocks/hr
  4. stick-duration distribution (p50 / p90 / max, counts over 60s and 180s)
  5. frontier-freeze count (intervals where the [frontier] height did not move)

THE REGIME GUARD
----------------
A cold node has no backlog, so it never sticks, so it always looks green. Every
number in this report is meaningless unless the node has soaked back into the
far-ahead regime: the committed tip chronically lagging the header frontier with
the downstream idle. That is not a caveat to remember, it is a precondition to
enforce, so this script REFUSES to emit (exit 2) when:

  * the window contains a process-start marker -- a restart means the node came
    back cold and the backlog was reset;
  * there is no frontier series, so the backlog cannot be established at all;
  * the median backlog (frontier height - tip height) is below --min-backlog;
  * the tip advanced at every single sample, i.e. it climbed monotonically with
    nothing queued behind it.

--i-know-this-is-not-far-ahead bypasses the refusal, but the bypass is stamped
into the report itself (and into the JSON as regime_guard_bypassed) so a green
number captured in the wrong regime cannot be quoted later as if it were valid.

Usage:
  ./soak_report.py --events events.jsonl --tips tips.tsv
  ./soak_report.py --events events.jsonl --tips tips.tsv --json > window.json
"""

import sys
import os
import json
import argparse
from datetime import datetime, timezone

# Buckets we cause ourselves and can therefore fix. max_peers / duplicate_
# connection / banned are inbound-pressure or peer-quality events: they are
# worth watching but they are not what the plan's "<= ~5 self-inflicted
# disconnects/hr" gate is measuring.
SELF_INFLICTED = {
    "idle",
    "stalled_or_misbehaving",
    "head_of_line",
    "unrequested_headers",
    "unrequested_block",
    "nonconnecting_headers",
    "sync_peer_rotation",
}


def percentile(vals, p):
    """Nearest-rank percentile. Small n here, so exactness beats interpolation."""
    if not vals:
        return None
    s = sorted(vals)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def load_events(path):
    events = []
    with open(path, "r", errors="replace") as fh:
        for ln, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                events.append(json.loads(raw))
            except json.JSONDecodeError:
                sys.stderr.write("warning: skipping malformed event line %d\n" % ln)
    return events


def load_tips(path):
    """Return (samples, dropped) where samples is [(ts, height)] ascending.

    ERR rows are sampler-side failures (postgres unreachable), not chain
    behaviour. They are counted and excluded: treating a monitoring outage as a
    chain stick would manufacture exactly the symptom we are hunting.
    """
    samples, dropped = [], 0
    with open(path, "r", errors="replace") as fh:
        for raw in fh:
            parts = raw.strip().split("\t")
            if len(parts) < 3:
                continue
            try:
                ts = float(parts[0])
            except ValueError:
                continue
            if parts[2] == "ERR" or not parts[2].isdigit():
                dropped += 1
                continue
            samples.append((ts, int(parts[2])))
    samples.sort()
    return samples, dropped


def sticks_from(samples):
    """Closed stick durations, plus the open trailing stick.

    A stick runs from the moment a height is first observed to the moment a
    greater height is observed. The final run is still open at the end of the
    window, so it is reported separately as a lower bound rather than folded
    into the distribution (where it would drag percentiles down).
    """
    closed, run_start, advances = [], None, 0
    for i, (ts, h) in enumerate(samples):
        if i == 0:
            run_start = ts
            continue
        if h > samples[i - 1][1]:
            advances += 1
            closed.append(ts - run_start)
            run_start = ts
        elif h < samples[i - 1][1]:
            # A reorg or a store rollback: the run restarts, and it is not a
            # stick, so nothing is recorded for it.
            run_start = ts
    open_stick = (samples[-1][0] - run_start) if samples and run_start is not None else 0.0
    return closed, open_stick, advances


def median_backlog(frontier_evs, tip_samples):
    """Median (frontier height - committed tip) across the frontier series.

    For each frontier observation, compare against the most recent tip sample
    at or before it -- the tip series is coarser, so aligning forward would
    credit the node with blocks it had not yet committed.
    """
    if not frontier_evs or not tip_samples:
        return None
    gaps, ti = [], 0
    for ev in sorted(frontier_evs, key=lambda e: e["ts"]):
        while ti + 1 < len(tip_samples) and tip_samples[ti + 1][0] <= ev["ts"]:
            ti += 1
        if tip_samples[ti][0] <= ev["ts"]:
            gaps.append(ev["height"] - tip_samples[ti][1])
    return percentile(gaps, 50) if gaps else None


def frontier_freezes(frontier_evs, threshold):
    """Count and size the intervals where the header frontier did not advance."""
    evs = sorted([e for e in frontier_evs if e.get("ts") is not None],
                 key=lambda e: e["ts"])
    freezes, run_start = [], None
    for i, e in enumerate(evs):
        if i == 0:
            run_start = e["ts"]
            continue
        if e["height"] > evs[i - 1]["height"]:
            freezes.append(e["ts"] - run_start)
            run_start = e["ts"]
        elif e["height"] < evs[i - 1]["height"]:
            run_start = e["ts"]
    over = [f for f in freezes if f >= threshold]
    return len(over), (max(freezes) if freezes else 0.0)


def main():
    ap = argparse.ArgumentParser(description="Summarise a netsync soak window.")
    ap.add_argument("--events", required=True, help="JSONL from netsync_soak_scrape.sh")
    ap.add_argument("--tips", required=True, help="TSV from tip_sampler.sh")
    ap.add_argument("--json", action="store_true", help="emit JSON for diffing")
    ap.add_argument("--min-backlog", type=int, default=100,
                    help="median frontier-tip gap required to call the window "
                         "far-ahead (default 100 blocks)")
    ap.add_argument("--frontier-freeze-seconds", type=float, default=60.0,
                    help="a frontier interval this long or longer counts as a freeze")
    ap.add_argument("--i-know-this-is-not-far-ahead", action="store_true",
                    dest="bypass",
                    help="emit anyway; stamps the report as NOT VALID FOR "
                         "ACCEPTANCE")
    args = ap.parse_args()

    for p in (args.events, args.tips):
        if not os.path.exists(p):
            sys.stderr.write("error: no such file: %s\n" % p)
            return 1

    events = load_events(args.events)
    tips, dropped = load_tips(args.tips)

    disconnects = [e for e in events if e.get("kind") == "disconnect"]
    conns = [e for e in events if e.get("kind") == "conns"]
    frontier = [e for e in events if e.get("kind") == "frontier"
                and e.get("ts") is not None]
    starts = [e for e in events if e.get("kind") == "proc_start"]

    # --- window ------------------------------------------------------------
    stamps = [e["ts"] for e in events if e.get("ts") is not None]
    stamps += [t for t, _ in tips]
    if not stamps:
        sys.stderr.write("error: no timestamped data in either input\n")
        return 1
    t0, t1 = min(stamps), max(stamps)
    hours = (t1 - t0) / 3600.0
    if hours <= 0:
        sys.stderr.write("error: zero-length window\n")
        return 1

    # --- regime guard ------------------------------------------------------
    blocking = []
    if starts:
        blocking.append(
            "window contains %d process-start/shutdown marker(s): the node "
            "restarted cold, so the backlog was reset and no stall number here "
            "means anything" % len(starts))
    mb = median_backlog(frontier, tips)
    if not frontier:
        blocking.append(
            "no [frontier] events found: without the header frontier the "
            "backlog cannot be established, so the far-ahead regime cannot be "
            "verified")
    elif mb is None:
        blocking.append("frontier events could not be aligned to any tip sample")
    elif mb < args.min_backlog:
        blocking.append(
            "median backlog is %d blocks, below the --min-backlog threshold of "
            "%d: the node is at or near the tip, not far-ahead"
            % (mb, args.min_backlog))

    closed, open_stick, advances = sticks_from(tips)
    if len(tips) >= 3 and advances == len(tips) - 1:
        blocking.append(
            "the tip advanced at every one of the %d samples: it climbed "
            "monotonically with nothing queued behind it, which is the cold-node "
            "signature this report refuses to score" % len(tips))

    if blocking and not args.bypass:
        sys.stderr.write("\n" + "=" * 72 + "\n")
        sys.stderr.write("REGIME GUARD: REFUSING TO EMIT A REPORT\n")
        sys.stderr.write("=" * 72 + "\n")
        for b in blocking:
            sys.stderr.write("  * %s\n" % b)
        sys.stderr.write(
            "\nThis window cannot be used to accept or reject a fix. Let the "
            "node soak\nback into the far-ahead regime and capture again. To "
            "override anyway (the\nresulting report is stamped invalid), pass "
            "--i-know-this-is-not-far-ahead.\n")
        return 2

    # --- 1. disconnects/hr by bucket --------------------------------------
    by_bucket, unknown_reasons = {}, {}
    for d in disconnects:
        b = d.get("bucket", "other")
        by_bucket[b] = by_bucket.get(b, 0) + 1
        if b == "other":
            r = d.get("reason", "")
            unknown_reasons[r] = unknown_reasons.get(r, 0) + 1
    rates = {b: c / hours for b, c in by_bucket.items()}
    self_rate = sum(c for b, c in by_bucket.items() if b in SELF_INFLICTED) / hours

    # --- 2. conns >= target ------------------------------------------------
    at_target = sum(1 for c in conns if c["conns"] >= c["target"])
    frac_at_target = (at_target / len(conns)) if conns else None

    # --- 3. blocks/hr ------------------------------------------------------
    blocks_per_hr = None
    if len(tips) >= 2:
        span = tips[-1][0] - tips[0][0]
        if span > 0:
            blocks_per_hr = (tips[-1][1] - tips[0][1]) / (span / 3600.0)

    # --- 4. sticks ---------------------------------------------------------
    interval = None
    if len(tips) >= 3:
        gaps = sorted(tips[i + 1][0] - tips[i][0] for i in range(len(tips) - 1))
        interval = gaps[len(gaps) // 2]
    stick = {
        "p50_s": percentile(closed, 50),
        "p90_s": percentile(closed, 90),
        "max_s": max(closed) if closed else None,
        "over_60s": sum(1 for s in closed if s > 60),
        "over_180s": sum(1 for s in closed if s > 180),
        "count": len(closed),
        "open_trailing_s": open_stick,
        "sample_interval_s": interval,
    }
    # A 60s sampler cannot resolve a "stick max < 60s" gate: its smallest
    # observable stick is one sample period, so a pass would be an artefact of
    # the sampling rate rather than evidence about the node.
    stick_gate_judgeable = interval is not None and interval <= 30
    stick["gate_judgeable"] = stick_gate_judgeable

    # --- 5. frontier freezes ----------------------------------------------
    freeze_count, freeze_max = frontier_freezes(
        frontier, args.frontier_freeze_seconds)

    invalid = bool(blocking)
    result = {
        "window": {
            "start": datetime.fromtimestamp(t0, timezone.utc).isoformat(),
            "end": datetime.fromtimestamp(t1, timezone.utc).isoformat(),
            "hours": hours,
        },
        "regime_guard_bypassed": invalid,
        "regime_guard_reasons": blocking,
        "valid_for_acceptance": not invalid,
        "median_backlog_blocks": mb,
        "disconnects": {
            "total": len(disconnects),
            "per_hour_by_bucket": rates,
            "counts_by_bucket": by_bucket,
            "self_inflicted_per_hour": self_rate,
            "unbucketed_reasons": unknown_reasons,
        },
        "connections": {
            "samples": len(conns),
            "fraction_at_or_above_target": frac_at_target,
        },
        "blocks_per_hour": blocks_per_hr,
        "sticks": stick,
        "frontier": {
            "samples": len(frontier),
            "freeze_count": freeze_count,
            "freeze_max_s": freeze_max,
            "freeze_threshold_s": args.frontier_freeze_seconds,
        },
        "tip_samples": len(tips),
        "tip_samples_dropped": dropped,
    }

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    # --- human summary -----------------------------------------------------
    o = sys.stdout.write
    if invalid:
        o("\n" + "!" * 72 + "\n")
        o("!!  REGIME GUARD BYPASSED -- THIS REPORT IS NOT VALID FOR ACCEPTANCE\n")
        for b in blocking:
            o("!!  %s\n" % b)
        o("!!  Every number below was measured outside the far-ahead regime and\n")
        o("!!  must not be quoted as a baseline or as evidence a fix worked.\n")
        o("!" * 72 + "\n")

    o("\nnetsync soak report\n")
    o("  window   : %s .. %s (%.2f h)\n" % (
        result["window"]["start"], result["window"]["end"], hours))
    o("  backlog  : median %s blocks behind the header frontier\n" %
      ("unknown" if mb is None else mb))

    o("\n1. disconnects/hr by bucket (total %d)\n" % len(disconnects))
    if by_bucket:
        for b in sorted(rates, key=lambda k: -rates[k]):
            o("   %-24s %8.2f /hr   (%d)%s\n" % (
                b, rates[b], by_bucket[b], "  <- self-inflicted"
                if b in SELF_INFLICTED else ""))
    else:
        o("   (none)\n")
    o("   %-24s %8.2f /hr   [gate: <= ~5]\n" % ("SELF-INFLICTED TOTAL", self_rate))
    if unknown_reasons:
        o("\n   UNBUCKETED reasons -- add rules to netsync_soak_scrape.sh:\n")
        for r in sorted(unknown_reasons, key=lambda k: -unknown_reasons[k]):
            o("     %6d  %s\n" % (unknown_reasons[r], r))

    o("\n2. connection level\n")
    if frac_at_target is None:
        o("   no [connmgr] replenish samples in window\n")
    else:
        o("   conns >= target in %.1f%% of %d samples   [gate: >= 95%%]\n" % (
            100.0 * frac_at_target, len(conns)))

    o("\n3. throughput\n")
    o("   blocks/hr: %s   (from %d tip samples, %d dropped)\n" % (
        "unknown" if blocks_per_hr is None else "%.1f" % blocks_per_hr,
        len(tips), dropped))

    o("\n4. stick durations (tip not advancing)\n")
    if not closed:
        o("   no closed sticks observed\n")
    else:
        o("   p50 %.1fs   p90 %.1fs   max %.1fs   (n=%d)\n" % (
            stick["p50_s"], stick["p90_s"], stick["max_s"], stick["count"]))
        o("   over 60s: %d     over 180s: %d   [gate: max < 60s]\n" % (
            stick["over_60s"], stick["over_180s"]))
    o("   open trailing stick: %.1fs\n" % open_stick)
    if not stick_gate_judgeable:
        o("   NOTE: sample interval is %ss, so the 'stick max < 60s' gate is NOT\n"
          "         judgeable from this window -- the smallest resolvable stick is\n"
          "         one sample period. Re-capture with INTERVAL=10.\n" % (
              "unknown" if interval is None else "%.0f" % interval))

    o("\n5. frontier freezes\n")
    if not frontier:
        o("   no [frontier] samples in window\n")
    else:
        o("   %d freeze(s) >= %.0fs, longest %.1fs, from %d samples\n" % (
            freeze_count, args.frontier_freeze_seconds, freeze_max, len(frontier)))
    o("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
