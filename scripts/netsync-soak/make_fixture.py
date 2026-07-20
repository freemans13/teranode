#!/usr/bin/env python3
"""make_fixture.py -- generate a synthetic log + tip series.

This exists so the parsing in netsync_soak_scrape.sh and soak_report.py is
proven against known inputs rather than assumed correct, and so the regime
guard can be exercised in both directions (a far-ahead window that scores, and
a cold window that must be refused) without waiting hours for a real soak.

  ./make_fixture.py --out-dir /tmp/fix --regime far-ahead
  ./make_fixture.py --out-dir /tmp/fix --regime cold
  ./make_fixture.py --out-dir /tmp/fix --regime restart

Emits teranode.log and tips.tsv into --out-dir.
"""

import os
import argparse
from datetime import datetime, timezone

REASONS = [
    # The idle reason is emitted at the POST-F3 duration on purpose: the
    # fixture must look like the world after the fix lands, so that a bucketer
    # keyed on today's rendered duration fails here loudly.
    "No answer from peer for 20m0s",
    "Peer appears to be stalled or misbehaving, getdata timeout",
    "head-of-line block-stalling timeout: next-needed block not delivered in time after re-fetch race",
    "Got 2000 unrequested headers from peer 10.0.0.7:8333",
    "Received block header that does not properly connect to the chain",
    "updateSyncPeer - disconnect old sync peer",
    "Max peers reached [125] - disconnecting peer",
    "a reason that does not exist yet",  # must surface as unbucketed
]


def ts(base, off):
    return datetime.fromtimestamp(base + off, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--regime", default="far-ahead",
                    choices=["far-ahead", "cold", "restart"])
    ap.add_argument("--hours", type=float, default=3.0)
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)

    base = 1_760_000_000
    span = int(a.hours * 3600)
    log, tips = [], []

    tip_h = 650_000
    # Far-ahead: the frontier runs well ahead of the committed tip. Cold: the
    # frontier sits right on top of the tip, which is what the guard must catch.
    front_h = tip_h + (5000 if a.regime != "cold" else 2)

    for off in range(0, span, 10):
        if a.regime == "cold":
            # Monotonic climb, one block every sample, nothing queued.
            tip_h += 1
            front_h = tip_h + 2
        else:
            # Sawtooth: bursts of progress separated by sticks, which is the
            # far-ahead signature this project is trying to remove.
            phase = (off // 10) % 30
            if phase < 12:
                tip_h += 2
            # phases 12..29 are a ~180s stick
            front_h += 3

        if off % 10 == 0:
            tips.append("%d\t%s\t%d" % (
                base + off,
                datetime.fromtimestamp(base + off, timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"),
                tip_h))

        if off % 30 == 0:
            log.append("%s INFO [frontier] height=%d gap=%d"
                       % (ts(base, off), front_h, front_h - tip_h))

        if off % 60 == 0:
            n = 8 if (off // 60) % 4 else 5
            log.append("%s INFO [connmgr] replenish check: conns=%d pending=0 "
                       "target=8 dialing=%d" % (ts(base, off), n, 8 - n))

        if off % 300 == 0:
            r = REASONS[(off // 300) % len(REASONS)]
            log.append("%s INFO Disconnecting (peer 10.0.0.%d:8333 (outbound)) "
                       "reason: %s" % (ts(base, off), (off // 300) % 20, r))

    if a.regime == "restart":
        mid = span // 2
        log.append("%s INFO Health check endpoint listening on 0.0.0.0:8000"
                   % ts(base, mid))

    log.sort()
    with open(os.path.join(a.out_dir, "teranode.log"), "w") as f:
        f.write("\n".join(log) + "\n")
    with open(os.path.join(a.out_dir, "tips.tsv"), "w") as f:
        f.write("\n".join(tips) + "\n")
    print("wrote %d log lines and %d tip samples to %s (regime=%s)"
          % (len(log), len(tips), a.out_dir, a.regime))


if __name__ == "__main__":
    main()
