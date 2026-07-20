#!/usr/bin/env bash
#
# netsync_soak_scrape.sh -- turn a teranode log into a machine-readable event stream.
#
# Every peer teardown in the legacy stack funnels through ONE log site,
# services/legacy/peer/peer.go: DisconnectWithLogFunc, which emits
#
#     Disconnecting (%s) reason: %s
#
# Scraping that single anchor is what makes the disconnect count trustworthy:
# there is exactly one line per teardown, so nothing is missed and nothing is
# counted twice. Do NOT add a second disconnect pattern here -- a peer that
# takes two code paths into the same teardown would then be counted twice and
# the whole denominator of this project would silently inflate.
#
# THE BUCKETING RULE: match on STABLE SUBSTRINGS of the reason, never on a
# rendered duration. The idle reason is built with a Go time.Duration:
#
#     fmt.Sprintf("No answer from peer for %s", p.settings.Legacy.PeerIdleTimeout)
#
# which renders "2m5s", not "125s", and which the F3 work in this same project
# deliberately CHANGES (to 20m0s while catching up). A bucketer keyed on the
# rendered duration would stop matching at exactly the moment the fix it exists
# to measure lands -- an instrument that breaks when the experiment succeeds is
# worse than no instrument. Hence prefix/substring keys only, and hence the
# --self-test below, which fails the script if 20m0s and 2m5s ever stop landing
# in the same bucket.
#
# Substring (not left-anchored prefix) matching is also deliberate: several real
# reasons carry a leading clause, e.g. the header one is
# "Received block header that does not properly connect to the chain".
#
# Anything unrecognised goes to the "other" bucket AND has its literal reason
# text printed, so a new reason introduced upstream shows up as a named unknown
# rather than being quietly absorbed into a known bucket.
#
# Usage:
#   ./netsync_soak_scrape.sh --log /root/logs/teranode.log --out events.jsonl
#   ./netsync_soak_scrape.sh --self-test
#   cat teranode.log | ./netsync_soak_scrape.sh --out events.jsonl
#
# Output is JSON Lines, one event per line, consumed by soak_report.py.
#
# POSIX bash + python3 only. No pip packages, no network.

set -euo pipefail

exec python3 - "$@" <<'PYEOF'
import sys, os, re, json, time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Bucketing table. Ordered: the first matching substring wins, so more specific
# reasons MUST come before the more general ones they contain.
# ---------------------------------------------------------------------------
BUCKET_RULES = [
    ("No answer from peer for",                   "idle"),
    ("Peer appears to be stalled or misbehaving", "stalled_or_misbehaving"),
    ("head-of-line block-stalling timeout",       "head_of_line"),
    ("unrequested headers",                       "unrequested_headers"),
    ("Got unrequested block",                     "unrequested_block"),
    ("does not properly connect",                 "nonconnecting_headers"),
    ("updateSyncPeer - disconnect old sync peer", "sync_peer_rotation"),
    # "Max peers per IP reached" must precede "Max peers reached": they are
    # distinct strings, but keeping the specific one first survives any future
    # rewording that makes one a substring of the other.
    ("Max peers per IP reached",                  "max_peers_per_ip"),
    ("Max peers reached",                         "max_peers"),
    ("Already connected to",                      "duplicate_connection"),
    ("Misbehaving peer -- banning",               "banned"),
]

def bucket_for(reason):
    for needle, name in BUCKET_RULES:
        if needle in reason:
            return name
    return "other"

# ---------------------------------------------------------------------------
# Line patterns.
# ---------------------------------------------------------------------------
# Greedy (.*) before ") reason: " is intentional: the peer descriptor itself
# contains parentheses, e.g. "peer 1.2.3.4:8333 (outbound)", so a lazy match
# would truncate at the wrong bracket.
RE_DISCONNECT = re.compile(r"Disconnecting \((.*)\) reason: (.*)$")
RE_REPLENISH  = re.compile(
    r"\[connmgr\] replenish check: conns=(\d+) pending=(\d+) target=(\d+) dialing=(\d+)")
# [frontier] is emitted by this project's own instrumentation, as
#   [frontier] headerFrontier=N headerListLen=N committedHeight=N ...
# Match headerFrontier= (the frontier height) and accept a bare height= as well
# so a later reword of the surrounding text does not blind the freeze detector.
# The value may be -1 when the header list is empty, hence the optional sign.
RE_FRONTIER   = re.compile(r"\[frontier\].*?\b(?:headerFrontier|height)=(-?\d+)")
RE_ANSI       = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

# A restart inside the measurement window invalidates every number in the
# report: the node comes back cold, with no backlog and therefore no stalls.
# soak_report.py refuses to emit if it sees any of these.
PROC_START_PATTERNS = [
    "Health check endpoint listening on",
    "PostgreSQL is up - ready to go!",
    "daemon shutdown completed",
    "daemon shutdown requested",
    "STARTING",
    "VERSION\n",
]
RE_PROC_START = re.compile(
    "|".join(re.escape(p.rstrip("\n")) for p in PROC_START_PATTERNS))

# ---------------------------------------------------------------------------
# Timestamps. Two logger backends are in play (ulogger/filelogger.go uses
# "2006-01-02 15:04:05", ulogger/zerologger.go uses RFC3339), both leading the
# line, so try both rather than assuming which binary produced the file.
# ---------------------------------------------------------------------------
RE_TS_PLAIN = re.compile(r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")
RE_TS_RFC   = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2}))")

def parse_ts(line):
    m = RE_TS_RFC.match(line)
    if m:
        s = m.group(1)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s).timestamp()
        except ValueError:
            pass
    m = RE_TS_PLAIN.match(line)
    if m:
        s = m.group(1).replace("T", " ")
        try:
            # Naive stamps are container-local; treat them as UTC so that two
            # windows scraped on the same box remain comparable to each other.
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    return None

# ---------------------------------------------------------------------------
def scrape(fh, out):
    unknown = {}
    counts = {}
    n_lines = 0
    no_ts = 0
    first_ts = last_ts = None

    for raw in fh:
        n_lines += 1
        line = RE_ANSI.sub("", raw.rstrip("\n"))
        ts = parse_ts(line)
        if ts is None:
            no_ts += 1
        else:
            if first_ts is None or ts < first_ts:
                first_ts = ts
            if last_ts is None or ts > last_ts:
                last_ts = ts

        ev = None
        m = RE_DISCONNECT.search(line)
        if m:
            reason = m.group(2).strip()
            b = bucket_for(reason)
            counts[b] = counts.get(b, 0) + 1
            if b == "other":
                unknown[reason] = unknown.get(reason, 0) + 1
            ev = {"kind": "disconnect", "bucket": b,
                  "peer": m.group(1), "reason": reason}
        else:
            m = RE_REPLENISH.search(line)
            if m:
                ev = {"kind": "conns",
                      "conns": int(m.group(1)), "pending": int(m.group(2)),
                      "target": int(m.group(3)), "dialing": int(m.group(4))}
            else:
                m = RE_FRONTIER.search(line)
                if m:
                    ev = {"kind": "frontier", "height": int(m.group(1))}
                elif RE_PROC_START.search(line):
                    ev = {"kind": "proc_start", "marker": line[:200]}

        if ev is not None:
            ev["ts"] = ts
            out.write(json.dumps(ev) + "\n")

    # Stderr summary: operator-facing, never parsed.
    sys.stderr.write("scraped %d lines (%d without a parseable timestamp)\n"
                     % (n_lines, no_ts))
    if first_ts and last_ts:
        sys.stderr.write("window: %s .. %s (%.2f h)\n" % (
            datetime.fromtimestamp(first_ts, timezone.utc).isoformat(),
            datetime.fromtimestamp(last_ts, timezone.utc).isoformat(),
            (last_ts - first_ts) / 3600.0))
    for b in sorted(counts, key=lambda k: -counts[k]):
        sys.stderr.write("  %-24s %d\n" % (b, counts[b]))
    if unknown:
        sys.stderr.write(
            "\nUNBUCKETED reasons -- add a rule to BUCKET_RULES for each:\n")
        for r in sorted(unknown, key=lambda k: -unknown[k]):
            sys.stderr.write("  %6d  %s\n" % (unknown[r], r))
    return 0

# ---------------------------------------------------------------------------
def self_test():
    failures = []

    def check(desc, got, want):
        if got != want:
            failures.append("%s: got %r want %r" % (desc, got, want))

    # The whole point of the harness. Both of these are the SAME failure mode
    # and must land in the same bucket; 20m0s is what F3 sets while catching up,
    # 2m5s is today's tip value. A duration-keyed bucketer passes the second and
    # fails the first, which is exactly the regression this guards.
    check("idle @ 20m0s (post-F3 IBD value)",
          bucket_for("No answer from peer for 20m0s"), "idle")
    check("idle @ 2m5s (current tip value)",
          bucket_for("No answer from peer for 2m5s"), "idle")
    check("idle @ 1h0m0s",
          bucket_for("No answer from peer for 1h0m0s"), "idle")

    check("stalled", bucket_for(
        "Peer appears to be stalled or misbehaving, getdata timeout"),
        "stalled_or_misbehaving")
    check("stalled inHandler", bucket_for(
        "Peer appears to be stalled or misbehaving, inHandler break out"),
        "stalled_or_misbehaving")
    check("head-of-line", bucket_for(
        "head-of-line block-stalling timeout: next-needed block not "
        "delivered in time after re-fetch race"), "head_of_line")
    check("unrequested headers", bucket_for(
        "Got 2000 unrequested headers from peer 1.2.3.4:8333"),
        "unrequested_headers")
    check("unrequested block", bucket_for(
        "Got unrequested block 00000000000000000abc"), "unrequested_block")
    # Left-anchored prefix matching would miss this one: the real string has a
    # leading clause before the key phrase.
    check("nonconnecting headers", bucket_for(
        "Received block header that does not properly connect to the chain"),
        "nonconnecting_headers")
    check("sync peer rotation", bucket_for(
        "updateSyncPeer - disconnect old sync peer"), "sync_peer_rotation")
    check("max peers", bucket_for(
        "Max peers reached [125] - disconnecting peer"), "max_peers")
    check("max peers per ip", bucket_for(
        "Max peers per IP reached [5] - disconnecting peer"), "max_peers_per_ip")
    check("unknown falls through", bucket_for(
        "some brand new reason nobody has seen"), "other")

    # End-to-end: the anchor regex must survive parentheses in the peer string.
    line = ("2026-07-20 11:22:33 INFO Disconnecting (peer 1.2.3.4:8333 "
            "(outbound)) reason: No answer from peer for 20m0s")
    m = RE_DISCONNECT.search(line)
    if not m:
        failures.append("anchor regex did not match a realistic log line")
    else:
        check("peer captured through inner parens",
              m.group(1), "peer 1.2.3.4:8333 (outbound)")
        check("reason captured", m.group(2), "No answer from peer for 20m0s")
        check("bucket end-to-end", bucket_for(m.group(2)), "idle")

    check("timestamp plain parses", parse_ts(line) is not None, True)
    check("timestamp rfc3339 parses",
          parse_ts("2026-07-20T11:22:33Z INFO hello") is not None, True)

    m = RE_REPLENISH.search(
        "2026-07-20 11:22:33 INFO [connmgr] replenish check: conns=6 "
        "pending=1 target=8 dialing=2")
    check("replenish parsed", None if not m else m.groups(),
          ("6", "1", "8", "2"))
    m = RE_FRONTIER.search("2026-07-20 11:22:33 INFO [frontier] height=653071 gap=42")
    check("frontier parsed", None if not m else m.group(1), "653071")

    if failures:
        sys.stderr.write("SELF-TEST FAILED (%d):\n" % len(failures))
        for f in failures:
            sys.stderr.write("  - %s\n" % f)
        return 1
    sys.stderr.write("self-test OK: all bucket, anchor and timestamp "
                     "assertions passed\n")
    return 0

# ---------------------------------------------------------------------------
def main(argv):
    log_path = None
    out_path = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--self-test":
            return self_test()
        elif a == "--log":
            i += 1; log_path = argv[i]
        elif a == "--out":
            i += 1; out_path = argv[i]
        elif a in ("-h", "--help"):
            sys.stderr.write(__doc__ or "")
            sys.stderr.write(
                "usage: netsync_soak_scrape.sh [--log FILE] [--out FILE] "
                "[--self-test]\n")
            return 0
        else:
            sys.stderr.write("unknown argument: %s\n" % a)
            return 1
        i += 1

    fh = open(log_path, "r", errors="replace") if log_path else sys.stdin
    out = open(out_path, "w") if out_path else sys.stdout
    try:
        return scrape(fh, out)
    finally:
        if log_path:
            fh.close()
        if out_path:
            out.close()

sys.exit(main(sys.argv[1:]))
PYEOF
