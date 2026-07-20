# netsync soak harness

Phase 0 of the IBD peer-robustness work: the measurement harness that makes
every other fix judgeable. Nothing in this project can be accepted or rejected
without it — it is the denominator.

The node under test runs on the Hetzner box in the `mainnet` LXC container, logs
to `/root/logs/teranode.log`, and keeps the `blocks` table in the local
postgres. These scripts are POSIX `sh`/`bash` and `python3` only: no pip
packages, no network calls, nothing to install on the container.

## What's here

| File | Role |
| --- | --- |
| `netsync_soak_scrape.sh` | Log → JSONL event stream (disconnects, connection levels, frontier heights, restart markers) |
| `tip_sampler.sh` | Long-running sampler: committed chain tip from postgres, on an interval |
| `soak_report.py` | The two above → five judgeable numbers, human or `--json` |
| `make_fixture.py` | Synthetic log + tip series, used to prove the parsing |

## Why the bucketing looks the way it does

Every peer teardown funnels through exactly one log site
(`services/legacy/peer/peer.go`, `DisconnectWithLogFunc`):

```text
Disconnecting (%s) reason: %s
```

One grep for that anchor catches 100% of disconnects with no double-counting.
Please do not add a second disconnect pattern — a peer whose teardown crosses two
code paths would then be counted twice and inflate the project's denominator.

**The reason is bucketed on stable substrings, never on a rendered duration.**
The idle reason is built as `fmt.Sprintf("No answer from peer for %s", ...)` with
a Go `time.Duration`, so it renders `2m5s` today and `20m0s` once F3 raises the
IBD timeout. A bucketer keyed on the rendered duration would silently stop
matching at exactly the moment the fix it exists to measure lands — an
instrument that breaks when the experiment succeeds. `--self-test` fails the
script if `20m0s` and `2m5s` ever stop landing in the same bucket.

Matching is substring, not left-anchored prefix, because several real reasons
carry a leading clause (`Received block header that does not properly
connect...`).

Anything unrecognised lands in `other` **and has its literal text printed**, by
both the scraper and the report, so a reason added upstream shows up as a named
unknown rather than being quietly absorbed into a known bucket. When you see
one, add a rule to `BUCKET_RULES` in `netsync_soak_scrape.sh`.

## Deploy to the container

```bash
# from the repo on the mac
scp scripts/netsync-soak/* root@<hetzner>:/root/soak/
ssh root@<hetzner> 'chmod +x /root/soak/*.sh /root/soak/*.py'
```

Or, if you work inside the container: `lxc shell mainnet`, then copy into
`/root/soak/`.

Sanity-check the bucketer before trusting a single number from it:

```bash
/root/soak/netsync_soak_scrape.sh --self-test    # must exit 0
```

## Capture a baseline (>= 3 hours)

The tip sampler is the long pole: start it first and leave it running.

```bash
mkdir -p /root/soak/run
cd /root/soak

# INTERVAL=10 matters -- see "Sampling interval" below.
INTERVAL=10 nohup ./tip_sampler.sh --out /root/soak/run/tips.tsv \
    > /root/soak/run/tip_sampler.log 2>&1 &
```

The sampler discovers its postgres connection itself, in this order:

1. `TERANODE_SOAK_DB_URL` — explicit `postgres://user:pass@host:port/db`
2. `blockchain_store` — as exported into the node's environment
3. `blockchain_store[.$SETTINGS_CONTEXT]` in `settings_local.conf`, then
   `settings.conf` (looked for in `$SETTINGS_DIR`, default `/root`)

No password is ever hardcoded, printed, or written to the output. It reaches
`psql` through `PGPASSWORD` in the child environment only — never on a command
line, where `ps` would expose it. If discovery fails the script says so and
exits rather than guessing.

Let it soak for at least three hours, **in the far-ahead regime** (see below).
Then build the reference window:

```bash
./netsync_soak_scrape.sh --log /root/logs/teranode.log --out run/events.jsonl
./soak_report.py --events run/events.jsonl --tips run/tips.tsv            # eyeball it
./soak_report.py --events run/events.jsonl --tips run/tips.tsv --json > reference.json
```

Keep `reference.json` somewhere safe. It is the baseline every later window is
argued against.

## Diff a later window against the baseline

After a fix lands, repeat the capture into a fresh directory and compare:

```bash
./soak_report.py --events run2/events.jsonl --tips run2/tips.tsv --json > post-fix.json
diff <(python3 -m json.tool reference.json) <(python3 -m json.tool post-fix.json)
```

Both windows must be far-ahead and of comparable length, or the comparison is
not meaningful — a shorter window has fewer chances to show a rare long stick.

## The regime guard (this is the important bit)

A cold node has no backlog and therefore no sticks, so it **always looks green**.
Every number in this report is meaningless unless the node has soaked back into
the far-ahead regime: committed tip chronically lagging the header frontier,
downstream idle. That is a precondition, not a caveat, so `soak_report.py`
machine-enforces it and **refuses to emit (exit 2)** when:

- the window contains a process-start/shutdown marker — a restart means the node
  came back cold and the backlog was reset;
- there is no `[frontier]` series, so the backlog cannot be established at all;
- the median backlog (frontier height − tip height) is below `--min-backlog`
  (default 100 blocks);
- the tip advanced at every single sample — it climbed monotonically with
  nothing queued behind it, the cold-node signature.

`--i-know-this-is-not-far-ahead` overrides the refusal, but the bypass is stamped
into the report itself and into the JSON as `regime_guard_bypassed` /
`valid_for_acceptance: false`, so a green number captured in the wrong regime
cannot be quoted later as if it were valid. If a window is refused, the answer is
to let the node soak and capture again — not to reach for the flag.

## Sampling interval

The stick gate is "max < 60s". A 60s sampler **cannot resolve that**: its
smallest observable stick is one sample period, so a pass would be an artefact of
the sampling rate rather than evidence about the node. Use `INTERVAL=10` for any
window you intend to judge the stick gate on. The report detects a too-coarse
interval and says the gate is not judgeable rather than printing a number it
cannot support.

This is not theoretical: the same fixture window sampled at 10s reports a true
max stick of 190s with 35 sticks over 180s; sampled at 60s the same window
reports max 180s and *zero* over 180s.

## Acceptance gates

From the plan — what "good" looks like:

| Metric | Gate | Where in the report |
| --- | --- | --- |
| Stick duration | **max < 60s** | §4, needs `INTERVAL=10` |
| Self-inflicted disconnects | **<= ~5 /hr** | §1, `SELF-INFLICTED TOTAL` |
| Connection level | **conns >= target in >= 95% of samples** | §2 |
| Throughput | **sustained blk/hr, sawtooth gone** | §3 + §4 (`over_60s`/`over_180s` → 0) |

"Self-inflicted" means the buckets we cause and can therefore fix: `idle`,
`stalled_or_misbehaving`, `head_of_line`, `unrequested_headers`,
`unrequested_block`, `nonconnecting_headers`, `sync_peer_rotation`. Buckets like
`max_peers`, `duplicate_connection` and `banned` are inbound-pressure or
peer-quality events — worth watching, but not what the <=5/hr gate measures.

The sawtooth ("busy then quiet then busy") shows up as a healthy blk/hr average
hiding a bimodal stick distribution: watch `p90` and `over_60s`, not just the
mean. A window can hit its blocks/hr target and still be badly sawtoothed.

## Self-check / development

```bash
./netsync_soak_scrape.sh --self-test          # bucketer, anchor regex, timestamps

# prove the pipeline end to end against known input
./make_fixture.py --out-dir /tmp/fix --regime far-ahead
./netsync_soak_scrape.sh --log /tmp/fix/teranode.log --out /tmp/fix/events.jsonl
./soak_report.py --events /tmp/fix/events.jsonl --tips /tmp/fix/tips.tsv

# the guard must refuse these two (exit 2)
./make_fixture.py --out-dir /tmp/cold --regime cold
./make_fixture.py --out-dir /tmp/restart --regime restart
```

## Input formats

`tips.tsv` — appended by the sampler, so an interrupted run can just be
restarted against the same file:

```text
epoch_seconds <TAB> iso8601_utc <TAB> height
```

A height of `ERR` records a sampler-side failure (postgres unreachable). Those
rows are counted and excluded: treating a monitoring outage as a chain stick
would manufacture the exact symptom we are hunting.

`events.jsonl` — one JSON object per line, `kind` in
`disconnect` / `conns` / `frontier` / `proc_start`.
