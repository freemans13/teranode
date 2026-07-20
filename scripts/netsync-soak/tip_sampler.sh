#!/usr/bin/env bash
#
# tip_sampler.sh -- sample the committed chain tip on a fixed interval.
#
# The tip series is the load-bearing input to the soak report: blocks/hr comes
# from its slope and the stick-duration distribution comes from the intervals
# where it does not move. A "stick" is a stretch where the node has committed
# no new block; on a far-ahead node that is the symptom this whole project is
# chasing, so the sampler must keep running unattended for hours.
#
# Credentials are NEVER hardcoded and NEVER printed. The connection URL is
# discovered, in order:
#
#   1. $TERANODE_SOAK_DB_URL         -- explicit override, postgres://... URL
#   2. $blockchain_store             -- as exported into the node's environment
#   3. blockchain_store[.CONTEXT] in settings_local.conf, then settings.conf,
#      resolved against $SETTINGS_CONTEXT
#
# The password is handed to psql through PGPASSWORD in the child environment
# only; it is never echoed, never placed on a command line (where it would be
# visible in `ps`), and never written to the output file.
#
# Usage:
#   ./tip_sampler.sh --out /root/soak/tips.tsv
#   INTERVAL=10 ./tip_sampler.sh --out /root/soak/tips.tsv
#   nohup ./tip_sampler.sh --out /root/soak/tips.tsv >/root/soak/tip_sampler.log 2>&1 &
#
# Output is TSV: epoch_seconds <TAB> iso8601_utc <TAB> height
# Appends, so an interrupted run can simply be restarted against the same file.
#
# NOTE ON INTERVAL: the plan's acceptance gate is "stick max < 60s". A 60s
# sampling interval cannot resolve that -- the smallest stick it can see is one
# sample period. Use INTERVAL=10 for any window you intend to judge the stick
# gate on. soak_report.py detects a too-coarse interval and says so rather than
# reporting a number it cannot support.

set -euo pipefail

INTERVAL="${INTERVAL:-60}"
OUT=""
SETTINGS_DIR="${SETTINGS_DIR:-/root}"

while [ $# -gt 0 ]; do
  case "$1" in
    --out)      OUT="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

[ -n "$OUT" ] || { echo "tip_sampler: --out FILE is required" >&2; exit 1; }

# --- discover the connection URL -------------------------------------------
find_url() {
  if [ -n "${TERANODE_SOAK_DB_URL:-}" ]; then
    printf '%s' "$TERANODE_SOAK_DB_URL"; return 0
  fi
  if [ -n "${blockchain_store:-}" ]; then
    printf '%s' "$blockchain_store"; return 0
  fi

  ctx="${SETTINGS_CONTEXT:-}"
  for f in "$SETTINGS_DIR/settings_local.conf" "$SETTINGS_DIR/settings.conf" \
           ./settings_local.conf ./settings.conf; do
    [ -f "$f" ] || continue
    # Most specific first: the context-qualified key, then the bare key.
    for key in ${ctx:+"blockchain_store.$ctx"} "blockchain_store"; do
      line=$(grep -E "^[[:space:]]*${key}[[:space:]]*=" "$f" 2>/dev/null | tail -1 || true)
      if [ -n "$line" ]; then
        url=$(printf '%s' "$line" | sed -E 's/^[^=]*=[[:space:]]*//; s/[[:space:]]*$//')
        case "$url" in
          postgres://*|postgresql://*) printf '%s' "$url"; return 0 ;;
        esac
      fi
    done
  done
  return 1
}

URL="$(find_url || true)"
if [ -z "$URL" ]; then
  cat >&2 <<'MSG'
tip_sampler: could not discover a postgres blockchain_store URL.
Set one of:
  TERANODE_SOAK_DB_URL=postgres://user:pass@host:port/db
  SETTINGS_CONTEXT=<context>   (with settings_local.conf present in $SETTINGS_DIR)
MSG
  exit 1
fi

# Settings files may embed ${POSTGRES_PORT} and similar. Expand only variables
# that are actually present in the environment; anything left unresolved is a
# hard error rather than a mystery connection failure later.
URL="$(python3 - "$URL" <<'PY'
import os, re, sys
u = sys.argv[1]
def sub(m):
    v = os.environ.get(m.group(1))
    if v is None:
        sys.stderr.write("tip_sampler: unresolved ${%s} in blockchain_store URL\n" % m.group(1))
        sys.exit(1)
    return v
sys.stdout.write(re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", sub, u))
PY
)"

# Split the URL into psql parameters so the password can travel via PGPASSWORD
# instead of a command line that `ps` would expose.
eval "$(python3 - "$URL" <<'PY'
import sys, shlex
from urllib.parse import urlparse, unquote
u = urlparse(sys.argv[1])
def emit(k, v):
    print("%s=%s" % (k, shlex.quote(v or "")))
emit("PGHOST", u.hostname or "localhost")
emit("PGPORT", str(u.port or 5432))
emit("PGUSER", unquote(u.username or ""))
emit("PGPASSWORD", unquote(u.password or ""))
emit("PGDATABASE", (u.path or "/").lstrip("/"))
PY
)"
export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

# Report where we are pointed WITHOUT the credentials.
echo "tip_sampler: sampling ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE} every ${INTERVAL}s -> ${OUT}" >&2

command -v psql >/dev/null 2>&1 || { echo "tip_sampler: psql not found on PATH" >&2; exit 1; }

# Fail fast on a bad credential rather than silently writing an empty series
# for three hours.
if ! psql -qtAX -c 'SELECT 1' >/dev/null 2>&1; then
  echo "tip_sampler: cannot connect to postgres (credentials or host wrong)" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")"

trap 'echo "tip_sampler: stopping" >&2; exit 0' INT TERM

while :; do
  # -qtAX: quiet, tuples only, unaligned, ignore ~/.psqlrc -- a stray psqlrc on
  # the container would otherwise inject banner text into the data file.
  height="$(psql -qtAX -c 'SELECT max(height) FROM blocks;' 2>/dev/null | tr -d '[:space:]' || true)"
  now="$(date -u +%s)"
  iso="$(date -u -d "@$now" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [ -n "$height" ] && [ "$height" != "" ]; then
    printf '%s\t%s\t%s\n' "$now" "$iso" "$height" >> "$OUT"
  else
    # A dropped sample is data too: a gap here means the DB was unreachable, and
    # the report must not mistake that gap for a genuine chain stick.
    printf '%s\t%s\t%s\n' "$now" "$iso" "ERR" >> "$OUT"
  fi
  sleep "$INTERVAL"
done
