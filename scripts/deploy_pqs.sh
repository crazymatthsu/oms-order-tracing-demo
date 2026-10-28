#!/usr/bin/env bash
# Step 6 of docs/deployment.md: create or update every oms-* Persistent Query from pq/*.json.
#
#   bash scripts/deploy_pqs.sh --dry-run          # the definitions as a table (for the Query Monitor the first time)
#   bash scripts/deploy_pqs.sh                    # through the Core+ controller client (needs deephaven_enterprise)
#   bash scripts/deploy_pqs.sh --only oms-lineage # one PQ
#   bash scripts/deploy_pqs.sh --export           # dhconfig pq export of every oms-* PQ into pq/export/ (versioned)
#
# The controller-client calls follow the Core+ Python documentation as read at design time and are
# marked confirm on 2026.01.054 in src/oms_trace/pqdefs.py; the definitions themselves are the
# source of truth whatever the API spelling turns out to be.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/repos.sh"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_lib.sh"
deploy_load

if [ "${1:-}" = "--export" ]; then
  mkdir -p "$DEPLOY_ROOT/pq/export"
  for f in "$DEPLOY_ROOT"/pq/*.json; do
    name="$(basename "$f" .json)"
    run dh pq export --name "$name" --file "$DEPLOY_ROOT/pq/export/$name.xml"
  done
  exit 0
fi

# The Core+ Python environment (deephaven_enterprise) when applying; any python for --dry-run.
PY="${COREPLUS_PYTHON:-}"
if [ -z "$PY" ]; then PY="$(venv_python "$DEPLOY_ROOT/.venv" 2>/dev/null || find_python3 3.10)"; fi
PYTHONPATH="$DEPLOY_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" "$PY" -m oms_trace.pqdefs --apply "$DEPLOY_ROOT/pq" "$@"
