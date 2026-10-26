#!/usr/bin/env bash
#
# The Community demo: bash scripts/run_demo.sh [up|down|status|logs|export-files]
#
#   up            start the server (images from repos.env), wait for health, print the URL
#   down          stop and remove it
#   status/logs   compose ps / compose logs -f
#   export-files  write the scenario book as JSON lines into docker/files (for OMS_TRACE_SOURCE=files)
#
# Uses podman compose when podman is installed, else docker compose.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/repos.sh"   # DEEPHAVEN_IMAGE etc., override-aware (never sourced, parsed)

COMPOSE_FILE="$ROOT/docker/docker-compose.yml"
if command -v podman >/dev/null 2>&1; then COMPOSE=(podman compose); else COMPOSE=(docker compose); fi

case "${1:-up}" in
  up)
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d
    echo "[oms-trace] waiting for the server..."
    for _ in $(seq 1 60); do
      if "${COMPOSE[@]}" -f "$COMPOSE_FILE" ps 2>/dev/null | grep -q "healthy"; then break; fi
      sleep 2
    done
    echo "[oms-trace] open http://localhost:${DH_PORT:-10000}/ide  -> Panels: drift_edges, drift_by_a_order, feed_health, oms_trace_dashboard"
    ;;
  down) "${COMPOSE[@]}" -f "$COMPOSE_FILE" down ;;
  status) "${COMPOSE[@]}" -f "$COMPOSE_FILE" ps ;;
  logs) "${COMPOSE[@]}" -f "$COMPOSE_FILE" logs -f ;;
  export-files)
    PY="$(venv_python "$ROOT/.venv" 2>/dev/null || find_python3 3.10)"
    PYTHONPATH="$ROOT/src" "$PY" -m oms_trace.trace_check --export-mock "$ROOT/docker/files"
    ;;
  *) echo "usage: $0 [up|down|status|logs|export-files]" >&2; exit 2 ;;
esac
