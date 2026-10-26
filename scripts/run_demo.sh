#!/usr/bin/env bash
#
# The Community demo: bash scripts/run_demo.sh [up|down|status|logs|export-files|up-amps|publish-amps|down-amps]
#
#   up            start the server (images from repos.env), wait for health, print the URL
#   down          stop and remove it
#   status/logs   compose ps / compose logs -f
#   export-files  write the scenario book as JSON lines into docker/files (for OMS_TRACE_SOURCE=files)
#   up-amps       the AMPS overlay (docs/design/11): the derived image with the AMPS client, a local
#                 broker (profile `broker`, AMPS_IMAGE from repos.env) and OMS_TRACE_SOURCE=amps
#   publish-amps  publish the scenario book into the broker's SOW topics (then watch the panels fill)
#   down-amps     stop the overlay stack
#
# Uses podman compose when podman is installed, else docker compose.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/repos.sh"   # DEEPHAVEN_IMAGE etc., override-aware (never sourced, parsed)

COMPOSE_FILE="$ROOT/docker/docker-compose.yml"
AMPS_OVERLAY="$ROOT/docker/docker-compose.amps.yml"
if command -v podman >/dev/null 2>&1; then COMPOSE=(podman compose); CLI=podman; else COMPOSE=(docker compose); CLI=docker; fi

wait_healthy() {
  echo "[oms-trace] waiting for the server..."
  for _ in $(seq 1 90); do
    if "$@" ps 2>/dev/null | grep -q "healthy"; then break; fi
    sleep 2
  done
  echo "[oms-trace] open http://localhost:${DH_PORT:-10000}/ide  -> Panels: drift_edges, drift_by_a_order, feed_health, oms_trace_dashboard"
}

case "${1:-up}" in
  up)
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d
    wait_healthy "${COMPOSE[@]}" -f "$COMPOSE_FILE"
    ;;
  down) "${COMPOSE[@]}" -f "$COMPOSE_FILE" down ;;
  up-amps)
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" -f "$AMPS_OVERLAY" --profile broker up -d --build
    wait_healthy "${COMPOSE[@]}" -f "$COMPOSE_FILE" -f "$AMPS_OVERLAY" --profile broker
    echo "[oms-trace] now: bash scripts/run_demo.sh publish-amps   (amps_topics shows the subscriptions)"
    ;;
  publish-amps)
    # the client library lives in the derived image; publish from inside the container (no absolute
    # path arguments, so Git Bash needs no MSYS_NO_PATHCONV)
    "$CLI" exec "${DH_CONTAINER:-oms-trace-deephaven}" python -m oms_trace.amps_publish \
      --uri "${OMS_AMPS_URI:-tcp://amps:9007/amps/json}" --mock --seed "${OMS_TRACE_MOCK_SEED:-42}" --families "${OMS_TRACE_MOCK_FAMILIES:-24}"
    ;;
  down-amps) "${COMPOSE[@]}" -f "$COMPOSE_FILE" -f "$AMPS_OVERLAY" --profile broker down ;;
  status) "${COMPOSE[@]}" -f "$COMPOSE_FILE" ps ;;
  logs) "${COMPOSE[@]}" -f "$COMPOSE_FILE" logs -f ;;
  export-files)
    PY="$(venv_python "$ROOT/.venv" 2>/dev/null || find_python3 3.10)"
    PYTHONPATH="$ROOT/src" "$PY" -m oms_trace.trace_check --export-mock "$ROOT/docker/files"
    ;;
  *) echo "usage: $0 [up|down|status|logs|export-files|up-amps|publish-amps|down-amps]" >&2; exit 2 ;;
esac
