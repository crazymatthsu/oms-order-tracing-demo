#!/usr/bin/env bash
# Step 2 of docs/deployment.md: import every schema under schemas/ (OMS_RAW.Orders{A,B,C},
# OMS_TRACE.DriftEvents) and list what the cluster now knows. Idempotent: re-importing an
# unchanged schema is a no-op; a changed one must be additive (docs/design/07 section 10).
#
#   bash scripts/deploy_schemas.sh
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_lib.sh"
deploy_load

step "importing schemas (dhconfig schema import; confirm the flag names on the installed release)"
for f in "$DEPLOY_ROOT"/schemas/*.schema; do
  if printf '%s' "$DHCONFIG" | grep -q "podman exec"; then
    # the file lives on the host: stream it into the container
    printf '    $ %s schema import --file - < %s\n' "$DHCONFIG" "$(basename "$f")"
    [ "${DRY_RUN:-0}" = "1" ] || dh schema import --file /dev/stdin < "$f"
  else
    run dh schema import --file "$f"
  fi
done

step "schemas known to the cluster"
run dh schema list --namespace OMS_RAW
run dh schema list --namespace OMS_TRACE
