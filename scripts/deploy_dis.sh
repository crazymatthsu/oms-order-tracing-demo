#!/usr/bin/env bash
# Step 3 of docs/deployment.md: one in-worker Data Import Server per raw table with private storage
# and an exclusive claim, plus the derived-results DIS (docs/design/05 section 2); then validate the
# routing. Run where the DIS storage is mounted (or through the infra container).
#
#   bash scripts/deploy_dis.sh
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_lib.sh"
deploy_load

step "Data Import Servers under ${DIS_STORAGE_ROOT}"
for X in a b c; do
  U="$(printf '%s' "$X" | tr '[:lower:]' '[:upper:]')"
  run dh dis add --name "oms_ingest_$X" --storage "$DIS_STORAGE_ROOT/oms_ingest_$X" --claim "OMS_RAW.Orders$U"
done
# oms-drift-writer appends OMS_TRACE.DriftEvents through the Derived Table Writer (docs/design/05 section 8.1)
run dh dis add --name oms_trace --storage "$DIS_STORAGE_ROOT/oms_trace" --claim OMS_TRACE

step "routing"
run dh dis list
run dh routing validate

cat <<'EOF'

    Verify from a Core+ console that the Table Data Service sees the claims (an empty table, not an error):
        db.live_table("OMS_RAW", "OrdersA")
        db.live_table("OMS_TRACE", "DriftEvents")
EOF
