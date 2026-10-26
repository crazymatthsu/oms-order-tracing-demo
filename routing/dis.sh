#!/usr/bin/env bash
# One in-worker Data Import Server per system with private storage and an exclusive claim
# (docs/design/05 section 2). Run on a node with the DIS storage mounted; validate the routing
# afterwards. Command names: confirm against the installed release (dhconfig dis --help).
set -euo pipefail
STORAGE_ROOT="${DIS_STORAGE_ROOT:-/db/dataImportServers}"
for X in a b c; do
  U="$(printf '%s' "$X" | tr '[:lower:]' '[:upper:]')"
  dhconfig dis add --name "oms_ingest_$X" --storage "$STORAGE_ROOT/oms_ingest_$X" --claim "OMS_RAW.Orders$U"
done
dhconfig dis list
