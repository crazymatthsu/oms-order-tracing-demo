#!/usr/bin/env bash
# Step 1 of docs/deployment.md: is this cluster what the design was written for, and can the
# deployment scripts reach what they need? Read-only; exit 1 on a blocking finding.
#
#   bash scripts/check_env.sh            # with deploy/deploy.env
#   DRY_RUN=1 bash scripts/check_env.sh  # print the commands only
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_lib.sh"
deploy_load
failures=0

step "dhconfig reachable (${DHCONFIG})"
if dh version >/dev/null 2>&1; then
  version="$(dh version 2>/dev/null | head -1)"
  ok "dhconfig version: $version"
  if [ -n "${DH_EXPECTED_VERSION:-}" ] && ! printf '%s' "$version" | grep -q "$DH_EXPECTED_VERSION"; then
    warn "expected $DH_EXPECTED_VERSION (docs/design/01 Q10): re-run the API confirmation of docs/design/05 section 11 on this release"
  fi
else
  warn "dhconfig not reachable; set DHCONFIG (e.g. 'podman exec -i deephaven-infra dhconfig') or run on the infra node"
  failures=$((failures + 1))
fi

step "DIS storage root ${DIS_STORAGE_ROOT} (shared filesystem mounted on merge and query nodes)"
if [ -d "$DIS_STORAGE_ROOT" ]; then
  ok "exists here; confirm it is the same mount on every merge and query node"
else
  warn "not visible from this host (fine when dhconfig runs in the infra container; verify on the nodes)"
fi

step "the scripts the PQs run"
for f in queries/oms_trace/lineage.py queries/oms_trace/ingest.py queries/oms_trace/ingest_amps.py \
         queries/oms_trace/drift_writer.py queries/oms_trace/ui.py; do
  [ -f "$DEPLOY_ROOT/$f" ] && ok "$f" || { warn "missing $f"; failures=$((failures + 1)); }
done

step "schemas and PQ definitions"
for f in "$DEPLOY_ROOT"/schemas/*.schema; do ok "$(basename "$f")"; done
for f in "$DEPLOY_ROOT"/pq/*.json; do ok "$(basename "$f")"; done

step "PQ definitions validate (oms_trace.pqdefs)"
PY="$(venv_python "$DEPLOY_ROOT/.venv" 2>/dev/null || true)"
if [ -z "$PY" ]; then PY="$(command -v python3 || true)"; fi
if [ -n "$PY" ]; then
  if PYTHONPATH="$DEPLOY_ROOT/src" "$PY" -m oms_trace.pqdefs --check "$DEPLOY_ROOT/pq" >/dev/null; then
    ok "every definition names an existing script, a known type and valid settings"
  else
    warn "pq/*.json failed validation"; failures=$((failures + 1))
  fi
else
  warn "no python3: skipped the definition check"
fi

step "the controller's Git repository (docs/git-persistent-query-scripts.md)"
if dh pq check-repo >/dev/null 2>&1; then
  ok "dhconfig pq check-repo passed"
else
  warn "dhconfig pq check-repo failed or not yet configured: run scripts/deploy_git_repo.sh (step 5)"
fi

step "connectivity from this host (informational)"
for hp in ${KAFKA_BOOTSTRAP//,/ }; do
  host="${hp%%:*}"; port="${hp##*:}"
  if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then ok "kafka $hp reachable"; else warn "kafka $hp not reachable from here (the merge nodes must reach it)"; fi
done
if [ -n "${OMS_AMPS_URI:-}" ]; then
  hostport="$(printf '%s' "$OMS_AMPS_URI" | sed -e 's#^[a-z]*://##' -e 's#/.*##' -e 's#^.*@##')"
  host="${hostport%%:*}"; port="${hostport##*:}"
  if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then ok "amps $host:$port reachable"; else warn "amps $host:$port not reachable from here"; fi
fi

if [ "$failures" -gt 0 ]; then die "$failures blocking finding(s)"; fi
step "environment check passed"
