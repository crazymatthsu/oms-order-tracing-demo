#!/usr/bin/env bash
# Step 4 of docs/deployment.md: put the oms_trace wheel (and the AMPS client when the feeds are
# AMPS) into the Core+ Python environment of every query and merge node, so `from oms_trace import
# app` works in a worker without OMS_TRACE_SRC.
#
#   bash scripts/build_wheel.sh && bash scripts/install_worker_venv.sh            # every node in WORKER_NODES over ssh
#   bash scripts/install_worker_venv.sh --local                                    # this node only
#
# COREPLUS_VENV is the venv the Core+ workers run from; confirm the path on the installed release
# (docs/design/07 section 6: build worker environments from the release's frozen-requirements file,
# never from floating versions). The install runs as irisadmin. The pip index is the one repos.env
# names, so a JFrog-proxied company needs no change here.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/repos.sh"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_lib.sh"
deploy_load
: "${COREPLUS_VENV:=/usr/illumon/dnd/venv}"

wheel="$(ls -1 "$DEPLOY_ROOT"/dist/*.whl 2>/dev/null | head -1 || true)"
[ -n "$wheel" ] || die "no wheel in dist/: run scripts/build_wheel.sh first"
extras=""
[ -n "${OMS_AMPS_URI:-}" ] && extras="amps-python-client>=5.3,<6"

install_cmd="sudo -u irisadmin $COREPLUS_VENV/bin/pip install --disable-pip-version-check --index-url '$PIP_INDEX_URL'"
[ -n "${PIP_TRUSTED_HOST:-}" ] && install_cmd="$install_cmd --trusted-host '$PIP_TRUSTED_HOST'"

if [ "${1:-}" = "--local" ]; then
  step "installing $(basename "$wheel") into $COREPLUS_VENV on this node"
  run sh -c "$install_cmd '$wheel' $extras"
  exit 0
fi

: "${WORKER_NODES:?set WORKER_NODES in deploy/deploy.env (space separated) or use --local}"
for node in $WORKER_NODES; do
  step "$node: copying and installing $(basename "$wheel")"
  run scp -q "$wheel" "$node:/tmp/$(basename "$wheel")"
  run ssh "$node" "$install_cmd '/tmp/$(basename "$wheel")' $extras && rm -f '/tmp/$(basename "$wheel")'"
  run ssh "$node" "sudo -u irisadmin $COREPLUS_VENV/bin/python -c 'import oms_trace, sys; print(\"oms_trace ok\", sys.version)'"
done
