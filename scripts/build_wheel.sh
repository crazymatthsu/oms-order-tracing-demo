#!/usr/bin/env bash
# Build the oms_trace wheel the workers import (docs/deployment.md step 4). The pip index comes
# from repos.env / repos.local.env, never from a hardcoded URL.
#
#   bash scripts/build_wheel.sh            # -> dist/oms_order_tracing-<version>-py3-none-any.whl
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/repos.sh"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if ! PYTHON_BIN="$(find_python3 3.10)"; then echo "ERROR: Python 3.10+ is required" >&2; exit 1; fi
VENV_DIR="$ROOT/.venv"
[ -d "$VENV_DIR" ] || "$PYTHON_BIN" -m venv "$VENV_DIR"
PY="$(venv_python "$VENV_DIR")"
"$PY" -m pip install --quiet --disable-pip-version-check build
rm -rf "$ROOT/dist"
"$PY" -m build --wheel --outdir "$ROOT/dist" "$ROOT" >/dev/null
ls -1 "$ROOT"/dist/*.whl
