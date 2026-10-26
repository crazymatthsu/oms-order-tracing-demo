#!/usr/bin/env bash
#
# Create/refresh the virtualenv and run the pytest suite.
#
#   bash run_tests.sh                      # pure-python suite (oracle, mock book, config, AMPS fold, check)
#   OMS_TRACE_DH_TEST=1 bash run_tests.sh  # also the embedded-engine tests (installs deephaven-server 42.5)
#   OMS_TRACE_SKIP_AMPS=1 bash run_tests.sh # without the AMPS client wheel (its tests then skip)
#
# Artifact sources (pip index, images) come from repos.env and your override file; the files are
# parsed, never sourced. Portable across macOS bash 3.2, Linux and Git Bash (no GNU-only flags).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/scripts/repos.sh"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if ! PYTHON_BIN="$(find_python3 3.10)"; then
  echo "ERROR: Python 3.10+ is required (set PYTHON=/path/to/python3)." >&2
  exit 1
fi

VENV_DIR="$ROOT/.venv"
[ -d "$VENV_DIR" ] || { echo "[oms-trace] creating virtualenv at $VENV_DIR"; "$PYTHON_BIN" -m venv "$VENV_DIR"; }
VENV_PYTHON="$(venv_python "$VENV_DIR")"

EXTRAS="test"
if [ "${OMS_TRACE_SKIP_AMPS:-0}" != "1" ]; then EXTRAS="$EXTRAS,amps"; fi
if [ "${OMS_TRACE_DH_TEST:-0}" = "1" ]; then EXTRAS="$EXTRAS,engine"; fi
"$VENV_PYTHON" -m pip install --quiet --disable-pip-version-check --editable ".[$EXTRAS]"

exec "$VENV_PYTHON" -m pytest tests/ -q "$@"
