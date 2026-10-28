#!/usr/bin/env bash
# Moved: the Data Import Server definitions are created by scripts/deploy_dis.sh (docs/deployment.md
# step 3), which reads DIS_STORAGE_ROOT and DHCONFIG from deploy/deploy.env. This wrapper keeps the
# documented path working.
set -euo pipefail
exec bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/scripts/deploy_dis.sh" "$@"
