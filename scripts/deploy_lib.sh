#!/usr/bin/env bash
# Shared helpers for the deployment scripts (docs/deployment.md). Sourced, never executed.
# Portable across bash 3.2 (macOS), Linux and Git Bash: no arrays beyond simple ones, no GNU-only flags.
#
#   deploy_load            load deploy/deploy.env (or $DEPLOY_ENV), then export the keys it defines
#   dh <args>              run dhconfig (locally or through the infra container, see DHCONFIG)
#   require_cmd <name>     fail with a clear message when a command is missing
#   step / ok / warn / die logging

DEPLOY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

step() { printf '\n==> %s\n' "$*"; }
ok()   { printf '    ok   %s\n' "$*"; }
warn() { printf '    WARN %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "'$1' is not on PATH ($2)"
}

# Load deploy/deploy.env: KEY=value lines, '#' comments, no shell expansion of values beyond quotes.
deploy_load() {
  local file="${DEPLOY_ENV:-$DEPLOY_ROOT/deploy/deploy.env}"
  [ -f "$file" ] || die "no $file: copy deploy/deploy.env.example to deploy/deploy.env and fill it in"
  local line key value
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    key="${line%%=*}"; value="${line#*=}"
    key="$(printf '%s' "$key" | tr -d '[:space:]')"
    case "$key" in *[!A-Za-z0-9_]*|'') continue ;; esac
    value="${value%\"}"; value="${value#\"}"
    # the environment wins over the file (same precedence as repos.env)
    if [ -z "$(eval "printf '%s' \"\${$key:-}\"")" ]; then
      export "$key=$value"
    fi
  done < "$file"
  : "${DHCONFIG:=dhconfig}"
  : "${DIS_STORAGE_ROOT:=/db/dataImportServers}"
  : "${GIT_REPO_NAME:=oms_trace}"
  : "${OMS_TRACE_TZ:=America/New_York}"
}

# dhconfig, locally or `podman exec -i <infra> dhconfig` when DHCONFIG says so.
dh() {
  # shellcheck disable=SC2086 - DHCONFIG is a command line on purpose
  $DHCONFIG "$@"
}

# Confirmation marker for commands whose exact flags come from the Core+ documentation as read at
# design time (docs/design/05 section 11): print the command before running it so a difference on
# the installed release is visible at once.
run() {
  printf '    $ %s\n' "$*"
  if [ "${DRY_RUN:-0}" = "1" ]; then return 0; fi
  "$@"
}
