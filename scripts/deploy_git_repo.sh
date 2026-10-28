#!/usr/bin/env bash
# Step 5 of docs/deployment.md: tell the Persistent Query Controller to pull this repository over
# SSH and expose queries/oms_trace (docs/git-persistent-query-scripts.md, Podman section).
#
#   bash scripts/deploy_git_repo.sh            # writes deploy/iris-environment.oms_trace.prop and imports it
#   DRY_RUN=1 bash scripts/deploy_git_repo.sh  # write the fragment only
#
# Before this: irisadmin's SSH key pair and known_hosts exist under /db/TempFiles/irisadmin/.ssh
# in the infra container (no passphrase, mode 600) and the public key is a read-only deploy key
# on the repository. After this: restart the controller so it clones, then `dhconfig pq check-repo`.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_lib.sh"
deploy_load
: "${GIT_REPO_URI:?set GIT_REPO_URI in deploy/deploy.env}"
: "${GIT_REPO_BRANCH:=main}"
: "${GIT_REPO_ROOT:=/db/TempFiles/irisadmin/git/$GIT_REPO_NAME}"

frag="$DEPLOY_ROOT/deploy/iris-environment.$GIT_REPO_NAME.prop"
step "writing the controller properties fragment $frag"
cat > "$frag" <<EOF
# Git-sourced Persistent Query scripts for the OMS order tracing PQs (docs/deployment.md step 5).
# Append to the controller's iris-environment.prop (or import with dhconfig properties) and restart
# the controller. Property names per docs/git-persistent-query-scripts.md; confirm on the installed release.
iris.scripts.repos=$GIT_REPO_NAME
PersistentQueryController.scriptUpdateEnabled=true
iris.scripts.repo.$GIT_REPO_NAME.uri=$GIT_REPO_URI
iris.scripts.repo.$GIT_REPO_NAME.branch=$GIT_REPO_BRANCH
iris.scripts.repo.$GIT_REPO_NAME.paths=queries/oms_trace
iris.scripts.repo.$GIT_REPO_NAME.root=$GIT_REPO_ROOT
iris.scripts.repo.$GIT_REPO_NAME.updateEnabled=true
iris.scripts.repo.$GIT_REPO_NAME.prefixDisplayPathsWithRepoName=true
iris.scripts.repo.$GIT_REPO_NAME.groups=${GIT_REPO_GROUPS:-oms-trace-ops}
EOF
ok "fragment written"
cat "$frag" | sed 's/^/    /'

step "importing the properties (dhconfig properties import; confirm the subcommand on the installed release)"
if printf '%s' "$DHCONFIG" | grep -q "podman exec"; then
  printf '    $ %s properties import --file - < %s\n' "$DHCONFIG" "$(basename "$frag")"
  [ "${DRY_RUN:-0}" = "1" ] || dh properties import --file /dev/stdin < "$frag"
else
  run dh properties import --file "$frag"
fi

cat <<'EOF'

    Then, in the infra container:
        podman exec -it deephaven-infra bash -c 'sudo -u irisadmin ssh -T -o BatchMode=yes git@github.com'   # key accepted
        podman exec -it deephaven-infra /usr/illumon/latest/bin/iris_controller_restart                      # or the service unit
        dhconfig pq check-repo
    The scripts appear in the Query Monitor's script dropdown as oms_trace/queries/oms_trace/*.py.
EOF
