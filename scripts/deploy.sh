#!/usr/bin/env bash
# Builds the release and deploys it: this machine first, then the remote host.
#
#   scripts/deploy.sh             check, build, deploy here, wait, deploy to the remote host
#   scripts/deploy.sh local       only this machine
#   scripts/deploy.sh remote      only the remote host
#   scripts/deploy.sh status      what runs where
#   scripts/deploy.sh rollback-local | rollback-remote
#
# It deploys only what is on origin/main: the tree must be clean, HEAD must be pushed and
# scripts/check.sh must pass. The work on each host is done by /usr/local/sbin/asaroton-deploy
# (deploy/systemd/), which verifies the checksum, checks health and rolls back.
#
# The remote host is named in ASAROTON_DEPLOY_REMOTE or, on one line, in
# ~/.config/asaroton-deploy/remote (`root@matrix.example.org`). It is reached with the key in
# ~/.config/asaroton-deploy/ when there is one (a key that can run nothing but
# asaroton-deploy), otherwise with whatever ssh access the caller has.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-all}
REMOTE=${ASAROTON_DEPLOY_REMOTE:-$(cat "$HOME/.config/asaroton-deploy/remote" 2>/dev/null || true)}
KEY=${ASAROTON_DEPLOY_KEY:-$HOME/.config/asaroton-deploy/id_ed25519}
TOOL=/usr/local/sbin/asaroton-deploy
SOAK=${ASAROTON_DEPLOY_SOAK:-30}
BINARY=target/release/asaroton

die() { echo "deploy: $*" >&2; exit 1; }
here() { sudo -n "$TOOL" "$@"; }
there() {   # there REQUEST: the binary, if any, is passed on standard input
  [ -n "$REMOTE" ] || die "no remote host: name it in ASAROTON_DEPLOY_REMOTE or ~/.config/asaroton-deploy/remote, or deploy with 'local'"
  if [ -f "$KEY" ]; then
    ssh -i "$KEY" -o IdentitiesOnly=yes -o IdentityAgent=none -o BatchMode=yes -o ConnectTimeout=20 "$REMOTE" "$1"
  else
    ssh -o BatchMode=yes -o ConnectTimeout=20 "$REMOTE" "$TOOL $1"
  fi
}
announce() {   # best effort: an executable ~/.config/asaroton-deploy/announce is told what was deployed
  local hook="$HOME/.config/asaroton-deploy/announce"
  if [ -x "$hook" ]; then "$hook" "$1" >/dev/null 2>&1 || true; fi
}

case "$MODE" in
  status) here status; if [ -n "$REMOTE" ]; then there status; fi; exit 0 ;;
  rollback-local) here rollback; exit ;;
  rollback-remote) there rollback; exit ;;
  all | local | remote) ;;
  *) die "unknown mode '$MODE' (all, local, remote, status, rollback-local, rollback-remote)" ;;
esac

git fetch -q origin
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "there are uncommitted changes; commit and push first"
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || die "HEAD is not origin/main; push first"
echo "checking $(git log -1 --format='%h %s')"
scripts/check.sh >/tmp/asaroton-deploy-check.log 2>&1 || { tail -n 20 /tmp/asaroton-deploy-check.log; die "scripts/check.sh fails; not deploying"; }
cargo build --release 2>&1 | tail -n 1
SHA=$(sha256sum "$BINARY" | cut -d' ' -f1)
echo "binary ${SHA:0:12}"
DONE=""

if [ "$MODE" != remote ]; then
  here deploy "$SHA" < "$BINARY"
  DONE="$(hostname)"
fi
if [ "$MODE" = all ]; then
  echo "waiting ${SOAK}s before the remote host"
  sleep "$SOAK"
  here status | grep -q "running=${SHA:0:12} .*active=yes health=200" || die "this machine is not healthy after ${SOAK}s; not touching $REMOTE"
fi
if [ "$MODE" != local ]; then
  there "deploy $SHA" < "$BINARY"
  DONE="${DONE:+$DONE and }${REMOTE#*@}"
fi
announce "Deployed \`$(git log -1 --format='%h %s')\` to $DONE (binary ${SHA:0:12})."
echo "deployed to $DONE"
