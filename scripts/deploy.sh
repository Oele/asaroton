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
# The remote host is reached with the key in ~/.config/asaroton-deploy/ when there is one (a key
# that can run nothing but asaroton-deploy), otherwise with whatever ssh access the caller has.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-all}
REMOTE=${ASAROTON_DEPLOY_REMOTE:-root@cluephone.org}
KEY=${ASAROTON_DEPLOY_KEY:-$HOME/.config/asaroton-deploy/id_ed25519}
TOOL=/usr/local/sbin/asaroton-deploy
SOAK=${ASAROTON_DEPLOY_SOAK:-30}
BINARY=target/release/asaroton

die() { echo "deploy: $*" >&2; exit 1; }
here() { sudo -n "$TOOL" "$@"; }
there() {   # there REQUEST: the binary, if any, is passed on standard input
  if [ -f "$KEY" ]; then
    ssh -i "$KEY" -o IdentitiesOnly=yes -o IdentityAgent=none -o BatchMode=yes -o ConnectTimeout=20 "$REMOTE" "$1"
  else
    ssh -o BatchMode=yes -o ConnectTimeout=20 "$REMOTE" "$TOOL $1"
  fi
}
announce() {   # best effort: a line in the watch room, so every deploy leaves a trace
  python3 - "$1" <<'PY' 2>/dev/null || true
import pathlib, sys, tomllib
sys.path.insert(0, "watch")
import publish
publish.send_matrix(tomllib.loads(pathlib.Path("watch/config.toml").read_text()), sys.argv[1])
PY
}

case "$MODE" in
  status) here status; there status; exit 0 ;;
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
