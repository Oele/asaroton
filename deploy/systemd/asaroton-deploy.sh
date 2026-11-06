#!/bin/bash
# Installs a new asaroton binary on this host, restarts the service, checks that it is healthy
# and puts the previous binary back if it is not.
#
#   asaroton-deploy status
#   asaroton-deploy deploy SHA256 < binary      the binary arrives on standard input
#   asaroton-deploy rollback
#
# Runs as root: directly (sudo) on the machine that builds, or as the forced command of an ssh
# key on a remote host. In the second case the key can do nothing but the three commands above:
#   restrict,command="/usr/local/sbin/asaroton-deploy" ssh-ed25519 AAAA… asaroton-deploy
#
# What a deploy does, in order: receive the binary and compare its checksum with the one the
# caller announced; run `--version` on it as the service user; keep the running binary as
# `.previous`; install; restart; wait for the unit to be active and the client API to answer.
# If that fails it restores the previous binary and restarts again, unless the new build
# changed the database schema: an older binary may not start on a newer schema, so then it
# stops and says so instead of guessing.
#
# Settings: /etc/default/asaroton-deploy (see install-deploy.sh).
set -u
CONF=${ASAROTON_DEPLOY_CONF:-/etc/default/asaroton-deploy}
BIN=/usr/local/bin/asaroton
UNIT=asaroton
SERVICE_USER=asaroton
DB=asaroton
HEALTH_URL=http://127.0.0.1:8018/_matrix/client/versions
HEALTH_TIMEOUT=60
INCOMING=/var/lib/asaroton-deploy
LOG=/var/log/asaroton-deploy.log
MAX_BYTES=1073741824
RESTART_CMD='systemctl restart "$UNIT"'
ACTIVE_CMD='systemctl is-active --quiet "$UNIT"'
SCHEMA_CMD='runuser -u postgres -- psql -AtX "$DB" -c "select coalesce(max(version),0) from _sqlx_migrations"'
# shellcheck disable=SC1090
[ -r "$CONF" ] && . "$CONF"

say() { echo "$*"; echo "$(date '+%F %T') $*" >> "$LOG" 2>/dev/null; }
die() { say "FAILED: $*"; exit 1; }
sha_of() { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }
short() { printf '%.12s' "${1:-none}"; }
schema() { eval "$SCHEMA_CMD" 2>/dev/null | head -n 1; }

healthy() {
  # Active and answering, twice in a row, so a process that starts and dies does not pass.
  local deadline=$(( $(date +%s) + HEALTH_TIMEOUT )) streak=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if eval "$ACTIVE_CMD" && curl -fsS -m 5 -o /dev/null "$HEALTH_URL" 2>/dev/null; then
      streak=$((streak + 1))
      [ "$streak" -ge 2 ] && return 0
    else
      streak=0
    fi
    sleep 2
  done
  return 1
}

status() {
  local code
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$HEALTH_URL" 2>/dev/null)
  echo "host=$(hostname) running=$(short "$(sha_of "$BIN")") previous=$(short "$(sha_of "$BIN.previous")")" \
       "active=$(eval "$ACTIVE_CMD" && echo yes || echo no) health=${code:-000} schema=$(schema)"
}

swap_in() {   # swap_in FILE: install FILE as the binary and restart
  install -m 0755 "$1" "$BIN.new" && mv -f "$BIN.new" "$BIN" || return 1
  [ "$(id -u)" = 0 ] && chown root:root "$BIN"
  # The lock stays with this script: a service started here must not inherit it and block
  # every later deploy for as long as it runs.
  eval "$RESTART_CMD" 8>&-
}

deploy() {
  local want=$1 got old schema_before started
  [[ "$want" =~ ^[0-9a-f]{64}$ ]] || die "the checksum must be 64 hex characters"
  mkdir -p "$INCOMING" && chmod 755 "$INCOMING" || die "cannot use $INCOMING"
  rm -f "$INCOMING"/asaroton.??????
  tmp=$(mktemp "$INCOMING/asaroton.XXXXXX") || die "cannot create a file in $INCOMING"
  trap 'rm -f "$tmp"' EXIT    # `tmp` is global on purpose: the trap runs after this function
  head -c "$MAX_BYTES" > "$tmp" || die "could not read the binary"
  got=$(sha_of "$tmp")
  [ "$got" = "$want" ] || die "checksum mismatch: announced $(short "$want"), received $(short "$got") ($(stat -c %s "$tmp") bytes)"
  old=$(sha_of "$BIN")
  if [ "$got" = "$old" ]; then
    say "$(short "$got") is already installed on $(hostname); nothing to do"; status; return 0
  fi
  chmod 755 "$tmp"
  # Never run an uploaded binary as root, not even for --version.
  if [ "$(id -u)" = 0 ]; then
    timeout 20 runuser -u "$SERVICE_USER" -- "$tmp" --version 2>/dev/null | grep -q '^asaroton ' || die "the upload does not run as an asaroton binary"
  else
    timeout 20 "$tmp" --version 2>/dev/null | grep -q '^asaroton ' || die "the upload does not run as an asaroton binary"
  fi
  schema_before=$(schema)
  # The running binary is kept aside as `.replaced` and only becomes `.previous` once the new
  # one is healthy, so a failed deploy does not cost the older rollback target.
  [ -f "$BIN" ] && cp -pf "$BIN" "$BIN.replaced"
  started=$(date +%s)
  swap_in "$tmp" || die "could not install or restart; $BIN.replaced holds the old binary"
  if healthy; then
    [ -f "$BIN.replaced" ] && mv -f "$BIN.replaced" "$BIN.previous"
    say "deployed $(short "$got") on $(hostname): healthy after $(( $(date +%s) - started ))s (previous $(short "$old"), schema $(schema))"
    return 0
  fi
  if [ "$(schema)" != "$schema_before" ]; then
    say "FAILED: $(short "$got") is not healthy on $(hostname), and it changed the database schema from $schema_before to $(schema)." \
        "The previous binary may not start on the new schema, so nothing was rolled back; it is kept as $BIN.replaced. Needs a person."
    exit 2
  fi
  if [ -f "$BIN.replaced" ] && swap_in "$BIN.replaced" && healthy; then
    rm -f "$BIN.replaced"
    say "FAILED: $(short "$got") did not become healthy on $(hostname) within ${HEALTH_TIMEOUT}s; rolled back to $(short "$old"), which is healthy"
    exit 1
  fi
  say "FAILED: $(short "$got") is not healthy on $(hostname) and the rollback to $(short "$old") is not healthy either. Needs a person."
  exit 2
}

rollback() {
  [ -f "$BIN.previous" ] || die "there is no previous binary on $(hostname)"
  local cur prev
  cur=$(sha_of "$BIN"); prev=$(sha_of "$BIN.previous")
  [ "$cur" = "$prev" ] && { say "the previous binary is the one already installed"; return 0; }
  cp -pf "$BIN" "$BIN.rolledback"
  swap_in "$BIN.previous" || die "could not restore the previous binary"
  mv -f "$BIN.rolledback" "$BIN.previous"
  healthy && { say "rolled back $(hostname) from $(short "$cur") to $(short "$prev"): healthy"; return 0; }
  say "FAILED: $(hostname) is not healthy after rolling back to $(short "$prev"). Needs a person."
  exit 2
}

# Through ssh the request is the forced command's original command line; nothing else from
# the caller is used.
set -f
if [ -n "${SSH_ORIGINAL_COMMAND:-}" ]; then
  # shellcheck disable=SC2086
  set -- $SSH_ORIGINAL_COMMAND
fi
lock() {
  mkdir -p "$INCOMING" 2>/dev/null || die "cannot create $INCOMING; this has to run as root"
  exec 8>"$INCOMING/.lock" || die "cannot open the lock in $INCOMING"
  flock -n 8 || die "another deploy is running on $(hostname)"
}
case "${1:-} $#" in
  "status 1") status ;;
  "deploy 2") lock; deploy "$2" ;;
  "rollback 1") lock; rollback ;;
  *) echo "usage: asaroton-deploy status | deploy SHA256 < binary | rollback" >&2; exit 64 ;;
esac
