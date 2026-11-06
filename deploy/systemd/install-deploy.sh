#!/bin/sh
# Installs tessera-deploy on this host. Run as root:
#   ./install-deploy.sh [UNIT [DBNAME ['ssh-ed25519 AAAA… comment']]]
#
# With a public key as third argument, that key is allowed to run tessera-deploy over ssh and
# nothing else (a forced command). Whoever holds the key can replace the tessera binary on this
# host, which then runs as the service user: treat it like a password to the homeserver.
set -eu
UNIT=${1:-tessera}
DB=${2:-tessera}
KEY=${3:-}
HERE=$(cd "$(dirname "$0")" && pwd)
install -m 0755 -o root -g root "$HERE/tessera-deploy.sh" /usr/local/sbin/tessera-deploy
install -d -m 0755 -o root -g root /var/lib/tessera-deploy
if [ ! -e /etc/default/tessera-deploy ]; then
  printf 'UNIT=%s\nDB=%s\n' "$UNIT" "$DB" > /etc/default/tessera-deploy
fi
if [ -n "$KEY" ]; then
  mkdir -p /root/.ssh && chmod 700 /root/.ssh
  touch /root/.ssh/authorized_keys
  grep -v ' tessera-deploy$' /root/.ssh/authorized_keys > /root/.ssh/authorized_keys.new || true
  echo "restrict,command=\"/usr/local/sbin/tessera-deploy\" $KEY" >> /root/.ssh/authorized_keys.new
  chmod 600 /root/.ssh/authorized_keys.new && mv /root/.ssh/authorized_keys.new /root/.ssh/authorized_keys
  echo "key installed: it can only run /usr/local/sbin/tessera-deploy"
fi
/usr/local/sbin/tessera-deploy status
