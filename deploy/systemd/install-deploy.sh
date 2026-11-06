#!/bin/sh
# Installs asaroton-deploy on this host. Run as root:
#   ./install-deploy.sh [UNIT [DBNAME ['ssh-ed25519 AAAA… comment']]]
#
# With a public key as third argument, that key is allowed to run asaroton-deploy over ssh and
# nothing else (a forced command). Whoever holds the key can replace the asaroton binary on this
# host, which then runs as the service user: treat it like a password to the homeserver.
set -eu
UNIT=${1:-asaroton}
DB=${2:-asaroton}
KEY=${3:-}
HERE=$(cd "$(dirname "$0")" && pwd)
install -m 0755 -o root -g root "$HERE/asaroton-deploy.sh" /usr/local/sbin/asaroton-deploy
install -d -m 0755 -o root -g root /var/lib/asaroton-deploy
if [ ! -e /etc/default/asaroton-deploy ]; then
  printf 'UNIT=%s\nDB=%s\n' "$UNIT" "$DB" > /etc/default/asaroton-deploy
fi
if [ -n "$KEY" ]; then
  mkdir -p /root/.ssh && chmod 700 /root/.ssh
  touch /root/.ssh/authorized_keys
  grep -v ' asaroton-deploy$' /root/.ssh/authorized_keys > /root/.ssh/authorized_keys.new || true
  echo "restrict,command=\"/usr/local/sbin/asaroton-deploy\" $KEY" >> /root/.ssh/authorized_keys.new
  chmod 600 /root/.ssh/authorized_keys.new && mv /root/.ssh/authorized_keys.new /root/.ssh/authorized_keys
  echo "key installed: it can only run /usr/local/sbin/asaroton-deploy"
fi
/usr/local/sbin/asaroton-deploy status
