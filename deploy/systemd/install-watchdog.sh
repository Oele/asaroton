#!/bin/sh
# Installs the asaroton watchdog as a systemd service that starts at boot. Run as root from
# this directory:
#   ./install-watchdog.sh [UNIT [DBNAME]]      defaults: asaroton asaroton
set -eu
UNIT=${1:-asaroton}
DB=${2:-asaroton}
HERE=$(cd "$(dirname "$0")" && pwd)
install -m 0755 -o root -g root "$HERE/asaroton-watchdog.sh" /usr/local/sbin/asaroton-watchdog.sh
install -m 0644 -o root -g root "$HERE/asaroton-watchdog.service" /etc/systemd/system/asaroton-watchdog.service
if [ ! -e /etc/default/asaroton-watchdog ]; then
  printf 'ASAROTON_UNIT=%s\nASAROTON_DB=%s\n' "$UNIT" "$DB" > /etc/default/asaroton-watchdog
fi
systemctl daemon-reload
# A watchdog started by hand before this unit existed would run alongside it.
pkill -f '^sh /[a-z/]*asaroton-watchdog\.sh' || true
systemctl enable --now asaroton-watchdog.service
sleep 2
systemctl is-enabled asaroton-watchdog.service
systemctl is-active asaroton-watchdog.service
tail -n 1 /var/log/asaroton-watchdog.log
