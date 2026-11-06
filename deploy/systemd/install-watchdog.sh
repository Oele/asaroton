#!/bin/sh
# Installs the tessera watchdog as a systemd service that starts at boot. Run as root from
# this directory:
#   ./install-watchdog.sh [UNIT [DBNAME]]      defaults: tessera tessera
set -eu
UNIT=${1:-tessera}
DB=${2:-tessera}
HERE=$(cd "$(dirname "$0")" && pwd)
install -m 0755 -o root -g root "$HERE/tessera-watchdog.sh" /usr/local/sbin/tessera-watchdog.sh
install -m 0644 -o root -g root "$HERE/tessera-watchdog.service" /etc/systemd/system/tessera-watchdog.service
if [ ! -e /etc/default/tessera-watchdog ]; then
  printf 'TESSERA_UNIT=%s\nTESSERA_DB=%s\n' "$UNIT" "$DB" > /etc/default/tessera-watchdog
fi
systemctl daemon-reload
# A watchdog started by hand before this unit existed would run alongside it.
pkill -f '^sh /[a-z/]*tessera-watchdog\.sh' || true
systemctl enable --now tessera-watchdog.service
sleep 2
systemctl is-enabled tessera-watchdog.service
systemctl is-active tessera-watchdog.service
tail -n 1 /var/log/tessera-watchdog.log
