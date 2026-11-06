#!/bin/sh
# Stops tessera when its federation output or CPU use looks like a runaway, and says why.
#
# A homeserver that misbehaves bothers every server it federates with, so this errs on the
# side of stopping: better an hour of downtime than a flood. It only ever stops the unit; it
# never starts it again. Once the cause is fixed, `systemctl start tessera` brings it back.
#
# Runs as root from tessera-watchdog.service. Settings come from the environment (see
# /etc/default/tessera-watchdog); the unit and database can also be given as arguments:
#   tessera-watchdog.sh [UNIT [DBNAME]]
#
# The transaction count is read from tessera's own log ("transaction sent", a DEBUG line), so
# it only works while tessera logs at debug level.
UNIT=${1:-${TESSERA_UNIT:-tessera}}
DB=${2:-${TESSERA_DB:-tessera}}
LOG=${WATCHDOG_LOG:-/var/log/tessera-watchdog.log}
MAX_TXN_PER_MIN=${MAX_TXN_PER_MIN:-1000}   # a flood ran at ~15000/min; a chatty minute is a few hundred
MAX_CPU=${MAX_CPU:-60}                     # percent, three samples in a row
MAX_OUTBOX=${MAX_OUTBOX:-5000}             # queued federation rows
hot=0
echo "$(date '+%F %T') watchdog started (unit=$UNIT db=$DB)" >> "$LOG"
while :; do
  sleep 10
  systemctl is-active --quiet "$UNIT" || continue
  txn=$(journalctl -u "$UNIT" --since '60 sec ago' --no-pager 2>/dev/null | grep -c 'transaction sent')
  pid=$(systemctl show -p MainPID --value "$UNIT")
  cpu=$(ps -o %cpu= -p "$pid" 2>/dev/null | awk '{printf "%d", $1+0}'); cpu=${cpu:-0}
  outbox=$(runuser -u postgres -- psql -At "$DB" -c 'select count(*) from federation_outbox' 2>/dev/null || echo 0)
  outbox=${outbox:-0}
  [ "$cpu" -gt "$MAX_CPU" ] && hot=$((hot+1)) || hot=0
  reason=""
  [ "$txn" -gt "$MAX_TXN_PER_MIN" ] && reason="$txn federation transactions in the last minute"
  [ "$hot" -ge 3 ] && reason="${reason:+$reason; }tessera at ${cpu}% CPU for 30 s"
  [ "$outbox" -gt "$MAX_OUTBOX" ] && reason="${reason:+$reason; }$outbox rows queued in federation_outbox"
  if [ -n "$reason" ]; then
    systemctl stop "$UNIT"
    msg="$(date '+%F %T') STOPPED $UNIT: $reason"
    echo "$msg" >> "$LOG"; logger -t tessera-watchdog "$msg"
    command -v mail >/dev/null && echo "$msg" | mail -s "tessera watchdog stopped $UNIT on $(hostname)" root
    hot=0
  fi
done
