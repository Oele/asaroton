#!/bin/sh
# Stops asaroton when its federation output or CPU use looks like a runaway, and says why.
# Also warns, without stopping anything, when the disk that holds the database runs low.
#
# A homeserver that misbehaves bothers every server it federates with, so this errs on the
# side of stopping: better an hour of downtime than a flood. It only ever stops the unit; it
# never starts it again. Once the cause is fixed, `systemctl start asaroton` brings it back.
#
# Runs as root from asaroton-watchdog.service. Settings come from the environment (see
# /etc/default/asaroton-watchdog); the unit and database can also be given as arguments:
#   asaroton-watchdog.sh [UNIT [DBNAME]]
#
# The transaction count is read from asaroton's own log ("transaction sent", a DEBUG line), so
# it only works while asaroton logs at debug level.
#
# What it does is written to $LOG and to syslog, and mailed to $WATCHDOG_MAIL_TO (default root;
# empty for no mail) with mail(1), or sendmail where there is no mail(1).
UNIT=${1:-${ASAROTON_UNIT:-asaroton}}
DB=${2:-${ASAROTON_DB:-asaroton}}
LOG=${WATCHDOG_LOG:-/var/log/asaroton-watchdog.log}
MAX_TXN_PER_MIN=${MAX_TXN_PER_MIN:-1000}   # a flood ran at ~15000/min; a chatty minute is a few hundred
MAX_CPU=${MAX_CPU:-60}                     # percent, three samples in a row
MAX_OUTBOX=${MAX_OUTBOX:-5000}             # queued federation rows
MIN_FREE_GB=${MIN_FREE_GB:-10}             # warn when the database's disk has less free than this
MAIL_TO=${WATCHDOG_MAIL_TO-root}
INTERVAL=${WATCHDOG_INTERVAL:-10}          # seconds between looks
# The disk to watch: where PostgreSQL keeps its data. If it fills, the database stops writing.
DISK_PATH=${DISK_PATH:-$(runuser -u postgres -- psql -At -c 'show data_directory' 2>/dev/null)}
DISK_PATH=${DISK_PATH:-/var/lib/postgresql}

note() { echo "$1" >> "$LOG"; logger -t asaroton-watchdog "$1"; }
say() {   # say SUBJECT MESSAGE: note it and mail it
  note "$2"
  [ -n "$MAIL_TO" ] || return 0
  if command -v mail >/dev/null; then
    echo "$2" | mail -s "$1" "$MAIL_TO"
  elif command -v sendmail >/dev/null; then
    printf 'To: %s\nSubject: %s\n\n%s\n' "$MAIL_TO" "$1" "$2" | sendmail -t
  fi
}

hot=0
low=0
echo "$(date '+%F %T') watchdog started (unit=$UNIT db=$DB disk=$DISK_PATH min_free=${MIN_FREE_GB}G mail=${MAIL_TO:-none})" >> "$LOG"
while :; do
  # The disk first: it fills whether asaroton runs or not. One mail when the free space drops
  # below the line, a note when it is 2 GB above it again, and a mail the next time it drops.
  free_mb=$(df -P -BM "$DISK_PATH" 2>/dev/null | awk 'NR == 2 { sub("M", "", $4); print $4 + 0 }')
  if [ -n "$free_mb" ]; then
    if [ "$low" = 0 ] && [ "$free_mb" -lt $((MIN_FREE_GB * 1024)) ]; then
      low=1
      say "asaroton: disk nearly full on $(hostname)" \
        "$(date '+%F %T') LOW DISK: $((free_mb / 1024))G free on the disk that holds $DISK_PATH (the line is ${MIN_FREE_GB}G). If it fills, PostgreSQL can no longer write and asaroton goes down. Nothing was stopped."
    elif [ "$low" = 1 ] && [ "$free_mb" -ge $(((MIN_FREE_GB + 2) * 1024)) ]; then
      low=0
      note "$(date '+%F %T') disk recovered: $((free_mb / 1024))G free on the disk that holds $DISK_PATH"
    fi
  fi
  sleep "$INTERVAL"
  systemctl is-active --quiet "$UNIT" || continue
  txn=$(journalctl -u "$UNIT" --since '60 sec ago' --no-pager 2>/dev/null | grep -c 'transaction sent')
  pid=$(systemctl show -p MainPID --value "$UNIT")
  cpu=$(ps -o %cpu= -p "$pid" 2>/dev/null | awk '{printf "%d", $1+0}'); cpu=${cpu:-0}
  outbox=$(runuser -u postgres -- psql -At "$DB" -c 'select count(*) from federation_outbox' 2>/dev/null || echo 0)
  outbox=${outbox:-0}
  [ "$cpu" -gt "$MAX_CPU" ] && hot=$((hot+1)) || hot=0
  reason=""
  [ "$txn" -gt "$MAX_TXN_PER_MIN" ] && reason="$txn federation transactions in the last minute"
  [ "$hot" -ge 3 ] && reason="${reason:+$reason; }asaroton at ${cpu}% CPU for 30 s"
  [ "$outbox" -gt "$MAX_OUTBOX" ] && reason="${reason:+$reason; }$outbox rows queued in federation_outbox"
  if [ -n "$reason" ]; then
    systemctl stop "$UNIT"
    say "asaroton watchdog stopped $UNIT on $(hostname)" "$(date '+%F %T') STOPPED $UNIT: $reason"
    hot=0
  fi
done
