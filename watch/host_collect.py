#!/usr/bin/env python3
"""Read-only collector for the asaroton watch job.

Prints one JSON document describing an asaroton host: a digest of the service log, the state of
the federation outbox, and new messages in the Matrix rooms listed in ROOMS (read straight from
asaroton's database, so no account has to join anything).

On the homeserver host this file is installed as /usr/local/sbin/asaroton-watch-collect and is
the only thing the watch job's ssh key can run (a forced command). Its two arguments, taken
from SSH_ORIGINAL_COMMAND, are integers; nothing else from the caller is used. It never writes.

    asaroton-watch-collect SINCE_STREAM SINCE_EPOCH
    host_collect.py --local SINCE_EPOCH        (journal and host facts only, no database)
"""
import collections
import json
import os
import re
import subprocess
import sys
import time

UNIT = "asaroton"
DATABASE = "asaroton"
# The service was called tessera until 2026-11-06. Its journal from before the rename lives
# under the old unit name and logs "tessera starting"; both are still read so a digest that
# spans the rename is complete.
LEGACY_UNIT = "tessera"
# Rooms whose messages the watch job may read. The caller cannot change this list.
ROOMS = {
    "!i5D5LLct_DYG-4hQprLzrxdbZ580U9UB6AEgFnk6rZQ": "#synapse-dev:matrix.org",
}
MAX_MESSAGES = 3000
FIRST_RUN_DAYS = 14
MAX_GROUPS = 80

ANSI = re.compile(r"\x1b\[[0-9;]*m")
LINE = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+Z)\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s+([\w:]+): (.*)$")
SIGILS = re.compile(r"(?:[!$@#]|%21|%24|%40|%23)[^\s/\"',)]+")
HOSTS = re.compile(r"\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?::\d+)?\b")
NUMBERS = re.compile(r"\d+")
TOKENISH = re.compile(r"^[A-Za-z0-9_+=-]{16,}$")


def run(cmd, timeout=300):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, errors="replace").stdout
    except (OSError, subprocess.SubprocessError) as e:
        return f"<failed: {e}>"


def normalise(message):
    """A log message with identifiers, host names and numbers blanked, so repeats group."""
    m = SIGILS.sub("<id>", message)
    m = HOSTS.sub("<host>", m)
    return NUMBERS.sub("N", m)[:220]


def normalise_path(path):
    out = []
    for seg in path.split("/"):
        if SIGILS.match(seg) or TOKENISH.match(seg):
            out.append("{id}")
        elif seg.isdigit():
            out.append("{n}")
        elif "." in seg and HOSTS.fullmatch(seg):
            out.append("{server}")
        else:
            out.append(seg[:60])
    return "/".join(out)


def journal(since_epoch):
    raw = run(["journalctl", "-u", UNIT, "-u", LEGACY_UNIT, "--since", f"@{since_epoch}", "--no-pager", "-o", "cat"], 600)
    levels = collections.Counter()
    groups = {}
    unrecognized = collections.Counter()
    per_minute = collections.Counter()
    edu_types = collections.Counter()
    starts, panics, systemd = [], [], collections.Counter()
    for line in raw.splitlines():
        line = ANSI.sub("", line)
        m = LINE.match(line)
        if not m:
            if line.startswith((f"{UNIT}.service:", f"{LEGACY_UNIT}.service:")):
                systemd[NUMBERS.sub("N", line)[:160]] += 1
            if "panicked" in line:
                panics.append(line[:400])
            continue
        ts, level, target, msg = m.groups()
        levels[level] += 1
        if "panicked" in msg:
            panics.append(f"{ts} {msg[:400]}")
        if msg.startswith(("asaroton starting", "tessera starting")):
            starts.append(ts)
        elif msg.startswith("transaction sent"):
            per_minute[ts[:16]] += 1
            t = re.search(r"edu_types=(\S+)", msg)
            for name in (t.group(1).split(",") if t else []):
                edu_types[name] += 1
        elif msg.startswith("unrecognized endpoint"):
            method = re.search(r"method=(\S+)", msg)
            path = re.search(r'path="?([^\s"]+)', msg)
            if method and path:
                unrecognized[f"{method.group(1)} {normalise_path(path.group(1))}"] += 1
        if level in ("WARN", "ERROR"):
            key = f"{level} {target}: {normalise(msg)}"
            g = groups.setdefault(key, {"count": 0, "first": ts, "example": msg[:600]})
            g["count"] += 1
            g["last"] = ts
    top = sorted(groups.items(), key=lambda kv: -kv[1]["count"])
    busiest = per_minute.most_common(5)
    return {
        "since_epoch": since_epoch,
        "lines_by_level": dict(levels),
        "warn_error_groups": [dict(signature=k, **v) for k, v in top[:MAX_GROUPS]],
        "warn_error_groups_omitted": max(0, len(top) - MAX_GROUPS),
        "unrecognized_endpoints": unrecognized.most_common(MAX_GROUPS),
        "federation_transactions": {
            "total": sum(per_minute.values()),
            "busiest_minutes": busiest,
            "edu_types": dict(edu_types.most_common(12)),
        },
        "service_starts": starts[-20:],
        "panics": panics[-20:],
        "systemd_messages": systemd.most_common(10),
    }


def host_facts():
    show = run(["systemctl", "show", UNIT, "-p", "ActiveState,ActiveEnterTimestamp,NRestarts,MemoryCurrent,ExecMainPID"])
    facts = dict(l.split("=", 1) for l in show.splitlines() if "=" in l)
    facts["loadavg"] = open("/proc/loadavg").read().split()[:3]
    facts["cpus"] = os.cpu_count()
    binary = run(["sha256sum", "/usr/local/bin/asaroton"]).split(" ")[0][:12]
    facts["binary_sha256"] = binary
    facts["disk_root"] = run(["df", "-h", "--output=pcent,avail", "/"]).split("\n")[1:2]
    facts["watchdog_running"] = bool(run(["pgrep", "-f", "asaroton-watchdog"]).strip())
    try:
        facts["watchdog_log_tail"] = open("/var/log/asaroton-watchdog.log").read().splitlines()[-5:]
    except OSError:
        facts["watchdog_log_tail"] = []
    facts["livekit"] = run(["systemctl", "is-active", "livekit"]).strip()
    return facts


def psql(sql):
    cmd = ["psql", "-d", DATABASE, "-AtX", "-v", "ON_ERROR_STOP=1", "-c", sql]
    if os.geteuid() == 0:
        cmd = ["runuser", "-u", "postgres", "--"] + cmd
    return run(cmd, 120)


def database(since_stream):
    room_list = ",".join("'" + r.replace("'", "") + "'" for r in ROOMS)
    window = (
        f"stream_ordering > {since_stream}"
        if since_stream > 0
        else f"origin_server_ts > {int((time.time() - FIRST_RUN_DAYS * 86400) * 1000)}"
    )
    rows = psql(
        "SELECT coalesce(json_agg(t ORDER BY t.stream), '[]') FROM ("
        " SELECT stream_ordering AS stream, room_id, sender, origin_server_ts AS ts, event_id,"
        "  json::json->'content' AS content FROM events"
        f" WHERE room_id IN ({room_list}) AND {window} AND stream_ordering > 0"
        "  AND event_type = 'm.room.message' AND NOT rejected AND NOT outlier"
        f" ORDER BY stream_ordering LIMIT {MAX_MESSAGES}) t"
    )
    messages = []
    try:
        for r in json.loads(rows or "[]"):
            c = r.get("content") or {}
            rel = c.get("m.relates_to") or {}
            if rel.get("rel_type") == "m.replace":
                continue
            messages.append({
                "stream": r["stream"],
                "room": ROOMS.get(r["room_id"], r["room_id"]),
                "ts": r["ts"],
                "sender": r["sender"],
                "event_id": r["event_id"],
                "reply_to": (rel.get("m.in_reply_to") or {}).get("event_id"),
                "thread": rel.get("event_id") if rel.get("rel_type") == "m.thread" else None,
                "body": str(c.get("body", ""))[:4000],
            })
    except ValueError:
        messages = [{"error": rows[:300]}]
    outbox = psql(
        "SELECT json_build_object('total', (SELECT count(*) FROM federation_outbox),"
        " 'oldest_age_hours', (SELECT round((extract(epoch from now())*1000 - min(created_ts))/3600000) FROM federation_outbox),"
        " 'top_destinations', (SELECT json_agg(d) FROM (SELECT destination, count(*) AS rows FROM federation_outbox"
        "   GROUP BY 1 ORDER BY 2 DESC LIMIT 10) d),"
        " 'failing_destinations', (SELECT count(*) FROM federation_destinations WHERE failure_ts IS NOT NULL),"
        " 'known_destinations', (SELECT count(*) FROM federation_destinations),"
        " 'database_size', (SELECT pg_size_pretty(pg_database_size(current_database()))),"
        " 'local_users', (SELECT count(*) FROM users),"
        " 'rooms', (SELECT count(*) FROM rooms))"
    )
    try:
        outbox = json.loads(outbox)
    except ValueError:
        outbox = {"error": outbox[:300]}
    return {
        "messages": messages,
        "max_stream": max([m.get("stream", 0) for m in messages] + [since_stream]),
        "truncated": len(messages) >= MAX_MESSAGES,
        "stats": outbox,
    }


def main():
    local = "--local" in sys.argv
    args = os.environ.get("SSH_ORIGINAL_COMMAND", " ".join(a for a in sys.argv[1:] if a != "--local"))
    numbers = re.findall(r"(?<![\w.-])\d{1,15}(?![\w.-])", args)
    if local:
        since_stream, since_epoch = 0, int(numbers[0]) if numbers else 0
    else:
        if len(numbers) != 2:
            sys.exit("usage: asaroton-watch-collect SINCE_STREAM SINCE_EPOCH")
        since_stream, since_epoch = int(numbers[0]), int(numbers[1])
    since_epoch = max(since_epoch, int(time.time()) - 31 * 86400)
    out = {
        "collected_at": int(time.time()),
        "host": os.uname().nodename,
        "journal": journal(since_epoch),
        "facts": host_facts(),
    }
    if not local:
        out["database"] = database(since_stream)
    json.dump(out, sys.stdout, ensure_ascii=False)


if __name__ == "__main__":
    main()
