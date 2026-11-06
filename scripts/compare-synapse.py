#!/usr/bin/env python3
"""Compare a Synapse database with the asaroton database imported from it.

Runs the same counts on both sides for the selected rooms and users and prints the differences.
Usage: compare-synapse.py SYNAPSE_DSN ASAROTON_DSN   (libpq connection strings or URLs)
"""
import subprocess, sys, json

def q(dsn, sql):
    out = subprocess.run(["psql", dsn, "-At", "-F", "\t", "-c", sql], capture_output=True, text=True, check=True).stdout
    return [line.split("\t") for line in out.splitlines()]

syn, tes = sys.argv[1], sys.argv[2]
active = [r[0] for r in q(syn, "SELECT name FROM users WHERE deactivated = 0 ORDER BY 1")]
rooms = [r[0] for r in q(syn, "SELECT DISTINCT room_id FROM local_current_membership WHERE user_id IN (SELECT name FROM users WHERE deactivated = 0) AND room_id NOT IN (SELECT room_id FROM partial_state_rooms) ORDER BY 1")]
arr = "ARRAY[" + ",".join("'" + r.replace("'", "''") + "'" for r in rooms) + "]::text[]"
users_arr = "ARRAY[" + ",".join("'" + u + "'" for u in active) + "]::text[]"
problems = 0

def compare(label, syn_sql, tes_sql, key=lambda r: r[0]):
    global problems
    a = {key(r): r[1:] for r in q(syn, syn_sql)}
    b = {key(r): r[1:] for r in q(tes, tes_sql)}
    diff = [(k, a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)]
    print(f"{label}: {len(a)} synapse, {len(b)} asaroton, {len(diff)} differences")
    for k, x, y in diff[:15]:
        print(f"   {k}: synapse={x} asaroton={y}")
    problems += len(diff)

compare("events per room",
        f"SELECT room_id, count(*) FROM events WHERE room_id = ANY({arr}) GROUP BY 1",
        f"SELECT room_id, count(*) FROM events WHERE room_id = ANY({arr}) GROUP BY 1")
compare("current state rows per room",
        f"SELECT room_id, count(*) FROM current_state_events WHERE room_id = ANY({arr}) GROUP BY 1",
        f"SELECT room_id, count(*) FROM current_state_events WHERE room_id = ANY({arr}) GROUP BY 1")
compare("joined members per room",
        f"SELECT room_id, count(*) FROM current_state_events WHERE room_id = ANY({arr}) AND type = 'm.room.member' AND membership = 'join' GROUP BY 1",
        f"SELECT room_id, count(*) FROM current_state_events WHERE room_id = ANY({arr}) AND event_type = 'm.room.member' AND membership = 'join' GROUP BY 1")
compare("forward extremities per room",
        f"SELECT room_id, string_agg(event_id, ',' ORDER BY event_id) FROM event_forward_extremities WHERE room_id = ANY({arr}) GROUP BY 1",
        f"SELECT room_id, string_agg(event_id, ',' ORDER BY event_id) FROM forward_extremities WHERE room_id = ANY({arr}) GROUP BY 1")
compare("latest event per room",
        f"SELECT room_id, max(stream_ordering) FROM events WHERE room_id = ANY({arr}) GROUP BY 1",
        f"SELECT room_id, max(stream_ordering) FROM events WHERE room_id = ANY({arr}) GROUP BY 1")
compare("state groups per room",
        f"SELECT room_id, count(*) FROM state_groups WHERE room_id = ANY({arr}) GROUP BY 1",
        f"SELECT room_id, count(*) FROM state_groups WHERE room_id = ANY({arr}) GROUP BY 1")
compare("devices per active user",
        f"SELECT user_id, count(*) FROM devices WHERE user_id = ANY({users_arr}) GROUP BY 1",
        f"SELECT user_id, count(*) FROM devices WHERE user_id = ANY({users_arr}) GROUP BY 1")
compare("access tokens per active user",
        f"SELECT user_id, count(*) FROM access_tokens WHERE user_id = ANY({users_arr}) GROUP BY 1",
        f"SELECT user_id, count(*) FROM access_tokens WHERE user_id = ANY({users_arr}) GROUP BY 1")
compare("device keys per active user",
        f"SELECT user_id, count(*) FROM e2e_device_keys_json WHERE user_id = ANY({users_arr}) GROUP BY 1",
        f"SELECT user_id, count(*) FROM device_keys WHERE user_id = ANY({users_arr}) GROUP BY 1")
compare("backup keys per active user",
        f"SELECT user_id, count(*) FROM e2e_room_keys WHERE user_id = ANY({users_arr}) GROUP BY 1",
        f"SELECT user_id, count(*) FROM key_backup_keys WHERE user_id = ANY({users_arr}) GROUP BY 1")
compare("global account data per active user",
        f"SELECT user_id, count(*) FROM account_data WHERE user_id = ANY({users_arr}) GROUP BY 1",
        f"SELECT user_id, count(*) FROM account_data WHERE user_id = ANY({users_arr}) AND room_id = '' GROUP BY 1")
compare("media per active user",
        f"SELECT user_id, count(*) FROM local_media_repository WHERE user_id = ANY({users_arr}) GROUP BY 1",
        f"SELECT user_id, count(*) FROM media WHERE user_id = ANY({users_arr}) GROUP BY 1")
compare("pushers per active user",
        f"SELECT user_name, count(*) FROM pushers WHERE user_name = ANY({users_arr}) AND COALESCE(enabled, true) GROUP BY 1",
        f"SELECT user_id, count(*) FROM pushers WHERE user_id = ANY({users_arr}) GROUP BY 1")
compare("users (all) by deactivation",
        "SELECT deactivated::text, count(*) FROM users GROUP BY 1",
        "SELECT CASE WHEN is_deactivated THEN '1' ELSE '0' END, count(*) FROM users GROUP BY 1")
print("OK" if problems == 0 else f"{problems} differences")
sys.exit(1 if problems else 0)
