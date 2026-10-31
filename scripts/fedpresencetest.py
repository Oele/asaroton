#!/usr/bin/env python3
"""Presence fan-out over federation stays bounded: two devices of one user on hs1 disagree about
their presence on every sync, and hs2 must receive only a handful of m.presence EDUs.

Needs the two-server lab (scratchpad lab/start.sh: hs1 on :18008 with debug logging to
hs1/server.log, hs2 on :28008). Usage: fedpresencetest.py LAB_DIR
"""
import json, re, sys, time, urllib.request, urllib.error, urllib.parse

LAB = sys.argv[1]
H1, H2, S1 = "http://127.0.0.1:18008", "http://127.0.0.1:28008", "localhost:18448"
fails = 0


def check(c, what):
    global fails
    print(("ok   " if c else "FAIL ") + what)
    fails += 0 if c else 1


def req(hs, method, path, body=None, token=None):
    data = None if body is None else json.dumps(body).encode()
    r = urllib.request.Request(hs + path, data=data, method=method)
    if body is not None:
        r.add_header("Content-Type", "application/json")
    if token:
        r.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}


def register(hs, name):
    user = f"{name}{int(time.time())}"
    st, r = req(hs, "POST", "/_matrix/client/v3/register", {"username": user, "password": "pwpwpwpw", "auth": {"type": "m.login.dummy"}})
    assert st == 200, r
    return r["user_id"], r["access_token"]


def login(hs, user):
    st, r = req(hs, "POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user}, "password": "pwpwpwpw", "initial_device_display_name": "second device"})
    assert st == 200, r
    return r["access_token"]


ANSI = re.compile(r"\x1b\[[0-9;]*m")


def presence_edus_to_hs2():
    n = 0
    with open(f"{LAB}/hs1/server.log", errors="replace") as f:
        for line in f:
            line = ANSI.sub("", line)
            if "transaction sent" in line and "destination=localhost:28448" in line and "m.presence" in line:
                n += 1
    return n


alice, phone = register(H1, "alice")
web = login(H1, alice)
bob, btok = register(H2, "bob")
st, r = req(H1, "POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, phone)
room = r["room_id"]
st, r = req(H2, "POST", f"/_matrix/client/v3/join/{urllib.parse.quote(room)}?server_name={S1}", {}, btok)
check(st == 200, f"bob (hs2) joined alice's room ({st})")
time.sleep(2)
before = presence_edus_to_hs2()

# Thirty rounds of disagreement, as fast as the server answers.
for _ in range(30):
    req(H1, "GET", "/_matrix/client/v3/sync?timeout=0&set_presence=online", None, phone)
    req(H1, "GET", "/_matrix/client/v3/sync?timeout=0&set_presence=unavailable", None, web)
st, r = req(H1, "GET", f"/_matrix/client/v3/presence/{urllib.parse.quote(alice)}/status", None, phone)
check(r.get("presence") == "online", f"alice stays online while one device says unavailable ({r.get('presence')})")
time.sleep(20)  # the spacing interval plus the worker tick
sent = presence_edus_to_hs2() - before
check(1 <= sent <= 3, f"presence EDUs hs1 -> hs2 during 60 flapping syncs: {sent} (expected 1..3)")
st, r = req(H2, "GET", f"/_matrix/client/v3/presence/{urllib.parse.quote(alice)}/status", None, btok)
check(r.get("presence") == "online", f"hs2 sees alice online ({r.get('presence')})")
print("FAILURES:", fails)
sys.exit(1 if fails else 0)
