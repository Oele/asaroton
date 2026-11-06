#!/usr/bin/env python3
"""Regression test: a waiting sync wakes for a room its user was not in when the wait began.

    scripts/syncwaketest.py http://127.0.0.1:18099      (a test server with open registration)

Starts a long poll, changes something one second in, and expects the response well before the
timeout, carrying the change: an invitation, and a room created on the user's other device.
Both sync flavours.
"""
import json
import secrets
import sys
import threading
import time
import urllib.error
import urllib.request

HS = sys.argv[1].rstrip("/")
TIMEOUT_MS = 8000
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
failures = []


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: {e.code} {e.read().decode()[:300]}")


def register(name):
    password = secrets.token_urlsafe(16)
    r = call("POST", "/_matrix/client/v3/register", {"username": f"{name}{secrets.token_hex(4)}", "password": password, "auth": {"type": "m.login.dummy"}})
    return r["user_id"], r["access_token"], password


def classic(token, since=None):
    r = call("GET", f"/_matrix/client/v3/sync?timeout={TIMEOUT_MS if since else 0}" + (f"&since={since}" if since else ""), token=token)
    rooms = r.get("rooms", {})
    return r["next_batch"], sorted(list(rooms.get("invite", {})) + list(rooms.get("join", {})))


def sliding(token, pos=None):
    body = {"conn_id": "wake", "lists": {"all": {"ranges": [[0, 19]], "timeline_limit": 1, "required_state": []}}}
    r = call("POST", f"{SLIDING}?timeout={TIMEOUT_MS if pos else 0}" + (f"&pos={pos}" if pos else ""), body, token)
    return r["pos"], sorted(r.get("rooms", {}))


def expect_wake(label, sync, token, change):
    """Runs `change` one second into a long poll and checks the poll returns with a room."""
    position, _ = sync(token)
    # Drain what the first response left pending, so the next request really waits.
    for _ in range(3):
        started = time.monotonic()
        nxt, rooms = sync(token, position)
        if time.monotonic() - started > TIMEOUT_MS / 1000 * 0.8:
            position = nxt
            break
        position = nxt
    result = {}

    def poll():
        started = time.monotonic()
        _, rooms = sync(token, position)
        result.update(took=time.monotonic() - started, rooms=rooms)

    t = threading.Thread(target=poll)
    t.start()
    time.sleep(1)
    room = change()
    t.join()
    ok = result["took"] < TIMEOUT_MS / 1000 * 0.6 and room in result["rooms"]
    print(("ok    " if ok else "FAIL  ") + f"{label}: returned after {result['took']:.1f}s with {len(result['rooms'])} room(s)")
    if not ok:
        failures.append(label)


for name, sync in (("classic sync", classic), ("sliding sync", sliding)):
    _, alice, _ = register("alice")
    bob_id, bob, bob_password = register("bob")
    expect_wake(f"{name}, invitation to a user in no rooms", sync, bob,
                lambda: call("POST", "/_matrix/client/v3/createRoom", {"invite": [bob_id]}, alice)["room_id"])
    expect_wake(f"{name}, second invitation", sync, bob,
                lambda: call("POST", "/_matrix/client/v3/createRoom", {"invite": [bob_id]}, alice)["room_id"])
    other_device = call("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": bob_id}, "password": bob_password})["access_token"]
    expect_wake(f"{name}, room created on the user's other device", sync, bob,
                lambda: call("POST", "/_matrix/client/v3/createRoom", {"name": "mine"}, other_device)["room_id"])

print(f"\n{len(failures)} failure(s)")
sys.exit(1 if failures else 0)
