#!/usr/bin/env python3
"""Regression test: a waiting sync wakes for a room its user was not in when the wait began.

    scripts/syncwaketest.py http://127.0.0.1:18099      (a test server with open registration)

Starts a long poll, changes something one second in, and expects the response soon after the
change is made, carrying it: an invitation, and a room created on the user's other device.
Both sync flavours.

"Soon after the change is made", not "soon after the poll began": creating a room is a row of
writes, and on a disk that is busy with something else it can take seconds. The test used to
count that time against the sync and failed now and then on a loaded machine, with an answer
that had in fact followed the invitation within half a second.
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
SOON = 2.0  # seconds after a change is made within which a waiting sync has to carry it
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
    """Runs `change` one second into a long poll and checks that the poll returns with the room
    soon after the change is made."""
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

    def poll(position):
        nxt, rooms = sync(token, position)
        result.update(ended=time.monotonic(), rooms=rooms, position=nxt)

    t = threading.Thread(target=poll, args=(position,))
    t.start()
    time.sleep(1)
    began = time.monotonic()
    room = change()
    made = time.monotonic()
    t.join()
    early = result["ended"] < began
    if room not in result["rooms"] and began <= result["ended"] < made:
        # The change took longer than the poll lasted, or the poll was answered with something
        # else while the change was being made: the next poll has to bring it.
        poll(result["position"])
    after = result["ended"] - made
    ok = not early and room in result["rooms"] and after < SOON
    what = "returned before anything had changed" if early else f"returned {max(after, 0.0):.1f}s after the change was made (which took {made - began:.1f}s)"
    print(("ok    " if ok else "FAIL  ") + f"{label}: {what} with {len(result['rooms'])} room(s)")
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
