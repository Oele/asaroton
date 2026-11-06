#!/usr/bin/env python3
"""Regression test: a pending invite must not keep sliding sync from long-polling.

    scripts/ssinvitetest.py http://127.0.0.1:18099      (a test server with open registration)

Creates two users, has one invite the other, and syncs as the invitee with a timeout. The first
response carries the invited room; after that a request has to wait for the timeout and return
no rooms, until the invite changes.
"""
import json
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HS = sys.argv[1].rstrip("/")
SYNC = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
TIMEOUT_MS = 2000


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: {e.code} {e.read().decode()[:300]}")


def register(name):
    r = call("POST", "/_matrix/client/v3/register", {"username": f"{name}{secrets.token_hex(4)}", "password": secrets.token_urlsafe(16), "auth": {"type": "m.login.dummy"}})
    return r["user_id"], r["access_token"]


def sync(token, pos):
    body = {"conn_id": "probe", "lists": {"all": {"ranges": [[0, 19]], "required_state": [["m.room.name", ""], ["m.room.create", ""]], "timeline_limit": 1}}}
    started = time.monotonic()
    r = call("POST", f"{SYNC}?timeout={TIMEOUT_MS}" + (f"&pos={pos}" if pos else ""), body, token)
    return r, time.monotonic() - started


def main():
    _, alice = register("alice")
    bob_id, bob = register("bob")
    room = call("POST", "/_matrix/client/v3/createRoom", {"name": "before", "invite": [bob_id]}, alice)["room_id"]
    failures = []

    r, took = sync(bob, None)
    print(f"request 1: {took:.2f}s rooms={list(r.get('rooms', {}))}")
    if "invite_state" not in r.get("rooms", {}).get(room, {}):
        failures.append("the first response does not carry the invite")
    for n in (2, 3):
        r, took = sync(bob, r["pos"])
        print(f"request {n}: {took:.2f}s rooms={list(r.get('rooms', {}))}")
        if r.get("rooms") or took < TIMEOUT_MS / 1000 * 0.8:
            failures.append(f"request {n} returned after {took:.2f}s with rooms {list(r.get('rooms', {}))}: it should have waited and been empty")

    # A change to the stripped state has to reach the invitee, once.
    q = urllib.parse.quote(room)
    call("PUT", f"/_matrix/client/v3/rooms/{q}/state/m.room.name/", {"name": "after"}, alice)
    r, took = sync(bob, r["pos"])
    names = [e["content"].get("name") for e in r.get("rooms", {}).get(room, {}).get("invite_state", []) if e["type"] == "m.room.name"]
    print(f"after rename: {took:.2f}s names={names}")
    if names != ["after"]:
        failures.append(f"the renamed room did not reach the invitee (got {names})")
    r, took = sync(bob, r["pos"])
    print(f"then: {took:.2f}s rooms={list(r.get('rooms', {}))}")
    if r.get("rooms") or took < TIMEOUT_MS / 1000 * 0.8:
        failures.append("the request after the rename did not wait")

    # Accepting the invite turns it into a joined room.
    call("POST", f"/_matrix/client/v3/rooms/{q}/join", {}, bob)
    r, took = sync(bob, r["pos"])
    joined = r.get("rooms", {}).get(room, {})
    print(f"after join: {took:.2f}s keys={sorted(joined)}")
    if "invite_state" in joined or not joined:
        failures.append("after joining, the room is missing or still an invite")

    for f in failures:
        print("FAIL:", f)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
