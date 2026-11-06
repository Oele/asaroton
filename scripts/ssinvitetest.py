#!/usr/bin/env python3
"""Regression test: invitations over sliding sync.

    scripts/ssinvitetest.py http://127.0.0.1:18099      (a test server with open registration)

Creates two users, has one invite the other, and syncs as the invitee with a timeout.

A pending invite must not keep the connection from long-polling: the first response carries
the invited room; after that a request has to wait for the timeout and return no rooms, until
the invite changes.

Accepting the invite must bring the room down as a fresh room (`initial`, the required state,
`limited` and a `prev_batch`), so that the client can load what was said before the join. And
a room subscription that asks for a longer timeline than the list did gets the older events.
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


def sync(token, pos, subscriptions=None):
    body = {"conn_id": "probe", "lists": {"all": {"ranges": [[0, 19]], "required_state": [["m.room.name", ""], ["m.room.create", ""]], "timeline_limit": 1}}}
    if subscriptions:
        body["room_subscriptions"] = subscriptions
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

    # Accepting the invite turns it into a joined room, sent as a room the connection has not
    # seen before: until now it only had stripped state, and there is history to load.
    call("PUT", f"/_matrix/client/v3/rooms/{q}/send/m.room.message/before", {"msgtype": "m.text", "body": "said before the join"}, alice)
    call("POST", f"/_matrix/client/v3/rooms/{q}/join", {}, bob)
    r, took = sync(bob, r["pos"])
    joined = r.get("rooms", {}).get(room, {})
    kinds = [(e["type"], e["content"].get("membership")) for e in joined.get("timeline", [])]
    state = sorted(e["type"] for e in joined.get("required_state", []))
    print(f"after join: {took:.2f}s initial={joined.get('initial')} limited={joined.get('limited')} prev_batch={'yes' if joined.get('prev_batch') else 'NO'} num_live={joined.get('num_live')} timeline={kinds} state={state}")
    if "invite_state" in joined or not joined:
        failures.append("after joining, the room is missing or still an invite")
    if joined.get("initial") is not True:
        failures.append("after joining, the room is not marked initial")
    if joined.get("limited") is not True or not joined.get("prev_batch"):
        failures.append("after joining, the room has no limited flag or prev_batch to load history with")
    if kinds != [("m.room.member", "join")] or joined.get("num_live") != 1:
        failures.append(f"after joining, the timeline should be the join alone, as one live event (got {kinds}, num_live {joined.get('num_live')})")
    if state != ["m.room.create", "m.room.name"]:
        failures.append(f"after joining, the required state is not sent in full (got {state})")
    if joined.get("prev_batch"):
        older = call("GET", f"/_matrix/client/v3/rooms/{q}/messages?dir=b&limit=50&from={urllib.parse.quote(joined['prev_batch'])}", None, bob)
        bodies = [e["content"].get("body") for e in older.get("chunk", []) if e["type"] == "m.room.message"]
        if bodies != ["said before the join"]:
            failures.append(f"paging back from prev_batch does not reach the message sent before the join (got {bodies})")
    r, took = sync(bob, r["pos"])
    if r.get("rooms") or took < TIMEOUT_MS / 1000 * 0.8:
        failures.append("the request after the join did not wait")

    # A subscription with a longer timeline than the list's gets the older events, once.
    sub = {room: {"timeline_limit": 20, "required_state": [["m.room.name", ""]]}}
    r, took = sync(bob, r["pos"], sub)
    opened = r.get("rooms", {}).get(room, {})
    kinds = [e["type"] for e in opened.get("timeline", [])]
    print(f"subscribed: {took:.2f}s expanded={opened.get('expanded_timeline')} limited={opened.get('limited')} prev_batch={'yes' if opened.get('prev_batch') else 'NO'} num_live={opened.get('num_live')} timeline={kinds}")
    if kinds[:1] != ["m.room.create"] or "m.room.message" not in kinds or kinds[-1:] != ["m.room.member"]:
        failures.append(f"a longer timeline_limit did not bring the older events (got {kinds})")
    if opened.get("expanded_timeline") is not True or opened.get("limited") is not False or not opened.get("prev_batch") or opened.get("num_live") != 0:
        failures.append("the expanded timeline should be flagged, complete (not limited), without live events and with a prev_batch")
    if opened.get("initial") or opened.get("required_state"):
        failures.append("expanding the timeline should not resend the room's state")
    r, took = sync(bob, r["pos"], sub)
    if r.get("rooms") or took < TIMEOUT_MS / 1000 * 0.8:
        failures.append("the request after the expanded timeline did not wait")
    # Dropping the subscription and taking it again sends nothing twice.
    r, took = sync(bob, r["pos"])
    r, took = sync(bob, r["pos"], sub)
    if r.get("rooms"):
        failures.append("subscribing again resent the room")
    # New events still arrive as ordinary updates.
    call("PUT", f"/_matrix/client/v3/rooms/{q}/send/m.room.message/after", {"msgtype": "m.text", "body": "said after"}, alice)
    r, took = sync(bob, r["pos"], sub)
    update = r.get("rooms", {}).get(room, {})
    bodies = [e["content"].get("body") for e in update.get("timeline", [])]
    if bodies != ["said after"] or update.get("initial") or update.get("limited") or update.get("expanded_timeline") or update.get("num_live") != 1:
        failures.append(f"a new message should arrive as a plain update (got {bodies}, {sorted(update)})")

    # A room without a name is listed by its other members ("heroes"), with the field names
    # of a profile: displayname and avatar_url.
    unnamed = call("POST", "/_matrix/client/v3/createRoom", {"invite": [bob_id], "is_direct": True}, alice)["room_id"]
    call("POST", f"/_matrix/client/v3/rooms/{urllib.parse.quote(unnamed)}/join", {}, bob)
    r, took = sync(bob, None)
    heroes = r.get("rooms", {}).get(unnamed, {}).get("heroes", [])
    print(f"unnamed room: heroes={heroes}")
    if len(heroes) != 1 or not heroes[0].get("displayname") or set(heroes[0]) - {"user_id", "displayname", "avatar_url"}:
        failures.append(f"heroes of an unnamed room should carry user_id, displayname and avatar_url (got {heroes})")

    for f in failures:
        print("FAIL:", f)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
