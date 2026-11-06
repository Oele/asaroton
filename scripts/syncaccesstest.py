#!/usr/bin/env python3
"""Regression test: sync shows a user only what they may read.

    scripts/syncaccesstest.py http://127.0.0.1:18099      (a test server with open registration)

Covers what once leaked: a pending knock read through a sliding sync room subscription, a
banned user still receiving the room's state, and messages from before a user joined in a room
whose history visibility hides them (both sync flavours, compared with /messages).
"""
import json
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request

HS = sys.argv[1].rstrip("/")
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0"
failures = []


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


def room_path(room, rest):
    return f"/_matrix/client/v3/rooms/{urllib.parse.quote(room)}/{rest}"


def send(token, room, text):
    call("PUT", room_path(room, f"send/m.room.message/{secrets.token_hex(6)}"), {"msgtype": "m.text", "body": text}, token)


def set_state(token, room, event_type, content):
    call("PUT", room_path(room, f"state/{event_type}/"), content, token)


def subscribe(token, room, conn, pos=None):
    body = {"conn_id": conn, "room_subscriptions": {room: {"timeline_limit": 20, "required_state": [["*", "*"]]}}}
    return call("POST", SLIDING + (f"&pos={pos}" if pos else ""), body, token)


def listed(token, conn, pos=None):
    body = {"conn_id": conn, "lists": {"all": {"ranges": [[0, 19]], "timeline_limit": 20, "required_state": [["*", "*"]]}}}
    return call("POST", SLIDING + (f"&pos={pos}" if pos else ""), body, token)


def bodies(events):
    return [e.get("content", {}).get("body") for e in events if e["type"] == "m.room.message"]


def check(ok, what):
    print(("ok    " if ok else "FAIL  ") + what)
    if not ok:
        failures.append(what)


def knock():
    _, alice = register("alice")
    _, bob = register("bob")
    room = call("POST", "/_matrix/client/v3/createRoom", {"name": "knock room", "initial_state": [
        {"type": "m.room.join_rules", "state_key": "", "content": {"join_rule": "knock"}}]}, alice)["room_id"]
    send(alice, room, "MEMBERS ONLY")
    call("POST", f"/_matrix/client/v3/knock/{urllib.parse.quote(room)}", {}, bob)
    r = subscribe(bob, room, "k")
    got = r.get("rooms", {}).get(room, {})
    check("MEMBERS ONLY" not in json.dumps(r), "knock: the room's messages are not shown to a pending knock")
    check(not got.get("timeline") and not got.get("required_state"), "knock: no timeline and no room state")
    check(any(e["type"] == "m.room.name" for e in got.get("invite_state", [])), "knock: the stripped state is shown")
    r = subscribe(bob, room, "k", r["pos"])
    check(room not in r.get("rooms", {}), "knock: the room is not repeated in the next response")


def ban():
    _, alice = register("alice")
    bob_id, bob = register("bob")
    carol_id, carol = register("carol")
    room = call("POST", "/_matrix/client/v3/createRoom", {"name": "public room", "preset": "public_chat"}, alice)["room_id"]
    call("POST", room_path(room, "join"), {}, bob)
    call("POST", room_path(room, "join"), {}, carol)
    send(alice, room, "before the ban")
    kept = subscribe(bob, room, "kept")
    seen_by_list = listed(carol, "list")
    check(room in seen_by_list.get("rooms", {}), "member: a joined room is in the list response")
    call("POST", room_path(room, "ban"), {"user_id": bob_id}, alice)
    set_state(alice, room, "m.room.topic", {"topic": "SET AFTER"})
    set_state(alice, room, "m.room.name", {"name": "RENAMED AFTER"})
    send(alice, room, "SAID AFTER")

    fresh = subscribe(bob, room, "fresh")
    got = fresh.get("rooms", {}).get(room, {})
    check("AFTER" not in json.dumps(fresh), "ban, new connection: nothing from after the ban")
    check(bodies(got.get("timeline", [])) == ["before the ban"], "ban, new connection: the timeline ends at the ban")
    check(got.get("name") == "public room", "ban, new connection: the name is the one at the ban")
    check(any(e["type"] == "m.room.member" and e["state_key"] == bob_id and e["content"]["membership"] == "ban"
              for e in got.get("required_state", [])), "ban, new connection: the state is the state at the ban")

    first = subscribe(bob, room, "kept", kept["pos"])
    got = first.get("rooms", {}).get(room, {})
    check("AFTER" not in json.dumps(first), "ban, kept subscription: nothing from after the ban")
    check(any(e["type"] == "m.room.member" and e["state_key"] == bob_id for e in got.get("timeline", [])),
          "ban, kept subscription: the ban itself is delivered")
    set_state(alice, room, "m.room.topic", {"topic": "SET AFTER AGAIN"})
    call("POST", room_path(room, "kick"), {"user_id": carol_id}, alice)
    later = subscribe(bob, room, "kept", first["pos"])
    check(room not in later.get("rooms", {}), "ban, kept subscription: later changes are not delivered")

    # A member removed while syncing a list is told once, then hears nothing more.
    send(alice, room, "SAID AFTER THE KICK")
    kicked = listed(carol, "list", seen_by_list["pos"])
    got = kicked.get("rooms", {}).get(room, {})
    check(any(e["type"] == "m.room.member" and e["state_key"] == carol_id and e["content"]["membership"] == "leave"
              for e in got.get("timeline", [])), "kick, list: the kick is delivered")
    check("AFTER THE KICK" not in json.dumps(kicked), "kick, list: nothing from after the kick")
    check(bodies(got.get("timeline", [])) == ["SAID AFTER"], "kick, list: what was said while a member is delivered")
    again = listed(carol, "list", kicked["pos"])
    check(room not in again.get("rooms", {}), "kick, list: the room is not repeated")

    member = subscribe(alice, room, "a")
    got = member.get("rooms", {}).get(room, {})
    check("SAID AFTER THE KICK" in bodies(got.get("timeline", [])) and got.get("name") == "RENAMED AFTER",
          "member: a joined user still gets the current timeline and name")


def history_visibility():
    expected = {
        "joined": ["after join"],
        "invited": ["while invited", "after join"],
        "shared": ["before invite", "while invited", "after join"],
    }
    for hv, want in expected.items():
        _, alice = register("alice")
        bob_id, bob = register("bob")
        room = call("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "initial_state": [
            {"type": "m.room.history_visibility", "state_key": "", "content": {"history_visibility": hv}}]}, alice)["room_id"]
        send(alice, room, "before invite")
        call("POST", room_path(room, "invite"), {"user_id": bob_id}, alice)
        send(alice, room, "while invited")
        call("POST", room_path(room, "join"), {}, bob)
        send(alice, room, "after join")
        classic = call("GET", "/_matrix/client/v3/sync?timeout=0", token=bob)["rooms"]["join"][room]
        sliding = listed(bob, "hv")["rooms"][room]
        messages = call("GET", room_path(room, "messages?dir=b&limit=50"), token=bob)["chunk"]
        check(bodies(classic["timeline"]["events"]) == want, f"history_visibility={hv}: classic sync shows {want}")
        check(bodies(sliding["timeline"]) == want, f"history_visibility={hv}: sliding sync shows {want}")
        check(list(reversed(bodies(messages))) == want, f"history_visibility={hv}: /messages agrees")
        state_types = {e["type"] for e in classic["state"]["events"]} | {e["type"] for e in classic["timeline"]["events"]}
        check({"m.room.create", "m.room.history_visibility"} <= state_types, f"history_visibility={hv}: classic sync still delivers the room state")
        check({"m.room.create", "m.room.history_visibility"} <= {e["type"] for e in sliding["required_state"]},
              f"history_visibility={hv}: sliding sync still delivers the room state")
        full = listed(alice, "hv")["rooms"][room]
        check(bodies(full["timeline"]) == ["before invite", "while invited", "after join"], f"history_visibility={hv}: the long-standing member sees everything")


def never_a_member():
    """An invite that was declined and a knock that was withdrawn give no claim to room state."""
    _, alice = register("alice")
    bob_id, bob = register("bob")
    room = call("POST", "/_matrix/client/v3/createRoom", {"name": "members club", "initial_state": [
        {"type": "m.room.join_rules", "state_key": "", "content": {"join_rule": "knock"}},
        {"type": "m.room.topic", "state_key": "", "content": {"topic": "INTERNAL TOPIC"}},
        {"type": "org.example.secret", "state_key": "", "content": {"value": "INTERNAL STATE"}}]}, alice)["room_id"]
    since = call("GET", "/_matrix/client/v3/sync?timeout=0", token=bob)["next_batch"]
    call("POST", room_path(room, "invite"), {"user_id": bob_id}, alice)
    invited = listed(bob, "list")
    check("invite_state" in invited.get("rooms", {}).get(room, {}), "invite, sliding list: the invitation is shown")
    call("POST", room_path(room, "leave"), {}, bob)
    declined = listed(bob, "list", invited["pos"])
    got = declined.get("rooms", {}).get(room, {})
    # Clients built on the Rust SDK read membership from required_state only.
    check([(e["type"], e["state_key"], e["content"]["membership"]) for e in got.get("required_state", [])]
          == [("m.room.member", bob_id, "leave")], "declined invite, sliding list: required_state holds the user's own leave and nothing else")
    check("INTERNAL" not in json.dumps(declined), "declined invite, sliding list: no room state")
    after = listed(bob, "list", declined["pos"])
    check(room not in after.get("rooms", {}), "declined invite, sliding list: the room is not repeated")
    for label, query in (("classic sync", ""), ("classic sync with state_after", "&use_state_after=true")):
        r = call("GET", f"/_matrix/client/v3/sync?timeout=0&since={since}{query}", token=bob)
        check("INTERNAL STATE" not in json.dumps(r), f"declined invite, {label}: no room state")
        check(room in r.get("rooms", {}).get("leave", {}), f"declined invite, {label}: the room is reported as left")
    everything = urllib.parse.quote(json.dumps({"room": {"include_leave": True}}))
    r = call("GET", f"/_matrix/client/v3/sync?timeout=0&filter={everything}", token=bob)
    check("INTERNAL STATE" not in json.dumps(r), "declined invite, initial classic sync with include_leave: no room state")
    r = subscribe(bob, room, "declined")
    check("INTERNAL STATE" not in json.dumps(r), "declined invite, sliding subscription: no room state")
    check([e["state_key"] for e in r.get("rooms", {}).get(room, {}).get("required_state", [])] == [bob_id],
          "declined invite, sliding subscription: required_state holds only the user's own membership")

    call("POST", f"/_matrix/client/v3/knock/{urllib.parse.quote(room)}", {}, bob)
    call("POST", room_path(room, "leave"), {}, bob)
    r = subscribe(bob, room, "withdrawn")
    check("INTERNAL" not in json.dumps(r), "withdrawn knock, sliding subscription: no room state")
    r = call("GET", f"/_matrix/client/v3/sync?timeout=0&filter={everything}", token=bob)
    check("INTERNAL STATE" not in json.dumps(r), "withdrawn knock, initial classic sync with include_leave: no room state")


def left_in_window():
    """What happens after a member is removed stays out of the sync that reports the removal."""
    for label, query in (("classic sync", ""), ("classic sync with state_after", "&use_state_after=true")):
        _, alice = register("alice")
        bob_id, bob = register("bob")
        room = call("POST", "/_matrix/client/v3/createRoom", {"name": "public room", "preset": "public_chat"}, alice)["room_id"]
        call("POST", room_path(room, "join"), {}, bob)
        since = call("GET", "/_matrix/client/v3/sync?timeout=0", token=bob)["next_batch"]
        set_state(alice, room, "m.room.topic", {"topic": "set while a member"})
        call("POST", room_path(room, "kick"), {"user_id": bob_id}, alice)
        set_state(alice, room, "m.room.topic", {"topic": "SET AFTER"})
        send(alice, room, "SAID AFTER")
        r = call("GET", f"/_matrix/client/v3/sync?timeout=0&since={since}{query}", token=bob)
        left = r.get("rooms", {}).get("leave", {}).get(room, {})
        check("AFTER" not in json.dumps(r), f"kicked, {label}: nothing from after the kick")
        check("set while a member" in json.dumps(left), f"kicked, {label}: changes from before the kick are delivered")
        check(any(e["type"] == "m.room.member" and e["state_key"] == bob_id for e in left.get("timeline", {}).get("events", [])),
              f"kicked, {label}: the kick is delivered")


knock()
ban()
history_visibility()
never_a_member()
left_in_window()
print(f"\n{len(failures)} failure(s)")
sys.exit(1 if failures else 0)
