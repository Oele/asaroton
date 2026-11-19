#!/usr/bin/env python3
"""Profile updates in sliding sync: the profiles extension (MSC4262, `org.matrix.msc4262.profiles`).

A connection that asks for it gets, for the people it shares a room with (and the user
themselves): every set field the first time, afterwards only what changed (`updated` with the
new value, `removed` for a cleared field), and `null` for someone who no longer shares a room.
`fields` limits the fields, `lists`/`rooms` the rooms. A waiting request wakes for a change in a
profile it hands out. A status of one's own (MSC4426) is the case smalt shows live.

The lab: Alice and Bob share a room, Carol shares none with Alice.

Needs what scripts/fedlab.py needs. Takes about fifteen seconds.

Usage: scripts/profilestest.py [--keep]
"""
import threading, time, urllib.parse

from fedlab import check, run_lab

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
EXT = "org.matrix.msc4262.profiles"
STATUS = "org.matrix.msc4426.status"


def run(hs, peers):
    q = urllib.parse.quote
    alice, a_token = hs.register("alice")
    bob, b_token = hs.register("bob")
    carol, c_token = hs.register("carol")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11", "name": "Studio"}, a_token)
    assert st == 200, created
    room_id = created["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room_id)}", {}, b_token)
    assert st == 200, r

    def set_status(token, who, text, emoji):
        st, r = hs.req("PUT", f"/_matrix/client/v3/profile/{q(who)}/{STATUS}", {STATUS: {"text": text, "emoji": emoji}}, token)
        assert st == 200, r

    set_status(b_token, bob, "at the kiln", "🔥")
    set_status(c_token, carol, "elsewhere", "🌙")

    def sync(conn, pos=None, ext=None, timeout=0):
        body = {"conn_id": conn, "lists": {"all": {"ranges": [[0, 10]], "timeline_limit": 1}},
                "extensions": {EXT: {"enabled": True, **(ext or {})}} if ext is not None else {}}
        path = f"{SLIDING}?timeout={timeout}" + (f"&pos={pos}" if pos else "")
        st, r = hs.req("POST", path, body, a_token)
        assert st == 200, r
        return r["pos"], r.get("extensions", {}).get(EXT, {}).get("users")

    # First answer: every set field of everyone Alice shares a room with, and her own.
    pos, users = sync("all", ext={})
    users = users or {}
    check(set(users) == {alice, bob}, f"the first answer covers Alice and Bob, not Carol ({sorted(users)})")
    check(users.get(bob, {}).get("updated", {}).get(STATUS) == {"text": "at the kiln", "emoji": "🔥"}
          and users.get(bob, {}).get("updated", {}).get("displayname") is not None,
          f"with all of Bob's set fields ({sorted(users.get(bob, {}).get('updated', {}))})")

    # Nothing changed: nothing for profiles.
    pos, users = sync("all", pos, ext={})
    check(users is None, f"an unchanged profile is not sent again ({users})")

    # A change: only that field.
    set_status(b_token, bob, "glazing", "🏺")
    pos, users = sync("all", pos, ext={})
    check(users == {bob: {"updated": {STATUS: {"text": "glazing", "emoji": "🏺"}}, "removed": []}},
          f"a new status comes as Bob's only update ({users})")

    # A waiting request wakes for it.
    got = {}

    def wait():
        t0 = time.monotonic()
        got["result"] = sync("all", pos, ext={}, timeout=10000)
        got["took"] = time.monotonic() - t0

    waiter = threading.Thread(target=wait)
    waiter.start()
    time.sleep(1)
    set_status(b_token, bob, "back at four", "🕓")
    waiter.join(15)
    pos, users = got.get("result", (pos, None))
    check(users and users.get(bob, {}).get("updated", {}).get(STATUS, {}).get("emoji") == "🕓" and got.get("took", 99) < 5,
          f"a waiting request wakes for Bob's new status ({got.get('took', 0):.1f}s)")

    # A cleared field.
    st, r = hs.req("DELETE", f"/_matrix/client/v3/profile/{q(bob)}/{STATUS}", None, b_token)
    assert st == 200, r
    pos, users = sync("all", pos, ext={})
    check(users == {bob: {"updated": {}, "removed": [STATUS]}}, f"a cleared status comes as removed ({users})")

    # Someone who no longer shares a room: null.
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/leave", {}, b_token)
    assert st == 200, r
    pos, users = sync("all", pos, ext={})
    check(users == {bob: None}, f"Bob, gone from the only shared room, comes as null ({users})")

    # Fields and scope.
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room_id)}", {}, b_token)
    assert st == 200, r
    set_status(b_token, bob, "here again", "👋")
    _, users = sync("fields", ext={"fields": [STATUS]})
    check(users == {bob: {"updated": {STATUS: {"text": "here again", "emoji": "👋"}}, "removed": []}},
          f"fields limits the answer to the status (Alice has none) ({users})")
    _, users = sync("scoped", ext={"lists": ["no-such-list"]})
    check(users is None, f"a scope that covers no room hands out no profiles ({users})")
    _, users = sync("plain")
    check(users is None, "a connection without the extension gets nothing of it")


if __name__ == "__main__":
    run_lab(["mock"], run)
