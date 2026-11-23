#!/usr/bin/env python3
"""/get_missing_events answers with the events nearest to `latest_events` (tracker #86).

The spec defines the walk as breadth-first from `latest_events`, stopping at `limit`. asaroton
used to keep the *oldest* `limit` events within reach, so with a fork in range the direct prev
events of the event the caller is trying to accept were the first to be cut, and the caller
refused that event ("isn't divulging details about prev_events").

Here Bob, a member on a mock server, writes a side branch of twelve messages from his join on,
while Alice, the local user, writes three on the main line; Alice's next message merges both.
The mock server then asks for that message's missing events with a small limit:

- with limit 4 it gets the two direct prevs (the newest of each branch) and the one before each,
  oldest first;
- with limit 1 it gets one of the direct prevs;
- `earliest_events` still stops the walk.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/missingeventstest.py [--keep]
"""
import time
import urllib.parse
import uuid

from fedlab import check, run_lab, signed_event, wait_for


def run(hs, peers):
    mock = peers["mock"]
    q = urllib.parse.quote
    alice, token = hs.register("alice")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, token)
    assert st == 200, created
    room_id = created["room_id"]
    bob = mock.join(hs.fed, room_id, "bob")

    st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/state", None, token)
    assert st == 200, state
    by_type = {(e["type"], e["state_key"]): e["event_id"] for e in state}
    create, power = by_type[("m.room.create", "")], by_type[("m.room.power_levels", "")]
    bob_join = by_type[("m.room.member", bob)]

    def say(body):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/send/m.room.message/{uuid.uuid4().hex}",
                       {"msgtype": "m.text", "body": body}, token)
        assert st == 200, r
        return r["event_id"]

    def timeline():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/messages?dir=b&limit=50", None, token)
        assert st == 200, r
        return {e["event_id"] for e in r.get("chunk", [])}

    # The main line: three messages from Alice.
    a = [say(f"a{i}") for i in (1, 2, 3)]

    # Bob's side branch: twelve messages, the first building on his join, not on Alice's messages.
    clock = int(time.time() * 1000)
    prev, depth, b = bob_join, 100, []
    for i in range(1, 13):
        clock += 10
        depth += 1
        ev_id, ev = signed_event(mock.name, mock.key, "ed25519:k1", {
            "type": "m.room.message", "room_id": room_id, "sender": bob, "content": {"msgtype": "m.text", "body": f"b{i}"},
            "auth_events": [create, power, bob_join], "prev_events": [prev], "depth": depth, "origin_server_ts": clock})
        st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}",
                         {"origin": mock.name, "origin_server_ts": clock, "pdus": [ev], "edus": []})
        assert st == 200 and r.get("pdus", {}).get(ev_id) == {}, (st, r)
        prev = ev_id
        b.append(ev_id)
    wait_for(lambda: b[-1] in timeline(), 10, 0.2)
    check(b[-1] in timeline(), "Bob's side branch of twelve messages is in the room")

    # Alice's next message merges the two branches.
    merge = say("merge")

    def missing(latest, earliest, limit):
        st, r = mock.fed(hs.fed, "POST", f"/_matrix/federation/v1/get_missing_events/{q(room_id)}",
                         {"latest_events": latest, "earliest_events": earliest, "limit": limit, "min_depth": 0})
        events = r.get("events", []) if isinstance(r, dict) else []
        return st, [e.get("content", {}).get("body", e.get("type")) for e in events], [e.get("depth", 0) for e in events]

    st, bodies, depths = missing([merge], [bob_join], 4)
    check(st == 200 and set(bodies) == {"a3", "b12", "a2", "b11"},
          f"limit 4: the direct prevs of the merge (a3, b12) and the one before each, not the oldest four ({bodies})")
    check(depths == sorted(depths), f"oldest first, so each can be taken after its prevs ({depths})")
    st, bodies, depths = missing([merge], [bob_join], 1)
    check(st == 200 and len(bodies) == 1 and bodies[0] in ("a3", "b12"), f"limit 1: one of the direct prevs ({bodies})")
    st, bodies, depths = missing([b[-1]], [b[-4]], 10)
    check(st == 200 and bodies == ["b10", "b11"], f"earliest_events stops the walk: from b12 back to b9 gives b10, b11 ({bodies})")
    st, bodies, depths = missing([merge], [a[0], b[0]], 20)
    check(st == 200 and set(bodies) == {"a2", "a3"} | {f"b{i}" for i in range(2, 13)},
          f"with an earliest event on each branch: everything between ({len(bodies)} events)")


if __name__ == "__main__":
    run_lab(["mock"], run)
