#!/usr/bin/env python3
"""A remote server that resets `depth` does not hide a room's newest events (tracker #99).

Synapse #20524: a server sent events with `depth: 0` on top of a room at depth ~1450, and the
events built on them continued from 0. asaroton ordered the first view of a room (initial
sync, a newly joined room) by that wire depth, so it showed events from before the reset as the
latest and left the new ones out.

Here Alice, the local user, writes twelve messages; Bob, a member on a mock server, then sends
three events that build on her last one but carry depth 0, 1 and 2, and a fourth with a far too
high depth. A first sync with a small timeline must show Bob's newest events, in the order they
were built, not Alice's older ones; Alice's next message must come after Bob's.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/depthresettest.py [--keep]
"""
import json, time, urllib.parse, uuid

from fedlab import check, run_lab, signed_event, wait_for

q = urllib.parse.quote


def run(hs, peers):
    mock = peers["mock"]
    alice, a = hs.register("alice")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, a)
    room = r["room_id"]
    bob = mock.join(hs.fed, room, "bob")
    st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/state", None, a)
    by_type = {(e["type"], e["state_key"]): e["event_id"] for e in state}
    create, power = by_type[("m.room.create", "")], by_type[("m.room.power_levels", "")]
    bob_join = by_type[("m.room.member", bob)]

    def say(body):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{uuid.uuid4().hex}",
                       {"msgtype": "m.text", "body": body}, a)
        assert st == 200, r
        return r["event_id"]

    def timeline_ids():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/messages?dir=b&limit=60", None, a)
        return [e["event_id"] for e in r.get("chunk", [])]

    last = None
    for i in range(12):
        last = say(f"alice {i}")

    clock = int(time.time() * 1000)
    prev, bobs = last, []
    for i, depth in enumerate((0, 1, 2, 10 ** 9)):
        clock += 10
        ev_id, ev = signed_event(mock.name, mock.key, "ed25519:k1", {
            "type": "m.room.message", "room_id": room, "sender": bob,
            "content": {"msgtype": "m.text", "body": f"bob {i} at depth {depth}"},
            "auth_events": [create, power, bob_join], "prev_events": [prev], "depth": depth, "origin_server_ts": clock})
        st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}",
                         {"origin": mock.name, "origin_server_ts": clock, "pdus": [ev], "edus": []})
        assert st == 200 and r.get("pdus", {}).get(ev_id) == {}, (st, r)
        prev = ev_id
        bobs.append(ev_id)
    wait_for(lambda: bobs[-1] in timeline_ids(), 10, 0.2)
    check(all(b in timeline_ids() for b in bobs), "Bob's four events, at depth 0, 1, 2 and 10^9, are in the room")
    after = say("alice after bob")

    def first_view(limit):
        flt = q(json.dumps({"room": {"timeline": {"limit": limit}}}))
        st, r = hs.req("GET", f"/_matrix/client/v3/sync?filter={flt}", None, a)
        return [e["event_id"] for e in r.get("rooms", {}).get("join", {}).get(room, {}).get("timeline", {}).get("events", [])]

    got = first_view(5)
    want = bobs + [after]
    check(got == want, f"a first sync with timeline limit 5 shows Bob's four events then Alice's last message, in order "
                       f"({['bob%d' % bobs.index(e) if e in bobs else ('alice-after' if e == after else 'alice-old') for e in got]})")
    got = first_view(2)
    check(got == [bobs[-1], after], f"and with limit 2 the newest two ({['bob3' if e == bobs[-1] else ('alice-after' if e == after else 'other') for e in got]})")


if __name__ == "__main__":
    run_lab(["mock"], run)
