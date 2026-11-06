#!/usr/bin/env python3
"""Events delivered by a server other than the sender's, end to end.

When a user of server C joins a room through resident server B, it is B that passes the join
on to the other servers in the room; C does not. The receiving server has to take such an
event on the strength of its signatures. It does so when the deliverer is itself in the
room, and refuses when it is not.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/fedrelaytest.py [--keep]
"""
import time, urllib.parse, uuid

from fedlab import check, run_lab


def run(hs, peers):
    resident, joiner, stranger, other = peers["resident"], peers["joiner"], peers["stranger"], peers["other"]
    alice, token = hs.register("alice")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, token)
    assert st == 200, created
    room_id = created["room_id"]
    resident.join(hs.fed, room_id, "bob")

    def members():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room_id)}/joined_members", None, token)
        return sorted(r.get("joined", {}))

    def deliver(via, event):
        return via.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": via.name, "origin_server_ts": int(time.time() * 1000), "pdus": [event], "edus": []})

    # A join of the joiner's user, handed over by the resident server.
    carol, event_id, event = joiner.make_join_event(hs.fed, room_id, "carol")
    st, r = deliver(resident, event)
    verdict = r.get("pdus", {}).get(event_id)
    check(st == 200 and verdict == {}, f"a join passed on by a server in the room is accepted ({st} {verdict})")
    check(carol in members(), "and the user is a member")

    # The same from a server that is not in the room.
    dave, event_id, event = other.make_join_event(hs.fed, room_id, "dave")
    st, r = deliver(stranger, event)
    verdict = r.get("pdus", {}).get(event_id)
    check(st == 200 and isinstance(verdict, dict) and "not in the room" in verdict.get("error", ""), f"a join passed on by a server outside the room is refused ({st} {verdict})")
    check(dave not in members(), "and the user is not a member")

    # Delivered by its own server it is fine, of course.
    st, r = deliver(other, event)
    check(st == 200 and r.get("pdus", {}).get(event_id) == {} and dave in members(), "the same join from the user's own server is accepted")


if __name__ == "__main__":
    run_lab(["resident", "joiner", "stranger", "other"], run)
