#!/usr/bin/env python3
"""Federation readers that take an event ID from the caller hand out only what the caller may
see (tracker #98).

- `/event/{eventId}` applies history visibility as backfill and `/get_missing_events` do: in a
  room with `joined` visibility, a message sent before the mock server's user joined comes back
  redacted, one sent after in full.
- `/event_auth/{roomId}/{eventId}` answers only for an event in `roomId`: the mock server, a
  member of one room, cannot read the auth chain of an event in another room it is not in.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/fedreadtest.py [--keep]
"""
import urllib.parse
import uuid

from fedlab import check, run_lab

q = urllib.parse.quote


def run(hs, peers):
    mock = peers["mock"]
    alice, token = hs.register("alice")

    def room(visibility):
        st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, token)
        assert st == 200, r
        rid = r["room_id"]
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(rid)}/state/m.room.history_visibility/",
                       {"history_visibility": visibility}, token)
        assert st == 200, r
        return rid

    def say(rid, body):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(rid)}/send/m.room.message/{uuid.uuid4().hex}",
                       {"msgtype": "m.text", "body": body}, token)
        assert st == 200, r
        return r["event_id"]

    shared = room("joined")
    before = say(shared, "said before the mock server was here")
    mock.join(hs.fed, shared, "bob")
    after = say(shared, "said after bob joined")
    other = room("shared")
    elsewhere = say(other, "in a room the mock server is not in")

    def event(eid):
        st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/event/{q(eid)}", None)
        pdus = r.get("pdus", []) if isinstance(r, dict) else []
        return st, (pdus[0].get("content", {}) if pdus else None)

    st, content = event(before)
    check(st == 200 and content == {}, f"/event of a message from before the join, in a 'joined' room: redacted ({st}, {content})")
    st, content = event(after)
    check(st == 200 and (content or {}).get("body") == "said after bob joined", f"/event of a message after the join: in full ({st})")
    st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/event/{q(elsewhere)}", None)
    check(st == 403, f"/event of a message in a room the mock server is not in: refused ({st})")

    st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/event_auth/{q(shared)}/{q(after)}", None)
    check(st == 200 and len(r.get("auth_chain", [])) >= 3, f"/event_auth for an event in the shared room: its auth chain ({st}, {len(r.get('auth_chain', []))} events)")
    st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/event_auth/{q(shared)}/{q(elsewhere)}", None)
    check(st == 404 and not r.get("auth_chain"),
          f"/event_auth naming the shared room but an event of another room: 404, no auth chain ({st}, {len(r.get('auth_chain', []))} events)")


if __name__ == "__main__":
    run_lab(["mock"], run)
