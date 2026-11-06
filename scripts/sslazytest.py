#!/usr/bin/env python3
"""Sliding sync with lazily loaded members hands out the memberships a timeline is about.

A client that asks for `["m.room.member", "$LAZY"]` gets, as required state, the membership
of the people its timeline is about: whoever sent an event, and whoever a membership event is
about (the person invited, removed or banned). Clients built on matrix-rust-sdk take a room's
state from `required_state` only, so a membership that is in the timeline alone never reaches
their list of members.

Three rules are checked, with Alice syncing and others acting:

  1. The person a membership event is about comes as state, whoever sent the event.
  2. Someone who speaks for the first time on a connection comes as state too, although
     their membership did not change.
  3. A member the connection has been given stays up to date, also when the change itself
     falls outside a short timeline. And it is still lazy: not everyone is sent.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/sslazytest.py [--keep]
"""
import urllib.parse, uuid

from fedlab import check, run_lab

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
STATE = [["m.room.create", ""], ["m.room.encryption", ""], ["m.room.member", "$LAZY"], ["m.room.member", "$ME"],
         ["m.room.name", ""], ["m.room.power_levels", ""]]


class Connection:
    """One sliding sync connection that subscribes to one room."""

    def __init__(self, hs, token, room_id, timeline_limit):
        self.hs, self.token, self.room_id, self.pos = hs, token, room_id, None
        self.body = {"conn_id": uuid.uuid4().hex[:8],
                     "lists": {"all": {"ranges": [[0, 19]], "timeline_limit": 1, "required_state": STATE}},
                     "room_subscriptions": {room_id: {"timeline_limit": timeline_limit, "required_state": STATE}}}

    def answer(self):
        """The room's part of the next answer; empty when the answer does not mention it."""
        path = f"{SLIDING}?timeout=0" + (f"&pos={self.pos}" if self.pos else "")
        st, r = self.hs.req("POST", path, self.body, self.token)
        assert st == 200, r
        self.pos = r["pos"]
        return r.get("rooms", {}).get(self.room_id, {})


def state_members(room):
    """{user: membership} of the member events in the answer's required state."""
    return {e["state_key"]: e["content"]["membership"] for e in room.get("required_state", []) if e["type"] == "m.room.member"}


def timeline(room):
    return [(e["type"], e.get("state_key"), e["content"].get("membership") or e["content"].get("body")) for e in room.get("timeline", [])]


def run(hs, peers):
    q = urllib.parse.quote
    (alice, a), (bob, b), (carol, c), (dave, d), (erin, _e) = (hs.register(n) for n in ("alice", "bob", "carol", "dave", "erin"))
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "name": "Lazy", "invite": [bob]}, a)
    assert st == 200, created
    room_id = created["room_id"]
    room = f"/_matrix/client/v3/rooms/{q(room_id)}"

    def act(token, action, body):
        st, r = hs.req("POST", f"{room}/{action}", body, token)
        assert st == 200, (action, r)

    act(b, "join", {})
    hs.message(b, room_id, "hello from Bob")
    hs.message(a, room_id, "hello from Alice")

    print("-- the person a membership event is about")
    wide = Connection(hs, a, room_id, 20)
    first = wide.answer()
    check(state_members(first) == {alice: "join", bob: "join"}, f"the first answer holds the members its timeline is about ({state_members(first)})")

    act(a, "invite", {"user_id": carol})
    got = wide.answer()
    check(state_members(got).get(carol) == "invite", f"someone Alice invites comes as state ({state_members(got)}; timeline {timeline(got)})")
    act(b, "invite", {"user_id": dave})
    got = wide.answer()
    check(state_members(got).get(dave) == "invite", f"someone Bob invites comes as state ({state_members(got)})")
    act(c, "join", {})
    got = wide.answer()
    check(state_members(got).get(carol) == "join", f"Carol's own join comes as state ({state_members(got)})")
    act(a, "kick", {"user_id": carol})
    got = wide.answer()
    check(state_members(got).get(carol) == "leave", f"someone who is removed comes as state ({state_members(got)})")
    act(a, "ban", {"user_id": dave})
    got = wide.answer()
    check(state_members(got).get(dave) == "ban", f"someone who is banned comes as state ({state_members(got)})")
    act(a, "unban", {"user_id": dave})
    got = wide.answer()
    check(state_members(got).get(dave) == "leave", f"and when the ban is lifted ({state_members(got)})")
    act(a, "invite", {"user_id": erin})
    act(a, "kick", {"user_id": erin})
    got = wide.answer()
    check(state_members(got).get(erin) == "leave", f"an invitation that is taken back leaves the person as gone ({state_members(got)})")
    check(wide.answer() == {}, "and with nothing new the room is not in the answer")

    print("-- someone who speaks for the first time on a connection")
    hs.message(a, room_id, "Alice again")
    narrow = Connection(hs, a, room_id, 1)
    first = narrow.answer()
    check(len(first.get("timeline", [])) == 1 and state_members(first) == {alice: "join"},
          f"a timeline of one event brings one member: lazy means not everyone ({state_members(first)}; timeline {timeline(first)})")
    hs.message(b, room_id, "Bob speaks")
    got = narrow.answer()
    check(state_members(got) == {bob: "join"}, f"Bob's membership comes with his first message ({state_members(got)}; timeline {timeline(got)})")
    hs.message(b, room_id, "Bob speaks again")
    got = narrow.answer()
    check(len(got.get("timeline", [])) == 1 and state_members(got) == {}, f"and is not sent again with his next ({state_members(got)})")

    print("-- a member the connection has been given stays up to date")
    st, r = hs.req("PUT", f"/_matrix/client/v3/profile/{q(bob)}/displayname", {"displayname": "Robert"}, b)
    assert st == 200, r
    hs.message(a, room_id, "Alice has the last word")
    got = narrow.answer()
    names = {e["state_key"]: e["content"].get("displayname") for e in got.get("required_state", []) if e["type"] == "m.room.member"}
    check([t[2] for t in timeline(got)] == ["Alice has the last word"], f"the timeline holds only the newest event ({timeline(got)})")
    check(names.get(bob) == "Robert", f"Bob's new name comes as state although the timeline does not hold the change ({names})")
    # A longer timeline on the same connection: the events come again, and whoever they
    # are about and has not been given yet comes with them.
    narrow.body["room_subscriptions"][room_id]["timeline_limit"] = 20
    got = narrow.answer()
    members = state_members(got)
    check(len(got.get("timeline", [])) > 1 and {carol, dave, erin} <= set(members), f"a longer timeline brings the members it is about ({members})")


if __name__ == "__main__":
    run_lab(["mock"], run)
