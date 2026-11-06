#!/usr/bin/env python3
"""Sliding sync with lazily loaded members hands out the memberships a timeline is about.

A client that asks for `["m.room.member", "$LAZY"]` gets, as required state, the membership
of the people its timeline is about: whoever sent an event, and whoever a membership event is
about (the person invited, removed or banned). Clients built on matrix-rust-sdk take a room's
state from `required_state` only, so a membership that is in the timeline alone never reaches
their list of members.

The rules that are checked, with Alice syncing and others acting:

  1. The person a membership event is about comes as state, whoever sent the event.
  2. Someone who speaks for the first time on a connection comes as state too, although
     their membership did not change.
  3. A member the connection has been given stays up to date, also when the change itself
     falls outside a short timeline, and also after the client asked for more state. And it
     is still lazy: not everyone is sent.
  4. Whoever removes Alice comes as state with the removal, and when she comes back the
     room starts over.
  5. State the connection has is not sent again. In a room with two loose ends (events of
     two servers that did not know of each other) the server's comparison of "then" and
     "now" keeps finding the same change; an answer that repeated it would never wait.
     And when such a room's state falls back to an earlier event, the connection is told,
     although "then" and "now" look the same.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/sslazytest.py [--keep]
"""
import time, urllib.parse, uuid

from fedlab import check, query, run_lab, signed_event

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
    check(bob not in members and alice not in members, f"and not the ones the connection has ({sorted(members)})")
    # The client asks for more state on the same connection. What it has is not sent again,
    # and the members it was given are still kept up to date.
    narrow.body["room_subscriptions"][room_id]["timeline_limit"] = 1
    st, r = hs.req("PUT", f"{room}/state/m.room.topic", {"topic": "Laziness"}, a)
    assert st == 200, r
    got = narrow.answer()
    types = sorted(e["type"] for e in got.get("required_state", []))
    check(types == [] and [t[0] for t in timeline(got)] == ["m.room.topic"], f"a topic nobody asked for is not sent as state ({types}; timeline {timeline(got)})")
    narrow.body["room_subscriptions"][room_id]["required_state"] = STATE + [["m.room.topic", ""]]
    got = narrow.answer()
    types = sorted(e["type"] for e in got.get("required_state", []))
    check(types == ["m.room.topic"], f"asking for more state brings what is new to the connection and nothing else ({types})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/profile/{q(bob)}/displayname", {"displayname": "Bobby"}, b)
    assert st == 200, r
    hs.message(a, room_id, "Alice, once more")
    got = narrow.answer()
    names = {e["state_key"]: e["content"].get("displayname") for e in got.get("required_state", []) if e["type"] == "m.room.member"}
    check(names.get(bob) == "Bobby", f"after that, a change to Bob still comes although the timeline does not hold it ({names})")

    print("-- whoever removes Alice, and coming back")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "name": "Door"}, d)
    assert st == 200, created
    door_id = created["room_id"]
    door = f"/_matrix/client/v3/rooms/{q(door_id)}"
    st, r = hs.req("POST", f"{door}/join", {}, a)
    assert st == 200, r
    hs.message(a, door_id, "Alice is in")
    conn = Connection(hs, a, door_id, 1)
    first = conn.answer()
    check(state_members(first) == {alice: "join"}, f"Alice knows only herself in the new room ({state_members(first)})")
    st, r = hs.req("POST", f"{door}/kick", {"user_id": alice}, d)
    assert st == 200, r
    got = conn.answer()
    check(state_members(got) == {alice: "leave", dave: "join"}, f"being removed brings her own membership and that of who did it ({state_members(got)}; timeline {timeline(got)})")
    hs.message(d, door_id, "said while Alice was out")
    st, r = hs.req("POST", f"{door}/join", {}, a)
    assert st == 200, r
    got = conn.answer()
    check(got.get("initial") is True and state_members(got).get(alice) == "join", f"coming back starts the room over (initial: {got.get('initial')}, {state_members(got)})")
    hs.message(d, door_id, "Dave speaks")
    got = conn.answer()
    check(timeline(got) == [("m.room.message", None, "Dave speaks")] and state_members(got) == {dave: "join"},
          f"and Dave's membership comes with his next message ({state_members(got)}; timeline {timeline(got)})")
    # Out again, and the room gets another name before the answer that says so is made up.
    # Then, between two answers: back in, and out once more. She was a member in between,
    # so the room is hers as it was at that last leave, new name included.
    st, r = hs.req("POST", f"{door}/kick", {"user_id": alice}, d)
    assert st == 200, r
    st, r = hs.req("PUT", f"{door}/state/m.room.name", {"name": "Revolving door"}, d)
    assert st == 200, r
    got = conn.answer()
    check(got.get("name") == "Door" and state_members(got).get(alice) == "leave", f"out again: the room as it was when she went ({got.get('name')}, {state_members(got)})")
    st, r = hs.req("POST", f"{door}/join", {}, a)
    assert st == 200, r
    st, r = hs.req("POST", f"{door}/kick", {"user_id": alice}, d)
    assert st == 200, r
    got = conn.answer()
    names = [e["content"].get("name") for e in got.get("required_state", []) if e["type"] == "m.room.name"]
    check(got.get("initial") is True and got.get("name") == "Revolving door" and names == ["Revolving door"],
          f"back and out again between two answers: the room starts over, as it was at the last leave (initial: {got.get('initial')}, name {got.get('name')}, state {names})")

    print("-- a room with two loose ends")
    mock = peers["mock"]
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11", "name": "Fork"}, a)
    assert st == 200, created
    fork_id = created["room_id"]
    remote = mock.join(hs.fed, fork_id, "remote")
    st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{q(fork_id)}/state", None, a)
    assert st == 200, state
    ids = {(e["type"], e["state_key"]): e["event_id"] for e in state}
    create, power, rules, joined = ids[("m.room.create", "")], ids[("m.room.power_levels", "")], ids[("m.room.join_rules", "")], ids[("m.room.member", remote)]
    st, sent = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(fork_id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "the common past"}, a)
    assert st == 200, sent
    past = sent["event_id"]
    clock = [int(time.time() * 1000)]

    def remote_event(kind, content, auth, state_key=None):
        # Both events build on the same past, as two servers do that have not heard of
        # each other's event.
        clock[0] += 1000
        ev = {"type": kind, "room_id": fork_id, "sender": remote, "content": content, "auth_events": auth, "prev_events": [past],
              "depth": 20, "origin_server_ts": clock[0]}
        if state_key is not None:
            ev["state_key"] = state_key
        event_id, ev = signed_event(mock.name, mock.key, "ed25519:k1", ev)
        st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": mock.name, "origin_server_ts": clock[0], "pdus": [ev], "edus": []})
        assert st == 200 and r.get("pdus", {}).get(event_id) == {}, (st, r)
        return event_id

    for label, members in (("lazily loaded members", "$LAZY"), ("all members", "*")):
        conn = Connection(hs, a, fork_id, 20)
        for entry in (conn.body["lists"]["all"], conn.body["room_subscriptions"][fork_id]):
            entry["required_state"] = [["m.room.create", ""], ["m.room.member", members], ["m.room.name", ""]]
        conn.answer()
        renamed = remote_event("m.room.member", {"membership": "join", "displayname": f"Remote ({label})"}, [create, power, rules, joined], remote)
        got = conn.answer()
        names = {e["state_key"]: e["content"].get("displayname") for e in got.get("required_state", []) if e["type"] == "m.room.member"}
        check(names.get(remote) == f"Remote ({label})", f"{label}: a name change over federation comes as state ({names})")
        remote_event("m.room.message", {"msgtype": "m.text", "body": "from the other end"}, [create, power, joined])
        got = conn.answer()
        check([t[2] for t in timeline(got)] == ["from the other end"], f"{label}: a message that does not build on the change arrives ({timeline(got)})")
        repeats = [conn.answer() for _ in range(3)]
        check(repeats == [{}, {}, {}], f"{label}: and after that the room is not in the answers ({[state_members(r) for r in repeats]})")
        if members == "$LAZY":
            # The room's state falls back to the membership from before the change, as a
            # later event can make state resolution do. (Done here by hand, in the table of
            # current state.) The room's newest event, the message, has had that older
            # membership in its state all along, so "then" and "now" are the same; what
            # differs is what the connection holds.
            changed = query(f"UPDATE current_state_events SET event_id = '{joined}' WHERE room_id = '{fork_id}' AND event_type = 'm.room.member' AND state_key = '{remote}' RETURNING 1")
            assert changed, "the state row was not found"
            got = conn.answer()
            held = {e["state_key"]: e["event_id"] for e in got.get("required_state", []) if e["type"] == "m.room.member"}
            check(held.get(remote) == joined, f"{label}: state that falls back to an earlier event is sent ({'the earlier event' if held.get(remote) == joined else held})")
            check(conn.answer() == {}, f"{label}: once")
            # Put back, for what follows.
            query(f"UPDATE current_state_events SET event_id = '{renamed}' WHERE room_id = '{fork_id}' AND event_type = 'm.room.member' AND state_key = '{remote}' RETURNING 1")
        # The next answers build on a new name, so the second round starts from this one.
        joined = renamed
        hs.message(a, fork_id, f"Alice ties the ends ({label})")
        past = hs.req("GET", f"/_matrix/client/v3/rooms/{q(fork_id)}/messages?dir=b&limit=1", None, a)[1]["chunk"][0]["event_id"]


if __name__ == "__main__":
    run_lab(["mock"], run)
