#!/usr/bin/env python3
"""An event that cannot be checked when it arrives is checked later, end to end.

An event is checked against the auth events it cites. When the server that sends it cannot
(or does not) hand over one of those, the event cannot be checked. That says nothing about
the event: it is set aside and tried again, and once the missing event can be had it goes
through the normal checks and reaches the clients as a message that arrived late. So do the
events that were rejected in the meantime only because they cite it.

The lab: Alice's room on the homeserver, Bob on a mock server.

  P  Bob changes his display name. The mock keeps this event to itself, and at first
     answers 404 when asked for it.
  X  Bob changes his display name again; X cites P.          -> cannot be checked, set aside
  Y  A message of Bob that cites P.                          -> the same, without asking again
  Z  A message of Bob that cites X.                          -> rejected: X is rejected
  W  Another message that cites P.                           -> set aside
  V  A message that builds on W and cites only what is known -> accepted at once
  Then the mock starts serving P, and X, Y, Z and W have to turn up for Alice. W, which V
  already builds on, must not become a loose end of the room.

Needs what scripts/fedlab.py needs. Takes about twenty seconds.

Usage: scripts/fedrechecktest.py [--keep]
"""
import re, time, urllib.parse, uuid

from fedlab import check, query, run_lab, signed_event, wait_for

RETRY_SECS = 2


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
    create, power, rules = by_type[("m.room.create", "")], by_type[("m.room.power_levels", "")], by_type[("m.room.join_rules", "")]
    bob_join = by_type[("m.room.member", bob)]
    st, sent = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "hello Bob"}, token)
    assert st == 200, sent
    latest = sent["event_id"]

    # Each event a second after the one before, and all after Bob's join: state resolution
    # orders a user's membership events by timestamp (and by ID within one millisecond).
    clock = [int(time.time() * 1000)]

    def event(kind, content, auth, prev, depth, state_key=None):
        clock[0] += 1000
        ev = {"type": kind, "room_id": room_id, "sender": bob, "content": content, "auth_events": auth, "prev_events": [prev],
              "depth": depth, "origin_server_ts": clock[0]}
        if state_key is not None:
            ev["state_key"] = state_key
        return signed_event(mock.name, mock.key, "ed25519:k1", ev)

    def text(body, auth, prev, depth):
        return event("m.room.message", {"msgtype": "m.text", "body": body}, auth, prev, depth)

    p_id, p = event("m.room.member", {"membership": "join", "displayname": "Bob the First"}, [create, power, rules, bob_join], latest, 20, bob)
    x_id, x = event("m.room.member", {"membership": "join", "displayname": "Bob the Second"}, [create, power, rules, p_id], latest, 21, bob)
    y_id, y = text("Y: cites the event that is withheld", [create, power, p_id], latest, 21)
    z_id, z = text("Z: cites the second name change", [create, power, x_id], x_id, 22)
    w_id, w = text("W: cites the withheld event too", [create, power, p_id], latest, 21)
    v_id, v = text("V: builds on W", [create, power, bob_join], w_id, 22)

    # The mock is asked for P over /event; it says 404 until `serving` is set.
    serving, asked = [], []

    def serve_event(handler, m):
        wanted = urllib.parse.unquote(m.group(1))
        asked.append(wanted)
        if serving and wanted == p_id:
            return handler.answer(200, {"origin": mock.name, "origin_server_ts": int(time.time() * 1000), "pdus": [p]})
        handler.answer(404, {"errcode": "M_NOT_FOUND", "error": "mock: no such event"})

    mock.routes.append((re.compile(r"/_matrix/federation/v1/event/([^/?]+)"), serve_event))

    def deliver(event_id, ev):
        st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": mock.name, "origin_server_ts": int(time.time() * 1000), "pdus": [ev], "edus": []})
        return st, r.get("pdus", {}).get(event_id)

    def sync(since=None):
        st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0" + (f"&since={since}" if since else ""), None, token)
        assert st == 200, r
        room = r.get("rooms", {}).get("join", {}).get(room_id, {})
        return r["next_batch"], [e["event_id"] for e in room.get("timeline", {}).get("events", [])]

    def bobs_name():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/joined_members", None, token)
        return r.get("joined", {}).get(bob, {}).get("display_name")

    def waiting():
        return {row[0]: int(row[1]) for row in query("SELECT event_id, tries FROM events_awaiting_auth")}

    def loose_ends():
        return {row[0] for row in query(f"SELECT event_id FROM forward_extremities WHERE room_id = '{room_id}'")}

    def stored(event_id):
        rows = query(f"SELECT rejected, outlier FROM events WHERE event_id = '{event_id}'")
        return tuple(v == "t" for v in rows[0]) if rows else None

    since, _ = sync()

    print("-- while the cited event is withheld")
    st, verdict = deliver(x_id, x)
    check(st == 200 and verdict == {}, f"the sender is not told the event failed ({st} {verdict})")
    check(stored(x_id) == (True, True), f"X is kept, as rejected and outside the timeline ({stored(x_id)})")
    check(x_id in waiting(), "and listed to be tried again")
    fetches = asked.count(p_id)
    check(fetches == 1, f"the mock was asked for the withheld event once ({fetches})")
    st, verdict = deliver(y_id, y)
    check(st == 200 and verdict == {} and y_id in waiting(), "Y, which cites the same event, is set aside too")
    check(asked.count(p_id) == fetches, f"without asking the mock again so soon ({asked.count(p_id)})")
    st, verdict = deliver(z_id, z)
    check(st == 200 and stored(z_id) == (True, False), f"Z, which cites X, is rejected in the timeline ({st} {verdict} {stored(z_id)})")
    check(z_id not in waiting(), "and not listed: it could be checked, against a rejected event")
    st, verdict = deliver(w_id, w)
    check(st == 200 and w_id in waiting(), "W is set aside as well")
    st, verdict = deliver(v_id, v)
    check(st == 200 and verdict == {} and stored(v_id) == (False, False), f"V, which builds on W but can be checked, is accepted ({st} {verdict} {stored(v_id)})")
    check(len(hs.log_lines("could not be checked and is set aside")) == 3, "the log says so for X, Y and W")
    _, seen = sync(since)
    check(not {x_id, y_id, z_id, w_id} & set(seen) and v_id in seen and bobs_name() != "Bob the Second", "Alice sees V and none of the others")

    # The worker tries again, and goes on waiting.
    check(wait_for(lambda: asked.count(p_id) > fetches and min(waiting().values(), default=0) >= 1, 15, 0.2), f"the events are tried again while the event stays withheld (asked {asked.count(p_id)} times, tries {waiting()})")
    check(set(waiting()) == {x_id, y_id, w_id}, "all three are still listed")

    print("-- once the cited event can be had")
    serving.append(True)
    arrived = wait_for(lambda: not waiting() and stored(z_id) == (False, False), 40, 0.2)
    check(arrived, f"the list empties (still waiting: {waiting()}, Z: {stored(z_id)})")
    check(stored(x_id) == (False, False) and stored(y_id) == (False, False) and stored(w_id) == (False, False), f"X, Y and W are accepted and in the timeline ({stored(x_id)} {stored(y_id)} {stored(w_id)})")
    check(stored(p_id) == (False, True), f"the event that was withheld is held as an auth event ({stored(p_id)})")
    check(bobs_name() == "Bob the Second", f"Bob's new name is the room's state ({bobs_name()})")
    _, seen = sync(since)
    check({x_id, y_id, z_id, w_id} <= set(seen), f"Alice receives all four in her next sync ({len(seen)} events)")
    ends = loose_ends()
    check(w_id not in ends and x_id not in ends, f"W, which V builds on, is no loose end of the room, and neither is X, which Z builds on ({len(ends)} ends)")
    check(seen.index(x_id) < seen.index(z_id) if {x_id, z_id} <= set(seen) else False, "the name change before the message that cites it")
    check(len(hs.log_lines("could be checked now, and is accepted")) == 3, "the log reports X, Y and W as accepted")
    check(len(hs.log_lines("are accepted now")) >= 1, "and the event that had been rejected for citing X")

    # A search finds a restored message: its index rows were written when it was accepted.
    st, found = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {"search_term": "withheld"}}}, token)
    hits = [h["result"]["event_id"] for h in found.get("search_categories", {}).get("room_events", {}).get("results", [])]
    check(y_id in hits, f"search finds the restored message ({st}, {len(hits)} hits)")

    # Bob's next message cites his current membership and passes straight away.
    n_id, n = text("N: a new message", [create, power, x_id], z_id, 23)
    st, verdict = deliver(n_id, n)
    check(st == 200 and verdict == {} and stored(n_id) == (False, False), f"a new message of Bob is accepted at once ({st} {verdict} {stored(n_id)})")

    # Alice's next message ties up what loose ends there are.
    hs.message(token, room_id, "tying up")
    check(len(loose_ends()) == 1, f"after a local message the room has one end again ({len(loose_ends())})")


if __name__ == "__main__":
    run_lab(["mock"], run, f"unchecked_event_retry_secs = {RETRY_SECS}")
