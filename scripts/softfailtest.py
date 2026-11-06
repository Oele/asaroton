#!/usr/bin/env python3
"""A soft-failed event is kept and not shown to clients.

An event that is allowed where it sits in the room's history, but not by the room's state as
it is now, is soft-failed: stored, counted in state resolution, handed to other servers that
ask, and never relayed to clients. The case it exists for: a user is banned, and their server
goes on sending messages that build on the room as it was before the ban. Each of them passes
the checks for its place in history. Shown to clients, the ban would stop nobody.

The lab: Alice on the homeserver, Bob on one mock server and Carol on another share a room.
Alice bans Bob. Bob's server then sends a message, and a reaction to one of Alice's messages,
that both name the last event from before the ban as their predecessor.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/softfailtest.py [--keep]
"""
import time, urllib.parse, uuid

from fedlab import check, query, run_lab, signed_event

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"


def run(hs, peers):
    mock = peers["mock"]
    q = urllib.parse.quote
    alice, token = hs.register("alice")
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11", "name": "Soft failure"}, token)
    assert st == 200, created
    room_id = created["room_id"]
    room = f"/_matrix/client/v3/rooms/{q(room_id)}"
    bob = mock.join(hs.fed, room_id, "bob")
    other = peers["other"]
    other.join(hs.fed, room_id, "carol")
    st, state = hs.req("GET", f"{room}/state", None, token)
    assert st == 200, state
    ids = {(e["type"], e["state_key"]): e["event_id"] for e in state}
    create, power, joined = ids[("m.room.create", "")], ids[("m.room.power_levels", "")], ids[("m.room.member", bob)]
    st, sent = hs.req("PUT", f"{room}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "before the ban"}, token)
    assert st == 200, sent
    past = sent["event_id"]
    clock = [int(time.time() * 1000)]

    def from_bob(kind, content):
        clock[0] += 1000
        ev = {"type": kind, "room_id": room_id, "sender": bob, "content": content, "auth_events": [create, power, joined],
              "prev_events": [past], "depth": 20, "origin_server_ts": clock[0]}
        event_id, ev = signed_event(mock.name, mock.key, "ed25519:k1", ev)
        st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}", {"origin": mock.name, "origin_server_ts": clock[0], "pdus": [ev], "edus": []})
        assert st == 200, (st, r)
        return event_id

    def bodies(events):
        return [e["content"].get("body") or e["content"].get("membership") or e["type"] for e in events]

    def classic(since=None):
        st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0" + (f"&since={since}" if since else ""), None, token)
        assert st == 200, r
        return r["next_batch"], r.get("rooms", {}).get("join", {}).get(room_id, {})

    def sliding():
        body = {"conn_id": uuid.uuid4().hex[:8], "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 20, "required_state": []}}}
        st, r = hs.req("POST", f"{SLIDING}?timeout=0", body, token)
        assert st == 200, r
        return r.get("rooms", {}).get(room_id, {})

    print("-- while Bob is a member")
    since, _ = classic()
    welcome = from_bob("m.room.message", {"msgtype": "m.text", "body": "said as a member"})
    since, got = classic(since)
    check(bodies(got.get("timeline", {}).get("events", [])) == ["said as a member"], "a message of Bob arrives")

    print("-- after the ban")
    st, r = hs.req("POST", f"{room}/ban", {"user_id": bob}, token)
    assert st == 200, r
    evaded = from_bob("m.room.message", {"msgtype": "m.text", "body": "said by a banned user"})
    reaction = from_bob("m.reaction", {"m.relates_to": {"rel_type": "m.annotation", "event_id": past, "key": "👎"}})
    stored = query(f"SELECT rejected, soft_failed, outlier FROM events WHERE event_id IN ('{evaded}', '{reaction}')")
    check(stored == [["f", "t", "f"], ["f", "t", "f"]], f"both events are kept, as soft-failed ({stored})")
    ends = [row[0] for row in query(f"SELECT event_id FROM forward_extremities WHERE room_id = '{room_id}'")]
    check(evaded not in ends and reaction not in ends, "and new events will not build on them")

    since, got = classic(since)
    check(bodies(got.get("timeline", {}).get("events", [])) == ["ban"], f"classic sync shows the ban and nothing after it ({bodies(got.get('timeline', {}).get('events', []))})")
    _, full = classic()
    seen = bodies(full.get("timeline", {}).get("events", []))
    check("said by a banned user" not in seen and "m.reaction" not in seen and "said as a member" in seen, f"a sync from scratch does not show them either ({seen[-4:]})")
    seen = bodies(sliding().get("timeline", []))
    check("said by a banned user" not in seen and "m.reaction" not in seen and "said as a member" in seen, f"nor does sliding sync ({seen[-4:]})")
    st, r = hs.req("GET", f"{room}/messages?dir=b&limit=50", None, token)
    seen = bodies(r.get("chunk", []))
    check(st == 200 and "said by a banned user" not in seen and "m.reaction" not in seen and "said as a member" in seen, f"nor /messages ({seen[:4]})")
    for label, event_id in (("the message", evaded), ("the reaction", reaction)):
        st, r = hs.req("GET", f"{room}/event/{q(event_id)}", None, token)
        check(st == 404, f"/event does not hand out {label} ({st})")
        st, r = hs.req("GET", f"{room}/context/{q(event_id)}", None, token)
        check(st == 404, f"/context does not hand out {label} ({st})")
    st, r = hs.req("GET", f"{room}/context/{q(past)}?limit=10", None, token)
    seen = bodies(r.get("events_after", []))
    check(st == 200 and "said by a banned user" not in seen and "m.reaction" not in seen, f"the events after an earlier one do not include them ({seen})")
    st, r = hs.req("GET", f"/_matrix/client/v1/rooms/{q(room_id)}/relations/{q(past)}", None, token)
    check(st == 200 and r.get("chunk") == [], f"the reaction is not among the relations of Alice's message ({st}, {len(r.get('chunk', []))})")
    st, r = hs.req("GET", f"{room}/event/{q(past)}", None, token)
    check(st == 200 and "m.relations" not in r.get("unsigned", {}), f"nor counted on the message itself ({r.get('unsigned', {}).get('m.relations')})")
    st, r = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {"search_term": "banned"}}}, token)
    hits = [h["result"]["event_id"] for h in r.get("search_categories", {}).get("room_events", {}).get("results", [])]
    check(st == 200 and evaded not in hits, f"search does not find the message ({len(hits)} hits)")
    st, r = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {"search_term": "member"}}}, token)
    hits = [h["result"]["event_id"] for h in r.get("search_categories", {}).get("room_events", {}).get("results", [])]
    check(welcome in hits, f"while what Bob said as a member is still found ({len(hits)} hits)")

    print("-- other servers")
    st, r = other.fed(hs.fed, "GET", f"/_matrix/federation/v1/event/{q(evaded)}")
    check(st == 200 and [p.get("content", {}).get("body") for p in r.get("pdus", [])] == ["said by a banned user"], f"a server in the room that asks for the event by ID still gets it ({st})")


if __name__ == "__main__":
    run_lab(["mock", "other"], run)
