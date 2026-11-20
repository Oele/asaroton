#!/usr/bin/env python3
"""A NUL character in an event stalls no room (tracker issue 67).

PostgreSQL cannot hold NUL in text or jsonb. A state event with NUL anywhere in it (a display
name) used to make asaroton fail to store it and everything after it in the room; one with NUL in
its type or state key cannot be stored at all. Here Bob, a member on a mock server, changes his
display name to one with NUL, sends a state event whose state key holds NUL, and goes on talking.
Alice, the local user, must see his name change and his later messages; the event with the
unstorable state key is refused alone. Locally, requests with NUL in the path or the JSON are
answered 400, not 500.

usage: scripts/nultest.py   (starts its own server, like the other fedlab scripts)
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
    create, power, rules = by_type[("m.room.create", "")], by_type[("m.room.power_levels", "")], by_type[("m.room.join_rules", "")]
    bob_join = by_type[("m.room.member", bob)]
    st, sent = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "hello Bob"}, token)
    assert st == 200, sent
    latest = sent["event_id"]
    clock = [int(time.time() * 1000)]

    def event(kind, content, auth, prev, depth, state_key=None):
        clock[0] += 1000
        ev = {"type": kind, "room_id": room_id, "sender": bob, "content": content, "auth_events": auth,
              "prev_events": [prev], "depth": depth, "origin_server_ts": clock[0]}
        if state_key is not None:
            ev["state_key"] = state_key
        return signed_event(mock.name, mock.key, "ed25519:k1", ev)

    def deliver(event_id, ev):
        st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}",
                         {"origin": mock.name, "origin_server_ts": int(time.time() * 1000), "pdus": [ev], "edus": []})
        return st, r.get("pdus", {}).get(event_id)

    def timeline():
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/messages?dir=b&limit=30", None, token)
        assert st == 200, r
        return {e["event_id"]: e for e in r.get("chunk", [])}

    # 1. A display name with NUL: stored, and the room goes on.
    name_id, name_ev = event("m.room.member", {"membership": "join", "displayname": "Bob\u0000the Nul"}, [create, power, rules, bob_join], latest, 20, bob)
    st, result = deliver(name_id, name_ev)
    check(st == 200 and result == {}, f"the name change with NUL is taken ({st} {result})")
    wait_for(lambda: name_id in timeline(), 10, 0.2)
    check(name_id in timeline(), "and is in the room")
    st, members = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/joined_members", None, token)
    shown = members.get("joined", {}).get(bob, {}).get("display_name")
    check(shown == "Bobthe Nul", f"the member list shows the name without the NUL ({shown!r})")

    # 2. A state event whose state key holds NUL cannot be stored: refused, on its own.
    bad_id, bad_ev = event("org.example.nul", {"x": 1}, [create, power, name_id], name_id, 21, "a\u0000b")
    st, result = deliver(bad_id, bad_ev)
    check(st == 200 and result and "NUL" in result.get("error", ""), f"the state event with NUL in its key is refused ({st} {result})")

    # 3. Bob goes on talking, building on his name change.
    msg_id, msg_ev = event("m.room.message", {"msgtype": "m.text", "body": "still here"}, [create, power, name_id], name_id, 22)
    st, result = deliver(msg_id, msg_ev)
    check(st == 200 and result == {}, f"his next message is taken ({st} {result})")
    wait_for(lambda: msg_id in timeline(), 10, 0.2)
    check(msg_id in timeline() and bad_id not in timeline(), "it is in the room, the refused event is not")

    # 3b. A message with NUL in its body is found by /search, also with the filters that read
    #     the event's content (net.oele.search_filters, contains_url).
    nul_id, nul_ev = event("m.room.message", {"msgtype": "m.text", "body": "zoek\u0000woord https://example.org"}, [create, power, name_id], msg_id, 23)
    st, result = deliver(nul_id, nul_ev)
    check(st == 200 and result == {}, f"a message with NUL in its body is taken ({st} {result})")
    wait_for(lambda: nul_id in timeline(), 10, 0.2)
    for flt in ({}, {"net.oele.contains_link": True}, {"net.oele.msgtypes": ["m.text"]}, {"contains_url": False}):
        st, r = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {
            "search_term": "zoekwoord", "filter": {"rooms": [room_id], **flt}}}}, token)
        hits = [x["result"]["event_id"] for x in r.get("search_categories", {}).get("room_events", {}).get("results", [])]
        check(st == 200 and hits == [nul_id], f"/search finds it with filter {flt} ({st} {r.get('errcode', '')} {len(hits)} hits)")

    # 4. Locally: NUL in the path or in the body is a 400, not a 500.
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/state/org.example.nul/a%00b", {"x": 1}, token)
    check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"a state key with NUL in the path: {st} {r.get('errcode')}")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/state/m.room.topic/", {"topic": "a\u0000b"}, token)
    check(st == 400 and r.get("errcode") == "M_BAD_JSON", f"NUL in a state event's content: {st} {r.get('errcode')}")
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/org.example.nul", {"v": "a\u0000b"}, token)
    check(st == 400 and r.get("errcode") == "M_BAD_JSON", f"NUL in account data: {st} {r.get('errcode')}")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "fine"}, token)
    check(st == 200, f"and an ordinary message after all that works ({st})")


if __name__ == "__main__":
    run_lab(["mock"], run)
