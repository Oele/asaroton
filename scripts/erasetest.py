#!/usr/bin/env python3
"""Account erasure: `erase: true` on `/account/deactivate` (Matrix v1.10).

"Any users (or servers) which join the room after the erasure request are served redacted
copies of the events sent by this account. Users which had visibility on those events prior to
the erasure are still able to see unredacted copies. No redactions are sent and the erasure
request is not shared over federation." And the account's other data goes.

The lab: Alice and Bob share a room (and the mock server `early` is in it). Alice names it,
sets a topic that Bob replaces, sends messages (one edited, one sticky), and keeps account
data, a push rule, a filter, an email address, a display name and a custom profile field.
Then she deactivates with `erase: true`, and:

- Bob, who was there, still gets her messages as they were: `/messages`, `/event`, `/sync`,
  the bundled edit, the old topic in `prev_content`.
- Carol, who joins afterwards, gets redacted copies of them (same event IDs, content gone) in
  `/sync`, sliding sync, `/messages`, `/event` and `/context`, no bundled edit, the old topic
  redacted in `prev_content`, Alice's old membership without her display name; `/search` does
  not find them, and her sticky event is not in Carol's sticky section. The room's name, set by
  Alice and still the room's state, stays.
- Alice's account data, push rules, filters, email addresses and profile are gone.
- The mock server `mock`, whose first user joins afterwards, gets redacted copies over
  `/event` and `/backfill`, and is not pushed Alice's sticky event (Bob's it is); `early` still
  gets the original.
- Nothing is sent: no redaction events, here or to the mock servers.
- Dave deactivates without `erase`: Carol, joining afterwards, sees his messages as they were.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/erasetest.py [--keep]
"""
import json, time, urllib.parse, uuid

from fedlab import check, query, run_lab

q = urllib.parse.quote
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"


def run(hs, peers):
    mock, early = peers["mock"], peers["early"]
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    dave, d = hs.register("dave")

    def ok(st, r, what):
        assert st == 200, (what, st, r)
        return r

    def send(token, room_id, content, event_type="m.room.message", query=""):
        path = f"/_matrix/client/v3/rooms/{q(room_id)}/send/{q(event_type)}/{uuid.uuid4().hex}"
        st, r = hs.req("PUT", path + (f"?{query}" if query else ""), content, token)
        return ok(st, r, "send")["event_id"]

    def text(token, room_id, body, query=""):
        return send(token, room_id, {"msgtype": "m.text", "body": body}, query=query)

    def join(token, room_id):
        st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room_id)}", {}, token)
        ok(st, r, "join")

    def messages(token, room_id):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/messages?dir=b&limit=100", None, token)
        return {e["event_id"]: e for e in ok(st, r, "messages")["chunk"]}

    def event(token, room_id, event_id):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room_id)}/event/{q(event_id)}", None, token)
        return ok(st, r, "event")

    def deactivate(token, user, erase):
        body = {"erase": True} if erase else {}
        st, r = hs.req("POST", "/_matrix/client/v3/account/deactivate", body, token)
        body["auth"] = {"type": "m.login.password", "session": r.get("session"),
                        "identifier": {"type": "m.id.user", "user": user}, "password": "pwpwpwpw"}
        st, r = hs.req("POST", "/_matrix/client/v3/account/deactivate", body, token)
        return st, r

    def body_of(ev):
        return ev.get("content", {}).get("body")

    # ---------------------------------------------------------------- before the erasure
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom",
                   {"preset": "public_chat", "room_version": "11", "name": "Erasure lab",
                    "power_level_content_override": {"events": {"m.room.topic": 0}}}, a)
    room = ok(st, r, "createRoom")["room_id"]
    join(b, room)
    early.join(hs.fed, room, "eve")
    ok(*hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/displayname", {"displayname": "Alice Example"}, a), "displayname")
    ok(*hs.req("PUT", f"/_matrix/client/v3/profile/{q(alice)}/m.tz", {"m.tz": "Europe/Amsterdam"}, a), "m.tz")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/state/m.room.topic/", {"topic": "alice's topic"}, a)
    alice_topic = ok(st, r, "topic")["event_id"]
    first = text(a, room, "alice secret one")
    second = text(a, room, "alice secret two")
    edit = send(a, room, {"msgtype": "m.text", "body": "* alice secret edited",
                          "m.new_content": {"msgtype": "m.text", "body": "alice secret edited"},
                          "m.relates_to": {"rel_type": "m.replace", "event_id": first}})
    alice_sticky = text(a, room, "alice sticky secret", "org.matrix.msc4354.sticky_duration_ms=600000")
    bob_sticky = text(b, room, "bob sticky", "org.matrix.msc4354.sticky_duration_ms=600000")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/state/m.room.topic/", {"topic": "bob's topic"}, b)
    bob_topic = ok(st, r, "topic")["event_id"]
    last_before = text(b, room, "bob after alice")

    # Alice's other data.
    ok(*hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/net.example.private", {"secret": 1}, a), "account data")
    ok(*hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/rooms/{q(room)}/account_data/net.example.room", {"secret": 2}, a), "room account data")
    ok(*hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/rooms/{q(room)}/tags/u.secret", {}, a), "tag")
    ok(*hs.req("PUT", "/_matrix/client/v3/pushrules/global/override/net.example.rule",
               {"actions": [], "conditions": [{"kind": "event_match", "key": "content.body", "pattern": "secret"}]}, a), "push rule")
    ok(*hs.req("POST", f"/_matrix/client/v3/user/{q(alice)}/filter", {"room": {"timeline": {"limit": 5}}}, a), "filter")
    query(f"INSERT INTO user_threepids VALUES ('{alice}', 'email', 'alice@example.org', 0, 0)")
    st, r = hs.req("GET", "/_matrix/client/v3/account/3pid", None, a)
    check(st == 200 and len(r.get("threepids", [])) == 1, f"Alice has an email address before ({r})")

    # Dave, deactivated without erase, in a room of his own with Bob.
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, d)
    control = ok(st, r, "createRoom")["room_id"]
    join(b, control)
    dave_said = text(d, control, "dave keeps his words")

    def alice_rows():
        return {t: query(f"SELECT count(*) FROM {t} WHERE user_id = '{alice}'")[0][0]
                for t in ("account_data", "push_rules", "filters", "user_threepids", "pushers")}

    before = alice_rows()
    check(before["account_data"] != "0" and before["push_rules"] == "1" and before["filters"] != "0",
          f"Alice has account data, a push rule and a filter before ({before})")

    # ---------------------------------------------------------------- the erasure
    marks = {p: p.count() for p in (mock, early)}
    st, r = deactivate(a, "alice", erase=True)
    check(st == 200 and r.get("id_server_unbind_result") == "success", f"deactivation with erase: true succeeds ({st}, {r})")
    st, r = deactivate(d, "dave", erase=False)
    check(st == 200, f"deactivation without erase succeeds ({st}, {r})")
    erased = query("SELECT user_id FROM erased_users")
    check(erased == [[alice]], f"Alice is marked as erased, Dave is not ({erased})")

    after = alice_rows()
    check(all(n == "0" for n in after.values()),
          f"Alice's account data, push rules, filters, email addresses and pushers are gone ({after})")
    st, r = hs.req("GET", f"/_matrix/client/v3/profile/{q(alice)}", None, b)
    check("displayname" not in r and "avatar_url" not in r and "m.tz" not in r, f"her profile is empty ({st}, {r})")

    # Carol joins both rooms after the erasure; so does a user of the mock server.
    join(c, room)
    join(c, control)
    mock.join(hs.fed, room, "mallory")
    mock.join(hs.fed, control, "mallory")

    # ---------------------------------------------------------------- Bob, who was there
    print("-- Bob, who was there before")
    seen = messages(b, room)
    check(body_of(seen.get(first, {})) == "alice secret one" and body_of(seen.get(second, {})) == "alice secret two",
          "/messages: Bob still gets Alice's messages as they were")
    check(seen.get(first, {}).get("unsigned", {}).get("m.relations", {}).get("m.replace", {}).get("event_id") == edit,
          "and the bundled edit")
    check(seen.get(bob_topic, {}).get("unsigned", {}).get("prev_content") == {"topic": "alice's topic"},
          f"and Alice's topic as the prev_content of Bob's ({seen.get(bob_topic, {}).get('unsigned')})")
    check(body_of(event(b, room, second)) == "alice secret two", "/event: the original")
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0&filter=" + q(json.dumps({"room": {"timeline": {"limit": 50}}})), None, b)
    timeline = {e["event_id"]: e for e in r.get("rooms", {}).get("join", {}).get(room, {}).get("timeline", {}).get("events", [])}
    check(body_of(timeline.get(second, {})) == "alice secret two", "/sync: the original")
    names = [e.get("content", {}).get("displayname") for e in seen.values()
             if e.get("type") == "m.room.member" and e.get("state_key") == alice]
    check("Alice Example" in names, f"and Alice's old membership with her display name ({names})")

    # ---------------------------------------------------------------- Carol, who joined after
    print("-- Carol, who joined after the erasure")
    seen = messages(c, room)
    for event_id, what in ((first, "the first message"), (second, "the second"), (edit, "the edit"), (alice_sticky, "the sticky one")):
        ev = seen.get(event_id)
        check(ev is not None and ev.get("content") == {} and ev.get("sender") == alice,
              f"/messages: {what} is there, redacted ({ev and ev.get('content')})")
    check(body_of(seen.get(last_before, {})) == "bob after alice", "Bob's messages are not")
    check("m.replace" not in seen.get(first, {}).get("unsigned", {}).get("m.relations", {}),
          f"no bundled edit ({seen.get(first, {}).get('unsigned', {}).get('m.relations')})")
    alice_topic_ev = seen.get(alice_topic, {})
    check(alice_topic_ev.get("content") == {}, f"Alice's old topic is redacted ({alice_topic_ev.get('content')})")
    prev = seen.get(bob_topic, {}).get("unsigned", {}).get("prev_content")
    check(seen.get(bob_topic, {}).get("content") == {"topic": "bob's topic"} and prev is not None and "topic" not in prev,
          f"Bob's topic is there, its prev_content redacted ({prev})")
    name = [e for e in seen.values() if e.get("type") == "m.room.name"]
    create = [e for e in seen.values() if e.get("type") == "m.room.create"]
    check(name and name[0].get("content") == {"name": "Erasure lab"} and name[0].get("sender") == alice,
          f"the room's name, set by Alice and still the room's state, stays ({name and name[0].get('content')})")
    check(create and create[0].get("content", {}).get("room_version") == "11", "so does the create event")
    names = [e.get("content", {}).get("displayname") for e in seen.values()
             if e.get("type") == "m.room.member" and e.get("state_key") == alice]
    check(names and not any(names), f"Alice's memberships carry no display name ({names})")

    ev = event(c, room, second)
    check(ev.get("event_id") == second and ev.get("content") == {}, f"/event: redacted ({ev.get('content')})")
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/context/{q(second)}?limit=10", None, c)
    around = {e["event_id"]: e for e in r.get("events_before", []) + r.get("events_after", [])}
    check(st == 200 and r.get("event", {}).get("content") == {} and around.get(first, {}).get("content") == {},
          f"/context: the event and those around it redacted ({st}, {r.get('event', {}).get('content')})")
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0&filter=" + q(json.dumps({"room": {"timeline": {"limit": 50}}})), None, c)
    joined = r.get("rooms", {}).get("join", {}).get(room, {})
    timeline = {e["event_id"]: e for e in joined.get("timeline", {}).get("events", [])}
    check(second in timeline and timeline[second].get("content") == {}, f"/sync: redacted ({timeline.get(second, {}).get('content')})")
    check(body_of(timeline.get(last_before, {})) == "bob after alice", "with Bob's messages as they are")
    sticky = [e["event_id"] for e in joined.get("msc4354_sticky", {}).get("events", []) + joined.get("timeline", {}).get("events", [])
              if e.get("event_id") in (alice_sticky, bob_sticky) and body_of(e)]
    check(alice_sticky not in sticky and bob_sticky in sticky, f"her sticky event is not handed out as sticky, Bob's is ({sticky})")
    body = {"conn_id": "carol", "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 50, "required_state": []}}}
    st, r = hs.req("POST", f"{SLIDING}?timeout=0", body, c)
    ss = {e["event_id"]: e for e in r.get("rooms", {}).get(room, {}).get("timeline", [])}
    check(second in ss and ss[second].get("content") == {}, f"sliding sync: redacted ({ss.get(second, {}).get('content')})")
    st, r = hs.req("GET", f"/_matrix/client/v1/rooms/{q(room)}/relations/{q(first)}", None, c)
    rel = {e["event_id"]: e for e in r.get("chunk", [])}
    check(rel.get(edit, {}).get("content") == {}, f"/relations: the edit redacted ({rel.get(edit, {}).get('content')})")

    def search(token):
        st, r = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {"search_term": "secret"}}}, token)
        return [h["result"]["event_id"] for h in r.get("search_categories", {}).get("room_events", {}).get("results", [])]

    hits_c, hits_b = search(c), search(b)
    check(not ({first, second} & set(hits_c)), f"/search does not find her messages for Carol ({len(hits_c)} hits)")
    check({first, second} <= set(hits_b), f"but does for Bob ({len(hits_b)} hits)")

    # ---------------------------------------------------------------- other servers
    print("-- other servers")

    def fed_event(peer, event_id):
        st, r = peer.fed(hs.fed, "GET", f"/_matrix/federation/v1/event/{q(event_id)}")
        pdus = r.get("pdus", [])
        return st, pdus[0] if pdus else {}

    st, pdu = fed_event(mock, second)
    check(st == 200 and pdu.get("content") == {} and pdu.get("sender") == alice,
          f"/event: a server that joined after gets the redacted copy ({st}, {pdu.get('content')})")
    st, pdu = fed_event(early, second)
    check(st == 200 and body_of(pdu) == "alice secret two", f"one that was there before, the original ({st}, {pdu.get('content')})")
    st, r = mock.fed(hs.fed, "GET", f"/_matrix/federation/v1/backfill/{q(room)}?v={q(last_before)}&limit=50")
    got = [(p.get("sender"), p.get("type"), p.get("content")) for p in r.get("pdus", [])]
    alices = [g for g in got if g[0] == alice and g[1] == "m.room.message"]
    check(st == 200 and len(alices) >= 3 and all(g[2] == {} for g in alices),
          f"/backfill: her messages redacted ({st}, {[g[2] for g in alices]})")
    check(any(g[0] == bob and (g[2] or {}).get("body") == "bob sticky" for g in got), "Bob's as they are")
    time.sleep(1.5)
    pushed = [p.get("content", {}).get("body") for req in mock.requests(marks[mock]) for p in req["body"].get("pdus", [])]
    check("alice sticky secret" not in pushed and "bob sticky" in pushed,
          f"the joining server is pushed Bob's sticky event, not Alice's ({pushed})")

    print("-- nothing sent")
    redactions = query(f"SELECT count(*) FROM events WHERE room_id IN ('{room}', '{control}') AND event_type = 'm.room.redaction'")
    check(redactions == [["0"]], f"no redaction events ({redactions})")
    sent = [p.get("type") for peer in (mock, early) for req in peer.requests(marks[peer]) for p in req["body"].get("pdus", [])]
    check("m.room.redaction" not in sent, f"none sent to other servers ({sorted(set(sent))})")

    # ---------------------------------------------------------------- without erase
    print("-- deactivation without erase")
    seen = messages(c, control)
    check(body_of(seen.get(dave_said, {})) == "dave keeps his words", "Carol, joining after, sees Dave's message as it was")
    check(body_of(event(c, control, dave_said)) == "dave keeps his words", "also with /event")
    st, pdu = fed_event(mock, dave_said)
    check(st == 200 and body_of(pdu) == "dave keeps his words", f"and so does a server that joined after ({st})")


if __name__ == "__main__":
    run_lab(["mock", "early"], run)
