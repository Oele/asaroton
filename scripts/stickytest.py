#!/usr/bin/env python3
"""Sticky events (MSC4354, Matrix v1.20; tracker #90), stage A: sending and marking.

- `PUT /rooms/{roomId}/send/{type}/{txnId}?sticky_duration_ms=N` gives the event a top-level
  `sticky: {duration_ms: N}`; with `?org.matrix.msc4354.sticky_duration_ms=N` (what
  matrix-js-sdk sends) it is `msc4354_sticky` instead.
- Clients see the time left in `unsigned`: `sticky_duration_remaining_ms` for `sticky`,
  `msc4354_sticky_duration_ttl_ms` for `msc4354_sticky`. It counts down, is at most an hour
  whatever duration was asked for, and is gone once the event has expired.
- A duration that is zero, negative or not a number is `M_INVALID_PARAM`, and so is a sticky
  state event through a delayed send.
- A delayed send can be sticky: `org.matrix.msc4354.sticky_duration_ms` (or
  `sticky_duration_ms`) in the body of `PUT .../delayed_event/...`, or the query names on a
  delayed `/send` (`?org.matrix.msc4140.delay=`). The event it becomes carries the marker.
- An event without a sticky duration has neither marker nor field.

Stage B, `/sync` v3 (the joined room's `msc4354_sticky` section, the name matrix-js-sdk reads):
- A user who joins a room whose history visibility is `joined` gets the unexpired sticky events
  sent before they joined in their first sync of the room, and may fetch them with `/event`,
  while the plain messages from before stay hidden. Expired ones and those of ignored users are
  left out.
- On an incremental sync, a sticky event that the (limited) timeline does not show comes in the
  section; one the timeline shows does not come twice; nothing comes again on the next sync.
- With lazy-loaded members, the sticky event's sender's membership is in the state.

Stage C, sliding sync (MSC4480, extension `org.matrix.msc4354.sticky_events` as matrix-js-sdk
sends it, or `sticky_events`; the answer uses the name asked by):
- A new connection gets every unexpired sticky event of its joined rooms in scope that the
  timeline does not show; later responses those sent since, once each; a repeated `pos` gets
  the same again.
- `limit` is per room: the oldest come first, the rest in the next responses, which do not wait.

Stage D, federation: when the first user of another server joins a room, that server is sent
our own sticky events in the room that are still sticky, oldest first, with their marker; not
expired ones, and not again when a second user of that server joins.

Stage E, soft failure: a remote sticky event that passes its own auth events but not the room's
current state is soft-failed (kept, not shown). When the state changes so that it passes, it is
accepted after all and reaches clients like a new event.

Needs what scripts/fedlab.py needs. Takes about fifteen seconds.

Usage: scripts/stickytest.py [--keep]
"""
import json, time, urllib.parse, uuid

from fedlab import check, query, run_lab, signed_event

q = urllib.parse.quote
UNSTABLE_DELAYED = "/_matrix/client/unstable/org.matrix.msc4140"
HOUR = 3_600_000


def run(hs, peers):
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r

    def send(query, body="hi", token=a, event_type="m.room.message"):
        path = f"/_matrix/client/v3/rooms/{q(room)}/send/{q(event_type)}/{uuid.uuid4().hex}"
        if query:
            path += "?" + query
        return hs.req("PUT", path, {"msgtype": "m.text", "body": body}, token)

    def event(event_id, token=b):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/event/{q(event_id)}", None, token)
        assert st == 200, (st, r)
        return r

    def messages(token=b):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/messages?dir=b&limit=50", None, token)
        return {e["event_id"]: e for e in r.get("chunk", [])}

    # Both dialects: the marker the API names, and the matching unsigned field.
    st, r = send("sticky_duration_ms=600000", "stable")
    stable = r.get("event_id")
    check(st == 200 and stable, f"/send with sticky_duration_ms is accepted ({st}, {r})")
    ev = event(stable)
    left = ev.get("unsigned", {}).get("sticky_duration_remaining_ms")
    check(ev.get("sticky") == {"duration_ms": 600000} and "msc4354_sticky" not in ev,
          f"the event carries sticky: {{duration_ms}} ({ev.get('sticky')}, {sorted(ev)})")
    check(isinstance(left, int) and 590_000 < left <= 600_000
          and "msc4354_sticky_duration_ttl_ms" not in ev["unsigned"],
          f"unsigned.sticky_duration_remaining_ms counts the time left ({ev.get('unsigned')})")

    st, r = send("org.matrix.msc4354.sticky_duration_ms=300000", "unstable")
    unstable = r.get("event_id")
    check(st == 200 and unstable, f"/send with the unstable name is accepted ({st}, {r})")
    ev = messages().get(unstable, {})
    left = ev.get("unsigned", {}).get("msc4354_sticky_duration_ttl_ms")
    check(ev.get("msc4354_sticky") == {"duration_ms": 300000} and "sticky" not in ev,
          f"the event carries msc4354_sticky ({ev.get('msc4354_sticky')}, {sorted(ev)})")
    check(isinstance(left, int) and 290_000 < left <= 300_000
          and "sticky_duration_remaining_ms" not in ev["unsigned"],
          f"/messages: unsigned.msc4354_sticky_duration_ttl_ms counts the time left ({ev.get('unsigned')})")

    # Longer than an hour: the marker keeps what was asked, the time left is at most an hour.
    st, r = send("sticky_duration_ms=7200000", "long")
    ev = event(r["event_id"])
    left = ev.get("unsigned", {}).get("sticky_duration_remaining_ms", 0)
    check(ev.get("sticky") == {"duration_ms": 7200000} and HOUR - 10_000 < left <= HOUR,
          f"two hours asked: the marker says so, the time left is at most an hour ({ev.get('sticky')}, {left})")

    # It counts down and goes once the event has expired.
    st, r = send("sticky_duration_ms=2000", "brief")
    brief = r["event_id"]
    first = event(brief)["unsigned"].get("sticky_duration_remaining_ms", 0)
    time.sleep(0.5)
    second = event(brief)["unsigned"].get("sticky_duration_remaining_ms", 0)
    check(0 < second < first <= 2000, f"the time left counts down ({first} -> {second})")
    time.sleep(2)
    ev = event(brief)
    check(ev.get("sticky") == {"duration_ms": 2000} and "sticky_duration_remaining_ms" not in ev["unsigned"],
          f"expired: the marker stays, the time left is gone ({ev.get('unsigned')})")

    # Not sticky: no marker, no field.
    st, r = send(None, "plain")
    ev = event(r["event_id"])
    check(not ({"sticky", "msc4354_sticky"} & set(ev))
          and not ({"sticky_duration_remaining_ms", "msc4354_sticky_duration_ttl_ms"} & set(ev["unsigned"])),
          f"a plain event has no sticky marker ({sorted(ev)}, {ev['unsigned']})")

    # Durations that are not positive numbers are refused.
    for bad in ["sticky_duration_ms=0", "sticky_duration_ms=-5", "sticky_duration_ms=soon",
                "org.matrix.msc4354.sticky_duration_ms=0"]:
        st, r = send(bad, "bad")
        check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"?{bad} is M_INVALID_PARAM ({st}, {r})")

    # Delayed + sticky through the dedicated endpoint (as matrix-js-sdk does it), both names.
    for name, marker in [("org.matrix.msc4354.sticky_duration_ms", "msc4354_sticky"),
                         ("sticky_duration_ms", "sticky")]:
        st, r = hs.req("PUT", f"{UNSTABLE_DELAYED}/rooms/{q(room)}/delayed_event/m.room.message/{uuid.uuid4().hex}",
                       {"delay_ms": 60000, "content": {"msgtype": "m.text", "body": f"delayed {marker}"},
                        name: 600000}, a)
        delay_id = r.get("delay_id")
        check(st == 200 and delay_id, f"a delayed send with {name} in the body is scheduled ({st}, {r})")
        st, r = hs.req("POST", f"/_matrix/client/v1/delayed_events/{q(delay_id)}/send", {}, a)
        st2, v = hs.req("GET", f"/_matrix/client/v1/delayed_events/{q(delay_id)}", None, a)
        sent = v.get("finalised", {}).get("event_id")
        ev = event(sent) if sent else {}
        check(ev.get(marker) == {"duration_ms": 600000},
              f"the event it became carries {marker} ({st}, {v.get('finalised')}, {sorted(ev)})")

    # Delayed + sticky through the query of /send (the older MSC4140 form).
    st, r = send("org.matrix.msc4140.delay=60000&org.matrix.msc4354.sticky_duration_ms=600000", "query form")
    delay_id = r.get("delay_id")
    check(st == 200 and delay_id, f"a delayed /send with the sticky query is scheduled ({st}, {r})")
    st, r = hs.req("POST", f"/_matrix/client/v1/delayed_events/{q(delay_id)}/send", {}, a)
    st2, v = hs.req("GET", f"/_matrix/client/v1/delayed_events/{q(delay_id)}", None, a)
    sent = v.get("finalised", {}).get("event_id")
    ev = event(sent) if sent else {}
    check(ev.get("msc4354_sticky") == {"duration_ms": 600000},
          f"the event it became carries msc4354_sticky ({v.get('finalised')}, {sorted(ev)})")

    # A state event cannot be sticky.
    st, r = hs.req("PUT", f"{UNSTABLE_DELAYED}/rooms/{q(room)}/delayed_event/org.example.state/{uuid.uuid4().hex}",
                   {"delay_ms": 60000, "state_key": "", "content": {"a": 1},
                    "org.matrix.msc4354.sticky_duration_ms": 600000}, a)
    check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"a sticky delayed state event is M_INVALID_PARAM ({st}, {r})")
    st, r = hs.req("PUT", f"{UNSTABLE_DELAYED}/rooms/{q(room)}/delayed_event/m.room.message/{uuid.uuid4().hex}",
                   {"delay_ms": 60000, "content": {"msgtype": "m.text", "body": "x"}, "sticky_duration_ms": 0}, a)
    check(st == 400 and r.get("errcode") == "M_INVALID_PARAM", f"a zero sticky duration in the body is M_INVALID_PARAM ({st}, {r})")

    sync_part(hs, a)
    sliding_part(hs, a)
    federation_part(hs, a, peers["mock"])
    soft_fail_part(hs, a, peers["mock"])


def sync_part(hs, a):
    carol, c = hs.register("carol")
    dave, d = hs.register("dave")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {
        "preset": "public_chat",
        "initial_state": [{"type": "m.room.history_visibility", "state_key": "", "content": {"history_visibility": "joined"}}],
    }, a)
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, d)
    assert st == 200, r

    def send(token, body, sticky_ms=None):
        path = f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{uuid.uuid4().hex}"
        if sticky_ms:
            path += f"?org.matrix.msc4354.sticky_duration_ms={sticky_ms}"
        st, r = hs.req("PUT", path, {"msgtype": "m.text", "body": body}, token)
        assert st == 200, (st, r)
        return r["event_id"]

    def sync(token, since=None, limit=10, lazy=False):
        flt = {"room": {"timeline": {"limit": limit}}}
        if lazy:
            flt["room"]["state"] = {"lazy_load_members": True}
        path = f"/_matrix/client/v3/sync?timeout=0&filter={q(json.dumps(flt))}" + (f"&since={since}" if since else "")
        st, r = hs.req("GET", path, None, token)
        assert st == 200, (st, r)
        joined = r.get("rooms", {}).get("join", {}).get(room, {})
        return r["next_batch"], joined

    def ids(section):
        return [e["event_id"] for e in section.get("events", [])]

    before_plain = send(a, "said before carol came")
    before_sticky = send(a, "call membership, before carol came", 600000)
    expired = send(a, "brief", 1000)
    ignored_sticky = send(d, "dave's sticky", 600000)
    # Carol ignores Dave.
    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(carol)}/account_data/m.ignored_user_list",
                   {"ignored_users": {dave: {}}}, c)
    assert st == 200, r
    time.sleep(1.2)
    for i in range(3):
        send(a, f"filler {i}")
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, c)
    assert st == 200, r

    batch, joined = sync(c, limit=2, lazy=True)
    sticky = ids(joined.get("msc4354_sticky", {}))
    timeline = ids(joined.get("timeline", {}))
    check(sticky == [before_sticky],
          f"carol's first sync: the sticky event from before she joined, nothing else ({sticky})")
    check(before_plain not in timeline + sticky, "the plain message from before stays hidden")
    ev = next(iter(joined.get("msc4354_sticky", {}).get("events", [])), {})
    check(ev.get("msc4354_sticky") == {"duration_ms": 600000}
          and ev.get("unsigned", {}).get("msc4354_sticky_duration_ttl_ms", 0) > 0,
          f"it carries its marker and the time left ({ev.get('unsigned')})")
    members = [e.get("state_key") for e in joined.get("state", {}).get("events", []) if e.get("type") == "m.room.member"]
    check(a_user(hs, a) in members, f"lazy members: the sticky sender's membership is in the state ({members})")
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/event/{q(before_sticky)}", None, c)
    check(st == 200, f"/event: carol may fetch the sticky event from before she joined ({st})")
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/event/{q(before_plain)}", None, c)
    check(st == 404, f"/event: not the plain message from before ({st})")
    check(expired not in sticky and ignored_sticky not in sticky, "expired and ignored senders' sticky events are left out")

    # Incremental: one sticky event behind a limited timeline, one the timeline shows.
    hidden = send(a, "sticky, then pushed out of the timeline", 600000)
    for i in range(3):
        send(a, f"more {i}")
    batch2, joined = sync(c, since=batch, limit=2)
    check(ids(joined.get("msc4354_sticky", {})) == [hidden] and joined.get("timeline", {}).get("limited"),
          f"incremental, limited timeline: the sticky event comes in the section ({ids(joined.get('msc4354_sticky', {}))})")
    shown = send(a, "sticky and in the timeline", 600000)
    batch3, joined = sync(c, since=batch2, limit=10)
    check(shown in ids(joined.get("timeline", {})) and "msc4354_sticky" not in joined,
          f"one the timeline shows does not come twice ({list(joined)})")
    batch4, joined = sync(c, since=batch3, limit=10)
    check(not joined, f"nothing again on the next sync ({list(joined)})")


def sliding_part(hs, a):
    erin, e = hs.register("erin")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, e)
    assert st == 200, r

    def send(body, sticky_ms=None):
        path = f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{uuid.uuid4().hex}"
        if sticky_ms:
            path += f"?org.matrix.msc4354.sticky_duration_ms={sticky_ms}"
        st, r = hs.req("PUT", path, {"msgtype": "m.text", "body": body}, a)
        assert st == 200, (st, r)
        return r["event_id"]

    def ss(conn, pos=None, limit=100, name="org.matrix.msc4354.sticky_events", timeout=0):
        body = {"conn_id": conn, "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 1, "required_state": []}},
                "extensions": {name: {"enabled": True, "limit": limit}}}
        if name.startswith("org."):
            body["extensions"][name]["since"] = None   # the older shape js-sdk sends
        path = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=%d" % timeout
        if pos:
            path += "&pos=" + pos
        t0 = time.time()
        st, r = hs.req("POST", path, body, e)
        assert st == 200, (st, r)
        got = r.get("extensions", {}).get(name, {}).get("rooms", {}).get(room, {}).get("events", [])
        return r["pos"], [ev["event_id"] for ev in got], time.time() - t0, r

    s1 = send("sticky one", 600000)
    s2 = send("sticky two", 600000)
    send("plain")
    pos, got, _, _ = ss("c1")
    check(got == [s1, s2], f"a new connection gets the unexpired sticky events, oldest first ({got})")
    again, got_again, _, _ = ss("c1", pos=None)
    s3 = send("sticky three", 600000)
    send("plain after")
    pos2, got, _, r = ss("c1", pos=again)
    check(got == [s3], f"the next response: the one sent since, not the earlier ones ({got})")
    pos2b, got_b, _, _ = ss("c1", pos=again)
    check(got_b == [s3], f"the same pos again: the same sticky events again ({got_b})")
    pos3, got, waited, r = ss("c1", pos=pos2, timeout=1500)
    check(got == [], f"nothing new: none ({got})")

    # limit per room: the oldest first, the rest right after.
    pos, got, _, _ = ss("c2", limit=2)
    check(got == [s1, s2], f"limit 2: the two oldest ({got})")
    pos, got, waited, _ = ss("c2", pos=pos, limit=2, timeout=5000)
    check(got == [s3] and waited < 2, f"the rest in the next response, without waiting ({got}, {waited:.1f}s)")

    # The stable name is answered under the stable name.
    pos, got, _, r = ss("c3", name="sticky_events")
    check(got == [s1, s2, s3] and "org.matrix.msc4354.sticky_events" not in r.get("extensions", {}),
          f"asked as sticky_events, answered as sticky_events ({got})")


def federation_part(hs, a, mock):
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, a)
    room = r["room_id"]

    def send(body, sticky_ms=None, query="org.matrix.msc4354.sticky_duration_ms"):
        path = f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{uuid.uuid4().hex}"
        if sticky_ms:
            path += f"?{query}={sticky_ms}"
        st, r = hs.req("PUT", path, {"msgtype": "m.text", "body": body}, a)
        assert st == 200, (st, r)
        return r["event_id"]

    first = send("sticky before the mock joins", 600000)
    second = send("stable sticky before the mock joins", 600000, query="sticky_duration_ms")
    plain = send("plain before the mock joins")
    gone = send("expires before the mock joins", 1000)
    time.sleep(1.2)

    def pdus_since(n):
        out = []
        for req in mock.requests(n):
            out.extend(req["body"].get("pdus", []))
        return out

    mark = mock.count()
    mock.join(hs.fed, room, "joiner")
    deadline = time.time() + 10
    pushed = {}
    while time.time() < deadline:
        pushed = {}
        for p in pdus_since(mark):
            # The event ID of a v11 PDU is its reference hash; match by body instead.
            pushed[p.get("content", {}).get("body")] = p
        if "sticky before the mock joins" in pushed and "stable sticky before the mock joins" in pushed:
            break
        time.sleep(0.2)
    a1 = pushed.get("sticky before the mock joins", {})
    a2 = pushed.get("stable sticky before the mock joins", {})
    check(a1.get("msc4354_sticky") == {"duration_ms": 600000} and a2.get("sticky") == {"duration_ms": 600000},
          f"the joining server is sent our sticky events, with their markers ({sorted(pushed)})")
    check("plain before the mock joins" not in pushed and "expires before the mock joins" not in pushed,
          f"not the plain message, not the expired sticky one ({sorted(pushed)})")
    order = [p.get("content", {}).get("body") for p in pdus_since(mark)]
    check(order.index("sticky before the mock joins") < order.index("stable sticky before the mock joins"),
          f"oldest first ({order})")

    mark = mock.count()
    mock.join(hs.fed, room, "second")
    time.sleep(1.5)
    again = [p.get("content", {}).get("body") for p in pdus_since(mark)]
    check("sticky before the mock joins" not in again,
          f"a second user of that server joining does not bring them again ({again})")


def soft_fail_part(hs, a, mock):
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, a)
    room_id = r["room_id"]
    room = f"/_matrix/client/v3/rooms/{q(room_id)}"
    frank = mock.join(hs.fed, room_id, "frank")
    st, state = hs.req("GET", f"{room}/state", None, a)
    ids = {(e["type"], e["state_key"]): e["event_id"] for e in state}
    power = next(e for e in state if e["type"] == "m.room.power_levels")
    create, old_power, joined = ids[("m.room.create", "")], power["event_id"], ids[("m.room.member", frank)]
    st, r = hs.req("PUT", f"{room}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "before"}, a)
    past = r["event_id"]

    def set_message_level(level):
        content = dict(power["content"])
        content["events"] = dict(content.get("events", {}), **{"m.room.message": level})
        st, r = hs.req("PUT", f"{room}/state/m.room.power_levels/", content, a)
        assert st == 200, (st, r)

    def sync(since=None):
        st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0" + (f"&since={since}" if since else ""), None, a)
        assert st == 200, r
        return r["next_batch"], r.get("rooms", {}).get("join", {}).get(room_id, {})

    since, _ = sync()
    # Messages now need power level 50; Frank's server sends one built on the room as it was.
    set_message_level(50)
    now = int(time.time() * 1000)
    ev = {"type": "m.room.message", "room_id": room_id, "sender": frank,
          "content": {"msgtype": "m.text", "body": "frank's sticky one"},
          "msc4354_sticky": {"duration_ms": 600000},
          "auth_events": [create, old_power, joined], "prev_events": [past], "depth": 20, "origin_server_ts": now}
    event_id, ev = signed_event(mock.name, mock.key, "ed25519:k1", ev)
    st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}",
                     {"origin": mock.name, "origin_server_ts": now, "pdus": [ev], "edus": []})
    assert st == 200, (st, r)
    time.sleep(0.5)
    stored = query(f"SELECT soft_failed FROM events WHERE event_id = '{event_id}'")
    since, got = sync(since)
    shown = [e["event_id"] for e in got.get("timeline", {}).get("events", []) + got.get("msc4354_sticky", {}).get("events", [])]
    check(stored == [["t"]] and event_id not in shown, f"it is soft-failed and not shown ({stored}, {shown})")

    # The level goes back to 0: now it passes, and is accepted after all.
    set_message_level(0)
    deadline = time.time() + 5
    stored = None
    while time.time() < deadline:
        stored = query(f"SELECT soft_failed FROM events WHERE event_id = '{event_id}'")
        if stored == [["f"]]:
            break
        time.sleep(0.2)
    since, got = sync(since)
    shown = {e["event_id"]: e for e in got.get("timeline", {}).get("events", []) + got.get("msc4354_sticky", {}).get("events", [])}
    left = shown.get(event_id, {}).get("unsigned", {}).get("msc4354_sticky_duration_ttl_ms", 0)
    check(stored == [["f"]] and event_id in shown and left > 0,
          f"after the state change it is accepted and reaches clients, still sticky ({stored}, {list(shown)}, {left})")


def a_user(hs, token):
    st, r = hs.req("GET", "/_matrix/client/v3/account/whoami", None, token)
    return r["user_id"]


if __name__ == "__main__":
    run_lab(["mock"], run)
