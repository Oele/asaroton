#!/usr/bin/env python3
"""What Matrix v1.20 asks of a server beyond sticky and delayed events (they have their own tests).

- `m.preview_url` capability (MSC4452): `enabled` true by default. With `[media] url_previews =
  false` it says false, and `/media/preview_url` (v1 and the legacy v3) answers 403
  `M_FORBIDDEN`.
- `/relations` with `dir=f` and no `from` starts at the oldest relation.
- `unsigned.prev_sender` (with `prev_content`) and `replaces_state` (v1.19) on a state event that replaced another, also for a
  member who could not see the event it replaced (history visibility `joined`).
- A PDU with a negative `depth` is refused over federation.

Needs what scripts/fedlab.py needs. Takes about ten seconds.

Usage: scripts/v120test.py [--keep]
"""
import json, time, urllib.parse, uuid

from fedlab import check, run_lab, signed_event

q = urllib.parse.quote


def run(hs, peers):
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")

    st, r = hs.req("GET", "/_matrix/client/v3/capabilities", None, a)
    check(r.get("capabilities", {}).get("m.preview_url") == {"enabled": True},
          f"m.preview_url is enabled by default ({r.get('capabilities', {}).get('m.preview_url')})")

    # /relations, dir=f without from: the oldest first.
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {
        "preset": "public_chat",
        "initial_state": [{"type": "m.room.history_visibility", "state_key": "", "content": {"history_visibility": "joined"}}],
        "power_level_content_override": {"events": {"m.room.topic": 0}},
    }, a)
    room = r["room_id"]
    path = f"/_matrix/client/v3/rooms/{q(room)}"
    for token in (b,):
        st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, token)
        assert st == 200, r

    def send(token, content, event_type="m.room.message"):
        st, r = hs.req("PUT", f"{path}/send/{event_type}/{uuid.uuid4().hex}", content, token)
        assert st == 200, (st, r)
        return r["event_id"]

    root = send(a, {"msgtype": "m.text", "body": "root"})
    replies = [send(b, {"msgtype": "m.text", "body": f"in thread {i}",
                        "m.relates_to": {"rel_type": "m.thread", "event_id": root}}) for i in range(4)]
    st, r = hs.req("GET", f"/_matrix/client/v1/rooms/{q(room)}/relations/{q(root)}?dir=f&limit=2", None, a)
    got = [e["event_id"] for e in r.get("chunk", [])]
    check(st == 200 and got == replies[:2], f"/relations dir=f without from: the two oldest ({got})")
    st, r = hs.req("GET", f"/_matrix/client/v1/rooms/{q(room)}/relations/{q(root)}?dir=b&limit=2", None, a)
    got = [e["event_id"] for e in r.get("chunk", [])]
    check(st == 200 and got == replies[:1:-1], f"/relations dir=b: the two newest, newest first ({got})")

    # prev_sender, also for someone who joined after the event it replaced.
    st, r = hs.req("PUT", f"{path}/state/m.room.topic/", {"topic": "alice's topic"}, a)
    assert st == 200, r
    alices_topic = r["event_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, c)
    assert st == 200, r
    st, r = hs.req("PUT", f"{path}/state/m.room.topic/", {"topic": "bob's topic"}, b)
    assert st == 200, r
    bobs_topic = r["event_id"]
    for who, token in (("alice", a), ("carol, who joined after alice's topic", c)):
        st, r = hs.req("GET", f"{path}/messages?dir=b&limit=10", None, token)
        ev = next((e for e in r.get("chunk", []) if e["event_id"] == bobs_topic), {})
        u = ev.get("unsigned", {})
        check(u.get("prev_sender") == alice and u.get("prev_content", {}).get("topic") == "alice's topic",
              f"/messages for {who}: unsigned.prev_sender and prev_content ({u})")
        check(u.get("replaces_state") == alices_topic, f"/messages for {who}: unsigned.replaces_state (v1.19) ({u.get('replaces_state')})")
    # The same in /sync: with Alice's topic in the window (the one seen before it), and with
    # only Bob's (the state before the window).
    for limit, how in ((10, "both topics in the timeline"), (1, "only bob's topic in the timeline")):
        flt = q(json.dumps({"room": {"timeline": {"limit": limit}, "rooms": [room]}}))
        st, r = hs.req("GET", f"/_matrix/client/v3/sync?timeout=0&filter={flt}", None, a)
        tl = r.get("rooms", {}).get("join", {}).get(room, {}).get("timeline", {}).get("events", [])
        ev = next((e for e in tl if e["event_id"] == bobs_topic), {})
        check(ev.get("unsigned", {}).get("replaces_state") == alices_topic,
              f"/sync, {how}: unsigned.replaces_state ({ev.get('unsigned', {}).get('replaces_state')})")

    # A PDU with a negative depth is refused.
    mock = peers["mock"]
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, a)
    fed_room = r["room_id"]
    dan = mock.join(hs.fed, fed_room, "dan")
    st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{q(fed_room)}/state", None, a)
    ids = {(e["type"], e["state_key"]): e["event_id"] for e in state}
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(fed_room)}/messages?dir=b&limit=1", None, a)
    newest = r["chunk"][0]["event_id"]
    now = int(time.time() * 1000)
    ev = {"type": "m.room.message", "room_id": fed_room, "sender": dan,
          "content": {"msgtype": "m.text", "body": "below zero"},
          "auth_events": [ids[("m.room.create", "")], ids[("m.room.power_levels", "")], ids[("m.room.member", dan)]],
          "prev_events": [newest], "depth": -1, "origin_server_ts": now}
    event_id, ev = signed_event(mock.name, mock.key, "ed25519:k1", ev)
    st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{uuid.uuid4().hex}",
                     {"origin": mock.name, "origin_server_ts": now, "pdus": [ev], "edus": []})
    # Refused while it is parsed, before it has an event ID, so the answer lists nothing for it.
    st2, r2 = hs.req("GET", f"/_matrix/client/v3/rooms/{q(fed_room)}/event/{q(event_id)}", None, a)
    refused = bool(hs.log_lines("`depth` is negative"))
    check(st == 200 and st2 == 404 and refused,
          f"a PDU with depth -1 is refused and not stored ({st}, /event {st2}, logged {refused})")

    # Previews switched off.
    hs.stop()
    hs.start(federation_extra="[media]\nurl_previews = false")
    st, r = hs.req("GET", "/_matrix/client/v3/capabilities", None, a)
    check(r.get("capabilities", {}).get("m.preview_url") == {"enabled": False},
          f"url_previews = false: the capability says so ({r.get('capabilities', {}).get('m.preview_url')})")
    for endpoint in ("/_matrix/client/v1/media/preview_url", "/_matrix/media/v3/preview_url"):
        st, r = hs.req("GET", f"{endpoint}?url={q('https://example.org/')}", None, a)
        check(st == 403 and r.get("errcode") == "M_FORBIDDEN", f"{endpoint}: 403 M_FORBIDDEN ({st}, {r})")


if __name__ == "__main__":
    run_lab(["mock"], run)
