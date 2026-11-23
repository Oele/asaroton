#!/usr/bin/env python3
"""Delayed events through the spec's stable API (MSC4140 merged, v1.20; tracker #87).

- `PUT /v3/rooms/{roomId}/delayed_event/{eventType}/{txnId}` schedules (a repeated transaction ID
  gets the same delay ID); a delay over the maximum is `M_DELAY_TOO_LARGE` (the unstable path
  keeps its own code).
- `GET /v1/delayed_events/{delayId}` shows one of the user's events, pending or, after it was
  sent or cancelled, with `finalised` (the event it became, or the error that cancelled it);
  another user gets a 404.
- `POST /v1/delayed_events/{delayId}/restart|send|cancel`: sending a sent one or cancelling a
  cancelled one succeeds, the opposite or a restart is a 409; a send that fails answers with
  the error and leaves the event scheduled.
- The worker sends a due state event, and cancels one it cannot send, storing the error.
- An event sent from a delayed event carries `unsigned.delay_id` for its sender (in `/messages`,
  `/sync` and sliding sync, on every device), no `transaction_id`, and nothing for other users.
- The unstable API is unchanged: an event that was sent or cancelled is gone there (404), and
  its list shows only pending events.

Needs what scripts/fedlab.py needs. Takes about fifteen seconds.

Usage: scripts/delayedstabletest.py [--keep]
"""
import json, time, urllib.parse, uuid

from fedlab import check, run_lab

q = urllib.parse.quote
UNSTABLE = "/_matrix/client/unstable/org.matrix.msc4140"


def run(hs, peers):
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r

    def schedule(token, event_type, body, txn=None):
        return hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/delayed_event/{q(event_type)}/{txn or uuid.uuid4().hex}", body, token)

    def get(token, delay_id):
        return hs.req("GET", f"/_matrix/client/v1/delayed_events/{q(delay_id)}", None, token)

    def act(token, delay_id, action):
        return hs.req("POST", f"/_matrix/client/v1/delayed_events/{q(delay_id)}/{action}", {}, token)

    def messages(token):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/messages?dir=b&limit=50", None, token)
        return {e["event_id"]: e for e in r.get("chunk", [])}

    # Schedule, idempotent by transaction ID; read it back.
    st, r = schedule(a, "m.room.message", {"delay_ms": 60000, "content": {"msgtype": "m.text", "body": "later, maybe"}}, txn="t1")
    d1 = r.get("delay_id")
    check(st == 200 and d1, f"the stable PUT schedules a delayed event ({st}, {r})")
    st, r = schedule(a, "m.room.message", {"delay_ms": 60000, "content": {"msgtype": "m.text", "body": "later, maybe"}}, txn="t1")
    check(st == 200 and r.get("delay_id") == d1, "the same transaction ID again: the same delay ID")
    st, v = get(a, d1)
    check(st == 200 and v.get("delay_id") == d1 and v.get("room_id") == room and v.get("type") == "m.room.message"
          and v.get("delay_ms") == 60000 and isinstance(v.get("delayed_since_ts"), int)
          and v.get("content", {}).get("body") == "later, maybe" and "finalised" not in v and "state_key" not in v,
          f"GET shows it pending ({st}, {sorted(v)})")
    st, r = get(b, d1)
    check(st == 404, f"another user cannot see it ({st})")
    since = v["delayed_since_ts"]
    time.sleep(0.05)
    st, r = act(a, d1, "restart")
    st2, v = get(a, d1)
    check(st == 200 and v.get("delayed_since_ts", 0) > since, f"restart moves delayed_since_ts on ({st}, {since} -> {v.get('delayed_since_ts')})")

    # Send now: finalised with the event; the event carries delay_id for alice only.
    st, r = act(a, d1, "send")
    st2, v = get(a, d1)
    fin = v.get("finalised", {})
    e1 = fin.get("event_id")
    check(st == 200 and e1 and isinstance(fin.get("finalised_ts"), int) and "error" not in fin,
          f"send: finalised with the event it became ({st}, {fin})")
    ev = messages(a).get(e1, {})
    check(ev.get("unsigned", {}).get("delay_id") == d1 and "transaction_id" not in ev.get("unsigned", {}),
          f"/messages for alice: unsigned.delay_id and no transaction_id ({ev.get('unsigned')})")
    ev = messages(b).get(e1, {})
    check(ev and "delay_id" not in ev.get("unsigned", {}), f"/messages for bob: no delay_id ({ev.get('unsigned')})")
    flt = q(json.dumps({"room": {"timeline": {"limit": 50}}}))
    st, r = hs.req("GET", f"/_matrix/client/v3/sync?filter={flt}", None, a)
    tl = r.get("rooms", {}).get("join", {}).get(room, {}).get("timeline", {}).get("events", [])
    ev = next((e for e in tl if e["event_id"] == e1), {})
    check(ev.get("unsigned", {}).get("delay_id") == d1, f"/sync for alice: unsigned.delay_id ({ev.get('unsigned')})")
    st, r = hs.req("POST", "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0",
                   {"room_subscriptions": {room: {"timeline_limit": 50, "required_state": []}}}, a)
    tl = r.get("rooms", {}).get(room, {}).get("timeline", [])
    ev = next((e for e in tl if e["event_id"] == e1), {})
    check(ev.get("unsigned", {}).get("delay_id") == d1, f"sliding sync for alice: unsigned.delay_id ({ev.get('unsigned')})")
    st, r = act(a, d1, "send")
    check(st == 200, f"sending it again: 200, it was sent ({st})")
    st, r = act(a, d1, "cancel")
    check(st == 409, f"cancelling a sent one: 409 ({st}, {r.get('errcode')})")
    st, r = act(a, d1, "restart")
    check(st == 409, f"restarting a sent one: 409 ({st}, {r.get('errcode')})")

    # Cancel: finalised without an event.
    st, r = schedule(a, "m.room.message", {"delay_ms": 60000, "content": {"msgtype": "m.text", "body": "never"}})
    d2 = r["delay_id"]
    st, r = act(a, d2, "cancel")
    st2, v = get(a, d2)
    fin = v.get("finalised", {})
    check(st == 200 and isinstance(fin.get("finalised_ts"), int) and "event_id" not in fin and "error" not in fin,
          f"cancel: finalised without an event ({st}, {fin})")
    st, r = act(a, d2, "cancel")
    check(st == 200, f"cancelling it again: 200 ({st})")
    st, r = act(a, d2, "send")
    check(st == 409, f"sending a cancelled one: 409 ({st}, {r.get('errcode')})")
    st, r = hs.req("POST", f"{UNSTABLE}/delayed_events/{q(d2)}/cancel", {}, a)
    check(st == 404, f"the unstable API: a cancelled event is gone, as before ({st})")

    # A send that fails answers with the error and keeps the event scheduled.
    pl = {"users": {alice: 100, bob: 100}}
    st, r = schedule(b, "m.room.power_levels", {"delay_ms": 60000, "state_key": "", "content": pl})
    d5 = r.get("delay_id")
    st, r = act(b, d5, "send")
    st2, v = get(b, d5)
    check(st == 403 and r.get("errcode") == "M_FORBIDDEN" and st2 == 200 and "finalised" not in v,
          f"a send bob may not make: 403, and still scheduled ({st}, {r.get('errcode')}, {sorted(v)})")

    # The worker: a due state event is sent; one that cannot be sent is cancelled with the error.
    st, r = schedule(a, "m.room.topic", {"delay_ms": 1500, "state_key": "", "content": {"topic": "set later"}})
    d3 = r.get("delay_id")
    st, r = schedule(b, "m.room.power_levels", {"delay_ms": 1500, "state_key": "", "content": pl})
    d4 = r.get("delay_id")
    v3 = v4 = {}
    for _ in range(40):
        time.sleep(0.25)
        _, v3 = get(a, d3)
        _, v4 = get(b, d4)
        if "finalised" in v3 and "finalised" in v4:
            break
    e3 = v3.get("finalised", {}).get("event_id")
    st, topic = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/state/m.room.topic/", None, a)
    check(e3 and v3.get("state_key") == "" and topic.get("topic") == "set later",
          f"the worker sends a due state event ({v3.get('finalised')}, topic {topic.get('topic')!r})")
    ev = messages(a).get(e3, {})
    check(ev.get("unsigned", {}).get("delay_id") == d3, f"it carries delay_id for alice ({ev.get('unsigned')})")
    err = v4.get("finalised", {}).get("error", {})
    check("event_id" not in v4.get("finalised", {}) and err.get("errcode") == "M_FORBIDDEN",
          f"one bob may not send is cancelled with the error ({v4.get('finalised')})")

    # Errors and the unstable list.
    st, r = schedule(a, "m.room.message", {"delay_ms": 10 ** 12, "content": {}})
    check(st == 400 and r.get("errcode") == "M_DELAY_TOO_LARGE", f"too long a delay, stable: M_DELAY_TOO_LARGE ({st}, {r.get('errcode')})")
    st, r = hs.req("PUT", f"{UNSTABLE}/rooms/{q(room)}/delayed_event/m.room.message/{uuid.uuid4().hex}", {"delay_ms": 10 ** 12, "content": {}}, a)
    check(st == 400 and r.get("errcode") == "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE", f"unstable: its own code, as before ({st}, {r.get('errcode')})")
    st, r = get(a, "nonesuch")
    st2, r2 = act(a, "nonesuch", "send")
    check(st == 404 and st2 == 404, f"an unknown delay ID: 404 ({st}, {st2})")
    st, r = hs.req("GET", f"{UNSTABLE}/delayed_events", None, b)
    listed = {e["delay_id"] for e in r.get("delayed_events", [])}
    check(st == 200 and listed == {d5}, f"the unstable list shows only pending events ({len(listed)}: {'d5' if d5 in listed else ''})")


if __name__ == "__main__":
    run_lab(["mock"], run)
