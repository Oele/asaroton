#!/usr/bin/env python3
"""Regression test: a room subscribed again after a gap gets its timeline whole again (tracker #103).

    scripts/ssresubscribetest.py http://127.0.0.1:18099      (a test server with open registration)

On one sliding sync connection:

1. a list with `timeline_limit` 1 sends room R with its last message, `limited`;
2. a subscription to R with `timeline_limit` 20 sends R's latest events (`expanded_timeline`);
3. the subscription is dropped and two messages are sent: the list sends R with the last one only,
   `limited` (a gap: matrix-rust-sdk drops what it held of R before it);
4. R is subscribed again with `timeline_limit` 20: it must come at once, with its latest events,
   the one in the gap among them, and `expanded_timeline` (MSC4186, "Changes to timeline_limit":
   the server has not sent all of the latest 20 events).

asaroton remembered the longest timeline it ever sent for R and sent nothing in step 4. And the
other way round: dropping the subscription and taking it again with no gap in between sends no
old events again.
"""
import json
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HS = sys.argv[1].rstrip("/")
SYNC = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
failures = []


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(),
                                 method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: {e.code} {e.read().decode()[:300]}")


def main():
    r = call("POST", "/_matrix/client/v3/register",
             {"username": f"resub{secrets.token_hex(4)}", "password": secrets.token_urlsafe(16),
              "auth": {"type": "m.login.dummy"}})
    token = r["access_token"]
    room = call("POST", "/_matrix/client/v3/createRoom", {"name": "resubscribe"}, token)["room_id"]
    q = urllib.parse.quote(room, safe="")

    def say(body):
        return call("PUT", f"/_matrix/client/v3/rooms/{q}/send/m.room.message/{secrets.token_hex(8)}",
                    {"msgtype": "m.text", "body": body}, token)["event_id"]

    ids = {f"m{i}": say(f"m{i}") for i in range(10)}
    pos = None

    def sync(subscribe, timeout=0):
        nonlocal pos
        body = {"conn_id": "resub", "lists": {"all": {"ranges": [[0, 19]], "required_state": [["m.room.name", ""]],
                                                       "timeline_limit": 1}},
                "room_subscriptions": {room: {"required_state": [["m.room.name", ""]], "timeline_limit": 20}}
                if subscribe else {}}
        started = time.monotonic()
        r = call("POST", f"{SYNC}?timeout={timeout}" + (f"&pos={pos}" if pos else ""), body, token)
        pos = r["pos"]
        got = r.get("rooms", {}).get(room)
        bodies = [e["content"].get("body") for e in got.get("timeline", []) if e["type"] == "m.room.message"] if got else None
        return got, bodies, time.monotonic() - started

    got, bodies, _ = sync(False)
    print(f"1. list: {bodies} limited={got and got.get('limited')}")
    if bodies != ["m9"] or not got.get("limited"):
        failures.append(f"step 1: the list should send R as [m9], limited (got {bodies})")

    got, bodies, _ = sync(True)
    print(f"2. subscribed: {bodies} expanded={got and got.get('expanded_timeline')}")
    if bodies != [f"m{i}" for i in range(10)] or not got.get("expanded_timeline"):
        failures.append(f"step 2: the subscription should send m0..m9, expanded (got {bodies})")

    sync(False)
    say("m10")
    say("m11")
    got, bodies, _ = sync(False)
    print(f"3. unsubscribed, two new: {bodies} limited={got and got.get('limited')}")
    if bodies != ["m11"] or not got.get("limited"):
        failures.append(f"step 3: the list should send R as [m11], limited (got {bodies})")

    got, bodies, took = sync(True, timeout=2000)
    print(f"4. subscribed again: {took:.2f}s {bodies} expanded={got and got.get('expanded_timeline')} "
          f"limited={got and got.get('limited')} prev_batch={'yes' if got and got.get('prev_batch') else 'no'}")
    if bodies != [f"m{i}" for i in range(12)] or not got.get("expanded_timeline") or not got.get("prev_batch"):
        failures.append(f"step 4: the subscription should send m0..m11 at once, expanded, with a prev_batch (got {bodies})")
    if took > 1.5:
        failures.append(f"step 4: the expanded room should come at once, not after {took:.2f}s")

    # No gap: dropping the subscription and taking it again sends nothing old.
    sync(False)
    say("m12")
    got, bodies, _ = sync(False)
    print(f"5. unsubscribed, one new: {bodies} limited={got and got.get('limited')}")
    if bodies != ["m12"] or got.get("limited"):
        failures.append(f"step 5: the list should send R as [m12], not limited (got {bodies})")
    got, bodies, took = sync(True, timeout=1500)
    print(f"6. subscribed again, no gap: {took:.2f}s {bodies}")
    if got and bodies:
        failures.append(f"step 6: nothing was missed, yet R came again with {bodies}")

    if failures:
        for f in failures:
            print("FAIL:", f)
        sys.exit(1)
    print("ok")


if __name__ == "__main__":
    main()
