#!/usr/bin/env python3
"""Regression test: the limits on delayed events (MSC4140) and how they are reported.

    scripts/delayedeventstest.py http://127.0.0.1:18099      (a test server with open registration)

Clients read the limits from the capabilities and treat a missing capability as "no delayed
events here"; Element Call then joins without the delayed leave that cleans up after a client
that drops off. A delay over the maximum has its own error code, and scheduling more events
than allowed is a rate-limit error that says when room frees up.
"""
import json, secrets, sys, urllib.error, urllib.parse, urllib.request

HS = sys.argv[1].rstrip("/")
BASE = "/_matrix/client/unstable/org.matrix.msc4140"
failures = []


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.load(r), r.headers
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}"), e.headers


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what)
    if not ok:
        failures.append(what)


def main():
    st, r, _ = call("POST", "/_matrix/client/v3/register", {"username": f"delay{secrets.token_hex(4)}", "password": secrets.token_urlsafe(16), "auth": {"type": "m.login.dummy"}})
    token = r["access_token"]
    st, r, _ = call("POST", "/_matrix/client/v3/createRoom", {}, token)
    room = urllib.parse.quote(r["room_id"])

    st, r, _ = call("GET", "/_matrix/client/v3/capabilities", None, token)
    caps = r.get("capabilities", {})
    stable, unstable = caps.get("m.delayed_events"), caps.get("org.matrix.msc4140.delayed_events")
    check(isinstance(stable, dict) and stable == unstable and stable.get("max_delay_ms", 0) > 0 and stable.get("max_scheduled", 0) > 0, f"the limits are advertised under both capability names ({stable})")
    max_delay, max_scheduled = stable["max_delay_ms"], stable["max_scheduled"]

    def schedule(delay, n):
        return call("PUT", f"{BASE}/rooms/{room}/delayed_event/m.room.message/t{n}", {"delay_ms": delay, "content": {"msgtype": "m.text", "body": f"later {n}"}}, token)

    st, r, _ = schedule(max_delay + 1, "big")
    check(st == 400 and r.get("errcode") == "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE", f"a delay over the maximum is refused with the proposal's error code ({st} {r.get('errcode')})")
    st, r, _ = schedule(max_delay, "max")
    check(st == 200 and r.get("delay_id"), f"the maximum itself is accepted ({st})")

    accepted = 1
    for n in range(max_scheduled + 2):
        st, r, headers = schedule(60_000 + n, n)
        if st != 200:
            break
        accepted += 1
    check(accepted == max_scheduled, f"{max_scheduled} events can be scheduled at once ({accepted} were)")
    retry = headers.get("Retry-After", "")
    check(st == 429 and r.get("errcode") == "M_LIMIT_EXCEEDED" and retry.isdigit() and 1 <= int(retry) <= 60, f"one more is a rate-limit error that says when to come back ({st} {r.get('errcode')} Retry-After: {retry or 'absent'})")

    # Cancelling one makes room again.
    st, listed, _ = call("GET", f"{BASE}/delayed_events", None, token)
    events = listed.get("scheduled") or listed.get("delayed_events") or []
    if events:
        call("POST", f"{BASE}/delayed_events/{events[0]['delay_id']}/cancel", {}, token)
        st, r, _ = schedule(60_000, "again")
        check(st == 200, f"after cancelling one, another can be scheduled ({st})")
    else:
        check(False, f"the scheduled events can be listed ({sorted(listed)})")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
