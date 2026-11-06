#!/usr/bin/env python3
"""To-device messages all arrive, however many are waiting.

A device that was away comes back to a pile of to-device messages: room keys, key requests,
verification steps. An answer carries a page of them, and the position the client gets back
must not run ahead of what it was sent, because the next sync deletes everything before its
position.

Alice sends Bob's two devices 250 messages each while they are away. One device then reads
them over classic sync, the other over sliding sync. Each has to get all 250, once, in order.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/todevicetest.py [--keep]
"""
import urllib.parse, uuid

from fedlab import check, run_lab

WAITING = 250
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"


def run(hs, peers):
    q = urllib.parse.quote
    alice, a = hs.register("alice")
    bob, classic_device = hs.register("bob")
    st, r = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": bob}, "password": "pwpwpwpw"})
    assert st == 200, r
    sliding_device = r["access_token"]

    # Both devices have synced before, so that the messages are news to them.
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, classic_device)
    assert st == 200, r
    since = r["next_batch"]
    body = {"conn_id": "td", "lists": {}, "extensions": {"to_device": {"enabled": True}}}
    st, r = hs.req("POST", f"{SLIDING}?timeout=0", body, sliding_device)
    assert st == 200, r
    pos, td_since = r["pos"], r.get("extensions", {}).get("to_device", {}).get("next_batch")

    for n in range(WAITING):
        st, r = hs.req("PUT", f"/_matrix/client/v3/sendToDevice/m.test.marker/{uuid.uuid4().hex}", {"messages": {bob: {"*": {"n": n}}}}, a)
        assert st == 200, r

    print("-- classic sync")
    got, answers = [], 0
    for _ in range(20):
        st, r = hs.req("GET", f"/_matrix/client/v3/sync?timeout=0&since={q(since)}", None, classic_device)
        assert st == 200, r
        since = r["next_batch"]
        page = [e["content"]["n"] for e in r.get("to_device", {}).get("events", []) if e["type"] == "m.test.marker"]
        if not page:
            break
        got += page
        answers += 1
    check(got == list(range(WAITING)), f"all {WAITING} messages arrive, once and in order ({len(got)} in {answers} answers, {len(set(got))} different)")
    check(answers > 1, f"over more than one answer ({answers})")

    print("-- sliding sync")
    got, answers = [], 0
    for _ in range(20):
        body["extensions"]["to_device"]["since"] = td_since
        st, r = hs.req("POST", f"{SLIDING}?timeout=0&pos={pos}", body, sliding_device)
        assert st == 200, r
        pos = r["pos"]
        ext = r.get("extensions", {}).get("to_device", {})
        td_since = ext.get("next_batch", td_since)
        page = [e["content"]["n"] for e in ext.get("events", []) if e["type"] == "m.test.marker"]
        if not page:
            break
        got += page
        answers += 1
    check(got == list(range(WAITING)), f"all {WAITING} messages arrive, once and in order ({len(got)} in {answers} answers, {len(set(got))} different)")


if __name__ == "__main__":
    run_lab(["mock"], run)
