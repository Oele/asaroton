#!/usr/bin/env python3
"""The federation sender, end to end, against mock destination servers.

Starts a throwaway asaroton with its own database and plain-HTTP federation to a set of mock
servers, and checks what those servers receive:

  * a retry carries the same transaction ID and the same contents, also when more was queued
    in the meantime and also across a restart;
  * rate limits, lost answers, 401, 404 and 5xx keep the batch; 400 and 403 discard it;
  * a destination's Retry-After is honoured, and `{}` counts as an acceptance;
  * a batch the destination calls too large is sent again in smaller parts;
  * transactions with only EDUs are coalesced, spaced and held under the ceiling, while
    events and to-device messages go out at once;
  * passing the ceiling is logged as an error.

Needs what scripts/fedlab.py needs. Takes about a minute.

Usage: scripts/fedsendertest.py [--keep]     (--keep leaves the work directory and database)
"""
import threading, time, uuid

from fedlab import check, run_lab, wait_for

def markers(req):
    """The test markers in a received transaction's to-device EDUs."""
    out = []
    for edu in req["body"].get("edus", []):
        if edu.get("edu_type") == "m.direct_to_device":
            for devices in edu["content"]["messages"].values():
                out += [c.get("marker") for c in devices.values()]
    return out


def edu_only(req):
    return not req["body"].get("pdus") and bool(req["body"].get("edus"))


def typing_states(reqs):
    return [e["content"]["typing"] for r in reqs for e in r["body"].get("edus", []) if e.get("edu_type") == "m.typing"]


def typing_storm(hs, token, room_id, user_id, seconds, every):
    """Flip the typing state back and forth, ending on 'not typing'. Returns the flip count."""
    flips, end = 0, time.time() + seconds
    while time.time() < end:
        hs.typing(token, room_id, user_id, flips % 2 == 0)
        flips += 1
        time.sleep(every)
    if flips % 2 == 1:
        hs.typing(token, room_id, user_id, False)
        flips += 1
    return flips


def same_transaction(reqs, what):
    check(len({r["txn"] for r in reqs}) == 1, f"{what}: every attempt carries the same transaction ID")
    check(len({r["raw"] for r in reqs}) == 1, f"{what}: every attempt carries the same body")



def main():
    labels = ["limited", "busy", "silent", "nokey", "notfound", "denied", "malformed", "small", "queued", "restart", "room"]
    run_lab(labels, run)


def run(hs, peers):
    alice, token = hs.register("alice")
    p = peers
    limit = {"errcode": "M_LIMIT_EXCEEDED", "error": "Too Many Requests"}

    # ---- answers that pass keep the batch; answers that cannot change discard it
    p["limited"].script += [(429, limit, {"Retry-After": "3"})]
    p["busy"].script += [(503, b"<html>\n<body>\n  busy\n</body>\n</html>", None, "text/html"), (200, {})]
    p["silent"].script += ["drop"]
    p["nokey"].script += [(401, {"errcode": "M_UNAUTHORIZED", "error": "Unable to acquire your signing key"})]
    p["notfound"].script += [(404, b"<html>\n  <body>Not Found</body>\n</html>\n", None, "text/html")]
    p["denied"].script += [(403, {"errcode": "M_FORBIDDEN", "error": "Federation denied with sender.test"})]
    p["malformed"].script += [(400, {"errcode": "M_BAD_JSON", "error": "mock"})]
    p["small"].script += [(403, {"errcode": "M_FORBIDDEN", "error": "mock"}), (413, b"too large", None, "text/plain")]
    p["queued"].script += [(503, {"errcode": "M_UNKNOWN", "error": "mock"})]
    for label in ["limited", "busy", "silent", "nokey", "notfound", "denied", "malformed", "small", "queued"]:
        hs.to_device(token, p[label], f"{label}-1")

    # While "queued" backs off, more arrives for it: the retry must not change.
    wait_for(lambda: p["queued"].count() >= 1, 5)
    hs.to_device(token, p["queued"], "queued-2")
    # While "small" backs off after its discard, two more arrive and make one batch.
    wait_for(lambda: p["small"].count() >= 1, 5)
    hs.to_device(token, p["small"], "small-2")
    hs.to_device(token, p["small"], "small-3")

    # First retries come after 2 s (3 s where the destination asked), a wrongly scheduled
    # second retry would come 4 s after that.
    time.sleep(8.5)

    r = p["limited"].requests()
    check(len(r) == 2, f"429: retried once and then delivered ({len(r)} requests)")
    same_transaction(r, "429")
    if len(r) >= 2:
        gap = r[1]["t"] - r[0]["t"]
        check(2.9 <= gap < 4.5, f"429: Retry-After of 3 s honoured over the 2 s default ({gap:.1f} s)")
    hs.to_device(token, p["limited"], "limited-2")
    wait_for(lambda: p["limited"].count() >= 3, 5)
    r = p["limited"].requests()
    check(len(r) == 3 and markers(r[2]) == ["limited-2"] and r[2]["txn"] != r[0]["txn"], "429: the next message is a new transaction with only that message")

    r = p["busy"].requests()
    check(len(r) == 2, f"503 then 200 with an empty object: accepted, not sent a third time ({len(r)} requests)")
    same_transaction(r, "503")

    r = p["silent"].requests()
    check(len(r) == 2, f"no answer: retried once and then delivered ({len(r)} requests)")
    same_transaction(r, "no answer")

    r = p["nokey"].requests()
    check(len(r) == 2 and markers(r[-1]) == ["nokey-1"], f"401: kept and delivered on retry ({len(r)} requests)")
    same_transaction(r, "401")

    r = p["notfound"].requests()
    check(len(r) == 2 and markers(r[-1]) == ["notfound-1"], f"404: kept and delivered on retry ({len(r)} requests)")
    same_transaction(r, "404")
    lines = hs.log_lines(f"transaction failed destination={p['notfound'].name}")
    check(len(lines) == 1 and "answered 404 Not Found: <html> <body>Not Found</body> </html>" in lines[0] and 'action="retry"' in lines[0], "404: the answer is logged on one line, as a retry")

    for label, status in [("denied", 403), ("malformed", 400)]:
        r = p[label].requests()
        check(len(r) == 1, f"{status}: not retried ({len(r)} requests)")
        hs.to_device(token, p[label], f"{label}-2")
        wait_for(lambda: p[label].count() >= 2, 5)
        r = p[label].requests()
        check(len(r) == 2 and markers(r[1]) == [f"{label}-2"] and r[1]["txn"] != r[0]["txn"], f"{status}: the batch was discarded; the next message arrives alone")
        lines = hs.log_lines(f"transaction failed destination={p[label].name}")
        check(len(lines) == 1 and 'action="discard"' in lines[0], f"{status}: logged as a discard")

    r = p["small"].requests()
    got = [markers(x) for x in r]
    check(got == [["small-1"], ["small-2", "small-3"], ["small-2"], ["small-3"]], f"413: the batch is sent again in smaller parts ({got})")
    check(len({x["txn"] for x in r}) == len(r), "413: each part is a transaction of its own")

    r = p["queued"].requests()
    got = [markers(x) for x in r]
    check(got == [["queued-1"], ["queued-1"], ["queued-2"]], f"a retry is not changed by what was queued meanwhile ({got})")
    same_transaction(r[:2], "queued meanwhile")

    # ---- a restart between attempts keeps the transaction ID
    p["restart"].script += [(503, {"errcode": "M_UNKNOWN", "error": "mock"})]
    hs.to_device(token, p["restart"], "restart-1")
    wait_for(lambda: p["restart"].count() >= 1, 5)
    hs.stop()
    hs.start()
    wait_for(lambda: p["restart"].count() >= 2, 10)
    r = p["restart"].requests()
    check(len(r) == 2 and r[0]["txn"] == r[1]["txn"] and markers(r[1]) == ["restart-1"], f"restart: the retry after a restart carries the same transaction ID ({len(r)} requests)")
    time.sleep(1)

    # ---- pacing, with a mock server that is a member of a room
    room = p["room"]
    st, created = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "room_version": "11"}, token)
    assert st == 200, created
    room_id = created["room_id"]
    remote_user = room.join(hs.fed, room_id, "bob")
    time.sleep(2.5)  # let the traffic of the join itself pass

    start = room.count()
    flips = typing_storm(hs, token, room_id, alice, 2.0, 0.1)
    time.sleep(3)
    r = room.requests(start)
    only = [x for x in r if edu_only(x)]
    check(1 <= len(only) <= 4, f"{flips} typing changes in 2 s reach the destination in {len(only)} transactions")
    states = typing_states(r)
    check(bool(states) and states[-1] is False, "the last typing state the destination hears is the final one")

    # Events and to-device messages are not held back, even in the middle of a burst.
    start = room.count()
    storm = threading.Thread(target=typing_storm, args=(hs, token, room_id, alice, 3.0, 0.1))
    storm.start()
    time.sleep(1.45)
    sent_event = hs.message(token, room_id, "prompt")
    time.sleep(0.6)
    sent_device = time.time()
    st, _ = hs.req("PUT", f"/_matrix/client/v3/sendToDevice/m.test.marker/{uuid.uuid4().hex}", {"messages": {remote_user: {"*": {"marker": "prompt"}}}}, token)
    assert st == 200
    storm.join()
    time.sleep(2.5)
    r = room.requests(start)
    with_event = [x for x in r if any(pdu.get("content", {}).get("body") == "prompt" for pdu in x["body"].get("pdus", []))]
    with_device = [x for x in r if "prompt" in markers(x)]
    check(len(with_event) == 1 and with_event[0]["t"] - sent_event < 0.5, "an event goes out at once during a typing burst" + (f" ({with_event[0]['t'] - sent_event:.2f} s)" if with_event else ""))
    check(len(with_device) == 1 and with_device[0]["t"] - sent_device < 0.5, "a to-device message goes out at once during a typing burst" + (f" ({with_device[0]['t'] - sent_device:.2f} s)" if with_device else ""))
    check(not hs.log_lines("outbound federation is unusually busy"), "no alarm under the default ceiling")

    # ---- the ceiling over all destinations: 6 a minute means 3 at once, then one per 10 s
    hs.stop()
    hs.start("max_edu_transactions_per_minute = 6")
    time.sleep(1)
    start = room.count()
    flips = typing_storm(hs, token, room_id, alice, 12.0, 0.2)
    time.sleep(1.5)
    only = [x for x in room.requests(start) if edu_only(x)]
    check(3 <= len(only) <= 5, f"under a ceiling of 6 a minute, {flips} typing changes in 12 s make {len(only)} transactions")
    start = room.count()
    sent = [hs.message(token, room_id, f"ceiling {i}") for i in range(4)]
    wait_for(lambda: sum(len(x["body"].get("pdus", [])) for x in room.requests(start)) >= 4, 5)
    r = room.requests(start)
    delivered = sum(len(x["body"].get("pdus", [])) for x in r)
    check(delivered == 4 and r[-1]["t"] - sent[0] < 2, "events are not subject to the ceiling")
    alarms = hs.log_lines("outbound federation is unusually busy")
    check(len(alarms) == 1 and " ERROR " in alarms[0], f"passing the ceiling is logged as an error, once ({len(alarms)} lines)")


if __name__ == "__main__":
    main()
