#!/usr/bin/env python3
"""A push worker that falls behind catches up from the database, and a burst of events does
not hold up the pushes of the events after it (tracker #78, #100).

The lab runs the server with a tiny notification channel (ASAROTON_NOTIFIER_CAPACITY=8), so a
burst makes the push worker lag on purpose. Bob has an HTTP pusher and is in many rooms (the
badge count of a push goes over all of them); Alice sends a burst of messages into Bob's rooms
and Carol one into rooms Bob is not in, from eight threads at once. Every message of the burst
in Bob's rooms must reach Bob's gateway, each once, and the server must say it caught up. Then
Alice sends one more message to Bob: it must reach the gateway within two seconds.

Under smaltino's soak the worker took about 45 ms per event, one at a time, and caught up inside
its only loop: a catch-up of 2,000 events held up every push for a minute and a half. Before the
catch-up ran in a task of its own, here the message after the burst came 4 s late. And the
worker went on from the highest position it had handled live, which could lie above events
whose notifications were lost later: a few of the burst were never pushed.

Needs what scripts/fedlab.py needs. Takes about half a minute.

Usage: scripts/pushcatchuptest.py [--keep]
"""
import json, os, re, threading, time, urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, free_port, run_lab, wait_for

os.environ["ASAROTON_NOTIFIER_CAPACITY"] = "8"
BOB_ROOMS = int(os.environ.get("BOB_ROOMS", "40"))
TO_BOB = int(os.environ.get("TO_BOB", "400"))
ELSEWHERE = int(os.environ.get("ELSEWHERE", "800"))
DEADLINE = float(os.environ.get("DEADLINE", "2"))
pushed = Counter()
arrived = {}
unread = {}
lock = threading.Lock()


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        event_id = body.get("notification", {}).get("event_id")
        if event_id:
            with lock:
                pushed[event_id] += 1
                arrived.setdefault(event_id, time.monotonic())
                unread[event_id] = body["notification"].get("counts", {}).get("unread")
        data = json.dumps({"rejected": []}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


PORT = free_port()
threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", PORT), Gateway).serve_forever, daemon=True).start()


def run(hs, peers):
    q = urllib.parse.quote
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {
        "app_id": "test.burst", "pushkey": "key-of-bob", "kind": "http", "app_display_name": "Test",
        "device_display_name": "Phone", "lang": "en",
        "data": {"url": f"http://127.0.0.1:{PORT}/_matrix/push/v1/notify"},
    }, b)
    assert st == 200, r

    def room_with_bob(i):
        st, r = hs.req("POST", "/_matrix/client/v3/createRoom",
                       {"preset": "private_chat", "invite": [bob], "name": f"with bob {i}"}, a)
        assert st == 200, r
        st, j = hs.req("POST", f"/_matrix/client/v3/join/{q(r['room_id'])}", {}, b)
        assert st == 200, j
        return r["room_id"]

    with ThreadPoolExecutor(8) as pool:
        bob_rooms = list(pool.map(room_with_bob, range(BOB_ROOMS)))
    elsewhere = [hs.req("POST", "/_matrix/client/v3/createRoom", {"name": f"carol {i}"}, c)[1]["room_id"] for i in range(4)]
    time.sleep(1)
    with lock:
        pushed.clear()
        arrived.clear()

    def send(token, room, i):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/m{i}",
                       {"msgtype": "m.text", "body": f"burst {i}"}, token)
        assert st == 200, r
        return r["event_id"]

    jobs = [(a, bob_rooms[i % BOB_ROOMS], i) for i in range(TO_BOB)] + \
           [(c, elsewhere[i % 4], TO_BOB + i) for i in range(ELSEWHERE)]
    jobs.sort(key=lambda j: j[2] % 7)
    started = time.monotonic()
    with ThreadPoolExecutor(8) as pool:
        sent = list(pool.map(lambda j: (j[0] == a, j[1], send(*j)), jobs))
    burst_took = time.monotonic() - started
    to_bob = [e for mine, _, e in sent if mine]

    # Bob reads the first of his rooms up to its newest message, so that the badge count of
    # the next push is the messages in the other rooms, and that one.
    read_room = bob_rooms[0]
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(read_room)}/messages?dir=b&limit=1", None, b)
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(read_room)}/receipt/m.read/{q(r['chunk'][0]['event_id'])}", {}, b)
    assert st == 200, r
    in_read_room = sum(1 for mine, room, _ in sent if mine and room == read_room)

    t0 = time.monotonic()
    probe = send(a, bob_rooms[0], "probe")
    wait_for(lambda: probe in arrived, 300, 0.05)
    probe_took = arrived[probe] - t0 if probe in arrived else None
    wait_for(lambda: all(e in pushed for e in to_bob), 300, 0.2)
    all_took = max((arrived[e] for e in to_bob if e in arrived), default=t0) - t0

    lines = hs.log_lines("catching up from the database")
    spans = []
    for line in lines:
        m = re.search(r"from=(\d+) upto=(\d+)", line)
        if m:
            spans.append(int(m.group(2)) - int(m.group(1)))
    print(f"burst: {len(jobs)} messages in {burst_took:.1f}s; {len(lines)} catch-ups over {sum(spans)} positions")
    print(f"probe pushed after {probe_took if probe_took is None else round(probe_took, 2)}s; "
          f"the burst's last push {all_took:.1f}s after the burst")
    check(probe_took is not None and probe_took < DEADLINE,
          f"the message after the burst reaches Bob's gateway within {DEADLINE:.0f}s ({probe_took})")
    check(all(e in pushed for e in to_bob),
          f"every message of the burst to Bob is pushed ({sum(1 for e in to_bob if e in pushed)} of {len(to_bob)})")
    check(all(pushed[e] == 1 for e in to_bob), f"each once ({max((pushed[e] for e in to_bob), default=0)} at most)")
    check(bool(lines), "the push worker did fall behind, and caught up")
    want = TO_BOB - in_read_room + 1
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, b)
    by_sync = sum(room.get("unread_notifications", {}).get("notification_count", 0)
                  for room in r.get("rooms", {}).get("join", {}).values())
    check(unread.get(probe) == want and by_sync == want,
          f"the badge count of the push after it is the unread messages in all of Bob's rooms "
          f"({unread.get(probe)}; sync says {by_sync}; {want} expected)")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra='disabled = true\nallowed_internal_ranges = ["127.0.0.0/8"]\n')
