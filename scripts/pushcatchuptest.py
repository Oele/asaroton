#!/usr/bin/env python3
"""A push worker that falls behind its notifications catches up from the database: no message
goes unpushed when the room traffic outruns it.

The lab runs the server with a tiny notification channel (ASAROTON_NOTIFIER_CAPACITY=8), so a
burst of messages makes the push worker lag on purpose. Bob has an HTTP pusher; Alice sends 60
messages from eight threads at once. Every one of them must reach Bob's gateway, each once, and
the server must say it caught up.

Needs what scripts/fedlab.py needs. Takes about fifteen seconds.

Usage: scripts/pushcatchuptest.py [--keep]
"""
import json, os, threading, time, urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, free_port, run_lab, wait_for

os.environ["ASAROTON_NOTIFIER_CAPACITY"] = "8"
MESSAGES = 60
pushed = Counter()
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
    st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {
        "app_id": "test.catchup", "pushkey": "key-of-bob", "kind": "http", "app_display_name": "Test",
        "device_display_name": "Phone", "lang": "en",
        "data": {"url": f"http://127.0.0.1:{PORT}/_matrix/push/v1/notify"},
    }, b)
    assert st == 200, r
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "invite": [bob]}, a)
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r
    time.sleep(1)

    def send(i):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/burst{i}",
                       {"msgtype": "m.text", "body": f"burst {i}"}, a)
        assert st == 200, r
        return r["event_id"]

    with ThreadPoolExecutor(8) as pool:
        sent = list(pool.map(send, range(MESSAGES)))
    check(wait_for(lambda: all(e in pushed for e in sent), 20, 0.2),
          f"every one of {MESSAGES} messages reaches Bob's gateway ({sum(1 for e in sent if e in pushed)} of {MESSAGES})")
    check(all(pushed[e] == 1 for e in sent), f"each once ({max((pushed[e] for e in sent), default=0)} at most)")
    check(bool(hs.log_lines("catching up from the database")), "the push worker did fall behind, and caught up")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra='disabled = true\nallowed_internal_ranges = ["127.0.0.0/8"]\n')
