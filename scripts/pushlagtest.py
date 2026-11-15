#!/usr/bin/env python3
"""A push gateway that does not answer holds up nobody else's pushes.

Before, one worker delivered every push itself, three attempts over seven seconds for a gateway
that does not answer, and the pushes of all other users waited behind them (tracker issue 66).
Here Bob's pusher points at a port where nothing listens and Carol's at a gateway that answers;
Alice sends several messages in a row to a room with both. Each must reach Carol's gateway
within a few seconds, in order, while Bob's attempts are still failing.

usage: scripts/pushlagtest.py   (starts its own server, like the other fedlab scripts)
"""
import json
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, free_port, run_lab, wait_for

arrived = {}  # event_id -> time the gateway was called with it


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        event_id = body.get("notification", {}).get("event_id")
        if event_id:
            arrived.setdefault(event_id, time.monotonic())
        data = json.dumps({"rejected": []}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


LIVE = free_port()
gateway = ThreadingHTTPServer(("127.0.0.1", LIVE), Gateway)
threading.Thread(target=gateway.serve_forever, daemon=True).start()
DEAD = free_port()  # nothing listens here: every attempt is refused at once

MESSAGES = 6
# Two seconds is generous for a push to a gateway on the same machine; with the old worker
# the k-th message waited about 7 * k seconds behind Bob's failing attempts.
PROMPT = 2.0


def run(hs, peers):
    q = urllib.parse.quote
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    for token, port, key in ((b, DEAD, "key-of-bob"), (c, LIVE, "key-of-carol")):
        st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {
            "app_id": "test.lag", "pushkey": key, "kind": "http", "app_display_name": "Test",
            "device_display_name": "Phone", "lang": "en",
            "data": {"url": f"http://127.0.0.1:{port}/_matrix/push/v1/notify"},
        }, token)
        check(st == 200, f"pusher {key} set ({st} {r})")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "invite": [bob, carol]}, a)
    assert st == 200, r
    room = r["room_id"]
    for token in (b, c):
        st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, token)
        assert st == 200, r
    time.sleep(1)  # let the joins' own pushes go by

    sent = []
    for i in range(MESSAGES):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/lag{i}", {"msgtype": "m.text", "body": f"message {i}"}, a)
        assert st == 200, r
        sent.append((r["event_id"], time.monotonic()))
        time.sleep(0.2)

    wait_for(lambda: all(e in arrived for e, _ in sent), 10 + 8 * MESSAGES, 0.2)
    delays = [arrived[e] - t if e in arrived else None for e, t in sent]
    shown = ", ".join("lost" if d is None else f"{d:.2f}s" for d in delays)
    check(all(d is not None for d in delays), f"carol's gateway is called for every message ({shown})")
    check(all(d is not None and d < PROMPT for d in delays), f"each within {PROMPT}s of sending, though bob's gateway never answers ({shown})")
    order = [arrived[e] for e, _ in sent if e in arrived]
    check(order == sorted(order), "in the order the messages were sent")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra='disabled = true\nallowed_internal_ranges = ["127.0.0.0/8"]\n')
