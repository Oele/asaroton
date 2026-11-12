#!/usr/bin/env python3
"""A server with federation disabled still tells those who asked to be told.

Push gateways and application services are no part of federation: the users of a closed
server expect to be woken on their phones like anybody else, and a bridge on it expects its
events. The workers that do this used to be started with federation, so that on a server
with `federation.disabled = true` a pusher was accepted and never called, and an application
service got nothing.

A closed server here has a push gateway and an application service listening next to it.
Bob sets a pusher and Alice writes to him: the gateway is called with that event. The
application service, which is interested in the room's alias, is sent the same event.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/closedservertest.py [--keep]
"""
import json, os, socket, tempfile, threading, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import SERVER_NAME, check, free_port, run_lab, wait_for

received = {"push": [], "appservice": []}


class Listener(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def answer(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        if self.path.startswith("/_matrix/push/v1/notify"):
            received["push"].append(body)
            out = {"rejected": []}
        else:
            if "/transactions/" in self.path:
                received["appservice"].append((self.headers.get("Authorization"), body))
            out = {}
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_POST = do_PUT = do_GET = answer


PORT = free_port()
listener = ThreadingHTTPServer(("127.0.0.1", PORT), Listener)
threading.Thread(target=listener.serve_forever, daemon=True).start()

REG_DIR = tempfile.mkdtemp(prefix="fedlab-as-")
with open(os.path.join(REG_DIR, "bridge.yaml"), "w") as f:
    f.write(f"""id: bridge
url: http://127.0.0.1:{PORT}
as_token: as-token-of-the-bridge
hs_token: hs-token-of-the-bridge
sender_localpart: bridgebot
namespaces:
  users: []
  aliases:
    - exclusive: false
      regex: '^#bridged_.*:{SERVER_NAME.replace(".", chr(92) + ".")}$'
  rooms: []
""")


def run(hs, peers):
    q = urllib.parse.quote
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    try:
        socket.create_connection(("127.0.0.1", hs.fed_port), timeout=2).close()
        listening = True
    except OSError:
        listening = False
    check(not listening, "the server is closed: nothing listens for federation")

    st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {
        "app_id": "test.closed", "pushkey": "key-of-bob", "kind": "http", "app_display_name": "Test", "device_display_name": "Phone", "lang": "en",
        "data": {"url": f"http://127.0.0.1:{PORT}/_matrix/push/v1/notify"},
    }, b)
    check(st == 200, f"bob sets a pusher ({st} {r})")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "trusted_private_chat", "is_direct": True, "invite": [bob], "room_alias_name": "bridged_talk"}, a)
    assert st == 200, r
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/one", {"msgtype": "m.text", "body": "are you there?"}, a)
    assert st == 200, r
    event_id = r["event_id"]

    pushed = lambda: [p for p in received["push"] if p.get("notification", {}).get("event_id") == event_id]
    wait_for(pushed, 20, 0.2)
    got = pushed()
    check(len(got) == 1, f"the push gateway is called for alice's message, once ({len(got)} calls for it, {len(received['push'])} in all)")
    devices = got[0]["notification"].get("devices", []) if got else []
    check([d.get("pushkey") for d in devices] == ["key-of-bob"] and got[0]["notification"].get("room_id") == room, f"for bob's pusher and this room ({devices})")

    sent = lambda: [e for _, txn in received["appservice"] for e in txn.get("events", []) if e.get("event_id") == event_id]
    wait_for(sent, 20, 0.2)
    check(len(sent()) == 1, f"the application service is sent the same event, once ({len(sent())}, in {len(received['appservice'])} transactions)")
    tokens = {auth for auth, _ in received["appservice"]}
    check(tokens == {"Bearer hs-token-of-the-bridge"}, f"with the token it was registered with ({len(tokens)} different)")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra=f'disabled = true\nallowed_internal_ranges = ["127.0.0.0/8"]\n[appservices]\nregistration_dir = "{REG_DIR}"\n')
