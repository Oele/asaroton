#!/usr/bin/env python3
"""An invitation pushes the invited user (`.m.rule.invite_for_me`): the user it is for is not a
member yet, but is the one to wake. Both for an invitation on this server and for one that comes
from another server over `/invite`. A message after the join pushes as before, and an invitation
for someone else wakes nobody.

The lab: Bob has an HTTP pusher pointed at a gateway the test runs; Alice invites him, and so
does a user on a mock server.

Needs what scripts/fedlab.py needs. Takes about ten seconds.

Usage: scripts/invitepushtest.py [--keep]
"""
import json, threading, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, free_port, run_lab, signed_event, wait_for

pushed = {}  # event_id -> the notification


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        n = body.get("notification", {})
        if n.get("event_id"):
            pushed[n["event_id"]] = n
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
    mock = peers["mock"]
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    for token, key in ((b, "key-of-bob"), (c, "key-of-carol")):
        st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {
            "app_id": "test.invites", "pushkey": key, "kind": "http", "app_display_name": "Test",
            "device_display_name": "Phone", "lang": "en",
            "data": {"url": f"http://127.0.0.1:{PORT}/_matrix/push/v1/notify"},
        }, token)
        assert st == 200, r

    # An invitation on this server.
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "name": "Kiln rota", "invite": [bob]}, a)
    assert st == 200, r
    room = r["room_id"]
    found = {}

    def invite_listed():
        st, members = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/members?membership=invite", None, a)
        for e in members.get("chunk", []):
            if e.get("state_key") == bob:
                found["id"] = e["event_id"]
        return "id" in found

    assert wait_for(invite_listed, 5, 0.1), "Bob's invitation is not listed"
    invite_id = found["id"]
    check(wait_for(lambda: invite_id in pushed, 5, 0.1), "Bob's pusher is called for his invitation")
    n = pushed.get(invite_id, {})
    check(n.get("type") == "m.room.member" and n.get("user_is_target") is True and n.get("content", {}).get("membership") == "invite",
          f"as an invitation for him ({n.get('type')}, user_is_target={n.get('user_is_target')})")

    # After the join, messages push as before; Carol, not invited, hears of nothing.
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/m1", {"msgtype": "m.text", "body": "who fires the kiln on Friday?"}, a)
    msg_id = r["event_id"]
    check(wait_for(lambda: msg_id in pushed, 5, 0.1), "a message after the join is pushed")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "private_chat", "invite": [bob]}, a)
    time.sleep(1)
    carol_pushed = [e for e, v in pushed.items() if "key-of-carol" in json.dumps(v)]
    check(carol_pushed == [], f"an invitation for someone else wakes nobody else ({len(carol_pushed)})")

    # An invitation from another server, over /invite.
    host = f"@host:{mock.name}"
    fed_room = f"!glaze:{mock.name}"
    now = int(time.time() * 1000)
    create_id, create = signed_event(mock.name, mock.key, "ed25519:k1", {
        "type": "m.room.create", "state_key": "", "sender": host, "room_id": fed_room,
        "content": {"room_version": "11"}, "origin_server_ts": now, "depth": 1, "auth_events": [], "prev_events": []})
    invite_id2, invite = signed_event(mock.name, mock.key, "ed25519:k1", {
        "type": "m.room.member", "state_key": bob, "sender": host, "room_id": fed_room,
        "content": {"membership": "invite"}, "origin_server_ts": now + 1, "depth": 2,
        "auth_events": [create_id], "prev_events": [create_id]})
    stripped = [{"type": "m.room.name", "state_key": "", "sender": host, "content": {"name": "Glaze lab"}}]
    st, r = mock.fed(hs.fed, "PUT", f"/_matrix/federation/v2/invite/{q(fed_room)}/{q(invite_id2)}",
                     {"room_version": "11", "event": invite, "invite_room_state": [create] + stripped})
    check(st == 200, f"the other server's invitation is taken ({st}, {r.get('errcode')})")
    check(wait_for(lambda: invite_id2 in pushed, 5, 0.1), "and Bob's pusher is called for it too")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra='allowed_internal_ranges = ["127.0.0.0/8"]\n')
