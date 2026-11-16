#!/usr/bin/env python3
"""Events of an ignored user stay out of every API that hands out events (tracker issue 68).

Sync left them out, but /messages and /context did not, and matrix-rust-sdk refills its
timelines from /messages after the ignore list changes: the ignored user's messages came back.
Here Bob talks (a message, a thread reply, a reaction, a thread of his own); Alice ignores him.
Then /messages, /context, /event, /relations, /threads and /search must leave out his messages,
while his state events (his join) stay, as the specification says. Carol, who ignores
nobody, still sees everything.

usage: scripts/ignoretest.py   (starts its own server, like the other fedlab scripts)
"""
import json
import threading
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, free_port, run_lab, wait_for

pushed = []  # event IDs Alice's push gateway was called with


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        pushed.append(body.get("notification", {}).get("event_id"))
        data = b'{"rejected": []}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


GATEWAY = free_port()
threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", GATEWAY), Gateway).serve_forever, daemon=True).start()


def run(hs, peers):
    q = urllib.parse.quote
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    assert st == 200, r
    room = r["room_id"]
    for token in (b, c):
        st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, token)
        assert st == 200, r

    def send(token, content, kind="m.room.message"):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/{kind}/{uuid.uuid4().hex}", content, token)
        assert st == 200, r
        return r["event_id"]

    root = send(a, {"msgtype": "m.text", "body": "a thread of mine, findme"})
    bob_msg = send(b, {"msgtype": "m.text", "body": "bob talks, findme"})
    bob_reply = send(b, {"msgtype": "m.text", "body": "bob in the thread", "m.relates_to": {"rel_type": "m.thread", "event_id": root}})
    bob_root = send(b, {"msgtype": "m.text", "body": "bob's own thread"})
    send(c, {"msgtype": "m.text", "body": "carol in bob's thread", "m.relates_to": {"rel_type": "m.thread", "event_id": bob_root}})
    bob_react = send(b, {"m.relates_to": {"rel_type": "m.annotation", "event_id": root, "key": "x"}}, "m.reaction")
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/state/m.room.member/{q(bob)}?format=event", None, a)
    topic = r.get("event_id")  # bob's join: a state event of his, which stays
    alice_msg = send(a, {"msgtype": "m.text", "body": "alice after bob"})
    bobs = {bob_msg, bob_reply, bob_root, bob_react}

    st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/m.ignored_user_list", {"ignored_users": {bob: {}}}, a)
    check(st == 200, f"alice ignores bob ({st})")

    def ids_messages(token):
        st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/messages?dir=b&limit=50", None, token)
        assert st == 200, r
        return {e["event_id"] for e in r["chunk"]}

    got = ids_messages(a)
    check(not (got & bobs), f"/messages leaves out bob's events for alice ({len(got & bobs)} of {len(bobs)} still there)")
    check(topic is not None and topic in got and alice_msg in got, "but keeps bob's join (a state event) and alice's own messages")
    check(bobs <= ids_messages(c), "carol, who ignores nobody, still gets them all")

    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/context/{q(alice_msg)}?limit=20", None, a)
    around = {e["event_id"] for e in r.get("events_before", []) + r.get("events_after", [])}
    check(st == 200 and not (around & bobs) and topic in around, f"/context leaves them out too ({st}, {len(around & bobs)} of bob's, his join {'in' if topic in around else 'missing'})")
    st, r = hs.req("GET", f"/_matrix/client/v3/rooms/{q(room)}/event/{q(bob_msg)}", None, a)
    check(st == 404, f"/event for bob's message: {st}")

    st, r = hs.req("GET", f"/_matrix/client/v1/rooms/{q(room)}/relations/{q(root)}", None, a)
    rel = {e["event_id"] for e in r.get("chunk", [])}
    check(st == 200 and not (rel & bobs), f"/relations of alice's message leaves out bob's reply and reaction ({st}, {len(rel & bobs)})")
    st, r = hs.req("GET", f"/_matrix/client/v1/rooms/{q(room)}/threads", None, a)
    roots = {e["event_id"] for e in r.get("chunk", [])}
    check(st == 200 and bob_root not in roots and root in roots, f"/threads leaves out the thread bob started ({st}, roots {len(roots)})")

    st, r = hs.req("POST", "/_matrix/client/v3/search", {"search_categories": {"room_events": {"search_term": "findme"}}}, a)
    hits = {x["result"]["event_id"] for x in r.get("search_categories", {}).get("room_events", {}).get("results", [])}
    check(st == 200 and bob_msg not in hits and root in hits, f"/search leaves out bob's message ({st}, {len(hits)} hits)")

    # Push: Alice is not woken for Bob any more; Carol still wakes her.
    st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {
        "app_id": "test.ignore", "pushkey": "key-of-alice", "kind": "http", "app_display_name": "Test",
        "device_display_name": "Phone", "lang": "en",
        "data": {"url": f"http://127.0.0.1:{GATEWAY}/_matrix/push/v1/notify"},
    }, a)
    assert st == 200, r
    from_bob = send(b, {"msgtype": "m.text", "body": "bob after the ignore"})
    from_carol = send(c, {"msgtype": "m.text", "body": "carol after the ignore"})
    wait_for(lambda: from_carol in pushed, 10, 0.1)
    check(from_carol in pushed and from_bob not in pushed, f"alice's gateway is called for carol's message, not for bob's ({len(pushed)} pushes)")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra='disabled = true\nallowed_internal_ranges = ["127.0.0.0/8"]\n')
