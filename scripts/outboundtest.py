#!/usr/bin/env python3
"""URLs that somebody else chose only lead to public addresses, end to end.

Starts a throwaway asaroton that exempts 127.0.0.2, a listener on 127.0.0.1 that stands in for
an internal service and must never hear from the server, and a listener on 127.0.0.2 that
stands in for the outside world. Checks that

  * a URL preview of the internal service is refused, however its address is spelled, and so
    is a preview that reaches it through a redirect or through the page's image;
  * a push gateway on an internal address gets no notifications;
  * media that a homeserver redirects to an internal address is not fetched;
  * all three work for the exempted address.

Needs what scripts/fedlab.py needs. Takes about ten seconds.

Usage: scripts/outboundtest.py [--keep]
"""
import json, re, threading, time, urllib.error, urllib.parse, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, run_lab, wait_for


class Listener:
    """An HTTP server on one loopback address that records what it is asked."""

    def __init__(self, address, internal_port=None):
        self.hits = []
        me = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def reply(self, status, body, content_type="text/html", headers=None):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                me.hits.append(("GET", self.path))
                inside = f"http://127.0.0.1:{me.internal_port}"
                if self.path == "/redirect":
                    return self.reply(302, b"", headers={"Location": inside + "/internal"})
                if self.path == "/page":
                    return self.reply(200, f'<html><head><title>outside page</title><meta property="og:image" content="{inside}/image.png"></head></html>'.encode())
                if self.path == "/file":
                    return self.reply(200, b"file from the outside", "application/octet-stream")
                self.reply(200, b"<html><head><title>internal admin page</title></head></html>")

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                me.hits.append(("POST", self.path, json.loads(body or b"{}")))
                self.reply(200, b'{"rejected": []}', "application/json")

        self.httpd = ThreadingHTTPServer((address, 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.internal_port = internal_port or self.port
        self.url = f"http://{address}:{self.port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()


def run(hs, peers):
    internal = Listener("127.0.0.1")
    outside = Listener("127.0.0.2", internal.port)
    alice, atok = hs.register("alice")
    bob, btok = hs.register("bob")
    q = urllib.parse.quote

    def preview(url):
        return hs.req("GET", "/_matrix/client/v1/media/preview_url?url=" + q(url, safe=""), None, atok)

    # ---- URL previews
    spellings = [
        f"http://127.0.0.1:{internal.port}/internal",
        f"http://localhost:{internal.port}/internal",
        f"http://2130706433:{internal.port}/internal",
        f"http://0x7f.0.0.1:{internal.port}/internal",
        f"http://[::ffff:127.0.0.1]:{internal.port}/internal",
        f"http://[::1]:{internal.port}/internal",
    ]
    for url in spellings:
        st, r = preview(url)
        check(st != 200 and "internal admin page" not in json.dumps(r), f"preview of {url.replace(str(internal.port), 'PORT')} is refused ({st})")
    st, r = preview(outside.url + "/redirect")
    check(st != 200 and "internal admin page" not in json.dumps(r), f"preview that redirects to the internal service is refused ({st})")
    st, r = preview(outside.url + "/page")
    check(st == 200 and r.get("og:title") == "outside page", f"preview of the exempted address works ({st} {r.get('og:title')!r})")
    check(not str(r.get("og:image", "")).startswith("mxc://"), "its image on the internal service is not fetched")
    st, r = preview("file:///etc/hostname")
    check(st != 200, f"preview of a file: URL is refused ({st})")

    # ---- push gateways
    st, room = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "trusted_private_chat", "invite": [bob], "is_direct": True}, atok)
    assert st == 200, room
    room_id = room["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room_id)}/join", {}, btok)
    assert st == 200, r

    def pusher(gateway, key):
        st, r = hs.req("POST", "/_matrix/client/v3/pushers/set", {"kind": "http", "app_id": "test.outbound", "pushkey": key, "app_display_name": "test", "device_display_name": "test", "lang": "en", "data": {"url": gateway + "/_matrix/push/v1/notify"}}, btok)
        assert st == 200, r

    pusher(internal.url, "internal-key")
    hs.message(atok, room_id, "first")
    time.sleep(3)
    refused = hs.log_lines("not pushing to this gateway")
    check(bool(refused) and all("is not a public address" in line for line in refused), "a pusher on the internal service is skipped, with a log line")
    pusher(outside.url, "outside-key")
    hs.message(atok, room_id, "second")
    wait_for(lambda: any(h[0] == "POST" for h in outside.hits), 10)
    posts = [h for h in outside.hits if h[0] == "POST"]
    check(len(posts) == 1 and posts[0][1] == "/_matrix/push/v1/notify" and posts[0][2].get("notification", {}).get("devices", [{}])[0].get("pushkey") == "outside-key", f"a pusher on the exempted address gets the notification ({len(posts)} posts)")

    # ---- media a homeserver redirects to
    peer = peers["media"]
    boundary = "mock-boundary"

    def authenticated(handler, m):
        target = (internal.url if m.group(1) == "inside" else outside.url) + "/file"
        body = f"--{boundary}\r\nContent-Type: application/json\r\n\r\n{{}}\r\n--{boundary}\r\nLocation: {target}\r\n\r\n\r\n--{boundary}--\r\n".encode()
        handler.answer(200, body, None, f"multipart/mixed; boundary={boundary}")

    peer.routes.append((re.compile(r"/_matrix/federation/v1/media/download/([^/?]+)"), authenticated))

    def download(media_id):
        r = urllib.request.Request(f"{hs.url}/_matrix/client/v1/media/download/{peer.name}/{media_id}")
        r.add_header("Authorization", "Bearer " + atok)
        try:
            with urllib.request.urlopen(r, timeout=60) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    st, body = download("inside")
    check(st != 200 and b"internal admin page" not in body, f"media redirected to the internal service is not fetched ({st})")
    st, body = download("outside")
    check(st == 200 and body == b"file from the outside", f"media redirected to the exempted address is fetched ({st})")

    # ---- the internal service never heard from the server
    check(internal.hits == [], f"the internal service received nothing ({internal.hits})")
    check(hs.proc.poll() is None, "the server is still running")


if __name__ == "__main__":
    run_lab(["media"], run, 'allowed_internal_ranges = ["127.0.0.2"]')
