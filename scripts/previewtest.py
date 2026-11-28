#!/usr/bin/env python3
"""URL previews of pages the site does not give: a refusal (a site firewall's "406 WAF
Forbidden") or an error page is not the page the user linked, so there is no preview, not one
titled after the error. An image that answers with an error is not stored as the preview image.

A page larger than the server reads (1 MB of HTML) is previewed from its beginning, where the
title and the og: tags are, not refused. A page that cannot be fetched is a 502, and the log
names the site's host and the reason, not the URL.

The lab: a small web server on localhost (allowed to the server for this test) with an article,
a page refused with 406, a redirect to that refusal, an article whose image is missing, and a
3 MB article.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/previewtest.py [--keep]
"""
import base64, threading, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fedlab import check, free_port, run_lab

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
PORT = free_port()
SITE = f"http://127.0.0.1:{PORT}"


def article(title, image):
    return (f'<html><head><title>{title}</title><meta property="og:title" content="{title}">'
            f'<meta property="og:image" content="{image}"></head><body>text</body></html>').encode()


class Site(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def answer(self, status, body, ctype="text/html; charset=utf-8", headers=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/kiln":
            self.answer(200, article("Kiln opening on Saturday", "/kiln.png"))
        elif self.path == "/kiln.png":
            self.answer(200, PNG, "image/png")
        elif self.path == "/waf":
            self.answer(406, b"<html><head><title>406 WAF Forbidden</title></head><body>Forbidden</body></html>")
        elif self.path == "/moved":
            self.answer(302, b"", headers={"Location": "/waf"})
        elif self.path == "/glaze":
            self.answer(200, article("Glazes for beginners", "/gone.png"))
        elif self.path == "/big":
            head = article("A very long catalogue", "/kiln.png").replace(b"</body></html>", b"")
            self.answer(200, head + b"<p>" + b"clay " * (3 * 1024 * 1024 // 5) + b"</p></body></html>")
        else:
            self.answer(404, b"<html><head><title>404 Not Found</title></head></html>")


threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", PORT), Site).serve_forever, daemon=True).start()


def run(hs, peers):
    alice, a = hs.register("alice")

    def preview(path):
        url = urllib.parse.quote(SITE + path, safe="")
        return hs.req("GET", f"/_matrix/client/v1/media/preview_url?url={url}", None, a)

    st, r = preview("/kiln")
    check(st == 200 and r.get("og:title") == "Kiln opening on Saturday" and str(r.get("og:image", "")).startswith("mxc://"),
          f"an article gets its title and image ({st}, {r.get('og:title')}, {r.get('og:image')})")
    st, r = preview("/waf")
    check(st == 200 and "og:title" not in r, f"a page the site refuses with 406 has no preview, not one titled after the refusal ({st}, {r})")
    st, r = preview("/moved")
    check(st == 200 and "og:title" not in r, f"nor has a redirect that ends at the refusal ({st}, {r})")
    st, r = preview("/nowhere")
    check(st == 200 and "og:title" not in r, f"nor a page that is not there ({st}, {r})")
    st, r = preview("/glaze")
    check(st == 200 and r.get("og:title") == "Glazes for beginners" and "og:image" not in r,
          f"an article whose image is missing keeps its title, without an image ({st}, {r.get('og:image')})")
    st, r = preview("/big")
    check(st == 200 and r.get("og:title") == "A very long catalogue",
          f"a 3 MB page is previewed from its beginning ({st}, {r.get('og:title') or r})")
    dead = free_port()
    url = urllib.parse.quote(f"http://127.0.0.1:{dead}/secret-path?token=abc", safe="")
    st, r = hs.req("GET", f"/_matrix/client/v1/media/preview_url?url={url}", None, a)
    lines = hs.log_lines("URL preview failed")
    check(st == 502 and lines and "127.0.0.1" in lines[-1] and "secret-path" not in lines[-1] and "token" not in lines[-1],
          f"an unreachable site: 502, logged by host without the URL ({st}, {lines[-1][-200:] if lines else None})")


if __name__ == "__main__":
    run_lab(["mock"], run, federation_extra='allowed_internal_ranges = ["127.0.0.0/8"]\n')
