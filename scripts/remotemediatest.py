#!/usr/bin/env python3
"""Remote media and thumbnails stay within bounds, end to end.

Starts a throwaway asaroton and a mock homeserver that serves media, and checks that

  * ordinary remote media is fetched (over the authenticated endpoint and the legacy one) and
    cached;
  * a remote file larger than an upload may be is refused with M_TOO_LARGE: at once when the
    size is announced, and after reading no more than the limit when it is not;
  * an image that declares enormous dimensions is not decoded for a thumbnail, whether it was
    uploaded here or comes from a remote server, while ordinary images are thumbnailed.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/remotemediatest.py [--keep]
"""
import json, re, struct, time, urllib.error, urllib.parse, urllib.request, zlib

from fedlab import SERVER_NAME, check, run_lab

LIMIT = 100 * 1024 * 1024  # media::MAX_UPLOAD_BYTES
MIB = 1024 * 1024


def png(width, height, declared=None):
    """A PNG of width x height pixels; `declared` overrides the size its header claims."""

    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    rows = b"".join(b"\x00" + b"".join(bytes((x % 256, y % 256, 128)) for x in range(width)) for y in range(height))
    w, h = declared or (width, height)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b"")


SMALL = png(64, 48)
BOMB = png(4, 4, declared=(40_000, 40_000))


def serve_media(peer):
    """Teach the mock server to serve media, and count what it manages to send."""
    peer.sent = {}
    peer.hits = []

    def count(media_id, n):
        peer.sent[media_id] = peer.sent.get(media_id, 0) + n

    def endless(handler, media_id, content_type, preamble=b""):
        handler.send_response(200)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Transfer-Encoding", "chunked")
        handler.end_headers()
        block = b"\x00" * MIB
        try:
            if preamble:
                handler.wfile.write(b"%x\r\n%s\r\n" % (len(preamble), preamble))
            while peer.sent.get(media_id, 0) < 4 * LIMIT:
                handler.wfile.write(b"%x\r\n%s\r\n" % (len(block), block))
                count(media_id, len(block))
        except OSError:
            pass
        handler.close_connection = True

    def authenticated(handler, m):
        media_id = m.group(1)
        peer.hits.append(("authenticated", media_id))
        boundary = "mock-boundary"
        head = f"--{boundary}\r\nContent-Type: application/json\r\n\r\n{{}}\r\n--{boundary}\r\n"
        if media_id == "authsmall":
            body = (head + 'Content-Type: image/png\r\nContent-Disposition: attachment; filename="small.png"\r\n\r\n').encode() + SMALL + f"\r\n--{boundary}--\r\n".encode()
            return handler.answer(200, body, None, f"multipart/mixed; boundary={boundary}")
        if media_id == "authendless":
            return endless(handler, media_id, f"multipart/mixed; boundary={boundary}", (head + "Content-Type: application/octet-stream\r\n\r\n").encode())
        handler.answer(404, {"errcode": "M_UNRECOGNIZED", "error": "no authenticated media here"})

    def legacy(handler, m):
        media_id = m.group(1)
        peer.hits.append(("legacy", media_id))
        if media_id == "small":
            return handler.answer(200, SMALL, {"Content-Disposition": 'attachment; filename="small.png"'}, "image/png")
        if media_id == "bomb":
            return handler.answer(200, BOMB, None, "image/png")
        if media_id == "announced":
            handler.send_response(200)
            handler.send_header("Content-Type", "application/octet-stream")
            handler.send_header("Content-Length", str(LIMIT + 1))
            handler.end_headers()
            block = b"\x00" * MIB
            try:
                while peer.sent.get(media_id, 0) < LIMIT + 1:
                    handler.wfile.write(block)
                    count(media_id, len(block))
            except OSError:
                pass
            handler.close_connection = True
            return None
        if media_id == "endless":
            return endless(handler, media_id, "application/octet-stream")
        handler.answer(404, {"errcode": "M_NOT_FOUND", "error": "mock"})

    peer.routes.append((re.compile(r"/_matrix/federation/v1/media/download/([^/?]+)"), authenticated))
    peer.routes.append((re.compile(r"/_matrix/media/v3/download/[^/]+/([^/?]+)"), legacy))


def fetch(hs, token, path):
    r = urllib.request.Request(hs.url + path)
    r.add_header("Authorization", "Bearer " + token)
    started = time.time()
    try:
        with urllib.request.urlopen(r, timeout=300) as resp:
            return resp.status, resp.read(), resp.headers.get("Content-Type"), time.time() - started
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            body = json.loads(body)
        except ValueError:
            pass
        return e.code, body, e.headers.get("Content-Type"), time.time() - started


def too_large(status, body, expected_status):
    return status == expected_status and isinstance(body, dict) and body.get("errcode") == "M_TOO_LARGE"


def run(hs, peers):
    peer = peers["media"]
    serve_media(peer)
    _, token = hs.register("alice")
    download = f"/_matrix/client/v1/media/download/{peer.name}"
    thumbnail = f"/_matrix/client/v1/media/thumbnail/{peer.name}"

    # ---- ordinary media
    st, body, ct, _ = fetch(hs, token, f"{download}/small")
    check(st == 200 and body == SMALL and ct == "image/png", f"a remote file is fetched over the legacy endpoint ({st})")
    st, body, _, _ = fetch(hs, token, f"{download}/small")
    check(st == 200 and body == SMALL and peer.hits.count(("legacy", "small")) == 1, "and served from the cache the second time")
    st, body, ct, _ = fetch(hs, token, f"{download}/authsmall")
    check(st == 200 and body == SMALL and ct == "image/png", f"a remote file is fetched over the authenticated endpoint ({st})")
    st, body, ct, _ = fetch(hs, token, f"{thumbnail}/small?width=32&height=32&method=crop")
    check(st == 200 and ct == "image/png" and body[:4] == b"\x89PNG", f"a remote image is thumbnailed ({st})")

    # ---- too large
    st, body, _, took = fetch(hs, token, f"{download}/announced")
    sent = peer.sent.get("announced", 0)
    check(too_large(st, body, 502), f"a remote file announced as larger than the limit is refused ({st} {body if isinstance(body, dict) else ''})")
    check(sent <= 8 * MIB and took < 5, f"without reading it ({sent // MIB} MiB sent, {took:.1f} s)")

    for media_id, how in [("endless", "the legacy endpoint"), ("authendless", "the authenticated endpoint")]:
        st, body, _, took = fetch(hs, token, f"{download}/{media_id}")
        sent = peer.sent.get(media_id, 0)
        check(too_large(st, body, 502), f"an endless file over {how} is refused ({st} {body if isinstance(body, dict) else ''})")
        check(LIMIT <= sent <= LIMIT + 16 * MIB, f"after reading no more than the limit ({sent // MIB} MiB sent, {took:.1f} s)")
        st, body, _, _ = fetch(hs, token, f"{download}/{media_id}")
        check(st != 200, "and nothing of it was cached")

    # ---- images that claim to be enormous
    st, body, _, took = fetch(hs, token, f"{thumbnail}/bomb?width=96&height=96&method=crop")
    check(too_large(st, body, 502) and took < 3, f"a remote image declaring 40000x40000 pixels is not thumbnailed ({st}, {took:.2f} s)")

    def upload(data, name):
        r = urllib.request.Request(hs.url + f"/_matrix/media/v3/upload?filename={name}", data=data, method="POST")
        r.add_header("Authorization", "Bearer " + token)
        r.add_header("Content-Type", "image/png")
        with urllib.request.urlopen(r, timeout=30) as resp:
            return json.loads(resp.read())["content_uri"].rsplit("/", 1)[1]

    local = f"/_matrix/client/v1/media/thumbnail/{SERVER_NAME}"
    st, body, _, took = fetch(hs, token, f"{local}/{upload(BOMB, 'bomb.png')}?width=96&height=96&method=crop")
    check(too_large(st, body, 413) and took < 3, f"an uploaded image declaring 40000x40000 pixels is not thumbnailed ({st}, {took:.2f} s)")
    st, body, ct, _ = fetch(hs, token, f"{local}/{upload(png(640, 480), 'ok.png')}?width=320&height=240&method=scale")
    check(st == 200 and ct == "image/png" and body[:4] == b"\x89PNG", f"an ordinary uploaded image is thumbnailed ({st})")
    check(hs.proc.poll() is None, "the server is still running")


if __name__ == "__main__":
    run_lab(["media"], run)
