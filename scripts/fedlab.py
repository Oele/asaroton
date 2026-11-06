#!/usr/bin/env python3
"""A small federation lab for end-to-end tests: a throwaway asaroton with its own database,
and mock homeservers it federates with over plain HTTP.

Used by scripts/fedsendertest.py and scripts/remotemediatest.py. Needs target/debug/asaroton
(cargo build), the development database role (DATABASE_URL as in scripts/check.sh) and PyNaCl.
"""
import base64, collections, hashlib, json, os, re, shutil, socket, subprocess, sys, tempfile
import threading, time, urllib.error, urllib.parse, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from nacl.signing import SigningKey

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.environ.get("ASAROTON_BIN") or os.path.join(REPO, "target", "debug", "asaroton")
ADMIN_DB = os.environ.get("DATABASE_URL", "postgres://asaroton:asaroton@localhost/asaroton")
DATABASE = f"asaroton_fedlab_{os.getpid()}"
SERVER_NAME = "sender.test"
KEEP = "--keep" in sys.argv

failures = []


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what)
    if not ok:
        failures.append(what)


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def b64(raw, urlsafe=False):
    enc = base64.urlsafe_b64encode if urlsafe else base64.b64encode
    return enc(raw).decode().rstrip("=")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(cond, timeout, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return cond()


# ---------------------------------------------------------------- mock destinations


class Peer:
    """A mock homeserver. Answers /send from a script (then 200) and records what arrived."""

    def __init__(self, label):
        self.label = label
        self.key = SigningKey.generate()
        self.script = collections.deque()
        self.received = []
        self.lock = threading.Lock()
        # Extra GET routes: (compiled regex on the path, function(handler, match)).
        self.routes = []
        # Extra PUT/POST routes: (compiled regex on the path, function(handler, match, body)).
        self.put_routes = []
        peer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def answer(self, status, body, headers=None, content_type="application/json"):
                raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(raw)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                if self.path.startswith("/_matrix/key/v2/server"):
                    return self.answer(200, peer.server_keys())
                for pattern, serve in peer.routes:
                    m = pattern.match(self.path)
                    if m:
                        return serve(self, m)
                self.answer(404, {"errcode": "M_UNRECOGNIZED", "error": "mock"})

            def do_PUT(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                for pattern, serve in peer.put_routes:
                    m = pattern.match(self.path)
                    if m:
                        return serve(self, m, json.loads(raw or b"{}"))
                m = re.match(r"/_matrix/federation/v1/send/([^/?]+)", self.path)
                if not m:
                    return self.answer(404, {"errcode": "M_UNRECOGNIZED", "error": "mock"})
                with peer.lock:
                    peer.received.append(
                        {"t": time.time(), "txn": urllib.parse.unquote(m.group(1)), "raw": raw, "body": json.loads(raw)}
                    )
                    action = peer.script.popleft() if peer.script else (200, {"pdus": {}})
                if action == "drop":
                    # Take the request and hang up without a word: the sender cannot know
                    # whether it was processed.
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                self.answer(*action)

            do_POST = do_PUT

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.name = f"127.0.0.1:{self.port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def sign(self, obj):
        return b64(self.key.sign(canonical(obj)).signature)

    def server_keys(self):
        obj = {
            "server_name": self.name,
            "valid_until_ts": int(time.time() * 1000) + 86_400_000,
            "verify_keys": {"ed25519:k1": {"key": b64(bytes(self.key.verify_key))}},
            "old_verify_keys": {},
        }
        obj["signatures"] = {self.name: {"ed25519:k1": self.sign(obj)}}
        return obj

    def count(self):
        with self.lock:
            return len(self.received)

    def requests(self, since=0):
        with self.lock:
            return list(self.received[since:])

    # -- acting as a server towards the homeserver under test

    def fed(self, hs_fed, method, uri, content=None):
        signed = {"method": method, "uri": uri, "origin": self.name, "destination": SERVER_NAME}
        if content is not None:
            signed["content"] = content
        auth = f'X-Matrix origin="{self.name}",destination="{SERVER_NAME}",key="ed25519:k1",sig="{self.sign(signed)}"'
        data = None if content is None else canonical(content)
        r = urllib.request.Request(hs_fed + uri, data=data, method=method)
        r.add_header("Authorization", auth)
        if data is not None:
            r.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def join(self, hs_fed, room_id, localpart):
        """Join a public room (version 11) as a user of this mock server."""
        user = f"@{localpart}:{self.name}"
        q = urllib.parse.quote
        st, r = self.fed(hs_fed, "GET", f"/_matrix/federation/v1/make_join/{q(room_id, safe='')}/{q(user, safe='')}?ver=11")
        assert st == 200, (st, r)
        ev = r["event"]
        ev.pop("origin", None)
        ev["origin_server_ts"] = int(time.time() * 1000)
        bare = {k: v for k, v in ev.items() if k not in ("unsigned", "signatures", "hashes")}
        ev["hashes"] = {"sha256": b64(hashlib.sha256(canonical(bare)).digest())}
        kept = ("type", "room_id", "sender", "state_key", "content", "hashes", "depth", "prev_events", "auth_events", "origin_server_ts")
        redacted = {k: v for k, v in ev.items() if k in kept}
        redacted["content"] = {k: v for k, v in ev["content"].items() if k in ("membership", "join_authorised_via_users_server")}
        ev["signatures"] = {self.name: {"ed25519:k1": self.sign(redacted)}}
        event_id = "$" + b64(hashlib.sha256(canonical(redacted)).digest(), urlsafe=True)
        st, r = self.fed(hs_fed, "PUT", f"/_matrix/federation/v2/send_join/{q(room_id, safe='')}/{q(event_id, safe='')}", ev)
        assert st == 200, (st, r)
        return user


# ---------------------------------------------------------------- the server under test


class Homeserver:
    def __init__(self, work, peers):
        self.work = work
        self.client_port, self.fed_port = free_port(), free_port()
        self.url = f"http://127.0.0.1:{self.client_port}"
        self.fed = f"http://127.0.0.1:{self.fed_port}"
        self.peers = peers
        self.log = os.path.join(work, "server.log")
        self.proc = None

    def start(self, federation_extra=""):
        names = ", ".join(f'"{p.name}"' for p in self.peers)
        base, _, _ = ADMIN_DB.rpartition("/")
        config = os.path.join(self.work, "asaroton.toml")
        with open(config, "w") as f:
            f.write(
                f"""server_name = "{SERVER_NAME}"
database_url = "{base}/{DATABASE}"
data_dir = "{self.work}/data"
[listeners.client]
bind = "127.0.0.1:{self.client_port}"
[listeners.federation]
bind = "127.0.0.1:{self.fed_port}"
[federation]
insecure_http_servers = [{names}]
trusted_key_servers = ["{self.peers[0].name}"]
{federation_extra}
[registration]
enabled = true
[rate_limiting]
enabled = false
[logging]
filter = "info,asaroton=debug,ruma_state_res=warn"
"""
            )
        out = open(self.log, "ab")
        self.proc = subprocess.Popen([BINARY, "--config", config], stdout=out, stderr=subprocess.STDOUT, env={**os.environ, "NO_COLOR": "1"})
        if not wait_for(self.up, 30, 0.2):
            raise SystemExit(f"server did not start, see {self.log}")

    def up(self):
        try:
            urllib.request.urlopen(self.url + "/_matrix/client/versions", timeout=2).read()
            return True
        except Exception:
            return False

    def stop(self):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None

    def log_lines(self, needle):
        ansi = re.compile(r"\x1b\[[0-9;]*m")
        with open(self.log, errors="replace") as f:
            return [line for line in (ansi.sub("", l) for l in f) if needle in line]

    def req(self, method, path, body=None, token=None):
        data = None if body is None else json.dumps(body).encode()
        r = urllib.request.Request(self.url + path, data=data, method=method)
        if body is not None:
            r.add_header("Content-Type", "application/json")
        if token:
            r.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def register(self, name):
        st, r = self.req("POST", "/_matrix/client/v3/register", {"username": name, "password": "pwpwpwpw", "auth": {"type": "m.login.dummy"}})
        assert st == 200, r
        return r["user_id"], r["access_token"]

    def to_device(self, token, peer, marker):
        st, r = self.req("PUT", f"/_matrix/client/v3/sendToDevice/m.test.marker/{uuid.uuid4().hex}", {"messages": {f"@x:{peer.name}": {"*": {"marker": marker}}}}, token)
        assert st == 200, r
        return time.time()

    def typing(self, token, room_id, user_id, on):
        q = urllib.parse.quote
        st, r = self.req("PUT", f"/_matrix/client/v3/rooms/{q(room_id)}/typing/{q(user_id)}", {"typing": on, "timeout": 30000}, token)
        assert st == 200, r

    def message(self, token, room_id, body):
        st, r = self.req("PUT", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room_id)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": body}, token)
        assert st == 200, r
        return time.time()


def psql(statement):
    subprocess.run(["psql", "-q", ADMIN_DB, "-c", statement], check=True, stdout=subprocess.DEVNULL)




def run_lab(labels, scenario, federation_extra=""):
    """Start mock peers named by `labels` and a homeserver, run `scenario(hs, peers)`, clean up
    and exit with the result."""
    if not os.path.exists(BINARY):
        raise SystemExit(f"{BINARY} missing: run cargo build first")
    work = tempfile.mkdtemp(prefix="fedlab-")
    peers = {label: Peer(label) for label in labels}
    hs = Homeserver(work, list(peers.values()))
    psql(f"CREATE DATABASE {DATABASE}")
    try:
        hs.start(federation_extra)
        scenario(hs, peers)
    finally:
        hs.stop()
        if KEEP or failures:
            print(f"kept: {work} and database {DATABASE}")
        else:
            psql(f"DROP DATABASE IF EXISTS {DATABASE} WITH (FORCE)")
            shutil.rmtree(work, ignore_errors=True)
    print("\nFAILED" if failures else "\nall passed")
    sys.exit(1 if failures else 0)
