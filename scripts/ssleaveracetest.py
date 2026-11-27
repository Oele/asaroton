#!/usr/bin/env python3
"""Regression test: a leave or ban that commits while a sliding sync answer is built (tracker #104).

    scripts/ssleaveracetest.py http://127.0.0.1:18099 [TRIALS]   (a test server with open registration)

Clients built on matrix-rust-sdk (Element X, smaltino) read the user's own membership only from
`required_state`: a room in an answer without `invite_state` counts as joined unless
`required_state` carries the user's membership event, and the timeline is never read for it.
asaroton used to cap a joined room's timeline at the answer's position but read its state as it
was when the room's turn came, so a leave that committed in between went out in `required_state`
of an answer that still showed the room as joined, and the next answer, which reported the room
as left, held the leave back because the connection had it. The room then stayed joined in the
client for good.

Here a user with twenty rooms opens a room list connection as matrix-sdk-ui does (`$ME` and
`$LAZY` memberships, timeline limit 1), and a sync request is started a few milliseconds before
the user leaves a room, or before someone else bans them from it. Following the SDK's rule over
every answer, the room must end up left (or banned), every trial. And:

- after the room is reported gone, a request waits for its timeout instead of reporting it again;
- a user who joins and is banned between two answers is told (MSC4186: rooms the user was kicked
  or banned from are always in the lists), and so is a fresh connection;
- a room the user left themselves is not in a fresh connection's list.
"""
import json
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HS = sys.argv[1].rstrip("/")
TRIALS = int(sys.argv[2]) if len(sys.argv) > 2 else 12
SYNC = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
REQUIRED = [["m.room.member", "$ME"], ["m.room.member", "$LAZY"], ["m.room.name", ""],
            ["m.room.create", ""], ["m.room.encryption", ""], ["m.room.power_levels", ""]]
failures = []


def call(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(HS + path, data=None if body is None else json.dumps(body).encode(),
                                 method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: {e.code} {e.read().decode()[:300]}")


def register(name):
    r = call("POST", "/_matrix/client/v3/register",
             {"username": f"{name}{secrets.token_hex(4)}", "password": secrets.token_urlsafe(16),
              "auth": {"type": "m.login.dummy"}})
    return r["user_id"], r["access_token"]


def q(s):
    return urllib.parse.quote(s, safe="")


class Client:
    """One sliding sync connection, and the rooms' memberships as matrix-rust-sdk derives them."""

    def __init__(self, user, token, conn):
        self.user, self.token, self.conn, self.pos = user, token, conn, None
        self.state = {}

    def sync(self, timeout=0):
        body = {"conn_id": self.conn, "lists": {"all": {"ranges": [[0, 199]], "required_state": REQUIRED,
                                                         "timeline_limit": 1}}}
        path = f"{SYNC}?timeout={timeout}" + (f"&pos={self.pos}" if self.pos else "")
        r = call("POST", path, body, self.token)
        self.pos = r["pos"]
        for room, data in r.get("rooms", {}).items():
            if "invite_state" in data:
                self.state[room] = "invite"
                continue
            own = [e for e in data.get("required_state", [])
                   if e["type"] == "m.room.member" and e.get("state_key") == self.user]
            self.state[room] = own[-1]["content"]["membership"] if own else "join"
        return r

    def settle(self):
        for _ in range(10):
            if not self.sync().get("rooms"):
                return
        failures.append(f"{self.conn}: the connection never went quiet")


def trial(kind, n, owner, owner_token, user, token):
    c = Client(user, token, f"race-{kind}-{n}")
    c.settle()
    room = call("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "name": f"race {kind} {n}"},
                owner_token)["room_id"]
    call("POST", f"/_matrix/client/v3/join/{q(room)}", {}, token)
    for _ in range(10):
        c.sync()
        if c.state.get(room) == "join":
            break
    else:
        failures.append(f"{kind} {n}: the room never came as joined")
        return False

    def put_out():
        time.sleep((n % 7) * 0.004)
        if kind == "leave":
            call("POST", f"/_matrix/client/v3/rooms/{q(room)}/leave", {}, token)
        else:
            call("POST", f"/_matrix/client/v3/rooms/{q(room)}/ban", {"user_id": user, "reason": "race"}, owner_token)

    t = threading.Thread(target=put_out)
    t.start()
    c.sync()
    t.join()
    c.sync()
    c.sync()
    want = "leave" if kind == "leave" else "ban"
    if c.state.get(room) != want:
        failures.append(f"{kind} {n}: the room is '{c.state.get(room)}' for the client, not '{want}'")
        return False
    return True


def main():
    owner, owner_token = register("owner")
    user, token = register("racer")
    for i in range(20):
        call("POST", "/_matrix/client/v3/createRoom", {"name": f"other {i}"}, token)

    for kind in ("leave", "ban"):
        ok = sum(trial(kind, n, owner, owner_token, user, token) for n in range(TRIALS))
        print(f"{kind}: {ok}/{TRIALS} trials end with the room {kind} for the client")

    # Reported once: the connection waits afterwards.
    c = Client(user, token, "quiet")
    c.settle()
    room = call("POST", "/_matrix/client/v3/createRoom", {"name": "quiet"}, token)["room_id"]
    c.sync()
    call("POST", f"/_matrix/client/v3/rooms/{q(room)}/leave", {}, token)
    c.sync()
    if c.state.get(room) != "leave":
        failures.append(f"a leave outside any race came as {c.state.get(room)}")
    started = time.monotonic()
    r = c.sync(timeout=1500)
    took = time.monotonic() - started
    print(f"after the leave was reported: {took:.2f}s, rooms {list(r.get('rooms', {}))}")
    if room in r.get("rooms", {}) or took < 1.2:
        failures.append(f"after the leave was reported the next request returned after {took:.2f}s with "
                        f"{list(r.get('rooms', {}))}: it should wait")

    # Joined and banned between two answers: the room comes, banned.
    c = Client(user, token, "between")
    c.settle()
    room = call("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat", "name": "between"},
                owner_token)["room_id"]
    call("POST", f"/_matrix/client/v3/join/{q(room)}", {}, token)
    call("POST", f"/_matrix/client/v3/rooms/{q(room)}/ban", {"user_id": user}, owner_token)
    r = c.sync()
    got = r.get("rooms", {}).get(room)
    print(f"joined and banned between two answers: {'sent, ' + str(c.state.get(room)) if got else 'not sent'}")
    if c.state.get(room) != "ban":
        failures.append(f"a room joined and banned from between two answers came as {c.state.get(room)}")
    started = time.monotonic()
    r = c.sync(timeout=1500)
    if room in r.get("rooms", {}) or time.monotonic() - started < 1.2:
        failures.append("the banned room was sent again: the connection should wait")

    # A fresh connection: the rooms the user was banned from are in its list, those it left are not.
    c = Client(user, token, "fresh")
    r = c.sync()
    rooms = r.get("rooms", {})
    names = {}
    for rid, data in rooms.items():
        n = [e["content"].get("name") for e in data.get("required_state", []) if e["type"] == "m.room.name"]
        names[rid] = n[0] if n else data.get("name")
    banned = [rid for rid, s in c.state.items() if s == "ban"]
    left = [rid for rid, s in c.state.items() if s == "leave"]
    print(f"fresh connection: {len(rooms)} rooms, {len(banned)} banned, {len(left)} left")
    if len(banned) != TRIALS + 1:
        failures.append(f"a fresh connection lists {len(banned)} rooms the user was banned from, not {TRIALS + 1}")
    if left:
        failures.append(f"a fresh connection lists {len(left)} rooms the user left themselves")

    if failures:
        for f in failures:
            print("FAIL:", f)
        sys.exit(1)
    print("ok")


if __name__ == "__main__":
    main()
