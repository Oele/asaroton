#!/usr/bin/env python3
"""Synthetic client swarm: registers users, puts them in shared rooms, then hammers the
server with message sends and incremental syncs from many threads and reports throughput
and latency percentiles.

Usage: scripts/loadtest.py --base http://127.0.0.1:8008 --users 50 --rooms 10 --messages 20
"""
import argparse, json, random, statistics, string, threading, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor

def call(base, method, path, body=None, token=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          **({"Authorization": "Bearer " + token} if token else {})})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.loads(r.read() or b"{}"); status = r.status
    except urllib.error.HTTPError as e:
        out = json.loads(e.read() or b"{}"); status = e.code
    return status, out, time.perf_counter() - t0

def register(base, name):
    _, d, _ = call(base, "POST", "/_matrix/client/v3/register", {"username": name, "password": "pw-" + name})
    _, d, _ = call(base, "POST", "/_matrix/client/v3/register",
                   {"username": name, "password": "pw-" + name,
                    "auth": {"type": "m.login.dummy", "session": d["session"]}})
    return d["access_token"], d["user_id"]

def pct(xs, p):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8008")
    ap.add_argument("--users", type=int, default=50)
    ap.add_argument("--rooms", type=int, default=10)
    ap.add_argument("--messages", type=int, default=20, help="messages per user")
    ap.add_argument("--threads", type=int, default=32)
    a = ap.parse_args()
    tag = "".join(random.choices(string.ascii_lowercase, k=5))
    lat = {"register": [], "join": [], "send": [], "sync": [], "sync_incr": []}
    lock = threading.Lock()
    def rec(k, v):
        with lock: lat[k].append(v)

    t_start = time.perf_counter()
    with ThreadPoolExecutor(a.threads) as ex:
        users = list(ex.map(lambda i: register(a.base, f"lt{tag}{i}"), range(a.users)))
    rec("register", time.perf_counter() - t_start)
    creator_tok = users[0][0]
    rooms = []
    for r in range(a.rooms):
        _, d, dt = call(a.base, "POST", "/_matrix/client/v3/createRoom",
                        {"preset": "public_chat", "name": f"load {tag} {r}"}, creator_tok)
        rooms.append(d["room_id"])
    def join_all(u):
        tok, _ = u
        for rid in rooms:
            s, _, dt = call(a.base, "POST", f"/_matrix/client/v3/join/{urllib.parse.quote(rid, safe='')}", {}, tok)
            rec("join", dt)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(a.threads) as ex:
        list(ex.map(join_all, users[1:]))
    join_time = time.perf_counter() - t0

    def chatter(u):
        tok, uid = u
        s, d, dt = call(a.base, "GET", "/_matrix/client/v3/sync?timeout=0", None, tok)
        rec("sync", dt); since = d.get("next_batch")
        errors = 0
        for i in range(a.messages):
            rid = random.choice(rooms)
            s, _, dt = call(a.base, "PUT",
                            f"/_matrix/client/v3/rooms/{urllib.parse.quote(rid, safe='')}/send/m.room.message/{tag}{i}",
                            {"msgtype": "m.text", "body": f"hello {i} from {uid}"}, tok)
            rec("send", dt)
            if s != 200: errors += 1
            if i % 5 == 4:
                s, d, dt = call(a.base, "GET", f"/_matrix/client/v3/sync?timeout=0&since={urllib.parse.quote(since)}", None, tok)
                rec("sync_incr", dt); since = d.get("next_batch", since)
        return errors
    t0 = time.perf_counter()
    with ThreadPoolExecutor(a.threads) as ex:
        errors = sum(ex.map(chatter, users))
    chat_time = time.perf_counter() - t0
    sends = len(lat["send"])
    print(f"users={a.users} rooms={a.rooms} threads={a.threads}")
    print(f"registration of {a.users} users: {lat['register'][0]:.1f}s")
    print(f"joins: {len(lat['join'])} in {join_time:.1f}s ({len(lat['join'])/join_time:.0f}/s), p50 {pct(lat['join'],.5)*1000:.0f}ms p95 {pct(lat['join'],.95)*1000:.0f}ms")
    print(f"sends: {sends} in {chat_time:.1f}s ({sends/chat_time:.0f} msg/s), errors {errors}, p50 {pct(lat['send'],.5)*1000:.0f}ms p95 {pct(lat['send'],.95)*1000:.0f}ms p99 {pct(lat['send'],.99)*1000:.0f}ms")
    print(f"initial sync: p50 {pct(lat['sync'],.5)*1000:.0f}ms p95 {pct(lat['sync'],.95)*1000:.0f}ms; incremental sync: {len(lat['sync_incr'])} p50 {pct(lat['sync_incr'],.5)*1000:.0f}ms p95 {pct(lat['sync_incr'],.95)*1000:.0f}ms")

if __name__ == "__main__":
    main()
