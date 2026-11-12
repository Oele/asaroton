#!/usr/bin/env python3
"""A server that is told to stop finishes what it is working on.

Every deploy stops the server with SIGTERM. It used to end on the spot, with every request
where it was. A request that writes is a row of steps (the event is stored, the room moves on
to it, those who wait are woken, it goes out to other servers, the transaction ID it answers
is noted), and one that was between two of them left the rest undone: the client, which got
no answer, sent it again to the server that came up and had its message in the room twice.

Now the server takes no new connections, answers the requests that wait for news at once,
lets the requests that write finish, also those whose client is gone, and then ends.

The test stops and starts a server a number of times while clients write to it, some of them
hanging up without waiting for the answer, and while two clients wait for news. Each time:
the waiting clients are answered (not cut off), the server ends by itself with exit status 0
well within its time limit and says that every request was finished, and every stored message
has the record of its transaction ID. At the end every message that was not answered is sent
again with the same transaction ID, and each has to be in the room once.

Needs what scripts/fedlab.py needs. Takes about half a minute.

Usage: scripts/stoptest.py [--keep]
"""
import json, random, signal, socket, threading, time, urllib.parse, urllib.request

from fedlab import check, query, run_lab

ROUNDS = 6
WRITERS = 8
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
BODY = "(json::jsonb)->'content'->>'body'"


def cut_off(port, method, path, body, token, after):
    """Send a request and hang up `after` seconds later without reading the answer."""
    data = json.dumps(body).encode()
    head = (
        f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n"
    )
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=2)
    except OSError:
        return False
    try:
        s.sendall(head.encode() + data)
        time.sleep(after)
    except OSError:
        return False
    finally:
        s.close()
    return True


def run(hs, peers):
    q = urllib.parse.quote
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    assert st == 200, r
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r
    send = lambda txn: f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{txn}"
    content = lambda txn: {"msgtype": "m.text", "body": txn}

    answered = {}       # transaction ID -> event ID, for the requests that got their answer
    unanswered = set()  # sent, or possibly sent, and no answer
    lock = threading.Lock()
    waits, exits, said, took = [], [], [], []

    for rnd in range(ROUNDS):
        # Carol is in no room, so nothing happens to her and her two syncs really wait.
        st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, c)
        assert st == 200, r
        since = r["next_batch"]
        sliding = {"conn_id": f"stop-{rnd}", "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 5, "required_state": []}}}
        st, r = hs.req("POST", f"{SLIDING}?timeout=0", sliding, c)
        assert st == 200, r
        pos = r["pos"]
        stopped_at = [None]
        waiting = {}

        def wait(name, method, path, body):
            asked = time.time()
            try:
                st, _ = hs.req(method, path, body, c)
            except Exception as e:  # cut off: no answer at all
                st = type(e).__name__
            now = time.time()
            waiting[name] = (st, now - asked, now - stopped_at[0] if stopped_at[0] else -1)

        waiters = [
            threading.Thread(target=wait, args=("sync", "GET", f"/_matrix/client/v3/sync?timeout=25000&since={q(since)}", None)),
            threading.Thread(target=wait, args=("sliding sync", "POST", f"{SLIDING}?timeout=25000&pos={pos}", sliding)),
        ]
        going = threading.Event()
        going.set()

        def write(seed):
            rng = random.Random(seed)
            token = (a, b)[seed % 2]
            n = 0
            while going.is_set():
                n += 1
                txn = f"stop-{rnd}-{seed}-{n}"
                if rng.random() < 0.3:
                    # A client that hangs up at once, or a little later.
                    with lock:
                        unanswered.add(txn)
                    if not cut_off(hs.client_port, "PUT", send(txn), content(txn), token, rng.uniform(0, 0.004)):
                        return
                    continue
                try:
                    st, r = hs.req("PUT", send(txn), content(txn), token)
                except Exception:
                    with lock:
                        unanswered.add(txn)
                    return
                assert st == 200, (st, r)
                with lock:
                    answered[txn] = r["event_id"]

        writers = [threading.Thread(target=write, args=(rnd * 100 + n,)) for n in range(WRITERS)]
        for t in waiters + writers:
            t.start()
        time.sleep(random.Random(rnd).uniform(0.6, 1.0))
        proc = hs.proc
        stopped_at[0] = time.time()
        proc.send_signal(signal.SIGTERM)
        try:
            exits.append(proc.wait(30))
        except Exception:
            proc.kill()
            exits.append("killed after 30 s")
        took.append(time.time() - stopped_at[0])
        going.clear()
        for t in waiters + writers:
            t.join(40)
        waits.append(dict(waiting))
        said.append(len(hs.log_lines("stopped: every request was finished")))
        hs.proc = None
        hs.start()

    print(f"-- {ROUNDS} stops, each with {WRITERS} clients writing and two waiting for news")
    check(all(e == 0 for e in exits), f"the server ends by itself, with exit status 0 ({exits})")
    check(max(took) < 2, f"well within its time limit of five seconds (the longest took {max(took):.2f} s)")
    check(said == list(range(1, ROUNDS + 1)), f"and says each time that every request was finished ({said})")
    for name in ("sync", "sliding sync"):
        got = [w.get(name, ("no answer", 0, 0)) for w in waits]
        check(all(st == 200 for st, _, _ in got), f"a {name} that waits is answered, not cut off ({[st for st, _, _ in got]})")
        check(all(waited > 0.5 and 0 <= after < 2 for _, waited, after in got),
              f"having waited until the stop, within two seconds of it ({', '.join(f'{waited:.2f}/{after:.3f}' for _, waited, after in got)})")

    stored = dict(query(f"SELECT {BODY}, event_id FROM events WHERE event_type = 'm.room.message' AND {BODY} LIKE 'stop-%'"))
    bare = query(f"SELECT {BODY} FROM events e WHERE event_type = 'm.room.message' AND {BODY} LIKE 'stop-%' AND NOT EXISTS (SELECT 1 FROM event_txn_ids t WHERE t.txn_id = 'event:' || e.event_id)")
    # Enough to have met the stops at work; how many depends on what else the machine does.
    check(len(answered) > 10 * ROUNDS, f"the clients got a good many messages through ({len(answered)} answered, {len(unanswered)} not, {len(stored)} stored)")
    check(not bare, f"every stored message has the record of its transaction ID ({len(bare)} without: {bare[:3]})")
    lost = [txn for txn, event_id in answered.items() if stored.get(txn) != event_id]
    check(not lost, f"every message that was answered is in the room ({len(lost)} are not)")
    forks = int(query("SELECT count(*) FROM (SELECT prev_event_id FROM event_edges GROUP BY 1 HAVING count(*) > 1) AS forked")[0][0])
    check(forks == 0, f"the room is one line of events ({forks} events have two that follow them)")

    print(f"-- the {len(unanswered)} messages without an answer are sent again")
    again = {}
    for txn in sorted(unanswered):
        # By the client that sent it the first time: a transaction ID is a device's own.
        seed = int(txn.split("-")[2])
        st, r = hs.req("PUT", send(txn), content(txn), (a, b)[seed % 2])
        assert st == 200, (st, r)
        again[txn] = r["event_id"]
    copies = {}
    for txn, event_id in query(f"SELECT {BODY}, event_id FROM events WHERE event_type = 'm.room.message' AND {BODY} LIKE 'stop-%'"):
        copies.setdefault(txn, []).append(event_id)
    wrong = [(txn, copies.get(txn)) for txn, event_id in again.items() if copies.get(txn) != [event_id]]
    check(again and not wrong, f"each of them is in the room once, and that is what the second attempt was answered with ({len(wrong)} are not: {wrong[:2]})")
    before = sum(1 for txn in again if txn in stored)
    print(f"   ({before} of them had been stored by the first attempt, {len(again) - before} had not)")


run_lab(["peer"], run)
