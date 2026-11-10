#!/usr/bin/env python3
"""Requests that are cut off halfway leave nothing half done.

A client that goes away in the middle of a request used to take the request down with it: the
server stopped working on it wherever it was. Two things went wrong with that.

Where it was could be between the steps of a write. An event was stored, and the room did not
move on to it, nobody was woken, and the transaction ID it answers was not noted: the client
that tried again got its message twice. Requests that write are now seen through to the end,
and a second attempt waits for the first.

And where it was could be right after BEGIN went to the database. sqlx 0.9.0 then gave the
connection back to the pool inside that transaction, and what the next user wrote on it was
acknowledged and not there for any other connection. That hole is closed in the store, and
the test for it is there (a_call_dropped_while_its_transaction_begins_leaves_the_connection_
clean): the requests that can still be dropped here, the ones that wait for news, begin no
transaction. What this test would notice is that hole coming back together with writes that
can be dropped again.

The test cuts a few thousand requests off after zero to some milliseconds, most of them
writing, some waiting for news, and then looks at what is left: no connection of the server
inside a transaction, every stored message with the record of its transaction ID, the room
one line of events, and no word from the database about a transaction that was already or no
longer in progress. Then it cuts requests off and tries them again at once with the same
transaction ID: a message, a redaction, a to-device message and a delayed event each have to
be there once. So does what another server sends in a transaction that arrives twice at the
same time. And it reads writes that were acknowledged afterwards over another connection.

Needs what scripts/fedlab.py needs. Takes about a minute.

Usage: scripts/droppedrequesttest.py [--keep]
"""
import json, random, socket, struct, threading, time, urllib.parse, uuid

from fedlab import check, query, run_lab, wait_for

THREADS = 8
ROUNDS = 600        # requests per thread
LONGEST = 0.006     # a request is cut off at most this many seconds after it was sent
AGAIN = 400         # requests that are cut off and tried again at once
TWICE = 100         # transactions of another server that arrive twice at the same time
AFTERWARDS = 300    # acknowledged writes that are read back
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
DELAYED = "/_matrix/client/unstable/org.matrix.msc4140"
BODY = "(json::jsonb)->'content'->>'body'"


def raw(method, path, body, token, host):
    data = json.dumps(body).encode()
    head = (
        f"{method} {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {token}\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n"
    )
    return head.encode() + data


def cut_off(port, request, after, reset=False):
    """Send a request and hang up `after` seconds later without reading the answer."""
    s = socket.create_connection(("127.0.0.1", port))
    try:
        s.sendall(request)
        time.sleep(after)
        if reset:
            # Without a goodbye: a reset instead of a closed stream.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    finally:
        s.close()


def settle():
    """Wait until the server has worked off what it was sent: no statement running, and the
    number of events the same, twice in a row."""
    seen = [None]

    def quiet():
        busy = query("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active'")[0][0]
        now = (busy, query("SELECT count(*) FROM events")[0][0], query("SELECT count(*) FROM to_device_messages")[0][0])
        before, seen[0] = seen[0], now
        return busy == "0" and now == before

    return wait_for(quiet, 90, 0.5)


def run(hs, peers):
    q = urllib.parse.quote
    host, port = f"127.0.0.1:{hs.client_port}", hs.client_port
    alice, a = hs.register("alice")
    bob, b = hs.register("bob")
    carol, c = hs.register("carol")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"preset": "public_chat"}, a)
    assert st == 200, r
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, b)
    assert st == 200, r

    # Carol is in no room, so that nothing happens to her and her syncs really wait.
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, c)
    assert st == 200, r
    since = r["next_batch"]
    sliding = {"conn_id": "storm", "lists": {"all": {"ranges": [[0, 9]], "timeline_limit": 5, "required_state": []}}, "extensions": {"to_device": {"enabled": True}, "e2ee": {"enabled": True}}}
    st, r = hs.req("POST", f"{SLIDING}?timeout=0", sliding, c)
    assert st == 200, r
    sliding["pos"] = r["pos"]

    sent = []

    def request(rnd, who, token):
        """A request as bytes. Six kinds write, and take the server through a transaction or
        through a statement of its own; the last two wait for news."""
        txn = uuid.uuid4().hex
        kind = rnd.randrange(8)
        sent.append(kind)
        return [
            lambda: raw("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{txn}", {"msgtype": "m.text", "body": f"storm-{txn}"}, token, host),
            lambda: raw("PUT", f"/_matrix/client/v3/sendToDevice/m.test.marker/{txn}", {"messages": {bob: {"*": {"marker": txn}}}}, token, host),
            lambda: raw("PUT", f"/_matrix/client/v3/user/{q(who)}/rooms/{q(room)}/account_data/test.dropped", {"n": txn}, token, host),
            lambda: raw("PUT", f"/_matrix/client/v3/rooms/{q(room)}/typing/{q(who)}", {"typing": True, "timeout": 1000}, token, host),
            lambda: raw("POST", "/_matrix/client/v3/keys/upload", {"one_time_keys": {f"curve25519:{txn[:8]}": txn}}, token, host),
            lambda: raw("PUT", f"/_matrix/client/v3/rooms/{q(room)}/state/test.dropped/{txn[:4]}", {"n": txn}, token, host),
            lambda: raw("GET", f"/_matrix/client/v3/sync?timeout=2000&since={q(since)}", {}, c, host),
            lambda: raw("POST", f"{SLIDING}?timeout=2000", sliding, c, host),
        ][kind]()

    def storm(seed):
        rnd = random.Random(seed)
        who, token = ((alice, a), (bob, b))[seed % 2]
        for _ in range(ROUNDS):
            cut_off(port, request(rnd, who, token), rnd.uniform(0, LONGEST), reset=rnd.random() < 0.5)

    print(f"-- {THREADS * ROUNDS} requests, each cut off within {LONGEST * 1000:.0f} ms")
    threads = [threading.Thread(target=storm, args=(n,)) for n in range(THREADS)]
    started = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    took = time.time() - started
    check(settle(), f"the server works off what it was sent ({len(sent)} requests in {took:.1f} s, quiet {time.time() - started - took:.1f} s later)")

    # A connection that was left inside a transaction stays there. One that is in the middle of
    # a transaction is there for a moment, which is why this asks for a second.
    time.sleep(1.5)
    left = query(
        "SELECT pid, state FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
        " AND state LIKE 'idle in transaction%' AND state_change < now() - interval '1 second'"
    )
    check(not left, f"no connection of the server is left inside a transaction ({left})")

    # A message that was stored has had all its steps: the note of the transaction ID it
    # answers is the last of them.
    messages = sent.count(0)
    stored = int(query(f"SELECT count(*) FROM events WHERE event_type = 'm.room.message' AND {BODY} LIKE 'storm-%'")[0][0])
    bare = int(query(f"SELECT count(*) FROM events e WHERE event_type = 'm.room.message' AND {BODY} LIKE 'storm-%' AND NOT EXISTS (SELECT 1 FROM event_txn_ids t WHERE t.txn_id = 'event:' || e.event_id)")[0][0])
    check(stored >= messages // 2, f"most of the messages that were cut off are in the room all the same ({stored} of {messages})")
    check(bare == 0, f"and each of them has the record of its transaction ID ({bare} without)")
    forks = int(query("SELECT count(*) FROM (SELECT prev_event_id FROM event_edges GROUP BY 1 HAVING count(*) > 1) AS forked")[0][0])
    check(forks == 0, f"the room is one line of events: none has two that follow it ({forks})")
    st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": "after the storm"}, a)
    check(st == 200, f"a message sent afterwards is accepted ({st})")

    print(f"-- {AGAIN} requests, each cut off and tried again at once with the same transaction ID")
    # Events to redact, sent beforehand.
    targets = {}
    for seed in range(THREADS):
        for n in range(AGAIN // THREADS):
            if n % 4 == 1:
                st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/target-{seed}-{n}", {"msgtype": "m.text", "body": "to be redacted"}, (a, b)[seed % 2])
                assert st == 200, r
                targets[f"again-{seed}-{n}"] = r["event_id"]
    answers = {}

    def again(seed):
        rnd = random.Random(1000 + seed)
        token = (a, b)[seed % 2]
        for n in range(AGAIN // THREADS):
            txn = f"again-{seed}-{n}"
            method, path, body = [
                ("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{txn}", {"msgtype": "m.text", "body": txn}),
                ("PUT", f"/_matrix/client/v3/rooms/{q(room)}/redact/{q(targets.get(txn, ''))}/{txn}", {"reason": txn}),
                ("PUT", f"/_matrix/client/v3/sendToDevice/m.test.again/{txn}", {"messages": {carol: {"*": {"marker": txn}}}}),
                ("PUT", f"{DELAYED}/rooms/{q(room)}/delayed_event/m.room.message/{txn}", {"delay_ms": 3600000, "content": {"msgtype": "m.text", "body": txn}}),
            ][n % 4]
            cut_off(port, raw(method, path, body, token, host), rnd.uniform(0, LONGEST))
            st, r = hs.req(method, path, body, token)
            answers[txn] = (n % 4, r.get("event_id") or r.get("delay_id") or "") if st == 200 else (n % 4, f"{st} {r}")

    threads = [threading.Thread(target=again, args=(n,)) for n in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    settle()
    failed = {txn: answer for txn, (_, answer) in answers.items() if " " in answer}
    check(len(answers) == AGAIN // THREADS * THREADS and not failed, f"every second attempt is answered ({len(failed)} were not: {list(failed.items())[:2]})")
    of = lambda kind: sorted(txn for txn, (k, _) in answers.items() if k == kind)

    def once(what, kind, rows, answered=True):
        """`rows` are (transaction ID, what is there for it) pairs: one for each request of
        this kind, and where there is an answer to compare, the one it was answered with."""
        there = {}
        for txn, thing in rows:
            there.setdefault(txn, []).append(thing)
        wrong = [txn for txn in of(kind) if len(there.get(txn, [])) != 1 or (answered and there[txn][0] != answers[txn][1])]
        check(of(kind) and not wrong, f"{what} ({len(of(kind))} of them, {len(wrong)} not: {[(t, there.get(t), answers[t][1]) for t in wrong[:2]]})")

    once("each message is in the room once, and that is the one the second attempt was answered with", 0,
         query(f"SELECT {BODY}, event_id FROM events WHERE event_type = 'm.room.message' AND {BODY} LIKE 'again-%'"))
    redacted = dict((event_id, txn) for txn, event_id in targets.items())
    once("each redaction likewise", 1,
         [(redacted.get(target), event_id) for target, event_id in query("SELECT (json::jsonb)->'content'->>'redacts', event_id FROM events WHERE event_type = 'm.room.redaction'")])
    once("each to-device message is waiting for its device once", 2,
         query("SELECT content->>'marker', stream_id FROM to_device_messages WHERE event_type = 'm.test.again'"), answered=False)
    once("each delayed event is scheduled once, under the ID the second attempt was answered with", 3,
         query("SELECT txn_id, delay_id FROM delayed_events"))

    print(f"-- {TWICE} transactions from another server, each sent twice at the same time")
    # A server that got no answer in time sends its transaction again, and the first may
    # still be at work.
    peer, statuses = peers["peer"], []

    def transaction(txn):
        edu = {"edu_type": "m.direct_to_device", "content": {"sender": f"@x:{peer.name}", "type": "m.test.twice", "message_id": txn, "messages": {carol: {"*": {"marker": txn}}}}}
        st, _ = peer.fed(hs.fed, "PUT", f"/_matrix/federation/v1/send/{txn}", {"origin": peer.name, "origin_server_ts": int(time.time() * 1000), "pdus": [], "edus": [edu]})
        statuses.append(st)

    for n in range(TWICE):
        both = [threading.Thread(target=transaction, args=(f"twice-{n}",)) for _ in range(2)]
        for t in both:
            t.start()
        for t in both:
            t.join()
    taken_in = dict(query("SELECT content->>'marker', count(*) FROM to_device_messages WHERE event_type = 'm.test.twice' GROUP BY 1"))
    wrong = [f"twice-{n}" for n in range(TWICE) if taken_in.get(f"twice-{n}") != "1"]
    check(statuses == [200] * (2 * TWICE) and not wrong, f"each is answered both times and its to-device message taken in once ({len(wrong)} not: {[(w, taken_in.get(w)) for w in wrong[:3]]}, answers other than 200: {[s for s in statuses if s != 200][:3]})")

    print(f"-- {AFTERWARDS} writes that are answered, read back over another connection")
    for n in range(AFTERWARDS):
        st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(alice)}/account_data/test.visible.{n}", {"n": n}, a)
        assert st == 200, r
    there = int(query("SELECT count(*) FROM account_data WHERE data_type LIKE 'test.visible.%'")[0][0])
    check(there == AFTERWARDS, f"all of them are there ({there} of {AFTERWARDS})")

    already = hs.log_lines("there is already a transaction in progress")
    check(not already, f"the database never said a transaction was already in progress ({len(already)} times)")
    none = hs.log_lines("there is no transaction in progress")
    check(not none, f"nor that there was none to end ({len(none)} times)")
    errors = [l for l in hs.log_lines(" ERROR ")]
    check(not errors, f"and the server logged no error ({len(errors)}: {[e.strip()[:160] for e in errors[:3]]})")


if __name__ == "__main__":
    run_lab(["peer"], run)
