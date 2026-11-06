#!/usr/bin/env python3
"""`POST /createRoom` under pressure: several rooms at once, and a sync that is watching.

Rooms asked for at the same moment each come into being. Since room version 12 a room's ID
is the hash of its create event, and two create events of one user with the same content and
the same millisecond are one and the same event. The server has to tell them apart.

A room that is being created shows over sync only once all of it is written. `createRoom`
writes a room as a row of events: the create event, the creator's membership, power levels,
what the preset says, `initial_state`, name and topic. A sync of the same user that is
answered between two of them would show a room without, say, its `m.room.encryption` event.
A client built on matrix-rust-sdk then takes the room for unencrypted until a later answer
says otherwise, and a message sent in between leaves the client in plain text. Here one
device creates encrypted rooms as fast as it can, one after the other and four at a time,
while another device of the same user long-polls. For every room, the first answer that
mentions it has to hold the whole room. Both sync flavours.

A room exists whole or not at all. A `createRoom` that fails halfway takes away what it
wrote, and one whose client hangs up halfway goes on to the end.

Needs what scripts/fedlab.py needs. Takes about half a minute.

Usage: scripts/createroomtest.py [--keep]
"""
import json, socket, threading, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor

from fedlab import check, run_lab, wait_for

AT_ONCE = 8
ROUNDS = 5
ROOMS = 25
SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
# What createRoom writes for an encrypted room with a name and a topic, apart from the
# creator's membership.
WHOLE = {"m.room.create", "m.room.power_levels", "m.room.join_rules", "m.room.history_visibility", "m.room.guest_access",
         "m.room.encryption", "m.room.name", "m.room.topic"}
ENCRYPTION = {"type": "m.room.encryption", "state_key": "", "content": {"algorithm": "m.megolm.v1.aes-sha2"}}


def run(hs, peers):
    def create(token, body):
        return hs.req("POST", "/_matrix/client/v3/createRoom", body, token)

    def login(user):
        st, r = hs.req("POST", "/_matrix/client/v3/login", {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user}, "password": "pwpwpwpw"})
        assert st == 200, r
        return r["access_token"]

    print("-- rooms asked for at the same moment")
    for version in ("12", "11"):
        _, token = hs.register(f"twins{version}")
        answers = []
        with ThreadPoolExecutor(AT_ONCE) as pool:
            for _ in range(ROUNDS):
                answers += list(pool.map(lambda _: create(token, {"preset": "private_chat", "room_version": version}), range(AT_ONCE)))
        refused = [(st, r.get("errcode")) for st, r in answers if st != 200]
        rooms = {r["room_id"] for st, r in answers if st == 200}
        check(not refused, f"room version {version}: all {len(answers)} requests are answered with a room ({len(refused)} were not: {sorted(set(refused))})")
        check(len(rooms) == len(answers) - len(refused), f"room version {version}: and each with a room of its own ({len(rooms)} different rooms)")
        st, joined = hs.req("GET", "/_matrix/client/v3/joined_rooms", None, token)
        check(st == 200 and set(joined.get("joined_rooms", [])) == rooms, f"room version {version}: the creator is in exactly those rooms ({len(joined.get('joined_rooms', []))})")

    def encrypted(n, **extra):
        return {"preset": "private_chat", "name": f"Room {n}", "topic": "Whole or not at all", "initial_state": [ENCRYPTION], **extra}

    def timed_create(token, n):
        started = time.monotonic()
        st, r = create(token, encrypted(n))
        assert st == 200, r
        return r["room_id"], time.monotonic() - started

    def watch_sliding(token, seen, stop):
        body = {"conn_id": "watch", "lists": {"all": {"ranges": [[0, 199]], "timeline_limit": 1, "required_state": [[t, ""] for t in sorted(WHOLE)]}}}
        pos = None
        while not stop.is_set():
            st, r = hs.req("POST", f"{SLIDING}?timeout=2000" + (f"&pos={pos}" if pos else ""), body, token)
            assert st == 200, r
            pos = r["pos"]
            for room_id, room in r.get("rooms", {}).items():
                seen.setdefault(room_id, {e["type"] for e in room.get("required_state", [])})

    def watch_classic(token, seen, stop):
        since = None
        while not stop.is_set():
            st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=2000" + (f"&since={since}" if since else ""), None, token)
            assert st == 200, r
            since = r["next_batch"]
            for room_id, room in r.get("rooms", {}).get("join", {}).items():
                events = room.get("state", {}).get("events", []) + room.get("timeline", {}).get("events", [])
                seen.setdefault(room_id, {e["type"] for e in events})

    runs = (("sliding sync", watch_sliding, 1), ("classic sync", watch_classic, 1),
            ("sliding sync, four rooms at a time", watch_sliding, 4), ("classic sync, four rooms at a time", watch_classic, 4))
    for number, (label, watch, at_a_time) in enumerate(runs):
        print(f"-- a room is whole when sync first shows it: {label}")
        user, creator = hs.register(f"creator{number}")
        watcher = login(user)
        seen, stop = {}, threading.Event()
        thread = threading.Thread(target=watch, args=(watcher, seen, stop), daemon=True)
        thread.start()
        time.sleep(0.5)
        with ThreadPoolExecutor(at_a_time) as pool:
            results = list(pool.map(lambda n: timed_create(creator, n), range(ROOMS)))
        made, slowest = [room_id for room_id, _ in results], max(took for _, took in results)
        arrived = wait_for(lambda: all(room_id in seen for room_id in made), 15, 0.1)
        stop.set()
        thread.join(10)
        check(arrived, f"the other device learns of all {ROOMS} rooms ({sum(room_id in seen for room_id in made)})")
        partial = {room_id: sorted(WHOLE - seen[room_id]) for room_id in made if room_id in seen and not WHOLE <= seen[room_id]}
        missing = sorted({t for types in partial.values() for t in types})
        check(not partial, f"every room is whole in the first answer that mentions it ({len(partial)} of {ROOMS} were not; missing: {missing})")
        check(slowest < 5, f"creating a room stays quick while a sync is waiting (slowest {slowest:.2f} s)")
        # Nothing got lost on the way: a sync from scratch knows the same rooms.
        if watch is watch_classic:
            st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, watcher)
            assert st == 200, r
            joined = r.get("rooms", {}).get("join", {})
            check(set(made) <= set(joined), f"a fresh sync of that device has all {ROOMS} rooms ({len(set(made) & set(joined))})")

    print("-- other rooms while a room is being created (classic sync)")
    user, creator = hs.register("busy")
    watcher = login(user)
    st, r = create(creator, {"preset": "private_chat", "name": "Old room"})
    assert st == 200, r
    old_room = r["room_id"]
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, watcher)
    assert st == 200, r
    since, got, created, stop = r["next_batch"], [], [], threading.Event()
    _, host_token = hs.register("host")
    invited_to, invitations = [], {}
    # A timeline long enough for everything that waits behind a room in the making.
    wide = urllib.parse.quote(json.dumps({"room": {"timeline": {"limit": 1000}}}))

    def chatter():
        n = 0
        while not stop.is_set():
            hs.message(creator, old_room, f"message {n}")
            n += 1
        got.append(n)

    def creating():
        while not stop.is_set():
            st, r = create(creator, encrypted(len(created)))
            if st != 200:
                break
            created.append(r["room_id"])

    def inviting():
        # Someone else invites the user into rooms of theirs, all the while.
        while not stop.is_set():
            st, r = create(host_token, {"preset": "private_chat", "invite": [user]})
            if st != 200:
                break
            invited_to.append(r["room_id"])

    threads = [threading.Thread(target=t, daemon=True) for t in (chatter, creating, inviting)]
    for t in threads:
        t.start()
    bodies, ends = [], time.monotonic() + 4

    def read_on():
        nonlocal since
        st, r = hs.req("GET", f"/_matrix/client/v3/sync?timeout=300&since={since}&filter={wide}", None, watcher)
        assert st == 200, r
        since = r["next_batch"]
        for room_id in r.get("rooms", {}).get("invite", {}):
            invitations[room_id] = invitations.get(room_id, 0) + 1
        old = r.get("rooms", {}).get("join", {}).get(old_room, {}).get("timeline", {})
        assert not old.get("limited"), "the test's timeline is too short for what waited"
        bodies.extend(e["content"]["body"] for e in old.get("events", []) if e["type"] == "m.room.message")

    while time.monotonic() < ends:
        read_on()
    stop.set()
    for t in threads:
        t.join(10)
    for _ in range(4):
        read_on()
    sent = got[0] if got else 0
    check(len(created) > 5, f"rooms were being created all the while ({len(created)})")
    twice = sorted(n for n in invitations.values() if n != 1)
    check(len(invited_to) > 5 and set(invitations) == set(invited_to) and not twice,
          f"every invitation that arrived meanwhile is reported, and once ({len(invitations)} of {len(invited_to)}; reported more than once: {len(twice)})")
    check(sent > 5 and bodies == [f"message {n}" for n in range(sent)], f"the messages of another room arrive once and in order meanwhile ({len(bodies)} of {sent}, {len(set(bodies))} different)")

    print("-- a createRoom that fails halfway")
    user, creator = hs.register("failing")
    st, r = hs.req("GET", "/_matrix/client/v3/sync?timeout=0", None, creator)
    assert st == 200, r
    since = r["next_batch"]
    # In room version 11 the creator's power comes from the power levels. Without any, the
    # power levels event itself is still allowed, and the join rules after it are not.
    st, r = create(creator, encrypted(0, room_version="11", power_level_content_override={"users": {}}))
    check(st != 200, f"a room whose creator gives away all power is turned down ({st} {r.get('errcode')})")
    # The create event, the membership and the power levels had been written by then.
    st, r = hs.req("GET", f"/_matrix/client/v3/sync?timeout=0&since={since}", None, creator)
    left_behind = r.get("rooms", {}).get("join", {})
    st2, joined = hs.req("GET", "/_matrix/client/v3/joined_rooms", None, creator)
    check(st == 200 and not left_behind and joined.get("joined_rooms") == [],
          f"and nothing of it is left: not over sync, not among the creator's rooms ({len(left_behind)}, {joined.get('joined_rooms')})")
    st, r = create(creator, encrypted(1, invite=[user.replace("failing", "creator0")]))
    check(st == 200, f"the same user can create a room afterwards ({st})")
    invited = hs.req("GET", f"/_matrix/client/v3/rooms/{urllib.parse.quote(r['room_id'])}/members?membership=invite", None, creator)[1]
    check(len(invited.get("chunk", [])) == 1, "and its invitation went out")

    print("-- a client that hangs up halfway")
    user, creator = hs.register("impatient")
    host, port = hs.url.removeprefix("http://").split(":")
    for n in range(12):
        body = json.dumps(encrypted(n)).encode()
        request = (f"POST /_matrix/client/v3/createRoom HTTP/1.1\r\nHost: {host}:{port}\r\nAuthorization: Bearer {creator}\r\n"
                   f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n").encode() + body
        with socket.create_connection((host, int(port))) as s_:
            s_.sendall(request)
            # Long enough for the request to be taken up, too short for the room to be done.
            time.sleep(0.004 * (n % 6))
    wait_for(lambda: False, 2)
    st, joined = hs.req("GET", "/_matrix/client/v3/joined_rooms", None, creator)
    rooms = joined.get("joined_rooms", [])
    partial = {}
    for room_id in rooms:
        st, state = hs.req("GET", f"/_matrix/client/v3/rooms/{urllib.parse.quote(room_id)}/state", None, creator)
        missing = WHOLE - {e["type"] for e in state}
        if missing:
            partial[room_id] = sorted(missing)
    check(len(rooms) >= 3, f"rooms come into being although nobody waited for the answer ({len(rooms)} of 12)")
    check(not partial, f"and every one of them is whole ({len(partial)} are not: {sorted({t for m in partial.values() for t in m})})")


if __name__ == "__main__":
    run_lab(["mock"], run)
