#!/usr/bin/env python3
"""A change to global account data reaches the user's sliding sync, also when the connection
is being answered for another reason at that moment.

A client sets its ignore list and expects to see it in its own sync a moment later; until it
does, it goes on showing the ignored user's messages. The change must not fall between two
answers: an answer that is put together around the moment of the write either carries the
change, or leaves the connection at a position from which the next answer does.

Vic's device follows a sliding sync connection with the account data extension. Many times
over, Uma writes in a room they share and Vic sets a new ignore list a moment before or
after; every list has to arrive on the connection within a few seconds, in order.

Needs what scripts/fedlab.py needs. Takes about a minute.

Usage: scripts/ssaccountdatatest.py [--keep] [ROUNDS]
"""
import random, sys, threading, time, urllib.parse, uuid

from fedlab import check, run_lab

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
ROUNDS = int(next((a for a in sys.argv[1:] if a.isdigit()), 150))
WITHIN = 5.0


def run(hs, peers):
    q = urllib.parse.quote
    vic, v = hs.register("vic")
    uma, u = hs.register("uma")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"invite": [uma]}, v)
    assert st == 200, r
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/join/{q(room)}", {}, u)
    assert st == 200, r
    body = {"conn_id": "main", "lists": {"all": {"ranges": [[0, 19]], "timeline_limit": 5, "required_state": []}},
            "extensions": {"account_data": {"enabled": True}, "receipts": {"enabled": True, "rooms": ["*"]}, "typing": {"enabled": True}}}

    arrived = {}        # the number in an ignore list -> when the connection carried it
    stop = threading.Event()

    def follow():
        pos = None
        while not stop.is_set():
            st, r = hs.req("POST", f"{SLIDING}?timeout=3000" + (f"&pos={pos}" if pos else ""), body, v)
            if st != 200:
                arrived["error"] = (st, r)
                return
            pos = r["pos"]
            for ev in r.get("extensions", {}).get("account_data", {}).get("global", []):
                if ev["type"] == "m.ignored_user_list":
                    for name in ev["content"].get("ignored_users", {}):
                        arrived.setdefault(name, time.time())

    follower = threading.Thread(target=follow)
    follower.start()
    time.sleep(0.5)
    rnd = random.Random(7)
    late, lost = [], []
    for n in range(ROUNDS):
        name = f"@ignored{n}:example.org"
        gap = rnd.uniform(-0.15, 0.15)   # the message this long before (or after) the write

        def message():
            hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/{uuid.uuid4().hex}", {"msgtype": "m.text", "body": str(n)}, u)

        other = threading.Timer(max(0.0, 0.15 - gap), message)
        other.start()
        time.sleep(max(0.0, 0.15 + min(gap, 0.0) * 0 + (gap if gap > 0 else 0)))
        st, r = hs.req("PUT", f"/_matrix/client/v3/user/{q(vic)}/account_data/m.ignored_user_list", {"ignored_users": {name: {}}}, v)
        assert st == 200, r
        written = time.time()
        other.join()
        deadline = written + WITHIN
        while name not in arrived and time.time() < deadline and "error" not in arrived:
            time.sleep(0.02)
        if name not in arrived:
            lost.append(n)
        elif arrived[name] - written > 1.5:
            late.append((n, round(arrived[name] - written, 1)))
    stop.set()
    follower.join(10)
    check("error" not in arrived, f"the connection is answered throughout ({arrived.get('error')})")
    check(not lost, f"every one of {ROUNDS} ignore lists reaches the connection within {WITHIN:.0f} s ({len(lost)} did not: {lost[:8]})")
    check(not late, f"and none takes longer than a second and a half ({late[:8]})")


if __name__ == "__main__":
    run_lab(["mock"], run)
