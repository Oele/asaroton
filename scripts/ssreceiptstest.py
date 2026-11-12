#!/usr/bin/env python3
"""Sliding sync hands out read receipts for the rooms of a list, also after a lost answer.

A request's `receipts` extension says which rooms it wants receipts for with `lists` and
`rooms`, and each of the two means "all of them" when it is left out. A request that named
only `rooms: ["*"]` (the room subscriptions) used to be read as "and no lists": the rooms of
its lists got no receipts at all, and a reader's mark stayed where the first answer had put
it.

Ben reads on in a room while Ada's client has it in a list. Ada's next answer has to carry
his receipt, and so does the answer to a request from the same position again, as a client
sends when the answer to its waiting request never reached it.

Needs what scripts/fedlab.py needs. Takes a few seconds.

Usage: scripts/ssreceiptstest.py [--keep]
"""
import http.client, json, time, urllib.parse

from fedlab import check, run_lab

SLIDING = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
WORDS = ["one", "two", "three", "four"]


def run(hs, peers):
    q = urllib.parse.quote
    ada, a = hs.register("ada")
    ben, b = hs.register("ben")
    st, r = hs.req("POST", "/_matrix/client/v3/createRoom", {"name": "Reads", "invite": [ben]}, a)
    assert st == 200, r
    room = r["room_id"]
    st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/join", {}, b)
    assert st == 200, r
    ids = []
    for n, word in enumerate(WORDS):
        st, r = hs.req("PUT", f"/_matrix/client/v3/rooms/{q(room)}/send/m.room.message/t{n}", {"msgtype": "m.text", "body": word}, a)
        assert st == 200, r
        ids.append(r["event_id"])

    def reads(word):
        st, r = hs.req("POST", f"/_matrix/client/v3/rooms/{q(room)}/receipt/m.read/{q(ids[WORDS.index(word)])}", {}, b)
        assert st == 200, r

    def sync(extension, pos=None):
        body = {"conn_id": "room-list", "lists": {"all_rooms": {"ranges": [[0, 10]], "timeline_limit": 1, "required_state": []}}, "extensions": {"receipts": extension}}
        st, r = hs.req("POST", f"{SLIDING}?timeout=0" + (f"&pos={pos}" if pos else ""), body, a)
        assert st == 200, r
        marks = []
        for event in r.get("extensions", {}).get("receipts", {}).get("rooms", {}).values():
            for event_id, kinds in event.get("content", {}).items():
                if any(ben in users for users in kinds.values()):
                    marks.append(WORDS[ids.index(event_id)] if event_id in ids else event_id)
        return r["pos"], marks, body

    reads("one")
    for label, extension in (("only `rooms` named", {"enabled": True, "rooms": ["*"]}), ("only `lists` named", {"enabled": True, "lists": ["all_rooms"]}), ("neither named", {"enabled": True})):
        print(f"-- receipts extension with {label}")
        pos, _, _ = sync(extension)
        pos, _, body = sync(extension, pos)
        reads("two")
        _, marks, _ = sync(extension, pos)
        check(marks == ["two"], f"ben reads on, and ada's next answer carries his receipt ({marks})")
        _, marks, _ = sync(extension, pos)
        check(marks == ["two"], f"asked again from the same position, it carries it again ({marks})")
        reads("one")

    print("-- the answer to a waiting request is never read")
    extension = {"enabled": True, "rooms": ["*"]}
    pos, _, _ = sync(extension)
    pos, _, body = sync(extension, pos)
    conn = http.client.HTTPConnection("127.0.0.1", hs.client_port, timeout=60)
    conn.request("POST", f"{SLIDING}?timeout=30000&pos={pos}", json.dumps(body), {"Content-Type": "application/json", "Authorization": "Bearer " + a})
    time.sleep(1)
    reads("three")
    time.sleep(2)       # the server answers the waiting request now
    conn.sock.close()   # and the client is gone without having read it
    after, marks, _ = sync(extension, pos)
    check(marks == ["three"], f"asked again from the position the client still has, the receipt is there ({marks})")
    reads("four")
    _, marks, _ = sync(extension, after)
    check(marks == ["four"], f"and the connection goes on from there ({marks})")
    scoped = {"enabled": True, "lists": ["no_such_list"], "rooms": []}
    pos, _, _ = sync(scoped)
    reads("three")
    _, marks, _ = sync(scoped, pos)
    check(marks == [], f"an extension that names another list and no rooms gets none ({marks})")


if __name__ == "__main__":
    run_lab(["mock"], run)
